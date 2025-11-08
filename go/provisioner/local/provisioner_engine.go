// Copyright 2025 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package local

import (
	"context"
	"fmt"
	"sync"

	"github.com/multigres/multigres/go/clustermetadata/topo"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/provisioner"
)

// ProvisionerEngine orchestrates the provisioning of resources with
// dependency management and parallel execution
type ProvisionerEngine struct {
	context       ProvisionContext
	statusMonitor *StatusMonitor
	failFast      bool // Stop on first error if true
}

// NewProvisionerEngine creates a new provisioner engine
func NewProvisionerEngine(pctx ProvisionContext, failFast bool) *ProvisionerEngine {
	return &ProvisionerEngine{
		context:       pctx,
		statusMonitor: NewStatusMonitor(),
		failFast:      failFast,
	}
}

// ProvisionResources provisions a list of resources, respecting dependencies
// and provisioning in parallel where possible
func (pe *ProvisionerEngine) ProvisionResources(ctx context.Context, resources []Resource) ([]*provisioner.ProvisionResult, error) {
	if len(resources) == 0 {
		return []*provisioner.ProvisionResult{}, nil
	}

	// Build dependency graph
	graph := pe.buildDependencyGraph(resources)

	// Start status monitoring
	pe.statusMonitor.Start(resources)
	defer pe.statusMonitor.Stop()

	// Execute provisioning with topological ordering
	results, err := pe.executeProvisioning(ctx, graph)
	if err != nil {
		return results, err
	}

	return results, nil
}

// DeprovisionResources deprovisions a list of resources in reverse dependency order
func (pe *ProvisionerEngine) DeprovisionResources(ctx context.Context, resources []Resource) error {
	if len(resources) == 0 {
		return nil
	}

	// Build dependency graph (we'll reverse it for deprovisioning)
	graph := pe.buildDependencyGraph(resources)

	// Deprovision in reverse topological order
	levels := graph.topologicalLevels()

	// Track errors
	var allErrors []error
	var errorsMu sync.Mutex

	// Reverse the levels for deprovisioning
	for i := len(levels) - 1; i >= 0; i-- {
		level := levels[i]

		// Deprovision all resources in this level in parallel
		var wg sync.WaitGroup

		for _, resource := range level {
			wg.Add(1)
			go func(r Resource) {
				defer wg.Done()

				// Update status
				pe.statusMonitor.UpdateStatus(r.ID(), StatusDeprovisioning)

				// Deprovision the resource
				if err := r.Deprovision(ctx, pe.context); err != nil {
					err = fmt.Errorf("failed to deprovision %s (%s): %w",
						r.ID().Component, r.ID().Name, err)
					errorsMu.Lock()
					allErrors = append(allErrors, err)
					errorsMu.Unlock()
					pe.statusMonitor.UpdateStatus(r.ID(), StatusFailed)

					if pe.failFast {
						return
					}
					return
				}

				// Delete state file
				if err := pe.context.DeleteState(r.ID()); err != nil {
					// Log warning but don't fail
					fmt.Printf("Warning: failed to delete state for %s: %v\n", r.ID().Name, err)
				}

				// Update status
				pe.statusMonitor.UpdateStatus(r.ID(), StatusReady)
			}(resource)
		}

		wg.Wait()

		// Check for errors
		if len(allErrors) > 0 {
			if pe.failFast {
				return allErrors[0]
			}
			// Continue but remember the errors
			for _, err := range allErrors {
				fmt.Printf("Warning: error during deprovisioning: %v\n", err)
			}
		}
	}

	if len(allErrors) > 0 {
		return fmt.Errorf("deprovisioning completed with %d error(s)", len(allErrors))
	}

	return nil
}

// buildDependencyGraph creates a dependency graph from the resources
func (pe *ProvisionerEngine) buildDependencyGraph(resources []Resource) *dependencyGraph {
	graph := newDependencyGraph()

	// Add all resources to the graph
	for _, r := range resources {
		graph.addResource(r)
	}

	// Add explicit dependencies
	for _, r := range resources {
		for _, depID := range r.Dependencies() {
			graph.addDependency(r.ID(), depID)
		}
	}

	// Add implicit dependencies
	for _, r := range resources {
		// Global topology is a dependency for all resources except itself
		if r.ID().Component != clustermetadatapb.ID_GLOBAL_TOPO {
			globalTopoID := findResourceByComponent(resources, clustermetadatapb.ID_GLOBAL_TOPO)
			if globalTopoID != nil {
				graph.addDependency(r.ID(), globalTopoID)
			}
		}

		// Cell topology is a dependency for all cell-scoped resources except cell topology itself
		if r.ID().Cell != topo.GlobalCell && r.ID().Component != clustermetadatapb.ID_CELL_TOPO {
			cellTopoID := findResourceByCellAndComponent(resources, r.ID().Cell, clustermetadatapb.ID_CELL_TOPO)
			if cellTopoID != nil {
				graph.addDependency(r.ID(), cellTopoID)
			}
		}
	}

	return graph
}

// executeProvisioning executes the provisioning with parallel execution where possible
func (pe *ProvisionerEngine) executeProvisioning(ctx context.Context, graph *dependencyGraph) ([]*provisioner.ProvisionResult, error) {
	// Get topological levels (resources that can be provisioned in parallel at each level)
	levels := graph.topologicalLevels()

	var allResults []*provisioner.ProvisionResult
	var resultsMu sync.Mutex

	// Track errors
	var allErrors []error
	var errorsMu sync.Mutex

	// Provision each level in sequence, but provision resources within each level in parallel
	for _, level := range levels {
		var wg sync.WaitGroup

		for _, resource := range level {
			wg.Add(1)
			go func(r Resource) {
				defer wg.Done()

				// Update status
				pe.statusMonitor.UpdateStatus(r.ID(), StatusProvisioning)

				// Provision the resource
				result, err := r.Provision(ctx, pe.context)
				if err != nil {
					err = fmt.Errorf("failed to provision %s (%s): %w",
						r.ID().Component, r.ID().Name, err)
					errorsMu.Lock()
					allErrors = append(allErrors, err)
					errorsMu.Unlock()
					pe.statusMonitor.UpdateStatus(r.ID(), StatusFailed)

					if pe.failFast {
						return
					}
					return
				}

				// Update status
				pe.statusMonitor.UpdateStatus(r.ID(), StatusReady)

				// Add result to collection
				if result != nil {
					resultsMu.Lock()
					allResults = append(allResults, result)
					resultsMu.Unlock()
				}
			}(resource)
		}

		wg.Wait()

		// Check for errors
		if len(allErrors) > 0 {
			if pe.failFast {
				return allResults, allErrors[0]
			}
			// Continue but log the errors
			for _, err := range allErrors {
				fmt.Printf("Warning: error during provisioning: %v\n", err)
			}
		}
	}

	if len(allErrors) > 0 {
		return allResults, fmt.Errorf("provisioning completed with %d error(s)", len(allErrors))
	}

	return allResults, nil
}

// findResourceByComponent finds a resource ID by component type
func findResourceByComponent(resources []Resource, component clustermetadatapb.ID_ComponentType) *clustermetadatapb.ID {
	for _, r := range resources {
		if r.ID().Component == component {
			return r.ID()
		}
	}
	return nil
}

// findResourceByCellAndComponent finds a resource ID by cell and component type
func findResourceByCellAndComponent(resources []Resource, cell string, component clustermetadatapb.ID_ComponentType) *clustermetadatapb.ID {
	for _, r := range resources {
		if r.ID().Cell == cell && r.ID().Component == component {
			return r.ID()
		}
	}
	return nil
}

// dependencyGraph represents a directed acyclic graph of resource dependencies
type dependencyGraph struct {
	resources    map[string]Resource
	dependencies map[string][]string // resourceKey -> []dependencyKeys
	mu           sync.RWMutex
}

// newDependencyGraph creates a new dependency graph
func newDependencyGraph() *dependencyGraph {
	return &dependencyGraph{
		resources:    make(map[string]Resource),
		dependencies: make(map[string][]string),
	}
}

// resourceKey generates a unique key for a resource ID
func resourceKey(id *clustermetadatapb.ID) string {
	return fmt.Sprintf("%s:%s:%s", id.Component, id.Cell, id.Name)
}

// addResource adds a resource to the graph
func (g *dependencyGraph) addResource(r Resource) {
	g.mu.Lock()
	defer g.mu.Unlock()

	key := resourceKey(r.ID())
	g.resources[key] = r

	// Initialize dependencies slice if not exists
	if _, exists := g.dependencies[key]; !exists {
		g.dependencies[key] = []string{}
	}
}

// addDependency adds a dependency relationship (from depends on to)
func (g *dependencyGraph) addDependency(from, to *clustermetadatapb.ID) {
	g.mu.Lock()
	defer g.mu.Unlock()

	fromKey := resourceKey(from)
	toKey := resourceKey(to)

	// Add dependency if not already present
	if !contains(g.dependencies[fromKey], toKey) {
		g.dependencies[fromKey] = append(g.dependencies[fromKey], toKey)
	}
}

// topologicalLevels returns resources grouped by dependency level
// Resources in the same level can be provisioned in parallel
func (g *dependencyGraph) topologicalLevels() [][]Resource {
	g.mu.RLock()
	defer g.mu.RUnlock()

	// Calculate in-degree for each resource
	inDegree := make(map[string]int)
	for key := range g.resources {
		inDegree[key] = 0
	}
	for _, deps := range g.dependencies {
		for _, dep := range deps {
			inDegree[dep]++
		}
	}

	// Find all resources with no dependencies (in-degree 0)
	var levels [][]Resource
	queue := []string{}
	for key, degree := range inDegree {
		if degree == 0 {
			queue = append(queue, key)
		}
	}

	// Process resources level by level
	for len(queue) > 0 {
		levelSize := len(queue)
		var currentLevel []Resource

		for i := 0; i < levelSize; i++ {
			key := queue[0]
			queue = queue[1:]

			resource := g.resources[key]
			currentLevel = append(currentLevel, resource)

			// Reduce in-degree for resources that depend on this one
			for depKey := range g.resources {
				if contains(g.dependencies[depKey], key) {
					inDegree[depKey]--
					if inDegree[depKey] == 0 {
						queue = append(queue, depKey)
					}
				}
			}
		}

		levels = append(levels, currentLevel)
	}

	// Check for cycles (any resources with non-zero in-degree)
	for key, degree := range inDegree {
		if degree > 0 {
			panic(fmt.Sprintf("circular dependency detected involving resource %s", key))
		}
	}

	return levels
}

// contains checks if a string slice contains a string
func contains(slice []string, item string) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}
