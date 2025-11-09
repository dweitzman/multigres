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

	// Check for circular dependencies
	if err := graph.detectCycles(); err != nil {
		return nil, err
	}

	// Start status monitoring
	pe.statusMonitor.Start(resources)
	defer pe.statusMonitor.Stop()

	// Execute provisioning
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

	// Build dependency graph and invert it for deprovisioning
	graph := pe.buildDependencyGraph(resources)
	invertedGraph := graph.invert()

	// Track errors
	var allErrors []error
	var errorsMu sync.Mutex

	// Start resources as they become eligible
	for !invertedGraph.allStarted() {
		resource, err := invertedGraph.startEligibleResource(ctx)
		if err != nil {
			// Context cancelled
			return err
		}

		if resource == nil {
			// No more resources to start (all started or failed)
			break
		}

		// Start goroutine to deprovision this resource
		go func(r Resource) {
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

				// Mark as failed in the graph (cascades to dependents)
				invertedGraph.markFailed(r.ID())

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

			// Mark as completed in the graph
			invertedGraph.markCompleted(r.ID())
		}(resource)
	}

	// Wait for all resources to complete or fail
	if err := invertedGraph.Wait(ctx); err != nil {
		return err
	}

	// Check for errors
	if len(allErrors) > 0 {
		if pe.failFast {
			return allErrors[0]
		}
		// Log all errors
		for _, err := range allErrors {
			fmt.Printf("Warning: error during deprovisioning: %v\n", err)
		}
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
	var allResults []*provisioner.ProvisionResult
	var resultsMu sync.Mutex

	// Track errors
	var allErrors []error
	var errorsMu sync.Mutex

	// Start resources as they become eligible
	for !graph.allStarted() {
		resource, err := graph.startEligibleResource(ctx)
		if err != nil {
			// Context cancelled
			return allResults, err
		}

		if resource == nil {
			// No more resources to start (all started or failed)
			break
		}

		// Start goroutine to provision this resource
		go func(r Resource) {
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

				// Mark as failed in the graph (cascades to dependents)
				graph.markFailed(r.ID())

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

			// Mark as completed in the graph
			graph.markCompleted(r.ID())
		}(resource)
	}

	// Wait for all resources to complete or fail
	if err := graph.Wait(ctx); err != nil {
		return allResults, err
	}

	// Check for errors
	if len(allErrors) > 0 {
		if pe.failFast {
			return allResults, allErrors[0]
		}
		// Log all errors
		for _, err := range allErrors {
			fmt.Printf("Warning: error during provisioning: %v\n", err)
		}
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
