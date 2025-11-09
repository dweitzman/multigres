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
	graph, resourceMap, err := pe.buildDependencyGraph(resources)
	if err != nil {
		return nil, err
	}

	// Start status monitoring
	pe.statusMonitor.Start(resources)
	defer pe.statusMonitor.Stop()

	// Execute provisioning
	results, err := pe.executeProvisioning(ctx, graph, resourceMap)
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

	// Build dependency graph
	graph, resourceMap, err := pe.buildDependencyGraph(resources)
	if err != nil {
		return err
	}

	// Traverse backwards to deprovision in reverse dependency order
	result, err := graph.TraverseBackwards(ctx, func(key string) error {
		r := resourceMap[key]

		// Update status
		pe.statusMonitor.UpdateStatus(r.ID(), StatusDeprovisioning)

		// Deprovision the resource
		if err := r.Deprovision(ctx, pe.context); err != nil {
			pe.statusMonitor.UpdateStatus(r.ID(), StatusFailed)
			return fmt.Errorf("failed to deprovision %s (%s): %w",
				r.ID().Component, r.ID().Name, err)
		}

		// Delete state file
		if err := pe.context.DeleteState(r.ID()); err != nil {
			// Log warning but don't fail
			fmt.Printf("Warning: failed to delete state for %s: %v\n", r.ID().Name, err)
		}

		// Update status
		pe.statusMonitor.UpdateStatus(r.ID(), StatusReady)

		return nil
	})
	if err != nil {
		return err
	}

	// Check if any nodes failed or were skipped
	if len(result.Failed) > 0 || len(result.Skipped) > 0 {
		if pe.failFast && len(result.Failed) > 0 {
			// Return first error
			for _, key := range result.Failed {
				if err := result.Errors[key]; err != nil {
					return err
				}
			}
		}
		// Log all errors
		for key, err := range result.Errors {
			fmt.Printf("Warning: error during deprovisioning of %s: %v\n", key, err)
		}
		if len(result.Failed) > 0 {
			return fmt.Errorf("deprovisioning completed with %d error(s), %d skipped", len(result.Failed), len(result.Skipped))
		}
	}

	return nil
}

// buildDependencyGraph creates a dependency graph from the resources
// Returns the graph and a map from resource key to Resource
func (pe *ProvisionerEngine) buildDependencyGraph(resources []Resource) (*Graph, map[string]Resource, error) {
	graph := NewGraph()
	resourceMap := make(map[string]Resource)

	// Add all nodes
	for _, r := range resources {
		key := resourceKey(r.ID())
		resourceMap[key] = r
		graph.AddNode(key)
	}

	// Add explicit dependencies
	for _, r := range resources {
		for _, depID := range r.Dependencies() {
			graph.AddDependency(resourceKey(r.ID()), resourceKey(depID))
		}
	}

	// Add implicit dependencies
	for _, r := range resources {
		// Global topology is a dependency for all resources except itself
		if r.ID().Component != clustermetadatapb.ID_GLOBAL_TOPO {
			globalTopoID := findResourceByComponent(resources, clustermetadatapb.ID_GLOBAL_TOPO)
			if globalTopoID != nil {
				graph.AddDependency(resourceKey(r.ID()), resourceKey(globalTopoID))
			}
		}

		// Cell topology is a dependency for all cell-scoped resources except cell topology itself
		if r.ID().Cell != topo.GlobalCell && r.ID().Component != clustermetadatapb.ID_CELL_TOPO {
			cellTopoID := findResourceByCellAndComponent(resources, r.ID().Cell, clustermetadatapb.ID_CELL_TOPO)
			if cellTopoID != nil {
				graph.AddDependency(resourceKey(r.ID()), resourceKey(cellTopoID))
			}
		}
	}

	return graph, resourceMap, nil
}

// executeProvisioning executes the provisioning with parallel execution where possible
func (pe *ProvisionerEngine) executeProvisioning(ctx context.Context, graph *Graph, resourceMap map[string]Resource) ([]*provisioner.ProvisionResult, error) {
	var allResults []*provisioner.ProvisionResult
	var resultsMu sync.Mutex

	// Traverse the graph and provision each resource
	result, err := graph.Traverse(ctx, func(key string) error {
		r := resourceMap[key]

		// Update status
		pe.statusMonitor.UpdateStatus(r.ID(), StatusProvisioning)

		// Provision the resource
		provResult, err := r.Provision(ctx, pe.context)
		if err != nil {
			pe.statusMonitor.UpdateStatus(r.ID(), StatusFailed)
			return fmt.Errorf("failed to provision %s (%s): %w",
				r.ID().Component, r.ID().Name, err)
		}

		// Update status
		pe.statusMonitor.UpdateStatus(r.ID(), StatusReady)

		// Add result to collection
		if provResult != nil {
			resultsMu.Lock()
			allResults = append(allResults, provResult)
			resultsMu.Unlock()
		}

		return nil
	})
	if err != nil {
		return allResults, err
	}

	// Check if any nodes failed or were skipped
	if len(result.Failed) > 0 || len(result.Skipped) > 0 {
		if pe.failFast && len(result.Failed) > 0 {
			// Return first error
			for _, key := range result.Failed {
				if err := result.Errors[key]; err != nil {
					return allResults, err
				}
			}
		}
		// Log all errors
		for key, err := range result.Errors {
			fmt.Printf("Warning: error during provisioning of %s: %v\n", key, err)
		}
		if len(result.Failed) > 0 {
			return allResults, fmt.Errorf("provisioning completed with %d error(s), %d skipped", len(result.Failed), len(result.Skipped))
		}
	}

	return allResults, nil
}

// resourceKey generates a unique key for a resource ID
func resourceKey(id *clustermetadatapb.ID) string {
	return fmt.Sprintf("%s:%s:%s", id.Component, id.Cell, id.Name)
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
