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

	"github.com/multigres/multigres/go/pb/clustermetadata"
)

// ResourceOrchestrator manages the parallel execution of resource provisioning
// with dependency resolution and error handling.
type ResourceOrchestrator struct {
	root          *ResourceNode
	nodes         map[string]*ResourceNode // ID string -> Node
	errorStrategy ErrorStrategy
	progressChan  chan *ResourceNode // For sending progress updates
	provisioner   *localProvisioner  // Reference to provisioner for config access
}

// NewResourceOrchestrator creates a new orchestrator for the given root resource.
// It builds and validates the resource graph before returning.
func NewResourceOrchestrator(root Resource, errorStrategy ErrorStrategy, provisioner *localProvisioner) (*ResourceOrchestrator, error) {
	o := &ResourceOrchestrator{
		nodes:         make(map[string]*ResourceNode),
		errorStrategy: errorStrategy,
		progressChan:  make(chan *ResourceNode, 100), // Buffered channel for progress updates
		provisioner:   provisioner,
	}

	// Build the resource graph
	rootNode, err := o.buildGraph(root)
	if err != nil {
		return nil, fmt.Errorf("failed to build resource graph: %w", err)
	}
	o.root = rootNode

	// Validate the graph (check for cycles, missing dependencies, etc.)
	if err := o.validateGraph(); err != nil {
		return nil, fmt.Errorf("invalid resource graph: %w", err)
	}

	return o, nil
}

// buildGraph recursively builds the resource graph from the root resource.
func (o *ResourceOrchestrator) buildGraph(resource Resource) (*ResourceNode, error) {
	if resource == nil {
		return nil, fmt.Errorf("resource is nil")
	}

	id := resource.ID()
	if id == nil {
		return nil, fmt.Errorf("resource has nil ID")
	}

	idStr := ResourceIDString(id)

	// Check if we've already seen this resource (prevents infinite loops)
	if existing, ok := o.nodes[idStr]; ok {
		return existing, nil
	}

	// Create node for this resource
	node := NewResourceNode(resource)
	o.nodes[idStr] = node

	// Recursively build children
	for _, child := range resource.Children() {
		childNode, err := o.buildGraph(child)
		if err != nil {
			return nil, err
		}
		node.Children = append(node.Children, childNode)
	}

	return node, nil
}

// validateGraph checks that the resource graph is valid.
func (o *ResourceOrchestrator) validateGraph() error {
	// Check that all dependencies exist in the graph
	for idStr, node := range o.nodes {
		for _, depID := range node.Resource.Dependencies() {
			depIDStr := ResourceIDString(depID)
			if _, ok := o.nodes[depIDStr]; !ok {
				return fmt.Errorf("resource %s depends on %s, but %s not found in graph",
					idStr, depIDStr, depIDStr)
			}
		}
	}

	// Check for cycles using DFS
	visited := make(map[string]bool)
	recStack := make(map[string]bool)

	var hasCycle func(idStr string) bool
	hasCycle = func(idStr string) bool {
		if recStack[idStr] {
			return true
		}
		if visited[idStr] {
			return false
		}

		visited[idStr] = true
		recStack[idStr] = true

		node := o.nodes[idStr]
		for _, depID := range node.Resource.Dependencies() {
			depIDStr := ResourceIDString(depID)
			if hasCycle(depIDStr) {
				return true
			}
		}

		recStack[idStr] = false
		return false
	}

	for idStr := range o.nodes {
		if hasCycle(idStr) {
			return fmt.Errorf("cycle detected in resource dependencies involving %s", idStr)
		}
	}

	return nil
}

// Execute runs discovery and provisioning for all resources in parallel where possible,
// respecting dependencies. Returns an error if any resource fails, depending on the error strategy.
func (o *ResourceOrchestrator) Execute(ctx context.Context) error {
	var wg sync.WaitGroup
	errChan := make(chan error, len(o.nodes))
	cancelCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Start a goroutine for each resource
	for _, node := range o.nodes {
		wg.Add(1)
		go func(n *ResourceNode) {
			defer wg.Done()

			// Execute this resource
			if err := o.executeResource(cancelCtx, n, cancel); err != nil {
				errChan <- err
			}
		}(node)
	}

	// Wait for all goroutines to complete
	wg.Wait()
	close(errChan)
	close(o.progressChan)

	// Collect errors
	var errs []error
	for err := range errChan {
		errs = append(errs, err)
	}

	if len(errs) > 0 {
		return fmt.Errorf("provisioning failed with %d error(s): %v", len(errs), errs[0])
	}

	return nil
}

// getImplicitDependencies returns the implicit dependencies for a resource.
// All resources except CLUSTER and GLOBAL_TOPO implicitly depend on GLOBAL_TOPO.
// Cell resources (except CELL_TOPO) implicitly depend on their cell's CELL_TOPO.
func (o *ResourceOrchestrator) getImplicitDependencies(node *ResourceNode) []*clustermetadata.ID {
	resID := node.Resource.ID()
	if resID == nil {
		return nil
	}

	var deps []*clustermetadata.ID

	// Skip implicit dependencies for cluster root and global topo itself
	if resID.Component == clustermetadata.ID_CLUSTER || resID.Component == clustermetadata.ID_GLOBAL_TOPO {
		return nil
	}

	// All non-cluster, non-global-topo resources depend on global topo
	deps = append(deps, &clustermetadata.ID{
		Component: clustermetadata.ID_GLOBAL_TOPO,
		Cell:      "global",
		Name:      "etcd",
	})

	// Cell resources (except cell topo itself) depend on cell topo
	if resID.Cell != "global" && resID.Cell != "" && resID.Component != clustermetadata.ID_CELL_TOPO {
		// TODO: We need a way to identify the cell topo for this cell
		// For now, skip this implicit dependency - it can be added explicitly if needed
	}

	return deps
}

// buildProvisionContext creates a ProvisionContext for a resource.
func (o *ResourceOrchestrator) buildProvisionContext(node *ResourceNode) (*ProvisionContext, error) {
	provCtx := &ProvisionContext{}

	// Get topology config from provisioner
	if o.provisioner != nil {
		topoConfig, err := o.provisioner.getTopologyConfig()
		if err == nil {
			provCtx.TopoBackend = topoConfig.Backend
			provCtx.TopoGlobalRoot = topoConfig.GlobalRootPath
		}
	}

	// Get cell name from resource ID
	resID := node.Resource.ID()
	if resID != nil && resID.Cell != "global" {
		provCtx.CellName = resID.Cell
	}

	// Try to find etcd service to get address
	etcdIDStr := ResourceIDString(&clustermetadata.ID{
		Component: clustermetadata.ID_GLOBAL_TOPO,
		Cell:      "global",
		Name:      "etcd",
	})

	if etcdNode, ok := o.nodes[etcdIDStr]; ok {
		// etcd node exists, get its address from metadata
		status := etcdNode.GetStatus()
		if addr, ok := status.Metadata["address"].(string); ok {
			provCtx.EtcdAddress = addr
		}
	}

	return provCtx, nil
}

// executeResource executes a single resource: waits for dependencies, discovers, and provisions.
func (o *ResourceOrchestrator) executeResource(ctx context.Context, node *ResourceNode, cancel context.CancelFunc) error {
	// Collect all dependencies (explicit + implicit)
	allDeps := node.Resource.Dependencies()
	implicitDeps := o.getImplicitDependencies(node)
	allDeps = append(allDeps, implicitDeps...)

	// Wait for all dependencies to complete
	for _, depID := range allDeps {
		depIDStr := ResourceIDString(depID)
		depNode, ok := o.nodes[depIDStr]
		if !ok {
			// Implicit dependency might not exist in graph, skip it
			continue
		}

		// Wait for dependency to complete
		select {
		case <-depNode.completed:
			// Check if dependency failed
			status := depNode.GetStatus()
			if status.State == StateFailed {
				// Dependency failed, mark this resource as failed too
				node.SetStatus(ResourceStatus{
					State:   StateFailed,
					Message: fmt.Sprintf("dependency %s failed", depIDStr),
					Error:   fmt.Errorf("dependency %s failed: %w", depIDStr, status.Error),
				})
				close(node.started)
				close(node.completed)
				o.progressChan <- node

				// Handle error based on strategy
				if o.errorStrategy == ErrorStrategyFailFast {
					cancel() // Cancel all other resources
				}
				return status.Error
			}
		case <-ctx.Done():
			// Context cancelled (e.g., fail-fast triggered by another resource)
			node.SetStatus(ResourceStatus{
				State:   StateFailed,
				Message: "cancelled due to context cancellation",
				Error:   ctx.Err(),
			})
			close(node.started)
			close(node.completed)
			return ctx.Err()
		}
	}

	// All dependencies satisfied, start execution
	close(node.started)

	// Check if context is already cancelled
	if ctx.Err() != nil {
		node.SetStatus(ResourceStatus{
			State:   StateFailed,
			Message: "cancelled before execution",
			Error:   ctx.Err(),
		})
		close(node.completed)
		return ctx.Err()
	}

	// Build provision context
	provCtx, err := o.buildProvisionContext(node)
	if err != nil {
		node.SetStatus(ResourceStatus{
			State:   StateFailed,
			Message: "failed to build provision context",
			Error:   fmt.Errorf("build provision context failed: %w", err),
		})
		close(node.completed)
		o.progressChan <- node

		if o.errorStrategy == ErrorStrategyFailFast {
			cancel()
		}
		return err
	}

	// Discover
	status, err := node.Resource.Discover(ctx, provCtx)
	if err != nil {
		node.SetStatus(ResourceStatus{
			State:   StateFailed,
			Message: "discovery failed",
			Error:   fmt.Errorf("discover failed: %w", err),
		})
		close(node.completed)
		o.progressChan <- node

		if o.errorStrategy == ErrorStrategyFailFast {
			cancel()
		}
		return err
	}

	// Update status after discovery
	node.SetStatus(status)
	o.progressChan <- node

	// Provision if needed
	if status.State == StateNotFound {
		node.SetStatus(ResourceStatus{
			State:   StateProvisioning,
			Message: "provisioning",
		})
		o.progressChan <- node

		status, err = node.Resource.Provision(ctx, provCtx)
		if err != nil {
			node.SetStatus(ResourceStatus{
				State:   StateFailed,
				Message: "provisioning failed",
				Error:   fmt.Errorf("provision failed: %w", err),
			})
			close(node.completed)
			o.progressChan <- node

			if o.errorStrategy == ErrorStrategyFailFast {
				cancel()
			}
			return err
		}

		// Update with final provisioned status
		node.SetStatus(status)
		o.progressChan <- node
	}

	close(node.completed)
	return nil
}

// ProgressChan returns a channel that receives progress updates as resources complete.
// This channel is closed when Execute completes.
func (o *ResourceOrchestrator) ProgressChan() <-chan *ResourceNode {
	return o.progressChan
}

// GetRootNode returns the root resource node.
func (o *ResourceOrchestrator) GetRootNode() *ResourceNode {
	return o.root
}

// GetAllNodes returns a map of all resource nodes by their ID string.
func (o *ResourceOrchestrator) GetAllNodes() map[string]*ResourceNode {
	return o.nodes
}
