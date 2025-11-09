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
	"testing"
	"time"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/provisioner"
)

// mockResource is a simple mock implementation of Resource for testing
type mockResource struct {
	id           *clustermetadatapb.ID
	dependencies []*clustermetadatapb.ID
}

func (m *mockResource) ID() *clustermetadatapb.ID {
	return m.id
}

func (m *mockResource) Dependencies() []*clustermetadatapb.ID {
	return m.dependencies
}

func (m *mockResource) Provision(ctx context.Context, pctx ProvisionContext) (*provisioner.ProvisionResult, error) {
	return nil, nil
}

func (m *mockResource) Deprovision(ctx context.Context, pctx ProvisionContext) error {
	return nil
}

func TestDependencyGraph_FailureCascade(t *testing.T) {
	// Create a dependency graph: A -> B -> C (A depends on B, B depends on C)
	graph := newDependencyGraph()

	resourceA := &mockResource{
		id: &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIORCH, Cell: "zone1", Name: "A"},
	}
	resourceB := &mockResource{
		id: &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "B"},
	}
	resourceC := &mockResource{
		id: &clustermetadatapb.ID{Component: clustermetadatapb.ID_GLOBAL_TOPO, Cell: "", Name: "C"},
	}

	graph.addResource(resourceA)
	graph.addResource(resourceB)
	graph.addResource(resourceC)

	graph.addDependency(resourceA.ID(), resourceB.ID())
	graph.addDependency(resourceB.ID(), resourceC.ID())

	// Mark C as failed
	graph.markFailed(resourceC.ID())

	// Give goroutine time to run
	time.Sleep(100 * time.Millisecond)

	// Check that A and B are also marked as failed (cascading)
	graph.mu.Lock()
	defer graph.mu.Unlock()

	if !graph.failed[resourceKey(resourceC.ID())] {
		t.Error("Resource C should be marked as failed")
	}
	if !graph.failed[resourceKey(resourceB.ID())] {
		t.Error("Resource B should be marked as failed (cascade from C)")
	}
	if !graph.failed[resourceKey(resourceA.ID())] {
		t.Error("Resource A should be marked as failed (cascade from C through B)")
	}
}

func TestDependencyGraph_DoneChannelClosesWhenAllFailed(t *testing.T) {
	// Create a simple graph with 3 resources: A -> B, C (independent)
	graph := newDependencyGraph()

	resourceA := &mockResource{
		id: &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIORCH, Cell: "zone1", Name: "A"},
	}
	resourceB := &mockResource{
		id: &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "B"},
	}
	resourceC := &mockResource{
		id: &clustermetadatapb.ID{Component: clustermetadatapb.ID_GLOBAL_TOPO, Cell: "", Name: "C"},
	}

	graph.addResource(resourceA)
	graph.addResource(resourceB)
	graph.addResource(resourceC)

	graph.addDependency(resourceA.ID(), resourceB.ID())

	// Start the graph (initializes pushEligibleResources)
	ctx := context.Background()
	go func() {
		for {
			resource, err := graph.startEligibleResource(ctx)
			if err != nil || resource == nil {
				break
			}
			// Immediately mark as started and failed
			time.Sleep(10 * time.Millisecond) // Simulate some work
			graph.markFailed(resource.ID())
		}
	}()

	// Wait for doneChan to close (with timeout)
	select {
	case <-graph.doneChan:
		// Success! All resources are done
	case <-time.After(5 * time.Second):
		// Check state for debugging
		graph.mu.Lock()
		t.Fatalf("doneChan did not close within timeout. State: %d completed, %d failed, %d started, %d total",
			len(graph.completed), len(graph.failed), len(graph.started), len(graph.resources))
		graph.mu.Unlock()
	}

	// Verify all resources are failed
	graph.mu.Lock()
	defer graph.mu.Unlock()

	if len(graph.failed) != 3 {
		t.Errorf("Expected 3 failed resources, got %d", len(graph.failed))
	}
}

func TestDependencyGraph_DoneChannelClosesOnMixedCompletion(t *testing.T) {
	// Create a graph where some resources complete and others fail
	graph := newDependencyGraph()

	resourceA := &mockResource{
		id: &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIORCH, Cell: "zone1", Name: "A"},
	}
	resourceB := &mockResource{
		id: &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "B"},
	}
	resourceC := &mockResource{
		id: &clustermetadatapb.ID{Component: clustermetadatapb.ID_DATABASE, Cell: "", Name: "C"},
	}

	graph.addResource(resourceA)
	graph.addResource(resourceB)
	graph.addResource(resourceC)

	graph.addDependency(resourceA.ID(), resourceC.ID())
	graph.addDependency(resourceB.ID(), resourceC.ID())

	// Start the graph
	ctx := context.Background()
	go func() {
		// C should be eligible first
		resource, _ := graph.startEligibleResource(ctx)
		if resource != nil && resource.ID().Name == "C" {
			time.Sleep(10 * time.Millisecond)
			graph.markCompleted(resource.ID()) // C completes successfully
		}

		// A should be eligible after C completes
		resource, _ = graph.startEligibleResource(ctx)
		if resource != nil && resource.ID().Name == "A" {
			time.Sleep(10 * time.Millisecond)
			graph.markFailed(resource.ID()) // A fails
		}

		// B should be eligible after C completes
		resource, _ = graph.startEligibleResource(ctx)
		if resource != nil && resource.ID().Name == "B" {
			time.Sleep(10 * time.Millisecond)
			graph.markCompleted(resource.ID()) // B completes successfully
		}

		// Wait for done
		for {
			resource, err := graph.startEligibleResource(ctx)
			if err != nil || resource == nil {
				break
			}
		}
	}()

	// Wait for doneChan to close (with timeout)
	select {
	case <-graph.doneChan:
		// Success!
	case <-time.After(5 * time.Second):
		graph.mu.Lock()
		t.Fatalf("doneChan did not close within timeout. State: %d completed, %d failed, %d started, %d total",
			len(graph.completed), len(graph.failed), len(graph.started), len(graph.resources))
		graph.mu.Unlock()
	}

	// Verify final state
	graph.mu.Lock()
	defer graph.mu.Unlock()

	if len(graph.completed)+len(graph.failed) != 3 {
		t.Errorf("Expected all 3 resources to be done, got %d completed + %d failed = %d",
			len(graph.completed), len(graph.failed), len(graph.completed)+len(graph.failed))
	}
}

func TestDependencyGraph_ConcurrentProvisioningWithFailures(t *testing.T) {
	// Simulate the cluster scenario with concurrent provisioning and failures
	// Structure:
	//   - Global topo (no deps) -> completes
	//   - Cell zone1 topo (depends on global) -> fails
	//   - Cell zone2 topo (depends on global) -> completes
	//   - Database (depends on both cell topos) -> fails (zone1 topo failed)
	//   - Zone1 services (depend on zone1 topo and database) -> cascade fail
	//   - Zone2 services (depend on zone2 topo and database) -> cascade fail
	graph := newDependencyGraph()

	// Create resources
	globalTopo := &mockResource{
		id: &clustermetadatapb.ID{Component: clustermetadatapb.ID_GLOBAL_TOPO, Cell: "", Name: "global"},
	}
	zone1Topo := &mockResource{
		id: &clustermetadatapb.ID{Component: clustermetadatapb.ID_CELL_TOPO, Cell: "zone1", Name: "zone1"},
	}
	zone2Topo := &mockResource{
		id: &clustermetadatapb.ID{Component: clustermetadatapb.ID_CELL_TOPO, Cell: "zone2", Name: "zone2"},
	}
	database := &mockResource{
		id: &clustermetadatapb.ID{Component: clustermetadatapb.ID_DATABASE, Cell: "", Name: "postgres"},
	}
	zone1Gateway := &mockResource{
		id: &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIGATEWAY, Cell: "zone1", Name: "gateway"},
	}
	zone1Orch := &mockResource{
		id: &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIORCH, Cell: "zone1", Name: "orch"},
	}
	zone1Pooler := &mockResource{
		id: &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "pooler"},
	}
	zone2Gateway := &mockResource{
		id: &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIGATEWAY, Cell: "zone2", Name: "gateway"},
	}
	zone2Orch := &mockResource{
		id: &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIORCH, Cell: "zone2", Name: "orch"},
	}
	zone2Pooler := &mockResource{
		id: &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone2", Name: "pooler"},
	}

	// Add all resources
	graph.addResource(globalTopo)
	graph.addResource(zone1Topo)
	graph.addResource(zone2Topo)
	graph.addResource(database)
	graph.addResource(zone1Gateway)
	graph.addResource(zone1Orch)
	graph.addResource(zone1Pooler)
	graph.addResource(zone2Gateway)
	graph.addResource(zone2Orch)
	graph.addResource(zone2Pooler)

	// Add dependencies
	graph.addDependency(zone1Topo.ID(), globalTopo.ID())
	graph.addDependency(zone2Topo.ID(), globalTopo.ID())
	graph.addDependency(database.ID(), zone1Topo.ID())
	graph.addDependency(database.ID(), zone2Topo.ID())
	graph.addDependency(zone1Gateway.ID(), zone1Topo.ID())
	graph.addDependency(zone1Gateway.ID(), database.ID())
	graph.addDependency(zone1Orch.ID(), zone1Topo.ID())
	graph.addDependency(zone1Orch.ID(), database.ID())
	graph.addDependency(zone1Pooler.ID(), zone1Topo.ID())
	graph.addDependency(zone1Pooler.ID(), database.ID())
	graph.addDependency(zone2Gateway.ID(), zone2Topo.ID())
	graph.addDependency(zone2Gateway.ID(), database.ID())
	graph.addDependency(zone2Orch.ID(), zone2Topo.ID())
	graph.addDependency(zone2Orch.ID(), database.ID())
	graph.addDependency(zone2Pooler.ID(), zone2Topo.ID())
	graph.addDependency(zone2Pooler.ID(), database.ID())

	// Start multiple goroutines to simulate concurrent provisioning
	ctx := context.Background()
	numWorkers := 3

	for i := 0; i < numWorkers; i++ {
		go func(workerID int) {
			for {
				resource, err := graph.startEligibleResource(ctx)
				if err != nil || resource == nil {
					return
				}

				// Simulate provisioning work
				time.Sleep(5 * time.Millisecond)

				// Determine outcome based on resource
				switch resource.ID().Name {
				case "zone1": // zone1Topo fails
					graph.markFailed(resource.ID())
				case "global": // globalTopo completes
					graph.markCompleted(resource.ID())
				case "zone2": // zone2Topo completes
					graph.markCompleted(resource.ID())
				default:
					// Database and all services should be marked as failed due to cascading
					// Don't manually mark them - let the cascade handle it
					// This simulates what happens in the real cluster where we don't get to provision them
				}
			}
		}(i)
	}

	// Wait for doneChan to close (with timeout)
	select {
	case <-graph.doneChan:
		// Success!
	case <-time.After(5 * time.Second):
		graph.mu.Lock()
		t.Fatalf("doneChan did not close within timeout. State: %d completed, %d failed, %d started, %d total resources=%v",
			len(graph.completed), len(graph.failed), len(graph.started), len(graph.resources), graph.resources)
		graph.mu.Unlock()
	}

	// Verify final state
	graph.mu.Lock()
	defer graph.mu.Unlock()

	// Should have 2 completed (global, zone2), 8 failed (zone1 + cascade)
	if len(graph.completed) != 2 {
		t.Errorf("Expected 2 completed resources, got %d", len(graph.completed))
	}
	if len(graph.failed) != 8 {
		t.Errorf("Expected 8 failed resources, got %d", len(graph.failed))
	}
	if len(graph.completed)+len(graph.failed) != 10 {
		t.Errorf("Expected all 10 resources to be done, got %d completed + %d failed = %d",
			len(graph.completed), len(graph.failed), len(graph.completed)+len(graph.failed))
	}

	// Verify specific resources are failed
	expectedFailed := []string{
		resourceKey(zone1Topo.ID()),
		resourceKey(database.ID()),
		resourceKey(zone1Gateway.ID()),
		resourceKey(zone1Orch.ID()),
		resourceKey(zone1Pooler.ID()),
		resourceKey(zone2Gateway.ID()),
		resourceKey(zone2Orch.ID()),
		resourceKey(zone2Pooler.ID()),
	}
	for _, key := range expectedFailed {
		if !graph.failed[key] {
			t.Errorf("Expected resource %s to be failed", key)
		}
	}
}
