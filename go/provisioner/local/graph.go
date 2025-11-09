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
)

// Graph is a directed acyclic graph of string node IDs with dependencies
type Graph struct {
	nodes        map[string]bool     // set of all node IDs
	dependencies map[string][]string // node ID -> dependency IDs
}

// TraverseResult contains the results of graph traversal
type TraverseResult struct {
	Successful []string         // IDs of nodes that completed successfully
	Failed     []string         // IDs of nodes that failed
	Skipped    []string         // IDs of nodes that were skipped due to dependency failures
	Errors     map[string]error // Errors for failed nodes
}

// NewGraph creates a new dependency graph
func NewGraph() *Graph {
	return &Graph{
		nodes:        make(map[string]bool),
		dependencies: make(map[string][]string),
	}
}

// AddNode adds a node to the graph
// This operation is idempotent - adding the same node multiple times is safe
func (g *Graph) AddNode(id string) {
	if !g.nodes[id] {
		g.nodes[id] = true
		g.dependencies[id] = []string{}
	}
}

// AddDependency adds a dependency edge: from depends on to
// This operation is idempotent - adding the same dependency multiple times is safe
// Nodes will be created automatically if they don't exist
func (g *Graph) AddDependency(from, to string) {
	// Ensure both nodes exist
	g.AddNode(from)
	g.AddNode(to)

	// Add the dependency if not already present
	for _, existing := range g.dependencies[from] {
		if existing == to {
			return // Already exists
		}
	}
	g.dependencies[from] = append(g.dependencies[from], to)
}

// detectCycles checks for circular dependencies in the graph
// This is called internally by Traverse to ensure the graph is acyclic
func (g *Graph) detectCycles() error {
	visited := make(map[string]bool)
	recStack := make(map[string]bool)

	var dfs func(string) error
	dfs = func(id string) error {
		visited[id] = true
		recStack[id] = true

		for _, depID := range g.dependencies[id] {
			if !visited[depID] {
				if err := dfs(depID); err != nil {
					return err
				}
			} else if recStack[depID] {
				return fmt.Errorf("circular dependency detected: %s -> %s", id, depID)
			}
		}

		recStack[id] = false
		return nil
	}

	for id := range g.nodes {
		if !visited[id] {
			if err := dfs(id); err != nil {
				return err
			}
		}
	}

	return nil
}

// Traverse executes the provided function for each node in dependency order
// Each node is executed as soon as all its dependencies complete successfully
// Nodes execute concurrently when their dependencies are satisfied
// If a node fails, all nodes that depend on it (transitively) are skipped
// Returns an error if the graph contains cycles
func (g *Graph) Traverse(ctx context.Context, fn func(id string) error) (*TraverseResult, error) {
	// Check for cycles first
	if err := g.detectCycles(); err != nil {
		return nil, err
	}

	result := &TraverseResult{
		Successful: []string{},
		Failed:     []string{},
		Skipped:    []string{},
		Errors:     make(map[string]error),
	}

	// Create a channel for each node that closes when the node is done (success or fail)
	doneChan := make(map[string]chan struct{})
	for id := range g.nodes {
		doneChan[id] = make(chan struct{})
	}

	// Track node completion status
	var statusMu sync.Mutex
	completed := make(map[string]bool)
	failed := make(map[string]bool)

	// Build reverse dependency map for failure cascading
	reverseDeps := make(map[string][]string)
	for id, deps := range g.dependencies {
		for _, depID := range deps {
			reverseDeps[depID] = append(reverseDeps[depID], id)
		}
	}

	// Start a goroutine for each node
	var wg sync.WaitGroup
	for id := range g.nodes {
		wg.Add(1)
		go func(nodeID string) {
			defer wg.Done()
			defer close(doneChan[nodeID])

			// Wait for all dependencies to complete
			for _, depID := range g.dependencies[nodeID] {
				select {
				case <-doneChan[depID]:
					// Dependency done, check if it failed
					statusMu.Lock()
					depFailed := failed[depID]
					statusMu.Unlock()

					if depFailed {
						// Dependency failed, skip this node
						statusMu.Lock()
						failed[nodeID] = true
						result.Skipped = append(result.Skipped, nodeID)
						statusMu.Unlock()
						return
					}
				case <-ctx.Done():
					// Context cancelled
					statusMu.Lock()
					result.Skipped = append(result.Skipped, nodeID)
					statusMu.Unlock()
					return
				}
			}

			// Check context again before executing
			if ctx.Err() != nil {
				statusMu.Lock()
				result.Skipped = append(result.Skipped, nodeID)
				statusMu.Unlock()
				return
			}

			// All dependencies satisfied, execute the node
			err := fn(nodeID)

			statusMu.Lock()
			if err != nil {
				failed[nodeID] = true
				result.Failed = append(result.Failed, nodeID)
				result.Errors[nodeID] = err

				// Mark all transitive dependents as failed
				g.markTransitiveDependentsFailed(nodeID, failed, reverseDeps)
			} else {
				completed[nodeID] = true
				result.Successful = append(result.Successful, nodeID)
			}
			statusMu.Unlock()
		}(id)
	}

	// Wait for all nodes to complete
	wg.Wait()

	return result, nil
}

// markTransitiveDependentsFailed marks all nodes that transitively depend on the given node as failed
// Must be called with statusMu held
func (g *Graph) markTransitiveDependentsFailed(failedID string, failed map[string]bool, reverseDeps map[string][]string) {
	// BFS to mark all transitive dependents
	queue := []string{failedID}
	visited := make(map[string]bool)
	visited[failedID] = true

	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]

		for _, dependent := range reverseDeps[current] {
			if !visited[dependent] {
				visited[dependent] = true
				failed[dependent] = true
				queue = append(queue, dependent)
			}
		}
	}
}

// TraverseBackwards executes the provided function in reverse topological order
// This is useful for deprovisioning, where dependents should be stopped before their dependencies
func (g *Graph) TraverseBackwards(ctx context.Context, fn func(id string) error) (*TraverseResult, error) {
	// Create an inverted graph
	inverted := NewGraph()

	// Add all nodes to inverted graph
	for id := range g.nodes {
		inverted.nodes[id] = true
	}

	// Build reverse dependency map and add to inverted graph
	reverseDeps := make(map[string][]string)
	for id, deps := range g.dependencies {
		for _, depID := range deps {
			reverseDeps[depID] = append(reverseDeps[depID], id)
		}
	}

	// In the inverted graph, dependencies become dependents
	for id := range inverted.nodes {
		inverted.dependencies[id] = reverseDeps[id]
	}

	// Traverse the inverted graph
	return inverted.Traverse(ctx, fn)
}
