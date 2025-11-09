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

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
)

// dependencyGraph represents a directed acyclic graph of resource dependencies
type dependencyGraph struct {
	resources           map[string]Resource
	dependencies        map[string][]string // resourceKey -> []dependencyKeys (what this resource depends on)
	reverseDependencies map[string][]string // resourceKey -> []dependentKeys (what depends on this resource)
	started             map[string]bool     // resourceKey -> started status
	completed           map[string]bool     // resourceKey -> completed status
	failed              map[string]bool     // resourceKey -> failed status
	eligibleChan        chan Resource       // channel for eligible resources
	doneChan            chan struct{}       // channel that's closed when all resources are done
	fullyDrained        bool                // whether all resources have been started or failed
	initialized         bool                // whether pushEligibleResources has been called for initial resources
	mu                  sync.Mutex
}

// newDependencyGraph creates a new dependency graph
func newDependencyGraph() *dependencyGraph {
	g := &dependencyGraph{
		resources:           make(map[string]Resource),
		dependencies:        make(map[string][]string),
		reverseDependencies: make(map[string][]string),
		started:             make(map[string]bool),
		completed:           make(map[string]bool),
		failed:              make(map[string]bool),
		eligibleChan:        make(chan Resource, 100), // Buffered channel
		doneChan:            make(chan struct{}),
	}
	return g
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

	// Add forward dependency if not already present
	if !contains(g.dependencies[fromKey], toKey) {
		g.dependencies[fromKey] = append(g.dependencies[fromKey], toKey)
	}

	// Add reverse dependency if not already present
	if _, exists := g.reverseDependencies[toKey]; !exists {
		g.reverseDependencies[toKey] = []string{}
	}
	if !contains(g.reverseDependencies[toKey], fromKey) {
		g.reverseDependencies[toKey] = append(g.reverseDependencies[toKey], fromKey)
	}
}

// detectCycles checks for circular dependencies using DFS
func (g *dependencyGraph) detectCycles() error {
	g.mu.Lock()
	defer g.mu.Unlock()

	visited := make(map[string]bool)
	recStack := make(map[string]bool)

	var dfs func(key string) error
	dfs = func(key string) error {
		visited[key] = true
		recStack[key] = true

		for _, dep := range g.dependencies[key] {
			if !visited[dep] {
				if err := dfs(dep); err != nil {
					return err
				}
			} else if recStack[dep] {
				return fmt.Errorf("circular dependency detected involving %s -> %s", key, dep)
			}
		}

		recStack[key] = false
		return nil
	}

	for key := range g.resources {
		if !visited[key] {
			if err := dfs(key); err != nil {
				return err
			}
		}
	}

	return nil
}

// markCompleted marks a resource as completed and pushes newly eligible resources to the channel
func (g *dependencyGraph) markCompleted(id *clustermetadatapb.ID) {
	g.mu.Lock()
	defer g.mu.Unlock()

	key := resourceKey(id)
	g.completed[key] = true

	// Push newly eligible resources to the channel
	g.pushEligibleResources()
}

// cascadeFailureUnlocked cascades failure from a resource to all dependent resources
// Must be called with lock held
func (g *dependencyGraph) cascadeFailureUnlocked(key string) {
	// BFS to cascade failure to all transitive dependents
	queue := []string{key}
	visited := make(map[string]bool)
	visited[key] = true

	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]

		// Mark all resources that depend on this one as failed
		for _, dependent := range g.reverseDependencies[current] {
			if !visited[dependent] {
				visited[dependent] = true
				g.failed[dependent] = true
				queue = append(queue, dependent)
			}
		}
	}
}

// markFailed marks a resource as failed and cascades the failure to all dependent resources
func (g *dependencyGraph) markFailed(id *clustermetadatapb.ID) {
	g.mu.Lock()
	defer g.mu.Unlock()

	key := resourceKey(id)
	g.failed[key] = true

	// Cascade failure to all dependents
	g.cascadeFailureUnlocked(key)

	// Push newly eligible resources to the channel (if any)
	g.pushEligibleResources()
}

// pushEligibleResources finds all eligible resources and pushes them to the channel
// Closes the channel if all resources have been started or failed
// Must be called with lock held
func (g *dependencyGraph) pushEligibleResources() {
	// Check if we're already fully drained
	if g.fullyDrained {
		return
	}

	// Find eligible resources while holding the lock
	resourcesToPush := make([]Resource, 0)

	for key, resource := range g.resources {
		// Skip if already started or failed
		if g.started[key] || g.failed[key] {
			continue
		}

		// Check if any dependency has failed
		depFailed := false
		for _, dep := range g.dependencies[key] {
			if g.failed[dep] {
				// Dependency failed, mark this as failed and cascade to dependents
				g.failed[key] = true
				g.cascadeFailureUnlocked(key)
				depFailed = true
				break
			}
		}

		if depFailed {
			continue
		}

		// Check if all dependencies are completed
		canStart := true
		for _, dep := range g.dependencies[key] {
			if !g.completed[dep] {
				canStart = false
				break
			}
		}

		if canStart {
			resourcesToPush = append(resourcesToPush, resource)
		}
	}

	// Start goroutine to push resources and potentially close done channel
	// Goroutine is needed to avoid blocking on channel writes
	go func() {
		// Push eligible resources to channel
		for _, r := range resourcesToPush {
			g.eligibleChan <- r
		}

		// Re-check if we're fully drained (with lock held)
		// This ensures we catch the latest state, even if more resources
		// completed/failed while this goroutine was queued
		g.mu.Lock()
		allDone := len(g.completed)+len(g.failed) == len(g.resources)
		if allDone && !g.fullyDrained {
			g.fullyDrained = true
			close(g.doneChan)
		}
		g.mu.Unlock()
	}()
}

// allStarted checks if all resources have been started
func (g *dependencyGraph) allStarted() bool {
	g.mu.Lock()
	defer g.mu.Unlock()

	return len(g.started) == len(g.resources)
}

// Wait blocks until all resources are either completed or failed, or the context is cancelled
func (g *dependencyGraph) Wait(ctx context.Context) error {
	select {
	case <-g.doneChan:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// invert creates a new dependency graph with inverted dependencies (for deprovisioning)
// Simply swaps dependencies and reverseDependencies since we maintain both
func (g *dependencyGraph) invert() *dependencyGraph {
	g.mu.Lock()
	defer g.mu.Unlock()

	inverted := newDependencyGraph()

	// Copy all resources
	for key, resource := range g.resources {
		inverted.resources[key] = resource
	}

	// Swap dependencies and reverseDependencies
	inverted.dependencies = make(map[string][]string)
	inverted.reverseDependencies = make(map[string][]string)

	for key, deps := range g.dependencies {
		inverted.reverseDependencies[key] = append([]string(nil), deps...)
	}

	for key, revDeps := range g.reverseDependencies {
		inverted.dependencies[key] = append([]string(nil), revDeps...)
	}

	return inverted
}

// startEligibleResource reads from the eligibleChan and marks the resource as started
// On first call, initializes the graph by pushing initial eligible resources
// Returns (nil, nil) when the channel is closed (all resources are done)
// Returns (nil, ctx.Err()) when the context is cancelled
func (g *dependencyGraph) startEligibleResource(ctx context.Context) (Resource, error) {
	// Initialize on first call
	g.mu.Lock()
	if !g.initialized {
		g.initialized = true
		g.pushEligibleResources() // Pushes initial eligible resources in a goroutine
	}
	g.mu.Unlock()

	for {
		select {
		case resource, ok := <-g.eligibleChan:
			if !ok {
				// Channel is closed, all resources are done
				return nil, nil
			}

			// Mark as started
			g.mu.Lock()
			key := resourceKey(resource.ID())
			// Skip if already started (may happen due to buffered channel)
			if g.started[key] {
				g.mu.Unlock()
				continue // Read next resource from channel
			}
			g.started[key] = true
			g.mu.Unlock()

			return resource, nil

		case <-g.doneChan:
			// All resources are done (completed or failed)
			return nil, nil

		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
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
