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
	"errors"
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"
)

func TestGraph_AddNode(t *testing.T) {
	g := NewGraph()

	// Add a node
	g.AddNode("a")

	if !g.nodes["a"] {
		t.Error("Expected node 'a' to exist")
	}

	// Adding the same node again should be idempotent
	g.AddNode("a")

	if !g.nodes["a"] {
		t.Error("Expected node 'a' to still exist after second add")
	}
}

func TestGraph_AddDependency(t *testing.T) {
	g := NewGraph()

	// Add dependency should auto-create nodes
	g.AddDependency("b", "a")

	if !g.nodes["a"] {
		t.Error("Expected node 'a' to be auto-created")
	}
	if !g.nodes["b"] {
		t.Error("Expected node 'b' to be auto-created")
	}

	// Check dependency was added
	if len(g.dependencies["b"]) != 1 || g.dependencies["b"][0] != "a" {
		t.Error("Expected 'b' to depend on 'a'")
	}

	// Adding the same dependency again should be idempotent
	g.AddDependency("b", "a")

	if len(g.dependencies["b"]) != 1 {
		t.Error("Expected only one dependency after duplicate add")
	}
}

func TestGraph_DetectCycles(t *testing.T) {
	g := NewGraph()

	// Create a cycle: a -> b -> c -> a
	g.AddDependency("a", "b")
	g.AddDependency("b", "c")
	g.AddDependency("c", "a")

	err := g.detectCycles()
	if err == nil {
		t.Error("Expected cycle detection to return error")
	}
}

func TestGraph_TraverseSimple(t *testing.T) {
	g := NewGraph()

	// Create simple graph: a -> b, c -> b
	g.AddDependency("a", "b")
	g.AddDependency("c", "b")

	var mu sync.Mutex
	executed := []string{}

	result, err := g.Traverse(context.Background(), func(id string) error {
		mu.Lock()
		executed = append(executed, id)
		mu.Unlock()
		return nil
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if len(result.Successful) != 3 {
		t.Errorf("Expected 3 successful nodes, got %d", len(result.Successful))
	}

	if len(result.Failed) != 0 {
		t.Errorf("Expected 0 failed nodes, got %d", len(result.Failed))
	}

	if len(result.Skipped) != 0 {
		t.Errorf("Expected 0 skipped nodes, got %d", len(result.Skipped))
	}

	// b should execute before a and c
	sort.Strings(executed)
	if len(executed) != 3 || executed[0] != "a" || executed[1] != "b" || executed[2] != "c" {
		t.Errorf("Expected all nodes to execute, got: %v", executed)
	}
}

func TestGraph_TraverseWithFailure(t *testing.T) {
	g := NewGraph()

	// Create graph: a -> b, c -> b, d -> c
	// If b fails, a should skip. If c fails, d should skip.
	g.AddDependency("a", "b")
	g.AddDependency("c", "b")
	g.AddDependency("d", "c")

	executed := make(map[string]bool)
	var mu sync.Mutex

	result, err := g.Traverse(context.Background(), func(id string) error {
		mu.Lock()
		executed[id] = true
		mu.Unlock()

		if id == "b" {
			return errors.New("b failed")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// b should fail
	if len(result.Failed) != 1 || result.Failed[0] != "b" {
		t.Errorf("Expected 'b' to fail, got: %v", result.Failed)
	}

	// a and c should be skipped because they depend on b
	if len(result.Skipped) != 3 {
		t.Errorf("Expected 3 skipped nodes (a, c, d), got %d: %v", len(result.Skipped), result.Skipped)
	}

	// Only b should have executed (and failed)
	if len(executed) != 1 || !executed["b"] {
		t.Errorf("Expected only 'b' to execute, got: %v", executed)
	}
}

func TestGraph_TraverseConcurrent(t *testing.T) {
	g := NewGraph()

	// Create graph where a, b, c can run concurrently, all depend on d
	g.AddDependency("a", "d")
	g.AddDependency("b", "d")
	g.AddDependency("c", "d")

	var mu sync.Mutex
	execTimes := make(map[string]time.Time)

	result, err := g.Traverse(context.Background(), func(id string) error {
		mu.Lock()
		execTimes[id] = time.Now()
		mu.Unlock()

		// Simulate work
		time.Sleep(10 * time.Millisecond)
		return nil
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if len(result.Successful) != 4 {
		t.Errorf("Expected 4 successful nodes, got %d", len(result.Successful))
	}

	// d should execute before a, b, c
	dTime := execTimes["d"]
	if execTimes["a"].Before(dTime) || execTimes["b"].Before(dTime) || execTimes["c"].Before(dTime) {
		t.Error("Expected 'd' to execute before 'a', 'b', 'c'")
	}

	// a, b, c should execute concurrently (within a small time window)
	// Since they all start after d completes and run for 10ms, they should overlap
	maxTime := execTimes["a"]
	minTime := execTimes["a"]
	for _, id := range []string{"b", "c"} {
		if execTimes[id].After(maxTime) {
			maxTime = execTimes[id]
		}
		if execTimes[id].Before(minTime) {
			minTime = execTimes[id]
		}
	}

	// The spread between first and last execution of a,b,c should be less than their execution time
	// (indicating they ran concurrently, not sequentially)
	spread := maxTime.Sub(minTime)
	if spread > 20*time.Millisecond {
		t.Errorf("Expected concurrent execution with spread < 20ms, got %v", spread)
	}
}

func TestGraph_TraverseWithContext(t *testing.T) {
	g := NewGraph()

	// Create graph: a -> b -> c
	g.AddDependency("a", "b")
	g.AddDependency("b", "c")

	ctx, cancel := context.WithCancel(context.Background())

	executed := make(map[string]bool)
	var mu sync.Mutex

	result, err := g.Traverse(ctx, func(id string) error {
		mu.Lock()
		executed[id] = true
		mu.Unlock()

		if id == "c" {
			// Cancel context after c executes
			cancel()
		}

		return nil
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// c should execute successfully (it cancels context)
	if !executed["c"] {
		t.Error("Expected 'c' to execute")
	}

	// b and a might be skipped if context cancellation propagates quickly enough
	// But at minimum, c should succeed
	if len(result.Successful) < 1 {
		t.Error("Expected at least 1 successful node")
	}
}

func TestGraph_TraverseBackwards(t *testing.T) {
	g := NewGraph()

	// Create graph: a -> b, c -> b
	g.AddDependency("a", "b")
	g.AddDependency("c", "b")

	var mu sync.Mutex
	executed := []string{}

	result, err := g.TraverseBackwards(context.Background(), func(id string) error {
		mu.Lock()
		executed = append(executed, id)
		mu.Unlock()
		return nil
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if len(result.Successful) != 3 {
		t.Errorf("Expected 3 successful nodes, got %d", len(result.Successful))
	}

	// In reverse order, a and c should execute before b
	// Find the position of b
	bPos := -1
	aPos := -1
	cPos := -1
	for i, id := range executed {
		switch id {
		case "b":
			bPos = i
		case "a":
			aPos = i
		case "c":
			cPos = i
		}
	}

	if bPos == -1 || aPos == -1 || cPos == -1 {
		t.Errorf("Not all nodes executed: %v", executed)
	}

	// a and c should execute before b (higher positions in the list)
	if aPos > bPos || cPos > bPos {
		t.Errorf("Expected 'a' and 'c' to execute before 'b', got order: %v", executed)
	}
}

func TestGraph_TraverseCycleDetection(t *testing.T) {
	g := NewGraph()

	// Create a cycle: a -> b -> c -> a
	g.AddDependency("a", "b")
	g.AddDependency("b", "c")
	g.AddDependency("c", "a")

	_, err := g.Traverse(context.Background(), func(id string) error {
		return nil
	})

	if err == nil {
		t.Error("Expected Traverse to return error for cyclic graph")
	}

	if err != nil && err.Error() == "" {
		t.Error("Expected error message for cycle")
	}
}

func TestGraph_TraverseComplexDependencies(t *testing.T) {
	g := NewGraph()

	// Create a more complex graph:
	//     a
	//    / \
	//   b   c
	//    \ / \
	//     d   e
	//      \ /
	//       f
	g.AddDependency("a", "b")
	g.AddDependency("a", "c")
	g.AddDependency("b", "d")
	g.AddDependency("c", "d")
	g.AddDependency("c", "e")
	g.AddDependency("d", "f")
	g.AddDependency("e", "f")

	var mu sync.Mutex
	executed := []string{}
	execTimes := make(map[string]time.Time)

	result, err := g.Traverse(context.Background(), func(id string) error {
		mu.Lock()
		executed = append(executed, id)
		execTimes[id] = time.Now()
		mu.Unlock()
		return nil
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if len(result.Successful) != 6 {
		t.Errorf("Expected 6 successful nodes, got %d", len(result.Successful))
	}

	// Verify dependency order
	// f must execute before d and e
	// d must execute before b and c
	// b and c must execute before a
	// e must execute before c

	for _, dep := range []struct{ from, to string }{
		{"a", "b"},
		{"a", "c"},
		{"b", "d"},
		{"c", "d"},
		{"c", "e"},
		{"d", "f"},
		{"e", "f"},
	} {
		fromTime, fromOk := execTimes[dep.from]
		toTime, toOk := execTimes[dep.to]

		if !fromOk || !toOk {
			t.Errorf("Missing execution time for %s or %s", dep.from, dep.to)
			continue
		}

		if fromTime.Before(toTime) {
			t.Errorf("Expected %s (dependency) to execute before %s, but %s executed at %v and %s at %v",
				dep.to, dep.from, dep.to, toTime, dep.from, fromTime)
		}
	}
}

func TestGraph_TraverseEmptyGraph(t *testing.T) {
	g := NewGraph()

	result, err := g.Traverse(context.Background(), func(id string) error {
		t.Error("Function should not be called for empty graph")
		return nil
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if len(result.Successful) != 0 || len(result.Failed) != 0 || len(result.Skipped) != 0 {
		t.Error("Expected empty result for empty graph")
	}
}

func TestGraph_TransitiveFailures(t *testing.T) {
	g := NewGraph()

	// Create linear chain: a -> b -> c -> d
	g.AddDependency("a", "b")
	g.AddDependency("b", "c")
	g.AddDependency("c", "d")

	executed := make(map[string]bool)
	var mu sync.Mutex

	result, err := g.Traverse(context.Background(), func(id string) error {
		mu.Lock()
		executed[id] = true
		mu.Unlock()

		if id == "c" {
			return fmt.Errorf("c failed")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// d should succeed, c should fail, b and a should be skipped
	if len(result.Successful) != 1 || result.Successful[0] != "d" {
		t.Errorf("Expected only 'd' to succeed, got: %v", result.Successful)
	}

	if len(result.Failed) != 1 || result.Failed[0] != "c" {
		t.Errorf("Expected only 'c' to fail, got: %v", result.Failed)
	}

	if len(result.Skipped) != 2 {
		t.Errorf("Expected 2 skipped nodes (a, b), got %d: %v", len(result.Skipped), result.Skipped)
	}

	// Only d and c should have executed
	if len(executed) != 2 || !executed["c"] || !executed["d"] {
		t.Errorf("Expected 'c' and 'd' to execute, got: %v", executed)
	}
}
