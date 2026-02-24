// Copyright 2026 Supabase, Inc.
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

package dstsim_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/tools/dstsim"
)

// Simple counter node for testing
type CounterNode struct {
	id    int
	count int
}

func (n *CounterNode) ID() int { return n.id }
func (n *CounterNode) Step(tick int) []string {
	n.count++
	return nil
}

// Condition: counter equals specific value
type CounterEquals struct {
	nodeID int
	value  int
}

func (c *CounterEquals) Name() string { return fmt.Sprintf("counter_equals_%d", c.value) }
func (c *CounterEquals) Eval(sim *dstsim.Simulator[int, string, int]) bool {
	for _, node := range sim.Nodes() {
		if counter := node.(*CounterNode); counter.ID() == c.nodeID {
			return counter.count == c.value
		}
	}
	return false
}

func (c *CounterEquals) Describe(sim *dstsim.Simulator[int, string, int]) string {
	for _, node := range sim.Nodes() {
		if counter := node.(*CounterNode); counter.ID() == c.nodeID {
			return fmt.Sprintf("counter=%d, expected=%d", counter.count, c.value)
		}
	}
	return "node not found"
}

// Condition: counter greater than value
type CounterGreaterThan struct {
	nodeID int
	value  int
}

func (c *CounterGreaterThan) Name() string {
	return fmt.Sprintf("counter_greater_than_%d", c.value)
}

func (c *CounterGreaterThan) Eval(sim *dstsim.Simulator[int, string, int]) bool {
	for _, node := range sim.Nodes() {
		if counter := node.(*CounterNode); counter.ID() == c.nodeID {
			return counter.count > c.value
		}
	}
	return false
}

func (c *CounterGreaterThan) Describe(sim *dstsim.Simulator[int, string, int]) string {
	for _, node := range sim.Nodes() {
		if counter := node.(*CounterNode); counter.ID() == c.nodeID {
			return fmt.Sprintf("counter=%d, threshold=%d", counter.count, c.value)
		}
	}
	return "node not found"
}

// Condition: counter less than or equal to value
type CounterLessOrEqual struct {
	nodeID int
	value  int
}

func (c *CounterLessOrEqual) Name() string {
	return fmt.Sprintf("counter_less_or_equal_%d", c.value)
}

func (c *CounterLessOrEqual) Eval(sim *dstsim.Simulator[int, string, int]) bool {
	for _, node := range sim.Nodes() {
		if counter := node.(*CounterNode); counter.ID() == c.nodeID {
			return counter.count <= c.value
		}
	}
	return false
}

func (c *CounterLessOrEqual) Describe(sim *dstsim.Simulator[int, string, int]) string {
	for _, node := range sim.Nodes() {
		if counter := node.(*CounterNode); counter.ID() == c.nodeID {
			return fmt.Sprintf("counter=%d, max=%d", counter.count, c.value)
		}
	}
	return "node not found"
}

// Simple tick handler that delivers ticks to all nodes
type SimpleTickHandler struct {
	nodes []int
}

func (h *SimpleTickHandler) OnTick(sim *dstsim.Simulator[int, string, int], tick int64) {
	for _, nodeID := range h.nodes {
		sim.DeliverIndicator(nodeID, int(tick))
	}
}

// Condition type enum for table-driven tests
type ConditionType int

const (
	CondEquals ConditionType = iota
	CondGreaterThan
	CondLessOrEqual
)

// TestAssertions_Comprehensive tests all assertion types with both success and failure cases
func TestAssertions_Comprehensive(t *testing.T) {
	tests := []struct {
		name        string
		quantifier  dstsim.TemporalQuantifier
		condType    ConditionType
		targetValue int
		numTicks    int
		shouldPass  bool
	}{
		// Always tests - condition must be true at every tick
		{"Always_Pass_LessOrEqual100For50Ticks", dstsim.Always, CondLessOrEqual, 100, 50, true},
		{"Always_Fail_LessOrEqual100For150Ticks", dstsim.Always, CondLessOrEqual, 100, 150, false},

		// Never tests - condition must never be true
		{"Never_Pass_NeverEquals50In20Ticks", dstsim.Never, CondEquals, 50, 20, true},
		{"Never_Fail_Equals50In60Ticks", dstsim.Never, CondEquals, 50, 60, false},

		// Sometimes tests - condition must be true at least once
		{"Sometimes_Pass_Hits25In50Ticks", dstsim.Sometimes, CondEquals, 25, 50, true},
		{"Sometimes_Fail_NeverHits1000In50Ticks", dstsim.Sometimes, CondEquals, 1000, 50, false},

		// Finally tests - condition must be true at the end
		{"Finally_Pass_GreaterThan40At50", dstsim.Finally, CondGreaterThan, 40, 50, true},
		{"Finally_Fail_GreaterThan1000At50", dstsim.Finally, CondGreaterThan, 1000, 50, false},

		// EventuallyAlways tests - condition must become true and stay true
		{"EventuallyAlways_Pass_GreaterThan10Stays", dstsim.EventuallyAlways, CondGreaterThan, 10, 50, true},
		{"EventuallyAlways_Fail_NeverReaches1000", dstsim.EventuallyAlways, CondGreaterThan, 1000, 50, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sim := dstsim.NewSimulator[int, string, int](dstsim.SimulatorOptions{Seed: 1})

			node := &CounterNode{id: 1}
			sim.RegisterNode(node)

			// Create the appropriate condition
			var cond dstsim.Condition[int, string, int]
			switch tt.condType {
			case CondLessOrEqual:
				cond = &CounterLessOrEqual{nodeID: 1, value: tt.targetValue}
			case CondGreaterThan:
				cond = &CounterGreaterThan{nodeID: 1, value: tt.targetValue}
			case CondEquals:
				cond = &CounterEquals{nodeID: 1, value: tt.targetValue}
			}

			// Add the assertion based on quantifier
			switch tt.quantifier {
			case dstsim.Always:
				sim.Always(cond)
			case dstsim.Never:
				sim.Never(cond)
			case dstsim.Sometimes:
				sim.Sometimes(cond)
			case dstsim.Finally:
				sim.Finally(cond)
			case dstsim.EventuallyAlways:
				sim.EventuallyAlways(cond)
			}

			sim.SetTickHandler(&SimpleTickHandler{nodes: []int{1}})

			initialTick := sim.CurrentTick()
			err := sim.RunUntil(initialTick + int64(tt.numTicks))

			if tt.shouldPass {
				require.NoError(t, err, "assertion should be satisfied")
			} else {
				require.Error(t, err, "assertion should be violated")
				var assertErr *dstsim.AssertionViolation
				require.ErrorAs(t, err, &assertErr, "error should be AssertionViolation")
			}
		})
	}
}

// TestAssertions_MultipleConditions tests that multiple assertions can coexist
func TestAssertions_MultipleConditions(t *testing.T) {
	sim := dstsim.NewSimulator[int, string, int](dstsim.SimulatorOptions{Seed: 1})

	node := &CounterNode{id: 1}
	sim.RegisterNode(node)

	// Multiple assertions can coexist
	sim.Never(&CounterGreaterThan{nodeID: 1, value: 100})  // Should never exceed 100
	sim.Sometimes(&CounterEquals{nodeID: 1, value: 25})    // Should hit 25 at some point
	sim.Finally(&CounterGreaterThan{nodeID: 1, value: 40}) // Should be > 40 at end

	sim.SetTickHandler(&SimpleTickHandler{nodes: []int{1}})

	initialTick := sim.CurrentTick()
	err := sim.RunUntil(initialTick + 50)

	require.NoError(t, err, "all assertions should be satisfied")
}
