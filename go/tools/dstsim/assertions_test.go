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
func (n *CounterNode) Step(tick int64, indicators []int) []string {
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

			err := sim.RunFor(int64(tt.numTicks))

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

	err := sim.RunFor(50)

	require.NoError(t, err, "all assertions should be satisfied")
}

// TestRelativeTickCondition tests that RelativeTickCondition measures ticks relative to first evaluation
func TestRelativeTickCondition(t *testing.T) {
	tests := []struct {
		name           string
		ticksToWait    int64
		runUntilOffset int64
		shouldBeTrue   bool
	}{
		{
			name:           "FirstEval_ReturnsFalse",
			ticksToWait:    10,
			runUntilOffset: 0, // Check immediately
			shouldBeTrue:   false,
		},
		{
			name:           "BeforeThreshold_ReturnsFalse",
			ticksToWait:    10,
			runUntilOffset: 5, // 5 ticks < 10
			shouldBeTrue:   false,
		},
		{
			name:           "AtThreshold_ReturnsTrue",
			ticksToWait:    10,
			runUntilOffset: 10, // Exactly 10 ticks
			shouldBeTrue:   true,
		},
		{
			name:           "AfterThreshold_ReturnsTrue",
			ticksToWait:    10,
			runUntilOffset: 20, // 20 ticks > 10
			shouldBeTrue:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sim := dstsim.NewSimulator[int, string, int](dstsim.SimulatorOptions{Seed: 1})
			node := &CounterNode{id: 1}
			sim.RegisterNode(node)

			cond := dstsim.TickCondition[int, string, int](tt.ticksToWait)

			// First evaluation should always return false
			require.False(t, cond.Eval(sim), "first evaluation should always return false")

			if tt.runUntilOffset > 0 {
				// Run simulation for specified ticks

				_ = sim.RunFor(tt.runUntilOffset)

				// Check condition
				result := cond.Eval(sim)
				if tt.shouldBeTrue {
					require.True(t, result, "condition should be true after %d ticks", tt.runUntilOffset)
				} else {
					require.False(t, result, "condition should be false after %d ticks", tt.runUntilOffset)
				}
			}
		})
	}
}

// TestAndCombinator tests that And only returns true when all sub-conditions are true
func TestAndCombinator(t *testing.T) {
	tests := []struct {
		name       string
		conditions []bool // true = CounterGreaterThan(10), false = CounterGreaterThan(1000)
		runTicks   int64
		shouldPass bool
	}{
		{
			name:       "AllTrue_ReturnsTrue",
			conditions: []bool{true, true, true},
			runTicks:   20,
			shouldPass: true,
		},
		{
			name:       "SomeFalse_ReturnsFalse",
			conditions: []bool{true, false, true},
			runTicks:   20,
			shouldPass: false,
		},
		{
			name:       "AllFalse_ReturnsFalse",
			conditions: []bool{false, false, false},
			runTicks:   20,
			shouldPass: false,
		},
		{
			name:       "SingleTrue_ReturnsTrue",
			conditions: []bool{true},
			runTicks:   20,
			shouldPass: true,
		},
		{
			name:       "SingleFalse_ReturnsFalse",
			conditions: []bool{false},
			runTicks:   20,
			shouldPass: false,
		},
		{
			name:       "Empty_ReturnsTrue", // Vacuous truth
			conditions: []bool{},
			runTicks:   20,
			shouldPass: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sim := dstsim.NewSimulator[int, string, int](dstsim.SimulatorOptions{Seed: 1})
			node := &CounterNode{id: 1}
			sim.RegisterNode(node)

			// Build And condition from sub-conditions
			var subConditions []dstsim.Condition[int, string, int]
			for _, shouldBeTrue := range tt.conditions {
				if shouldBeTrue {
					subConditions = append(subConditions, &CounterGreaterThan{nodeID: 1, value: 10})
				} else {
					subConditions = append(subConditions, &CounterGreaterThan{nodeID: 1, value: 1000})
				}
			}

			andCond := dstsim.And(subConditions...)

			// Use Sometimes assertion - will pass if condition is true at least once
			sim.Sometimes(andCond)

			err := sim.RunFor(tt.runTicks)

			if tt.shouldPass {
				require.NoError(t, err, "And condition should be satisfied")
			} else {
				require.Error(t, err, "And condition should not be satisfied")
			}
		})
	}
}

// TickingNode generates a self-message every tick to ensure message flow
type TickingNode struct {
	id    int
	count int
}

func (n *TickingNode) ID() int { return n.id }
func (n *TickingNode) Step(tick int64, indicators []int) []string {
	n.count++
	return []string{"tick"} // Generate a request every tick
}

// TickRequestHandler processes tick requests by sending indicators back to the node
type TickRequestHandler struct{}

func (h *TickRequestHandler) ProcessRequests(sim *dstsim.Simulator[int, string, int], fromNode int, requests []string) map[int][]int {
	// Send an indicator back to the same node for each tick request
	result := make(map[int][]int)
	for range requests {
		result[fromNode] = append(result[fromNode], 1) // Send a simple indicator
	}
	return result
}

// TestPolicySequence tests that PolicySequence advances stages correctly
func TestPolicySequence(t *testing.T) {
	tests := []struct {
		name         string
		stage1Ticks  int64 // How long until stage 1 condition becomes true
		stage2Ticks  int64 // How long until stage 2 condition becomes true
		runTicks     int64 // How long to run simulation
		expectStage1 bool  // Should we observe stage 1 active
		expectStage2 bool  // Should we observe stage 2 active
	}{
		{
			name:         "StaysInStage0_ConditionNeverMet",
			stage1Ticks:  100,
			stage2Ticks:  100,
			runTicks:     50,
			expectStage1: false,
			expectStage2: false,
		},
		{
			name:         "AdvancesToStage1_FirstConditionMet",
			stage1Ticks:  20,
			stage2Ticks:  100,
			runTicks:     50,
			expectStage1: true,
			expectStage2: false,
		},
		{
			name:         "AdvancesAllStages_BothConditionsMet",
			stage1Ticks:  20,
			stage2Ticks:  40,
			runTicks:     100,
			expectStage1: true,
			expectStage2: true,
		},
		{
			name:         "QuickTransitions",
			stage1Ticks:  5,
			stage2Ticks:  5,
			runTicks:     20,
			expectStage1: true,
			expectStage2: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sim := dstsim.NewSimulator[int, string, int](dstsim.SimulatorOptions{Seed: 1})
			node := &TickingNode{id: 1}
			sim.RegisterNode(node)

			// Use TickRequestHandler to create message flow (needed for policy sequence evaluation)
			sim.SetRequestHandler(&TickRequestHandler{})

			// Create a 3-stage policy sequence
			// Stage 0 -> Stage 1 after stage1Ticks
			// Stage 1 -> Stage 2 after stage2Ticks (relative to stage 1 start)
			policySeq := dstsim.NewPolicySequence[int, string, int](
				sim,
				&dstsim.FastNetwork[int, int]{},
				"stage0",
			)

			stage1Active := policySeq.AppendPolicy(
				&dstsim.FastNetwork[int, int]{},
				dstsim.TickCondition[int, string, int](tt.stage1Ticks),
				"stage1",
			)

			stage2Active := policySeq.AppendPolicy(
				&dstsim.FastNetwork[int, int]{},
				dstsim.TickCondition[int, string, int](tt.stage2Ticks),
				"stage2",
			)

			sim.SetDeliveryPolicy(policySeq)

			// Add Sometimes assertions to check if each stage activates
			// Note: Stage 0 is always active at the start, so we don't need to assert it
			if tt.expectStage1 {
				sim.Sometimes(stage1Active)
			}
			if tt.expectStage2 {
				sim.Sometimes(stage2Active)
			}

			err := sim.RunFor(tt.runTicks)

			require.NoError(t, err, "policy sequence assertions should be satisfied")
		})
	}
}
