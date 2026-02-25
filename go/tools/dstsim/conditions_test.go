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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/tools/dstsim"
)

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

// TestAbsoluteTickReached tests the AbsoluteTick condition
func TestAbsoluteTickReached(t *testing.T) {
	sim := dstsim.NewSimulator[int, string, int](dstsim.SimulatorOptions{Seed: 1})

	node := &CounterNode{id: 1}
	sim.RegisterNode(node)

	startTick := sim.CurrentTick()
	targetTick := startTick + 10

	// Create condition that becomes true at specific absolute tick
	cond := dstsim.AbsoluteTick[int, string, int](targetTick)

	// Verify condition is false before target tick
	require.False(t, cond.Eval(sim), "condition should be false before target tick")

	// Run until target tick
	err := sim.RunUntil(cond, 20)
	require.NoError(t, err)

	// Verify we stopped at the right tick
	require.GreaterOrEqual(t, sim.CurrentTick(), targetTick, "should have reached target tick")
	require.True(t, cond.Eval(sim), "condition should be true at or after target tick")
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

// TestOrCombinator tests that Or returns true when any sub-condition is true
func TestOrCombinator(t *testing.T) {
	tests := []struct {
		name            string
		cond1Value      int  // Node 1 counter value to check
		cond2Value      int  // Node 2 counter value to check
		runTicks        int  // How many ticks to run
		expectSatisfied bool // Should Or be satisfied
	}{
		{
			name:            "neither condition true",
			cond1Value:      100, // Node 1 will never reach 100 in 10 ticks
			cond2Value:      100, // Node 2 will never reach 100 in 10 ticks
			runTicks:        10,
			expectSatisfied: false,
		},
		{
			name:            "first condition true",
			cond1Value:      5,   // Node 1 will reach 5
			cond2Value:      100, // Node 2 won't reach 100
			runTicks:        10,
			expectSatisfied: true,
		},
		{
			name:            "second condition true",
			cond1Value:      100, // Node 1 won't reach 100
			cond2Value:      5,   // Node 2 will reach 5
			runTicks:        10,
			expectSatisfied: true,
		},
		{
			name:            "both conditions true",
			cond1Value:      5, // Node 1 will reach 5
			cond2Value:      5, // Node 2 will reach 5
			runTicks:        10,
			expectSatisfied: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sim := dstsim.NewSimulator[int, string, int](dstsim.SimulatorOptions{Seed: 1})

			node1 := &CounterNode{id: 1}
			node2 := &CounterNode{id: 2}
			sim.RegisterNode(node1)
			sim.RegisterNode(node2)

			// Create Or combinator
			orCond := dstsim.Or(
				&CounterGreaterThan{nodeID: 1, value: tt.cond1Value},
				&CounterGreaterThan{nodeID: 2, value: tt.cond2Value},
			)

			sim.Sometimes(orCond)

			err := sim.RunFor(int64(tt.runTicks))

			if tt.expectSatisfied {
				require.NoError(t, err, "Or condition should be satisfied")
			} else {
				require.Error(t, err, "Or condition should not be satisfied")
			}
		})
	}
}

// TestNotCombinator tests that Not inverts the condition result
func TestNotCombinator(t *testing.T) {
	tests := []struct {
		name        string
		threshold   int  // Counter threshold to check
		runTicks    int  // How many ticks to run
		expectNever bool // Should Not(counter > threshold) never be true (i.e., counter always > threshold)
	}{
		{
			name:        "counter never exceeds threshold - Not is always true",
			threshold:   100,
			runTicks:    10,
			expectNever: false, // Not(counter > 100) is always true (never violated)
		},
		{
			name:        "counter exceeds threshold - Not becomes false",
			threshold:   5,
			runTicks:    10,
			expectNever: true, // Not(counter > 5) becomes false when counter > 5
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sim := dstsim.NewSimulator[int, string, int](dstsim.SimulatorOptions{Seed: 1})

			node := &CounterNode{id: 1}
			sim.RegisterNode(node)

			// Not(counter > threshold) means "counter is NOT greater than threshold"
			notCond := dstsim.Not(&CounterGreaterThan{nodeID: 1, value: tt.threshold})

			sim.Always(notCond) // Assert that Not condition is always true

			err := sim.RunFor(int64(tt.runTicks))

			if tt.expectNever {
				// We expect the Always assertion to be violated (Not becomes false at some point)
				require.Error(t, err, "Not condition should be violated")
			} else {
				// Not condition should always be true
				require.NoError(t, err, "Not condition should always be true")
			}
		})
	}
}

// TestNestedCombinators tests complex nested combinations of And/Or/Not
func TestNestedCombinators(t *testing.T) {
	sim := dstsim.NewSimulator[int, string, int](dstsim.SimulatorOptions{Seed: 1})

	node1 := &CounterNode{id: 1}
	node2 := &CounterNode{id: 2}
	node3 := &CounterNode{id: 3}
	sim.RegisterNode(node1)
	sim.RegisterNode(node2)
	sim.RegisterNode(node3)

	// Complex condition: (node1 > 5 AND node2 > 5) OR (NOT (node3 > 100))
	// Since node3 will never reach 100 in 10 ticks, NOT(node3 > 100) is always true
	// So the entire Or should always be true
	complexCond := dstsim.Or(
		dstsim.And(
			&CounterGreaterThan{nodeID: 1, value: 5},
			&CounterGreaterThan{nodeID: 2, value: 5},
		),
		dstsim.Not(&CounterGreaterThan{nodeID: 3, value: 100}),
	)

	sim.Always(complexCond)

	err := sim.RunFor(10)
	require.NoError(t, err, "complex nested condition should be satisfied")
}

// TestStageActiveCondition tests the StageActiveCondition directly
func TestStageActiveCondition(t *testing.T) {
	sim := dstsim.NewSimulator[int, string, int](dstsim.SimulatorOptions{Seed: 1})

	node := &CounterNode{id: 1}
	sim.RegisterNode(node)

	// Create a PolicySequence
	policySeq := dstsim.NewPolicySequence(sim, &dstsim.FastNetwork[int, int]{}, "initial")

	stage1Active := policySeq.AppendPolicy(
		&dstsim.FastNetwork[int, int]{},
		dstsim.TickCondition[int, string, int](5),
		"stage1",
	)

	sim.SetDeliveryPolicy(policySeq)

	// Initially, stage1 should not be active
	require.False(t, stage1Active.Eval(sim), "stage1 should not be active initially")
	require.Equal(t, "stage_active_stage1", stage1Active.Name())

	// Run for 10 ticks to activate stage1
	err := sim.RunFor(10)
	require.NoError(t, err)

	// Now stage1 should have been active at some point
	// Note: We can't check if it's currently active without running assertions during the simulation
	// But we can verify the Name and Describe methods work
	require.Equal(t, "stage_active_stage1", stage1Active.Name())
	description := stage1Active.Describe(sim)
	require.Contains(t, description, "stage1")
}
