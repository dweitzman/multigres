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
	"math/rand/v2"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/tools/dstsim"
)

// AlwaysGeneratingNode generates a request on every tick to ensure continuous activity
type AlwaysGeneratingNode struct {
	id    int
	count int
}

func (n *AlwaysGeneratingNode) ID() int { return n.id }
func (n *AlwaysGeneratingNode) Step(tick int64, indicators []int) []string {
	// Count delivered indicators
	n.count += len(indicators)
	// Always generate a request to keep the loop going
	return []string{"tick"}
}

// TestUnreliableNetwork tests that UnreliableNetwork drops and delays messages
func TestUnreliableNetwork(t *testing.T) {
	tests := []struct {
		name        string
		maxDelay    int64
		dropRate    float64
		seed        int64
		runTicks    int
		expectDrops bool // Whether we expect some messages to be dropped
	}{
		{
			name:        "zero drop rate - all messages delivered",
			maxDelay:    5,
			dropRate:    0.0,
			seed:        42,
			runTicks:    50,
			expectDrops: false,
		},
		{
			name:        "high drop rate - some messages dropped",
			maxDelay:    5,
			dropRate:    0.5, // 50% drop rate
			seed:        42,
			runTicks:    50,
			expectDrops: true,
		},
		{
			name:        "100% drop rate - all messages dropped",
			maxDelay:    5,
			dropRate:    1.0,
			seed:        42,
			runTicks:    50,
			expectDrops: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sim := dstsim.NewSimulator[int, string, int](dstsim.SimulatorOptions{Seed: tt.seed})

			// Set delivery policy after construction
			sim.SetDeliveryPolicy(&dstsim.UnreliableNetwork[int, int]{
				MaxDelay: tt.maxDelay,
				DropRate: tt.dropRate,
				Rng:      rand.New(rand.NewPCG(uint64(tt.seed), uint64(tt.seed))),
			})

			// Use AlwaysGeneratingNode that generates requests every tick
			node := &AlwaysGeneratingNode{id: 1}
			sim.RegisterNode(node)

			// Set up request handler that delivers indicators back to the node
			sim.SetRequestHandler(&TickRequestHandler{})

			err := sim.RunFor(int64(tt.runTicks))
			require.NoError(t, err)

			// With deterministic seed and RNG, we get exact counts
			if !tt.expectDrops {
				// 46 indicators delivered: 50 ticks generated 50 requests, but some are still
				// in flight due to random delays (1-5 ticks). With seed 42, 4 are delayed.
				require.Equal(t, 46, node.count, "with 0%% drop rate")
			} else if tt.dropRate == 1.0 {
				// 0 indicators delivered: all messages dropped
				require.Equal(t, 0, node.count, "with 100%% drop rate")
			} else {
				// 23 indicators delivered: approximately half of 50 (minus in-flight) dropped
				require.Equal(t, 23, node.count, "with 50%% drop rate")
			}
		})
	}
}

// TestUntilPolicy tests that UntilPolicy switches permanently when condition becomes true
func TestUntilPolicy(t *testing.T) {
	tests := []struct {
		name         string
		switchAtTick int64
		runTicks     int64
		expectSwitch bool
	}{
		{
			name:         "switch before simulation ends",
			switchAtTick: 10,
			runTicks:     20,
			expectSwitch: true,
		},
		{
			name:         "switch at end of simulation",
			switchAtTick: 20,
			runTicks:     20,
			expectSwitch: false, // Condition becomes true exactly when sim ends, so AfterPolicy never used
		},
		{
			name:         "no switch - condition never true",
			switchAtTick: 100,
			runTicks:     20,
			expectSwitch: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sim := dstsim.NewSimulator[int, string, int](dstsim.SimulatorOptions{Seed: 1})

			// Track whether each policy was used
			initialPolicyUsed := false
			afterPolicyUsed := false

			// Create tracking policies
			initialPolicy := &testTrackingPolicy[int, int]{
				usedPtr: &initialPolicyUsed,
				delay:   1,
			}
			afterPolicy := &testTrackingPolicy[int, int]{
				usedPtr: &afterPolicyUsed,
				delay:   1,
			}

			// Create UntilPolicy that switches after N ticks (relative, not absolute)
			untilPolicy := &dstsim.UntilPolicy[int, string, int]{
				UntilCondition: dstsim.TickCondition[int, string, int](tt.switchAtTick),
				InitialPolicy:  initialPolicy,
				AfterPolicy:    afterPolicy,
				Sim:            sim, // Required for condition evaluation
			}

			sim.SetDeliveryPolicy(untilPolicy)

			node := &AlwaysGeneratingNode{id: 1}
			sim.RegisterNode(node)
			sim.SetRequestHandler(&TickRequestHandler{})

			err := sim.RunFor(tt.runTicks)
			require.NoError(t, err)

			// Verify policy usage
			if tt.expectSwitch {
				require.True(t, initialPolicyUsed, "initial policy should be used before switch")
				require.True(t, afterPolicyUsed, "after policy should be used after switch")
			} else {
				require.True(t, initialPolicyUsed, "initial policy should be used")
				require.False(t, afterPolicyUsed, "after policy should not be used if condition never true")
			}
		})
	}
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

// TestPartitionedNetwork verifies PartitionedNetwork partition correctness.
func TestPartitionedNetwork(t *testing.T) {
	t.Run("consistent_within_partition", func(t *testing.T) {
		// A single long-lived partition assigns each node pair exactly once.
		// All 50 messages between nodes 1 and 2 must either all pass or all be dropped.
		rng := rand.New(rand.NewPCG(42, 0))
		policy := dstsim.NewPartitionedNetwork(
			&dstsim.FastNetwork[int, int]{},
			1.0,  // start immediately
			1000, // lasts longer than the test
			rng,
		)
		delivered := 0
		for tick := range int64(50) {
			ok, delay, _ := policy.ScheduleDelivery(dstsim.DeliveryArgs[int, int]{CurrentTick: tick, FromNode: 1, Target: 2, Indicator: 0})
			if ok {
				require.GreaterOrEqual(t, delay, int64(1))
				delivered++
			}
		}
		require.True(t, delivered == 0 || delivered == 50,
			"group assignment must be stable for the duration of one partition")
	})

	t.Run("lazy_assignment_is_consistent", func(t *testing.T) {
		// Nodes that first appear mid-partition get a lazily assigned group that
		// stays stable for the rest of the partition.
		rng := rand.New(rand.NewPCG(99, 0))
		policy := dstsim.NewPartitionedNetwork(&dstsim.FastNetwork[int, int]{}, 1.0, 1000, rng)

		ok1, _, _ := policy.ScheduleDelivery(dstsim.DeliveryArgs[int, int]{CurrentTick: 5, FromNode: 10, Target: 20, Indicator: 0}) // both nodes new to this partition
		ok2, _, _ := policy.ScheduleDelivery(dstsim.DeliveryArgs[int, int]{CurrentTick: 5, FromNode: 10, Target: 20, Indicator: 0}) // same tick, same result expected
		ok3, _, _ := policy.ScheduleDelivery(dstsim.DeliveryArgs[int, int]{CurrentTick: 6, FromNode: 10, Target: 20, Indicator: 0}) // later tick, same partition
		require.Equal(t, ok1, ok2, "group assignment must be stable within the same tick")
		require.Equal(t, ok1, ok3, "group assignment must be stable across ticks in the same partition")
	})

	t.Run("new_partition_reshuffles_groups", func(t *testing.T) {
		// With 1-tick partitions, each tick has a fresh independent group assignment.
		// Over 200 ticks roughly half should pass and half should be dropped.
		rng := rand.New(rand.NewPCG(7, 0))
		policy := dstsim.NewPartitionedNetwork(&dstsim.FastNetwork[int, int]{}, 1.0, 1, rng)
		passed, dropped := 0, 0
		for tick := range int64(200) {
			ok, _, _ := policy.ScheduleDelivery(dstsim.DeliveryArgs[int, int]{CurrentTick: tick, FromNode: 1, Target: 2, Indicator: 0})
			if ok {
				passed++
			} else {
				dropped++
			}
		}
		require.Greater(t, passed, 0, "some ticks should have nodes in the same group")
		require.Greater(t, dropped, 0, "some ticks should have nodes in different groups")
	})

	t.Run("no_partition_when_rate_zero", func(t *testing.T) {
		rng := rand.New(rand.NewPCG(1, 0))
		policy := dstsim.NewPartitionedNetwork(&dstsim.FastNetwork[int, int]{}, 0.0, 100, rng)
		for tick := range int64(50) {
			ok, delay, _ := policy.ScheduleDelivery(dstsim.DeliveryArgs[int, int]{CurrentTick: tick, FromNode: 1, Target: 2, Indicator: 0})
			require.True(t, ok)
			require.Equal(t, int64(1), delay)
		}
	})
}

// TestUnreliableNetworkPartitions verifies that UnreliableNetwork's built-in partition
// settings behave correctly.
func TestUnreliableNetworkPartitions(t *testing.T) {
	t.Run("consistent_within_partition", func(t *testing.T) {
		policy := &dstsim.UnreliableNetwork[int, int]{
			MaxDelay:             1,
			DropRate:             0.0,
			PartitionRate:        1.0,
			MaxPartitionDuration: 1000,
			Rng:                  rand.New(rand.NewPCG(42, 0)),
		}
		delivered := 0
		for tick := range int64(50) {
			ok, delay, _ := policy.ScheduleDelivery(dstsim.DeliveryArgs[int, int]{CurrentTick: tick, FromNode: 1, Target: 2, Indicator: 0})
			if ok {
				require.GreaterOrEqual(t, delay, int64(1))
				delivered++
			}
		}
		require.True(t, delivered == 0 || delivered == 50,
			"group assignment must be stable for the duration of one partition")
	})

	t.Run("lazy_assignment_is_consistent", func(t *testing.T) {
		policy := &dstsim.UnreliableNetwork[int, int]{
			MaxDelay:             1,
			DropRate:             0.0,
			PartitionRate:        1.0,
			MaxPartitionDuration: 1000,
			Rng:                  rand.New(rand.NewPCG(99, 0)),
		}
		ok1, _, _ := policy.ScheduleDelivery(dstsim.DeliveryArgs[int, int]{CurrentTick: 5, FromNode: 10, Target: 20, Indicator: 0})
		ok2, _, _ := policy.ScheduleDelivery(dstsim.DeliveryArgs[int, int]{CurrentTick: 5, FromNode: 10, Target: 20, Indicator: 0})
		ok3, _, _ := policy.ScheduleDelivery(dstsim.DeliveryArgs[int, int]{CurrentTick: 6, FromNode: 10, Target: 20, Indicator: 0})
		require.Equal(t, ok1, ok2, "group assignment must be stable within the same tick")
		require.Equal(t, ok1, ok3, "group assignment must be stable across ticks in the same partition")
	})

	t.Run("new_partition_reshuffles_groups", func(t *testing.T) {
		policy := &dstsim.UnreliableNetwork[int, int]{
			MaxDelay:             1,
			DropRate:             0.0,
			PartitionRate:        1.0,
			MaxPartitionDuration: 1,
			Rng:                  rand.New(rand.NewPCG(7, 0)),
		}
		passed, dropped := 0, 0
		for tick := range int64(200) {
			ok, _, _ := policy.ScheduleDelivery(dstsim.DeliveryArgs[int, int]{CurrentTick: tick, FromNode: 1, Target: 2, Indicator: 0})
			if ok {
				passed++
			} else {
				dropped++
			}
		}
		require.Greater(t, passed, 0, "some ticks should have nodes in the same group")
		require.Greater(t, dropped, 0, "some ticks should have nodes in different groups")
	})

	t.Run("no_partition_when_rate_zero", func(t *testing.T) {
		policy := &dstsim.UnreliableNetwork[int, int]{
			MaxDelay:             1,
			DropRate:             0.0,
			PartitionRate:        0.0,
			MaxPartitionDuration: 100,
			Rng:                  rand.New(rand.NewPCG(1, 0)),
		}
		for tick := range int64(50) {
			ok, delay, _ := policy.ScheduleDelivery(dstsim.DeliveryArgs[int, int]{CurrentTick: tick, FromNode: 1, Target: 2, Indicator: 0})
			require.True(t, ok)
			require.Equal(t, int64(1), delay)
		}
	})
}

// TestMinimumDelayEnforcement tests that simulator returns error if policy returns delay < 1
func TestMinimumDelayEnforcement(t *testing.T) {
	sim := dstsim.NewSimulator[int, string, int](dstsim.SimulatorOptions{Seed: 1})

	// Set invalid delivery policy
	sim.SetDeliveryPolicy(&testInvalidDelayPolicy[int, int]{})

	node := &AlwaysGeneratingNode{id: 1}
	sim.RegisterNode(node)
	sim.SetRequestHandler(&TickRequestHandler{})

	// This should return an error because the policy returns delay = 0
	err := sim.RunFor(1)
	require.Error(t, err, "should return error for invalid delay")
	require.Contains(t, err.Error(), "must be >= 1", "error should mention minimum delay requirement")
}
