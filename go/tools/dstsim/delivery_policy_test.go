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

// TestUnreliableNetwork tests that ChaosDeliveryManager drops and delays messages
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

			sim.SetDeliveryManager(&dstsim.ChaosDeliveryManager[int, int]{
				Chaos: dstsim.ChaosParams{
					MaxDelay: tt.maxDelay,
					DropRate: tt.dropRate,
					Rng:      rand.New(rand.NewPCG(uint64(tt.seed), uint64(tt.seed))),
				},
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

// TestUntilDeliveryManager tests that UntilDeliveryManager switches permanently when condition becomes true
func TestUntilDeliveryManager(t *testing.T) {
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
			expectSwitch: false, // Condition becomes true exactly when sim ends, so AfterManager never used
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

			// Track whether each manager was used
			initialManagerUsed := false
			afterManagerUsed := false

			initialManager := &testTrackingManager[int, int]{usedPtr: &initialManagerUsed}
			afterManager := &testTrackingManager[int, int]{usedPtr: &afterManagerUsed}

			untilManager := &dstsim.UntilDeliveryManager[int, string, int]{
				UntilCondition: dstsim.TickCondition[int, string, int](tt.switchAtTick),
				InitialManager: initialManager,
				AfterManager:   afterManager,
				Sim:            sim,
			}

			sim.SetDeliveryManager(untilManager)

			node := &AlwaysGeneratingNode{id: 1}
			sim.RegisterNode(node)
			sim.SetRequestHandler(&TickRequestHandler{})

			err := sim.RunFor(tt.runTicks)
			require.NoError(t, err)

			// Verify manager usage
			if tt.expectSwitch {
				require.True(t, initialManagerUsed, "initial manager should be used before switch")
				require.True(t, afterManagerUsed, "after manager should be used after switch")
			} else {
				require.True(t, initialManagerUsed, "initial manager should be used")
				require.False(t, afterManagerUsed, "after manager should not be used if condition never true")
			}
		})
	}
}

// TestSequenceDeliveryManager tests that SequenceDeliveryManager advances stages correctly
func TestSequenceDeliveryManager(t *testing.T) {
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

			// Use TickRequestHandler to create message flow
			sim.SetRequestHandler(&TickRequestHandler{})

			// Create a 3-stage sequence: stage0 -> stage1 -> stage2
			seq := dstsim.NewSequenceDeliveryManager[int, string, int](
				sim,
				&dstsim.ChaosDeliveryManager[int, int]{},
				"stage0",
			)

			stage1Active := seq.AppendStage(
				&dstsim.ChaosDeliveryManager[int, int]{},
				dstsim.TickCondition[int, string, int](tt.stage1Ticks),
				"stage1",
			)

			stage2Active := seq.AppendStage(
				&dstsim.ChaosDeliveryManager[int, int]{},
				dstsim.TickCondition[int, string, int](tt.stage2Ticks),
				"stage2",
			)

			sim.SetDeliveryManager(seq)

			if tt.expectStage1 {
				sim.Sometimes(stage1Active)
			}
			if tt.expectStage2 {
				sim.Sometimes(stage2Active)
			}

			err := sim.RunFor(tt.runTicks)
			require.NoError(t, err, "sequence delivery manager assertions should be satisfied")
		})
	}
}

// TestChaosDeliveryManagerPartitions verifies ChaosDeliveryManager partition correctness.
// Partition groups are assigned at Deliver time; messages in-flight when a partition starts
// may be dropped if they cross partition boundaries.
func TestChaosDeliveryManagerPartitions(t *testing.T) {
	t.Run("consistent_within_partition", func(t *testing.T) {
		// A single long-lived partition assigns each node pair exactly once.
		// All messages between nodes 1 and 2 must either all pass or all be dropped.
		rng := rand.New(rand.NewPCG(42, 0))
		manager := &dstsim.ChaosDeliveryManager[int, int]{
			Chaos:                dstsim.ChaosParams{Rng: rng},
			PartitionRate:        1.0,
			MaxPartitionDuration: 10000, // much longer than the test
		}
		allNodes := []int{1, 2}
		delivered, dropped := 0, 0
		for tick := range int64(50) {
			manager.Enqueue(tick, 1, 2, 0)
			pds, drops, _ := manager.Deliver(tick, allNodes)
			delivered += len(pds)
			dropped += len(drops)
		}
		// 49 messages are checked (tick-49 message still in flight).
		// Within a single partition, group assignment is stable: either all pass or all drop.
		require.True(t, delivered == 0 || dropped == 0,
			"group assignment must be stable for the duration of one partition: got %d delivered, %d dropped", delivered, dropped)
	})

	t.Run("lazy_assignment_is_consistent", func(t *testing.T) {
		// Nodes that first appear mid-partition get a lazily assigned group that
		// stays stable for the rest of the partition.
		rng := rand.New(rand.NewPCG(99, 0))
		manager := &dstsim.ChaosDeliveryManager[int, int]{
			Chaos:                dstsim.ChaosParams{Rng: rng},
			PartitionRate:        1.0,
			MaxPartitionDuration: 10000,
		}
		// Start partition without eagerly assigning groups (no allNodes provided).
		manager.Deliver(0, nil)

		// Enqueue two messages at the same tick and one at a later tick;
		// nodes 10 and 20 are new to this partition.
		manager.Enqueue(5, 10, 20, 0)
		manager.Enqueue(5, 10, 20, 0)
		manager.Enqueue(6, 10, 20, 0)

		pds1, drops1, _ := manager.Deliver(6, nil) // delivers tick-5 messages (deliverAt=6)
		pds2, drops2, _ := manager.Deliver(7, nil) // delivers tick-6 message (deliverAt=7)

		// Both tick-5 messages must have identical fate (same group assignment for the pair).
		require.True(t, len(pds1) == 0 || len(pds1) == 2,
			"both messages from same tick must have consistent fate: got %d delivered, %d dropped",
			len(pds1), len(drops1))

		// Tick-6 message must have the same fate as tick-5 messages (same partition, same groups).
		if len(pds1) > 0 {
			require.Equal(t, 1, len(pds2), "tick-6 message should be delivered (same partition group)")
			require.Empty(t, drops2)
		} else {
			require.Equal(t, 1, len(drops2), "tick-6 message should be dropped (same partition group)")
			require.Empty(t, pds2)
		}
	})

	t.Run("new_partition_reshuffles_groups", func(t *testing.T) {
		// With 1-tick partitions, each tick has a fresh independent group assignment.
		// Over 200 ticks roughly half should pass and half should be dropped.
		rng := rand.New(rand.NewPCG(7, 0))
		manager := &dstsim.ChaosDeliveryManager[int, int]{
			Chaos:                dstsim.ChaosParams{Rng: rng},
			PartitionRate:        1.0,
			MaxPartitionDuration: 1,
		}
		allNodes := []int{1, 2}
		passed, dropped := 0, 0
		for tick := range int64(200) {
			manager.Enqueue(tick, 1, 2, 0)
			pds, drops, _ := manager.Deliver(tick, allNodes)
			passed += len(pds)
			dropped += len(drops)
		}
		require.Greater(t, passed, 0, "some ticks should have nodes in the same group")
		require.Greater(t, dropped, 0, "some ticks should have nodes in different groups")
	})

	t.Run("no_partition_when_rate_zero", func(t *testing.T) {
		manager := &dstsim.ChaosDeliveryManager[int, int]{
			PartitionRate:        0.0,
			MaxPartitionDuration: 100,
		}
		allNodes := []int{1, 2}
		passed := 0
		for tick := range int64(50) {
			manager.Enqueue(tick, 1, 2, 0)
			pds, drops, _ := manager.Deliver(tick, allNodes)
			passed += len(pds)
			require.Empty(t, drops)
		}
		// 49 messages delivered (tick-49's message has deliverAt=50, still in flight)
		require.Equal(t, 49, passed)
	})
}
