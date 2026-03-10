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

package simulation

import (
	"cmp"
	"fmt"
	"math/rand/v2"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/consensus"
	"github.com/multigres/multigres/go/tools/dstsim"
	"github.com/multigres/multigres/go/tools/dstsim/sortedmaps"
)

// --- Generic value-tracking conditions ---

// valueChanged fires on any tick where getValue(sim) returns a different value
// than the previous tick. It does not fire on the very first evaluation.
// Suitable for use with dstsim.NewAtLeastNTimes to count how often a value changes.
type valueChanged[T comparable] struct {
	name     string
	getValue func(*simType) T
	last     T
	hasLast  bool
}

func (c *valueChanged[T]) Name() string { return c.name }

func (c *valueChanged[T]) Eval(sim *simType) bool {
	v := c.getValue(sim)
	changed := c.hasLast && v != c.last
	c.last = v
	c.hasLast = true
	return changed
}

func (c *valueChanged[T]) Describe(_ *simType) string {
	return fmt.Sprintf("%s: last known value=%v", c.name, c.last)
}

// valueNeverDecreases is a safety invariant: the value returned by getValue
// must be monotonically non-decreasing across ticks. Tracks the maximum ever
// seen; returns false if the current value drops below that maximum.
// Register with sim.Always to assert this holds for the entire simulation.
type valueNeverDecreases[T cmp.Ordered] struct {
	name     string
	getValue func(*simType) T
	max      T
	hasMax   bool
}

func (c *valueNeverDecreases[T]) Name() string { return c.name }

func (c *valueNeverDecreases[T]) Eval(sim *simType) bool {
	v := c.getValue(sim)
	if c.hasMax && v < c.max {
		return false // dropped below previously seen maximum
	}
	if !c.hasMax || v > c.max {
		c.max = v
		c.hasMax = true
	}
	return true
}

func (c *valueNeverDecreases[T]) Describe(sim *simType) string {
	return fmt.Sprintf("%s: max_seen=%v, current=%v", c.name, c.max, c.getValue(sim))
}

// --- Chaos crasher node ---

// chaosCrasher is a simulator node that periodically crashes and restarts its
// target nodes. avgTicksBetweenCrashes controls the expected number of ticks
// between successive crashes: on each tick a running target is crashed with
// probability 1/avgTicksBetweenCrashes. Crash counts are tracked per-target
// for post-run assertions.
type chaosCrasher struct {
	id                     consensus.NodeID
	sim                    *simType
	rng                    *rand.Rand
	targets                []consensus.NodeID
	avgTicksBetweenCrashes int64 // expected ticks between successive crashes
	downTicks              int64 // number of ticks to keep a crashed node down
	downUntil              map[consensus.NodeID]int64
	crashes                map[consensus.NodeID]int
}

func newChaosCrasher(
	id consensus.NodeID,
	sim *simType,
	rng *rand.Rand,
	targets []consensus.NodeID,
	avgTicksBetweenCrashes int64,
	downTicks int64,
) *chaosCrasher {
	return &chaosCrasher{
		id:                     id,
		sim:                    sim,
		rng:                    rng,
		targets:                targets,
		avgTicksBetweenCrashes: avgTicksBetweenCrashes,
		downTicks:              downTicks,
		downUntil:              make(map[consensus.NodeID]int64),
		crashes:                make(map[consensus.NodeID]int),
	}
}

func (c *chaosCrasher) ID() consensus.NodeID { return c.id }

func (c *chaosCrasher) Step(tick int64, _ []consensus.Indicator) []consensus.Request {
	crashProb := 1.0 / float64(c.avgTicksBetweenCrashes)
	for _, target := range c.targets {
		if until := c.downUntil[target]; until > 0 {
			if tick >= until {
				c.sim.RestartNode(target)
				c.downUntil[target] = 0
			}
		} else if c.rng.Float64() < crashProb {
			c.crashes[target]++
			c.downUntil[target] = tick + c.downTicks
			c.sim.StopNode(target)
		}
	}
	return nil
}

func (c *chaosCrasher) totalCrashes() int {
	total := 0
	for _, n := range sortedmaps.Values(c.crashes) {
		total += n
	}
	return total
}

// --- Chaos tests ---

// TestCohortExpansionUnreliableNetwork verifies that cohort expansion converges
// under unreliable network conditions (random delays and packet drops). The
// coordinator operates autonomously, adding observed replicas to the cohort.
func TestCohortExpansionUnreliableNetwork(t *testing.T) {
	const (
		coordID consensus.NodeID = "coord-1"
		node1ID consensus.NodeID = "node-1"
		node2ID consensus.NodeID = "node-2"
		node3ID consensus.NodeID = "node-3"
	)

	seed := uint64(42)
	t.Logf("Chaos seed: %d", seed)
	rng := rand.New(rand.NewPCG(seed, 0))

	sim := newTestSim(coordID)
	sim.SetDeliveryManager(reliableMembership(&dstsim.ChaosDeliveryManager[consensus.Indicator, consensus.NodeID]{
		Chaos: dstsim.ChaosParams{
			MaxDelay: 5,
			DropRate: 0.1,
			Rng:      rng,
		},
	}))

	seedTerm := &consensus.Term{
		Seq:     1,
		Primary: node1ID,
		Members: []consensus.CohortMember{{ID: node1ID}},
		Policy:  consensus.AtLeastPolicy(1),
	}
	pooler1 := newPrimaryPooler(node1ID, seedTerm, sim)
	sim.RegisterNode(pooler1)

	coord := NewSimCoordNode(consensus.NewCoordNode(coordID, consensus.AtLeastPolicy(2)), sim)
	sim.RegisterNode(coord)

	pooler2 := newReplicaPooler(node2ID, node1ID, sim)
	pooler3 := newReplicaPooler(node3ID, node1ID, sim)
	sim.RegisterNode(pooler2)
	sim.RegisterNode(pooler3)

	th := dstsim.NewSimulationTestHelper(t, sim)

	th.RequireWithinTicks(&allHaveAppliedRules{
		poolers:     []*SimPooler{pooler1, pooler2, pooler3},
		members:     []consensus.NodeID{node1ID, node2ID, node3ID},
		wantAtLeast: 2,
	}, 500)
}

// TestCohortExpansionCoordinatorCrashes verifies that cohort expansion converges
// despite repeated coordinator crashes and restarts.
//
// Two conditions are composed from generic building blocks:
//
//  1. valueNeverDecreases tracks the maximum PolicySeq committed across all
//     poolers and asserts it never decreases. This is the true durable state of
//     the cluster — persisted to storage and reloaded on crash — so it must be
//     monotonically non-decreasing regardless of coordinator state.
//
//  2. valueChanged counts how often the coordinator's quorum-confirmed term seq
//     changes. Each coordinator crash clears its view (on Restart) and it
//     re-learns the cluster on recovery, so the quorum seq oscillates between 0
//     and the current term seq. With one crash every ~600 ticks and a 3000-tick
//     run, at least 5 changes are expected across the run.
func TestCohortExpansionCoordinatorCrashes(t *testing.T) {
	const (
		coordID   consensus.NodeID = "coord-1"
		crasherID consensus.NodeID = "crasher"
		node1ID   consensus.NodeID = "node-1"
		node2ID   consensus.NodeID = "node-2"
		node3ID   consensus.NodeID = "node-3"
	)

	seed := uint64(42)
	t.Logf("Chaos seed: %d", seed)
	rng := rand.New(rand.NewPCG(seed, 0))

	sim := newTestSim(coordID)
	sim.SetDeliveryManager(reliableMembership(&dstsim.ChaosDeliveryManager[consensus.Indicator, consensus.NodeID]{
		Chaos: dstsim.ChaosParams{
			MaxDelay: 5,
			DropRate: 0.1,
			Rng:      rng,
		},
	}))

	seedTerm := &consensus.Term{
		Seq:     1,
		Primary: node1ID,
		Members: []consensus.CohortMember{{ID: node1ID}},
		Policy:  consensus.AtLeastPolicy(1),
	}
	pooler1 := newPrimaryPooler(node1ID, seedTerm, sim)
	sim.RegisterNode(pooler1)

	coord := NewSimCoordNode(consensus.NewCoordNode(coordID, consensus.AtLeastPolicy(2)), sim)
	sim.RegisterNode(coord)

	pooler2 := newReplicaPooler(node2ID, node1ID, sim)
	pooler3 := newReplicaPooler(node3ID, node1ID, sim)
	sim.RegisterNode(pooler2)
	sim.RegisterNode(pooler3)

	// crasher crashes the coordinator on average every 600 ticks (roughly once
	// per minute), keeping it down for 20 ticks before restarting. A separate
	// rng stream ensures the crash schedule is independent of the network chaos rng.
	crashRng := rand.New(rand.NewPCG(seed, 1))
	crasher := newChaosCrasher(crasherID, sim, crashRng, []consensus.NodeID{coordID}, 600, 20)
	sim.RegisterNode(crasher)

	allPoolers := []*SimPooler{pooler1, pooler2, pooler3}

	// maxCommittedSeq returns the highest PolicySeq committed across all poolers.
	// Poolers persist committed state to durable storage and reload it on restart,
	// so this value reflects true cluster durability and must never decrease.
	maxCommittedSeq := func(*simType) int64 {
		var max int64
		for _, sp := range allPoolers {
			if seq := sp.Node().CommittedState().PolicySeq(); seq > max {
				max = seq
			}
		}
		return max
	}

	// coordQuorumSeq returns the highest PolicySeq for which the coordinator
	// has confirmed write quorum. After a crash and Restart(), the coordinator's
	// known-pooler map is cleared and this returns 0 until it re-learns the state.
	coordQuorumSeq := func(sim *simType) int64 {
		view := coord.Node().ClusterView(sim.CurrentTick())
		if view.HighestQuorumTerm == nil {
			return 0
		}
		return view.HighestQuorumTerm.Seq
	}

	// Safety invariant: durable committed state must never decrease.
	sim.Always(&valueNeverDecreases[int64]{
		name:     "max_committed_seq_monotone",
		getValue: maxCommittedSeq,
	})

	// Track how often the coordinator's quorum view changes. Each crash cycle
	// (stop → Restart → re-learn) produces two changes: established-seq → 0
	// and 0 → established-seq. With one crash every ~600 ticks over a 3000-tick
	// run we expect ~5 crash cycles, yielding ~10 changes.
	coordQuorumSeqChanged := &valueChanged[int64]{
		name:     "coord_quorum_seq_changed",
		getValue: coordQuorumSeq,
	}

	th := dstsim.NewSimulationTestHelper(t, sim)

	th.RequireWithinTicks(dstsim.And(
		&allHaveAppliedRules{
			poolers:     allPoolers,
			members:     []consensus.NodeID{node1ID, node2ID, node3ID},
			wantAtLeast: 2,
		},
		dstsim.NewAtLeastNTimes(5, coordQuorumSeqChanged),
	), 3000)

	require.Greater(t, crasher.totalCrashes(), 0,
		"expected at least one coordinator crash during the test")
}
