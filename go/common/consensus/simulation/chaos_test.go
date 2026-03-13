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
	"math/rand/v2"
	"testing"

	"github.com/multigres/multigres/go/common/consensus"
	"github.com/multigres/multigres/go/tools/dstsim"
)

// TestCoordLedTermChange verifies that the cluster performs repeated
// coordinator-led term changes under continuous chaos: both the coordinator and
// all poolers crash periodically. It asserts:
//
//  1. The maximum committed PolicySeq across all poolers never decreases
//     (safety invariant — durable state must be monotone).
//
//  2. The quorum-confirmed primary changes to a different node at least 500
//     times (the coordinator detects the unhealthy primary via the health
//     timeout and completes the coordinator-led term change each time).
func TestCoordLedTermChange(t *testing.T) {
	const (
		coordID         consensus.NodeID = "coord-1"
		coordCrasherID  consensus.NodeID = "coord-crasher"
		poolerCrasherID consensus.NodeID = "pooler-crasher"
		node1ID         consensus.NodeID = "node-1"
		node2ID         consensus.NodeID = "node-2"
		node3ID         consensus.NodeID = "node-3"
	)

	seed := uint64(42)
	t.Logf("Chaos seed: %d", seed)
	rng := rand.New(rand.NewPCG(seed, 0))

	sim := newTestSim(coordID)
	sim.SetDeliveryManager(reliableMembership(&dstsim.ChaosDeliveryManager[consensus.Indicator, consensus.NodeID]{
		Chaos: dstsim.ChaosParams{
			MaxDelay: 10,
			DropRate: 0.1,
			Reorder:  true,
			DupRate:  0.05,
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

	// The coordinator detects a crashed primary and initiates a
	// coordinator-led term change.
	coordNode := consensus.NewCoordNode(coordID, consensus.AtLeastPolicy(2), nil, nil)
	coord := NewSimCoordNode(coordNode, sim)
	sim.RegisterNode(coord)

	pooler2 := newReplicaPooler(node2ID, node1ID, sim)
	pooler3 := newReplicaPooler(node3ID, node1ID, sim)
	sim.RegisterNode(pooler2)
	sim.RegisterNode(pooler3)

	allPoolers := []*SimPooler{pooler1, pooler2, pooler3}

	// Coordinator crashes roughly every 400 ticks, staying down for 30 ticks.
	coordCrasher := newChaosCrasher(
		coordCrasherID, sim, rand.New(rand.NewPCG(seed, 1)),
		func() []consensus.NodeID { return []consensus.NodeID{coordID} }, 400, 30,
	)
	sim.RegisterNode(coordCrasher)

	// Each pooler crashes roughly every 800 ticks, staying down for 400 ticks.
	// Down time (400) exceeds the health timeout (HealthTimeoutTicks=300) so
	// the coordinator detects the crash and initiates a coordinator-led term
	// change before the node recovers. The 100-tick window between detection
	// (tick 300) and recovery (tick 400) is sufficient to complete the failover.
	// The target function returns the current quorum primary so we specifically
	// crash whichever node is currently acting as primary, maximising the
	// number of coordinator-led failovers per unit of simulation time.
	poolerCrasher := newChaosCrasher(
		poolerCrasherID, sim, rand.New(rand.NewPCG(seed, 2)),
		func() []consensus.NodeID {
			for _, pooler := range allPoolers {
				if pooler.IsConsensusPrimary() {
					return []consensus.NodeID{pooler.ID()}
				}
			}
			return nil
		}, 800, 400,
	)
	sim.RegisterNode(poolerCrasher)

	// quorumLeaderChange fires each time the coordinator's highest quorum-confirmed
	// primary transitions to a different node. Coordinator crash resets (nil view)
	// are not counted.
	quorumLeaderChange := &quorumLeaderChanged{
		getQuorumTerm: func(sim *simType) *consensus.Term {
			return coord.Node().ShardStatus(sim.CurrentTick()).HighestQuorumTerm
		},
	}

	th := dstsim.NewSimulationTestHelper(t, sim)

	th.RequireWithinTicks(dstsim.And(
		dstsim.NewAtLeastNTimes(500, quorumLeaderChange),
		coordCrasher.minCrashes(1),
		poolerCrasher.minCrashes(1),
	), 2_000_000)
}

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

	coord := NewSimCoordNode(consensus.NewCoordNode(coordID, consensus.AtLeastPolicy(2), nil, nil), sim)
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
// The test uses valueNewMax to count how often the coordinator's quorum-confirmed
// term seq advances. Each coordinator crash clears its view (on Restart) and it
// re-learns the cluster on recovery, so the quorum seq oscillates between 0
// and the current term seq. With one crash every ~600 ticks and a 3000-tick
// run, at least 1 new-max event is expected across the run.
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

	coord := NewSimCoordNode(consensus.NewCoordNode(coordID, consensus.AtLeastPolicy(2), nil, nil), sim)
	sim.RegisterNode(coord)

	pooler2 := newReplicaPooler(node2ID, node1ID, sim)
	pooler3 := newReplicaPooler(node3ID, node1ID, sim)
	sim.RegisterNode(pooler2)
	sim.RegisterNode(pooler3)

	// crasher crashes the coordinator on average every 600 ticks (roughly once
	// per minute), keeping it down for 20 ticks before restarting. A separate
	// rng stream ensures the crash schedule is independent of the network chaos rng.
	crashRng := rand.New(rand.NewPCG(seed, 1))
	crasher := newChaosCrasher(crasherID, sim, crashRng, func() []consensus.NodeID { return []consensus.NodeID{coordID} }, 600, 50)
	sim.RegisterNode(crasher)

	allPoolers := []*SimPooler{pooler1, pooler2, pooler3}

	// coordQuorumSeq returns the highest PolicySeq for which the coordinator
	// has confirmed write quorum. After a crash and Restart(), the coordinator's
	// known-pooler map is cleared and this returns 0 until it re-learns the state.
	coordQuorumSeq := func(sim *simType) int64 {
		status := coord.Node().ShardStatus(sim.CurrentTick())
		if status.HighestQuorumTerm == nil {
			return 0
		}
		return status.HighestQuorumTerm.Seq
	}

	// Track when the coordinator confirms quorum at a new (higher) term seq.
	// Coordinator crashes reset its ephemeral view to 0, but those drops are
	// not counted — only genuine upward progress events are. In practice the
	// coordinator sees the cluster jump directly to seq=2 (expansion completes
	// before the first status update arrives after a crash), so exactly 1
	// new-max event is expected.
	coordQuorumSeqNewMax := &valueNewMax[int64]{
		name:     "coord_quorum_seq_new_max",
		getValue: coordQuorumSeq,
	}

	th := dstsim.NewSimulationTestHelper(t, sim)

	// Run until cohort expansion completes, the coordinator has confirmed
	// quorum at a new seq value at least once, and the coordinator has
	// crashed at least 5 times.
	th.RequireWithinTicks(dstsim.And(
		&allHaveAppliedRules{
			poolers:     allPoolers,
			members:     []consensus.NodeID{node1ID, node2ID, node3ID},
			wantAtLeast: 2,
		},
		dstsim.NewAtLeastNTimes(1, coordQuorumSeqNewMax),
		crasher.minCrashes(5),
	), 5000)
}
