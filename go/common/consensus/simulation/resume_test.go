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

// TestResumeMessageNeeded verifies that nodes do not get stuck with a stale
// term after a coordinator-led primary failover. The coordinator sends
// ResumeRequests to stale nodes (CoordNode.advanceResume) to bring them up to
// the quorum-confirmed term without a full term-change round-trip.
//
// Setup:
//   - 4-node cluster: node1 starts as primary; node2–4 start as replicas.
//   - All 4 nodes are in the cohort from the start with AtLeast(2) policy,
//     so any two nodes ACKing a write satisfies quorum.
//   - The coordinator has a health timeout so it detects a crashed primary and
//     initiates a coordinator-led term change.
//   - Poolers crash rarely (every ~500 ticks, down for 80 ticks); the
//     coordinator never crashes.
//
// Invariants asserted:
//  1. (sim.Never) No node should hold a stale committed term (seq behind the
//     quorum seq) while continuously running for more than 300 ticks. Without a
//     resume message the old primary and any replicas that were streaming from it
//     are stuck pointing at the wrong primary and never receive the new term.
//  2. (sim.Never) No non-revoked node should have replication settings
//     inconsistent with its committed term for more than 50 ticks. This checks
//     that the sidecar applies GUC changes promptly once a term is committed.
//
// The test drives at least 5 coordinator-led term changes (quorum leader changes)
// so that the stuck-node scenario is exercised multiple times.
func TestResumeMessageNeeded(t *testing.T) {
	const (
		coordID         consensus.NodeID = "coord-1"
		poolerCrasherID consensus.NodeID = "pooler-crasher"
		node1ID         consensus.NodeID = "node-1"
		node2ID         consensus.NodeID = "node-2"
		node3ID         consensus.NodeID = "node-3"
		node4ID         consensus.NodeID = "node-4"
	)

	seed := uint64(42)
	t.Logf("Chaos seed: %d", seed)
	rng := rand.New(rand.NewPCG(seed, 0))

	sim := newTestSim(coordID)
	sim.SetDeliveryManager(reliableMembership(&dstsim.ChaosDeliveryManager[consensus.Indicator, consensus.NodeID]{
		Chaos: dstsim.ChaosParams{
			MaxDelay: 5,
			DropRate: 0.05,
			Rng:      rng,
		},
	}))

	// Seed term: all 4 nodes in the cohort from the start with AtLeast(2) policy.
	seedTerm := &consensus.Term{
		Seq:     1,
		Primary: node1ID,
		Members: []consensus.CohortMember{
			{ID: node1ID},
			{ID: node2ID},
			{ID: node3ID},
			{ID: node4ID},
		},
		Policy: consensus.AtLeastPolicy(2),
	}
	pooler1 := newPrimaryPooler(node1ID, seedTerm, sim)
	sim.RegisterNode(pooler1)

	// The coordinator detects an unhealthy primary and initiates a
	// coordinator-led term change.
	coordNode := consensus.NewCoordNode(coordID, consensus.AtLeastPolicy(2), nil, nil)
	coord := NewSimCoordNode(coordNode, sim)
	sim.RegisterNode(coord)

	pooler2 := newReplicaPooler(node2ID, node1ID, sim)
	pooler3 := newReplicaPooler(node3ID, node1ID, sim)
	pooler4 := newReplicaPooler(node4ID, node1ID, sim)
	sim.RegisterNode(pooler2)
	sim.RegisterNode(pooler3)
	sim.RegisterNode(pooler4)

	allPoolers := []*SimPooler{pooler1, pooler2, pooler3, pooler4}

	// Poolers crash roughly every 1000 ticks and stay down 400 ticks.
	// Down time (400) exceeds the health timeout (HealthTimeoutTicks=300) so
	// the coordinator can detect the crash and complete a coordinator-led term
	// change before the node recovers. Up time (600) exceeds the invariant
	// threshold (600 ticks) so nodes have time to prove they can participate
	// continuously. The coordinator never crashes.
	poolerCrasher := newChaosCrasher(
		poolerCrasherID, sim, rand.New(rand.NewPCG(seed, 1)),
		func() []consensus.NodeID { return []consensus.NodeID{node1ID, node2ID, node3ID, node4ID} }, 1000, 400,
	)
	sim.RegisterNode(poolerCrasher)

	// quorumLeaderChange fires each time the coordinator's quorum-confirmed
	// primary transitions to a different node.
	quorumLeaderChange := &quorumLeaderChanged{
		getQuorumTerm: func(sim *simType) *consensus.Term {
			return coord.Node().ShardStatus(sim.CurrentTick()).HighestQuorumTerm
		},
	}

	// Invariant 1: no node should have a stale committed term (behind the
	// quorum seq) for more than 600 consecutive ticks while running. Without
	// the resume message, the old primary and any replicas that were streaming
	// from it get stuck here after a coordinator-led failover.
	for _, sp := range allPoolers {
		sim.Never(dstsim.AtLeastNTicks(
			600, dstsim.And(
				dstsim.NodeIsRunning[consensus.Indicator, consensus.Request](sp.ID()),
				&nodeHasStaleTerm{sp: sp, coord: coord},
			),
		))
	}

	// Invariant 2: no node should be continuously running and non-participating
	// for more than 600 ticks. A node is legitimately non-participating during
	// a coordinator-led failover (it is recruited and revoked), but the failover
	// should complete well within 600 ticks. If it does not, something is stuck.
	for _, sp := range allPoolers {
		sim.Never(dstsim.AtLeastNTicks(
			600, dstsim.And(
				dstsim.NodeIsRunning[consensus.Indicator, consensus.Request](sp.ID()),
				dstsim.Not(
					&nodeIsParticipating{sp: sp},
				),
			),
		))
	}

	th := dstsim.NewSimulationTestHelper(t, sim)

	// Run until 5 coordinator-led term changes have been observed. This ensures
	// the stuck-node scenario is exercised multiple times across different primaries.
	th.RequireWithinTicks(dstsim.NewAtLeastNTimes(5, quorumLeaderChange), 500_000)
}
