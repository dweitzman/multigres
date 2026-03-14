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
	"testing"

	"github.com/multigres/multigres/go/common/consensus"
	"github.com/multigres/multigres/go/tools/dstsim"
)

// TestGracefulPrimaryShutdown verifies that a TerminateIndicator (SIGTERM proxy)
// triggers a coordinator-led leader change proactively, well before
// HealthTimeoutTicks would elapse.
//
// Setup: 3-node cluster (node1 primary, node2+node3 replicas) at AtLeast(2).
// Once the coordinator confirms quorum at the seed term, a TerminateIndicator
// is delivered to the primary (node1). This sets ShutdownIntent=true on its
// next status broadcast, causing the coordinator to detect the primary as
// unhealthy and initiate a coordinator-led failover.
//
// The tick budget is well below HealthTimeoutTicks (300), so this test would
// fail if the ShutdownIntent check were removed from isNodeHealthy: without it,
// the coordinator would not detect the primary as unhealthy (it keeps sending
// heartbeats since postgres is still running), so no failover would ever fire.
func TestGracefulPrimaryShutdown(t *testing.T) {
	const (
		coordID consensus.NodeID = "coord-1"
		node1ID consensus.NodeID = "node-1" // primary — will receive TerminateIndicator
		node2ID consensus.NodeID = "node-2" // replica
		node3ID consensus.NodeID = "node-3" // replica
	)

	sim := newTestSim(coordID)

	seedTerm := &consensus.Term{
		Seq:     2,
		Primary: node1ID,
		Members: []consensus.CohortMember{
			{ID: node1ID},
			{ID: node2ID},
			{ID: node3ID},
		},
		Policy: consensus.AtLeastPolicy(2),
	}

	pooler1 := newPrimaryPooler(node1ID, seedTerm, sim)
	pooler2 := newReplicaPooler(node2ID, node1ID, sim)
	pooler3 := newReplicaPooler(node3ID, node1ID, sim)
	coordNode := consensus.NewCoordNode(coordID, consensus.AtLeastPolicy(2), nil, nil)
	coord := NewSimCoordNode(coordNode, sim)

	// onceTerminator delivers a TerminateIndicator to the primary exactly once,
	// after the coordinator has confirmed quorum at the seed term, modelling
	// SIGTERM arriving at the primary process.
	terminator := &onceTerminator{
		id:      "terminator",
		primary: pooler1,
		coord:   coord,
		minSeq:  seedTerm.Seq,
	}

	sim.SetDeliveryManager(reliableMembership(
		&dstsim.ChaosDeliveryManager[consensus.Indicator, consensus.NodeID]{},
	))

	sim.RegisterNode(pooler1)
	sim.RegisterNode(pooler2)
	sim.RegisterNode(pooler3)
	sim.RegisterNode(coord)
	sim.RegisterNode(terminator)

	// leaderChanged fires each time the quorum-confirmed primary transitions
	// to a different non-empty NodeID (i.e. a genuine seq-increasing failover).
	leaderChanged := &quorumLeaderChanged{
		getQuorumTerm: func(sim *simType) *consensus.Term {
			return coord.Node().ShardStatus(sim.CurrentTick()).HighestQuorumTerm
		},
	}

	// The budget is set well below HealthTimeoutTicks (300). If ShutdownIntent
	// were not checked in isNodeHealthy, the coordinator would never detect the
	// primary as unhealthy (the primary keeps heartbeating since postgres is
	// still running), so no leader change would occur within this window.
	th := dstsim.NewSimulationTestHelper(t, sim)
	th.RequireWithinTicks(
		dstsim.NewAtLeastNTimes(1, leaderChanged),
		consensus.HealthTimeoutTicks/2,
	)
}

// onceTerminator is a simulation node that delivers a TerminateIndicator to
// its target primary exactly once, after the coordinator's quorum term reaches
// minSeq. It models a k8s pod receiving SIGTERM after the cluster is healthy.
type onceTerminator struct {
	id      consensus.NodeID
	primary *SimPooler
	coord   *SimCoordNode
	minSeq  int64
	done    bool
}

func (t *onceTerminator) ID() consensus.NodeID { return t.id }

func (t *onceTerminator) Step(tick int64, _ []consensus.Indicator) []consensus.Request {
	if t.done {
		return nil
	}
	status := t.coord.Node().ShardStatus(tick)
	if status.HighestQuorumTerm != nil && status.HighestQuorumTerm.Seq >= t.minSeq {
		t.primary.EnqueueIndicator(consensus.TerminateIndicator{})
		t.done = true
	}
	return nil
}
