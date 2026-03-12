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
	"fmt"
	"slices"
	"testing"

	"github.com/multigres/multigres/go/common/consensus"
	"github.com/multigres/multigres/go/tools/dstsim"
)

// groupPartition is a delivery manager that drops indicators between nodes in
// different partition groups. When no groups are configured (groups == nil),
// all messages pass through to inner unchanged. Call setGroups to activate the
// partition.
//
// Nodes not assigned to any named group are placed in a shared implicit "other"
// group: they can communicate with each other but not with any named-group node.
type groupPartition struct {
	inner  dstsim.IndicatorDeliveryManager[consensus.Indicator, consensus.NodeID]
	groups [][]consensus.NodeID
}

func (m *groupPartition) setGroups(groups ...[]consensus.NodeID) {
	m.groups = groups
}

func (m *groupPartition) groupOf(id consensus.NodeID) int {
	for i, g := range m.groups {
		if slices.Contains(g, id) {
			return i
		}
	}
	return len(m.groups) // shared "other" group for unassigned nodes
}

func (m *groupPartition) Enqueue(tick int64, from, to consensus.NodeID, ind consensus.Indicator) (dropped bool, events []string) {
	if m.groups != nil && m.groupOf(from) != m.groupOf(to) {
		return true, nil
	}
	return m.inner.Enqueue(tick, from, to, ind)
}

func (m *groupPartition) Deliver(tick int64, allNodes []consensus.NodeID) ([]dstsim.PendingDelivery[consensus.Indicator, consensus.NodeID], []dstsim.PendingDelivery[consensus.Indicator, consensus.NodeID], []string) {
	return m.inner.Deliver(tick, allNodes)
}

func (m *groupPartition) Drain() []dstsim.PendingDelivery[consensus.Indicator, consensus.NodeID] {
	return m.inner.Drain()
}

// nodeRevoked is a simulation condition that is true when the given pooler
// has been revoked (its WAL receive has been disabled by the coordinator).
type nodeRevoked struct {
	pooler *SimPooler
}

func (c *nodeRevoked) Name() string         { return fmt.Sprintf("node_revoked(%v)", c.pooler.ID()) }
func (c *nodeRevoked) Eval(_ *simType) bool { return c.pooler.isRevoked() }
func (c *nodeRevoked) Describe(_ *simType) string {
	return fmt.Sprintf("node %v is revoked (WAL receive disabled)", c.pooler.ID())
}

// coordHasQuorumAtSeq is true every tick the coordinator has confirmed write
// quorum for a term with Seq >= minSeq.
type coordHasQuorumAtSeq struct {
	coord  *SimCoordNode
	minSeq int64
}

func (c *coordHasQuorumAtSeq) Name() string {
	return fmt.Sprintf("coord_quorum_seq_ge_%d", c.minSeq)
}

func (c *coordHasQuorumAtSeq) Eval(sim *simType) bool {
	status := c.coord.Node().ShardStatus(sim.CurrentTick())
	return status.HighestQuorumTerm != nil && status.HighestQuorumTerm.Seq >= c.minSeq
}

func (c *coordHasQuorumAtSeq) Describe(sim *simType) string {
	status := c.coord.Node().ShardStatus(sim.CurrentTick())
	if status.HighestQuorumTerm == nil {
		return fmt.Sprintf("coordinator has not confirmed quorum yet (need seq >= %d)", c.minSeq)
	}
	return fmt.Sprintf("coordinator quorum at seq=%d, need >= %d", status.HighestQuorumTerm.Seq, c.minSeq)
}

// TestCoordDoesNotRecruitWhenFailoverIsInfeasible verifies that the coordinator
// does not start recruitment when the number of visible healthy nodes is
// insufficient to satisfy the post-failover durability policy.
//
// Setup: 3-node cluster (node1 primary, node2 and node3 replicas) with
// AtLeast(2) policy.
//
// Scenario: once the cluster establishes quorum, a network partition splits the
// cluster into two groups: {node1, node2} and {node3, coordinator}. The
// coordinator can only see node3 — one node short of AtLeast(2). Meanwhile
// node1 and node2 continue to maintain write quorum directly via WAL
// replication, which is unaffected by the coordinator's view.
//
// Before the fix: the coordinator recruits node3 and gets stuck (without node1
// or node2 it cannot satisfy the quorum requirement), leaving node3 revoked
// from quorum and the shard worse off than before.
//
// After the fix: CanAttemptFailover detects that IsAchievable([node3]) is false
// for AtLeast(2) and the coordinator refrains from recruiting.
func TestCoordDoesNotRecruitWhenFailoverIsInfeasible(t *testing.T) {
	const (
		coordID consensus.NodeID = "coord-1"
		node1ID consensus.NodeID = "node-1" // primary — isolated from coordinator
		node2ID consensus.NodeID = "node-2" // replica — isolated from coordinator
		node3ID consensus.NodeID = "node-3" // replica — in coordinator's partition group
	)

	sim := newTestSim(coordID)

	// Pre-seed a fully-expanded 3-node cluster at AtLeast(2) so the test starts
	// in the steady state we care about.
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

	// Delivery stages:
	//   Stage 1 "pre-partition": reliable delivery while the cluster converges to
	//     quorum. Ends when the coordinator has confirmed the seed term.
	//   Stage 2 "partitioned": split into {node1, node2} and {node3, coord}.
	//     The coordinator can only see node3; node1 (the primary) eventually
	//     appears stale and triggers a failover attempt.
	reliable := &dstsim.ChaosDeliveryManager[consensus.Indicator, consensus.NodeID]{}
	partMgr := &groupPartition{
		inner: &dstsim.ChaosDeliveryManager[consensus.Indicator, consensus.NodeID]{},
	}
	partMgr.setGroups(
		[]consensus.NodeID{node1ID, node2ID},
		[]consensus.NodeID{node3ID, coordID},
	)

	seq := dstsim.NewSequenceDeliveryManager(
		sim, reliable, "pre-partition",
	)
	// Wait for quorum and for 100 ticks of quorum confirmation. The 100-tick
	// delay ensures node-1's second heartbeat is received and processed by the
	// coordinator before the partition activates. Without this delay, the very
	// first status broadcast from node-1 is lost (PoolerStatusIndicator arrives
	// before PoolerDiscoveredIndicator, so it is ignored), leaving
	// lastStatusTick[node-1]=0. With LastHeardTick=0, isNodeHealthy always
	// returns true (the "never heard from" grace case), so NeedsLeaderFailover
	// never fires. The PoolerHeartbeatIntervalTicks-length wait guarantees the
	// second heartbeat arrives and is processed before the partition splits the
	// network.
	partitionActive := seq.AppendStage(
		partMgr,
		dstsim.NewAtLeastNTimes(int(consensus.PoolerHeartbeatIntervalTicks), &coordHasQuorumAtSeq{coord: coord, minSeq: seedTerm.Seq}),
		"partitioned",
	)
	sim.SetDeliveryManager(reliableMembership(seq))

	sim.RegisterNode(pooler1)
	sim.RegisterNode(pooler2)
	sim.RegisterNode(pooler3)
	sim.RegisterNode(coord)

	allPoolers := []*SimPooler{pooler1, pooler2, pooler3}

	// Safety invariant: durable committed state must never decrease. Once a
	// term reaches write quorum, that seq must never roll back on any node.
	maxCommittedSeq := func(*simType) int64 {
		var max int64
		for _, sp := range allPoolers {
			if seq := sp.Node().CommittedState().PolicySeq(); seq > max {
				max = seq
			}
		}
		return max
	}
	sim.Always(&valueNeverDecreases[int64]{
		name:     "max_committed_seq_monotone",
		getValue: maxCommittedSeq,
	})

	// Safety invariant: node3 must never be revoked. Revoking would remove node3
	// from quorum with no realistic path to re-establishing AtLeast(2) durability
	// from the coordinator's perspective (it can only see one of two required
	// nodes).
	sim.Never(&nodeRevoked{pooler: pooler3})

	// Run until two conditions are both satisfied:
	//   1. Partition active for HealthTimeoutTicks+100 ticks: ensures the
	//      coordinator has had enough time to detect node1 as stale
	//      (NeedsLeaderFailover=true) and attempt (or correctly skip) a failover.
	//   2. Quorum confirmed for 1000 ticks: ensures the cluster reached quorum
	//      and held it throughout — verifying that coordinator inaction did not
	//      degrade the cluster's durable state.
	//
	// Both safety invariants above are checked every tick throughout the run.
	quorumHeld := &coordHasQuorumAtSeq{coord: coord, minSeq: seedTerm.Seq}
	th := dstsim.NewSimulationTestHelper(t, sim)
	th.RequireWithinTicks(
		dstsim.And(
			dstsim.NewAtLeastNTimes(int(consensus.HealthTimeoutTicks)+100, partitionActive),
			dstsim.NewAtLeastNTimes(1000, quorumHeld),
		),
		consensus.PoolerHeartbeatIntervalTicks+consensus.HealthTimeoutTicks+1200,
	)
}
