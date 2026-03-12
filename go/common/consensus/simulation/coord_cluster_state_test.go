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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/consensus"
)

// makeTerm is a convenience function for building a Term value in tests.
func makeTerm(seq int64, primary consensus.NodeID, members []consensus.NodeID, atLeast int) *consensus.Term {
	cohort := make([]consensus.CohortMember, len(members))
	for i, id := range members {
		cohort[i] = consensus.CohortMember{ID: id}
	}
	return &consensus.Term{
		Seq:     seq,
		Primary: primary,
		Members: cohort,
		Policy:  consensus.AtLeastPolicy(atLeast),
	}
}

// statusInd builds a PoolerStatusIndicator for use in coordinator Step() calls.
func statusInd(id consensus.NodeID, role consensus.PoolerRole, primary consensus.NodeID,
	term *consensus.Term, pgStatus consensus.PostgresStatus,
) consensus.PoolerStatusIndicator {
	return consensus.PoolerStatusIndicator{
		PoolerID: id,
		State: consensus.PoolerPersistentState{
			Role:       role,
			Primary:    primary,
			CachedTerm: term,
		},
		PostgresStatus: pgStatus,
	}
}

// stepWithDiscoveryAndStatus is a helper that discovers the given pooler IDs
// and delivers the given status indicators in a single Step call.
func stepWithDiscoveryAndStatus(coord *consensus.CoordNode, tick int64, ids []consensus.NodeID, statuses []consensus.PoolerStatusIndicator) {
	inds := make([]consensus.Indicator, 0, len(ids)+len(statuses))
	for _, id := range ids {
		inds = append(inds, consensus.PoolerDiscoveredIndicator{PoolerID: id})
	}
	for _, s := range statuses {
		inds = append(inds, s)
	}
	coord.Step(tick, inds)
}

// TestCoordClusterViewQuorumConfirmed verifies that when all cohort members have
// applied the same rules version, the coordinator considers it quorum-confirmed.
//
// Setup: 3 nodes (1 primary, 2 replicas) all reporting the same Seq=3 rules
// with AtLeast(3) policy. AtLeast(3) requires primary + 2 replica acks; both replicas confirm
// Seq=3, so the write is provably durable.
func TestCoordClusterViewQuorumConfirmed(t *testing.T) {
	const (
		node1 consensus.NodeID = "node-1"
		node2 consensus.NodeID = "node-2"
		node3 consensus.NodeID = "node-3"
	)
	coord := consensus.NewCoordNode("coord-1", consensus.AtLeastPolicy(3), nil)

	rules := makeTerm(3, node1, []consensus.NodeID{node1, node2, node3}, 3)

	stepWithDiscoveryAndStatus(coord, 1,
		[]consensus.NodeID{node1, node2, node3},
		[]consensus.PoolerStatusIndicator{
			statusInd(node1, consensus.RolePrimary, node1, rules, consensus.PostgresRunning),
			statusInd(node2, consensus.RoleReplica, node1, rules, consensus.PostgresRunning),
			statusInd(node3, consensus.RoleReplica, node1, rules, consensus.PostgresRunning),
		},
	)

	view := coord.ClusterView(1)
	require.NotNil(t, view.HighestSeenTerm, "should have seen rules")
	require.NotNil(t, view.HighestQuorumTerm, "AtLeast(3) quorum is met by primary+node2+node3")
	assert.Equal(t, int64(3), view.HighestSeenTerm.Seq)
	assert.Equal(t, int64(3), view.HighestQuorumTerm.Seq)
	assert.Equal(t, node1, view.HighestQuorumTerm.Primary)
}

// TestCoordClusterViewPartialChange verifies the "partial leader-led change" case:
// the primary has applied Seq=4 but only one replica confirms it, so quorum for
// Seq=4 is not met under AtLeast(3). The prior Seq=3 version does have quorum.
//
// This is the key signal for a coordinator-led term change: HighestSeenTerm.Seq >
// HighestQuorumTerm.Seq means a term change was started but not completed.
func TestCoordClusterViewPartialChange(t *testing.T) {
	const (
		node1 consensus.NodeID = "node-1"
		node2 consensus.NodeID = "node-2"
		node3 consensus.NodeID = "node-3"
	)
	coord := consensus.NewCoordNode("coord-1", consensus.AtLeastPolicy(3), nil)

	rules3 := makeTerm(3, node1, []consensus.NodeID{node1, node2, node3}, 3)
	rules4 := makeTerm(4, node1, []consensus.NodeID{node1, node2, node3}, 3)

	// node1 (primary) and node2 have Seq=4; node3 is still at Seq=3.
	stepWithDiscoveryAndStatus(coord, 1,
		[]consensus.NodeID{node1, node2, node3},
		[]consensus.PoolerStatusIndicator{
			statusInd(node1, consensus.RolePrimary, node1, rules4, consensus.PostgresRunning),
			statusInd(node2, consensus.RoleReplica, node1, rules4, consensus.PostgresRunning),
			statusInd(node3, consensus.RoleReplica, node1, rules3, consensus.PostgresRunning),
		},
	)

	view := coord.ClusterView(1)
	require.NotNil(t, view.HighestSeenTerm)
	require.NotNil(t, view.HighestQuorumTerm)
	// Seen the Seq=4 record (from node1 and node2).
	assert.Equal(t, int64(4), view.HighestSeenTerm.Seq, "highest seen should be Seq=4")
	// Quorum only confirmed for Seq=3 (node2+node3 both reported >= 3; only node2 reported >= 4).
	assert.Equal(t, int64(3), view.HighestQuorumTerm.Seq, "quorum only confirmed for Seq=3")
}

// TestCoordClusterViewSingleNodeAtLeast1 verifies that a 1-node cluster with
// AtLeast(1) always has quorum confirmed (the primary alone is sufficient).
func TestCoordClusterViewSingleNodeAtLeast1(t *testing.T) {
	const node1 consensus.NodeID = "node-1"
	coord := consensus.NewCoordNode("coord-1", nil, nil) // manual mode: no auto-expansion

	rules := makeTerm(1, node1, []consensus.NodeID{node1}, 1)

	stepWithDiscoveryAndStatus(coord, 1,
		[]consensus.NodeID{node1},
		[]consensus.PoolerStatusIndicator{
			statusInd(node1, consensus.RolePrimary, node1, rules, consensus.PostgresRunning),
		},
	)

	view := coord.ClusterView(1)
	require.NotNil(t, view.HighestSeenTerm)
	require.NotNil(t, view.HighestQuorumTerm, "AtLeast(1) always has quorum (primary alone is sufficient)")
	assert.Equal(t, int64(1), view.HighestQuorumTerm.Seq)
	assert.Equal(t, node1, view.HighestQuorumTerm.Primary)
}

// TestCoordClusterViewNoTerm verifies the coordinator's view when no pooler
// has reported any term yet.
func TestCoordClusterViewNoTerm(t *testing.T) {
	coord := consensus.NewCoordNode("coord-1", consensus.AtLeastPolicy(3), nil)

	coord.Step(1, []consensus.Indicator{
		consensus.PoolerDiscoveredIndicator{PoolerID: "node-1"},
		consensus.PoolerStatusIndicator{
			PoolerID: "node-1",
			State: consensus.PoolerPersistentState{
				Role: consensus.RolePrimary, Primary: "node-1",
			},
			PostgresStatus: consensus.PostgresRunning,
		},
	})

	view := coord.ClusterView(1)
	assert.Nil(t, view.HighestSeenTerm, "no term has been reported yet")
	assert.Nil(t, view.HighestQuorumTerm, "no quorum without any term")
}

// TestCoordClusterViewPrimaryUnhealthy verifies that a stopped primary appears
// in the ClusterView but the coordinator does not consider it a healthy primary.
// Health tracking feeds into the coordinator-led term change decision.
func TestCoordClusterViewPrimaryUnhealthy(t *testing.T) {
	const (
		node1 consensus.NodeID = "node-1"
		node2 consensus.NodeID = "node-2"
		node3 consensus.NodeID = "node-3"
	)
	coord := consensus.NewCoordNode("coord-1", consensus.AtLeastPolicy(3), nil)

	rules := makeTerm(3, node1, []consensus.NodeID{node1, node2, node3}, 3)

	// node1 (primary) is stopped; replicas are running.
	stepWithDiscoveryAndStatus(coord, 1,
		[]consensus.NodeID{node1, node2, node3},
		[]consensus.PoolerStatusIndicator{
			statusInd(node1, consensus.RolePrimary, node1, rules, consensus.PostgresStopped),
			statusInd(node2, consensus.RoleReplica, node1, rules, consensus.PostgresRunning),
			statusInd(node3, consensus.RoleReplica, node1, rules, consensus.PostgresRunning),
		},
	)

	view := coord.ClusterView(1)
	// Term is still quorum-verified (replicas confirmed Seq=3 even if primary is down).
	require.NotNil(t, view.HighestQuorumTerm)
	assert.Equal(t, int64(3), view.HighestQuorumTerm.Seq)
	// But the identified primary is unhealthy.
	assert.False(t, view.PrimaryHealthy, "stopped primary should not be considered healthy")
}

// TestCoordClusterViewHealthTimeout verifies that a primary that has stopped
// sending status updates is eventually marked unhealthy once the health timeout
// is exceeded.
func TestCoordClusterViewHealthTimeout(t *testing.T) {
	const (
		node1 consensus.NodeID = "node-1"
		node2 consensus.NodeID = "node-2"
		node3 consensus.NodeID = "node-3"
	)
	coord := consensus.NewCoordNode("coord-1", consensus.AtLeastPolicy(3), nil)
	coord.SetHealthTimeout(5) // mark primary stale after 5 ticks without status

	rules := makeTerm(3, node1, []consensus.NodeID{node1, node2, node3}, 3)

	// Tick 1: all three nodes report healthy status.
	stepWithDiscoveryAndStatus(coord, 1,
		[]consensus.NodeID{node1, node2, node3},
		[]consensus.PoolerStatusIndicator{
			statusInd(node1, consensus.RolePrimary, node1, rules, consensus.PostgresRunning),
			statusInd(node2, consensus.RoleReplica, node1, rules, consensus.PostgresRunning),
			statusInd(node3, consensus.RoleReplica, node1, rules, consensus.PostgresRunning),
		},
	)

	view := coord.ClusterView(1)
	assert.True(t, view.PrimaryHealthy, "primary just reported at tick 1, should be healthy at tick 1")

	// Tick 5: still within timeout window (5 ticks since tick 1).
	coord.Step(5, nil)
	view = coord.ClusterView(5)
	assert.True(t, view.PrimaryHealthy, "within timeout at tick 5")

	// Tick 7: 6 ticks since last primary status (tick 1) — exceeds timeout of 5.
	coord.Step(7, nil)
	view = coord.ClusterView(7)
	assert.False(t, view.PrimaryHealthy, "6 ticks since last status, exceeds timeout of 5")
}
