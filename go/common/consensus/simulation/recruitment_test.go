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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/consensus"
	"github.com/multigres/multigres/go/tools/dstsim"
)

// recruitResponse is a Condition that becomes true once a RecruitResponseRequest
// with the expected Accepted value arrives.
type recruitResponse struct {
	correlationID string
	wantAccepted  bool
	received      bool
}

func (c *recruitResponse) Eval(_ *simType) bool { return c.received }

func (c *recruitResponse) Name() string {
	return fmt.Sprintf("recruit-%s-accepted=%v", c.correlationID, c.wantAccepted)
}

func (c *recruitResponse) Describe(_ *simType) string {
	if c.received {
		return fmt.Sprintf("recruit %s response received (accepted=%v)", c.correlationID, c.wantAccepted)
	}
	return fmt.Sprintf("waiting for recruit %s response (want accepted=%v)", c.correlationID, c.wantAccepted)
}

// recruitCondition enqueues a RecruitIndicator to the given pooler and returns a
// Condition that becomes true once the correlated RecruitResponseRequest arrives
// with the expected Accepted value.
func recruitCondition(
	pooler *SimPooler,
	coordID consensus.NodeID,
	atTermSeq, proposedSeq int64,
	correlationID string,
	wantAccepted bool,
) *recruitResponse {
	cond := &recruitResponse{correlationID: correlationID, wantAccepted: wantAccepted}
	pooler.SendRecruitIndicator(consensus.RecruitIndicator{
		CorrelationID: correlationID,
		CoordID:       coordID,
		AtTermSeq:     atTermSeq,
		ProposedSeq:   proposedSeq,
	}, func(resp consensus.RecruitResponseRequest) {
		if resp.Accepted == wantAccepted {
			cond.received = true
		}
	})
	return cond
}

// TestRecruitRejection verifies that RecruitIndicator is rejected with
// Accepted=false in three cases:
//
//  1. Stale base: AtTermSeq < committed.PolicySeq().
//  2. Different coordinator with the same ProposedSeq as the existing commitment.
//  3. Any coordinator with a lower ProposedSeq than the existing commitment.
func TestRecruitRejection(t *testing.T) {
	const (
		coordID  consensus.NodeID = "coord-1"
		coord2ID consensus.NodeID = "coord-2"
		node1ID  consensus.NodeID = "node-1"
	)

	sim := newTestSim(coordID)

	seedTerm := &consensus.Term{
		Seq:     1,
		Primary: node1ID,
		Members: []consensus.CohortMember{{ID: node1ID}},
		Policy:  consensus.AtLeastPolicy(1),
	}
	pooler1 := newPrimaryPooler(node1ID, seedTerm, sim)
	sim.RegisterNode(pooler1)
	sim.RegisterNode(NewSimCoordNode(consensus.NewCoordNode(coordID, nil), sim))

	th := dstsim.NewSimulationTestHelper(t, sim)

	// Case 1: stale base — AtTermSeq=0 when primary is at PolicySeq=1.
	th.RequireWithinTicks(
		recruitCondition(pooler1, coordID, 0, 2, "reject-stale", false),
		10,
	)
	require.Nil(t, pooler1.Node().CommittedState().Commitment,
		"commitment must not be set after stale-base rejection")

	// Establish a valid commitment so cases 2 and 3 have something to compete with.
	th.RequireWithinTicks(
		recruitCondition(pooler1, coordID, 1, 3, "accept-coord1", true),
		20,
	)
	require.Equal(t, int64(3), pooler1.Node().CommittedState().Commitment.ProposedSeq)

	// Case 2: different coordinator, same ProposedSeq — not strictly higher, not idempotent.
	th.RequireWithinTicks(
		recruitCondition(pooler1, coord2ID, 1, 3, "reject-same-target", false),
		10,
	)
	require.Equal(t, coordID, pooler1.Node().CommittedState().Commitment.CoordID,
		"commitment coordinator must not change after same-target rejection")

	// Case 3: any coordinator with a lower ProposedSeq.
	th.RequireWithinTicks(
		recruitCondition(pooler1, coord2ID, 1, 2, "reject-lower", false),
		10,
	)
	require.Equal(t, int64(3), pooler1.Node().CommittedState().Commitment.ProposedSeq,
		"commitment ProposedSeq must not decrease after lower-target rejection")
}

// TestRecruitIdempotent verifies the idempotent fast path and supersede semantics:
//
//  1. Initial acceptance persists the commitment and revokes the node.
//  2. Same coordinator, same ProposedSeq: idempotent fast path responds immediately.
//  3. Same coordinator, higher ProposedSeq: supersedes and updates the commitment.
//  4. Different coordinator, even higher ProposedSeq: supersedes and updates the commitment.
func TestRecruitIdempotent(t *testing.T) {
	const (
		coordID  consensus.NodeID = "coord-1"
		coord2ID consensus.NodeID = "coord-2"
		node1ID  consensus.NodeID = "node-1"
	)

	sim := newTestSim(coordID)

	seedTerm := &consensus.Term{
		Seq:     1,
		Primary: node1ID,
		Members: []consensus.CohortMember{{ID: node1ID}},
		Policy:  consensus.AtLeastPolicy(1),
	}
	pooler1 := newPrimaryPooler(node1ID, seedTerm, sim)
	sim.RegisterNode(pooler1)
	sim.RegisterNode(NewSimCoordNode(consensus.NewCoordNode(coordID, nil), sim))

	th := dstsim.NewSimulationTestHelper(t, sim)

	// Initial acceptance: commitment is persisted, sidecar revokes the node.
	th.RequireWithinTicks(
		recruitCondition(pooler1, coordID, 1, 3, "accept-1", true),
		20,
	)
	require.Equal(t, &consensus.RecruitmentCommitment{
		CoordID:     coordID,
		AtTermSeq:   1,
		ProposedSeq: 3,
	}, pooler1.Node().CommittedState().Commitment)
	require.True(t, pooler1.isRevoked())

	// Idempotent fast path: same coordinator, same ProposedSeq responds immediately
	// without a sidecar round-trip.
	th.RequireWithinTicks(
		recruitCondition(pooler1, coordID, 1, 3, "accept-idempotent", true),
		5,
	)
	require.Equal(t, int64(3), pooler1.Node().CommittedState().Commitment.ProposedSeq,
		"commitment must be unchanged after idempotent re-request")

	// Supersede: same coordinator, strictly higher ProposedSeq.
	th.RequireWithinTicks(
		recruitCondition(pooler1, coordID, 1, 4, "accept-higher", true),
		20,
	)
	require.Equal(t, int64(4), pooler1.Node().CommittedState().Commitment.ProposedSeq,
		"commitment ProposedSeq must be updated to 4")
	require.True(t, pooler1.isRevoked())

	// Supersede by a different coordinator with an even higher ProposedSeq.
	th.RequireWithinTicks(
		recruitCondition(pooler1, coord2ID, 1, 5, "accept-coord2", true),
		20,
	)
	require.Equal(t, coord2ID, pooler1.Node().CommittedState().Commitment.CoordID,
		"commitment coordinator must be updated to coord-2")
	require.Equal(t, int64(5), pooler1.Node().CommittedState().Commitment.ProposedSeq)
	require.True(t, pooler1.isRevoked())
}

// TestRecruitRevokesParticipation verifies the sidecar revocation effects in a
// two-node cluster:
//
//  1. Recruiting a replica stops it from ACKing WAL, leaving the primary stuck
//     in awaitQuorum unable to complete a synchronous write.
//  2. Recruiting the primary aborts the stuck write (WAL truncation) and
//     prevents any further writes via isRevoked().
func TestRecruitRevokesParticipation(t *testing.T) {
	const (
		coordID consensus.NodeID = "coord-1"
		node1ID consensus.NodeID = "node-1"
		node2ID consensus.NodeID = "node-2"
	)

	sim := newTestSim(coordID)

	seedTerm := &consensus.Term{
		Seq:     1,
		Primary: node1ID,
		Members: []consensus.CohortMember{{ID: node1ID}},
		Policy:  consensus.AtLeastPolicy(1),
	}
	pooler1 := newPrimaryPooler(node1ID, seedTerm, sim)
	pooler2 := newReplicaPooler(node2ID, node1ID, sim)
	sim.RegisterNode(pooler1)
	sim.RegisterNode(pooler2)
	// Coordinator with AtLeast(2) target expands the cohort automatically.
	coord := NewSimCoordNode(consensus.NewCoordNode(coordID, consensus.AtLeastPolicy(2)), sim)
	sim.RegisterNode(coord)

	th := dstsim.NewSimulationTestHelper(t, sim)

	// Expand to a two-node AtLeast(2) cohort first so the primary requires node2's ACK.
	th.RequireWithinTicks(&allHaveAppliedRules{
		poolers:     []*SimPooler{pooler1, pooler2},
		members:     []consensus.NodeID{node1ID, node2ID},
		wantAtLeast: 2,
	}, 200)
	// After expansion the committed term is at Seq=2.
	require.Equal(t, int64(2), pooler1.Node().CommittedState().PolicySeq())

	// Recruit node2 (replica): it will stop ACKing WAL after its sidecar revokes.
	th.RequireWithinTicks(
		recruitCondition(pooler2, coordID, 2, 3, "recruit-node2", true),
		20,
	)
	require.True(t, pooler2.isRevoked(), "node2 must be revoked after recruitment")

	// Attempt a write that requires node2's ACK. With node2 revoked the primary
	// will be stuck in awaitQuorum indefinitely.
	var writeCallbackCalled bool
	pooler1.SendWritePolicyIndicator(consensus.WritePolicyIndicator{
		CorrelationID: "stuck-write",
		Term: consensus.Term{
			Seq:     3,
			Primary: node1ID,
			Members: []consensus.CohortMember{{ID: node1ID}, {ID: node2ID}},
			Policy:  consensus.AtLeastPolicy(2),
		},
	}, func(_ consensus.WritePolicyResponseRequest) {
		writeCallbackCalled = true
	})
	th.RequireAdvance(30)
	require.False(t, writeCallbackCalled, "write must not complete while node2 is revoked")

	// Recruit node1 (primary): aborts the stuck write and enters read-only mode.
	// The pending WAL entry is truncated (crash-recovery semantics) and the write
	// pipeline emits ApplyRulesResponseIndicator{Accepted:false}, triggering the
	// write callback.
	th.RequireWithinTicks(
		recruitCondition(pooler1, coordID, 2, 3, "recruit-node1", true),
		20,
	)
	require.True(t, writeCallbackCalled, "write callback must fire after primary is revoked")
	require.True(t, pooler1.isRevoked(), "node1 must be revoked after recruitment")
	require.Equal(t, int64(2), pooler1.Node().CommittedState().PolicySeq(),
		"committed PolicySeq must remain at 2 — the pending write was aborted")
}
