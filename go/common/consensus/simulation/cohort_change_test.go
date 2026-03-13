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

// policyWriteResponse is a Condition that becomes true once a
// WritePolicyResponseRequest with a matching Accepted value arrives in
// response to a SendWritePolicyIndicator call.
type policyWriteResponse struct {
	seq          int64
	wantAccepted bool
	received     bool
}

func (c *policyWriteResponse) Eval(_ *simType) bool { return c.received }

func (c *policyWriteResponse) Name() string {
	return fmt.Sprintf("policy-write-%d-accepted=%v", c.seq, c.wantAccepted)
}

func (c *policyWriteResponse) Describe(_ *simType) string {
	if c.received {
		return fmt.Sprintf("policy write seq=%d response received (accepted=%v)", c.seq, c.wantAccepted)
	}
	return fmt.Sprintf("waiting for policy write seq=%d response (want accepted=%v)", c.seq, c.wantAccepted)
}

// policyWriteCondition delivers a WritePolicyIndicator directly to the primary
// pooler and returns a condition that becomes true once the correlated response
// arrives with the given Accepted value.
//
// fromSeq is the CAS base: the primary's committed.PolicySeq() must equal this
// for the write to be accepted. For normal sequential writes pass seq-1; for
// stale-base rejection tests pass any value that does not match the primary's
// current PolicySeq.
func policyWriteCondition(
	pooler *SimPooler,
	fromSeq int64,
	seq int64,
	primary consensus.NodeID,
	members []consensus.CohortMember,
	policy consensus.DurabilityPolicy,
	wantAccepted bool,
) *policyWriteResponse {
	cond := &policyWriteResponse{seq: seq, wantAccepted: wantAccepted}
	pooler.SendWritePolicyIndicator(consensus.LeaderWritePolicyIndicator{
		CorrelationID: fmt.Sprintf("write-seq-%d", seq),
		FromSeq:       fromSeq,
		Term: consensus.Term{
			Seq:     seq,
			Primary: primary,
			Members: members,
			Policy:  policy,
		},
	}, func(resp consensus.LeaderWritePolicyResponseRequest) {
		if resp.Accepted == wantAccepted {
			cond.received = true
		}
	})
	return cond
}

// TestCohortChange verifies explicit cohort and policy management by injecting
// WritePolicyIndicator events directly into the primary pooler. The coordinator
// is present in manual mode (nil targetPolicy) to process status broadcasts and
// keep the cluster's view consistent, but all rule changes are driven by the
// test itself.
//
// Stages:
//  1. Expand: add node2 and node3 simultaneously with AtLeast(1).
//  2. Policy upgrade: change to AtLeast(2).
//  3. Policy downgrade: change back to AtLeast(1).
//  4. Shrink: remove node2 and node3, returning to a 1-node cohort.
func TestCohortChange(t *testing.T) {
	const (
		coordID consensus.NodeID = "coord-1"
		node1ID consensus.NodeID = "node-1"
		node2ID consensus.NodeID = "node-2"
		node3ID consensus.NodeID = "node-3"
	)

	sim := newTestSim(coordID)

	// Pre-initialize node1 as primary with a 1-node bootstrap policy.
	seedTerm := &consensus.Term{
		Seq:     1,
		Primary: node1ID,
		Members: []consensus.CohortMember{{ID: node1ID}},
		Policy:  consensus.AtLeastPolicy(1),
	}
	pooler1 := newPrimaryPooler(node1ID, seedTerm, sim)
	sim.RegisterNode(pooler1)

	// Coordinator in manual mode (nil targetPolicy): never auto-adds observers.
	coord := NewSimCoordNode(consensus.NewCoordNode(coordID, nil, nil, nil), sim)
	sim.RegisterNode(coord)

	// node2 and node3 join as replicas streaming from node1.
	pooler2 := newReplicaPooler(node2ID, node1ID, sim)
	pooler3 := newReplicaPooler(node3ID, node1ID, sim)
	sim.RegisterNode(pooler2)
	sim.RegisterNode(pooler3)

	th := dstsim.NewSimulationTestHelper(t, sim)

	allPoolers := []*SimPooler{pooler1, pooler2, pooler3}
	allMembers := []consensus.NodeID{node1ID, node2ID, node3ID}
	allMembersFull := []consensus.CohortMember{
		{ID: node1ID},
		{ID: node2ID},
		{ID: node3ID},
	}

	// Stage 1: add node2 and node3 simultaneously with AtLeast(1).
	th.RequireWithinTicks(dstsim.And(
		&allHaveAppliedRules{poolers: allPoolers, members: allMembers, wantAtLeast: 1},
		policyWriteCondition(pooler1, 1, 2, node1ID, allMembersFull, consensus.AtLeastPolicy(1), true),
	), 200)

	// Stage 2: upgrade durability policy to AtLeast(2).
	th.RequireWithinTicks(dstsim.And(
		&allHaveAppliedRules{poolers: allPoolers, members: allMembers, wantAtLeast: 2},
		policyWriteCondition(pooler1, 2, 3, node1ID, allMembersFull, consensus.AtLeastPolicy(2), true),
	), 200)

	// Stage 3: downgrade policy back to AtLeast(1).
	// The effective sync settings approach ensures the write uses the old
	// AtLeast(2) quorum before relaxing to AtLeast(1).
	th.RequireWithinTicks(dstsim.And(
		&allHaveAppliedRules{poolers: allPoolers, members: allMembers, wantAtLeast: 1},
		policyWriteCondition(pooler1, 3, 4, node1ID, allMembersFull, consensus.AtLeastPolicy(1), true),
	), 200)

	// Stage 4: shrink cohort to just node1, removing node2 and node3.
	// The primary applies effective sync settings (empty intersection of current
	// standbys and new members) before the write so removed nodes cannot ack
	// their own removal. Since effective policy is AtLeast(1), the write is
	// immediately durable. node2 and node3 continue streaming WAL and apply
	// the removal record, updating their committed term to the 1-node cohort.
	th.RequireWithinTicks(dstsim.And(
		&allHaveAppliedRules{poolers: allPoolers, members: []consensus.NodeID{node1ID}, wantAtLeast: 1},
		policyWriteCondition(pooler1, 4, 5, node1ID, []consensus.CohortMember{{ID: node1ID}}, consensus.AtLeastPolicy(1), true),
	), 200)
}

// TestPolicyWriteRejection verifies that WritePolicyIndicator is rejected with
// Accepted=false in three cases:
//  1. CAS mismatch: fromSeq does not match committed.PolicySeq().
//  2. Write to replica: replicas reject direct WAL writes and respond immediately.
//  3. Unachievable policy: AtLeast(N) with fewer than N members.
func TestPolicyWriteRejection(t *testing.T) {
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
	sim.RegisterNode(NewSimCoordNode(consensus.NewCoordNode(coordID, nil, nil, nil), sim))

	th := dstsim.NewSimulationTestHelper(t, sim)

	// Case 1: CAS mismatch — fromSeq=99 but primary's PolicySeq is 1.
	// A stale coordinator that believes the primary is at seq=99 is rejected.
	th.RequireWithinTicks(
		policyWriteCondition(pooler1, 99, 100, node1ID, []consensus.CohortMember{{ID: node1ID}}, consensus.AtLeastPolicy(1), false),
		10,
	)
	require.Equal(t, int64(1), pooler1.Node().CommittedState().PolicySeq(),
		"primary state must not change after CAS-mismatch rejection")

	// Case 2: write to a replica — replicas reject direct WAL writes.
	th.RequireWithinTicks(
		policyWriteCondition(pooler2, 1, 2, node1ID, []consensus.CohortMember{{ID: node1ID}}, consensus.AtLeastPolicy(1), false),
		10,
	)
	require.Equal(t, int64(1), pooler2.Node().CommittedState().PolicySeq(),
		"replica state must not change after write rejection")

	// Case 3: unachievable policy — AtLeast(4) requires 4 nodes, but cohort has 1.
	th.RequireWithinTicks(
		policyWriteCondition(pooler1, 1, 2, node1ID, []consensus.CohortMember{{ID: node1ID}}, consensus.AtLeastPolicy(4), false),
		10,
	)
	require.Equal(t, int64(1), pooler1.Node().CommittedState().PolicySeq(),
		"primary state must not change after unachievable policy rejection")
}
