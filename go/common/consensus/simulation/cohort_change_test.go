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
func policyWriteCondition(
	pooler *SimPooler,
	seq int64,
	members []consensus.CohortMember,
	policy consensus.AckPolicy,
	wantAccepted bool,
) *policyWriteResponse {
	cond := &policyWriteResponse{seq: seq, wantAccepted: wantAccepted}
	pooler.SendWritePolicyIndicator(consensus.WritePolicyIndicator{
		CorrelationID: fmt.Sprintf("write-seq-%d", seq),
		Rules: consensus.DurabilityRules{
			Seq:     seq,
			Members: members,
			Policy:  policy,
		},
	}, func(resp consensus.WritePolicyResponseRequest) {
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
//  1. Expand: add node2 and node3 simultaneously with AnyN(0).
//  2. Policy upgrade: change to AnyN(1).
//  3. Policy downgrade: change back to AnyN(0).
//  4. Shrink: remove node2 and node3, returning to a 1-node cohort.
func TestCohortChange(t *testing.T) {
	const (
		coordID consensus.NodeID = "coord-1"
		node1ID consensus.NodeID = "node-1"
		node2ID consensus.NodeID = "node-2"
		node3ID consensus.NodeID = "node-3"
	)

	sim := dstsim.NewSimulator[consensus.Indicator, consensus.Request, consensus.NodeID](
		dstsim.SimulatorOptions{Seed: 42},
	)
	sim.SetRequestHandler(NewHandler(coordID))

	// Pre-initialize node1 as primary with a 1-node bootstrap policy.
	seedRules := &consensus.DurabilityRules{
		Seq:     1,
		Members: []consensus.CohortMember{{ID: node1ID}},
		Policy:  consensus.AnyNPolicy(0),
	}
	pooler1 := newPrimaryPooler(node1ID, seedRules, sim)
	sim.RegisterNode(pooler1)

	// Coordinator in manual mode (nil targetPolicy): never auto-adds observers.
	coord := NewSimCoordNode(consensus.NewCoordNode(coordID, nil), sim)
	sim.RegisterNode(coord)

	// node2 and node3 join as replicas streaming from node1.
	pooler2 := newReplicaPooler(node2ID, node1ID, sim)
	pooler3 := newReplicaPooler(node3ID, node1ID, sim)
	sim.RegisterNode(pooler2)
	sim.RegisterNode(pooler3)

	// Safety invariant: every sync standby must be actively streaming from the primary.
	sim.Always(&syncStandbysAreReplicas{})

	th := dstsim.NewSimulationTestHelper(t, sim)

	allPoolers := []*SimPooler{pooler1, pooler2, pooler3}
	allMembers := []consensus.NodeID{node1ID, node2ID, node3ID}
	allMembersFull := []consensus.CohortMember{
		{ID: node1ID},
		{ID: node2ID},
		{ID: node3ID},
	}

	// Stage 1: add node2 and node3 simultaneously with AnyN(0).
	th.RequireWithinTicks(dstsim.And(
		&allHaveAppliedRules{poolers: allPoolers, members: allMembers, wantAnyN: 0},
		policyWriteCondition(pooler1, 2, allMembersFull, consensus.AnyNPolicy(0), true),
	), 200)

	// Stage 2: upgrade durability policy to AnyN(1).
	th.RequireWithinTicks(dstsim.And(
		&allHaveAppliedRules{poolers: allPoolers, members: allMembers, wantAnyN: 1},
		policyWriteCondition(pooler1, 3, allMembersFull, consensus.AnyNPolicy(1), true),
	), 200)

	// Stage 3: downgrade policy back to AnyN(0).
	// The effective sync settings approach ensures the write uses the old
	// AnyN(1) quorum before relaxing to AnyN(0).
	th.RequireWithinTicks(dstsim.And(
		&allHaveAppliedRules{poolers: allPoolers, members: allMembers, wantAnyN: 0},
		policyWriteCondition(pooler1, 4, allMembersFull, consensus.AnyNPolicy(0), true),
	), 200)

	// Stage 4: shrink cohort to just node1, removing node2 and node3.
	// The primary applies effective sync settings (empty intersection of current
	// standbys and new members) before the write so removed nodes cannot ack
	// their own removal. Since effective policy is AnyN(0), the write is
	// immediately durable. node2 and node3 continue streaming WAL and apply
	// the removal record, updating their committed rules to the 1-node cohort.
	th.RequireWithinTicks(dstsim.And(
		&allHaveAppliedRules{poolers: allPoolers, members: []consensus.NodeID{node1ID}, wantAnyN: 0},
		policyWriteCondition(pooler1, 5, []consensus.CohortMember{{ID: node1ID}}, consensus.AnyNPolicy(0), true),
	), 200)
}

// TestPolicyWriteRejection verifies that WritePolicyIndicator is rejected with
// Accepted=false in two cases:
//  1. CAS mismatch: the incoming Seq does not equal committed.PolicySeq()+1.
//  2. Write to replica: replicas reject direct WAL writes and respond immediately.
func TestPolicyWriteRejection(t *testing.T) {
	const (
		coordID consensus.NodeID = "coord-1"
		node1ID consensus.NodeID = "node-1"
		node2ID consensus.NodeID = "node-2"
	)

	sim := dstsim.NewSimulator[consensus.Indicator, consensus.Request, consensus.NodeID](
		dstsim.SimulatorOptions{Seed: 42},
	)
	sim.SetRequestHandler(NewHandler(coordID))

	seedRules := &consensus.DurabilityRules{
		Seq:     1,
		Members: []consensus.CohortMember{{ID: node1ID}},
		Policy:  consensus.AnyNPolicy(0),
	}
	pooler1 := newPrimaryPooler(node1ID, seedRules, sim)
	pooler2 := newReplicaPooler(node2ID, node1ID, sim)
	sim.RegisterNode(pooler1)
	sim.RegisterNode(pooler2)
	sim.RegisterNode(NewSimCoordNode(consensus.NewCoordNode(coordID, nil), sim))

	th := dstsim.NewSimulationTestHelper(t, sim)

	// Case 1: CAS mismatch — seq=99 when primary expects seq=2.
	th.RequireWithinTicks(
		policyWriteCondition(pooler1, 99, []consensus.CohortMember{{ID: node1ID}}, consensus.AnyNPolicy(0), false),
		10,
	)
	require.Equal(t, int64(1), pooler1.Node().CommittedState().PolicySeq(),
		"primary state must not change after CAS-mismatch rejection")

	// Case 2: write to a replica — replicas reject direct WAL writes.
	th.RequireWithinTicks(
		policyWriteCondition(pooler2, 2, []consensus.CohortMember{{ID: node1ID}}, consensus.AnyNPolicy(0), false),
		10,
	)
	require.Nil(t, pooler2.Node().CommittedState().Rules,
		"replica state must not change after write rejection")

	// Case 3: unachievable policy — AnyN(3) requires 4 nodes, but cohort has 1.
	th.RequireWithinTicks(
		policyWriteCondition(pooler1, 2, []consensus.CohortMember{{ID: node1ID}}, consensus.AnyNPolicy(3), false),
		10,
	)
	require.Equal(t, int64(1), pooler1.Node().CommittedState().PolicySeq(),
		"primary state must not change after unachievable policy rejection")
}
