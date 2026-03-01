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

package consensus_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/consensus"
	"github.com/multigres/multigres/go/tools/dstsim"
)

// TestBootstrap_CohortMemberPersistedAfterEstablish verifies that after a
// successful initial appointment, all participating poolers have CohortMember=true
// persisted to storage and that it survives a simulated crash-restart.
func TestBootstrap_CohortMemberPersistedAfterEstablish(t *testing.T) {
	policy := consensus.AnyNPolicy(1)
	sim, stores := newHappyPathSim(t, 42, policy)
	for _, inv := range standardInvariants(policy) {
		sim.Always(inv)
	}

	h := dstsim.NewSimulationTestHelper(t, sim)
	h.RequireRunUntil(&activePrimaryExists{}, 200)

	// All poolers must be cohort members after the first successful Establish.
	for _, id := range []consensus.NodeID{pooler1, pooler2, pooler3} {
		require.True(t, stores[id].state.CohortMember,
			"pooler %v should be a cohort member after initial establishment", id)
	}

	// CohortMember must survive a crash-restart (it is persisted to storage).
	for _, node := range sim.Nodes() {
		p, ok := node.(*consensus.PoolerNode)
		if !ok {
			continue
		}
		sim.RestartNode(p.ID())
		require.True(t, p.CommittedState().CohortMember,
			"pooler %v CohortMember should survive crash-restart", p.ID())
	}
}

// TestBootstrap_CohortRejectionBlocksBootstrapOrch verifies the bootstrap safety
// property: a pooler with CohortMember=true rejects proposals from an orch that
// has not yet discovered the existing cohort (empty CohortMembers list).
//
// This is tested directly against the PoolerNode state machine without a full
// simulation, keeping the test focused on the rejection logic.
func TestBootstrap_CohortRejectionBlocksBootstrapOrch(t *testing.T) {
	// Create a pooler that is already an established cohort member.
	store := &memStorage{state: consensus.PoolerPersistentState{
		CohortMember: true,
		VotedTerm:    5,
		VotedSeqNum:  3,
		VotedCoord:   orchA,
		Role:         consensus.RoleReplica,
		Primary:      pooler1,
		PrimaryTerm:  5,
	}}
	p := consensus.NewPoolerNode(pooler2, store)

	// A new orch that has not yet learned the cohort sends a Begin at a higher term
	// with an empty CohortMembers list — the hallmark of a bootstrap attempt.
	bootstrapBegin := consensus.OrchStateIndicator{
		FromOrch: orchB,
		State: consensus.ConsensusState{
			VotingTerm: 10, // higher term than any committed
			CoordID:    orchB,
			SeqNum:     1,
			Phase:      consensus.PhaseBegin,
			// CohortMembers intentionally empty: orch has no knowledge of existing cohort
		},
	}

	requests := p.Step(1, []consensus.Indicator{bootstrapBegin})

	require.Len(t, requests, 1, "should emit a response")
	resp, ok := requests[0].(consensus.PoolerResponseRequest)
	require.True(t, ok, "response should be PoolerResponseRequest")
	require.False(t, resp.Accepted, "cohort member must reject bootstrap proposal with empty CohortMembers")

	// The pooler's committed state must not have changed.
	require.Equal(t, store.state, p.CommittedState(), "committed state must be unchanged after rejection")
}

// TestBootstrap_CohortRejectionDoesNotBlockKnownCohort verifies the inverse:
// a pooler with CohortMember=true accepts proposals from an orch that correctly
// lists the pooler in its CohortMembers.
func TestBootstrap_CohortRejectionDoesNotBlockKnownCohort(t *testing.T) {
	store := &memStorage{state: consensus.PoolerPersistentState{
		CohortMember: true,
		VotedTerm:    5,
		VotedSeqNum:  3,
		VotedCoord:   orchA,
		Role:         consensus.RoleReplica,
		Primary:      pooler1,
		PrimaryTerm:  5,
	}}
	p := consensus.NewPoolerNode(pooler2, store)

	// A re-appointment orch that knows the cohort includes this pooler in CohortMembers.
	reappointBegin := consensus.OrchStateIndicator{
		FromOrch: orchB,
		State: consensus.ConsensusState{
			VotingTerm:    10,
			CoordID:       orchB,
			SeqNum:        1,
			Phase:         consensus.PhaseBegin,
			Primary:       pooler1,
			PrimaryTerm:   5,
			CohortMembers: []consensus.NodeID{pooler1, pooler2, pooler3},
		},
	}

	requests := p.Step(1, []consensus.Indicator{reappointBegin})

	var resp consensus.PoolerResponseRequest
	var found bool
	for _, r := range requests {
		if r, ok := r.(consensus.PoolerResponseRequest); ok {
			resp = r
			found = true
			break
		}
	}
	require.True(t, found, "should emit a PoolerResponseRequest")
	require.True(t, resp.Accepted, "cohort member must accept proposal when it is listed in CohortMembers")
}
