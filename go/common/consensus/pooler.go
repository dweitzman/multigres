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

package consensus

import "slices"

// poolerNode is the pooler state machine. It receives ConsensusState proposals from
// orch and votes to accept or reject them according to the safety invariant.
//
// State lifecycle: two distinct phases must both succeed for a change to take effect.
//
//  1. Commit (durable): the pooler writes the accepted ConsensusState to PoolerStorage
//     via a synchronous save before sending any response. This is what survives a crash.
//     Tracked in poolerNode.committed (a PoolerPersistentState derived from the proposal).
//
//  2. Apply (operational): the pooler executes the role change — e.g. pg_ctl promote,
//     reconfigure standby recovery.conf, update routing. This is ephemeral in general,
//     but in some implementations may be re-derived from committed state on startup.
//     Tracked by poolerNode.applied bool: true means the current committed state
//     (identified by committed.VotedStateID()) has been operationally applied.
//
// Using a bool for applied works because the committed StateID (term + SeqNum) already
// names the exact state. If applied is true and committed.VotedStateID() == (term=5, seq=2),
// the orch knows that exactly that proposal is running — not just "term 5 was applied".
//
// Currently apply is modelled as instant: it completes in the same tick as the commit.
// Future stages will introduce simulated apply delays and failures so that DST can
// find bugs where orch relies on a pooler having applied before it has actually done so.
// At that point the pooler will track a pendingApply and complete it in a later tick,
// broadcasting PoolerStatusIndicator updates as apply progresses (identified by StateID).
//
// poolerNode implements dstsim.Restartable. Currently Restart() is a no-op because
// apply is instant and committed state is managed separately via LoadCommittedState.
// Future stages may add ephemeral state (e.g. WAL position, in-flight writes) that
// should be cleared here to simulate crash recovery accurately.
type PoolerNode struct {
	id      NodeID
	storage PoolerStorage

	// committed is the last state successfully persisted via storage.Save.
	// All voting decisions are based on committed; it is the source of truth for the term.
	committed PoolerPersistentState

	// applied tracks whether the current committed state has been operationally executed.
	// Whether this survives a crash depends on the implementation; for now it is managed
	// in memory and re-set when the node re-applies state.
	applied bool
}

// NewPoolerNode creates a new pooler node with the given identity and storage.
// The node starts with zero committed state (as if freshly provisioned).
func NewPoolerNode(id NodeID, storage PoolerStorage) *PoolerNode {
	return &PoolerNode{
		id:      id,
		storage: storage,
	}
}

// ID returns the pooler node's unique identifier.
func (n *PoolerNode) ID() NodeID {
	return n.id
}

// Restart is called by the simulator when simulating a crash-restart.
// Currently a no-op: apply is instant and committed state survives in storage.
// Future stages will add ephemeral state (e.g. pendingApply) to clear here.
func (n *PoolerNode) Restart() {}

// LoadCommittedState restores the committed state after a restart.
// In production this is called during startup after reading the state file from disk.
// In simulation it is called by the test after RestartNode() to give the pooler back
// its durable state.
func (n *PoolerNode) LoadCommittedState(state PoolerPersistentState) {
	n.committed = state
}

// CommittedState returns the current committed state (for inspection in tests).
func (n *PoolerNode) CommittedState() PoolerPersistentState {
	return n.committed
}

// IsApplied reports whether the current committed state has been operationally executed.
func (n *PoolerNode) IsApplied() bool {
	return n.applied
}

// Step processes all indicators that arrived this tick and returns requests.
// Persistence via storage.Save is called synchronously before any response is emitted.
func (n *PoolerNode) Step(tick int64, indicators []Indicator) []Request {
	var requests []Request

	for _, ind := range indicators {
		switch v := ind.(type) {
		case OrchStateIndicator:
			requests = append(requests, n.handleOrchState(v)...)
		}
	}

	return requests
}

func (n *PoolerNode) handleOrchState(ind OrchStateIndicator) []Request {
	state := ind.State

	// Reject if the proposal is stale (term is behind what we've already voted for)
	if state.VotingTerm < n.committed.VotedTerm {
		return []Request{PoolerResponseRequest{
			ToOrch:       ind.FromOrch,
			Accepted:     false,
			KnownTerm:    n.committed.VotedTerm,
			KnownCoordID: n.committed.VotedCoord,
		}}
	}

	// Reject if same term but a different coordinator already won it (safety invariant)
	if state.VotingTerm == n.committed.VotedTerm &&
		n.committed.VotedCoord != "" &&
		state.CoordID != n.committed.VotedCoord {
		return []Request{PoolerResponseRequest{
			ToOrch:       ind.FromOrch,
			Accepted:     false,
			KnownTerm:    n.committed.VotedTerm,
			KnownCoordID: n.committed.VotedCoord,
		}}
	}

	// Reject if the orch's expected primary term does not match ours (stale cluster view)
	if ind.ExpectedPrimaryTerm > 0 && ind.ExpectedPrimaryTerm != n.committed.PrimaryTerm {
		return []Request{PoolerResponseRequest{
			ToOrch:    ind.FromOrch,
			Accepted:  false,
			KnownTerm: n.committed.VotedTerm,
		}}
	}

	// Determine this pooler's new role based on the proposed state
	role := RoleStandby
	if state.Primary == n.id {
		role = RolePrimary
	} else if slices.Contains(state.SyncReplicas, n.id) {
		role = RoleReplica
	}

	newState := PoolerPersistentState{
		VotedTerm:    state.VotingTerm,
		VotedSeqNum:  state.SeqNum,
		VotedCoord:   state.CoordID,
		PrimaryTerm:  state.PrimaryTerm,
		Primary:      state.Primary,
		Role:         role,
		SyncReplicas: state.SyncReplicas,
	}

	// Persist before responding. If storage fails, do not accept: orch will time out
	// and retry. On retry we will have no record of having accepted this term+seq.
	if err := n.storage.Save(newState); err != nil {
		return nil
	}
	n.committed = newState

	// Apply: currently instant — the role change is considered effective in the same tick
	// as the commit. Future stages will make this async: the pooler will track a pendingApply
	// and mark applied=true in a later tick, identified by committed.VotedStateID() so the
	// orch knows exactly which proposal (term + SeqNum) was applied.
	n.applied = true

	return []Request{PoolerResponseRequest{
		ToOrch:   ind.FromOrch,
		Accepted: true,
	}}
}
