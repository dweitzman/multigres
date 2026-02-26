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

import "math/rand/v2"

// PoolerNode is the pooler state machine. It receives ConsensusState proposals from
// orch and votes to accept or reject them according to the safety invariant.
//
// State lifecycle: two distinct phases must both succeed for a change to take effect.
//
//  1. Commit (durable): the pooler writes the accepted ConsensusState to PoolerStorage
//     via a synchronous save before sending any response. This survives a crash.
//     Tracked in PoolerNode.committed (derived from the proposal).
//
//  2. Apply (operational): the pooler executes the role change — pg_ctl promote,
//     reconfigure recovery settings, update routing. Tracked by committed.Applied.
//     Applied requires postgres to be running. If postgres was stopped when a
//     proposal was committed (Applied=false), the pooler retries the apply on each
//     subsequent Step while postgres is running, with an optional per-tick failure
//     rate for chaos simulation.
//
// Both committed state and applied status are persisted via storage.Save before
// being reported. PostgresStatus is ephemeral (not persisted). Restart() resets
// it to Running. LoadCommittedState restores the full durable state after a crash.
type PoolerNode struct {
	id      NodeID
	storage PoolerStorage

	// committed is the last state successfully persisted via storage.Save.
	// committed.Applied records whether the role change has been executed on disk.
	committed PoolerPersistentState

	// postgresStatus is the operational state of the postgres instance.
	// Ephemeral: starts as Running, set to Stopped on TerminateIndicator, reset by Restart().
	postgresStatus PostgresStatus

	// needsStatusBroadcast is set by Restart() so the node emits a status update on
	// the next Step() even without incoming indicators (signals to orch it is back up).
	needsStatusBroadcast bool

	// applyFailRate is the per-tick probability (0.0–1.0) that an apply attempt fails.
	// 0.0 (default) means always apply immediately; higher values simulate flaky postgres
	// restarts. Only used when rng is non-nil.
	applyFailRate float64
	rng           *rand.Rand
}

// NewPoolerNode creates a new pooler node with the given identity and storage.
// rng is used to simulate per-tick apply failures; pass nil for deterministic
// behaviour where applies always succeed immediately (the default for most tests).
func NewPoolerNode(id NodeID, storage PoolerStorage, rng *rand.Rand) *PoolerNode {
	return &PoolerNode{
		id:             id,
		storage:        storage,
		postgresStatus: PostgresRunning,
		rng:            rng,
	}
}

// SetApplyFailRate configures the per-tick probability (0.0–1.0) that an apply
// attempt fails. Use this in chaos tests to simulate flaky postgres restarts.
// rng must be non-nil when rate > 0.
func (n *PoolerNode) SetApplyFailRate(rate float64) {
	n.applyFailRate = rate
}

// ID returns the pooler node's unique identifier.
func (n *PoolerNode) ID() NodeID {
	return n.id
}

// Restart is called by the simulator when simulating a crash-restart.
// It resets only ephemeral state (postgres status). Applied status and all other
// committed state are restored by the subsequent LoadCommittedState call.
func (n *PoolerNode) Restart() {
	n.postgresStatus = PostgresRunning
	n.needsStatusBroadcast = true
}

// LoadCommittedState restores the committed state after a restart.
// In production this is called during startup after reading the state file from disk.
func (n *PoolerNode) LoadCommittedState(state PoolerPersistentState) {
	n.committed = state
}

// CommittedState returns the current committed state.
func (n *PoolerNode) CommittedState() PoolerPersistentState {
	return n.committed
}

// IsApplied reports whether the current committed state has been operationally executed.
func (n *PoolerNode) IsApplied() bool {
	return n.committed.Applied
}

// PostgresStatus returns the current postgres operational status.
func (n *PoolerNode) PostgresStatus() PostgresStatus {
	return n.postgresStatus
}

// IsActivePrimary reports whether this pooler is currently operating as primary:
// committed to the primary role, applied, and postgres is running.
func (n *PoolerNode) IsActivePrimary() bool {
	return n.committed.Role == RolePrimary && n.committed.Applied && n.postgresStatus == PostgresRunning
}

// Step processes all indicators that arrived this tick and returns requests.
// Persistence via storage.Save is called synchronously before any response is emitted.
func (n *PoolerNode) Step(tick int64, indicators []Indicator) []Request {
	var requests []Request
	changed := false

	for _, ind := range indicators {
		switch v := ind.(type) {
		case OrchStateIndicator:
			reqs, c := n.handleOrchState(v)
			requests = append(requests, reqs...)
			changed = changed || c
		case TerminateIndicator:
			// Consensus agreements are about replication configuration, not process liveness.
			// Stopping postgres does not unapply the committed configuration: the settings
			// (primary_conninfo, standby.signal, etc.) remain on disk and will be in effect
			// when postgres restarts. Only Applied changes.
			if n.postgresStatus != PostgresStopped {
				n.postgresStatus = PostgresStopped
				changed = true
			}
		}
	}

	// If we have committed-but-unapplied state and postgres is now running, try to
	// apply this tick. In production this corresponds to the startup sequence:
	// postgres has restarted with the previous on-disk configuration, and the pooler
	// now completes the role change (e.g. verifies streaming replication is active).
	// The applyFailRate allows chaos tests to simulate a flaky or slow apply.
	if !n.committed.Applied && n.postgresStatus == PostgresRunning && n.shouldApplyThisTick() {
		updated := n.committed
		updated.Applied = true
		if err := n.storage.Save(updated); err == nil {
			n.committed = updated
			changed = true
		}
	}

	if changed || n.needsStatusBroadcast {
		n.needsStatusBroadcast = false
		requests = append(requests, n.statusUpdate())
	}

	return requests
}

// shouldApplyThisTick returns true when the pooler should attempt to apply its
// committed state this tick. With applyFailRate == 0 (default) it always returns
// true; otherwise the rng determines whether the attempt succeeds.
func (n *PoolerNode) shouldApplyThisTick() bool {
	if n.applyFailRate == 0 || n.rng == nil {
		return true
	}
	return n.rng.Float64() >= n.applyFailRate
}

func (n *PoolerNode) handleOrchState(ind OrchStateIndicator) ([]Request, bool) {
	state := ind.State

	// Reject if the proposal is stale (term is behind what we've already voted for).
	if state.VotingTerm < n.committed.VotedTerm {
		return []Request{PoolerResponseRequest{
			ToOrch:       ind.FromOrch,
			Accepted:     false,
			KnownTerm:    n.committed.VotedTerm,
			KnownCoordID: n.committed.VotedCoord,
		}}, false
	}

	// Reject if same term but a different coordinator already won it (safety invariant).
	if state.VotingTerm == n.committed.VotedTerm &&
		n.committed.VotedCoord != "" &&
		state.CoordID != n.committed.VotedCoord {
		return []Request{PoolerResponseRequest{
			ToOrch:       ind.FromOrch,
			Accepted:     false,
			KnownTerm:    n.committed.VotedTerm,
			KnownCoordID: n.committed.VotedCoord,
		}}, false
	}

	// Reject if the orch's expected primary term does not match ours (stale cluster view).
	if ind.ExpectedPrimaryTerm > 0 && ind.ExpectedPrimaryTerm != n.committed.PrimaryTerm {
		return []Request{PoolerResponseRequest{
			ToOrch:    ind.FromOrch,
			Accepted:  false,
			KnownTerm: n.committed.VotedTerm,
		}}, false
	}

	// Determine this pooler's new role.
	newRole := RoleReplica
	if state.Primary == n.id {
		newRole = RolePrimary
	}

	// Applied tracks whether the role change has been operationally executed on this
	// node. All replication role changes require postgres to be running; a stopped
	// node commits the proposal (so it is durably recorded) but cannot apply it until
	// postgres restarts and receives the configuration.
	//
	// Applied is persisted as part of the committed state so it survives crashes.
	// Once Applied=true is written to disk it is never silently reverted: a subsequent
	// proposal may produce a new committed state with its own Applied value, but the
	// current state's Applied field is immutable after the Save call below.
	applied := n.postgresStatus == PostgresRunning

	newState := PoolerPersistentState{
		VotedTerm:    state.VotingTerm,
		VotedSeqNum:  state.SeqNum,
		VotedCoord:   state.CoordID,
		PrimaryTerm:  state.PrimaryTerm,
		Primary:      state.Primary,
		Role:         newRole,
		SyncReplicas: state.SyncReplicas,
		Applied:      applied,
	}

	// Persist before responding. If storage fails, do not respond.
	if err := n.storage.Save(newState); err != nil {
		return nil, false
	}
	n.committed = newState

	return []Request{
		PoolerResponseRequest{ToOrch: ind.FromOrch, Accepted: true},
	}, true
}

func (n *PoolerNode) statusUpdate() PoolerStatusUpdateRequest {
	return PoolerStatusUpdateRequest{
		Applied:        n.committed.Applied,
		PostgresStatus: n.postgresStatus,
		State:          n.committed,
	}
}
