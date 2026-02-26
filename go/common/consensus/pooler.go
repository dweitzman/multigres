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

// PoolerNode is the pooler state machine. It receives ConsensusState proposals from
// orch and votes to accept or reject them according to the safety invariant.
//
// State lifecycle: two distinct phases must both succeed for a change to take effect.
//
//  1. Commit (durable): the pooler writes the accepted ConsensusState to PoolerStorage
//     via a synchronous save before sending any response. This survives a crash.
//     Tracked in PoolerNode.committed (derived from the proposal). Always committed
//     with Applied=false; the apply step is always a separate operation.
//
//  2. Apply (operational): the pooler executes the role change — pg_ctl promote,
//     reconfigure recovery settings, update routing. Tracked by committed.Applied.
//     Applied requires postgres to be running. If postgres was stopped when a
//     proposal was committed, or if the RoleApplier reports a transient failure,
//     the pooler retries on each subsequent Step while postgres is running.
//
// Both committed state and applied status are persisted via storage.Save before
// being reported. PostgresStatus is ephemeral (not persisted). On a crash-restart,
// Restart() clears all in-memory state and reloads committed state from storage,
// faithfully simulating the production startup sequence.
type PoolerNode struct {
	id      NodeID
	storage PoolerStorage
	applier RoleApplier

	// committed is the last state successfully persisted via storage.Save.
	// committed.Applied records whether the role change has been executed on disk.
	committed PoolerPersistentState

	// postgresStatus is the operational state of the postgres instance.
	// Ephemeral: starts as Running, set to Stopped on TerminateIndicator, reset by Restart().
	postgresStatus PostgresStatus

	// needsStatusBroadcast is set by Restart() so the node emits a status update on
	// the next Step() even without incoming indicators (signals to orch it is back up).
	needsStatusBroadcast bool
}

// NewPoolerNode creates a new pooler node with the given identity, storage, and applier.
// It calls storage.Load() to restore any previously committed state — on first run with
// an empty storage this is a no-op (zero PoolerPersistentState). applier executes the
// operational role change (pg_ctl, postgresql.conf, etc.) on each tick where committed
// state is unapplied and postgres is running; pass nil to apply immediately every tick.
func NewPoolerNode(id NodeID, storage PoolerStorage, applier RoleApplier) *PoolerNode {
	n := &PoolerNode{
		id:             id,
		storage:        storage,
		applier:        applier,
		postgresStatus: PostgresRunning,
	}
	if state, err := storage.Load(); err == nil {
		n.committed = state
	}
	return n
}

// ID returns the pooler node's unique identifier.
func (n *PoolerNode) ID() NodeID {
	return n.id
}

// Restart is called by the simulator when simulating a crash-restart.
// It clears all in-memory state (as a real crash would) and restores the last
// committed state from durable storage — exactly the production startup sequence.
// PostgresStatus is reset to Running; needsStatusBroadcast is set so the node
// emits a status update on the next Step() to signal to orchs that it is back up.
func (n *PoolerNode) Restart() {
	n.committed = PoolerPersistentState{}
	if state, err := n.storage.Load(); err == nil {
		n.committed = state
	}
	n.postgresStatus = PostgresRunning
	n.needsStatusBroadcast = true
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

	// If we have committed-but-unapplied state and postgres is now running, ask the
	// RoleApplier to execute the role change. In production this corresponds to the
	// startup sequence: postgres has restarted with the previous on-disk configuration,
	// and the pooler now completes the role change (e.g. pg_ctl promote, updating
	// postgresql.conf). The applier may return false for transient failures; the
	// pooler retries on each subsequent tick until it succeeds.
	if !n.committed.Applied && n.postgresStatus == PostgresRunning && n.applyThisTick() {
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

// applyThisTick reports whether the role change should be applied this tick.
// With no applier (nil) it always returns true. Otherwise it delegates to the
// RoleApplier, which may return false to simulate a transient failure.
func (n *PoolerNode) applyThisTick() bool {
	if n.applier == nil {
		return true
	}
	return n.applier.Apply(n.committed)
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

	// Always commit with Applied=false. The apply step (pg_ctl, postgresql.conf, etc.)
	// is always a separate operation: the retry loop in Step() calls the RoleApplier
	// once postgres is running and the applier signals success.
	newState := PoolerPersistentState{
		VotedTerm:    state.VotingTerm,
		VotedSeqNum:  state.SeqNum,
		VotedCoord:   state.CoordID,
		PrimaryTerm:  state.PrimaryTerm,
		Primary:      state.Primary,
		Role:         newRole,
		SyncReplicas: state.SyncReplicas,
		Applied:      false,
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
