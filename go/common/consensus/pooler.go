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

// PoolerNode is the pooler state machine.
//
// # Periodic heartbeat
//
// When heartbeatInterval > 0, the node emits a PoolerStatusUpdateRequest at
// least once every heartbeatInterval ticks even if its state has not changed.
// This lets coordinators re-learn cluster state after a crash without waiting
// for a state-changing event.
//
// # Normal path — WAL-driven rules changes
//
// Primary: PoolerNode receives WritePolicyIndicator, validates the CAS
// (Rules.Seq == committed.PolicySeq() + 1), and emits PolicyRecordApplyRequest
// to the local postgres driver. The driver updates synchronous_standby_names
// and commits the SQL transaction; on completion it delivers
// ApplyRulesResponseIndicator back to the PoolerNode. The PoolerNode then
// persists the new rules and responds to the coordinator.
//
// Replica: the local WAL watcher detects the rules record in the WAL stream
// and delivers ApplyRulesResponseIndicator to the PoolerNode. The PoolerNode
// persists the updated rules and broadcasts a status update.
//
// # Crash recovery
//
// On crash-restart, Restart() clears all in-memory state (including any
// pending apply) and reloads committed state from durable storage — exactly
// the production startup sequence. A pending in-flight apply is abandoned;
// the coordinator will retry.
type PoolerNode struct {
	id         NodeID
	storage    PoolerStorage
	properties NodeProperties

	// committed is the last state successfully persisted via storage.Save.
	committed PoolerPersistentState

	// pgStatus is the operational state of the postgres instance. Ephemeral:
	// starts as Running, set to Stopped on TerminateIndicator, reset by Restart().
	pgStatus PostgresStatus

	// needsBroadcast is set when state changes so the node emits a status
	// update on the next Step(). Set by Restart() to signal to coordinators
	// that the node is back up.
	needsBroadcast bool

	// pendingApply tracks an in-flight PolicyRecordApplyRequest that has been
	// emitted to the local driver but not yet acknowledged. Only one apply may
	// be in flight at a time. On crash-restart this is cleared; the coordinator
	// will retry.
	pendingApply         *Term
	pendingCorrelationID string // correlation ID from the WritePolicyIndicator

	// pendingRecruitCorrelationID is set when a RecruitIndicator has been accepted
	// and a RevokeParticipationRequest has been sent to the sidecar but the response
	// has not yet been received. Cleared once the sidecar responds.
	pendingRecruitCorrelationID string

	// heartbeatInterval is the maximum number of ticks that may elapse between
	// status broadcasts. Zero disables periodic heartbeats.
	heartbeatInterval int64

	// lastBroadcastTick is the tick at which the most recent status update was sent.
	lastBroadcastTick int64
}

// NewPoolerNode creates a pooler node with the given identity, storage, and
// static node properties. It calls storage.Load() to restore any previously
// committed state.
func NewPoolerNode(id NodeID, storage PoolerStorage, properties NodeProperties) *PoolerNode {
	n := &PoolerNode{
		id:                id,
		storage:           storage,
		properties:        properties,
		pgStatus:          PostgresRunning,
		needsBroadcast:    true, // announce state on first Step so coordinators learn it
		heartbeatInterval: 300,
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

// SetHeartbeatInterval configures a periodic status broadcast: the node will
// emit a PoolerStatusUpdateRequest at least once every ticks ticks, even when
// its state has not changed. Zero disables periodic heartbeats (the default).
func (n *PoolerNode) SetHeartbeatInterval(ticks int64) {
	n.heartbeatInterval = ticks
}

// CommittedState returns the current committed state.
func (n *PoolerNode) CommittedState() PoolerPersistentState {
	return n.committed
}

// PostgresStatus returns the current postgres operational status.
func (n *PoolerNode) PostgresStatus() PostgresStatus {
	return n.pgStatus
}

// IsActivePrimary reports whether this pooler is operating as primary with
// postgres running.
func (n *PoolerNode) IsActivePrimary() bool {
	return n.committed.Role == RolePrimary && n.pgStatus == PostgresRunning
}

// Restart simulates a crash-restart: clears all in-memory state (including any
// pending apply) and restores the last committed state from durable storage.
// Sets needsBroadcast so the node signals to coordinators that it is back up.
func (n *PoolerNode) Restart() {
	n.committed = PoolerPersistentState{}
	n.pgStatus = PostgresRunning
	n.pendingApply = nil
	n.pendingCorrelationID = ""
	n.pendingRecruitCorrelationID = ""
	if state, err := n.storage.Load(); err == nil {
		n.committed = state
	}
	n.needsBroadcast = true
}

// Step processes all indicators that arrived this tick and returns requests.
// Persistence via storage.Save is called synchronously before any response is emitted.
func (n *PoolerNode) Step(tick int64, indicators []Indicator) []Request {
	var requests []Request
	changed := false

	for _, ind := range indicators {
		switch v := ind.(type) {
		case WritePolicyIndicator:
			reqs, c := n.handleWritePolicy(v)
			requests = append(requests, reqs...)
			changed = changed || c
		case ApplyRulesResponseIndicator:
			reqs, c := n.handleApplyResponse(v)
			requests = append(requests, reqs...)
			changed = changed || c
		case RecruitIndicator:
			reqs, c := n.handleRecruit(v)
			requests = append(requests, reqs...)
			changed = changed || c
		case RevokeParticipationResponseIndicator:
			requests = append(requests, n.handleRevokeParticipationResponse(v)...)
		case WriteShadowWALIndicator:
			reqs, c := n.handleWriteShadowWAL(v)
			requests = append(requests, reqs...)
			changed = changed || c
		case TerminateIndicator:
			if n.pgStatus != PostgresStopped {
				n.pgStatus = PostgresStopped
				changed = true
			}
		}
	}

	heartbeat := n.heartbeatInterval > 0 && tick-n.lastBroadcastTick >= n.heartbeatInterval
	if changed || n.needsBroadcast || heartbeat {
		n.needsBroadcast = false
		n.lastBroadcastTick = tick
		requests = append(requests, n.statusUpdate())
	}

	return requests
}

// handleWritePolicy processes a WritePolicyIndicator from a coord.
// Only primaries accept direct writes; replicas receive term changes via WAL.
// Validates the CAS (Term.Seq must equal committed.PolicySeq() + 1).
// On success, emits PolicyRecordApplyRequest to the local postgres driver.
func (n *PoolerNode) handleWritePolicy(ind WritePolicyIndicator) ([]Request, bool) {
	reject := func() ([]Request, bool) {
		return []Request{WritePolicyResponseRequest{
			RequestCorrelation: RequestCorrelation{CorrelationID: ind.CorrelationID},
			Accepted:           false,
			CurrentSeq:         n.committed.PolicySeq(),
		}}, false
	}

	if n.committed.Role != RolePrimary {
		// Emergency failover: a recruited node may accept a write that falls
		// within its committed authority range.
		if n.committed.Commitment == nil || !n.committed.Commitment.AllowsTermChange(ind.Term.Seq) {
			return reject()
		}
	}
	// Reject if a write is already in flight (serialise writes).
	if n.pendingApply != nil {
		return reject()
	}
	// CAS: incoming term must be exactly the next seq.
	if ind.Term.Seq != n.committed.PolicySeq()+1 {
		return reject()
	}
	// Reject if the proposed policy cannot be satisfied by the proposed members.
	if ind.Term.Policy != nil && !ind.Term.Policy.IsAchievable(ind.Term.Members) {
		return reject()
	}

	term := ind.Term
	n.pendingApply = &term
	n.pendingCorrelationID = ind.CorrelationID

	return []Request{PolicyRecordApplyRequest{Term: term}}, false
}

// handleApplyResponse processes an ApplyRulesResponseIndicator delivered by
// either the primary's local driver (after SQL commit) or the replica's WAL
// watcher (after WAL entry arrival). Persists the new rules and, for the
// primary, responds to the waiting coordinator.
//
// Accepted=false means the driver failed to commit the write (e.g. a
// compare-and-swap race in the multi-tick pipeline). The pending apply is
// cleared and a rejection response is sent back to the coordinator.
func (n *PoolerNode) handleApplyResponse(ind ApplyRulesResponseIndicator) ([]Request, bool) {
	if !ind.Accepted {
		if n.pendingApply == nil || n.pendingApply.Seq != ind.Term.Seq {
			return nil, false
		}
		reqs := []Request{WritePolicyResponseRequest{
			RequestCorrelation: RequestCorrelation{CorrelationID: n.pendingCorrelationID},
			Accepted:           false,
			CurrentSeq:         n.committed.PolicySeq(),
		}}
		n.pendingApply = nil
		n.pendingCorrelationID = ""
		return reqs, false
	}

	// Primary path: clear the pending apply and respond to the coordinator.
	var extraReqs []Request
	if n.pendingApply != nil && n.pendingApply.Seq == ind.Term.Seq {
		extraReqs = []Request{WritePolicyResponseRequest{
			RequestCorrelation: RequestCorrelation{CorrelationID: n.pendingCorrelationID},
			Accepted:           true,
		}}
		n.pendingApply = nil
		n.pendingCorrelationID = ""
	}

	newState := n.committed
	newState.CachedTerm = &ind.Term
	newState.Primary = ind.Term.Primary
	if ind.Term.Primary == n.id {
		newState.Role = RolePrimary
	} else {
		newState.Role = RoleReplica
	}
	// Clear the commitment once the authorised term has been applied: the
	// coordinator's write authority is consumed and the node resumes normal
	// quorum participation.
	// TODO: consider whether the outgoing coordinator could briefly retain
	// authority into the next term (until a new coordinator recruits); for now
	// we revoke on apply as the conservative safe choice.
	if newState.Commitment != nil && ind.Term.Seq >= newState.Commitment.ProposedSeq {
		newState.Commitment = nil
	}
	if err := n.storage.Save(newState); err != nil {
		return nil, false
	}
	n.committed = newState

	return extraReqs, true
}

// handleRecruit processes a RecruitIndicator from a coordinator.
//
// Acceptance criteria — reject unless ALL of the following hold:
//  1. ind.AtTermSeq >= committed.PolicySeq(): coordinator knows the current term.
//  2. ind.ProposedSeq > ind.AtTermSeq: proposal advances the term.
//  3. No commitment exists, OR the proposal is accepted via one of:
//     a. existing == proposed (same coordinator, same seqs) — idempotent fast path.
//     b. existing.IsRevokedBy(proposed) — higher AtTermSeq or higher ProposedSeq.
//
// On the idempotent fast path the method responds immediately without a sidecar
// round-trip (the commitment is already persisted and the sidecar already revoked).
//
// On a new or superseding acceptance, the commitment is replaced, saved to
// storage, and a RevokeParticipationRequest is sent to the sidecar.
func (n *PoolerNode) handleRecruit(ind RecruitIndicator) ([]Request, bool) {
	reject := func() ([]Request, bool) {
		return []Request{RecruitResponseRequest{
			RequestCorrelation: RequestCorrelation{CorrelationID: ind.CorrelationID},
			Accepted:           false,
			Term:               n.committed.CachedTerm,
		}}, false
	}

	// Check 1: coordinator must know the current term (not stale).
	if ind.AtTermSeq < n.committed.PolicySeq() {
		return reject()
	}
	// Check 2: proposal must advance the term.
	if ind.ProposedSeq <= ind.AtTermSeq {
		return reject()
	}

	proposed := RecruitmentCommitment{
		CoordID:     ind.CoordID,
		AtTermSeq:   ind.AtTermSeq,
		ProposedSeq: ind.ProposedSeq,
	}
	existing := n.committed.Commitment
	switch {
	case existing == nil:
		// No existing commitment: accept.
	case *existing == proposed:
		// Same coordinator, same parameters: idempotent fast path.
		return []Request{RecruitResponseRequest{
			RequestCorrelation: RequestCorrelation{CorrelationID: ind.CorrelationID},
			Accepted:           true,
			Term:               n.committed.CachedTerm,
		}}, false
	case existing.IsRevokedBy(proposed):
		// Proposal has higher authority (lexicographic on AtTermSeq, ProposedSeq): supersede.
	default:
		return reject()
	}

	newState := n.committed
	newState.Commitment = &proposed
	if err := n.storage.Save(newState); err != nil {
		return reject()
	}
	n.committed = newState
	n.pendingRecruitCorrelationID = ind.CorrelationID

	return []Request{RevokeParticipationRequest{
		RequestCorrelation: RequestCorrelation{CorrelationID: ind.CorrelationID},
	}}, true
}

// handleWriteShadowWAL processes a WriteShadowWALIndicator from a coordinator.
// The node must hold a commitment that authorises Term.Seq and must have
// received real WAL up to at least ind.BaseLSN so the entry is placed at the
// correct position in the WAL history.
//
// The term is appended to shadow WAL for durability, and
// WriteShadowWALAckedRequest is returned so the coordinator knows the write
// succeeded. When ApplyNow is true the sidecar is also asked to apply GUC
// settings immediately via ApplyWALTermRequest, allowing promotion or replica
// reconnection without a separate round-trip.
func (n *PoolerNode) handleWriteShadowWAL(ind WriteShadowWALIndicator) ([]Request, bool) {
	if n.committed.Commitment == nil || !n.committed.Commitment.AllowsTermChange(ind.Term.Seq) {
		return nil, false
	}

	// Persist to shadow WAL before responding to the coordinator.
	newState := n.committed
	term := ind.Term
	newState.ShadowWAL = append(newState.ShadowWAL, &term)
	if err := n.storage.Save(newState); err != nil {
		return nil, false
	}
	n.committed = newState

	var reqs []Request
	// Acknowledge the shadow WAL write to the coordinator.
	if ind.CorrelationID != "" {
		reqs = append(reqs, WriteShadowWALAckedRequest{
			RequestCorrelation: RequestCorrelation{CorrelationID: ind.CorrelationID},
			Accepted:           true,
		})
	}
	// When ApplyNow is set, ask the sidecar to apply GUC settings immediately.
	if ind.ApplyNow {
		reqs = append(reqs, ApplyWALTermRequest{Term: ind.Term})
	}
	return reqs, true
}

// handleRevokeParticipationResponse processes the sidecar's result for stopping
// this node from participating in write quorum. Forwards the outcome to the
// coordinator as a RecruitResponseRequest.
func (n *PoolerNode) handleRevokeParticipationResponse(ind RevokeParticipationResponseIndicator) []Request {
	if ind.CorrelationID != n.pendingRecruitCorrelationID {
		return nil // stale or spurious
	}
	n.pendingRecruitCorrelationID = ""
	req := RecruitResponseRequest{
		RequestCorrelation: RequestCorrelation{CorrelationID: ind.CorrelationID},
		Accepted:           ind.Accepted,
		Term:               n.committed.CachedTerm,
	}
	if ind.Accepted {
		req.LSN = ind.LSN
	}
	return []Request{req}
}

func (n *PoolerNode) statusUpdate() PoolerStatusUpdateRequest {
	return PoolerStatusUpdateRequest{
		State:          n.committed,
		PostgresStatus: n.pgStatus,
		Properties:     n.properties,
	}
}
