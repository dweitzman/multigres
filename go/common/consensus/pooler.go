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
	pendingApply         *DurabilityRules
	pendingCorrelationID string // correlation ID from the WritePolicyIndicator
}

// NewPoolerNode creates a pooler node with the given identity, storage, and
// static node properties. It calls storage.Load() to restore any previously
// committed state.
func NewPoolerNode(id NodeID, storage PoolerStorage, properties NodeProperties) *PoolerNode {
	n := &PoolerNode{
		id:             id,
		storage:        storage,
		properties:     properties,
		pgStatus:       PostgresRunning,
		needsBroadcast: true, // announce state on first Step so coordinators learn it
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
	if state, err := n.storage.Load(); err == nil {
		n.committed = state
	}
	n.needsBroadcast = true
}

// Step processes all indicators that arrived this tick and returns requests.
// Persistence via storage.Save is called synchronously before any response is emitted.
func (n *PoolerNode) Step(_ int64, indicators []Indicator) []Request {
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
		case TerminateIndicator:
			if n.pgStatus != PostgresStopped {
				n.pgStatus = PostgresStopped
				changed = true
			}
		}
	}

	if changed || n.needsBroadcast {
		n.needsBroadcast = false
		requests = append(requests, n.statusUpdate())
	}

	return requests
}

// handleWritePolicy processes a WritePolicyIndicator from a coord.
// Only primaries accept direct writes; replicas receive rules changes via WAL.
// Validates the CAS (Rules.Seq must equal committed.PolicySeq() + 1).
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
		return reject()
	}
	// Reject if a write is already in flight (serialise writes).
	if n.pendingApply != nil {
		return reject()
	}
	// CAS: incoming rules must be exactly the next seq.
	if ind.Rules.Seq != n.committed.PolicySeq()+1 {
		return reject()
	}
	// Reject if the proposed policy cannot be satisfied by the proposed members.
	if ind.Rules.Policy != nil && !ind.Rules.Policy.IsAchievable(ind.Rules.Members) {
		return reject()
	}

	rules := ind.Rules
	n.pendingApply = &rules
	n.pendingCorrelationID = ind.CorrelationID

	return []Request{PolicyRecordApplyRequest{Rules: rules}}, false
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
		if n.pendingApply == nil || n.pendingApply.Seq != ind.Rules.Seq {
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
	if n.pendingApply != nil && n.pendingApply.Seq == ind.Rules.Seq {
		extraReqs = []Request{WritePolicyResponseRequest{
			RequestCorrelation: RequestCorrelation{CorrelationID: n.pendingCorrelationID},
			Accepted:           true,
		}}
		n.pendingApply = nil
		n.pendingCorrelationID = ""
	}

	newState := n.committed
	newState.Rules = &ind.Rules
	if err := n.storage.Save(newState); err != nil {
		return nil, false
	}
	n.committed = newState

	return extraReqs, true
}

func (n *PoolerNode) statusUpdate() PoolerStatusUpdateRequest {
	return PoolerStatusUpdateRequest{
		State:          n.committed,
		PostgresStatus: n.pgStatus,
		Properties:     n.properties,
	}
}
