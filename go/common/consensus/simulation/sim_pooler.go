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

	"github.com/multigres/multigres/go/common/consensus"
)

// lsn is a Log Sequence Number, a monotonically increasing position in the WAL.
type lsn int64

// walEntry is one record in the simulated WAL buffer. Each entry either
// represents a user transaction (record == nil, just advances the LSN) or a
// DurabilityRules change.
//
// TODO(failover): research why replication is expected to fail in postgres if a
// replica and primary have diverging transactions (e.g. after a split-brain or
// a failed failover), and try to simulate something similar to what postgres
// does. In a graceful switchover the WAL timeline is compatible and replication
// continues from the new primary without gap. In emergency failover there may
// be timeline divergence that requires something like pg_rewind before the
// stale node can resume replication. For Stage 1 (cohort expansion, no
// failover) the current representation is sufficient.
//
// TODO(reclamation): implement WAL reclamation via periodic checkpoint entries.
// A checkpoint marks a position at which all effects can be safely discarded if
// all cohort members have applied past it. For now the buffer grows unboundedly.
type walEntry struct {
	pos    lsn
	record *consensus.DurabilityRules // nil = user transaction
}

// ruleChangePhase tracks which step of the multi-tick rules-apply pipeline the
// primary is currently executing. Splitting the pipeline across ticks models
// the non-atomic relationship between postgres GUC changes (synchronous_standby_names)
// and the SQL transaction that writes the rules record to the WAL.
type ruleChangePhase int

const (
	// ruleChangePhaseCombinedGUC: set synchronous_standby_names to the union of
	// old and new standbys and syncPolicy to bothPolicy, ensuring that both the
	// old and new durability guarantees must be satisfied before the write is
	// considered durable.
	ruleChangePhaseCombinedGUC ruleChangePhase = iota

	// ruleChangePhaseWriteWAL: re-validate the compare-and-swap and append the
	// rules record to the WAL. The CAS is re-checked because the combined-GUC
	// tick may have been interleaved with another write in a real system; if the
	// check fails the GUC is rolled back to the original settings.
	ruleChangePhaseWriteWAL

	// ruleChangePhaseAwaitQuorum: wait until writeQuorumMet returns true for the
	// WAL position of the rules record.
	ruleChangePhaseAwaitQuorum

	// ruleChangePhaseFinalGUC: set synchronous_standby_names to only the new
	// standbys and syncPolicy to the new policy.
	ruleChangePhaseFinalGUC

	// ruleChangePhaseSendIndicator: queue ApplyRulesResponseIndicator for
	// delivery to the wrapped PoolerNode on this tick, then clear pendingChange.
	ruleChangePhaseSendIndicator
)

// pendingRuleChange tracks an in-flight rules-apply pipeline on the primary.
type pendingRuleChange struct {
	rules consensus.DurabilityRules
	phase ruleChangePhase

	// walPos is set in the writeWAL phase once the record has been appended.
	walPos lsn

	// originalStandbys and originalPolicy are the GUC settings in effect
	// before the pipeline began. Saved so they can be restored if the CAS
	// fails in the writeWAL phase.
	originalStandbys []consensus.CohortMember
	originalPolicy   consensus.AckPolicy

	// combinedStandbys and combinedPolicy are used during the combinedGUC and
	// awaitQuorum phases: union of old+new standbys, conjunctive bothPolicy.
	combinedStandbys []consensus.CohortMember
	combinedPolicy   consensus.AckPolicy

	// finalStandbys and finalPolicy are applied in the finalGUC phase: the new
	// standbys and new policy from the incoming rules.
	finalStandbys []consensus.CohortMember
	finalPolicy   consensus.AckPolicy
}

// bothPolicy is a conjunctive AckPolicy used during cohort transitions.
// A write is considered durable only when the old policy is satisfied among
// the old standbys AND the new policy is satisfied among the new standbys.
// IsWriteQuorum filters the acking set through each policy's standby scope
// independently via memberIntersect.
type bothPolicy struct {
	oldPolicy   consensus.AckPolicy
	oldStandbys []consensus.CohortMember
	newPolicy   consensus.AckPolicy
	newStandbys []consensus.CohortMember
}

func (b *bothPolicy) IsWriteQuorum(acking []consensus.CohortMember) bool {
	return b.oldPolicy.IsWriteQuorum(memberIntersect(acking, b.oldStandbys)) &&
		b.newPolicy.IsWriteQuorum(memberIntersect(acking, b.newStandbys))
}

func (b *bothPolicy) IsAchievable(members []consensus.CohortMember) bool {
	return b.oldPolicy.IsAchievable(memberIntersect(members, b.oldStandbys)) &&
		b.newPolicy.IsAchievable(memberIntersect(members, b.newStandbys))
}

// IsRevoked checks each sub-policy against its own standby scope. Both must be
// independently revoked because a bothPolicy write requires both sub-quorums;
// a coordinator must cover enough members in each cohort to be certain no
// durable write slipped through on either side of the transition.
func (b *bothPolicy) IsRevoked(allMembers, recruited, leaders []consensus.CohortMember) bool {
	oldAll := memberIntersect(allMembers, b.oldStandbys)
	oldRecruited := memberIntersect(recruited, b.oldStandbys)
	oldLeaders := memberIntersect(leaders, b.oldStandbys)

	newAll := memberIntersect(allMembers, b.newStandbys)
	newRecruited := memberIntersect(recruited, b.newStandbys)
	newLeaders := memberIntersect(leaders, b.newStandbys)

	return b.oldPolicy.IsRevoked(oldAll, oldRecruited, oldLeaders) &&
		b.newPolicy.IsRevoked(newAll, newRecruited, newLeaders)
}

// SimPooler wraps a PoolerNode and acts as the local postgres driver in
// simulation. It models a postgres instance that can operate in two modes:
//
//   - Primary mode: maintains the authoritative WAL, tracks which replicas
//     have received each LSN, and enforces write quorum via simulated
//     synchronous_standby_names before considering a write durable.
//
//   - Replica mode: pulls WAL entries directly from its configured primary
//     each tick (analogous to postgres's primary_conninfo), tracks the
//     highest LSN received, and delivers ApplyRulesResponseIndicators to the
//     wrapped PoolerNode when rules records arrive.
//
// WAL replication is handled within Step() — each replica looks up its
// primary's SimPooler and reads new entries directly, mirroring how each
// postgres instance manages its own replication connection.
//
// SimPooler intercepts PolicyRecordApplyRequest before it reaches the
// RequestHandler, runs the multi-tick pipeline that models the non-atomic
// relationship between GUC changes and WAL writes, then queues
// ApplyRulesResponseIndicator once the write is durable.
type SimPooler struct {
	node *consensus.PoolerNode
	sim  *simType

	// wal is the WAL buffer. Primary appends entries here; replicas receive
	// entries via pullWAL. Each entry's pos is strictly increasing.
	wal []walEntry

	// nextPos is the next LSN to assign when appending a new entry (primary only).
	nextPos lsn

	// gucSyncStandbys is the simulated synchronous_standby_names GUC (primary
	// only). Write quorum is determined by checking how many of these nodes
	// have ACKed the write LSN using gucSyncPolicy.IsWriteQuorum.
	gucSyncStandbys []consensus.CohortMember
	gucSyncPolicy   consensus.AckPolicy // nil = AnyN(0), no ACKs required

	// gucWALReceiveEnabled is the simulated GUC that controls whether this
	// replica pulls WAL from the primary (analogous to recovery_target_action or
	// standby_mode). Set to false when the sidecar revokes WAL receive; restored
	// when new rules are applied. Ignored for primaries (which never pull WAL).
	gucWALReceiveEnabled bool

	// replicaACK tracks the highest WAL position each replica has confirmed
	// receiving (primary only). Updated by pullWAL calls from replicas.
	replicaACK map[consensus.NodeID]lsn

	// pendingChange is the active rules-apply pipeline (primary only). At most
	// one pipeline can be in flight at a time; PoolerNode serialises writes.
	pendingChange *pendingRuleChange

	// receivedLSN is the highest WAL position this node has received from the
	// primary (replica only). On graceful switchover the WAL timeline is
	// compatible so this position remains valid against the new primary.
	receivedLSN lsn

	// primaryConnInfo is the primary this node is currently configured to
	// replicate from, analogous to postgres's primary_conninfo setting. Tracked
	// to detect when the replication target changes.
	primaryConnInfo consensus.NodeID

	// pendingWAL holds entries received from the primary that have not yet
	// been processed in a Step call.
	pendingWAL []walEntry

	// queuedIndicators holds indicators to deliver to the wrapped PoolerNode on
	// the next Step call. Used both internally (e.g. ApplyRulesResponseIndicator
	// once a write is durable) and externally via EnqueueIndicator.
	queuedIndicators []consensus.Indicator

	// responseCallbacks maps a correlation ID to a callback invoked once when
	// the matching WritePolicyResponseRequest is emitted. Entries are removed
	// after the callback fires.
	responseCallbacks map[string]func(consensus.WritePolicyResponseRequest)

	// recruitResponseCallbacks maps a correlation ID to a callback invoked once
	// when the matching RecruitResponseRequest is emitted. Entries are removed
	// after the callback fires.
	recruitResponseCallbacks map[string]func(consensus.RecruitResponseRequest)

	// pendingRevokeCorrelationID is set when a RevokeParticipationRequest has
	// been received but the sidecar has not yet completed revoking. On the next
	// advancePendingRevoke call the revocation completes and
	// RevokeParticipationResponseIndicator is queued.
	pendingRevokeCorrelationID string
}

// NewSimPooler creates a SimPooler wrapping the given PoolerNode. sim is the
// simulator used to look up peer SimPoolers for WAL replication each tick.
func NewSimPooler(node *consensus.PoolerNode, sim *simType) *SimPooler {
	return &SimPooler{
		node:                     node,
		sim:                      sim,
		replicaACK:               make(map[consensus.NodeID]lsn),
		responseCallbacks:        make(map[string]func(consensus.WritePolicyResponseRequest)),
		recruitResponseCallbacks: make(map[string]func(consensus.RecruitResponseRequest)),
		gucWALReceiveEnabled:     node.CommittedState().Commitment == nil,
	}
}

// isRevoked returns true when the sidecar has revoked this node's participation
// in write quorum. Derived from gucWALReceiveEnabled: the sidecar sets it false
// for both replicas (stop ACKing) and primaries (read-only mode) on revocation,
// and restores it to true when new rules are applied.
func (s *SimPooler) isRevoked() bool {
	return !s.gucWALReceiveEnabled
}

// Node returns the wrapped PoolerNode.
func (s *SimPooler) Node() *consensus.PoolerNode {
	return s.node
}

// ID returns the node's identifier.
func (s *SimPooler) ID() consensus.NodeID {
	return s.node.ID()
}

// AppendUserTx simulates a user (non-rules) transaction on the primary,
// advancing the WAL LSN. Useful in tests for verifying replica lag tracking
// or creating WAL gaps between rules entries.
func (s *SimPooler) AppendUserTx() {
	s.nextPos++
	s.wal = append(s.wal, walEntry{pos: s.nextPos})
}

// SyncStandbys returns the current simulated synchronous_standby_names set.
// Useful in tests for asserting that sync settings are updated correctly.
func (s *SimPooler) SyncStandbys() []consensus.CohortMember {
	result := make([]consensus.CohortMember, len(s.gucSyncStandbys))
	copy(result, s.gucSyncStandbys)
	return result
}

// EnqueueIndicator queues an indicator to be delivered to the wrapped
// PoolerNode on its next Step() call. Use this in tests to inject explicit
// events (e.g. WritePolicyIndicator) between RequireWithinTicks calls.
func (s *SimPooler) EnqueueIndicator(ind consensus.Indicator) {
	s.queuedIndicators = append(s.queuedIndicators, ind)
}

// SendWritePolicyIndicator queues a WritePolicyIndicator and registers a
// callback to be invoked once the correlated WritePolicyResponseRequest is
// emitted by the wrapped PoolerNode. The callback fires exactly once and is
// removed from the registry after firing. The correlation ID in ind must be
// non-empty for the callback to be registered.
func (s *SimPooler) SendWritePolicyIndicator(ind consensus.WritePolicyIndicator, callback func(consensus.WritePolicyResponseRequest)) {
	s.queuedIndicators = append(s.queuedIndicators, ind)
	if ind.CorrelationID != "" && callback != nil {
		s.responseCallbacks[ind.CorrelationID] = callback
	}
}

// SendRecruitIndicator queues a RecruitIndicator and registers a callback to
// be invoked once the correlated RecruitResponseRequest is emitted by the
// wrapped PoolerNode. The callback fires exactly once.
func (s *SimPooler) SendRecruitIndicator(ind consensus.RecruitIndicator, callback func(consensus.RecruitResponseRequest)) {
	s.queuedIndicators = append(s.queuedIndicators, ind)
	if ind.CorrelationID != "" && callback != nil {
		s.recruitResponseCallbacks[ind.CorrelationID] = callback
	}
}

// applyRevokedGUC simulates the postgres GUC changes that take effect when
// this node stops participating in write quorum. Sets gucWALReceiveEnabled=false
// (making isRevoked() return true). Role-specific behaviour:
//
// Primary: clears sync standbys/policy and drops any pending WAL entries that
// did not reach write quorum. This mirrors postgres crash-recovery: WAL that
// never received sufficient replica ACKs is truncated, and any in-flight write
// pipeline is aborted with ApplyRulesResponseIndicator{Accepted:false}.
//
// Replica: stops pulling WAL by clearing primaryConnInfo and gucWALReceiveEnabled.
// The primary is left stuck in awaitQuorum if it was waiting for this replica's ACK.
func (s *SimPooler) applyRevokedGUC() {
	if s.node.CommittedState().Role == consensus.RolePrimary {
		s.gucSyncStandbys = nil
		s.gucSyncPolicy = nil
		if s.pendingChange != nil {
			// Truncate WAL entries that were appended but never reached quorum.
			if s.pendingChange.walPos > 0 {
				s.truncateWALAfter(s.pendingChange.walPos - 1)
			}
			s.queuedIndicators = append(s.queuedIndicators, consensus.ApplyRulesResponseIndicator{
				Rules:    s.pendingChange.rules,
				Accepted: false,
			})
			s.pendingChange = nil
		}
	}
	s.gucWALReceiveEnabled = false
	s.primaryConnInfo = ""
}

// truncateWALAfter removes all WAL entries with position > after and resets
// nextPos to after. This simulates crash recovery rolling back uncommitted writes.
func (s *SimPooler) truncateWALAfter(after lsn) {
	n := 0
	for _, e := range s.wal {
		if e.pos <= after {
			s.wal[n] = e
			n++
		}
	}
	s.wal = s.wal[:n]
	if s.nextPos > after {
		s.nextPos = after
	}
}

// advancePendingRevoke completes a sidecar revocation that was initiated on a
// prior tick. Applies revoked GUC settings and queues
// RevokeParticipationResponseIndicator for the wrapped PoolerNode. This models
// the at-least-one-tick latency of the real sidecar stopping ACKs (for a
// replica) or switching to read-only mode (for a primary).
func (s *SimPooler) advancePendingRevoke() {
	if s.pendingRevokeCorrelationID == "" {
		return
	}
	correlationID := s.pendingRevokeCorrelationID
	s.pendingRevokeCorrelationID = ""
	s.applyRevokedGUC()
	s.queuedIndicators = append(s.queuedIndicators, consensus.RevokeParticipationResponseIndicator{
		CorrelationID: correlationID,
		Accepted:      true,
	})
}

// Step processes indicators and advances the SimPooler state machine one tick.
//
// The method:
//  1. Completes any pending sidecar revocation (from a prior tick).
//  2. Pulls new WAL from the configured primary if this node is a replica.
//     Doing this after revocation means a newly-revoked node skips ACKs.
//  3. Advances the pending rules-apply pipeline (primary path).
//  4. Calls PoolerNode.Step with all accumulated indicators.
//  5. Intercepts PolicyRecordApplyRequest and RevokeParticipationRequest from output.
//
// Returns the subset of requests to pass to the RequestHandler.
func (s *SimPooler) Step(tick int64, externalInds []consensus.Indicator) []consensus.Request {
	// Complete any in-flight sidecar revocation before pulling WAL so that a
	// newly-revoked node skips ACKing on this tick.
	s.advancePendingRevoke()

	// Replica path: pull WAL from the configured primary.
	s.pullWAL()

	// Primary path: advance the rules-apply pipeline. This may append to
	// queuedIndicators (e.g. ApplyRulesResponseIndicator in the sendIndicator
	// phase), which are then drained into inds below so PoolerNode sees them
	// in the same tick.
	s.advancePendingChange()

	inds := make([]consensus.Indicator, 0, len(s.queuedIndicators)+len(externalInds)+len(s.pendingWAL))
	inds = append(inds, s.queuedIndicators...)
	inds = append(inds, externalInds...)
	s.queuedIndicators = nil

	// Replica path: process WAL entries received from the primary.
	for _, e := range s.pendingWAL {
		if e.record != nil {
			inds = append(inds, consensus.ApplyRulesResponseIndicator{
				Rules:    *e.record,
				Accepted: true,
			})
		}
	}
	s.pendingWAL = nil

	// Step the wrapped PoolerNode with all accumulated indicators.
	reqs := s.node.Step(tick, inds)

	// Intercept PolicyRecordApplyRequest and RevokeParticipationRequest before
	// forwarding to the RequestHandler. Invoke registered response callbacks.
	var forwarded []consensus.Request
	for _, req := range reqs {
		switch r := req.(type) {
		case consensus.PolicyRecordApplyRequest:
			s.handleApply(r)
		case consensus.RevokeParticipationRequest:
			// Sidecar intercept: start revocation. Will complete on the next tick
			// via advancePendingRevoke, modelling at-least-one-tick sidecar latency.
			s.pendingRevokeCorrelationID = r.CorrelationID
		case consensus.WritePolicyResponseRequest:
			if cb := s.responseCallbacks[r.CorrelationID]; cb != nil {
				delete(s.responseCallbacks, r.CorrelationID)
				cb(r)
			}
			forwarded = append(forwarded, req)
		case consensus.RecruitResponseRequest:
			if cb := s.recruitResponseCallbacks[r.CorrelationID]; cb != nil {
				delete(s.recruitResponseCallbacks, r.CorrelationID)
				cb(r)
			}
			forwarded = append(forwarded, req)
		default:
			forwarded = append(forwarded, req)
		}
	}
	return forwarded
}

// pullWAL simulates WAL streaming replication. If this SimPooler is a replica
// with a configured primary, it pulls new WAL entries from the primary's buffer
// and updates the primary's replica ACK tracking.
//
// This mirrors how each postgres replica maintains its own replication
// connection to the primary (via primary_conninfo), rather than relying on a
// separate replication mediator.
//
// On graceful switchover the WAL timeline remains compatible and receivedLSN is
// preserved — the replica resumes from where it left off against the new
// primary. Emergency failover with timeline divergence may require additional
// handling (see walEntry TODO).
func (s *SimPooler) pullWAL() {
	state := s.node.CommittedState()
	if state.Role != consensus.RoleReplica || state.Primary == "" {
		return
	}
	if !s.gucWALReceiveEnabled {
		return // sidecar has revoked WAL receive; replica is disconnected
	}

	// Track primary_conninfo changes so we can detect reconnections.
	s.primaryConnInfo = state.Primary

	primary := s.findSimPooler(s.primaryConnInfo)
	if primary == nil || primary.Node().CommittedState().Role != consensus.RolePrimary {
		return
	}

	entries := primary.walEntriesSince(s.receivedLSN)
	s.receiveWAL(entries)
	primary.ackLSN(s.ID(), s.receivedLSN)
}

// findSimPooler looks up a SimPooler by ID from the simulator's registered nodes.
func (s *SimPooler) findSimPooler(id consensus.NodeID) *SimPooler {
	for _, node := range s.sim.Nodes() {
		if sp, ok := node.(*SimPooler); ok && sp.ID() == id {
			return sp
		}
	}
	return nil
}

// handleApply is called when the PoolerNode emits a PolicyRecordApplyRequest,
// simulating the postgres driver receiving the instruction to apply new rules.
// It validates the CAS, computes the transition GUC settings, and initialises
// the multi-tick pipeline in the combinedGUC phase.
//
// The pipeline models the non-atomic relationship between the GUC update
// (synchronous_standby_names) and the SQL transaction that writes the rules
// record to the WAL:
//
//  1. combinedGUC: set standbys to union(old, new) and policy to bothPolicy.
//  2. writeWAL:    re-validate CAS; on failure roll back GUC and abort.
//  3. awaitQuorum: wait for writeQuorumMet at the rules record's WAL position.
//  4. finalGUC:    set standbys and policy to the new values.
//  5. sendIndicator: queue ApplyRulesResponseIndicator for PoolerNode.
func (s *SimPooler) handleApply(req consensus.PolicyRecordApplyRequest) {
	// Replicas do not maintain a writable WAL; reject the apply immediately.
	if s.node.CommittedState().Role != consensus.RolePrimary {
		s.queuedIndicators = append(s.queuedIndicators, consensus.ApplyRulesResponseIndicator{
			Rules:    req.Rules,
			Accepted: false,
		})
		return
	}
	// A revoked primary has been placed in read-only mode by the sidecar and
	// cannot commit new WAL entries.
	if s.isRevoked() {
		s.queuedIndicators = append(s.queuedIndicators, consensus.ApplyRulesResponseIndicator{
			Rules:    req.Rules,
			Accepted: false,
		})
		return
	}

	// Early CAS check: validates that this SimPooler's WAL is consistent with
	// the PoolerNode's committed state at the time of the request. A failure
	// here indicates a bug in the state machine, not a normal race condition.
	currentSeq := s.latestWALPolicySeq()
	if req.Rules.Seq != currentSeq+1 {
		panic(fmt.Sprintf("SimPooler CAS mismatch on %s: Rules.Seq=%d, expected %d",
			s.node.ID(), req.Rules.Seq, currentSeq+1))
	}

	rules := req.Rules

	// Capture original GUC settings for potential rollback in writeWAL.
	originalStandbys := make([]consensus.CohortMember, len(s.gucSyncStandbys))
	copy(originalStandbys, s.gucSyncStandbys)
	originalPolicy := s.gucSyncPolicy

	// Compute final GUC: all new cohort members except this node (the primary).
	var finalStandbys []consensus.CohortMember
	for _, m := range rules.Members {
		if m.ID != s.node.ID() {
			finalStandbys = append(finalStandbys, m)
		}
	}
	finalPolicy := rules.Policy

	// Compute combined GUC: union of old+new standbys.
	combinedStandbys := unionMembers(originalStandbys, finalStandbys)

	// Compute combined policy: must satisfy BOTH old and new simultaneously.
	// If there is no old policy (nil = AnyN(0)), the combined policy just needs
	// to satisfy the new policy since the old quorum requirement is zero.
	var combinedPolicy consensus.AckPolicy
	if originalPolicy == nil {
		combinedPolicy = finalPolicy
	} else {
		combinedPolicy = &bothPolicy{
			oldPolicy:   originalPolicy,
			oldStandbys: originalStandbys,
			newPolicy:   finalPolicy,
			newStandbys: finalStandbys,
		}
	}

	s.pendingChange = &pendingRuleChange{
		rules:            rules,
		phase:            ruleChangePhaseCombinedGUC,
		originalStandbys: originalStandbys,
		originalPolicy:   originalPolicy,
		combinedStandbys: combinedStandbys,
		combinedPolicy:   combinedPolicy,
		finalStandbys:    finalStandbys,
		finalPolicy:      finalPolicy,
	}
}

// advancePendingChange advances the rules-apply pipeline by one phase per tick.
// It is called at the start of Step, before draining queuedIndicators, so that
// ApplyRulesResponseIndicator queued in the sendIndicator phase is delivered
// to the wrapped PoolerNode within the same tick.
func (s *SimPooler) advancePendingChange() {
	if s.pendingChange == nil {
		return
	}
	c := s.pendingChange
	switch c.phase {
	case ruleChangePhaseCombinedGUC:
		// Apply combined GUC: union of old+new standbys, bothPolicy.
		s.gucSyncStandbys = c.combinedStandbys
		s.gucSyncPolicy = c.combinedPolicy
		c.phase = ruleChangePhaseWriteWAL

	case ruleChangePhaseWriteWAL:
		// Re-validate CAS. The combined-GUC phase consumed a tick; in a real
		// system another write could have landed in the WAL during that time.
		if s.latestWALPolicySeq() != c.rules.Seq-1 {
			// CAS failed: restore original GUC settings and report failure.
			s.gucSyncStandbys = c.originalStandbys
			s.gucSyncPolicy = c.originalPolicy
			s.queuedIndicators = append(s.queuedIndicators, consensus.ApplyRulesResponseIndicator{
				Rules:    c.rules,
				Accepted: false,
			})
			s.pendingChange = nil
			return
		}
		// Append the rules record to the WAL.
		s.nextPos++
		rules := c.rules
		s.wal = append(s.wal, walEntry{pos: s.nextPos, record: &rules})
		c.walPos = s.nextPos
		c.phase = ruleChangePhaseAwaitQuorum

	case ruleChangePhaseAwaitQuorum:
		if s.writeQuorumMet(c.walPos) {
			c.phase = ruleChangePhaseFinalGUC
		}

	case ruleChangePhaseFinalGUC:
		// Apply final GUC: only the new standbys and new policy.
		// Restore gucWALReceiveEnabled so isRevoked() returns false — new rules
		// supersede any prior commitment range and the node resumes participation.
		s.gucSyncStandbys = c.finalStandbys
		s.gucSyncPolicy = c.finalPolicy
		s.gucWALReceiveEnabled = true
		c.phase = ruleChangePhaseSendIndicator

	case ruleChangePhaseSendIndicator:
		// Queue the indicator so PoolerNode persists and responds in this tick.
		s.queuedIndicators = append(s.queuedIndicators, consensus.ApplyRulesResponseIndicator{
			Rules:    c.rules,
			Accepted: true,
		})
		s.pendingChange = nil
	}
}

// writeQuorumMet returns true if the current sync settings are satisfied for
// a write at the given WAL position. A nil syncPolicy (no policy applied yet)
// is treated as AnyN(0): no ACKs required.
func (s *SimPooler) writeQuorumMet(pos lsn) bool {
	if s.gucSyncPolicy == nil {
		return true
	}
	var acking []consensus.CohortMember
	for _, standby := range s.gucSyncStandbys {
		if s.replicaACK[standby.ID] >= pos {
			acking = append(acking, standby)
		}
	}
	return s.gucSyncPolicy.IsWriteQuorum(acking)
}

// latestWALPolicySeq returns the Seq of the most recently appended rules record
// in this node's WAL. If the WAL contains no rules entries yet, it falls back
// to the PoolerNode's committed state (handles pre-initialized nodes whose WAL
// buffer is empty but committed state was loaded from storage).
func (s *SimPooler) latestWALPolicySeq() int64 {
	for i := len(s.wal) - 1; i >= 0; i-- {
		if s.wal[i].record != nil {
			return s.wal[i].record.Seq
		}
	}
	return s.node.CommittedState().PolicySeq()
}

// walEntriesSince returns all WAL entries with position > after.
func (s *SimPooler) walEntriesSince(after lsn) []walEntry {
	var result []walEntry
	for _, e := range s.wal {
		if e.pos > after {
			result = append(result, e)
		}
	}
	return result
}

// ackLSN records that a replica has received WAL up to pos (primary only).
func (s *SimPooler) ackLSN(replicaID consensus.NodeID, pos lsn) {
	if pos > s.replicaACK[replicaID] {
		s.replicaACK[replicaID] = pos
	}
}

// receiveWAL accepts WAL entries from the primary. Only entries with
// pos > receivedLSN are accepted to prevent duplicate processing.
func (s *SimPooler) receiveWAL(entries []walEntry) {
	for _, e := range entries {
		if e.pos > s.receivedLSN {
			s.receivedLSN = e.pos
			s.pendingWAL = append(s.pendingWAL, e)
		}
	}
}

// memberIntersect returns the members from members whose ID appears in scope.
func memberIntersect(members []consensus.CohortMember, scope []consensus.CohortMember) []consensus.CohortMember {
	if len(scope) == 0 {
		return nil
	}
	scopeIDs := make(map[consensus.NodeID]bool, len(scope))
	for _, m := range scope {
		scopeIDs[m.ID] = true
	}
	var result []consensus.CohortMember
	for _, m := range members {
		if scopeIDs[m.ID] {
			result = append(result, m)
		}
	}
	return result
}

// unionMembers returns the union of two CohortMember slices, preserving order.
// Elements from a appear first, followed by elements from b not already in a.
func unionMembers(a, b []consensus.CohortMember) []consensus.CohortMember {
	seen := make(map[consensus.NodeID]bool, len(a))
	var result []consensus.CohortMember
	for _, m := range a {
		seen[m.ID] = true
		result = append(result, m)
	}
	for _, m := range b {
		if !seen[m.ID] {
			result = append(result, m)
		}
	}
	return result
}
