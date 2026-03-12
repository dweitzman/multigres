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
	"strings"

	"github.com/multigres/multigres/go/common/consensus"
	"github.com/multigres/multigres/go/tools/dstsim"
)

// postgresMode indicates whether the simulated postgres instance is in
// read-write primary mode or hot-standby mode, mirroring the postgres concept
// of recovery mode (pg_is_in_recovery()).
type postgresMode int8

const (
	// postgresHotStandby represents a postgres that has not been promoted:
	// it can stream WAL from a primary (if a replica) but cannot commit new
	// writes. All nodes start in this mode; promotion to postgresPrimary
	// requires a committed term designating the node as the current primary.
	postgresHotStandby postgresMode = iota

	// postgresPrimary represents a promoted, writable postgres instance.
	// The node has a committed term designating it as primary, and
	// synchronous_standby_names is set to enforce the term's durability policy.
	postgresPrimary
)

// lsn is a Log Sequence Number, a monotonically increasing position in the WAL.
type lsn int64

// walEntry is one record in the simulated WAL buffer. Each entry either
// represents a user transaction (record == nil, just advances the LSN) or a
// Term change.
//
// TODO(failover): research why replication is expected to fail in postgres if a
// replica and primary have diverging transactions (e.g. after a split-brain or
// a failed failover), and try to simulate something similar to what postgres
// does. In a graceful switchover the WAL timeline is compatible and replication
// continues from the new primary without gap. In a coordinator-led term change there may
// be timeline divergence that requires something like pg_rewind before the
// stale node can resume replication. For Stage 1 (cohort expansion, no
// failover) the current representation is sufficient.
//
// TODO(reclamation): implement WAL reclamation via periodic checkpoint entries.
// A checkpoint marks a position at which all effects can be safely discarded if
// all cohort members have applied past it. For now the buffer grows unboundedly.
type walEntry struct {
	pos    lsn
	record *consensus.Term // nil = user transaction
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

// pendingRuleChange tracks an in-flight term-apply pipeline on the primary.
type pendingRuleChange struct {
	term  consensus.Term
	phase ruleChangePhase

	// walPos is set in the writeWAL phase once the record has been appended.
	walPos lsn

	// originalStandbys and originalPolicy are the GUC settings in effect
	// before the pipeline began. Saved so they can be restored if the CAS
	// fails in the writeWAL phase.
	originalStandbys []consensus.CohortMember
	originalPolicy   consensus.DurabilityPolicy

	// combinedStandbys and combinedPolicy are used during the combinedGUC and
	// awaitQuorum phases: union of old+new standbys, conjunctive bothPolicy.
	combinedStandbys []consensus.CohortMember
	combinedPolicy   consensus.DurabilityPolicy

	// finalStandbys and finalPolicy are applied in the finalGUC phase: the new
	// standbys and new policy from the incoming rules.
	finalStandbys []consensus.CohortMember
	finalPolicy   consensus.DurabilityPolicy
}

// bothPolicy is a conjunctive DurabilityPolicy used during cohort transitions.
// A write is considered durable only when the old policy is satisfied among
// the old cohort AND the new policy is satisfied among the new cohort.
// IsDurable filters the acking set through each policy's cohort scope
// independently via memberIntersect.
type bothPolicy struct {
	primary     consensus.CohortMember
	oldPolicy   consensus.DurabilityPolicy
	oldStandbys []consensus.CohortMember
	newPolicy   consensus.DurabilityPolicy
	newStandbys []consensus.CohortMember
}

func (b *bothPolicy) IsDurable(cohortMembers, ackingMembers []consensus.CohortMember) bool {
	// Build each sub-cohort as primary + standbys.
	oldCohort := append([]consensus.CohortMember{b.primary}, b.oldStandbys...)
	newCohort := append([]consensus.CohortMember{b.primary}, b.newStandbys...)
	return b.oldPolicy.IsDurable(oldCohort, memberIntersect(ackingMembers, oldCohort)) &&
		b.newPolicy.IsDurable(newCohort, memberIntersect(ackingMembers, newCohort))
}

func (b *bothPolicy) IsAchievable(members []consensus.CohortMember) bool {
	oldCohort := append([]consensus.CohortMember{b.primary}, b.oldStandbys...)
	newCohort := append([]consensus.CohortMember{b.primary}, b.newStandbys...)
	return b.oldPolicy.IsAchievable(memberIntersect(members, oldCohort)) &&
		b.newPolicy.IsAchievable(memberIntersect(members, newCohort))
}

// RevokesAndSamplesAllRevocationSets checks each sub-policy against its own
// cohort scope. Both must be independently satisfied because a bothPolicy write
// requires both sub-quorums; a coordinator must cover enough members in each
// cohort to be certain no durable write slipped through on either side of the
// transition.
func (b *bothPolicy) RevokesAndSamplesAllRevocationSets(cohortMembers, recruitedMembers []consensus.CohortMember, primary consensus.CohortMember) bool {
	oldCohort := append([]consensus.CohortMember{b.primary}, b.oldStandbys...)
	newCohort := append([]consensus.CohortMember{b.primary}, b.newStandbys...)
	return b.oldPolicy.RevokesAndSamplesAllRevocationSets(oldCohort, memberIntersect(recruitedMembers, oldCohort), primary) &&
		b.newPolicy.RevokesAndSamplesAllRevocationSets(newCohort, memberIntersect(recruitedMembers, newCohort), primary)
}

// SimPooler wraps a PoolerNode and acts as the local postgres driver in
// simulation. The simulated postgres can run in two modes (postgresMode):
//
//   - postgresPrimary: maintains the authoritative WAL, tracks which replicas
//     have received each LSN, and enforces write quorum via simulated
//     synchronous_standby_names before considering a write durable.
//
//   - postgresHotStandby: pulls WAL entries from its configured primary each
//     tick (analogous to postgres primary_conninfo), tracks the highest LSN
//     received, and delivers ApplyRulesResponseIndicators to the wrapped
//     PoolerNode when rules records arrive. This covers both replica nodes
//     and primary nodes that are temporarily in standby during a transition.
//
// WAL replication is handled within Step() — each hot-standby node looks up
// its primary's SimPooler and reads new entries directly, mirroring how each
// postgres instance manages its own replication connection.
//
// SimPooler intercepts PolicyRecordApplyRequest before it reaches the
// RequestHandler, runs the multi-tick pipeline that models the non-atomic
// relationship between GUC changes and WAL writes, then queues
// ApplyRulesResponseIndicator once the write is durable.
type SimPooler struct {
	node *consensus.PoolerNode
	sim  *simType

	// mode is the current postgres operational mode. Nodes always start in
	// postgresHotStandby and transition to postgresPrimary only when a
	// committed term designates the node as primary with a valid cohort and
	// durability policy. When a primary is revoked by a coordinator, its
	// postgres is restarted in hot-standby mode (mode = postgresHotStandby),
	// mirroring real postgres where there is no way to halt all writes without
	// a restart.
	mode postgresMode

	// wal is the WAL buffer. Primary appends entries here; replicas receive
	// entries via pullWAL. Each entry's pos is strictly increasing.
	wal []walEntry

	// nextPos is the next LSN to assign when appending a new entry (primary only).
	nextPos lsn

	// gucSyncStandbys is the simulated synchronous_standby_names GUC (primary
	// only). Write quorum is determined by checking how many of these nodes
	// have ACKed the write LSN using gucSyncPolicy.IsWriteQuorum.
	gucSyncStandbys []consensus.CohortMember
	gucSyncPolicy   consensus.DurabilityPolicy // nil = AtLeast(1), no replicas required

	// gucWALReceiveEnabled tracks whether this replica is participating in
	// write quorum (analogous to streaming replication being active). Set to
	// false when the sidecar revokes WAL receive on behalf of the coordinator
	// (recruitment for a rule change); restored when new rules are applied.
	// Only meaningful for hot-standby nodes pulling WAL; ignored for primaries.
	gucWALReceiveEnabled bool

	// replicaACK tracks the highest WAL position each replica has confirmed
	// receiving (primary only). Updated by pullWAL calls from replicas.
	replicaACK map[consensus.NodeID]lsn

	// pendingChange is the active rules-apply pipeline (primary only). At most
	// one pipeline can be in flight at a time; PoolerNode serialises writes.
	pendingChange *pendingRuleChange

	// receivedLSN is the highest WAL position this node has received from the
	// primary (hot-standby only). On graceful switchover the WAL timeline is
	// compatible so this position remains valid against the new primary.
	receivedLSN lsn

	// primaryConnInfo is the primary this node is currently configured to
	// replicate from, analogous to postgres's primary_conninfo setting.
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

	// applyWALQueue buffers incoming ApplyWALTermRequests. The queue introduces
	// at least one tick of delay before calling applyGUCForTerm, modelling the
	// sidecar acquiring an exclusive lock before reconfiguring GUC settings.
	// Use SetApplyWALChaos to inject random delays for chaos testing.
	applyWALQueue dstsim.ChaosQueue[consensus.Term]
}

// IsConsensusPrimary returns true if this node is the primary of the
// highest quorum-confirmed term across all poolers currently known to the
// simulator. It uses the same durability computation as CoordNode.
func (s *SimPooler) IsConsensusPrimary() bool {
	nodeTerms := make(map[consensus.NodeID]*consensus.Term)
	for _, n := range s.sim.Nodes() {
		if sp, ok := n.(*SimPooler); ok {
			nodeTerms[sp.ID()] = sp.node.CommittedState().CachedTerm
		}
	}
	_, quorum := consensus.HighestTermVersions(nodeTerms)
	return quorum != nil && quorum.Primary == s.node.ID()
}

// NewSimPooler creates a SimPooler wrapping the given PoolerNode. sim is the
// simulator used to look up peer SimPoolers for WAL replication each tick.
//
// initialTerm, if non-nil, starts the node in postgresPrimary mode with the
// given term's cohort as synchronous_standby_names. This is the bootstrap
// assumption: production bootstrap (initial term write + basebackup for
// replicas) must complete before starting the consensus algorithm, so primary
// nodes always start with a committed term.
//
// Pass nil for initialTerm to start the node in postgresHotStandby mode
// (the default for replicas and any node whose bootstrap is not yet complete).
//
// TODO(bootstrap): add bootstrap support to the deterministic simulation so
// tests can exercise the full startup sequence rather than relying on seeding
// nodes with pre-committed state.
func NewSimPooler(node *consensus.PoolerNode, sim *simType, initialTerm *consensus.Term) *SimPooler {
	s := &SimPooler{
		node:                     node,
		sim:                      sim,
		replicaACK:               make(map[consensus.NodeID]lsn),
		responseCallbacks:        make(map[string]func(consensus.WritePolicyResponseRequest)),
		recruitResponseCallbacks: make(map[string]func(consensus.RecruitResponseRequest)),
	}
	if initialTerm != nil {
		s.mode = postgresPrimary
		s.gucWALReceiveEnabled = true
		for _, m := range initialTerm.Members {
			if m.ID != node.ID() {
				s.gucSyncStandbys = append(s.gucSyncStandbys, m)
			}
		}
		s.gucSyncPolicy = initialTerm.Policy
		// Seed the WAL with the initial term so replicas can stream it.
		// The seeded term was "written" to the shadow WAL at bootstrap, so the
		// WAL position starts at the term's seq. Replicas starting at receivedLSN=0
		// will pull this entry and apply the committed term via normal WAL streaming.
		s.nextPos = lsn(initialTerm.Seq)
		s.wal = append(s.wal, walEntry{pos: s.nextPos, record: initialTerm})
	} else {
		// Hot standby: replicas start pulling WAL immediately (gucWALReceiveEnabled=true)
		// unless this node has an active coordinator commitment (revoked from quorum).
		s.mode = postgresHotStandby
		state := node.CommittedState()
		s.gucWALReceiveEnabled = state.Commitment == nil
		s.primaryConnInfo = state.Primary
	}
	return s
}

// reinitGUC re-derives all simulated GUC settings from the node's current
// committed state, mirroring what postgres does on startup or after applying a
// new term from WAL. Called after Restart and whenever the node's role changes
// (e.g. graceful primary switchover).
//
// A node is promoted to postgresPrimary only when it has a committed term
// designating it as the current primary with a valid cohort and policy. In all
// other cases (pre-bootstrap, replica, revoked) it runs as postgresHotStandby.
func (s *SimPooler) reinitGUC() {
	state := s.node.CommittedState()
	switch state.Role {
	case consensus.RolePrimary:
		s.primaryConnInfo = ""
		s.gucSyncStandbys = nil
		s.gucSyncPolicy = nil
		if state.CachedTerm != nil && state.Commitment == nil {
			// Bootstrap complete and not revoked: promote to primary write mode.
			s.mode = postgresPrimary
			s.gucWALReceiveEnabled = true
			for _, m := range state.CachedTerm.Members {
				if m.ID != s.node.ID() {
					s.gucSyncStandbys = append(s.gucSyncStandbys, m)
				}
			}
			s.gucSyncPolicy = state.CachedTerm.Policy
		} else {
			// No committed term (pre-bootstrap) or recruited by coordinator:
			// postgres is in hot-standby mode, not yet writable.
			s.mode = postgresHotStandby
			s.gucWALReceiveEnabled = false
		}
	case consensus.RoleReplica:
		s.mode = postgresHotStandby
		s.primaryConnInfo = state.Primary
		s.gucSyncStandbys = nil
		s.gucSyncPolicy = nil
		s.gucWALReceiveEnabled = state.Commitment == nil
	default:
		// Role unknown: conservative non-writable standby.
		s.mode = postgresHotStandby
		s.primaryConnInfo = ""
		s.gucSyncStandbys = nil
		s.gucSyncPolicy = nil
		s.gucWALReceiveEnabled = false
	}
}

// Restart simulates a crash-restart: clears all ephemeral sidecar state,
// calls PoolerNode.Restart to reload committed state from storage, and
// re-initializes GUC settings via reinitGUC. This mirrors postgres crash
// recovery: the process reloads its last-checkpointed state and reapplies
// synchronous_standby_names / primary_conninfo from the committed term on disk.
//
// The WAL buffer (wal, nextPos, receivedLSN) is preserved — WAL is durable
// storage that survives crashes. Pending in-flight pipelines and queued
// indicators are cleared (lost like in-memory postgres state on crash).
// Implements dstsim.Restartable.
func (s *SimPooler) Restart() {
	s.node.Restart()
	s.replicaACK = make(map[consensus.NodeID]lsn)
	s.pendingChange = nil
	s.pendingWAL = nil
	s.queuedIndicators = nil
	s.pendingRevokeCorrelationID = ""
	s.applyWALQueue.Drain()
	s.responseCallbacks = make(map[string]func(consensus.WritePolicyResponseRequest))
	s.recruitResponseCallbacks = make(map[string]func(consensus.RecruitResponseRequest))
	s.reinitGUC()
}

// isRevoked returns true when the sidecar has revoked this node's participation
// in write quorum on behalf of a coordinator (recruitment for a rule change).
// For replicas this means stopping WAL ACKs; for primaries it means postgres
// has been restarted in hot-standby mode (mode = postgresHotStandby).
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

// StateSnapshot implements dstsim.NodeStateSnapshot to provide a human-readable
// summary of this node's current state for trace debugging.
func (s *SimPooler) StateSnapshot() string {
	state := s.node.CommittedState()

	var parts []string

	if s.mode == postgresPrimary {
		parts = append(parts, "mode=primary")
	} else {
		parts = append(parts, "mode=standby")
	}

	if state.CachedTerm == nil {
		parts = append(parts, "term=none")
	} else {
		parts = append(parts, fmt.Sprintf("term=seq%d/prim=%v", state.CachedTerm.Seq, state.CachedTerm.Primary))
	}

	switch state.Role {
	case consensus.RolePrimary:
		standbys := make([]string, len(s.gucSyncStandbys))
		for i, m := range s.gucSyncStandbys {
			standbys[i] = string(m.ID)
		}
		type atLeastThresholder interface{ AtLeastThreshold() int }
		if a, ok := s.gucSyncPolicy.(atLeastThresholder); ok {
			parts = append(parts, fmt.Sprintf("guc=AtLeast(%d)%v", a.AtLeastThreshold(), standbys))
		} else {
			parts = append(parts, fmt.Sprintf("guc=nil%v", standbys))
		}
		parts = append(parts, fmt.Sprintf("walLen=%d nextLSN=%d", len(s.wal), int64(s.nextPos)))
	case consensus.RoleReplica:
		parts = append(parts, fmt.Sprintf("streaming=%v recvLSN=%d", s.primaryConnInfo, int64(s.receivedLSN)))
	}

	if s.isRevoked() {
		parts = append(parts, "revoked")
	}
	if state.Commitment != nil {
		c := state.Commitment
		parts = append(parts, fmt.Sprintf("commit=%v(at=%d→%d)", c.CoordID, c.AtTermSeq, c.ProposedSeq))
	}
	if s.pendingChange != nil {
		parts = append(parts, fmt.Sprintf("pendingChange=phase%d", s.pendingChange.phase))
	}

	return strings.Join(parts, " ")
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

// SetApplyWALChaos configures chaos parameters for the ApplyWALTermRequest
// delivery queue. Use this in tests to inject random delays in sidecar GUC
// application, simulating variance in the time between the coordinator writing
// a shadow WAL entry and the node actually reconfiguring postgres.
func (s *SimPooler) SetApplyWALChaos(p dstsim.ChaosParams) {
	s.applyWALQueue.SetChaos(p)
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

// applyRevokedGUC simulates the sidecar completing a coordinator-requested
// revocation. Sets gucWALReceiveEnabled=false (making isRevoked() return true).
//
// For primaries, revocation simulates restarting postgres in hot-standby mode:
// there is no way to halt all writes in postgres without a restart, so we model
// this by transitioning mode to postgresHotStandby. Any in-flight write pipeline
// is aborted (WAL entries that never reached quorum are truncated, mirroring
// crash recovery rolling back uncommitted writes).
//
// For replicas, stops pulling WAL (clears gucWALReceiveEnabled and
// primaryConnInfo). The primary is left stuck in awaitQuorum if it was waiting
// for this replica's ACK.
func (s *SimPooler) applyRevokedGUC() {
	if s.node.CommittedState().Role == consensus.RolePrimary {
		// Restart postgres in hot-standby mode (cannot stop writes without restart).
		s.mode = postgresHotStandby
		s.gucSyncStandbys = nil
		s.gucSyncPolicy = nil
		if s.pendingChange != nil {
			// Truncate WAL entries that were appended but never reached quorum.
			if s.pendingChange.walPos > 0 {
				s.truncateWALAfter(s.pendingChange.walPos - 1)
			}
			s.queuedIndicators = append(s.queuedIndicators, consensus.ApplyRulesResponseIndicator{
				Term:     s.pendingChange.term,
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

// applyGUCForTerm updates the simulated postgres GUC settings for a term
// received via WAL replication, mirroring what the sidecar would do when it
// discovers a new term in the WAL stream and reconfigures postgres accordingly.
//
// If this node is the primary in the new term it transitions to postgresPrimary
// mode and sets synchronous_standby_names to the new cohort (minus self).
// Otherwise it remains in postgresHotStandby and updates primaryConnInfo to
// the term's primary. In both cases any prior recruitment commitment is
// considered resolved by the new term.
func (s *SimPooler) applyGUCForTerm(term *consensus.Term) {
	// Applying new rules resolves any prior recruitment commitment, but only
	// when the applied term reaches the committed ProposedSeq. If an older
	// WAL entry arrives while the node is revoked (e.g. a streamed entry
	// buffered before revocation), preserve the revoked state.
	commitment := s.node.CommittedState().Commitment
	if commitment == nil || term.Seq >= commitment.ProposedSeq {
		s.gucWALReceiveEnabled = true
	}
	if term.Primary == s.node.ID() {
		// This node is the primary in the new term: promote to write mode.
		s.mode = postgresPrimary
		s.primaryConnInfo = ""
		s.gucSyncStandbys = nil
		for _, m := range term.Members {
			if m.ID != s.node.ID() {
				s.gucSyncStandbys = append(s.gucSyncStandbys, m)
			}
		}
		s.gucSyncPolicy = term.Policy
	} else {
		// This node is a replica in the new term: stay in hot-standby mode.
		s.mode = postgresHotStandby
		s.primaryConnInfo = term.Primary
		s.gucSyncStandbys = nil
		s.gucSyncPolicy = nil
	}
}

// handlePropagatePosition performs the simulation-level history copy for a
// PropagatePositionIndicator before PoolerNode sees the event.
//
// It copies the source node's WAL buffer (entries up to TargetPosition.LSN)
// and saves the source's committed state (CachedTerm + ShadowWAL) into this
// node's storage while preserving this node's own Commitment. PoolerNode's
// handlePropagatePosition then reloads from storage and acks the coordinator.
func (s *SimPooler) handlePropagatePosition(ind consensus.PropagatePositionIndicator) {
	source := s.findSimPooler(ind.SourceNode)
	if source == nil {
		return
	}

	// Copy WAL entries from source up through TargetPosition.LSN. A zero LSN
	// means copy everything.
	targetLSN := lsn(ind.TargetPosition.LSN)
	var newWAL []walEntry
	for _, e := range source.wal {
		if targetLSN == 0 || e.pos <= targetLSN {
			newWAL = append(newWAL, e)
		}
	}
	s.wal = newWAL
	if targetLSN > 0 && source.receivedLSN > targetLSN {
		s.receivedLSN = targetLSN
	} else {
		s.receivedLSN = source.receivedLSN
	}

	// Build the new committed state: CachedTerm and ShadowWAL come from the
	// source; Commitment is preserved so subsequent shadow WAL writes are
	// still authorised; Role/Primary are re-derived from the new CachedTerm.
	sourceState := source.node.CommittedState()
	newState := s.node.CommittedState()
	newState.CachedTerm = sourceState.CachedTerm
	newState.ShadowWAL = sourceState.ShadowWAL
	if sourceState.CachedTerm != nil {
		newState.Primary = sourceState.CachedTerm.Primary
		if sourceState.CachedTerm.Primary == s.node.ID() {
			newState.Role = consensus.RolePrimary
		} else {
			newState.Role = consensus.RoleReplica
		}
	}
	// Save so PoolerNode.handlePropagatePosition can reload it.
	_ = s.node.Storage().Save(newState)
}

// advancePendingWAL processes WAL entries that were received from the primary
// in a previous tick. For each term record found, it applies GUC settings via
// applyGUCForTerm and then queues an ApplyRulesResponseIndicator so the wrapped
// PoolerNode can persist the new committed state.
//
// The one-tick delay between receiving a WAL entry and processing it here
// models the latency of the real postgres WAL receiver + sidecar: the sidecar
// must first apply the GUC change (synchronous_standby_names / primary_conninfo)
// and only then informs the consensus state machine that the term was applied.
func (s *SimPooler) advancePendingWAL() {
	committedSeq := s.node.CommittedState().PolicySeq()
	for _, e := range s.pendingWAL {
		if e.record != nil {
			// Skip term records older than our committed term. If the coordinator
			// has already sent a Resume for a newer term, we should replicate
			// toward that term rather than reverting GUC settings to an older one.
			if e.record.Seq < committedSeq {
				continue
			}
			s.applyGUCForTerm(e.record)
			s.queuedIndicators = append(s.queuedIndicators, consensus.ApplyRulesResponseIndicator{
				Term:     *e.record,
				Accepted: true,
			})
		}
	}
	s.pendingWAL = nil
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
		LSN:           consensus.LSN(s.receivedLSN),
	})
}

// advancePendingApplyWAL pulls ready terms from the applyWALQueue (delivered
// with at least one tick of delay, modelling sidecar GUC-apply latency) and
// applies each via applyGUCForTerm. Two WAL-position adjustments are made:
//
//   - Coordinator-led promotion (hot-standby → primary): nextPos is seeded from
//     receivedLSN so replicas can connect and stream from this node.
//   - Coordinator-led demotion (primary → replica, e.g. via Resume): receivedLSN
//     is advanced to nextPos so the node does not re-stream WAL entries it already
//     committed as primary. Without this, the node would pull entries it wrote
//     (e.g. the seed term at pos=1) from the new primary and incorrectly apply
//     an older term on top of the newer one learned via Resume.
//
// Returns true if any terms were applied. The caller should skip pullWAL in
// the same tick when this returns true, modelling the real sidecar's exclusive
// lock: GUC reconfiguration and WAL streaming cannot run concurrently.
func (s *SimPooler) advancePendingApplyWAL(tick int64) bool {
	terms := s.applyWALQueue.Pull(tick)
	for _, term := range terms {
		if term.Primary == s.node.ID() && s.mode == postgresHotStandby {
			// Coordinator-led promotion: seed the WAL position from the last real
			// WAL position received. Replicas will connect and stream from here.
			s.nextPos = s.receivedLSN
		} else if term.Primary != s.node.ID() && s.mode == postgresPrimary {
			// Coordinator-led demotion: advance receivedLSN to nextPos so that
			// when this node reconnects as a replica it streams only new entries,
			// not the WAL entries it already wrote as primary.
			s.receivedLSN = s.nextPos
			// Cancel any in-flight primary pipeline: the Resume supersedes whatever
			// policy write was in progress and advancePendingChange must not
			// re-promote this node during the same tick.
			s.pendingChange = nil
		}
		s.applyGUCForTerm(&term)
		s.queuedIndicators = append(s.queuedIndicators, consensus.ApplyRulesResponseIndicator{
			Term:     term,
			Accepted: true,
		})
	}
	return len(terms) > 0
}

// Step processes indicators and advances the SimPooler state machine one tick.
//
// The method:
//  1. Completes any pending sidecar revocation (from a prior tick).
//  2. Processes WAL entries received in previous ticks: applies GUC changes and
//     queues ApplyRulesResponseIndicators. This models the sidecar latency between
//     receiving a WAL entry and reconfiguring postgres + notifying consensus.
//  3. Pulls new WAL from the configured primary (stored for next tick's processing).
//  4. Advances the pending rules-apply pipeline (primary path).
//  5. Calls PoolerNode.Step with all accumulated indicators.
//  6. Intercepts PolicyRecordApplyRequest and RevokeParticipationRequest from output.
//
// Returns the subset of requests to pass to the RequestHandler.
func (s *SimPooler) Step(tick int64, externalInds []consensus.Indicator) []consensus.Request {
	// Complete any in-flight sidecar revocation before processing WAL so that
	// a newly-revoked node skips ACKing on this tick.
	s.advancePendingRevoke()

	// Replica path: process WAL entries received in previous ticks. This
	// applies GUC changes (via applyGUCForTerm) and queues
	// ApplyRulesResponseIndicators for the wrapped PoolerNode. Done before
	// pullWAL so that newly-pulled entries are not processed until next tick,
	// modelling the at-least-one-tick sidecar latency.
	s.advancePendingWAL()

	// Process any ApplyWALTermRequest items whose delivery delay has elapsed.
	// These come from WriteShadowWALIndicator with ApplyNow=true and model the
	// sidecar acquiring an exclusive lock before reconfiguring GUC settings.
	// If GUC reconfiguration ran this tick, skip WAL pulling: the sidecar lock
	// prevents concurrent reconfigure and streaming (modelling a fair mutex).
	if !s.advancePendingApplyWAL(tick) {
		// Replica path: pull new WAL from the configured primary. Entries are
		// stored in pendingWAL and will be processed in the next tick.
		s.pullWAL()
	}

	// Primary path: advance the rules-apply pipeline. This may append to
	// queuedIndicators (e.g. ApplyRulesResponseIndicator in the sendIndicator
	// phase), which are then drained into inds below so PoolerNode sees them
	// in the same tick.
	s.advancePendingChange()

	inds := make([]consensus.Indicator, 0, len(s.queuedIndicators)+len(externalInds))
	inds = append(inds, s.queuedIndicators...)
	s.queuedIndicators = nil

	// Process external indicators; intercept PropagatePositionIndicator to
	// perform the simulation-level WAL copy before PoolerNode sees the event.
	for _, ind := range externalInds {
		if v, ok := ind.(consensus.PropagatePositionIndicator); ok {
			s.handlePropagatePosition(v)
		}
		inds = append(inds, ind)
	}

	// Step the wrapped PoolerNode with all accumulated indicators.
	reqs := s.node.Step(tick, inds)

	// Intercept sidecar-bound requests before forwarding to the RequestHandler.
	// Invoke registered response callbacks for correlated responses.
	var forwarded []consensus.Request
	for _, req := range reqs {
		switch r := req.(type) {
		case consensus.PolicyRecordApplyRequest:
			s.handleApply(r)
		case consensus.ApplyWALTermRequest:
			// Sidecar intercept: push to queue with a minimum 1-tick delay
			// modelling the sidecar acquiring an exclusive lock before GUC apply.
			s.applyWALQueue.Push(r.Term, tick)
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
		case consensus.PropagatePositionAckedRequest:
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
// primary. A coordinator-led term change with timeline divergence may require additional
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
	// Only a postgres in primary write mode can commit new WAL entries.
	// mode == postgresPrimary implies: the committed term designates this node
	// as primary, bootstrap is complete, and the node has not been revoked by
	// a coordinator (which restarts postgres in hot-standby mode).
	// Hot-standby nodes (replicas, pre-bootstrap primaries, revoked primaries)
	// reject the apply.
	if s.mode != postgresPrimary {
		s.queuedIndicators = append(s.queuedIndicators, consensus.ApplyRulesResponseIndicator{
			Term:     req.Term,
			Accepted: false,
		})
		return
	}

	// CAS check: latestWALPolicySeq must equal FromSeq. This rejects stale
	// apply requests that arrive after a concurrent write has advanced the WAL.
	if s.latestWALPolicySeq() != req.FromSeq {
		s.queuedIndicators = append(s.queuedIndicators, consensus.ApplyRulesResponseIndicator{
			Term:     req.Term,
			Accepted: false,
		})
		return
	}

	term := req.Term

	// Capture original GUC settings for potential rollback in writeWAL.
	originalStandbys := make([]consensus.CohortMember, len(s.gucSyncStandbys))
	copy(originalStandbys, s.gucSyncStandbys)
	originalPolicy := s.gucSyncPolicy

	// Compute final GUC: all new cohort members except this node (the primary).
	var finalStandbys []consensus.CohortMember
	for _, m := range term.Members {
		if m.ID != s.node.ID() {
			finalStandbys = append(finalStandbys, m)
		}
	}
	finalPolicy := term.Policy

	// Compute combined GUC: union of old+new standbys.
	combinedStandbys := unionMembers(originalStandbys, finalStandbys)

	// Compute combined policy: must satisfy BOTH old and new simultaneously.
	// If there is no old policy (nil = no acks required), the combined policy
	// just needs to satisfy the new policy.
	primary := consensus.CohortMember{ID: s.node.ID()}
	var combinedPolicy consensus.DurabilityPolicy
	if originalPolicy == nil {
		combinedPolicy = finalPolicy
	} else {
		combinedPolicy = &bothPolicy{
			primary:     primary,
			oldPolicy:   originalPolicy,
			oldStandbys: originalStandbys,
			newPolicy:   finalPolicy,
			newStandbys: finalStandbys,
		}
	}

	s.pendingChange = &pendingRuleChange{
		term:             term,
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
		if s.latestWALPolicySeq() >= c.term.Seq {
			// CAS failed: restore original GUC settings and report failure.
			s.gucSyncStandbys = c.originalStandbys
			s.gucSyncPolicy = c.originalPolicy
			s.queuedIndicators = append(s.queuedIndicators, consensus.ApplyRulesResponseIndicator{
				Term:     c.term,
				Accepted: false,
			})
			s.pendingChange = nil
			return
		}
		// Append the rules record to the WAL.
		s.nextPos++
		term := c.term
		s.wal = append(s.wal, walEntry{pos: s.nextPos, record: &term})
		c.walPos = s.nextPos
		c.phase = ruleChangePhaseAwaitQuorum

	case ruleChangePhaseAwaitQuorum:
		if s.writeQuorumMet(c.walPos) {
			c.phase = ruleChangePhaseFinalGUC
		}

	case ruleChangePhaseFinalGUC:
		// Apply final GUC. New rules supersede any prior commitment.
		s.gucWALReceiveEnabled = true
		if c.term.Primary == s.node.ID() {
			// This node remains primary under the new term.
			s.primaryConnInfo = ""
			s.gucSyncStandbys = c.finalStandbys
			s.gucSyncPolicy = c.finalPolicy
			s.mode = postgresPrimary
		} else {
			// This node is being demoted (graceful switchover). Transition to
			// hot-standby mode and point at the new primary. The new primary
			// will begin its own pipeline when it processes the WAL entry.
			s.mode = postgresHotStandby
			s.gucSyncStandbys = nil
			s.gucSyncPolicy = nil
			s.primaryConnInfo = c.term.Primary
		}
		c.phase = ruleChangePhaseSendIndicator

	case ruleChangePhaseSendIndicator:
		// Queue the indicator so PoolerNode persists and responds in this tick.
		s.queuedIndicators = append(s.queuedIndicators, consensus.ApplyRulesResponseIndicator{
			Term:     c.term,
			Accepted: true,
		})
		s.pendingChange = nil
	}
}

// writeQuorumMet returns true if the current sync settings are satisfied for
// a write at the given WAL position. A nil syncPolicy (no policy applied yet)
// means no ACKs required. The primary is always included in both cohort and
// acking since it commits locally before propagating via WAL.
func (s *SimPooler) writeQuorumMet(pos lsn) bool {
	if s.gucSyncPolicy == nil {
		return true
	}
	primary := consensus.CohortMember{ID: s.node.ID()}
	cohort := append([]consensus.CohortMember{primary}, s.gucSyncStandbys...)
	acking := []consensus.CohortMember{primary} // primary always has the write
	for _, standby := range s.gucSyncStandbys {
		if s.replicaACK[standby.ID] >= pos {
			acking = append(acking, standby)
		}
	}
	return s.gucSyncPolicy.IsDurable(cohort, acking)
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
