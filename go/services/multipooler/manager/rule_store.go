// Copyright 2026 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package manager

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/multigres/multigres/go/common/consensus"
	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/timeouts"
	"github.com/multigres/multigres/go/common/topoclient"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	"github.com/multigres/multigres/go/services/multipooler/executor"
)

// ruleStorer is the interface for reading and writing the current shard rule.
// *ruleStore implements this; tests use fakeRuleStore.
type ruleStorer interface {
	// observePosition reads the current rule and WAL LSN from postgres.
	// Always returns a non-nil position when err is nil (the initial row guarantees a row exists).
	observePosition(ctx context.Context) (*clustermetadatapb.PoolerPosition, error)
	updateRule(ctx context.Context, update *ruleUpdateBuilder) (*clustermetadatapb.PoolerPosition, error)
	// createRuleTables creates multigres.current_rule and multigres.rule_history
	// if they do not already exist, and inserts the initial row for the default
	// shard, populated with the given durability policy. bootstrapID is recorded
	// as the initial row's coordinator_id, analogous to how a pooler is the
	// coordinator for leader-led rule changes. It is idempotent and safe to
	// call multiple times.
	createRuleTables(ctx context.Context, policy *clustermetadatapb.DurabilityPolicy, bootstrapID *clustermetadatapb.ID) error
	// cachedPosition returns the most recently observed or written PoolerPosition
	// from memory, without querying postgres. Returns nil if no position has been
	// cached yet (e.g. before the first observePosition or updateRule call).
	cachedPosition() *clustermetadatapb.PoolerPosition

	// hasInconsistentGUC returns true if the cached rule's policy would produce
	// different GUC strings than what postgres currently has. Safe to call
	// without the action lock.
	hasInconsistentGUC(ctx context.Context) bool

	// reconcileGUC re-reads the current rule (under SELECT FOR UPDATE when
	// inRecovery is false) and re-applies the GUC if needed. Requires the
	// action lock.
	reconcileGUC(ctx context.Context, inRecovery bool) error
}

// ruleStore manages the current shard rule in postgres.
//
// All DB operations that write or read the current rule go through ruleStore,
// ensuring consistent access to rule state.
type ruleStore struct {
	logger       *slog.Logger
	queryService executor.InternalQueryService
	syncStandby  SyncStandbyManager

	mu      sync.Mutex
	lastPos *clustermetadatapb.PoolerPosition // updated on every observePosition / updateRule
}

// newRuleStore creates a ruleStore. ssm must not be nil; tests that do not
// need GUC verification should pass noopSyncStandbyManager{}.
func newRuleStore(
	logger *slog.Logger,
	qs executor.InternalQueryService,
	ssm SyncStandbyManager,
) *ruleStore {
	return &ruleStore{
		logger:       logger,
		queryService: qs,
		syncStandby:  ssm,
	}
}

// cacheRuleObservation updates the in-memory position cache.
func (rs *ruleStore) cacheRuleObservation(pos *clustermetadatapb.PoolerPosition) {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	if rs.lastPos != nil && pos != nil && consensus.CompareRuleNumbers(pos.GetDecision().GetRuleNumber(), rs.lastPos.GetDecision().GetRuleNumber()) < 0 {
		// This position observation is stale. Ignore it.
		return
	}
	rs.lastPos = pos
}

// cachedPosition returns the most recently observed or written PoolerPosition
// from memory. Returns nil if no position has been cached yet.
func (rs *ruleStore) cachedPosition() *clustermetadatapb.PoolerPosition {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	return rs.lastPos
}

// hasInconsistentGUC returns true if the cached rule's policy would produce
// different GUC strings than what postgres currently has. Safe to call
// without the action lock.
func (rs *ruleStore) hasInconsistentGUC(ctx context.Context) bool {
	pos := rs.cachedPosition()
	if pos.GetDecision().GetDurabilityPolicy() == nil {
		return false
	}
	policy, err := consensus.NewPolicyFromProto(pos.GetDecision().GetDurabilityPolicy())
	if err != nil {
		return false
	}
	needs, err := rs.syncStandby.NeedsApply(ctx, consensus.PolicyWithCohort{
		Policy: policy,
		Cohort: pos.GetDecision().GetCohortMembers(),
	})
	if err != nil {
		return false
	}
	return needs
}

// reconcileGUC re-reads the current rule under SELECT FOR UPDATE to drain prior
// writers, then re-applies the GUC if the cached values are stale. Requires the
// action lock.
func (rs *ruleStore) reconcileGUC(ctx context.Context, inRecovery bool) error {
	if err := AssertActionLockHeld(ctx); err != nil {
		return fmt.Errorf("reconcileGUC: %w", err)
	}
	pos, lockedCtx, err := rs.readCurrentRuleLocked(ctx, inRecovery)
	if err != nil {
		return fmt.Errorf("reconcileGUC: %w", err)
	}
	if pos.GetDecision().GetDurabilityPolicy() == nil {
		return nil
	}
	policy, err := consensus.NewPolicyFromProto(pos.GetDecision().GetDurabilityPolicy())
	if err != nil {
		return fmt.Errorf("reconcileGUC: invalid durability policy: %w", err)
	}
	return rs.syncStandby.SetPolicy(lockedCtx, consensus.PolicyWithCohort{
		Policy: policy,
		Cohort: pos.GetDecision().GetCohortMembers(),
	})
}

// ----------------------------------------------------------------------------
// Rule Update Builder
// ----------------------------------------------------------------------------

// ruleNumber identifies a specific rule version by coordinator term and subterm.
type ruleNumber struct {
	coordinatorTerm int64
	leaderSubterm   int64
}

// ruleUpdateBuilder constructs the parameters for updateRule.
// coordinatorID, eventType, reason, and createdAt are always required.
// Fields not set via builder methods retain their current value in current_rule.
type ruleUpdateBuilder struct {
	// required
	termNumber    int64
	coordinatorID *clustermetadatapb.ID
	eventType     string
	reason        string
	createdAt     time.Time

	// optional; nil means keep the existing value in current_rule
	leaderID         *clustermetadatapb.ID
	cohortMembers    []*clustermetadatapb.ID
	durabilityPolicy *clustermetadatapb.DurabilityPolicy

	// history-only optional fields
	walPosition     string
	operation       string
	acceptedMembers []*clustermetadatapb.ID

	force              bool
	skipOutgoingQuorum bool        // skip BuildPolicyTransition; apply incoming GUC directly
	previousRule       *ruleNumber // for compare-and-swap; nil means no check
	promotionHook      promotionFn // non-nil iff postgres is known to be in recovery

	// propagation, when non-nil, signals propagation mode: finalise an existing
	// in-WAL proposal rather than writing a new one. The value is the expected
	// proposal that the rule store will CAS against current_rule.proposal_*.
	// previousRule must also be set, providing the expected current decision baseline.
	// In propagation mode, termNumber/coordinatorID/eventType/reason/createdAt
	// are ignored — the proposal already exists.
	propagation *clustermetadatapb.ShardRule
}

// promotionFn is called by updateRule after the pre-promote GUC is applied and
// before the rule history write. It must call pg_promote() and wait for promotion
// to complete. It is provided iff the caller has already verified that postgres
// is in recovery.
type promotionFn func(ctx context.Context) error

func newRuleUpdate(termNumber int64, coordinatorID *clustermetadatapb.ID, eventType, reason string, createdAt time.Time) *ruleUpdateBuilder {
	return &ruleUpdateBuilder{
		termNumber:    termNumber,
		coordinatorID: coordinatorID,
		eventType:     eventType,
		reason:        reason,
		createdAt:     createdAt,
	}
}

// newPropagationUpdate constructs a ruleUpdateBuilder that finalises an existing
// in-WAL proposal rather than writing a new one. previousDecisionTerm/Subterm are
// required: they CAS-guard the decision columns to detect concurrent advances.
// Callers must also supply a promotion hook via withPromotionHook — the WAL
// emission step requires a primary.
func newPropagationUpdate(
	expectedProposal *clustermetadatapb.ShardRule,
	previousDecisionTerm, previousDecisionSubterm int64,
) *ruleUpdateBuilder {
	return &ruleUpdateBuilder{
		propagation:  expectedProposal,
		previousRule: &ruleNumber{coordinatorTerm: previousDecisionTerm, leaderSubterm: previousDecisionSubterm},
	}
}

func (b *ruleUpdateBuilder) withLeader(id *clustermetadatapb.ID) *ruleUpdateBuilder {
	b.leaderID = id
	return b
}

func (b *ruleUpdateBuilder) withCohort(members []*clustermetadatapb.ID) *ruleUpdateBuilder {
	b.cohortMembers = members
	return b
}

func (b *ruleUpdateBuilder) withWALPosition(pos string) *ruleUpdateBuilder {
	b.walPosition = pos
	return b
}

func (b *ruleUpdateBuilder) withPromotionHook(fn promotionFn) *ruleUpdateBuilder {
	b.promotionHook = fn
	return b
}

func (b *ruleUpdateBuilder) withOperation(op string) *ruleUpdateBuilder {
	b.operation = op
	return b
}

func (b *ruleUpdateBuilder) withAcceptedMembers(members []*clustermetadatapb.ID) *ruleUpdateBuilder {
	b.acceptedMembers = members
	return b
}

func (b *ruleUpdateBuilder) withDurabilityPolicy(policy *clustermetadatapb.DurabilityPolicy) *ruleUpdateBuilder {
	b.durabilityPolicy = policy
	return b
}

func (b *ruleUpdateBuilder) withForce() *ruleUpdateBuilder {
	b.force = true
	return b
}

// withSkipOutgoingQuorum instructs updateRule to skip BuildPolicyTransition and apply
// the incoming cohort GUC directly (Both = Incoming). Used for coordinator-directed
// changes where the outgoing cohort is empty (bootstrap) or the coordinator has already
// verified the transition is safe, so no dual-ack window is needed.
func (b *ruleUpdateBuilder) withSkipOutgoingQuorum() *ruleUpdateBuilder {
	b.skipOutgoingQuorum = true
	return b
}

// withPreviousRule adds a compare-and-swap check: the update only proceeds if the
// current rule matches the given coordinator term and subterm.
func (b *ruleUpdateBuilder) withPreviousRule(coordinatorTerm, leaderSubterm int64) *ruleUpdateBuilder {
	b.previousRule = &ruleNumber{coordinatorTerm: coordinatorTerm, leaderSubterm: leaderSubterm}
	return b
}

// ----------------------------------------------------------------------------
// Schema Operations
// ----------------------------------------------------------------------------

// createRuleTables creates multigres.current_rule and multigres.rule_history if
// they do not already exist, then inserts the initial row for the default
// shard. It is idempotent and safe to call multiple times.
//
// current_rule holds a single row per shard representing the current cluster rule.
// It is used as a locking target (SELECT FOR UPDATE) to serialise concurrent
// writes; rule_history provides the append-only audit log.
//
// coordinator_term=0 in the initial row means no rule has been applied yet.
// policy is written into the initial row so all subsequent rule reads have a
// non-nil DurabilityPolicy; operations that do not change the policy (e.g.
// Promote) carry it forward via COALESCE in updateRule.
//
// bootstrapID becomes the initial row's coordinator_id. The pooler that
// initializes the schema acts as the coordinator for the initial row —
// analogous to how a pooler is the coordinator for leader-led rule changes.
func (rs *ruleStore) createRuleTables(ctx context.Context, policy *clustermetadatapb.DurabilityPolicy, bootstrapID *clustermetadatapb.ID) error {
	if policy == nil {
		return mterrors.Errorf(mtrpcpb.Code_INVALID_ARGUMENT, "durability policy required to initialize rule tables")
	}
	if policy.QuorumType == clustermetadatapb.QuorumType_QUORUM_TYPE_UNKNOWN || policy.RequiredCount <= 0 {
		return mterrors.Errorf(mtrpcpb.Code_INVALID_ARGUMENT,
			"invalid durability policy: quorum_type=%v required_count=%d", policy.QuorumType, policy.RequiredCount)
	}
	if bootstrapID == nil {
		return mterrors.Errorf(mtrpcpb.Code_INVALID_ARGUMENT, "bootstrapID is required to initialize rule tables")
	}

	execCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()

	if _, err := rs.queryService.Query(execCtx, `CREATE TABLE multigres.current_rule (
		shard_id                          BYTEA PRIMARY KEY,
		-- Last marked decision: always populated, represents the current durable rule.
		decision_coordinator_term          BIGINT NOT NULL,
		decision_leader_subterm            BIGINT NOT NULL,
		decision_leader_id                 TEXT,
		decision_coordinator_id            TEXT NOT NULL,
		decision_cohort_members            TEXT[] NOT NULL,
		decision_durability_policy_name    TEXT NOT NULL,
		decision_durability_quorum_type    TEXT NOT NULL,
		decision_durability_required_count INT NOT NULL,
		created_at                         TIMESTAMPTZ NOT NULL,
		-- In-flight proposal: null when no transition is in progress (the common case).
		-- Populated between the proposal write (sync replication wait) and the decision
		-- marking. Cleared atomically when the decision columns are updated.
		proposal_coordinator_term          BIGINT,
		proposal_leader_subterm            BIGINT,
		proposal_leader_id                 TEXT,
		proposal_coordinator_id            TEXT,
		proposal_cohort_members            TEXT[],
		proposal_durability_policy_name    TEXT,
		proposal_durability_quorum_type    TEXT,
		proposal_durability_required_count INT
	)`); err != nil {
		return mterrors.Wrap(err, "failed to create current_rule table")
	}

	if _, err := rs.queryService.QueryArgs(execCtx, `
		INSERT INTO multigres.current_rule
		  (shard_id, decision_coordinator_term, decision_leader_subterm,
		   decision_coordinator_id, decision_cohort_members,
		   decision_durability_policy_name, decision_durability_quorum_type,
		   decision_durability_required_count, created_at)
		VALUES ($1, 0, 0, $2, '{}', $3, $4, $5, now())`,
		[]byte("0"), topoclient.ClusterIDString(bootstrapID), policy.PolicyName, policy.QuorumType.String(), int64(policy.RequiredCount)); err != nil {
		return mterrors.Wrap(err, "failed to initialize current_rule")
	}

	// Each row records a cluster state change (promotion, cohort membership, durability policy).
	// The composite primary key (coordinator_term, leader_subterm) uniquely identifies each rule;
	// leader_subterm is assigned by the application as MAX(leader_subterm)+1 within a coordinator_term.
	if _, err := rs.queryService.Query(execCtx, `CREATE TABLE multigres.rule_history (
		coordinator_term          BIGINT NOT NULL,
		leader_subterm            BIGINT NOT NULL,
		event_type                TEXT NOT NULL,
		leader_id                 TEXT,
		coordinator_id            TEXT NOT NULL,
		wal_position              TEXT,
		accepted_members          TEXT[],
		reason                    TEXT NOT NULL,
		cohort_members            TEXT[] NOT NULL,
		durability_policy_name    TEXT NOT NULL,
		durability_quorum_type    TEXT NOT NULL,
		durability_required_count INT NOT NULL,
		operation                 TEXT,
		created_at                TIMESTAMPTZ NOT NULL,
		PRIMARY KEY (coordinator_term, leader_subterm)
	)`); err != nil {
		return mterrors.Wrap(err, "failed to create rule_history table")
	}

	return nil
}

// ----------------------------------------------------------------------------
// Read/Write Operations
// ----------------------------------------------------------------------------

// errRuleConflict is returned by updateRule when a compare-and-swap check fails:
// either withPreviousRule's explicit version check did not match, or a concurrent
// write changed the rule between our read and our write.
var errRuleConflict = errors.New("rule conflict: current rule version changed since last read")

// ----------------------------------------------------------------------------
// Shared row reader
// ----------------------------------------------------------------------------

// readCurrentRule reads the current_rule row for the default shard. If forUpdate
// is true, appends FOR UPDATE NOWAIT to acquire a row-level lock; the NOWAIT
// clause causes an immediate error if the row is already locked rather than
// blocking, so callers never wait indefinitely. On a standby this must be false
// since the node is read-only. Returns an error when the sentinel row is missing
// (tables not initialized) or when postgres is unreachable.
//
// The caller is responsible for adding an appropriate context timeout.
func (rs *ruleStore) readCurrentRule(ctx context.Context, forUpdate bool) (*clustermetadatapb.PoolerPosition, error) {
	suffix := ""
	if forUpdate {
		suffix = " FOR UPDATE NOWAIT"
	}
	result, err := rs.queryService.QueryArgs(ctx, `
		SELECT decision_coordinator_term, decision_leader_subterm,
		       decision_leader_id, decision_coordinator_id, decision_cohort_members,
		       decision_durability_policy_name, decision_durability_quorum_type,
		       decision_durability_required_count,
		       proposal_coordinator_term, proposal_leader_subterm,
		       proposal_leader_id, proposal_coordinator_id, proposal_cohort_members,
		       proposal_durability_policy_name, proposal_durability_quorum_type,
		       proposal_durability_required_count,
		       created_at,
		       CASE
		         WHEN pg_is_in_recovery()
		           THEN COALESCE(pg_last_wal_receive_lsn(), pg_last_wal_replay_lsn())
		         ELSE pg_current_wal_lsn()
		       END::text AS current_lsn
		FROM multigres.current_rule
		WHERE shard_id = $1`+suffix, []byte("0"))
	if err != nil {
		return nil, mterrors.Wrap(err, "failed to read current_rule")
	}
	if len(result.Rows) == 0 {
		return nil, mterrors.Errorf(mtrpcpb.Code_INTERNAL, "current_rule initial row missing for shard 0: tables may not be initialized")
	}

	var decisionCoordTerm, decisionLeaderSubterm int64
	var decisionLeaderIDStr, decisionCoordinatorIDStr *string
	var decisionCohortNames []string
	var decisionDPName, decisionDPQuorumType string
	var decisionDPRequiredCount int64
	var proposalCoordTerm, proposalLeaderSubterm *int64
	var proposalLeaderIDStr, proposalCoordinatorIDStr *string
	var proposalCohortNames []string
	var proposalDPName, proposalDPQuorumType *string
	var proposalDPRequiredCount *int64
	var createdAt time.Time
	var lsn string
	if err := executor.ScanRow(result.Rows[0],
		&decisionCoordTerm,
		&decisionLeaderSubterm,
		&decisionLeaderIDStr,
		&decisionCoordinatorIDStr,
		&decisionCohortNames,
		&decisionDPName,
		&decisionDPQuorumType,
		&decisionDPRequiredCount,
		&proposalCoordTerm,
		&proposalLeaderSubterm,
		&proposalLeaderIDStr,
		&proposalCoordinatorIDStr,
		&proposalCohortNames,
		&proposalDPName,
		&proposalDPQuorumType,
		&proposalDPRequiredCount,
		&createdAt,
		&lsn,
	); err != nil {
		return nil, mterrors.Wrap(err, "failed to scan current_rule")
	}

	var decisionCoordinatorIDStrVal string
	if decisionCoordinatorIDStr != nil {
		decisionCoordinatorIDStrVal = *decisionCoordinatorIDStr
	}
	pos, err := buildPoolerPosition(
		decisionCoordTerm, decisionLeaderSubterm,
		decisionLeaderIDStr, decisionCoordinatorIDStrVal, decisionCohortNames,
		decisionDPName, decisionDPQuorumType, decisionDPRequiredCount,
		proposalCoordTerm, proposalLeaderSubterm,
		proposalLeaderIDStr, proposalCoordinatorIDStr, proposalCohortNames,
		proposalDPName, proposalDPQuorumType, proposalDPRequiredCount,
		createdAt,
		lsn,
	)
	if err != nil {
		return nil, mterrors.Wrap(err, "failed to parse current_rule")
	}
	return pos, nil
}

// observePosition reads the current rule and WAL LSN from postgres and returns
// the observed position. Always returns a non-nil position when err is nil.
//
// Returns an error if postgres is unreachable or if the current_rule sentinel
// row is missing (which indicates the tables are not initialized).
func (rs *ruleStore) observePosition(ctx context.Context) (*clustermetadatapb.PoolerPosition, error) {
	queryCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()
	pos, err := rs.readCurrentRule(queryCtx, false)
	if err != nil {
		return nil, err
	}
	rs.cacheRuleObservation(pos)
	return pos, nil
}

// readCurrentRuleLocked reads the current_rule row and returns a lockedCtx that
// carries proof that prior rule writes from any previous action lock holder have
// been drained (withPriorRuleWritesDrained). The timeout is managed internally;
// lockedCtx is derived from ctx (not the internal timeout context) and remains
// valid for subsequent operations after the read completes.
//
// When inRecovery is false (primary path): uses FOR UPDATE NOWAIT, which
// succeeds immediately if no other transaction holds the row lock, or fails
// fast if the row is locked. Callers that receive an error should retry.
// When inRecovery is true (standby/promotion path): omits FOR UPDATE since the
// node is read-only and no concurrent writes to current_rule are possible.
func (rs *ruleStore) readCurrentRuleLocked(ctx context.Context, inRecovery bool) (*clustermetadatapb.PoolerPosition, context.Context, error) {
	readCtx, readCancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer readCancel()
	pos, err := rs.readCurrentRule(readCtx, !inRecovery)
	if err != nil {
		return nil, nil, err
	}
	lockedCtx := withPriorRuleWritesDrained(ctx)
	return pos, lockedCtx, nil
}

// updateRule writes a new rule to current_rule and rule_history.
//
// The leader_subterm is assigned as:
//   - 0 if termNumber is greater than the current coordinator_term (new term)
//   - current leader_subterm + 1 if termNumber equals the current coordinator_term
//
// Fields not set via the builder (leaderID, cohortMembers, durabilityPolicy) retain
// their current values from current_rule.
//
// GUC transition: the outgoing ("both") policy is applied before the WAL write so that
// writes issued during the transition satisfy both the old and new replication requirements.
// The incoming (new) policy is applied after the write commits. On a promotion the outgoing
// GUC is applied while still a standby (before pg_promote); on a primary-side rule change
// it is applied immediately before the write CTE.
//
// Returns the node's position (rule + WAL LSN) at the time of the write,
// or nil if force mode skipped the write.
//
// This operation uses the remote-operation-timeout and will fail if it cannot
// complete within that time. A timeout typically indicates that synchronous
// replication is not functioning.
func (rs *ruleStore) updateRule(ctx context.Context, update *ruleUpdateBuilder) (*clustermetadatapb.PoolerPosition, error) {
	if err := AssertActionLockHeld(ctx); err != nil {
		return nil, fmt.Errorf("updateRule: %w", err)
	}

	if update.propagation != nil {
		return rs.propagateProposal(ctx, update)
	}

	if update.force {
		// Force mode skips history recording entirely. Force operations are emergency
		// operations that must configure replication GUCs regardless. The write would
		// block on sync replication with unreachable standbys, consuming the parent
		// context's deadline and causing subsequent GUC changes to fail.
		rs.logger.InfoContext(ctx, "Skipping rule update in force mode",
			"coordinator_term", update.termNumber,
			"event_type", update.eventType)
		return nil, nil
	}

	// Identity and timing must be supplied by the caller. ClusterIDString(nil)
	// silently returns "" and the coordinator_id column is TEXT NOT NULL (not
	// rejected by postgres because "" != NULL), so without these checks a nil
	// coordinatorID would write a corrupt row instead of failing. createdAt
	// has the same property: a zero time.Time inserts as a zero timestamp.
	// Failing fast here also avoids leaving partial work in the caller, which
	// often touches postgres GUCs around this write.
	if update.coordinatorID == nil {
		return nil, mterrors.Errorf(mtrpcpb.Code_INVALID_ARGUMENT,
			"updateRule requires a non-nil coordinator_id")
	}
	if update.createdAt.IsZero() {
		return nil, mterrors.Errorf(mtrpcpb.Code_INVALID_ARGUMENT,
			"updateRule requires a non-zero created_at")
	}

	isPromotion := update.promotionHook != nil

	// Read the current rule to establish the CAS baseline and drain any in-flight
	// rule writes from a previous action lock holder.
	current, lockedCtx, err := rs.readCurrentRuleLocked(ctx, isPromotion)
	if err != nil {
		return nil, err
	}

	currentRule := current.GetDecision()
	currentTerm := currentRule.GetRuleNumber().GetCoordinatorTerm()
	currentSubterm := currentRule.GetRuleNumber().GetLeaderSubterm()

	// Optional explicit CAS: verify the caller's expected version matches what we read.
	if update.previousRule != nil {
		if currentTerm != update.previousRule.coordinatorTerm || currentSubterm != update.previousRule.leaderSubterm {
			return nil, errRuleConflict
		}
	}

	// Compute the next leader_subterm.
	var nextSubterm int64
	if update.termNumber > currentTerm {
		nextSubterm = 0
	} else if update.termNumber < currentTerm {
		return nil, mterrors.Errorf(mtrpcpb.Code_INTERNAL,
			"rule update rejected for term %d: current rule is at term %d",
			update.termNumber, currentTerm)
	} else {
		nextSubterm = currentSubterm + 1
	}

	// Resolve values to write: caller-supplied values take priority; nil retains existing.
	newLeader := currentRule.GetLeaderId()
	if update.leaderID != nil {
		newLeader = update.leaderID
	}
	newCohort := currentRule.GetCohortMembers()
	if update.cohortMembers != nil {
		newCohort = update.cohortMembers
	}
	newDP := currentRule.GetDurabilityPolicy()
	if update.durabilityPolicy != nil {
		dp := update.durabilityPolicy
		if dp.QuorumType == clustermetadatapb.QuorumType_QUORUM_TYPE_UNKNOWN || dp.RequiredCount <= 0 {
			return nil, mterrors.Errorf(mtrpcpb.Code_INVALID_ARGUMENT,
				"durability policy has missing or invalid fields: quorum_type=%v required_count=%d",
				dp.QuorumType, dp.RequiredCount)
		}
		newDP = dp
	}

	// Validate that the new cohort can satisfy the new durability policy.
	if len(newCohort) > 0 {
		policy, err := consensus.NewPolicyFromProto(newDP)
		if err != nil {
			return nil, mterrors.Errorf(mtrpcpb.Code_INVALID_ARGUMENT, "invalid durability policy: %v", err)
		}
		if err := policy.CheckAchievable(newCohort); err != nil {
			return nil, mterrors.Errorf(mtrpcpb.Code_INVALID_ARGUMENT, "cohort cannot achieve durability policy: %v", err)
		}
	}

	// Compute the GUC transition. The Both policy satisfies the old and new durability
	// requirements simultaneously and is applied before the WAL write. The Incoming
	// (new) policy is applied after the write commits.
	incomingPWC, err := consensus.NewPolicyWithCohort(newCohort, newDP)
	if err != nil {
		return nil, err
	}
	var transition *consensus.PolicyTransition
	if update.skipOutgoingQuorum {
		// Skip BuildPolicyTransition and apply the incoming cohort directly.
		// Used when the outgoing cohort is empty (bootstrap) or the coordinator
		// has already verified the transition is safe.
		transition = &consensus.PolicyTransition{Both: incomingPWC, Incoming: incomingPWC}
	} else {
		outgoingPWC, err := consensus.NewPolicyWithCohort(currentRule.GetCohortMembers(), currentRule.GetDurabilityPolicy())
		if err != nil {
			return nil, err
		}
		transition, err = consensus.BuildPolicyTransition(outgoingPWC, incomingPWC)
		if err != nil {
			return nil, fmt.Errorf("compute GUC transition: %w", err)
		}
	}

	// Convert values to SQL parameters.
	var newLeaderStr string
	if newLeader != nil {
		pid, err := newPoolerID(newLeader)
		if err != nil {
			return nil, mterrors.Wrap(err, "invalid leader ID")
		}
		newLeaderStr = pid.appName
	}

	cohortPIDs, err := toPoolerIDs(newCohort)
	if err != nil {
		return nil, mterrors.Wrap(err, "invalid cohort member ID")
	}
	newCohortParam := poolerIDsToAppNames(cohortPIDs)
	if newCohortParam == nil {
		newCohortParam = []string{}
	}

	var acceptedParam []string
	if len(update.acceptedMembers) > 0 {
		pids, err := toPoolerIDs(update.acceptedMembers)
		if err != nil {
			return nil, mterrors.Wrap(err, "invalid accepted member ID")
		}
		acceptedParam = poolerIDsToAppNames(pids)
	}

	coordinatorIDStr := topoclient.ClusterIDString(update.coordinatorID)
	// newDP is always non-nil: updateRule falls back to the current rule's policy when
	// the caller omits withDurabilityPolicy(), so these values are always present.
	dpName := newDP.PolicyName
	dpQuorumType := newDP.QuorumType.String()
	dpRequiredCount := int64(newDP.RequiredCount)

	// Apply the transition GUC before writing the rule. The transition (Both) policy
	// satisfies both old and new durability requirements simultaneously.
	// Promotion path: set GUC while still a standby, then call pg_promote().
	// Primary path: set GUC immediately before the write CTE.
	if isPromotion {
		if err := rs.syncStandby.SetPolicy(lockedCtx, transition.Both); err != nil {
			return nil, fmt.Errorf("pre-promote GUC: %w", err)
		}
		if err := update.promotionHook(lockedCtx); err != nil {
			return nil, fmt.Errorf("promotion hook: %w", err)
		}
	} else {
		if err := rs.syncStandby.SetPolicy(lockedCtx, transition.Both); err != nil {
			return nil, fmt.Errorf("pre-write GUC: %w", err)
		}
	}

	// Write the rule. The remote-operation timeout applies because this write must be
	// acknowledged by synchronous standbys; a timeout indicates replication is not functioning.
	execCtx, cancel := context.WithTimeout(ctx, timeouts.RemoteOperationTimeout)
	defer cancel()

	// Step 1: Write the proposal columns and append to rule_history. This write waits
	// for sync-standby acknowledgement (the durability gate). The proposal columns are
	// visible to concurrent readers between step 1 and step 2 — that's intentional;
	// step 2 below promotes the proposal to the decision after the post-write GUC.
	//
	// next_leader_subterm uses GREATEST across the decision and proposal subterms when
	// they are at the incoming coordinator_term, to avoid duplicate primary keys in
	// rule_history during the brief window between writing the proposal and marking
	// the decision.
	result, err := rs.queryService.QueryArgs(execCtx, `
		WITH
		  params AS (
		    -- Name all query parameters once so the rest of the CTE references them by name.
		    SELECT $1::bytea        AS shard_id,
		           $2::bigint       AS cas_term,
		           $3::bigint       AS cas_subterm,
		           $4::bigint       AS new_term,
		           $5::bigint       AS new_subterm,
		           NULLIF($6, '')   AS new_leader_id,
		           $7::text         AS new_coordinator_id,
		           $8::text[]       AS new_cohort,
		           $9::text         AS dp_name,
		           $10::text        AS dp_quorum_type,
		           $11::bigint      AS dp_required_count,
		           $12::timestamptz AS created_at,
		           $13::text        AS event_type,
		           NULLIF($14, '')  AS wal_position,
		           NULLIF($15, '')  AS operation,
		           $16::text        AS reason,
		           $17::text[]      AS accepted_members
		  ),
		  locked AS (
		    -- NOWAIT returns an error immediately if another transaction holds the row lock
		    -- rather than blocking; callers that see an error should retry.
		    -- CAS: only proceed if the decision hasn't changed since we read it above.
		    -- next_leader_subterm computed via GREATEST avoids duplicate PKs in rule_history
		    -- when both decision and proposal are at the incoming coordinator_term.
		    SELECT current_rule.shard_id,
		           GREATEST(
		             CASE WHEN current_rule.decision_coordinator_term = params.new_term
		                  THEN current_rule.decision_leader_subterm ELSE -1 END,
		             CASE WHEN current_rule.proposal_coordinator_term = params.new_term
		                  THEN current_rule.proposal_leader_subterm ELSE -1 END
		           ) + 1 AS computed_subterm
		    FROM multigres.current_rule, params
		    WHERE current_rule.shard_id           = params.shard_id
		      AND decision_coordinator_term       = params.cas_term
		      AND decision_leader_subterm         = params.cas_subterm
		    FOR UPDATE NOWAIT
		  ),
		  updated AS (
		    UPDATE multigres.current_rule
		    SET proposal_coordinator_term          = params.new_term,
		        proposal_leader_subterm            = locked.computed_subterm,
		        proposal_leader_id                 = params.new_leader_id,
		        proposal_coordinator_id            = params.new_coordinator_id,
		        proposal_cohort_members            = params.new_cohort,
		        proposal_durability_policy_name    = params.dp_name,
		        proposal_durability_quorum_type    = params.dp_quorum_type,
		        proposal_durability_required_count = params.dp_required_count,
		        created_at                         = params.created_at
		    FROM locked, params
		    WHERE current_rule.shard_id = params.shard_id
		    RETURNING proposal_coordinator_term, proposal_leader_subterm,
		              proposal_leader_id, proposal_coordinator_id, proposal_cohort_members,
		              proposal_durability_policy_name, proposal_durability_quorum_type,
		              proposal_durability_required_count,
		              params.created_at
		  ),
		  inserted AS (
		    INSERT INTO multigres.rule_history
		      (coordinator_term, leader_subterm, event_type, leader_id, coordinator_id,
		       wal_position, operation, reason, cohort_members, accepted_members,
		       durability_policy_name, durability_quorum_type, durability_required_count, created_at)
		    SELECT updated.proposal_coordinator_term, updated.proposal_leader_subterm,
		           params.event_type, updated.proposal_leader_id, updated.proposal_coordinator_id,
		           params.wal_position, params.operation, params.reason,
		           updated.proposal_cohort_members, params.accepted_members,
		           updated.proposal_durability_policy_name, updated.proposal_durability_quorum_type,
		           updated.proposal_durability_required_count, params.created_at
		    FROM updated, params
		    RETURNING coordinator_term
		  )
		-- Cross-joining inserted ensures a zero-row history insert (a bug) also returns zero
		-- rows here, causing the caller to surface an error rather than silently succeeding.
		SELECT updated.proposal_coordinator_term, updated.proposal_leader_subterm,
		       updated.proposal_leader_id, updated.proposal_coordinator_id, updated.proposal_cohort_members,
		       updated.proposal_durability_policy_name, updated.proposal_durability_quorum_type,
		       updated.proposal_durability_required_count,
		       updated.created_at,
		       CASE
		         WHEN pg_is_in_recovery()
		           THEN COALESCE(pg_last_wal_receive_lsn(), pg_last_wal_replay_lsn())
		         ELSE pg_current_wal_lsn()
		       END::text AS current_lsn
		FROM updated, inserted`,
		[]byte("0"),        // shard_id
		currentTerm,        // cas_term
		currentSubterm,     // cas_subterm
		update.termNumber,  // new_term
		nextSubterm,        // new_subterm
		newLeaderStr,       // new_leader_id (NULLIF: leader absent on sentinel row)
		coordinatorIDStr,   // new_coordinator_id
		newCohortParam,     // new_cohort
		dpName,             // dp_name
		dpQuorumType,       // dp_quorum_type
		dpRequiredCount,    // dp_required_count
		update.createdAt,   // created_at
		update.eventType,   // event_type
		update.walPosition, // wal_position (NULLIF: optional)
		update.operation,   // operation    (NULLIF: optional)
		update.reason,      // reason
		acceptedParam,      // accepted_members
	)
	if err != nil {
		return nil, mterrors.Wrap(err, "failed to write rule history record")
	}

	// Zero rows means either the CAS check failed (concurrent write between our read
	// and write) or the shard row is missing (should never happen after initialisation).
	if len(result.Rows) == 0 {
		return nil, errRuleConflict
	}

	// Scan the proposal's (term, subterm) so we can CAS-guard the mark-decision step.
	// We only need these two columns from the first-write RETURNING; the rest of the
	// row is re-read from the post-step-2 RETURNING in markProposalAsDecision.
	var writtenProposalTerm, writtenProposalSubterm int64
	if err := executor.ScanRow(result.Rows[0], &writtenProposalTerm, &writtenProposalSubterm); err != nil {
		return nil, mterrors.Wrap(err, "failed to scan written proposal term")
	}

	// Apply the incoming (new) GUC after the proposal is durable across both cohorts.
	// At this point the proposal sits in current_rule's proposal_* columns; the decision
	// is still the previous one until step 2 promotes it below.
	if err := rs.syncStandby.SetPolicy(lockedCtx, transition.Incoming); err != nil {
		return nil, fmt.Errorf("post-write GUC: %w", err)
	}

	// Step 2: promote the proposal columns to the decision columns and clear the
	// proposal columns. This is a local write (no sync wait) because quorum on the
	// proposal has already been established by step 1.
	pos, err := rs.markProposalAsDecision(ctx, writtenProposalTerm, writtenProposalSubterm)
	if err != nil {
		return nil, err
	}

	rs.cacheRuleObservation(pos)
	return pos, nil
}

// markProposalAsDecision copies the in-flight proposal columns to the decision columns
// and clears the proposal columns. The write is local (no sync replication wait) because
// quorum on the proposal has already been established by step 1 of updateRule.
//
// expectedTerm and expectedSubterm CAS-guard the proposal columns: the caller asserts
// which proposal it expects to be promoted. The update only fires when proposal_*
// matches these values, which catches "promoting the wrong proposal" bugs (e.g. a
// stale proposal left over from a crashed write).
//
// Returns the resulting PoolerPosition (decision populated from the former proposal,
// proposal nil) and the current LSN.
func (rs *ruleStore) markProposalAsDecision(ctx context.Context, expectedTerm, expectedSubterm int64) (*clustermetadatapb.PoolerPosition, error) {
	markCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()

	result, err := rs.queryService.QueryArgs(markCtx, `
		UPDATE multigres.current_rule
		SET decision_coordinator_term          = proposal_coordinator_term,
		    decision_leader_subterm            = proposal_leader_subterm,
		    decision_leader_id                 = proposal_leader_id,
		    decision_coordinator_id            = COALESCE(proposal_coordinator_id, decision_coordinator_id),
		    decision_cohort_members            = proposal_cohort_members,
		    decision_durability_policy_name    = proposal_durability_policy_name,
		    decision_durability_quorum_type    = proposal_durability_quorum_type,
		    decision_durability_required_count = proposal_durability_required_count,
		    proposal_coordinator_term          = NULL,
		    proposal_leader_subterm            = NULL,
		    proposal_leader_id                 = NULL,
		    proposal_coordinator_id            = NULL,
		    proposal_cohort_members            = NULL,
		    proposal_durability_policy_name    = NULL,
		    proposal_durability_quorum_type    = NULL,
		    proposal_durability_required_count = NULL
		WHERE shard_id = $1
		  AND proposal_coordinator_term = $2
		  AND proposal_leader_subterm   = $3
		RETURNING decision_coordinator_term, decision_leader_subterm,
		          decision_leader_id, decision_coordinator_id, decision_cohort_members,
		          decision_durability_policy_name, decision_durability_quorum_type,
		          decision_durability_required_count, created_at,
		          CASE
		            WHEN pg_is_in_recovery()
		              THEN COALESCE(pg_last_wal_receive_lsn(), pg_last_wal_replay_lsn())
		            ELSE pg_current_wal_lsn()
		          END::text AS current_lsn`,
		[]byte("0"), expectedTerm, expectedSubterm)
	if err != nil {
		return nil, mterrors.Wrap(err, "mark-decision failed")
	}
	if len(result.Rows) == 0 {
		return nil, mterrors.Errorf(mtrpcpb.Code_FAILED_PRECONDITION,
			"mark-decision: proposal (%d,%d) not found", expectedTerm, expectedSubterm)
	}

	var coordTerm, leaderSubterm int64
	var leaderIDStr *string
	var coordinatorIDStr string
	var cohortNames []string
	var dpName, dpQuorumType string
	var dpRequiredCount int64
	var createdAt time.Time
	var lsn string
	if err := executor.ScanSingleRow(result,
		&coordTerm,
		&leaderSubterm,
		&leaderIDStr,
		&coordinatorIDStr,
		&cohortNames,
		&dpName,
		&dpQuorumType,
		&dpRequiredCount,
		&createdAt,
		&lsn,
	); err != nil {
		return nil, mterrors.Wrap(err, "failed to scan mark-decision result")
	}

	// After step 2 the proposal columns are cleared; we build a PoolerPosition with
	// only the decision side populated.
	return buildPoolerPosition(
		coordTerm, leaderSubterm,
		leaderIDStr, coordinatorIDStr, cohortNames,
		dpName, dpQuorumType, dpRequiredCount,
		nil, nil, nil, nil, nil, nil, nil, nil,
		createdAt,
		lsn,
	)
}

// propagateProposal finalises an in-WAL proposal left by a dead leader. It is
// invoked from updateRule when update.propagation is set (via newPropagationUpdate).
//
//  1. Reads current_rule under FOR UPDATE NOWAIT (verifies the expected proposal
//     and the decision baseline from update.previousRule).
//  2. Applies the "Both" GUC (outgoing ∪ incoming cohort) so the WAL emission
//     in step 4 will require ACK from both cohorts.
//  3. Calls update.promotionHook (postgres promotes to primary).
//  4. Quorum gate: pg_logical_emit_message(true, ...) emits a transactional
//     WAL message that synchronous_standby_names must ACK. With sync configured
//     to "Both", this proves both cohorts hold the proposal WAL.
//  5. Marks the proposal as decision (local write, CAS-guarded).
//  6. Applies the "Incoming" GUC (just the new cohort).
//
// Returns the resulting PoolerPosition with the propagated rule as the decision
// (proposal cleared).
func (rs *ruleStore) propagateProposal(ctx context.Context, update *ruleUpdateBuilder) (*clustermetadatapb.PoolerPosition, error) {
	if update.propagation == nil || update.propagation.GetRuleNumber() == nil {
		return nil, mterrors.New(mtrpcpb.Code_INVALID_ARGUMENT, "propagateProposal requires update.propagation with a rule number")
	}
	if update.previousRule == nil {
		return nil, mterrors.New(mtrpcpb.Code_INVALID_ARGUMENT, "propagateProposal requires update.previousRule (decision CAS baseline)")
	}
	if update.promotionHook == nil {
		return nil, mterrors.New(mtrpcpb.Code_INVALID_ARGUMENT, "propagateProposal requires a promotion hook (WAL emission needs primary)")
	}
	// Propagation always respects the outgoing cohort: the in-WAL proposal was
	// written under the outgoing rule's durability policy, and the Both GUC
	// requires both cohorts to ACK before we mark the proposal as decision.
	// Bypassing outgoing quorum is the externally-certified path, not propagation.
	if update.skipOutgoingQuorum {
		return nil, mterrors.New(mtrpcpb.Code_INVALID_ARGUMENT,
			"propagateProposal cannot be combined with withSkipOutgoingQuorum; use an externally-certified proposal to bypass outgoing quorum")
	}

	expectedTerm := update.propagation.GetRuleNumber().GetCoordinatorTerm()
	expectedSubterm := update.propagation.GetRuleNumber().GetLeaderSubterm()

	// Read the current rule with lock; we promote postgres below so this read is
	// against a standby (inRecovery=true). The lockedCtx carries the
	// priorRuleWritesDrained token used by SetPolicy.
	current, lockedCtx, err := rs.readCurrentRuleLocked(ctx, true)
	if err != nil {
		return nil, err
	}

	// Verify the expected proposal is exactly what's in WAL on this node.
	currentProposal := current.GetProposal()
	if currentProposal == nil {
		return nil, mterrors.Errorf(mtrpcpb.Code_FAILED_PRECONDITION,
			"no in-flight proposal on this node; nothing to propagate")
	}
	pNum := currentProposal.GetRuleNumber()
	if pNum.GetCoordinatorTerm() != expectedTerm || pNum.GetLeaderSubterm() != expectedSubterm {
		return nil, mterrors.Errorf(mtrpcpb.Code_FAILED_PRECONDITION,
			"propagate: in-flight proposal (%d,%d) does not match expected (%d,%d)",
			pNum.GetCoordinatorTerm(), pNum.GetLeaderSubterm(), expectedTerm, expectedSubterm)
	}

	// Verify the decision baseline matches what the caller observed (CAS).
	currentDecision := current.GetDecision()
	dNum := currentDecision.GetRuleNumber()
	if dNum.GetCoordinatorTerm() != update.previousRule.coordinatorTerm || dNum.GetLeaderSubterm() != update.previousRule.leaderSubterm {
		return nil, errRuleConflict
	}

	// Build the GUC transition.
	outgoingPWC, err := consensus.NewPolicyWithCohort(currentDecision.GetCohortMembers(), currentDecision.GetDurabilityPolicy())
	if err != nil {
		return nil, mterrors.Wrap(err, "propagate: invalid outgoing policy")
	}
	incomingPWC, err := consensus.NewPolicyWithCohort(update.propagation.GetCohortMembers(), update.propagation.GetDurabilityPolicy())
	if err != nil {
		return nil, mterrors.Wrap(err, "propagate: invalid incoming policy")
	}
	transition, err := consensus.BuildPolicyTransition(outgoingPWC, incomingPWC)
	if err != nil {
		return nil, fmt.Errorf("propagate: compute GUC transition: %w", err)
	}

	// Pre-promote GUC: set sync_standby_names to "Both" (outgoing ∪ incoming).
	if err := rs.syncStandby.SetPolicy(lockedCtx, transition.Both); err != nil {
		return nil, fmt.Errorf("propagate: pre-promote GUC: %w", err)
	}

	// Promote postgres. After this returns, pg_logical_emit_message will work
	// (it requires a primary).
	if err := update.promotionHook(lockedCtx); err != nil {
		return nil, fmt.Errorf("propagate: promotion hook: %w", err)
	}

	// Quorum gate: emit a transactional WAL message. With synchronous_commit=on
	// and sync_standby_names configured to the "Both" cohort, postgres blocks
	// until enough standbys ACK. This proves both cohorts hold all WAL up to
	// and including the proposal entry.
	emitCtx, emitCancel := context.WithTimeout(ctx, timeouts.RemoteOperationTimeout)
	defer emitCancel()
	if _, err := rs.queryService.Query(emitCtx,
		`SELECT pg_logical_emit_message(true, 'multigres/propagate', '')`); err != nil {
		return nil, mterrors.Wrap(err, "propagate: quorum gate failed")
	}

	// Mark the proposal as decision (local write, CAS-guarded).
	pos, err := rs.markProposalAsDecision(ctx, expectedTerm, expectedSubterm)
	if err != nil {
		return nil, err
	}

	// Post-write GUC: transition to "Incoming" (just the new cohort).
	if err := rs.syncStandby.SetPolicy(lockedCtx, transition.Incoming); err != nil {
		return nil, fmt.Errorf("propagate: post-write GUC: %w", err)
	}

	rs.cacheRuleObservation(pos)
	return pos, nil
}

// queryRuleHistory returns the most recent rule history records in descending
// order by (coordinator_term, leader_subterm). Returns at most limit records.
func (rs *ruleStore) queryRuleHistory(ctx context.Context, limit int) ([]ruleHistoryRecord, error) {
	queryCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()

	result, err := rs.queryService.QueryArgs(queryCtx, `
		SELECT coordinator_term, leader_subterm, event_type, leader_id, coordinator_id,
		       wal_position, operation, reason, cohort_members, accepted_members,
		       durability_policy_name, durability_quorum_type, durability_required_count,
		       created_at
		FROM multigres.rule_history
		ORDER BY coordinator_term DESC, leader_subterm DESC
		LIMIT $1`, limit)
	if err != nil {
		return nil, mterrors.Wrap(err, "failed to query rule_history")
	}

	records := make([]ruleHistoryRecord, 0, len(result.Rows))
	for _, row := range result.Rows {
		var rec ruleHistoryRecord
		var leaderIDStr *string
		var cohortNames, acceptedNames []string
		var durabilityRequiredCount int64
		if err := executor.ScanRow(row,
			&rec.CoordinatorTerm,
			&rec.LeaderSubterm,
			&rec.EventType,
			&leaderIDStr,
			&rec.CoordinatorID,
			&rec.WALPosition,
			&rec.Operation,
			&rec.Reason,
			&cohortNames,
			&acceptedNames,
			&rec.DurabilityPolicyName,
			&rec.DurabilityQuorumType,
			&durabilityRequiredCount,
			&rec.CreatedAt,
		); err != nil {
			return nil, mterrors.Wrap(err, "failed to parse rule_history row")
		}
		rec.DurabilityRequiredCount = int32(durabilityRequiredCount)
		if err := scanRuleHistoryRow(&rec, leaderIDStr, cohortNames, acceptedNames); err != nil {
			return nil, mterrors.Wrap(err, "failed to parse rule_history row")
		}
		records = append(records, rec)
	}
	return records, nil
}

// ----------------------------------------------------------------------------
// Helpers
// ----------------------------------------------------------------------------

// buildPoolerPosition constructs a *clustermetadatapb.PoolerPosition from raw DB column values
// for the decision (always populated) and the optional in-flight proposal.
// leaderIDStr and coordinatorIDStr are app-name formatted strings (e.g. "zone1_pooler-name").
// Decision durability fields are NOT NULL in the DB and are always populated; proposal fields
// are NULL when no transition is in progress.
// createdAt is the coordinator-supplied CreationTime persisted with the rule; it is applied
// to the decision (a single created_at column is shared at the row level).
func buildPoolerPosition(
	decisionCoordTerm, decisionLeaderSubterm int64,
	decisionLeaderIDStr *string,
	decisionCoordinatorIDStr string,
	decisionCohortNames []string,
	decisionDPName, decisionDPQuorumType string,
	decisionDPRequiredCount int64,
	proposalCoordTerm, proposalLeaderSubterm *int64,
	proposalLeaderIDStr *string,
	proposalCoordinatorIDStr *string,
	proposalCohortNames []string,
	proposalDPName, proposalDPQuorumType *string,
	proposalDPRequiredCount *int64,
	createdAt time.Time,
	lsn string,
) (*clustermetadatapb.PoolerPosition, error) {
	decision, err := buildShardRule(
		decisionCoordTerm, decisionLeaderSubterm,
		decisionLeaderIDStr, decisionCoordinatorIDStr, decisionCohortNames,
		decisionDPName, decisionDPQuorumType, decisionDPRequiredCount,
	)
	if err != nil {
		return nil, mterrors.Wrap(err, "failed to parse decision")
	}
	if !createdAt.IsZero() {
		decision.CreationTime = timestamppb.New(createdAt)
	}

	pos := &clustermetadatapb.PoolerPosition{Decision: decision, Lsn: lsn}

	if proposalCoordTerm != nil {
		var proposalCoordinatorIDStrVal string
		if proposalCoordinatorIDStr != nil {
			proposalCoordinatorIDStrVal = *proposalCoordinatorIDStr
		}
		var proposalDPNameVal, proposalDPQuorumTypeVal string
		if proposalDPName != nil {
			proposalDPNameVal = *proposalDPName
		}
		if proposalDPQuorumType != nil {
			proposalDPQuorumTypeVal = *proposalDPQuorumType
		}
		var proposalDPRequiredCountVal int64
		if proposalDPRequiredCount != nil {
			proposalDPRequiredCountVal = *proposalDPRequiredCount
		}
		var proposalLeaderSubtermVal int64
		if proposalLeaderSubterm != nil {
			proposalLeaderSubtermVal = *proposalLeaderSubterm
		}
		proposal, err := buildShardRule(
			*proposalCoordTerm, proposalLeaderSubtermVal,
			proposalLeaderIDStr, proposalCoordinatorIDStrVal, proposalCohortNames,
			proposalDPNameVal, proposalDPQuorumTypeVal, proposalDPRequiredCountVal,
		)
		if err != nil {
			return nil, mterrors.Wrap(err, "failed to parse proposal")
		}
		pos.Proposal = proposal
	}

	return pos, nil
}

// buildShardRule constructs a *clustermetadatapb.ShardRule from raw DB column values.
// Used by buildPoolerPosition for both the decision and proposal sides.
func buildShardRule(
	coordinatorTerm, leaderSubterm int64,
	leaderIDStr *string,
	coordinatorIDStr string,
	cohortNames []string,
	durabilityPolicyName, durabilityQuorumType string,
	durabilityRequiredCount int64,
) (*clustermetadatapb.ShardRule, error) {
	rule := &clustermetadatapb.ShardRule{
		RuleNumber: &clustermetadatapb.RuleNumber{
			CoordinatorTerm: coordinatorTerm,
			LeaderSubterm:   leaderSubterm,
		},
	}

	if leaderIDStr != nil {
		id, err := parseApplicationName(*leaderIDStr)
		if err != nil {
			return nil, mterrors.Wrapf(err, "failed to parse leader_id %q", *leaderIDStr)
		}
		rule.LeaderId = id
	}

	if coordinatorIDStr != "" {
		// Coordinator IDs are multiorch, not multipooler — parseApplicationName
		// is pooler-specific, so decode the cell_name encoding directly.
		cell, name, err := topoclient.SplitClusterID(coordinatorIDStr)
		if err != nil {
			return nil, mterrors.Wrapf(err, "failed to parse coordinator_id %q", coordinatorIDStr)
		}
		rule.CoordinatorId = &clustermetadatapb.ID{
			Component: clustermetadatapb.ID_MULTIORCH,
			Cell:      cell,
			Name:      name,
		}
	}

	cohortIDs, err := appNamesToIDs(cohortNames)
	if err != nil {
		return nil, mterrors.Wrap(err, "failed to parse cohort_members")
	}
	rule.CohortMembers = cohortIDs

	v, ok := clustermetadatapb.QuorumType_value[durabilityQuorumType]
	if !ok {
		return nil, mterrors.Errorf(mtrpcpb.Code_INTERNAL, "unknown quorum_type %q", durabilityQuorumType)
	}
	rule.DurabilityPolicy = &clustermetadatapb.DurabilityPolicy{
		PolicyName:    durabilityPolicyName,
		QuorumType:    clustermetadatapb.QuorumType(v),
		RequiredCount: int32(durabilityRequiredCount),
	}

	return rule, nil
}

// appNamesToIDs converts a slice of app-name formatted strings to proto IDs.
func appNamesToIDs(names []string) ([]*clustermetadatapb.ID, error) {
	ids := make([]*clustermetadatapb.ID, 0, len(names))
	for _, name := range names {
		id, err := parseApplicationName(name)
		if err != nil {
			return nil, mterrors.Wrapf(err, "invalid ID %q", name)
		}
		ids = append(ids, id)
	}
	return ids, nil
}

// ruleHistoryRecord represents a row from multigres.rule_history or multigres.current_rule.
type ruleHistoryRecord struct {
	CoordinatorTerm         int64
	LeaderSubterm           int64
	EventType               string
	LeaderID                *poolerID // nil if not set
	CoordinatorID           *string   // informational only; component type is not stored
	WALPosition             *string
	Operation               *string
	Reason                  string
	CohortMembers           []poolerID
	AcceptedMembers         []poolerID
	DurabilityPolicyName    string
	DurabilityQuorumType    string
	DurabilityRequiredCount int32
	CreatedAt               time.Time
}

// parsePoolerIDStrings converts a slice of "cell_name" app name strings into poolerIDs.
// Returns nil for nil input, preserving the distinction between "not set" and "empty".
func parsePoolerIDStrings(names []string) ([]poolerID, error) {
	if names == nil {
		return nil, nil
	}
	result := make([]poolerID, 0, len(names))
	for _, s := range names {
		id, err := parseApplicationName(s)
		if err != nil {
			return nil, err
		}
		result = append(result, poolerID{id: id, appName: s})
	}
	return result, nil
}

// scanRuleHistoryRow scans string-typed DB columns into a ruleHistoryRecord,
// parsing leader_id, cohort_members, and accepted_members into poolerIDs.
// leaderIDStr, cohortNames, and acceptedNames are intermediary scan targets.
func scanRuleHistoryRow(rec *ruleHistoryRecord, leaderIDStr *string, cohortNames, acceptedNames []string) error {
	if leaderIDStr != nil {
		id, err := parseApplicationName(*leaderIDStr)
		if err != nil {
			return mterrors.Wrapf(err, "failed to parse leader_id %q", *leaderIDStr)
		}
		p := poolerID{id: id, appName: *leaderIDStr}
		rec.LeaderID = &p
	}
	cohort, err := parsePoolerIDStrings(cohortNames)
	if err != nil {
		return mterrors.Wrap(err, "failed to parse cohort_members")
	}
	rec.CohortMembers = cohort

	accepted, err := parsePoolerIDStrings(acceptedNames)
	if err != nil {
		return mterrors.Wrap(err, "failed to parse accepted_members")
	}
	rec.AcceptedMembers = accepted
	return nil
}

// priorRuleWritesDrainedKey is a context key proving that any in-flight rule
// writes from a previous action lock holder have been resolved before
// SyncStandbyManager.SetPolicy is called. This is established in one of two ways:
//
//   - Primary path: a SELECT FOR UPDATE on current_rule blocks until any
//     in-progress transaction from the prior holder commits or rolls back, after
//     which our row lock prevents new writers from interposing.
//   - Recovery path (standby before pg_promote): the node is read-only, so no
//     concurrent writes to current_rule are possible.
//
// The action lock (checked separately via AssertActionLockHeld) ensures no
// concurrent goroutine in this process can also hold this proof.
type priorRuleWritesDrainedKey struct{}

// withPriorRuleWritesDrained returns a derived context carrying proof that any
// in-flight rule writes from the previous action lock holder have been resolved.
// Called by readCurrentRuleLocked; callers must not stamp the context themselves.
func withPriorRuleWritesDrained(ctx context.Context) context.Context {
	return context.WithValue(ctx, priorRuleWritesDrainedKey{}, struct{}{})
}

// assertPriorRuleWritesDrained returns an error if the context does not carry
// proof that prior rule writes have been drained. Called automatically via
// readCurrentRuleLocked; callers must not stamp the context themselves.
func assertPriorRuleWritesDrained(ctx context.Context) error {
	if _, ok := ctx.Value(priorRuleWritesDrainedKey{}).(struct{}); !ok {
		return errors.New("SetPolicy requires prior rule writes to be drained (call readCurrentRuleLocked first)")
	}
	return nil
}
