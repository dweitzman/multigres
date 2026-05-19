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
	"log/slog"
	"sync"
	"time"

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
	// shard, populated with the given durability policy. It is idempotent and
	// safe to call multiple times.
	createRuleTables(ctx context.Context, policy *clustermetadatapb.DurabilityPolicy) error
	// cachedPosition returns the most recently observed or written PoolerPosition
	// from memory, without querying postgres. Returns nil if no position has been
	// cached yet (e.g. before the first observePosition or updateRule call).
	cachedPosition() *clustermetadatapb.PoolerPosition
}

// ruleStore manages the current shard rule in postgres.
//
// All DB operations that write or read the current rule go through ruleStore,
// ensuring consistent access to rule state.
type ruleStore struct {
	logger       *slog.Logger
	queryService executor.InternalQueryService

	mu      sync.Mutex
	lastPos *clustermetadatapb.PoolerPosition // updated on every observePosition / updateRule
}

// newRuleStore creates a ruleStore.
func newRuleStore(
	logger *slog.Logger,
	qs executor.InternalQueryService,
) *ruleStore {
	return &ruleStore{
		logger:       logger,
		queryService: qs,
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

	force        bool
	propagation  *clustermetadatapb.ShardRule // when non-nil, skip step 1; mark this exact proposal as decision
	previousRule *ruleNumber                  // for compare-and-swap; nil means no check
}

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
func (rs *ruleStore) createRuleTables(ctx context.Context, policy *clustermetadatapb.DurabilityPolicy) error {
	if policy == nil {
		return mterrors.Errorf(mtrpcpb.Code_INVALID_ARGUMENT, "durability policy required to initialize rule tables")
	}
	if policy.QuorumType == clustermetadatapb.QuorumType_QUORUM_TYPE_UNKNOWN || policy.RequiredCount <= 0 {
		return mterrors.Errorf(mtrpcpb.Code_INVALID_ARGUMENT,
			"invalid durability policy: quorum_type=%v required_count=%d", policy.QuorumType, policy.RequiredCount)
	}

	execCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()

	if _, err := rs.queryService.Query(execCtx, `CREATE TABLE multigres.current_rule (
		shard_id                          BYTEA PRIMARY KEY,
		-- Last marked decision: always populated, represents the current durable rule.
		decision_coordinator_term         BIGINT NOT NULL,
		decision_leader_subterm           BIGINT NOT NULL,
		decision_leader_id                TEXT,
		decision_coordinator_id           TEXT,
		decision_cohort_members           TEXT[] NOT NULL,
		decision_durability_policy_name   TEXT NOT NULL,
		decision_durability_quorum_type   TEXT NOT NULL,
		decision_durability_required_count INT NOT NULL,
		-- In-flight proposal: null when no transition is in progress (the common case).
		-- Populated between the proposal write (sync replication wait) and the decision
		-- marking. Cleared atomically when the decision columns are updated.
		proposal_coordinator_term         BIGINT,
		proposal_leader_subterm           BIGINT,
		proposal_leader_id                TEXT,
		proposal_coordinator_id           TEXT,
		proposal_cohort_members           TEXT[],
		proposal_durability_policy_name   TEXT,
		proposal_durability_quorum_type   TEXT,
		proposal_durability_required_count INT,
		created_at                        TIMESTAMPTZ NOT NULL
	)`); err != nil {
		return mterrors.Wrap(err, "failed to create current_rule table")
	}

	if _, err := rs.queryService.QueryArgs(execCtx, `
		INSERT INTO multigres.current_rule
		  (shard_id, decision_coordinator_term, decision_leader_subterm, decision_cohort_members,
		   decision_durability_policy_name, decision_durability_quorum_type,
		   decision_durability_required_count, created_at)
		VALUES ($1, 0, 0, '{}', $2, $3, $4, now())`,
		[]byte("0"), policy.PolicyName, policy.QuorumType.String(), int64(policy.RequiredCount)); err != nil {
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
		coordinator_id            TEXT,
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

// errRuleConflict is returned by updateRule when a withPreviousRule compare-and-swap
// check fails: the current rule's term/subterm did not match the expected values.
var errRuleConflict = errors.New("rule conflict: current rule version mismatch")

// observePosition reads the current rule and WAL LSN from postgres.
// Always returns a non-nil position when err is nil.
// Returns an error if postgres is unreachable or the initial row is missing.
func (rs *ruleStore) observePosition(ctx context.Context) (*clustermetadatapb.PoolerPosition, error) {
	queryCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()

	result, err := rs.queryService.QueryArgs(queryCtx, `
		SELECT decision_coordinator_term, decision_leader_subterm,
		       decision_leader_id, decision_coordinator_id, decision_cohort_members,
		       decision_durability_policy_name, decision_durability_quorum_type,
		       decision_durability_required_count,
		       proposal_coordinator_term, proposal_leader_subterm,
		       proposal_leader_id, proposal_coordinator_id, proposal_cohort_members,
		       proposal_durability_policy_name, proposal_durability_quorum_type,
		       proposal_durability_required_count,
		       CASE
		         WHEN pg_is_in_recovery()
		           THEN COALESCE(pg_last_wal_receive_lsn(), pg_last_wal_replay_lsn())
		         ELSE pg_current_wal_lsn()
		       END::text AS current_lsn
		FROM multigres.current_rule
		WHERE shard_id = $1`, []byte("0"))
	if err != nil {
		return nil, mterrors.Wrap(err, "failed to query current position")
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
		&lsn,
	); err != nil {
		return nil, mterrors.Wrap(err, "failed to scan current position")
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
		proposalLeaderIDStr, proposalCoordinatorIDStr,
		proposalCohortNames,
		proposalDPName, proposalDPQuorumType, proposalDPRequiredCount,
		lsn,
	)
	if err != nil {
		return nil, mterrors.Wrap(err, "failed to parse current position")
	}
	rs.cacheRuleObservation(pos)
	return pos, nil
}

// updateRule atomically writes a new rule by updating multigres.current_rule and
// appending to multigres.rule_history in a single CTE statement.
//
// The leader_subterm is assigned as:
//   - 0 if termNumber is greater than the current coordinator_term (new term)
//   - current leader_subterm + 1 if termNumber equals the current coordinator_term
//
// Fields not set via the builder (leaderID, cohortMembers) retain their current
// values in current_rule. All provided values are written to rule_history.
//
// current_rule is locked with SELECT FOR UPDATE before the update, serializing
// concurrent writes at the database level in addition to the caller's action lock.
//
// Returns the node's position (rule + WAL LSN) at the time of the write,
// or nil if force mode skipped the write.
//
// This operation uses the remote-operation-timeout and will fail if it cannot
// complete within that time. A timeout typically indicates that synchronous
// replication is not functioning.
func (rs *ruleStore) updateRule(ctx context.Context, update *ruleUpdateBuilder) (*clustermetadatapb.PoolerPosition, error) {
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

	if update.propagation != nil {
		return rs.propagateProposal(ctx, update)
	}

	// Convert optional leader ID; empty string causes NULLIF→COALESCE to keep existing.
	var leaderStr string
	if update.leaderID != nil {
		pid, err := newPoolerID(update.leaderID)
		if err != nil {
			return nil, mterrors.Wrap(err, "invalid leader ID")
		}
		leaderStr = pid.appName
	}

	// Convert optional cohort; nil slice becomes SQL NULL, triggering COALESCE to keep existing.
	var cohortParam []string
	if update.cohortMembers != nil {
		pids, err := toPoolerIDs(update.cohortMembers)
		if err != nil {
			return nil, mterrors.Wrap(err, "invalid cohort member ID")
		}
		cohortParam = poolerIDsToAppNames(pids)
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

	// For compare-and-swap: pass the expected term/subterm as SQL parameters.
	// NULL causes the WHERE clause to skip the check, allowing any current state.
	var previousTerm, previousSubterm *int64
	if update.previousRule != nil {
		previousTerm = &update.previousRule.coordinatorTerm
		previousSubterm = &update.previousRule.leaderSubterm
	}

	// Use the remote operation timeout for history writes. This write validates that synchronous
	// replication is functioning - it must wait long enough for standbys to connect and acknowledge.
	execCtx, cancel := context.WithTimeout(ctx, timeouts.RemoteOperationTimeout)
	defer cancel()

	var dpName, dpQuorumType string
	var dpRequiredCount *int64
	if dp := update.durabilityPolicy; dp != nil {
		if dp.QuorumType == clustermetadatapb.QuorumType_QUORUM_TYPE_UNKNOWN || dp.RequiredCount <= 0 {
			return nil, mterrors.Errorf(mtrpcpb.Code_INVALID_ARGUMENT,
				"durability policy has missing or invalid fields: quorum_type=%v required_count=%d",
				dp.QuorumType, dp.RequiredCount)
		}
		if len(update.cohortMembers) > 0 {
			policy, err := consensus.NewPolicyFromProto(dp)
			if err != nil {
				return nil, mterrors.Errorf(mtrpcpb.Code_INVALID_ARGUMENT, "invalid durability policy: %v", err)
			}
			if err := policy.CheckAchievable(update.cohortMembers); err != nil {
				return nil, mterrors.Errorf(mtrpcpb.Code_INVALID_ARGUMENT, "cohort cannot achieve durability policy: %v", err)
			}
		}
		dpName = dp.PolicyName
		dpQuorumType = dp.QuorumType.String()
		rc := int64(dp.RequiredCount)
		dpRequiredCount = &rc
	}
	// TODO: It'd be nice to validate that the rule is achievable if only one of the cohort
	// or durability policy is modified, but we'd have to look up the previous rule first.

	// Step 1: Write the proposal columns and append to rule_history. This write waits
	// for sync-standby acknowledgement (the durability gate). The proposal columns are
	// visible to concurrent readers between step 1 and step 2 — that's intentional.
	step1Result, err := rs.queryService.QueryArgs(execCtx, `
		WITH
		  params AS (
		    SELECT $1::bytea        AS shard_id,
		           $2::bigint       AS coordinator_term,
		           $3::text         AS event_type,
		           $4::text         AS leader_id,
		           $5::text         AS coordinator_id,
		           $6::text         AS wal_position,
		           $7::text         AS operation,
		           $8::text         AS reason,
		           $9::text[]       AS cohort_members,
		           $10::text[]      AS accepted_members,
		           $11::bigint      AS cas_coordinator_term,
		           $12::bigint      AS cas_leader_subterm,
		           $13::timestamptz AS created_at,
		           $14::text        AS durability_policy_name,
		           $15::text        AS durability_quorum_type,
		           $16::int         AS durability_required_count
		  ),
		  locked AS (
		    -- FOR UPDATE serializes concurrent writes at the database level, complementing
		    -- the action lock held by the caller. CAS check and advancement check are applied
		    -- against the decision columns (the committed state).
		    -- next_leader_subterm: 0 when starting a new coordinator term, otherwise the next
		    -- value after the highest allocated subterm for that coordinator term. We take the
		    -- GREATEST of the decision and proposal subtermss (when they are at the incoming
		    -- coordinator_term) to avoid duplicate primary keys in rule_history during the
		    -- brief window between writing the proposal (step 1) and marking the decision (step 2).
		    SELECT current_rule.decision_coordinator_term,
		           current_rule.decision_leader_subterm,
		           current_rule.decision_leader_id,
		           current_rule.decision_cohort_members,
		           current_rule.decision_durability_policy_name,
		           current_rule.decision_durability_quorum_type,
		           current_rule.decision_durability_required_count,
		           GREATEST(
		               CASE WHEN current_rule.decision_coordinator_term = params.coordinator_term
		                    THEN current_rule.decision_leader_subterm ELSE -1 END,
		               CASE WHEN current_rule.proposal_coordinator_term = params.coordinator_term
		                    THEN current_rule.proposal_leader_subterm ELSE -1 END
		           ) + 1 AS next_leader_subterm
		    FROM multigres.current_rule, params
		    WHERE current_rule.shard_id = params.shard_id
		      AND params.coordinator_term >= GREATEST(
		              current_rule.decision_coordinator_term,
		              COALESCE(current_rule.proposal_coordinator_term, 0))
		      AND (params.cas_coordinator_term IS NULL
		           OR (current_rule.decision_coordinator_term = params.cas_coordinator_term
		               AND current_rule.decision_leader_subterm = params.cas_leader_subterm))
		    FOR UPDATE
		  ),
		  updated AS (
		    UPDATE multigres.current_rule
		    SET proposal_coordinator_term          = params.coordinator_term,
		        proposal_leader_subterm            = locked.next_leader_subterm,
		        proposal_leader_id                 = COALESCE(NULLIF(params.leader_id, ''), locked.decision_leader_id),
		        proposal_coordinator_id            = NULLIF(params.coordinator_id, ''),
		        proposal_cohort_members            = COALESCE(params.cohort_members, locked.decision_cohort_members),
		        proposal_durability_policy_name    = COALESCE(NULLIF(params.durability_policy_name, ''), locked.decision_durability_policy_name),
		        proposal_durability_quorum_type    = COALESCE(NULLIF(params.durability_quorum_type, ''), locked.decision_durability_quorum_type),
		        proposal_durability_required_count = COALESCE(params.durability_required_count, locked.decision_durability_required_count),
		        created_at                         = params.created_at
		    FROM locked, params
		    WHERE current_rule.shard_id = params.shard_id
		    RETURNING current_rule.proposal_coordinator_term,
		              current_rule.proposal_leader_subterm,
		              current_rule.proposal_leader_id,
		              current_rule.proposal_coordinator_id,
		              current_rule.proposal_cohort_members,
		              current_rule.proposal_durability_policy_name,
		              current_rule.proposal_durability_quorum_type,
		              current_rule.proposal_durability_required_count
		  ),
		  inserted AS (
		    INSERT INTO multigres.rule_history
		      (coordinator_term, leader_subterm, event_type, leader_id, coordinator_id,
		       wal_position, operation, reason, cohort_members, accepted_members,
		       durability_policy_name, durability_quorum_type, durability_required_count, created_at)
		    SELECT updated.proposal_coordinator_term,
		           updated.proposal_leader_subterm,
		           params.event_type,
		           updated.proposal_leader_id,
		           updated.proposal_coordinator_id,
		           NULLIF(params.wal_position, ''), NULLIF(params.operation, ''), params.reason,
		           updated.proposal_cohort_members,
		           params.accepted_members,
		           updated.proposal_durability_policy_name,
		           updated.proposal_durability_quorum_type,
		           updated.proposal_durability_required_count,
		           params.created_at
		    FROM updated, params
		    RETURNING coordinator_term
		  )
		-- Cross-joining inserted ensures a zero-row history insert (a bug) also returns zero
		-- rows here, causing the caller to surface an error rather than silently succeeding.
		-- The LSN is captured here, after the sync-standby ack for this WAL write.
		SELECT updated.proposal_coordinator_term,
		       updated.proposal_leader_subterm,
		       updated.proposal_leader_id,
		       updated.proposal_coordinator_id,
		       updated.proposal_cohort_members,
		       updated.proposal_durability_policy_name,
		       updated.proposal_durability_quorum_type,
		       updated.proposal_durability_required_count,
		       CASE
		         WHEN pg_is_in_recovery()
		           THEN COALESCE(pg_last_wal_receive_lsn(), pg_last_wal_replay_lsn())
		         ELSE pg_current_wal_lsn()
		       END::text AS current_lsn
		FROM updated, inserted`,
		[]byte("0"),
		update.termNumber,
		update.eventType,
		leaderStr,
		coordinatorIDStr,
		update.walPosition,
		update.operation,
		update.reason,
		cohortParam,
		acceptedParam,
		previousTerm,
		previousSubterm,
		update.createdAt,
		dpName,
		dpQuorumType,
		dpRequiredCount,
	)
	if err != nil {
		return nil, mterrors.Wrap(err, "failed to write proposal")
	}

	// Zero rows means either:
	//   - CAS check failed (expectedPreviousRule didn't match)
	//   - advancement check failed (term/subterm would not advance current state — bug)
	//   - shard row missing from current_rule (should never happen after initialisation)
	if len(step1Result.Rows) == 0 {
		if update.previousRule != nil {
			return nil, errRuleConflict
		}
		return nil, mterrors.Errorf(mtrpcpb.Code_INTERNAL,
			"rule update rejected for term %d: current_rule already at equal or higher position",
			update.termNumber)
	}

	var proposalCoordTerm, proposalLeaderSubterm int64
	var proposalLeaderIDStr *string
	var proposalCoordinatorIDStr string
	var proposalCohortNames []string
	var proposalDPName, proposalDPQuorumType string
	var proposalDPRequiredCount int64
	var lsn string
	if err := executor.ScanSingleRow(step1Result,
		&proposalCoordTerm,
		&proposalLeaderSubterm,
		&proposalLeaderIDStr,
		&proposalCoordinatorIDStr,
		&proposalCohortNames,
		&proposalDPName,
		&proposalDPQuorumType,
		&proposalDPRequiredCount,
		&lsn,
	); err != nil {
		return nil, mterrors.Wrap(err, "failed to scan written proposal")
	}

	// Step 2: Mark the decision by copying proposal → decision columns and clearing
	// the proposal columns. This is a local write (no sync replication wait).
	markCtx, markCancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer markCancel()

	if _, err := rs.queryService.QueryArgs(markCtx, `
		UPDATE multigres.current_rule
		SET decision_coordinator_term          = proposal_coordinator_term,
		    decision_leader_subterm            = proposal_leader_subterm,
		    decision_leader_id                 = proposal_leader_id,
		    decision_coordinator_id            = proposal_coordinator_id,
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
		  AND proposal_coordinator_term IS NOT NULL`,
		[]byte("0")); err != nil {
		return nil, mterrors.Wrap(err, "failed to mark decision")
	}

	// Build the PoolerPosition from the proposal values (which are now the decision).
	// No proposal is in flight at this point — the decision was just marked.
	pos, err := buildPoolerPosition(
		proposalCoordTerm, proposalLeaderSubterm,
		proposalLeaderIDStr, proposalCoordinatorIDStr, proposalCohortNames,
		proposalDPName, proposalDPQuorumType, proposalDPRequiredCount,
		nil, nil, nil, nil, nil, nil, nil, nil,
		lsn,
	)
	if err != nil {
		return nil, mterrors.Wrap(err, "failed to parse written position")
	}
	rs.cacheRuleObservation(pos)
	return pos, nil
}

// markProposalAsDecision copies the in-flight proposal columns to the decision
// columns and clears the proposal columns. This is a local write (500ms timeout)
// because quorum has already been established by the caller.
// Returns an error if no proposal matching (expectedTerm, expectedSubterm) is found.
func (rs *ruleStore) markProposalAsDecision(ctx context.Context, expectedTerm, expectedSubterm int64) error {
	markCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()

	result, err := rs.queryService.QueryArgs(markCtx, `
		UPDATE multigres.current_rule
		SET decision_coordinator_term          = proposal_coordinator_term,
		    decision_leader_subterm            = proposal_leader_subterm,
		    decision_leader_id                 = proposal_leader_id,
		    decision_coordinator_id            = proposal_coordinator_id,
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
		RETURNING shard_id`,
		[]byte("0"), expectedTerm, expectedSubterm)
	if err != nil {
		return mterrors.Wrap(err, "mark-decision failed")
	}
	if len(result.Rows) == 0 {
		return mterrors.Errorf(mtrpcpb.Code_FAILED_PRECONDITION,
			"mark-decision: proposal (%d,%d) not found", expectedTerm, expectedSubterm)
	}
	return nil
}

// propagateProposal finalises an existing in-WAL proposal: emits a WAL record as a
// quorum gate (step 1), then marks the proposal as decided (step 2).
//
// Step 1 uses pg_logical_emit_message to emit a transactional WAL message that sync
// standbys must acknowledge, proving the proposal WAL is durable across the cohort.
// The WHERE clause simultaneously verifies the expected proposal is in flight and the
// decision CAS baseline matches. Once PR #992 lands, step 1 will also apply the "both"
// GUC before emitting so both the outgoing and incoming cohorts participate.
//
// Step 2 calls markProposalAsDecision to copy proposal → decision columns.
func (rs *ruleStore) propagateProposal(ctx context.Context, update *ruleUpdateBuilder) (*clustermetadatapb.PoolerPosition, error) {
	if update.previousRule == nil {
		return nil, mterrors.New(mtrpcpb.Code_INVALID_ARGUMENT,
			"propagation update requires withPreviousRule for CAS guard")
	}

	expected := update.propagation
	expectedTerm := expected.GetRuleNumber().GetCoordinatorTerm()
	expectedSubterm := expected.GetRuleNumber().GetLeaderSubterm()
	prevTerm := update.previousRule.coordinatorTerm
	prevSubterm := update.previousRule.leaderSubterm

	// Step 1: quorum gate. pg_logical_emit_message(true, ...) emits a transactional
	// WAL message; with synchronous_standby_names configured, the transaction blocks
	// until standbys ACK, proving the proposal WAL (written before us) is durable.
	quorumCtx, quorumCancel := context.WithTimeout(ctx, timeouts.RemoteOperationTimeout)
	defer quorumCancel()

	result, err := rs.queryService.QueryArgs(quorumCtx, `
		SELECT pg_logical_emit_message(true, 'multigres/propagate', '')
		FROM multigres.current_rule
		WHERE shard_id = $1
		  AND proposal_coordinator_term = $2
		  AND proposal_leader_subterm   = $3
		  AND decision_coordinator_term = $4
		  AND decision_leader_subterm   = $5`,
		[]byte("0"), expectedTerm, expectedSubterm, prevTerm, prevSubterm)
	if err != nil {
		return nil, mterrors.Wrap(err, "propagation quorum gate failed")
	}
	if len(result.Rows) == 0 {
		return nil, mterrors.Errorf(mtrpcpb.Code_FAILED_PRECONDITION,
			"propagation quorum gate: proposal (%d,%d) not found or decision (%d,%d) CAS mismatch",
			expectedTerm, expectedSubterm, prevTerm, prevSubterm)
	}

	// Step 2: mark the proposal as decided (local write).
	if err := rs.markProposalAsDecision(ctx, expectedTerm, expectedSubterm); err != nil {
		return nil, err
	}

	pos, err := rs.observePosition(ctx)
	if err != nil {
		return nil, mterrors.Wrap(err, "failed to read position after propagation")
	}
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

// buildShardRule constructs a ShardRule from raw DB column values.
// leaderIDStr and coordinatorIDStr are app-name formatted strings.
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
		cell, name, err := splitCellName(coordinatorIDStr)
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

// buildPoolerPosition constructs a *clustermetadatapb.PoolerPosition from raw DB column values.
// Decision columns are NOT NULL and always produce a populated Decision field.
// Proposal columns are nullable: if proposalCoordTerm is nil there is no in-flight proposal
// and Proposal is left nil. All other proposal pointer args are ignored when proposalCoordTerm is nil.
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
