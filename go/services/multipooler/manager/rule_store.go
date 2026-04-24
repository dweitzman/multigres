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
	consensusdatapb "github.com/multigres/multigres/go/pb/consensusdata"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	"github.com/multigres/multigres/go/services/multipooler/executor"
)

// ruleStorer is the interface for reading and writing the current shard rule.
// *ruleStore implements this; tests use fakeRuleStore.
type ruleStorer interface {
	observePosition(ctx context.Context) (*consensusdatapb.PoolerPosition, error)
	updateRule(ctx context.Context, update *ruleUpdateBuilder) (*consensusdatapb.PoolerPosition, error)
	// fenceRule forces synchronous replication of the existing WAL (including a
	// hung rule_history entry) to the required standbys without writing a new
	// rule_history entry. Used when re-proposing a hung cohort-change rule.
	fenceRule(ctx context.Context) error
	// createRuleTables creates multigres.current_rule and multigres.rule_history
	// if they do not already exist, and inserts the zero-state sentinel row for
	// the default shard. It is idempotent and safe to call multiple times.
	createRuleTables(ctx context.Context) error
	// cachedPosition returns the most recently observed or written PoolerPosition
	// from memory, without querying postgres. Returns nil if no position has been
	// cached yet (e.g. before the first observePosition or updateRule call).
	cachedPosition() *consensusdatapb.PoolerPosition
}

// ruleStore manages the current shard rule in postgres.
//
// All DB operations that write or read the current rule go through ruleStore,
// ensuring consistent access to rule state.
type ruleStore struct {
	logger       *slog.Logger
	queryService executor.InternalQueryService
	// applyGUC sets synchronous_standby_names, reloads the config, and waits for
	// the new setting to take effect. It is called during cohort-change transitions.
	applyGUC func(ctx context.Context, standbyNames string) error

	mu      sync.Mutex
	lastPos *consensusdatapb.PoolerPosition // updated on every observePosition / updateRule
}

// newRuleStore creates a ruleStore. applyGUC is called during cohort-change transitions
// to set synchronous_standby_names and reload the PostgreSQL configuration.
func newRuleStore(
	logger *slog.Logger,
	qs executor.InternalQueryService,
	applyGUC func(ctx context.Context, standbyNames string) error,
) *ruleStore {
	return &ruleStore{
		logger:       logger,
		queryService: qs,
		applyGUC:     applyGUC,
	}
}

// cacheRuleObservation updates the in-memory position cache.
func (rs *ruleStore) cacheRuleObservation(pos *consensusdatapb.PoolerPosition) {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	if rs.lastPos != nil && pos != nil && consensus.CompareRuleNumbers(pos.GetRule().GetRuleNumber(), rs.lastPos.GetRule().GetRuleNumber()) < 0 {
		// This position observation is stale. Ignore it.
		return
	}
	rs.lastPos = pos
}

// cachedPosition returns the most recently observed or written PoolerPosition
// from memory. Returns nil if no position has been cached yet.
func (rs *ruleStore) cachedPosition() *consensusdatapb.PoolerPosition {
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
	leaderID      *clustermetadatapb.ID
	cohortMembers []*clustermetadatapb.ID

	// history-only optional fields
	walPosition     string
	operation       string
	acceptedMembers []*clustermetadatapb.ID

	force        bool
	previousRule *ruleNumber // for compare-and-swap; nil means no check
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
// they do not already exist, then inserts the zero-state sentinel row for the
// default shard. It is idempotent and safe to call multiple times.
//
// current_rule holds a single row per shard representing the current cluster rule.
// It is used as a locking target (SELECT FOR UPDATE) to serialise concurrent
// writes; rule_history provides the append-only audit log.
//
// coordinator_term=0 in the sentinel row means no rule has been applied yet.
func (rs *ruleStore) createRuleTables(ctx context.Context) error {
	execCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()

	if _, err := rs.queryService.Query(execCtx, `CREATE TABLE multigres.current_rule (
		shard_id                  BYTEA PRIMARY KEY,
		coordinator_term          BIGINT NOT NULL,
		leader_subterm            BIGINT NOT NULL,
		leader_id                 TEXT,
		coordinator_id            TEXT,
		cohort_members            TEXT[] NOT NULL,
		durability_policy_name    TEXT,
		durability_quorum_type    TEXT,
		durability_required_count INT,
		durability_async_fallback TEXT,
		created_at                TIMESTAMPTZ NOT NULL,
		-- In-progress cohort change; NULL when no cohort change is in flight.
		proposal_coordinator_term BIGINT,
		proposal_leader_subterm   BIGINT,
		proposal_cohort_members   TEXT[]
	)`); err != nil {
		return mterrors.Wrap(err, "failed to create current_rule table")
	}

	if _, err := rs.queryService.QueryArgs(execCtx, `
		INSERT INTO multigres.current_rule
		  (shard_id, coordinator_term, leader_subterm, cohort_members, created_at)
		VALUES ($1, 0, 0, '{}', now())`,
		[]byte("0")); err != nil {
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
		durability_policy_name    TEXT,
		durability_quorum_type    TEXT,
		durability_required_count INT,
		durability_async_fallback TEXT,
		operation                 TEXT,
		created_at                TIMESTAMPTZ NOT NULL,
		PRIMARY KEY (coordinator_term, leader_subterm)
	)`); err != nil {
		return mterrors.Wrap(err, "failed to create rule_history table")
	}

	return nil
}

// ----------------------------------------------------------------------------
// Cohort Change Helpers
// ----------------------------------------------------------------------------

// cohortChangeState holds the data read from current_rule needed to manage a
// cohort transition. It is read at the start of both updateRuleWithCohortChange
// and fenceRule so both methods share the same GUC and decision-advance logic.
type cohortChangeState struct {
	// prevCoordinatorTerm and prevLeaderSubterm are the rule version at read time;
	// passed to writeProposalColumns as the CAS condition to reject concurrent writes.
	prevCoordinatorTerm int64
	prevLeaderSubterm   int64
	durabilityPolicy    consensus.DurabilityPolicy // nil if not yet set
	oldCohort           []*clustermetadatapb.ID
	// proposalCohort is nil when no cohort change is in flight.
	proposalCohort  []*clustermetadatapb.ID
	proposalTerm    int64
	proposalSubterm int64
}

// readCohortChangeState queries current_rule for the fields needed to manage a
// cohort transition: the existing cohort, durability policy, and any in-progress
// proposal. No lock is taken; the caller must hold the action lock.
func (rs *ruleStore) readCohortChangeState(ctx context.Context) (*cohortChangeState, error) {
	queryCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()

	result, err := rs.queryService.QueryArgs(queryCtx, `
		SELECT coordinator_term, leader_subterm, cohort_members,
		       durability_quorum_type, durability_required_count,
		       proposal_coordinator_term, proposal_leader_subterm, proposal_cohort_members
		FROM multigres.current_rule
		WHERE shard_id = $1`, []byte("0"))
	if err != nil {
		return nil, mterrors.Wrap(err, "failed to read cohort change state")
	}
	if len(result.Rows) == 0 {
		return nil, mterrors.New(mtrpcpb.Code_INTERNAL, "current_rule row missing")
	}

	var prevCoordinatorTerm, prevLeaderSubterm int64
	var cohortNames []string
	var durabilityQuorumType *string
	var durabilityRequiredCount *int64
	var proposalTerm, proposalSubterm *int64
	var proposalCohortNames []string

	if err := executor.ScanRow(result.Rows[0],
		&prevCoordinatorTerm,
		&prevLeaderSubterm,
		&cohortNames,
		&durabilityQuorumType,
		&durabilityRequiredCount,
		&proposalTerm,
		&proposalSubterm,
		&proposalCohortNames,
	); err != nil {
		return nil, mterrors.Wrap(err, "failed to scan cohort change state")
	}

	oldCohort, err := appNamesToIDs(cohortNames)
	if err != nil {
		return nil, mterrors.Wrap(err, "failed to parse cohort_members")
	}

	state := &cohortChangeState{
		prevCoordinatorTerm: prevCoordinatorTerm,
		prevLeaderSubterm:   prevLeaderSubterm,
		oldCohort:           oldCohort,
	}

	if durabilityQuorumType != nil && durabilityRequiredCount != nil {
		v, ok := clustermetadatapb.QuorumType_value[*durabilityQuorumType]
		if !ok {
			return nil, mterrors.Errorf(mtrpcpb.Code_INTERNAL, "unknown quorum_type %q", *durabilityQuorumType)
		}
		dp := &clustermetadatapb.DurabilityPolicy{
			QuorumType:    clustermetadatapb.QuorumType(v),
			RequiredCount: int32(*durabilityRequiredCount),
		}
		state.durabilityPolicy, err = consensus.NewPolicyFromProto(dp)
		if err != nil {
			return nil, mterrors.Wrap(err, "failed to parse durability policy")
		}
	}

	if proposalTerm != nil && proposalCohortNames != nil {
		state.proposalTerm = *proposalTerm
		if proposalSubterm != nil {
			state.proposalSubterm = *proposalSubterm
		}
		proposalCohort, err := appNamesToIDs(proposalCohortNames)
		if err != nil {
			return nil, mterrors.Wrap(err, "failed to parse proposal_cohort_members")
		}
		state.proposalCohort = proposalCohort
	}
	return state, nil
}

// idToAppName converts a cluster ID to its PostgreSQL application_name (cell_name format).
// It implements consensus.IDToAppName for use with DurabilityPolicy.StandbyNames.
func idToAppName(id *clustermetadatapb.ID) (string, error) {
	pid, err := newPoolerID(id)
	return pid.appName, err
}

// emitFenceWAL writes a transactional logical-replication message to force all
// preceding WAL to be acknowledged by the synchronous standbys defined in the
// current GUC. The message itself carries no semantic content.
func (rs *ruleStore) emitFenceWAL(ctx context.Context) error {
	execCtx, cancel := context.WithTimeout(ctx, timeouts.RemoteOperationTimeout)
	defer cancel()

	if _, err := rs.queryService.Query(execCtx,
		`SELECT pg_logical_emit_message(true, 'multigres', 'fence')`); err != nil {
		return mterrors.Wrap(err, "fencing WAL emit failed")
	}
	return nil
}

// writeProposalColumns writes the in-progress cohort-change proposal into
// current_rule under the current synchronous_standby_names GUC. Returns the
// proposal_leader_subterm assigned by the database, or an error if the CAS
// check fails (rule changed since state was read) or a proposal is already
// in flight for a different coordinator term.
func (rs *ruleStore) writeProposalColumns(ctx context.Context, state *cohortChangeState, coordinatorTerm int64, cohort []*clustermetadatapb.ID) (int64, error) {
	cohortNames := make([]string, 0, len(cohort))
	for _, id := range cohort {
		name, err := idToAppName(id)
		if err != nil {
			return 0, mterrors.Wrap(err, "invalid cohort member ID")
		}
		cohortNames = append(cohortNames, name)
	}

	execCtx, cancel := context.WithTimeout(ctx, timeouts.RemoteOperationTimeout)
	defer cancel()

	result, err := rs.queryService.QueryArgs(execCtx, `
		WITH
		  params AS (
		    SELECT $1::bytea  AS shard_id,
		           $2::bigint AS coordinator_term,
		           $3::text[] AS cohort_members,
		           $4::bigint AS prev_coordinator_term,
		           $5::bigint AS prev_leader_subterm
		  ),
		  locked AS (
		    SELECT CASE WHEN params.coordinator_term > current_rule.coordinator_term THEN 0
		                ELSE current_rule.leader_subterm + 1
		           END AS next_subterm
		    FROM multigres.current_rule, params
		    WHERE current_rule.shard_id = params.shard_id
		      AND params.coordinator_term >= current_rule.coordinator_term
		      AND current_rule.coordinator_term = params.prev_coordinator_term
		      AND current_rule.leader_subterm   = params.prev_leader_subterm
		      AND current_rule.proposal_cohort_members IS NULL
		    FOR UPDATE
		  ),
		  proposed AS (
		    UPDATE multigres.current_rule
		    SET proposal_coordinator_term = params.coordinator_term,
		        proposal_leader_subterm   = locked.next_subterm,
		        proposal_cohort_members   = params.cohort_members
		    FROM locked, params
		    WHERE current_rule.shard_id = params.shard_id
		    RETURNING current_rule.proposal_leader_subterm
		  )
		SELECT proposal_leader_subterm FROM proposed`,
		[]byte("0"), coordinatorTerm, cohortNames, state.prevCoordinatorTerm, state.prevLeaderSubterm)
	if err != nil {
		return 0, mterrors.Wrap(err, "failed to write proposal columns")
	}
	if len(result.Rows) == 0 {
		return 0, mterrors.Errorf(mtrpcpb.Code_FAILED_PRECONDITION,
			"proposal write rejected: cohort change already in progress or stale term")
	}
	var subterm int64
	if err := executor.ScanSingleRow(result, &subterm); err != nil {
		return 0, mterrors.Wrap(err, "failed to scan proposal_leader_subterm")
	}
	return subterm, nil
}

// advanceCohortToDecision advances the in-progress proposal identified by
// (proposalTerm, proposalSubterm) to a committed decision: it copies the
// proposal_cohort_members to cohort_members, clears the proposal columns, and
// appends a rule_history entry. It is used by both updateRuleWithCohortChange
// and fenceRule after the synchronous_standby_names GUC has been switched to
// the after-cohort.
func (rs *ruleStore) advanceCohortToDecision(ctx context.Context, proposalTerm, proposalSubterm int64, update *ruleUpdateBuilder) (*consensusdatapb.PoolerPosition, error) {
	var leaderStr string
	if update.leaderID != nil {
		pid, err := newPoolerID(update.leaderID)
		if err != nil {
			return nil, mterrors.Wrap(err, "invalid leader ID")
		}
		leaderStr = pid.appName
	}
	coordinatorIDStr := topoclient.ClusterIDString(update.coordinatorID)

	execCtx, cancel := context.WithTimeout(ctx, timeouts.RemoteOperationTimeout)
	defer cancel()

	result, err := rs.queryService.QueryArgs(execCtx, `
		WITH
		  params AS (
		    SELECT $1::bytea        AS shard_id,
		           $2::bigint       AS proposal_term,
		           $3::bigint       AS proposal_subterm,
		           $4::text         AS leader_id,
		           $5::text         AS coordinator_id,
		           $6::text         AS event_type,
		           $7::text         AS reason,
		           $8::timestamptz  AS created_at
		  ),
		  locked AS (
		    SELECT current_rule.proposal_coordinator_term, current_rule.proposal_leader_subterm,
		           current_rule.proposal_cohort_members, current_rule.leader_id,
		           current_rule.coordinator_id, current_rule.durability_policy_name,
		           current_rule.durability_quorum_type, current_rule.durability_required_count,
		           current_rule.durability_async_fallback
		    FROM multigres.current_rule, params
		    WHERE current_rule.shard_id = params.shard_id
		      AND current_rule.proposal_coordinator_term = params.proposal_term
		      AND current_rule.proposal_leader_subterm   = params.proposal_subterm
		    FOR UPDATE
		  ),
		  updated AS (
		    UPDATE multigres.current_rule
		    SET coordinator_term          = locked.proposal_coordinator_term,
		        leader_subterm            = locked.proposal_leader_subterm,
		        cohort_members            = locked.proposal_cohort_members,
		        leader_id                 = COALESCE(NULLIF(params.leader_id, ''), locked.leader_id),
		        coordinator_id            = COALESCE(NULLIF(params.coordinator_id, ''), locked.coordinator_id),
		        proposal_coordinator_term = NULL,
		        proposal_leader_subterm   = NULL,
		        proposal_cohort_members   = NULL,
		        created_at                = params.created_at
		    FROM locked, params
		    WHERE current_rule.shard_id = params.shard_id
		    RETURNING current_rule.coordinator_term, current_rule.leader_subterm,
		              current_rule.leader_id, current_rule.coordinator_id,
		              current_rule.cohort_members, current_rule.durability_policy_name,
		              current_rule.durability_quorum_type, current_rule.durability_required_count,
		              current_rule.durability_async_fallback
		  ),
		  inserted AS (
		    INSERT INTO multigres.rule_history
		      (coordinator_term, leader_subterm, event_type, leader_id, coordinator_id,
		       reason, cohort_members, durability_policy_name, durability_quorum_type,
		       durability_required_count, durability_async_fallback, created_at)
		    SELECT updated.coordinator_term, updated.leader_subterm,
		           params.event_type, updated.leader_id, updated.coordinator_id,
		           params.reason, updated.cohort_members, updated.durability_policy_name,
		           updated.durability_quorum_type, updated.durability_required_count,
		           updated.durability_async_fallback, params.created_at
		    FROM updated, params
		    RETURNING coordinator_term
		  )
		SELECT updated.coordinator_term, updated.leader_subterm,
		       updated.leader_id, updated.coordinator_id, updated.cohort_members,
		       updated.durability_policy_name, updated.durability_quorum_type,
		       updated.durability_required_count, updated.durability_async_fallback,
		       CASE
		         WHEN pg_is_in_recovery()
		           THEN COALESCE(pg_last_wal_receive_lsn(), pg_last_wal_replay_lsn())
		         ELSE pg_current_wal_lsn()
		       END::text AS current_lsn
		FROM updated, inserted`,
		[]byte("0"),
		proposalTerm,
		proposalSubterm,
		leaderStr,
		coordinatorIDStr,
		update.eventType,
		update.reason,
		update.createdAt,
	)
	if err != nil {
		return nil, mterrors.Wrap(err, "failed to advance cohort proposal to decision")
	}
	if len(result.Rows) == 0 {
		return nil, mterrors.Errorf(mtrpcpb.Code_INTERNAL,
			"cohort decision advance returned no rows: proposal (%d,%d) may not exist",
			proposalTerm, proposalSubterm)
	}

	var coordinatorTerm, leaderSubterm int64
	var leaderIDStr *string
	var coordinatorIDStrResult string
	var cohortNames []string
	var durabilityPolicyName, durabilityQuorumType, durabilityAsyncFallback *string
	var durabilityRequiredCount *int64
	var lsn string
	if err := executor.ScanSingleRow(result,
		&coordinatorTerm,
		&leaderSubterm,
		&leaderIDStr,
		&coordinatorIDStrResult,
		&cohortNames,
		&durabilityPolicyName,
		&durabilityQuorumType,
		&durabilityRequiredCount,
		&durabilityAsyncFallback,
		&lsn,
	); err != nil {
		return nil, mterrors.Wrap(err, "failed to scan advanced cohort position")
	}

	pos, err := buildPoolerPosition(
		coordinatorTerm, leaderSubterm,
		leaderIDStr, coordinatorIDStrResult, cohortNames,
		durabilityPolicyName, durabilityQuorumType, durabilityRequiredCount, durabilityAsyncFallback,
		lsn,
	)
	if err != nil {
		return nil, mterrors.Wrap(err, "failed to parse advanced cohort position")
	}
	rs.cacheRuleObservation(pos)
	return pos, nil
}

// bothStandbyNames returns the synchronous_standby_names GUC value that
// satisfies both the before and after cohort policies simultaneously.
// It delegates to consensus.JointPolicy to find a joint quorum expression.
func bothStandbyNames(policy consensus.DurabilityPolicy, oldCohort, newCohort []*clustermetadatapb.ID) (string, error) {
	jointPolicy, jointCohort, err := consensus.JointPolicy(policy, oldCohort, policy, newCohort)
	if err != nil {
		return "", mterrors.Wrap(err, "cannot compute joint quorum for cohort change")
	}
	return jointPolicy.StandbyNames(jointCohort, idToAppName)
}

// phaseTwo is the shared tail of a cohort change: it switches the GUC to the
// after-cohort (proposal cohort) and advances the proposal to a decision. It is
// called by both updateRuleWithCohortChange and fenceRule after the write under
// the "both" GUC has been committed.
func (rs *ruleStore) phaseTwo(ctx context.Context, state *cohortChangeState, proposalTerm, proposalSubterm int64, proposalCohort []*clustermetadatapb.ID, update *ruleUpdateBuilder) (*consensusdatapb.PoolerPosition, error) {
	afterValue, err := state.durabilityPolicy.StandbyNames(proposalCohort, idToAppName)
	if err != nil {
		return nil, mterrors.Wrap(err, "failed to compute after-cohort GUC")
	}
	if err := rs.applyGUC(ctx, afterValue); err != nil {
		return nil, mterrors.Wrap(err, "failed to apply after-cohort GUC")
	}
	return rs.advanceCohortToDecision(ctx, proposalTerm, proposalSubterm, update)
}

// updateRuleWithCohortChange implements the two-phase cohort-change protocol:
//  1. GUC → "both" (joint quorum of old and new cohort), write proposal columns
//  2. GUC → "after" (new cohort only), advance proposal to decision
//
// If a proposal for the same coordinator term is already in flight (e.g. after a
// crash between phases), the method resumes at phase 2 via emitFenceWAL to first
// force the pre-existing proposal WAL to be acknowledged.
func (rs *ruleStore) updateRuleWithCohortChange(ctx context.Context, update *ruleUpdateBuilder) (*consensusdatapb.PoolerPosition, error) {
	state, err := rs.readCohortChangeState(ctx)
	if err != nil {
		return nil, err
	}
	if state.durabilityPolicy == nil {
		return nil, mterrors.New(mtrpcpb.Code_FAILED_PRECONDITION,
			"cannot perform cohort change: no durability policy configured")
	}

	newCohort := update.cohortMembers

	var proposalTerm, proposalSubterm int64

	if state.proposalCohort != nil {
		// Proposal already in flight.
		if state.proposalTerm != update.termNumber {
			return nil, mterrors.Errorf(mtrpcpb.Code_FAILED_PRECONDITION,
				"cohort change proposal already in progress for term %d, cannot apply term %d",
				state.proposalTerm, update.termNumber)
		}
		// Resume: proposal written, may not be replicated. Set GUC to "both" and
		// force ack of the existing proposal WAL before advancing.
		proposalTerm = state.proposalTerm
		proposalSubterm = state.proposalSubterm
		newCohort = state.proposalCohort

		bothValue, err := bothStandbyNames(state.durabilityPolicy, state.oldCohort, newCohort)
		if err != nil {
			return nil, err
		}
		if err := rs.applyGUC(ctx, bothValue); err != nil {
			return nil, mterrors.Wrap(err, "failed to apply both-cohort GUC for resume")
		}
		if err := rs.emitFenceWAL(ctx); err != nil {
			return nil, err
		}
	} else {
		// Phase 1: set GUC to "both" and write proposal columns.
		bothValue, err := bothStandbyNames(state.durabilityPolicy, state.oldCohort, newCohort)
		if err != nil {
			return nil, err
		}
		if err := rs.applyGUC(ctx, bothValue); err != nil {
			return nil, mterrors.Wrap(err, "failed to apply both-cohort GUC")
		}
		proposalSubterm, err = rs.writeProposalColumns(ctx, state, update.termNumber, newCohort)
		if err != nil {
			return nil, err
		}
		proposalTerm = update.termNumber
	}

	return rs.phaseTwo(ctx, state, proposalTerm, proposalSubterm, newCohort, update)
}

// ----------------------------------------------------------------------------
// Read/Write Operations
// ----------------------------------------------------------------------------

// errRuleConflict is returned by updateRule when a withPreviousRule compare-and-swap
// check fails: the current rule's term/subterm did not match the expected values.
var errRuleConflict = errors.New("rule conflict: current rule version mismatch")

// observePosition reads the current rule and WAL LSN from postgres and returns
// the observed position. Returns nil if no rule has been applied yet
// (coordinator_term = 0).
//
// Returns an error if postgres is unreachable.
func (rs *ruleStore) observePosition(ctx context.Context) (*consensusdatapb.PoolerPosition, error) {
	queryCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()

	result, err := rs.queryService.QueryArgs(queryCtx, `
		SELECT coordinator_term, leader_subterm, leader_id, coordinator_id, cohort_members,
		       durability_policy_name, durability_quorum_type, durability_required_count,
		       durability_async_fallback,
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
		return nil, nil
	}

	var coordinatorTerm, leaderSubterm int64
	var leaderIDStr, coordinatorIDStr *string
	var cohortNames []string
	var durabilityPolicyName, durabilityQuorumType, durabilityAsyncFallback *string
	var durabilityRequiredCount *int64
	var lsn string
	if err := executor.ScanRow(result.Rows[0],
		&coordinatorTerm,
		&leaderSubterm,
		&leaderIDStr,
		&coordinatorIDStr,
		&cohortNames,
		&durabilityPolicyName,
		&durabilityQuorumType,
		&durabilityRequiredCount,
		&durabilityAsyncFallback,
		&lsn,
	); err != nil {
		return nil, mterrors.Wrap(err, "failed to scan current position")
	}

	// coordinator_term=0 is the sentinel initial state; no rule has been applied yet.
	if coordinatorTerm == 0 {
		return nil, nil
	}

	var coordinatorIDStrVal string
	if coordinatorIDStr != nil {
		coordinatorIDStrVal = *coordinatorIDStr
	}
	pos, err := buildPoolerPosition(
		coordinatorTerm, leaderSubterm,
		leaderIDStr, coordinatorIDStrVal, cohortNames,
		durabilityPolicyName, durabilityQuorumType, durabilityRequiredCount, durabilityAsyncFallback,
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
// current_rule is locked with SELECT FOR UPDATE before the update, serialising
// concurrent writes at the database level in addition to the caller's action lock.
//
// Returns the node's position (rule + WAL LSN) at the time of the write,
// or nil if force mode skipped the write.
//
// This operation uses the remote-operation-timeout and will fail if it cannot
// complete within that time. A timeout typically indicates that synchronous
// replication is not functioning.
func (rs *ruleStore) updateRule(ctx context.Context, update *ruleUpdateBuilder) (*consensusdatapb.PoolerPosition, error) {
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

	// Cohort changes require a two-phase GUC+WAL protocol to ensure both the
	// outgoing and incoming standbys durably acknowledge the transition.
	if update.cohortMembers != nil {
		return rs.updateRuleWithCohortChange(ctx, update)
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

	result, err := rs.queryService.QueryArgs(execCtx, `
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
		           $13::timestamptz AS created_at
		  ),
		  locked AS (
		    -- FOR UPDATE serializes concurrent writes at the database level, complementing
		    -- the action lock held by the caller.
		    -- Returns zero rows (causing a no-op write and error return) when any condition fails.
		    -- next_leader_subterm: 0 when starting a new coordinator term, otherwise increment within term.
		    SELECT current_rule.coordinator_term, current_rule.leader_subterm,
		           current_rule.leader_id, current_rule.cohort_members,
		           CASE WHEN params.coordinator_term > current_rule.coordinator_term THEN 0
		                ELSE current_rule.leader_subterm + 1
		           END AS next_leader_subterm
		    FROM multigres.current_rule, params
		    WHERE current_rule.shard_id = params.shard_id                   -- target shard
		      AND params.coordinator_term >= current_rule.coordinator_term  -- reject stale writes
		      AND (params.cas_coordinator_term IS NULL                      -- optimistic CAS check
		           OR (current_rule.coordinator_term = params.cas_coordinator_term
		               AND current_rule.leader_subterm = params.cas_leader_subterm))
		    FOR UPDATE
		  ),
		  updated AS (
		    UPDATE multigres.current_rule
		    SET coordinator_term = params.coordinator_term,
		        leader_subterm     = locked.next_leader_subterm,
		        leader_id        = COALESCE(NULLIF(params.leader_id, ''), locked.leader_id),
		        coordinator_id   = NULLIF(params.coordinator_id, ''),
		        cohort_members   = COALESCE(params.cohort_members, locked.cohort_members),
		        created_at       = params.created_at
		    FROM locked, params
		    -- Correlates the update target to the specific shard row. If locked is empty
		    -- (any condition above failed), the cross-join produces zero rows here too.
		    WHERE current_rule.shard_id = params.shard_id
		    RETURNING current_rule.coordinator_term, current_rule.leader_subterm,
		              current_rule.leader_id, current_rule.coordinator_id, current_rule.cohort_members,
		              current_rule.durability_policy_name, current_rule.durability_quorum_type,
		              current_rule.durability_required_count, current_rule.durability_async_fallback
		  ),
		  inserted AS (
		    INSERT INTO multigres.rule_history
		      (coordinator_term, leader_subterm, event_type, leader_id, coordinator_id,
		       wal_position, operation, reason, cohort_members, accepted_members, created_at)
		    SELECT updated.coordinator_term, updated.leader_subterm,
		           params.event_type,
		           updated.leader_id,
		           updated.coordinator_id,
		           NULLIF(params.wal_position, ''), NULLIF(params.operation, ''), params.reason,
		           updated.cohort_members,
		           params.accepted_members,
		           params.created_at
		    FROM updated, params
		    RETURNING coordinator_term
		  )
		-- Cross-joining inserted ensures a zero-row history insert (a bug) also returns zero
		-- rows here, causing the caller to surface an error rather than silently succeeding.
		SELECT updated.coordinator_term, updated.leader_subterm,
		       updated.leader_id, updated.coordinator_id, updated.cohort_members,
		       updated.durability_policy_name, updated.durability_quorum_type,
		       updated.durability_required_count, updated.durability_async_fallback,
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
	)
	if err != nil {
		return nil, mterrors.Wrap(err, "failed to write rule history record")
	}

	// Zero rows means either:
	//   - CAS check failed (expectedPreviousRule didn't match)
	//   - advancement check failed (term/subterm would not advance current state — bug)
	//   - shard row missing from current_rule (should never happen after initialisation)
	if len(result.Rows) == 0 {
		if update.previousRule != nil {
			return nil, errRuleConflict
		}
		return nil, mterrors.Errorf(mtrpcpb.Code_INTERNAL,
			"rule update rejected for term %d: current_rule already at equal or higher position",
			update.termNumber)
	}

	var coordinatorTerm, leaderSubterm int64
	var leaderIDStr *string
	var coordinatorIDStrResult string
	var cohortNames []string
	var durabilityPolicyName, durabilityQuorumType, durabilityAsyncFallback *string
	var durabilityRequiredCount *int64
	var lsn string
	if err := executor.ScanSingleRow(result,
		&coordinatorTerm,
		&leaderSubterm,
		&leaderIDStr,
		&coordinatorIDStrResult,
		&cohortNames,
		&durabilityPolicyName,
		&durabilityQuorumType,
		&durabilityRequiredCount,
		&durabilityAsyncFallback,
		&lsn,
	); err != nil {
		return nil, mterrors.Wrap(err, "failed to scan written rule position")
	}

	pos, err := buildPoolerPosition(
		coordinatorTerm, leaderSubterm,
		leaderIDStr, coordinatorIDStrResult, cohortNames,
		durabilityPolicyName, durabilityQuorumType, durabilityRequiredCount, durabilityAsyncFallback,
		lsn,
	)
	if err != nil {
		return nil, mterrors.Wrap(err, "failed to parse written rule position")
	}
	rs.cacheRuleObservation(pos)
	return pos, nil
}

// fenceRule forces synchronous replication of the existing WAL position to
// the required standbys without writing a new rule_history entry.
//
// This is used when re-proposing a hung cohort-change rule that already exists
// in the local WAL: the rule_history entry is already present, but has not yet
// been acknowledged by enough standbys to satisfy durability. Committing any
// transaction under synchronous_commit=on causes the standbys to acknowledge
// all preceding WAL, including the hung rule_history entry.
// fenceRule forces synchronous replication of in-flight WAL to the required
// standbys without writing a new rule_history entry. It handles two cases:
//
//   - No cohort change in progress: emits a transactional logical-replication
//     message to force ack of all preceding WAL under the current GUC.
//
//   - Cohort change in progress (proposal_cohort_members IS NOT NULL): sets GUC
//     to the union of the old and proposal cohorts ("both"), forces ack of the
//     proposal WAL, then switches to the proposal cohort ("after") and advances
//     the proposal to a committed decision.
func (rs *ruleStore) fenceRule(ctx context.Context) error {
	state, err := rs.readCohortChangeState(ctx)
	if err != nil {
		return err
	}

	if state.proposalCohort == nil {
		// Simple hung rule: force ack of any preceding WAL under the current GUC.
		return rs.emitFenceWAL(ctx)
	}

	// Hung cohort-change rule. Step through the two-phase protocol:
	// 1. GUC → both (joint quorum of old and proposal cohort), force ack of proposal WAL.
	if state.durabilityPolicy == nil {
		return mterrors.New(mtrpcpb.Code_FAILED_PRECONDITION,
			"cannot fence cohort change: no durability policy configured")
	}
	bothValue, err := bothStandbyNames(state.durabilityPolicy, state.oldCohort, state.proposalCohort)
	if err != nil {
		return err
	}
	if err := rs.applyGUC(ctx, bothValue); err != nil {
		return mterrors.Wrap(err, "failed to apply both-cohort GUC during fence")
	}
	if err := rs.emitFenceWAL(ctx); err != nil {
		return err
	}

	// 2. GUC → after (proposal cohort), advance proposal to decision.
	fenceUpdate := newRuleUpdate(
		state.proposalTerm, nil, "fence", "hung cohort-change rule fenced", time.Now())
	_, err = rs.phaseTwo(ctx, state, state.proposalTerm, state.proposalSubterm,
		state.proposalCohort, fenceUpdate)
	return err
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
		       durability_async_fallback, created_at
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
		var durabilityRequiredCount *int64
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
			&rec.DurabilityAsyncFallback,
			&rec.CreatedAt,
		); err != nil {
			return nil, mterrors.Wrap(err, "failed to parse rule_history row")
		}
		if durabilityRequiredCount != nil {
			v := int32(*durabilityRequiredCount)
			rec.DurabilityRequiredCount = &v
		}
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

// buildPoolerPosition constructs a *consensusdatapb.PoolerPosition from raw DB column values.
// leaderIDStr and coordinatorIDStr are app-name formatted strings (e.g. "zone1_pooler-name").
// durability fields are nil when not set in the DB.
func buildPoolerPosition(
	coordinatorTerm, leaderSubterm int64,
	leaderIDStr *string,
	coordinatorIDStr string,
	cohortNames []string,
	durabilityPolicyName, durabilityQuorumType *string,
	durabilityRequiredCount *int64,
	durabilityAsyncFallback *string,
	lsn string,
) (*consensusdatapb.PoolerPosition, error) {
	rule := &consensusdatapb.ShardRule{
		RuleNumber: &consensusdatapb.RuleNumber{
			CoordinatorTerm: coordinatorTerm,
			LeaderSubterm:   leaderSubterm,
		},
	}

	if leaderIDStr != nil {
		id, err := parseApplicationName(*leaderIDStr)
		if err != nil {
			return nil, mterrors.Wrapf(err, "failed to parse leader_id %q", *leaderIDStr)
		}
		rule.PrimaryId = id
	}

	if coordinatorIDStr != "" {
		id, err := parseApplicationName(coordinatorIDStr)
		if err != nil {
			return nil, mterrors.Wrapf(err, "failed to parse coordinator_id %q", coordinatorIDStr)
		}
		rule.CoordinatorId = id
	}

	cohortIDs, err := appNamesToIDs(cohortNames)
	if err != nil {
		return nil, mterrors.Wrap(err, "failed to parse cohort_members")
	}
	rule.CohortMembers = cohortIDs

	if durabilityPolicyName != nil || durabilityQuorumType != nil || durabilityRequiredCount != nil || durabilityAsyncFallback != nil {
		dp := &clustermetadatapb.DurabilityPolicy{}
		if durabilityPolicyName != nil {
			dp.PolicyName = *durabilityPolicyName
		}
		if durabilityQuorumType != nil {
			v, ok := clustermetadatapb.QuorumType_value[*durabilityQuorumType]
			if !ok {
				return nil, mterrors.Errorf(mtrpcpb.Code_INTERNAL, "unknown quorum_type %q", *durabilityQuorumType)
			}
			dp.QuorumType = clustermetadatapb.QuorumType(v)
		}
		if durabilityRequiredCount != nil {
			dp.RequiredCount = int32(*durabilityRequiredCount)
		}
		if durabilityAsyncFallback != nil {
			v, ok := clustermetadatapb.AsyncReplicationFallbackMode_value[*durabilityAsyncFallback]
			if !ok {
				return nil, mterrors.Errorf(mtrpcpb.Code_INTERNAL, "unknown async_fallback %q", *durabilityAsyncFallback)
			}
			dp.AsyncFallback = clustermetadatapb.AsyncReplicationFallbackMode(v)
		}
		rule.DurabilityPolicy = dp
	}

	return &consensusdatapb.PoolerPosition{
		Rule: rule,
		Lsn:  lsn,
	}, nil
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
	DurabilityPolicyName    *string
	DurabilityQuorumType    *string
	DurabilityRequiredCount *int32
	DurabilityAsyncFallback *string
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
