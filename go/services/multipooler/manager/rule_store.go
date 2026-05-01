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
	observePosition(ctx context.Context) (*clustermetadatapb.PoolerPosition, error)
	updateRule(ctx context.Context, update *ruleUpdateBuilder) (*clustermetadatapb.PoolerPosition, error)
	// createRuleTables creates multigres.current_rule and multigres.rule_history
	// if they do not already exist, and inserts the zero-state sentinel row for
	// the default shard. It is idempotent and safe to call multiple times.
	createRuleTables(ctx context.Context) error
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
	if rs.lastPos != nil && pos != nil && consensus.CompareRuleNumbers(pos.GetRule().GetRuleNumber(), rs.lastPos.GetRule().GetRuleNumber()) < 0 {
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

	force         bool
	previousRule  *ruleNumber // for compare-and-swap; nil means no check
	promotionHook promotionFn // non-nil iff postgres is known to be in recovery
}

// promotionFn is called by updateRule after the pre-promote GUC is applied and
// before the main Transact. It must call pg_promote() and wait for promotion to
// complete. It is provided iff the caller already knows postgres is in recovery.
// walPosition must be provided separately via withWALPosition — the rule store
// never computes it internally.
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

// withPromotionHook registers a callback that updateRule will invoke after
// applying the pre-promote GUC and before starting the main transaction.
// Provide this iff the caller has verified that postgres is in recovery (standby).
// The hook must call pg_promote() and wait for promotion to complete.
func (b *ruleUpdateBuilder) withPromotionHook(fn promotionFn) *ruleUpdateBuilder {
	b.promotionHook = fn
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
		created_at                TIMESTAMPTZ NOT NULL
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

// lockedRuleState holds the rule state read from current_rule inside an updateRule
// transaction, plus the next leader_subterm to assign.
type lockedRuleState struct {
	rule        *clustermetadatapb.ShardRule
	nextSubterm int64
}

// ruleRowLockKey is a context key that proves SELECT FOR UPDATE was executed on
// current_rule within the current action-lock session. Both the action lock and
// this row lock must be held before any call to SyncStandbyManager.SetPolicy:
//
//   - The action lock ensures no concurrent goroutine is modifying the GUC.
//   - The row lock ensures the GUC change is consistent with the rule the caller
//     just read; without it, a stale GUC update could race with a concurrent rule
//     write that committed after the action lock was acquired but before the GUC
//     was written.
//
// The row lock is released at COMMIT, but the context token remains valid for
// the post-commit SetPolicy call because the action lock is still held, and the
// next caller to modify the GUC will acquire both locks before doing so.
type ruleRowLockKey struct{}

// withRuleRowLock returns a derived context that carries proof of a
// SELECT FOR UPDATE on current_rule within this action-lock session.
func withRuleRowLock(ctx context.Context) context.Context {
	return context.WithValue(ctx, ruleRowLockKey{}, struct{}{})
}

// prePromoteKey is a context key used when SetPolicy is called before pg_promote(),
// while the node is still a standby. On a standby, SELECT FOR UPDATE is not
// allowed, but the action lock alone is sufficient — no concurrent writer can
// modify the rule table on a read-only standby.
type prePromoteKey struct{}

// withPrePromoteToken returns a derived context proving that SetPolicy is being
// called from the pre-promote path, where the action lock alone is sufficient.
func withPrePromoteToken(ctx context.Context) context.Context {
	return context.WithValue(ctx, prePromoteKey{}, struct{}{})
}

// assertRuleRowLockedSinceActionLock returns an error if the context carries
// neither a ruleRowLock token (SELECT FOR UPDATE was run since the action lock
// was acquired) nor a prePromote token (caller is on the standby pre-promote
// path, where action lock alone is sufficient because standbys have no
// concurrent rule writers).
func assertRuleRowLockedSinceActionLock(ctx context.Context) error {
	_, hasRowLock := ctx.Value(ruleRowLockKey{}).(struct{})
	_, hasPrePromote := ctx.Value(prePromoteKey{}).(struct{})
	if !hasRowLock && !hasPrePromote {
		return errors.New("SetPolicy requires SELECT FOR UPDATE on current_rule or pre-promote token")
	}
	return nil
}

// readRuleRow reads the current rule state and WAL LSN from current_rule. If
// forUpdate is true, appends FOR UPDATE to acquire a row-level lock. The query
// is executed on qs (either a transaction or the bare query service).
// Returns an error if no row exists for the default shard.
func (rs *ruleStore) readRuleRow(ctx context.Context, qs executor.InternalQueryService, forUpdate bool) (*clustermetadatapb.PoolerPosition, error) {
	suffix := ""
	if forUpdate {
		suffix = " FOR UPDATE"
	}
	result, err := qs.QueryArgs(ctx, `
		SELECT coordinator_term, leader_subterm, leader_id, coordinator_id, cohort_members,
		       durability_policy_name, durability_quorum_type, durability_required_count,
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
		return nil, errors.New("no rule found")
	}

	var coordinatorTerm, leaderSubterm int64
	var leaderIDStr, coordinatorIDStr *string
	var cohortNames []string
	var durabilityPolicyName, durabilityQuorumType *string
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
		&lsn,
	); err != nil {
		return nil, mterrors.Wrap(err, "failed to scan current_rule")
	}

	var coordinatorIDStrVal string
	if coordinatorIDStr != nil {
		coordinatorIDStrVal = *coordinatorIDStr
	}
	return buildPoolerPosition(
		coordinatorTerm, leaderSubterm,
		leaderIDStr, coordinatorIDStrVal, cohortNames,
		durabilityPolicyName, durabilityQuorumType, durabilityRequiredCount,
		lsn,
	)
}

// lockCurrentRule runs SELECT FOR UPDATE on current_rule to acquire a row-level
// lock and read the current state. It also validates the stale-write and CAS
// conditions, returning errRuleConflict when a CAS check fails.
//
// The returned context carries a ruleRowLock token (see withRuleRowLock); pass it
// to SyncStandbyManager.SetPolicy to satisfy the dual-lock invariant.
func (rs *ruleStore) lockCurrentRule(ctx context.Context, tx executor.InternalQueryService, update *ruleUpdateBuilder) (context.Context, *lockedRuleState, error) {
	pos, err := rs.readRuleRow(ctx, tx, true)
	if err != nil {
		return nil, nil, mterrors.Wrap(err, "failed to lock current_rule")
	}
	if pos == nil {
		return nil, nil, mterrors.Errorf(mtrpcpb.Code_INTERNAL, "current_rule row not found for default shard")
	}

	rule := pos.GetRule()
	coordinatorTerm := rule.GetRuleNumber().GetCoordinatorTerm()
	leaderSubterm := rule.GetRuleNumber().GetLeaderSubterm()

	// Stale write check: reject writes that would not advance the state.
	if update.termNumber < coordinatorTerm {
		return nil, nil, mterrors.Errorf(mtrpcpb.Code_INTERNAL,
			"rule update rejected for term %d: current_rule already at equal or higher position",
			update.termNumber)
	}

	// CAS check: if the caller provided an expected previous rule, verify it matches.
	if update.previousRule != nil {
		if coordinatorTerm != update.previousRule.coordinatorTerm ||
			leaderSubterm != update.previousRule.leaderSubterm {
			return nil, nil, errRuleConflict
		}
	}

	// Compute next leader_subterm: 0 when starting a new coordinator term, otherwise increment.
	var nextSubterm int64
	if update.termNumber > coordinatorTerm {
		nextSubterm = 0
	} else {
		nextSubterm = leaderSubterm + 1
	}

	return withRuleRowLock(ctx), &lockedRuleState{rule: rule, nextSubterm: nextSubterm}, nil
}

// ruleTransition captures the computed TransitionPolicy and resolved leader for a
// pending rule change. The pre-commit GUC uses the TransitionPolicy; the post-commit
// GUC uses TransitionPolicy.Incoming with TransitionPolicy.IncomingCohort.
type ruleTransition struct {
	policy consensus.TransitionPolicy
	leader *clustermetadatapb.ID
}

// buildTransitionPolicy computes the TransitionPolicy for a pending rule change.
//
// outgoing is the current rule state (from either a locked or plain SELECT). The
// TransitionPolicy is applied as the pre-commit "both" GUC (satisfying outgoing
// and incoming policies simultaneously). After the WAL write commits, the caller applies
// policy.Incoming with policy.IncomingCohort as the post-commit GUC.
//
// Returns nil when neither cohort nor policy is changing (no GUC update needed).
// When there is no prior policy (fresh system), Outgoing and OutgoingCohort are set
// equal to Incoming and IncomingCohort, giving an identity transition.
func (rs *ruleStore) buildTransitionPolicy(outgoing *clustermetadatapb.ShardRule, update *ruleUpdateBuilder) (*ruleTransition, error) {
	if update.cohortMembers == nil && update.durabilityPolicy == nil {
		return nil, nil
	}

	outgoingCohort := outgoing.GetCohortMembers()
	outgoingPolicyProto := outgoing.GetDurabilityPolicy()

	// Incoming (new) state: override outgoing values with update values.
	incomingCohort := outgoingCohort
	if update.cohortMembers != nil {
		incomingCohort = update.cohortMembers
	}
	incomingPolicyProto := outgoingPolicyProto
	if update.durabilityPolicy != nil {
		incomingPolicyProto = update.durabilityPolicy
	}
	if incomingPolicyProto == nil {
		return nil, nil // no policy in effect → no GUC
	}

	// Leader: prefer update value, fall back to outgoing rule.
	leader := outgoing.GetLeaderId()
	if update.leaderID != nil {
		leader = update.leaderID
	}

	incomingPolicy, err := consensus.NewPolicyFromProto(incomingPolicyProto)
	if err != nil {
		return nil, mterrors.Wrap(err, "failed to create incoming durability policy")
	}

	if outgoingPolicyProto == nil {
		// No prior policy: identity transition — both phases use the incoming policy.
		return &ruleTransition{
			policy: consensus.TransitionPolicy{
				Outgoing:       incomingPolicy,
				Incoming:       incomingPolicy,
				OutgoingCohort: incomingCohort,
				IncomingCohort: incomingCohort,
			},
			leader: leader,
		}, nil
	}

	outgoingPolicy, err := consensus.NewPolicyFromProto(outgoingPolicyProto)
	if err != nil {
		return nil, mterrors.Wrap(err, "failed to create outgoing durability policy")
	}

	return &ruleTransition{
		policy: consensus.TransitionPolicy{
			Outgoing:       outgoingPolicy,
			Incoming:       incomingPolicy,
			OutgoingCohort: outgoingCohort,
			IncomingCohort: incomingCohort,
		},
		leader: leader,
	}, nil
}

// observePosition reads the current rule and WAL LSN from postgres and returns
// the observed position. Returns nil if no rule has been applied yet
// (coordinator_term = 0).
//
// Returns an error if postgres is unreachable.
func (rs *ruleStore) observePosition(ctx context.Context) (*clustermetadatapb.PoolerPosition, error) {
	queryCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()

	pos, err := rs.readRuleRow(queryCtx, rs.queryService, false)
	if err != nil {
		return nil, mterrors.Wrap(err, "failed to query current position")
	}
	if pos != nil {
		rs.cacheRuleObservation(pos)
	}
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
func (rs *ruleStore) updateRule(ctx context.Context, update *ruleUpdateBuilder) (*clustermetadatapb.PoolerPosition, error) {
	// if update.force {
	// 	// Force mode skips history recording entirely. Force operations are emergency
	// 	// operations that must configure replication GUCs regardless. The write would
	// 	// block on sync replication with unreachable standbys, consuming the parent
	// 	// context's deadline and causing subsequent GUC changes to fail.
	// 	rs.logger.InfoContext(ctx, "Skipping rule update in force mode",
	// 		"coordinator_term", update.termNumber,
	// 		"event_type", update.eventType)
	// 	return nil, nil
	// }

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

	// Pre-promote phase: if the caller provided a promotion hook, the node is known
	// to be a standby (SELECT FOR UPDATE is not allowed in hot_standby mode). Apply
	// the transition GUC now so that once pg_promote() completes, the primary
	// immediately enforces the correct standby requirements. observePosition does a
	// plain SELECT, which is safe on a standby and does not require a transaction.
	if update.promotionHook != nil {
		if rs.syncStandby == nil {
			return nil, mterrors.Errorf(mtrpcpb.Code_INTERNAL,
				"promotionHook provided but SyncStandbyManager is not configured")
		}
		prePos, err := rs.observePosition(execCtx)
		if err != nil {
			return nil, mterrors.Wrap(err, "pre-promote: failed to read current rule")
		}
		if prePos == nil {
			return nil, mterrors.Errorf(mtrpcpb.Code_INTERNAL, "pre-promote: current rule row not found")
		}
		rt, err := rs.buildTransitionPolicy(prePos.GetRule(), update)
		if err != nil {
			return nil, mterrors.Wrap(err, "pre-promote: failed to build transition policy")
		}
		if rt != nil {
			prePromoteCtx := withPrePromoteToken(execCtx)
			if err := rs.syncStandby.SetPolicy(prePromoteCtx, &rt.policy, nil, rt.leader); err != nil {
				return nil, mterrors.Wrap(err, "pre-promote GUC apply failed")
			}
		}
		if err := update.promotionHook(execCtx); err != nil {
			return nil, mterrors.Wrap(err, "promotion hook failed")
		}
	}

	var pos *clustermetadatapb.PoolerPosition
	var postCommitPolicy consensus.DurabilityPolicy
	var postCommitCohort []*clustermetadatapb.ID
	var postCommitLeader *clustermetadatapb.ID
	// lockedCtx carries the ruleRowLock token set by lockCurrentRule and is used
	// for both the pre-commit and post-commit SetPolicy calls.
	var lockedCtx context.Context

	err := rs.queryService.Transact(execCtx, func(tx executor.InternalQueryService) error {
		// Step 1: Lock current rule and read its state. FOR UPDATE serializes concurrent
		// writes at the database level, complementing the action lock held by the caller.
		var locked *lockedRuleState
		var err error
		lockedCtx, locked, err = rs.lockCurrentRule(execCtx, tx, update)
		if err != nil {
			return err
		}

		// Step 2: Apply the "both" GUC before the WAL write. This ensures the rule change
		// record itself is acknowledged by standbys valid under both the outgoing and incoming
		// policies. SetPolicy is idempotent when the config has not changed.
		rt, err := rs.buildTransitionPolicy(locked.rule, update)
		if err != nil {
			return err
		}
		if rt != nil {
			if rs.syncStandby == nil {
				return mterrors.Errorf(mtrpcpb.Code_INTERNAL,
					"durability policy requires SyncStandbyManager, but it is not configured")
			}
			if err := rs.syncStandby.SetPolicy(lockedCtx, &rt.policy, nil, rt.leader); err != nil {
				return mterrors.Wrap(err, "pre-commit GUC apply failed")
			}
			postCommitPolicy = rt.policy.Incoming
			postCommitCohort = rt.policy.IncomingCohort
			postCommitLeader = rt.leader
		}

		// Step 3: Write the new rule and append to history. The COMMIT (below, after
		// this closure returns) is where synchronous replication acknowledgment waits.
		//
		// Parameters are aliased in the "p" CTE so that the UPDATE and INSERT reference
		// them by name rather than by positional index.
		result, err := tx.QueryArgs(execCtx, `
			WITH
			  p AS (
			    SELECT $1::bytea        AS shard_id,
			           $2::bigint       AS coordinator_term,
			           $3::bigint       AS leader_subterm,
			           $4::text         AS leader_id,
			           $5::text         AS coordinator_id,
			           $6::text[]       AS cohort_members,
			           $7::text         AS dp_name,
			           $8::text         AS dp_quorum_type,
			           $9::int          AS dp_required_count,
			           $10::timestamptz AS created_at,
			           $11::text        AS event_type,
			           $12::text        AS wal_position,
			           $13::text        AS operation,
			           $14::text        AS reason,
			           $15::text[]      AS accepted_members
			  ),
			  updated AS (
			    UPDATE multigres.current_rule
			    SET coordinator_term          = p.coordinator_term,
			        leader_subterm            = p.leader_subterm,
			        leader_id                 = COALESCE(NULLIF(p.leader_id, ''), current_rule.leader_id),
			        coordinator_id            = NULLIF(p.coordinator_id, ''),
			        cohort_members            = COALESCE(p.cohort_members, current_rule.cohort_members),
			        durability_policy_name    = COALESCE(NULLIF(p.dp_name, ''), current_rule.durability_policy_name),
			        durability_quorum_type    = COALESCE(NULLIF(p.dp_quorum_type, ''), current_rule.durability_quorum_type),
			        durability_required_count = COALESCE(p.dp_required_count, current_rule.durability_required_count),
			        created_at                = p.created_at
			    FROM p
			    WHERE current_rule.shard_id = p.shard_id
			    RETURNING current_rule.coordinator_term, current_rule.leader_subterm,
			              current_rule.leader_id, current_rule.coordinator_id, current_rule.cohort_members,
			              current_rule.durability_policy_name, current_rule.durability_quorum_type,
			              current_rule.durability_required_count
			  ),
			  inserted AS (
			    INSERT INTO multigres.rule_history
			      (coordinator_term, leader_subterm, event_type, leader_id, coordinator_id,
			       wal_position, operation, reason, cohort_members, accepted_members,
			       durability_policy_name, durability_quorum_type, durability_required_count, created_at)
			    SELECT updated.coordinator_term, updated.leader_subterm,
			           p.event_type,
			           updated.leader_id, updated.coordinator_id,
			           NULLIF(p.wal_position, ''), NULLIF(p.operation, ''), p.reason,
			           updated.cohort_members,
			           p.accepted_members,
			           updated.durability_policy_name, updated.durability_quorum_type,
			           updated.durability_required_count,
			           p.created_at
			    FROM updated, p
			    RETURNING coordinator_term
			  )
			-- Cross-joining inserted ensures a zero-row history insert (a bug) also returns zero
			-- rows here, causing the caller to surface an error rather than silently succeeding.
			SELECT updated.coordinator_term, updated.leader_subterm,
			       updated.leader_id, updated.coordinator_id, updated.cohort_members,
			       updated.durability_policy_name, updated.durability_quorum_type,
			       updated.durability_required_count,
			       CASE
			         WHEN pg_is_in_recovery()
			           THEN COALESCE(pg_last_wal_receive_lsn(), pg_last_wal_replay_lsn())
			         ELSE pg_current_wal_lsn()
			       END::text AS current_lsn
			FROM updated, inserted`,
			[]byte("0"),        // $1  shard_id
			update.termNumber,  // $2  coordinator_term
			locked.nextSubterm, // $3  leader_subterm
			leaderStr,          // $4  leader_id
			coordinatorIDStr,   // $5  coordinator_id
			cohortParam,        // $6  cohort_members
			dpName,             // $7  dp_name
			dpQuorumType,       // $8  dp_quorum_type
			dpRequiredCount,    // $9  dp_required_count
			update.createdAt,   // $10 created_at
			update.eventType,   // $11 event_type
			update.walPosition, // $12 wal_position
			update.operation,   // $13 operation
			update.reason,      // $14 reason
			acceptedParam,      // $15 accepted_members
		)
		if err != nil {
			return mterrors.Wrap(err, "failed to write rule history record")
		}

		if len(result.Rows) == 0 {
			// Should not happen: we just locked the row. If the UPDATE returns 0 rows the
			// cross-join with inserted also produces 0 rows, landing here. This is a bug.
			return mterrors.Errorf(mtrpcpb.Code_INTERNAL,
				"rule update produced no result after successful lock (term %d)", update.termNumber)
		}

		var coordinatorTerm, leaderSubterm int64
		var leaderIDStr *string
		var coordinatorIDStrResult string
		var cohortNames []string
		var durabilityPolicyName, durabilityQuorumType *string
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
			&lsn,
		); err != nil {
			return mterrors.Wrap(err, "failed to scan written rule position")
		}

		newPos, err := buildPoolerPosition(
			coordinatorTerm, leaderSubterm,
			leaderIDStr, coordinatorIDStrResult, cohortNames,
			durabilityPolicyName, durabilityQuorumType, durabilityRequiredCount,
			lsn,
		)
		if err != nil {
			return mterrors.Wrap(err, "failed to parse written rule position")
		}
		pos = newPos
		return nil
	})
	if err != nil {
		return nil, err
	}

	// Step 4: Apply the "after" GUC post-commit. The WAL write has already been
	// acknowledged by standbys, so the rule change is durable. Now switch to the
	// incoming policy alone (no longer need to satisfy both simultaneously).
	if postCommitPolicy != nil {
		if err := rs.syncStandby.SetPolicy(lockedCtx, postCommitPolicy, postCommitCohort, postCommitLeader); err != nil {
			return pos, mterrors.Wrap(err, "post-commit GUC apply failed")
		}
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

// buildPoolerPosition constructs a *clustermetadatapb.PoolerPosition from raw DB column values.
// leaderIDStr and coordinatorIDStr are app-name formatted strings (e.g. "zone1_pooler-name").
// durability fields are nil when not set in the DB.
func buildPoolerPosition(
	coordinatorTerm, leaderSubterm int64,
	leaderIDStr *string,
	coordinatorIDStr string,
	cohortNames []string,
	durabilityPolicyName, durabilityQuorumType *string,
	durabilityRequiredCount *int64,
	lsn string,
) (*clustermetadatapb.PoolerPosition, error) {
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

	if durabilityPolicyName != nil || durabilityQuorumType != nil || durabilityRequiredCount != nil {
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
		rule.DurabilityPolicy = dp
	}

	return &clustermetadatapb.PoolerPosition{
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
