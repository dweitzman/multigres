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
	"log/slog"
	"sync"
	"time"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/sqltypes"
	"github.com/multigres/multigres/go/common/timeouts"
	"github.com/multigres/multigres/go/common/topoclient"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	"github.com/multigres/multigres/go/services/multipooler/executor"
)

// ruleStore manages the current shard rule in postgres and maintains an
// in-memory cache of the last observed node position (rule + WAL LSN).
//
// All DB operations that write or read the current rule go through ruleStore,
// ensuring the position cache is always updated as a side effect. The cache
// can be read without I/O via cachedPosition().
//
// onChange fires after any operation that changes the cached rule number,
// allowing callers (e.g. the health streamer) to react to rule transitions.
// LSN-only updates are silent.
type ruleStore struct {
	logger    *slog.Logger
	query     func(ctx context.Context, sql string) (*sqltypes.Result, error)
	queryArgs func(ctx context.Context, sql string, args ...any) (*sqltypes.Result, error)

	mu       sync.Mutex
	lastPos  *nodePosition
	onChange func()
}

// newRuleStore creates a ruleStore. onChange is called (outside the lock) after
// any operation that changes the cached rule number.
func newRuleStore(
	logger *slog.Logger,
	query func(context.Context, string) (*sqltypes.Result, error),
	queryArgs func(context.Context, string, ...any) (*sqltypes.Result, error),
	onChange func(),
) *ruleStore {
	return &ruleStore{
		logger:    logger,
		query:     query,
		queryArgs: queryArgs,
		onChange:  onChange,
	}
}

// setOnChange sets the callback fired after any operation that changes the
// cached rule number. Must be called before the first DB operation.
func (rs *ruleStore) setOnChange(fn func()) {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	rs.onChange = fn
}

// cachedPosition returns the last observed node position without any I/O.
// Returns nil if no position has been observed yet.
func (rs *ruleStore) cachedPosition() *nodePosition {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	return rs.lastPos
}

// setPosition updates the cache and fires onChange if the rule number changed.
// Must NOT be called with rs.mu held.
func (rs *ruleStore) setPosition(pos *nodePosition) {
	rs.mu.Lock()
	ruleChanged := rs.lastPos == nil ||
		rs.lastPos.rule.CoordinatorTerm != pos.rule.CoordinatorTerm ||
		rs.lastPos.rule.RuleSubterm != pos.rule.RuleSubterm
	rs.lastPos = pos
	var notify func()
	if ruleChanged {
		notify = rs.onChange
	}
	rs.mu.Unlock()
	if notify != nil {
		notify()
	}
}

// observePosition reads the current rule and WAL LSN from postgres, updates
// the position cache, and returns the observed position. Returns nil if no
// rule has been applied yet (coordinator_term = 0).
//
// Returns an error if postgres is unreachable.
func (rs *ruleStore) observePosition(ctx context.Context) (*nodePosition, error) {
	queryCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()

	result, err := rs.queryArgs(queryCtx, `
		SELECT coordinator_term, rule_subterm, leader_id, coordinator_id, cohort_members,
		       durability_policy_name, durability_quorum_type, durability_required_count,
		       durability_async_fallback, created_at,
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

	var rec ruleHistoryRecord
	var leaderIDStr *string
	var cohortNames []string
	var lsn string
	if err := executor.ScanRow(result.Rows[0],
		&rec.CoordinatorTerm,
		&rec.RuleSubterm,
		&leaderIDStr,
		&rec.CoordinatorID,
		&cohortNames,
		&rec.DurabilityPolicyName,
		&rec.DurabilityQuorumType,
		&rec.DurabilityRequiredCount,
		&rec.DurabilityAsyncFallback,
		&rec.CreatedAt,
		&lsn,
	); err != nil {
		return nil, mterrors.Wrap(err, "failed to scan current position")
	}
	if err := scanRuleHistoryRow(&rec, leaderIDStr, cohortNames, nil); err != nil {
		return nil, mterrors.Wrap(err, "failed to parse current position")
	}

	// coordinator_term=0 is the sentinel initial state; no rule has been applied yet.
	if rec.CoordinatorTerm == 0 {
		return nil, nil
	}

	pos := &nodePosition{rule: &rec, lsn: lsn}
	rs.setPosition(pos)
	return pos, nil
}

// updateRule atomically writes a new rule by updating multigres.current_rule and
// appending to multigres.rule_history in a single CTE statement.
//
// The rule_subterm is assigned as:
//   - 0 if termNumber is greater than the current coordinator_term (new term)
//   - current rule_subterm + 1 if termNumber equals the current coordinator_term
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
func (rs *ruleStore) updateRule(ctx context.Context, update *ruleUpdateBuilder) (*nodePosition, error) {
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
		previousSubterm = &update.previousRule.ruleSubterm
	}

	// Use the remote operation timeout for history writes. This write validates that synchronous
	// replication is functioning - it must wait long enough for standbys to connect and acknowledge.
	execCtx, cancel := context.WithTimeout(ctx, timeouts.RemoteOperationTimeout)
	defer cancel()

	result, err := rs.queryArgs(execCtx, `
		WITH
		  locked AS (
		    SELECT coordinator_term, rule_subterm, leader_id, cohort_members
		    FROM multigres.current_rule
		    WHERE shard_id = $1
		      AND ($11::bigint IS NULL OR (coordinator_term = $11 AND rule_subterm = $12))
		    FOR UPDATE
		  ),
		  next_sub AS (
		    SELECT CASE
		      WHEN $2 > locked.coordinator_term THEN 0
		      ELSE locked.rule_subterm + 1
		    END AS value
		    FROM locked
		  ),
		  updated AS (
		    UPDATE multigres.current_rule
		    SET coordinator_term = $2,
		        rule_subterm     = next_sub.value,
		        leader_id        = COALESCE(NULLIF($4, ''), locked.leader_id),
		        coordinator_id   = NULLIF($5, ''),
		        cohort_members   = COALESCE($9, locked.cohort_members),
		        created_at       = now()
		    FROM next_sub, locked
		    WHERE shard_id = $1
		      AND ($2 > locked.coordinator_term OR ($2 = locked.coordinator_term AND next_sub.value > locked.rule_subterm))
		    RETURNING current_rule.coordinator_term, current_rule.rule_subterm
		  )
		INSERT INTO multigres.rule_history
		  (coordinator_term, rule_subterm, event_type, leader_id, coordinator_id,
		   wal_position, operation, reason, cohort_members, accepted_members)
		SELECT updated.coordinator_term, updated.rule_subterm,
		       $3,
		       COALESCE(NULLIF($4, ''), locked.leader_id),
		       NULLIF($5, ''),
		       NULLIF($6, ''), NULLIF($7, ''), $8,
		       COALESCE($9, locked.cohort_members),
		       $10
		FROM updated, locked
		RETURNING coordinator_term, rule_subterm, event_type, leader_id, coordinator_id,
		          wal_position, operation, reason, cohort_members, accepted_members,
		          NULL::text AS durability_policy_name,
		          NULL::text AS durability_quorum_type,
		          NULL::int  AS durability_required_count,
		          NULL::text AS durability_async_fallback,
		          created_at,
		          CASE
		            WHEN pg_is_in_recovery()
		              THEN COALESCE(pg_last_wal_receive_lsn(), pg_last_wal_replay_lsn())
		            ELSE pg_current_wal_lsn()
		          END::text AS current_lsn`,
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

	var rec ruleHistoryRecord
	var leaderIDStr *string
	var cohortNames, acceptedNames []string
	var lsn string
	if err := executor.ScanSingleRow(result,
		&rec.CoordinatorTerm,
		&rec.RuleSubterm,
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
		&rec.DurabilityRequiredCount,
		&rec.DurabilityAsyncFallback,
		&rec.CreatedAt,
		&lsn,
	); err != nil {
		return nil, mterrors.Wrap(err, "failed to scan written rule history record")
	}
	if err := scanRuleHistoryRow(&rec, leaderIDStr, cohortNames, acceptedNames); err != nil {
		return nil, mterrors.Wrap(err, "failed to parse written rule history record")
	}

	pos := &nodePosition{rule: &rec, lsn: lsn}
	rs.setPosition(pos)
	return pos, nil
}
