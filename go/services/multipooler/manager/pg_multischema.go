// Copyright 2025 Supabase, Inc.
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
	"time"

	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/common/mterrors"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/services/multipooler/executor"
)

// errRuleConflict is returned by updateRule when a withPreviousRule compare-and-swap
// check fails: the current rule's term/subterm did not match the expected values.
var errRuleConflict = errors.New("rule conflict: current rule version mismatch")

// ============================================================================
// Multigres Schema Operations
//
// This file contains methods for managing the multigres sidecar schema and
// its tables. These are operations that set up and maintain the multigres
// metadata within PostgreSQL.
// ============================================================================

// ----------------------------------------------------------------------------
// Schema Creation
// ----------------------------------------------------------------------------

// createSidecarSchema creates the multigres sidecar schema and all its tables.
//
// MVP Limitation: Currently, we only support the default tablegroup. This function
// validates that the multipooler is configured for the default tablegroup and will
// return an error otherwise.
//
// For the default tablegroup, this function also creates the multischema global
// tables (tablegroup, tablegroup_table, shard).
func (pm *MultiPoolerManager) createSidecarSchema(ctx context.Context) error {
	pm.logger.InfoContext(ctx, "Creating multigres sidecar schema")

	if err := pm.createSchema(ctx); err != nil {
		return err
	}

	if err := pm.createHeartbeatTable(ctx); err != nil {
		return err
	}

	if err := pm.createCurrentRuleTable(ctx); err != nil {
		return err
	}

	if err := pm.initializeCurrentRule(ctx); err != nil {
		return err
	}

	if err := pm.createRuleHistoryTable(ctx); err != nil {
		return err
	}

	// Create multischema global tables for the default tablegroup
	pm.logger.InfoContext(ctx, "Creating multischema global tables for default tablegroup")

	if err := pm.createTablegroup(ctx); err != nil {
		return err
	}

	if err := pm.createTablegroupTable(ctx); err != nil {
		return err
	}

	if err := pm.createShard(ctx); err != nil {
		return err
	}

	pm.logger.InfoContext(ctx, "Successfully created multigres sidecar schema")
	return nil
}

// initializeMultischemaData inserts the initial tablegroup and shard records.
//
// MVP Limitation: Currently, we only support the default tablegroup with shard "0-inf".
// This function validates these constraints and returns an error otherwise.
//
// TODO: In the future, tablegroup and shard insertion should be done via a dedicated
// RPC, and the bootstrap code should insert the tablegroup in the default primary
// pooler. For simplicity in the MVP, we do this as part of InitializePrimary since
// we only support a single tablegroup/shard for now.
func (pm *MultiPoolerManager) initializeMultischemaData(ctx context.Context) error {
	tableGroup := pm.multipooler.TableGroup
	shard := pm.multipooler.Shard

	// MVP validation: only default tablegroup with shard 0-inf is supported
	// This is an extra guardrail. Multipoolers shouldn't start unless they
	// are in the default tablegroup. However, we shouldn't be calling this function
	// by the time we support multiple tablegroups/shards.
	// This will ensure we make sure to remove this code when we get to that point.
	if err := constants.ValidateMVPTableGroupAndShard(tableGroup, shard); err != nil {
		return mterrors.Wrap(err, "MVP validation failed in initializeMultischemaData")
	}

	pm.logger.InfoContext(ctx, "Initializing multischema data",
		"tablegroup", tableGroup, "shard", shard)

	if err := pm.insertTablegroup(ctx, tableGroup); err != nil {
		return err
	}

	if err := pm.insertShard(ctx, tableGroup, shard); err != nil {
		return err
	}

	pm.logger.InfoContext(ctx, "Successfully initialized multischema data")
	return nil
}

// createSchema creates the multigres schema if it doesn't exist
func (pm *MultiPoolerManager) createSchema(ctx context.Context) error {
	execCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()
	if err := pm.exec(execCtx, "CREATE SCHEMA IF NOT EXISTS multigres"); err != nil {
		return mterrors.Wrap(err, "failed to create multigres schema")
	}
	return nil
}

// ----------------------------------------------------------------------------
// Table Creation
// ----------------------------------------------------------------------------

// createCurrentRuleTable creates the current_rule table.
// This table holds a single row per shard representing the current cluster rule.
// It is used as a locking target (SELECT FOR UPDATE) and a fast read path for
// current state, while rule_history provides the append-only audit log.
//
// Non-essential audit fields (event_type, wal_position, accepted_members, reason,
// operation) are stored only in rule_history to keep this table focused on
// operational state.
//
// created_at records when this particular rule was written, matching the
// corresponding rule_history.created_at for the same (coordinator_term, rule_subterm).
func (pm *MultiPoolerManager) createCurrentRuleTable(ctx context.Context) error {
	execCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()
	if err := pm.exec(execCtx, `CREATE TABLE IF NOT EXISTS multigres.current_rule (
		shard_id                  BYTEA PRIMARY KEY,
		coordinator_term          BIGINT NOT NULL,
		rule_subterm              BIGINT NOT NULL,
		leader_id                 TEXT,
		coordinator_id            TEXT,
		cohort_members            TEXT[] NOT NULL,
		durability_policy_name    TEXT,
		durability_quorum_type    TEXT,
		durability_required_count INT,
		durability_async_fallback TEXT,
		created_at                TIMESTAMPTZ NOT NULL
	)`); err != nil {
		return mterrors.Wrap(err, "failed to create current_rule table")
	}
	return nil
}

// initializeCurrentRule inserts the zero-state sentinel row for the default shard.
// This row must exist before any rule is written so that insertHistoryRecord can
// SELECT FOR UPDATE on it to serialise concurrent writes.
// coordinator_term=0 means no rule has been applied yet.
// Uses ON CONFLICT DO NOTHING so it is safe to call multiple times.
func (pm *MultiPoolerManager) initializeCurrentRule(ctx context.Context) error {
	execCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()
	err := pm.execArgs(execCtx, `
		INSERT INTO multigres.current_rule
		  (shard_id, coordinator_term, rule_subterm, cohort_members, created_at)
		VALUES ($1, 0, 0, '{}', now())
		ON CONFLICT (shard_id) DO NOTHING`,
		[]byte("0"))
	if err != nil {
		return mterrors.Wrap(err, "failed to initialize current_rule")
	}
	return nil
}

// createHeartbeatTable creates the heartbeat table for leader election
func (pm *MultiPoolerManager) createHeartbeatTable(ctx context.Context) error {
	execCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()
	if err := pm.exec(execCtx, `CREATE TABLE IF NOT EXISTS multigres.heartbeat (
		shard_id BYTEA PRIMARY KEY,
		leader_id TEXT NOT NULL,
		ts BIGINT NOT NULL
	)`); err != nil {
		return mterrors.Wrap(err, "failed to create heartbeat table")
	}
	return nil
}

// createRuleHistoryTable creates the rule_history table.
// Each row records a cluster state change (promotion, cohort membership, durability policy).
// The composite primary key (coordinator_term, rule_subterm) uniquely identifies each rule;
// rule_subterm is assigned by the application as MAX(rule_subterm)+1 within a coordinator_term.
func (pm *MultiPoolerManager) createRuleHistoryTable(ctx context.Context) error {
	execCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()
	if err := pm.exec(execCtx, `CREATE TABLE IF NOT EXISTS multigres.rule_history (
		coordinator_term          BIGINT NOT NULL,
		rule_subterm              BIGINT NOT NULL,
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
		created_at                TIMESTAMPTZ NOT NULL DEFAULT now(),
		PRIMARY KEY (coordinator_term, rule_subterm)
	)`); err != nil {
		return mterrors.Wrap(err, "failed to create rule_history table")
	}

	return nil
}

// ----------------------------------------------------------------------------
// Multischema Global Tables (default tablegroup only)
// ----------------------------------------------------------------------------

// createTablegroup creates the tablegroup table for tracking table groups
func (pm *MultiPoolerManager) createTablegroup(ctx context.Context) error {
	execCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()
	if err := pm.exec(execCtx, `CREATE TABLE IF NOT EXISTS multigres.tablegroup (
		oid BIGSERIAL PRIMARY KEY,
		name TEXT NOT NULL UNIQUE,
		type TEXT NOT NULL
	)`); err != nil {
		return mterrors.Wrap(err, "failed to create tablegroup table")
	}
	return nil
}

// createTablegroupTable creates the tablegroup_table table for tracking tables within tablegroups
func (pm *MultiPoolerManager) createTablegroupTable(ctx context.Context) error {
	execCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()
	if err := pm.exec(execCtx, `CREATE TABLE IF NOT EXISTS multigres.tablegroup_table (
		oid BIGSERIAL PRIMARY KEY,
		tablegroup_oid BIGINT NOT NULL REFERENCES multigres.tablegroup(oid),
		name TEXT NOT NULL,
		UNIQUE (tablegroup_oid, name)
	)`); err != nil {
		return mterrors.Wrap(err, "failed to create tablegroup_table table")
	}
	return nil
}

// createShard creates the shard table for tracking shards within tablegroups
func (pm *MultiPoolerManager) createShard(ctx context.Context) error {
	execCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()
	if err := pm.exec(execCtx, `CREATE TABLE IF NOT EXISTS multigres.shard (
		oid BIGSERIAL PRIMARY KEY,
		tablegroup_oid BIGINT NOT NULL REFERENCES multigres.tablegroup(oid),
		shard_name TEXT NOT NULL,
		key_range_start BYTEA NULL,
		key_range_end BYTEA NULL,
		UNIQUE (tablegroup_oid, shard_name)
	)`); err != nil {
		return mterrors.Wrap(err, "failed to create shard table")
	}
	return nil
}

// ----------------------------------------------------------------------------
// Data Operations
// ----------------------------------------------------------------------------

// insertTablegroup inserts a tablegroup record into the tablegroup table.
// Uses ON CONFLICT DO NOTHING to handle concurrent insertions gracefully.
// The type is hardcoded to "unsharded" for the MVP.
func (pm *MultiPoolerManager) insertTablegroup(ctx context.Context, name string) error {
	pm.logger.InfoContext(ctx, "Inserting tablegroup", "name", name)
	execCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()
	err := pm.execArgs(execCtx, `INSERT INTO multigres.tablegroup (name, type)
		VALUES ($1, 'unsharded')
		ON CONFLICT (name) DO NOTHING`, name)
	if err != nil {
		return mterrors.Wrap(err, "failed to insert tablegroup")
	}
	return nil
}

// insertShard inserts a shard record into the shard table.
// Returns an error if the tablegroup doesn't exist.
// Uses ON CONFLICT DO NOTHING on (tablegroup_oid, shard_name) to handle concurrent insertions gracefully.
func (pm *MultiPoolerManager) insertShard(ctx context.Context, tablegroupName string, shardName string) error {
	pm.logger.InfoContext(ctx, "Inserting shard", "tablegroup", tablegroupName, "shard", shardName)

	// First, fetch the tablegroup oid
	queryCtx, queryCancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer queryCancel()
	result, err := pm.queryArgs(queryCtx, "SELECT oid FROM multigres.tablegroup WHERE name = $1", tablegroupName)
	if err != nil {
		return mterrors.Wrap(err, "failed to find tablegroup: "+tablegroupName)
	}

	var tablegroupOid int64
	if err := executor.ScanSingleRow(result, &tablegroupOid); err != nil {
		return mterrors.Wrap(err, "failed to find tablegroup: "+tablegroupName)
	}

	// Insert the shard
	execCtx, execCancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer execCancel()
	err = pm.execArgs(execCtx, `INSERT INTO multigres.shard (tablegroup_oid, shard_name)
		VALUES ($1, $2)
		ON CONFLICT (tablegroup_oid, shard_name) DO NOTHING`, tablegroupOid, shardName)
	if err != nil {
		return mterrors.Wrap(err, "failed to insert shard")
	}

	return nil
}

// ruleHistoryRecord represents a row from multigres.rule_history or multigres.current_rule.
type ruleHistoryRecord struct {
	CoordinatorTerm         int64
	RuleSubterm             int64
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

// nodePosition is a snapshot of the node's position in the distributed system:
// the current rule and the WAL LSN observed at the same point in time.
type nodePosition struct {
	rule *ruleHistoryRecord
	lsn  string
}

// ruleNumber identifies a specific rule version by coordinator term and subterm.
type ruleNumber struct {
	coordinatorTerm int64
	ruleSubterm     int64
}

// ruleUpdateBuilder constructs the parameters for updateRule.
// Coordinator ID, event type, and reason are always required.
// Fields not set via builder methods retain their current value in current_rule.
type ruleUpdateBuilder struct {
	// required
	termNumber    int64
	coordinatorID *clustermetadatapb.ID
	eventType     string
	reason        string

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

func newRuleUpdate(termNumber int64, coordinatorID *clustermetadatapb.ID, eventType, reason string) *ruleUpdateBuilder {
	return &ruleUpdateBuilder{
		termNumber:    termNumber,
		coordinatorID: coordinatorID,
		eventType:     eventType,
		reason:        reason,
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
func (b *ruleUpdateBuilder) withPreviousRule(coordinatorTerm, ruleSubterm int64) *ruleUpdateBuilder {
	b.previousRule = &ruleNumber{coordinatorTerm: coordinatorTerm, ruleSubterm: ruleSubterm}
	return b
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
			return fmt.Errorf("failed to parse leader_id %q: %w", *leaderIDStr, err)
		}
		p := poolerID{id: id, appName: *leaderIDStr}
		rec.LeaderID = &p
	}
	cohort, err := parsePoolerIDStrings(cohortNames)
	if err != nil {
		return fmt.Errorf("failed to parse cohort_members: %w", err)
	}
	rec.CohortMembers = cohort

	accepted, err := parsePoolerIDStrings(acceptedNames)
	if err != nil {
		return fmt.Errorf("failed to parse accepted_members: %w", err)
	}
	rec.AcceptedMembers = accepted
	return nil
}

// queryRuleHistory returns the most recent rule_history records ordered by
// (coordinator_term DESC, rule_subterm DESC). limit controls the maximum number
// of records returned.
func (pm *MultiPoolerManager) queryRuleHistory(ctx context.Context, limit int) ([]ruleHistoryRecord, error) {
	queryCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()

	result, err := pm.queryArgs(queryCtx, `
		SELECT coordinator_term, rule_subterm, event_type, leader_id, coordinator_id,
		       wal_position, operation, reason, cohort_members, accepted_members,
		       durability_policy_name, durability_quorum_type, durability_required_count,
		       durability_async_fallback, created_at
		FROM multigres.rule_history
		ORDER BY coordinator_term DESC, rule_subterm DESC
		LIMIT $1`, limit)
	if err != nil {
		return nil, mterrors.Wrap(err, "failed to query rule_history")
	}

	records := make([]ruleHistoryRecord, 0, len(result.Rows))
	for _, row := range result.Rows {
		var rec ruleHistoryRecord
		var leaderIDStr *string
		var cohortNames, acceptedNames []string
		if err := executor.ScanRow(row,
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
		); err != nil {
			return nil, mterrors.Wrap(err, "failed to scan rule_history row")
		}
		if err := scanRuleHistoryRow(&rec, leaderIDStr, cohortNames, acceptedNames); err != nil {
			return nil, mterrors.Wrap(err, "failed to parse rule_history row")
		}
		records = append(records, rec)
	}

	return records, nil
}

// currentRuleRecord reads the current rule and WAL LSN, updates the position
// cache, and returns just the rule record for callers that don't need the LSN.
// Returns nil if no rule has been applied yet (coordinator_term = 0).
func (pm *MultiPoolerManager) currentRuleRecord(ctx context.Context) (*ruleHistoryRecord, error) {
	pos, err := pm.rules.observePosition(ctx)
	if err != nil {
		return nil, err
	}
	if pos == nil {
		return nil, nil
	}
	return pos.rule, nil
}

// updateRule delegates to ruleStore.updateRule. See ruleStore.updateRule for full docs.
func (pm *MultiPoolerManager) updateRule(ctx context.Context, update *ruleUpdateBuilder) (*nodePosition, error) {
	return pm.rules.updateRule(ctx, update)
}
