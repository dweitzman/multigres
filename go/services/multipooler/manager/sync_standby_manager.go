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

	commonconsensus "github.com/multigres/multigres/go/common/consensus"
	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/parser/ast"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
	"github.com/multigres/multigres/go/services/multipooler/executor"
)

// SyncStandbyManager owns all writes to the synchronous_standby_names GUC.
// Nobody sets the GUC directly; they ask this service to do it. This
// centralisation ensures the ordering of GUC changes relative to WAL writes is
// always correct for the transition type (add vs remove vs promotion).
//
// The manager is responsible for ordering the cohort by replication fitness
// (using pg_stat_replication) before calling into the durability policy. This
// allows the policy's representative-sample fallback to prefer the most
// caught-up standbys.
type SyncStandbyManager interface {
	// SetPolicy computes and applies the Postgres GUC for the given policy.
	// For TransitionPolicy, cohort is unused — both cohorts are embedded in the struct.
	// For simple policies (AtLeastN, MultiCell), cohort is the full participant set.
	// Future: manager re-applies dynamically as replication fitness changes.
	//
	// ctx must carry a ruleRowLock token (from lockCurrentRule) proving that
	// SELECT FOR UPDATE on current_rule was issued in this action-lock session.
	// Both locks are required: the action lock serializes concurrent GUC writers;
	// the row lock ensures the GUC change is consistent with the rule state read.
	SetPolicy(ctx context.Context, policy commonconsensus.DurabilityPolicy, cohort []*clustermetadatapb.ID, leader *clustermetadatapb.ID) error

	// Clear resets synchronous_standby_names to its default (empty) value and
	// invalidates the in-memory cache. It must only be called after postgres has
	// entered recovery mode (pg_is_in_recovery() = true); calling it on a primary
	// would allow commits to proceed without standby acknowledgment.
	Clear(ctx context.Context) error
}

// postgresqlSyncStandbyManager implements SyncStandbyManager against a live
// PostgreSQL instance. It is the sole writer of synchronous_commit and
// synchronous_standby_names; the in-memory cache is therefore always consistent
// with what was last applied.
type postgresqlSyncStandbyManager struct {
	logger *slog.Logger
	qs     executor.InternalQueryService

	mu               sync.Mutex
	lastSyncCommit   string // serialised GUC string ("on", "remote_apply", …); empty = unknown
	lastStandbyNames string // serialised GUC string ("FIRST 1 (…)"); empty = unknown
	// Future: add lastPolicy, lastCohort, lastLeader when committing the background reeval runner.
}

func newSyncStandbyManager(logger *slog.Logger, qs executor.InternalQueryService) *postgresqlSyncStandbyManager {
	return &postgresqlSyncStandbyManager{logger: logger, qs: qs}
}

func (s *postgresqlSyncStandbyManager) exec(ctx context.Context, sql string) error {
	_, err := s.qs.Query(ctx, sql)
	return err
}

func (s *postgresqlSyncStandbyManager) setSynchronousCommit(ctx context.Context, level multipoolermanagerdatapb.SynchronousCommitLevel) error {
	val, err := syncCommitString(level)
	if err != nil {
		return err
	}
	execCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()
	s.logger.InfoContext(ctx, "Setting synchronous_commit", "value", val)
	if err := s.exec(execCtx, fmt.Sprintf("ALTER SYSTEM SET synchronous_commit = '%s'", val)); err != nil {
		s.logger.ErrorContext(ctx, "Failed to set synchronous_commit", "error", err)
		return mterrors.Wrap(err, "failed to set synchronous_commit")
	}
	return nil
}

// applyStandbyNames writes a pre-computed synchronous_standby_names value via ALTER SYSTEM SET.
func (s *postgresqlSyncStandbyManager) applyStandbyNames(ctx context.Context, value string) error {
	s.logger.InfoContext(ctx, "Setting synchronous_standby_names", "value", value)
	execCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()
	sql := "ALTER SYSTEM SET synchronous_standby_names = " + ast.QuoteStringLiteral(value)
	if err := s.exec(execCtx, sql); err != nil {
		s.logger.ErrorContext(ctx, "Failed to set synchronous_standby_names", "error", err)
		return mterrors.Wrap(err, "failed to set synchronous_standby_names")
	}
	return nil
}

// setStandbyNames builds and applies synchronous_standby_names. An empty names
// list issues ALTER SYSTEM RESET instead of SET so the GUC reverts to its
// default rather than being explicitly set to empty string.
func (s *postgresqlSyncStandbyManager) setStandbyNames(ctx context.Context, method multipoolermanagerdatapb.SynchronousMethod, numSync int32, names []poolerID) error {
	if len(names) == 0 {
		execCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
		defer cancel()
		s.logger.InfoContext(ctx, "Clearing synchronous_standby_names (empty standby list)")
		if err := s.exec(execCtx, "ALTER SYSTEM RESET synchronous_standby_names"); err != nil {
			s.logger.ErrorContext(ctx, "Failed to clear synchronous_standby_names", "error", err)
			return mterrors.Wrap(err, "failed to clear synchronous_standby_names")
		}
		return nil
	}
	if numSync == 0 {
		numSync = 1
	}
	value, err := buildSynchronousStandbyNamesValue(method, numSync, names)
	if err != nil {
		return err
	}
	return s.applyStandbyNames(ctx, value)
}

func (s *postgresqlSyncStandbyManager) reloadConfig(ctx context.Context) error {
	s.logger.InfoContext(ctx, "Reloading PostgreSQL configuration")
	execCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()
	if err := s.exec(execCtx, "SELECT pg_reload_conf()"); err != nil {
		s.logger.ErrorContext(ctx, "Failed to reload configuration", "error", err)
		return mterrors.Wrap(err, "failed to reload PostgreSQL configuration")
	}
	return nil
}

// SetPolicy computes the Postgres GUC configuration for the given durability policy and applies it.
// Uses an in-memory cache of the last-written GUC strings to skip ALTER SYSTEM calls when the
// desired values haven't changed. This is safe because postgresqlSyncStandbyManager is the sole
// writer of synchronous_commit and synchronous_standby_names.
func (s *postgresqlSyncStandbyManager) SetPolicy(ctx context.Context, policy commonconsensus.DurabilityPolicy, cohort []*clustermetadatapb.ID, leader *clustermetadatapb.ID) error {
	if err := assertRuleRowLockedSinceActionLock(ctx); err != nil {
		return fmt.Errorf("SetPolicy: %w", err)
	}
	// TODO: Order the cohort by fitness (replication lag)
	cfg, err := policy.BuildLeaderDurabilityPostgresConfig(s.logger, cohort, leader)
	if err != nil {
		return fmt.Errorf("apply: build GUC config: %w", err)
	}

	standbyNames, err := validateSyncReplicationParams(int32(cfg.NumSync), cfg.SyncStandbyIDs)
	if err != nil {
		return err
	}

	wantCommit, err := syncCommitString(cfg.SyncCommit)
	if err != nil {
		return err
	}
	wantStandby, err := buildSynchronousStandbyNamesValue(cfg.SyncMethod, int32(cfg.NumSync), standbyNames)
	if err != nil {
		return err
	}

	s.mu.Lock()
	unchanged := s.lastSyncCommit == wantCommit && s.lastStandbyNames == wantStandby
	s.mu.Unlock()
	if unchanged {
		return nil
	}

	if err := s.setSynchronousCommit(ctx, cfg.SyncCommit); err != nil {
		return err
	}
	if err := s.setStandbyNames(ctx, cfg.SyncMethod, int32(cfg.NumSync), standbyNames); err != nil {
		return err
	}
	if err := s.reloadConfig(ctx); err != nil {
		return err
	}
	// pg_reload_conf sends SIGHUP to postgres backends asynchronously.
	// Sleep briefly to give pool backends time to reload before the next WAL write.
	// SHOW on a single connection is insufficient — it only confirms one backend
	// processed the SIGHUP; other pooled connections may still use the old value.
	time.Sleep(100 * time.Millisecond)

	s.mu.Lock()
	s.lastSyncCommit = wantCommit
	s.lastStandbyNames = wantStandby
	s.mu.Unlock()
	return nil
}

// Clear resets synchronous_standby_names to its default value and invalidates the
// in-memory cache. Called during demotion so that commits do not block on standbys
// that are no longer connected to this node.
func (s *postgresqlSyncStandbyManager) Clear(ctx context.Context) error {
	if err := AssertActionLockHeld(ctx); err != nil {
		return err
	}

	// Safety: clearing synchronous_standby_names on a primary would allow commits
	// to proceed without standby acknowledgment, violating durability guarantees.
	checkCtx, checkCancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer checkCancel()
	result, err := s.qs.Query(checkCtx, "SELECT pg_is_in_recovery()")
	if err != nil {
		return fmt.Errorf("clear: could not verify recovery mode: %w", err)
	}
	var inRecovery bool
	if err := executor.ScanSingleRow(result, &inRecovery); err != nil {
		return fmt.Errorf("clear: could not scan pg_is_in_recovery result: %w", err)
	}
	if !inRecovery {
		return errors.New("clear: postgres is not in recovery mode — refusing to clear synchronous_standby_names on a primary")
	}

	execCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := s.exec(execCtx, "ALTER SYSTEM RESET synchronous_standby_names"); err != nil {
		return fmt.Errorf("clear: failed to reset synchronous_standby_names: %w", err)
	}
	if err := s.reloadConfig(ctx); err != nil {
		return fmt.Errorf("clear: %w", err)
	}
	s.mu.Lock()
	s.lastSyncCommit = ""
	s.lastStandbyNames = ""
	s.mu.Unlock()
	return nil
}
