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
type SyncStandbyManager interface {
	// SetPolicy computes and applies the Postgres GUC for the given policy.
	// pc.Cohort is the full participant set (leader included). Returns an error if
	// the policy produces no eligible standbys; use Clear instead in that case.
	//
	// ctx must carry either a ruleRowLock token (from lockCurrentRule on a
	// primary) or a prePromote token (from lockCurrentRule on a standby), both
	// of which are set automatically by ruleStore.updateRule.
	SetPolicy(ctx context.Context, pc commonconsensus.PolicyWithCohort, leader *clustermetadatapb.ID) error

	// Clear resets synchronous_standby_names to its default (empty) value and
	// invalidates the in-memory cache. It must only be called after postgres has
	// entered recovery mode (pg_is_in_recovery() = true); calling it on a primary
	// would allow commits to proceed without standby acknowledgment.
	Clear(ctx context.Context) error
}

// noopSyncStandbyManager implements SyncStandbyManager with no-op operations.
// Used in tests that do not need GUC verification.
type noopSyncStandbyManager struct{}

func (noopSyncStandbyManager) SetPolicy(_ context.Context, _ commonconsensus.PolicyWithCohort, _ *clustermetadatapb.ID) error {
	return nil
}

func (noopSyncStandbyManager) Clear(_ context.Context) error {
	return nil
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

func (s *postgresqlSyncStandbyManager) setStandbyNames(ctx context.Context, method multipoolermanagerdatapb.SynchronousMethod, numSync int32, names []poolerID) error {
	value, err := buildSynchronousStandbyNamesValue(method, numSync, names)
	if err != nil {
		return err
	}
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
func (s *postgresqlSyncStandbyManager) SetPolicy(ctx context.Context, pc commonconsensus.PolicyWithCohort, leader *clustermetadatapb.ID) error {
	if err := assertPriorRuleWritesDrained(ctx); err != nil {
		return fmt.Errorf("SetPolicy: %w", err)
	}
	cfg, err := pc.Policy.BuildSyncReplicationConfig(s.logger, pc.Cohort, leader)
	if err != nil {
		return fmt.Errorf("SetPolicy: build GUC config: %w", err)
	}

	standbyNames, err := validateSyncReplicationParams(int32(cfg.NumSync), cfg.SyncStandbyIDs)
	if err != nil {
		return err
	}
	if len(standbyNames) == 0 {
		return errors.New("SetPolicy: policy produced no eligible standbys; use Clear to reset synchronous_standby_names")
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

// syncCommitString converts a SynchronousCommitLevel enum to the PostgreSQL GUC string.
func syncCommitString(level multipoolermanagerdatapb.SynchronousCommitLevel) (string, error) {
	switch level {
	case multipoolermanagerdatapb.SynchronousCommitLevel_SYNCHRONOUS_COMMIT_OFF:
		return "off", nil
	case multipoolermanagerdatapb.SynchronousCommitLevel_SYNCHRONOUS_COMMIT_LOCAL:
		return "local", nil
	case multipoolermanagerdatapb.SynchronousCommitLevel_SYNCHRONOUS_COMMIT_REMOTE_WRITE:
		return "remote_write", nil
	case multipoolermanagerdatapb.SynchronousCommitLevel_SYNCHRONOUS_COMMIT_ON:
		return "on", nil
	case multipoolermanagerdatapb.SynchronousCommitLevel_SYNCHRONOUS_COMMIT_REMOTE_APPLY:
		return "remote_apply", nil
	default:
		return "", fmt.Errorf("unknown synchronous_commit level: %v", level)
	}
}
