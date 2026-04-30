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
	"fmt"

	commonconsensus "github.com/multigres/multigres/go/common/consensus"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
)

// syncStandbySentinel is the synchronous_standby_names value written by Clear()
// to block primary commits. No real standby application name matches this value,
// so any commit on a primary where Clear() has been called will wait indefinitely
// until Apply() is called with a real standby configuration.
const syncStandbySentinel = "ANY 1 (multigres_demotion_sentinel)"

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

	// Clear writes the demotion sentinel to synchronous_standby_names so that
	// commits on a primary cannot succeed. Used when this node is being demoted
	// or decommissioned to prevent writes from going through until a new Apply()
	// call establishes a real standby list.
	Clear(ctx context.Context) error
}

// postgresqlSyncStandbyManager implements SyncStandbyManager by delegating to
// the manager's low-level GUC helpers.
type postgresqlSyncStandbyManager struct {
	pm *MultiPoolerManager
}

// SetPolicy computes the Postgres GUC configuration for the given durability policy
// and applies it. The cohort is passed directly to the policy; future work will
// reorder it by replication fitness from pg_stat_replication before this call.
func (s *postgresqlSyncStandbyManager) SetPolicy(ctx context.Context, policy commonconsensus.DurabilityPolicy, cohort []*clustermetadatapb.ID, leader *clustermetadatapb.ID) error {
	if err := assertRuleRowLockedSinceActionLock(ctx); err != nil {
		return fmt.Errorf("SetPolicy: %w", err)
	}
	// TODO: Order the cohort by fitness (replication lag)
	cfg, err := policy.BuildLeaderDurabilityPostgresConfig(s.pm.logger, cohort, leader)
	if err != nil {
		return fmt.Errorf("apply: build GUC config: %w", err)
	}

	standbyNames, err := validateSyncReplicationParams(int32(cfg.NumSync), cfg.SyncStandbyIDs)
	if err != nil {
		return err
	}
	// TODO: Read current_setting('synchronous_commit') and current_setting('synchronous_standby_names')
	// before writing. If both match cfg.SyncCommit and the computed standbyNames, skip the ALTER SYSTEM
	// calls entirely. This avoids unnecessary GUC writes and pg_reload_conf round-trips on paths that
	// call SetPolicy idempotently (e.g. the pre-commit and post-commit calls in updateRule when the
	// policy has not changed).
	if err := s.pm.setSynchronousCommit(ctx, cfg.SyncCommit); err != nil {
		return err
	}
	if err := s.pm.setSynchronousStandbyNames(ctx, cfg.SyncMethod, int32(cfg.NumSync), standbyNames); err != nil {
		return err
	}
	// TODO: After pg_reload_conf(), wait until the new synchronous_standby_names value is visible in
	// pg_show_all_settings() (or current_setting()) so that future commits issued immediately after
	// SetPolicy returns are guaranteed to observe the updated GUC. Without this wait, there is a small
	// window where a commit on the primary may still use the previous standby list if the SIGHUP has
	// not yet been processed by the postmaster and backend processes.
	return nil
}

// Clear writes the demotion sentinel so that primary commits cannot succeed.
// synchronous_commit is set to ON to ensure commits require standby
// acknowledgment, and synchronous_standby_names is set to a sentinel value
// that no real standby will ever match.
func (s *postgresqlSyncStandbyManager) Clear(ctx context.Context) error {
	if err := s.pm.setSynchronousCommit(ctx, multipoolermanagerdatapb.SynchronousCommitLevel_SYNCHRONOUS_COMMIT_ON); err != nil {
		return fmt.Errorf("clear: failed to set synchronous_commit: %w", err)
	}
	return s.pm.applySynchronousStandbyNames(ctx, syncStandbySentinel)
}
