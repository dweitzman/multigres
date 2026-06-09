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
	"fmt"
	"strings"
	"time"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/parser/ast"
	"github.com/multigres/multigres/go/services/multipooler/internal/executor"
	"github.com/multigres/multigres/go/services/multipooler/internal/manager/consensus"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
)

// ============================================================================
// Synchronous Replication Configuration
//
// This file contains the synchronous-replication policy that the manager owns.
// The low-level PostgreSQL replication mechanism lives in the postgres engine
// (pm.pg); see internal/manager/pgquery.
// ============================================================================

// applySynchronousStandbyNames applies the synchronous_standby_names setting to PostgreSQL
func (pm *MultiPoolerManager) applySynchronousStandbyNames(ctx context.Context, value string) error {
	pm.logger.InfoContext(ctx, "Setting synchronous_standby_names", "value", value)

	execCtx, execCancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer execCancel()

	// ALTER SYSTEM SET doesn't support parameterized queries, so we use string formatting
	sql := "ALTER SYSTEM SET synchronous_standby_names = " + ast.QuoteStringLiteral(value)
	if err := pm.exec(execCtx, sql); err != nil {
		pm.logger.ErrorContext(ctx, "Failed to set synchronous_standby_names", "error", err)
		return mterrors.Wrap(err, "failed to set synchronous_standby_names")
	}

	return nil
}

// setSynchronousStandbyNames builds and sets the PostgreSQL synchronous_standby_names configuration
// Format: https://www.postgresql.org/docs/current/runtime-config-replication.html#GUC-SYNCHRONOUS-STANDBY-NAMES
// Examples:
//
//	FIRST 2 (standby1, standby2, standby3)
//	ANY 1 (standby1, standby2)
//
// Note: Use '*' to match all connected standbys, or specify explicit standby application_name values
// Application names are generated from multipooler IDs using the shared consensus.NewReplicaID helper
func (pm *MultiPoolerManager) setSynchronousStandbyNames(ctx context.Context, synchronousMethod multipoolermanagerdatapb.SynchronousMethod, numSync int32, names []consensus.ReplicaID) error {
	// If standby list is empty, clear synchronous_standby_names
	if len(names) == 0 {
		execCtx, execCancel := context.WithTimeout(ctx, 500*time.Millisecond)
		defer execCancel()

		pm.logger.InfoContext(ctx, "Clearing synchronous_standby_names (empty standby list)")
		if err := pm.exec(execCtx, "ALTER SYSTEM RESET synchronous_standby_names"); err != nil {
			pm.logger.ErrorContext(ctx, "Failed to clear synchronous_standby_names", "error", err)
			return mterrors.Wrap(err, "failed to clear synchronous_standby_names")
		}
		return nil
	}

	// If numSync was not provided, default to 1
	if numSync == 0 {
		numSync = 1
	}

	// Build the synchronous_standby_names value using the shared helper
	standbyNamesValue, err := consensus.BuildSynchronousStandbyNamesValue(synchronousMethod, numSync, names)
	if err != nil {
		return err
	}

	// Apply the setting
	return pm.applySynchronousStandbyNames(ctx, standbyNamesValue)
}

// parseSyncReplicationConfig builds the synchronous replication configuration
// from the raw synchronous_standby_names and synchronous_commit GUC values. It
// is a pure function (no query) so the parsing rules are unit-testable directly;
// the caller (getPrimaryStatusInternal) reads the GUCs.
func parseSyncReplicationConfig(syncStandbyNames, syncCommit string) (*multipoolermanagerdatapb.SynchronousReplicationConfiguration, error) {
	config := &multipoolermanagerdatapb.SynchronousReplicationConfiguration{}

	// Only parse standby names if not empty
	syncStandbyNames = strings.TrimSpace(syncStandbyNames)
	if syncStandbyNames != "" {
		syncConfig, err := parseSynchronousStandbyNames(syncStandbyNames)
		if err != nil {
			return nil, err
		}
		config.SynchronousMethod = syncConfig.Method
		config.NumSync = syncConfig.NumSync
		config.StandbyIds = syncConfig.StandbyIDs
		appNames, err := consensus.ToReplicaIDs(syncConfig.StandbyIDs)
		if err != nil {
			return nil, mterrors.Wrap(err, "failed to convert standby IDs to application names")
		}
		config.StandbyApplicationNames = consensus.ReplicaIDsToAppNames(appNames)
	}

	// Map synchronous_commit string to enum
	var syncCommitLevel multipoolermanagerdatapb.SynchronousCommitLevel
	switch strings.ToLower(syncCommit) {
	case "off":
		syncCommitLevel = multipoolermanagerdatapb.SynchronousCommitLevel_SYNCHRONOUS_COMMIT_OFF
	case "local":
		syncCommitLevel = multipoolermanagerdatapb.SynchronousCommitLevel_SYNCHRONOUS_COMMIT_LOCAL
	case "remote_write":
		syncCommitLevel = multipoolermanagerdatapb.SynchronousCommitLevel_SYNCHRONOUS_COMMIT_REMOTE_WRITE
	case "on":
		syncCommitLevel = multipoolermanagerdatapb.SynchronousCommitLevel_SYNCHRONOUS_COMMIT_ON
	case "remote_apply":
		syncCommitLevel = multipoolermanagerdatapb.SynchronousCommitLevel_SYNCHRONOUS_COMMIT_REMOTE_APPLY
	default:
		return nil, mterrors.New(mtrpcpb.Code_INVALID_ARGUMENT,
			fmt.Sprintf("unknown synchronous_commit value: %q", syncCommit))
	}
	config.SynchronousCommit = syncCommitLevel

	return config, nil
}

// clearSyncReplicationForDemotion clears synchronous replication settings at the start of demotion.
//
// When a stale primary comes back online after failover:
// 1. It still has synchronous_standby_names configured
// 2. No standbys are connected (they're all connected to the new primary)
// 3. Any writes (like heartbeat) block indefinitely waiting for sync acknowledgment
// 4. This blocks the demote flow and causes timeout
//
// ALTER SYSTEM writes to postgresql.auto.conf, not to WAL, so it doesn't need sync
// replication acknowledgment and won't block even with no standbys connected.
func (pm *MultiPoolerManager) clearSyncReplicationForDemotion(ctx context.Context) error {
	pm.logger.InfoContext(ctx, "Clearing synchronous replication for demotion (early)")

	// Use a short timeout - if this hangs, the demote will fail anyway
	execCtx, execCancel := context.WithTimeout(ctx, 5*time.Second)
	defer execCancel()

	// ALTER SYSTEM writes to postgresql.auto.conf (not WAL), so it doesn't require
	// sync replication acknowledgment and won't block.
	if err := pm.exec(execCtx, "ALTER SYSTEM RESET synchronous_standby_names"); err != nil {
		pm.logger.WarnContext(ctx, "Failed to clear synchronous_standby_names for demotion", "error", err)
		return mterrors.Wrap(err, "failed to clear synchronous_standby_names for demotion")
	}

	if err := pm.pg.ReloadConfig(ctx); err != nil {
		return mterrors.Wrap(err, "failed to reload configuration for demotion")
	}

	pm.logger.InfoContext(ctx, "Successfully cleared synchronous replication for demotion")
	return nil
}

// resetSynchronousReplication clears the synchronous standby list
// This should be called after the server is read-only to safely clear settings
func (pm *MultiPoolerManager) resetSynchronousReplication(ctx context.Context) error {
	pm.logger.InfoContext(ctx, "Clearing synchronous standby list")

	execCtx, execCancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer execCancel()

	// Clear synchronous_standby_names to remove all standbys
	if err := pm.exec(execCtx, "ALTER SYSTEM RESET synchronous_standby_names"); err != nil {
		pm.logger.ErrorContext(ctx, "Failed to clear synchronous_standby_names", "error", err)
		return mterrors.Wrap(err, "failed to clear synchronous_standby_names")
	}

	if err := pm.pg.ReloadConfig(ctx); err != nil {
		return mterrors.Wrap(err, "failed to reload configuration after clearing standby list")
	}

	pm.logger.InfoContext(ctx, "Successfully cleared synchronous standby list")
	return nil
}

// ----------------------------------------------------------------------------
// standbyUpdateOperationName maps a CohortUpdateOperation enum to a short string for logging/history.
func standbyUpdateOperationName(op multipoolermanagerdatapb.CohortUpdateOperation) string {
	switch op {
	case multipoolermanagerdatapb.CohortUpdateOperation_COHORT_UPDATE_OPERATION_ADD:
		return "add"
	case multipoolermanagerdatapb.CohortUpdateOperation_COHORT_UPDATE_OPERATION_REMOVE:
		return "remove"
	default:
		return "unknown"
	}
}

// Standby List Operations
// ----------------------------------------------------------------------------

// poolerIDSetEqual returns true if a and b contain the same set of pooler IDs
// (order-independent comparison using appName as the key).
func poolerIDSetEqual(a, b []consensus.ReplicaID) bool {
	if len(a) != len(b) {
		return false
	}
	m := make(map[string]struct{}, len(a))
	for _, p := range a {
		m[p.AppName()] = struct{}{}
	}
	for _, p := range b {
		if _, ok := m[p.AppName()]; !ok {
			return false
		}
	}
	return true
}

// ----------------------------------------------------------------------------
// Primary-side Replication Queries
// ----------------------------------------------------------------------------

// getConnectedFollowerIDs queries pg_stat_replication for connected followers and returns their IDs
func (pm *MultiPoolerManager) getConnectedFollowerIDs(ctx context.Context) ([]*clustermetadatapb.ID, error) {
	queryCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()
	sql := "SELECT application_name FROM pg_stat_replication WHERE application_name IS NOT NULL AND application_name != ''"
	result, err := pm.query(queryCtx, sql)
	if err != nil {
		pm.logger.ErrorContext(ctx, "Failed to query pg_stat_replication", "error", err)
		return nil, mterrors.Wrap(err, "failed to query connected followers")
	}

	followers := []*clustermetadatapb.ID{}
	if result != nil {
		for _, row := range result.Rows {
			appName, err := executor.GetString(row, 0)
			if err != nil {
				pm.logger.ErrorContext(ctx, "Failed to scan application_name", "error", err)
				return nil, mterrors.Wrap(err, "failed to scan application_name from pg_stat_replication")
			}
			// Parse application_name back to cluster ID
			followerID, err := consensus.ParseApplicationName(appName)
			if err != nil {
				pm.logger.ErrorContext(ctx, "Failed to parse application_name", "application_name", appName, "error", err)
				return nil, mterrors.Wrap(err, "failed to parse application_name: "+appName)
			}
			followers = append(followers, followerID)
		}
	}

	return followers, nil
}
