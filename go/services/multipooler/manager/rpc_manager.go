// Copyright 2025 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
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
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/multigres/multigres/go/common/consensus"
	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/common/eventlog"
	"github.com/multigres/multigres/go/common/mterrors"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	consensusdatapb "github.com/multigres/multigres/go/pb/consensusdata"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
	pgctldpb "github.com/multigres/multigres/go/pb/pgctldservice"
	"github.com/multigres/multigres/go/services/multipooler/poolerserver"
)

// broadcastHealth broadcasts the current health state to all subscribers.
//
// This should be called whenever there is a state change that clients should be
// aware of (e.g., PostgreSQL availability, replication status, etc.). Clients
// will receive the latest health snapshot immediately if they are connected, or
// upon their next connection if they are not currently connected.
func (pm *MultiPoolerManager) broadcastHealth() {
	if pm.healthStreamer != nil {
		pm.healthStreamer.Broadcast()
	}
}

// WaitForLSN waits for PostgreSQL server to reach a specific LSN position
func (pm *MultiPoolerManager) WaitForLSN(ctx context.Context, targetLsn string) error {
	if err := pm.checkReady(); err != nil {
		return err
	}

	// Check REPLICA guardrails (pooler type and recovery mode)
	if err := pm.checkReplicaGuardrails(ctx); err != nil {
		return err
	}

	// Wait for the standby to replay WAL up to the target LSN
	// We use a polling approach to check if the replay LSN has reached the target
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			pm.logger.ErrorContext(ctx, "WaitForLSN context cancelled or timed out",
				"target_lsn", targetLsn,
				"error", ctx.Err())
			return mterrors.Wrap(ctx.Err(), "context cancelled or timed out while waiting for LSN")

		case <-ticker.C:
			// Check if the standby has replayed up to the target LSN
			reachedTarget, err := pm.checkLSNReached(ctx, targetLsn)
			if err != nil {
				pm.logger.ErrorContext(ctx, "Failed to check replay LSN", "error", err)
				return err
			}

			if reachedTarget {
				pm.logger.InfoContext(ctx, "Standby reached target LSN", "target_lsn", targetLsn)
				return nil
			}
		}
	}
}

// SetPrimaryConnInfo sets the primary connection info for a standby server
func (pm *MultiPoolerManager) SetPrimaryConnInfo(ctx context.Context, primary *clustermetadatapb.MultiPooler, stopReplicationBefore, startReplicationAfter bool, currentTerm int64, force bool) error {
	if err := pm.checkReady(); err != nil {
		return err
	}

	// Acquire the action lock to ensure only one mutation runs at a time
	ctx, err := pm.actionLock.Acquire(ctx, "SetPrimaryConnInfo")
	if err != nil {
		return err
	}
	defer pm.actionLock.Release(ctx)

	// Validate and update consensus term following consensus rules
	if err = pm.validateAndUpdateTerm(ctx, currentTerm, force); err != nil {
		return err
	}

	// Extract host and port from the MultiPooler (nil means clear the config)
	var host string
	var port int32
	if primary != nil {
		host = primary.Hostname
		var ok bool
		port, ok = primary.PortMap["postgres"]
		if !ok {
			return mterrors.Errorf(mtrpcpb.Code_INVALID_ARGUMENT,
				"primary %s has no postgres port configured", primary.Id.Name)
		}
	}

	// Store primary pooler ID (nil if clearing)
	pm.mu.Lock()
	if primary != nil {
		pm.primaryPoolerID = primary.Id
		pm.primaryHost = host
		pm.primaryPort = port
	} else {
		pm.primaryPoolerID = nil
		pm.primaryHost = ""
		pm.primaryPort = 0
	}
	pm.mu.Unlock()

	// Call the locked version that assumes action lock is already held
	if err := pm.setPrimaryConnInfoLocked(ctx, host, port, stopReplicationBefore, startReplicationAfter); err != nil {
		return err
	}

	// Push an immediate health snapshot so orchestrators learn about the new
	// replication configuration (e.g., cleared primary_conninfo) without waiting
	// for the next 30-second heartbeat.
	pm.broadcastHealth()
	return nil
}

// setPrimaryConnInfoLocked sets the primary connection info for a standby server.
// This function assumes the action lock is already held by the caller.
func (pm *MultiPoolerManager) setPrimaryConnInfoLocked(ctx context.Context, host string, port int32, stopReplicationBefore, startReplicationAfter bool) error {
	if err := AssertActionLockHeld(ctx); err != nil {
		return err
	}

	if err := pm.checkReady(); err != nil {
		return err
	}

	// Guardrail: Check if the PostgreSQL instance is in recovery (standby mode)
	isPrimary, err := pm.isPrimary(ctx)
	if err != nil {
		pm.logger.ErrorContext(ctx, "Failed to check if instance is in recovery", "error", err)
		return mterrors.Wrap(err, "failed to check recovery status")
	}

	if isPrimary {
		pm.logger.ErrorContext(ctx, "SetPrimaryConnInfo called on non-standby instance", "service_id", pm.serviceID.String())
		return mterrors.New(mtrpcpb.Code_FAILED_PRECONDITION,
			fmt.Sprintf("operation not allowed: the PostgreSQL instance is not in standby mode (service_id: %s)", pm.serviceID.String()))
	}

	appName := pm.servicePoolerID

	// Optionally stop replication before making changes
	if stopReplicationBefore {
		_, err := pm.pauseReplication(ctx, multipoolermanagerdatapb.ReplicationPauseMode_REPLICATION_PAUSE_MODE_REPLAY_ONLY, false)
		if err != nil {
			return err
		}
	}

	// Build primary_conninfo connection string
	// Format: host=<host> port=<port> user=<user> application_name=<name>
	// The heartbeat_interval is converted to keepalives_interval/keepalives_idle
	user := constants.DefaultPostgresUser
	if pm.connPoolMgr != nil {
		user = pm.connPoolMgr.PgUser()
	}
	connInfo := fmt.Sprintf("host=%s port=%d user=%s application_name=%s",
		host, port, user, appName.appName)

	// Set primary_conninfo using ALTER SYSTEM
	if err = pm.setPrimaryConnInfo(ctx, connInfo); err != nil {
		return err
	}

	// Reload PostgreSQL configuration to apply changes
	if err = pm.reloadPostgresConfig(ctx); err != nil {
		return err
	}

	// Optionally start replication after making changes.
	// Note: If replication was already running when calling SetPrimaryConnInfo,
	// even if we don't set startReplicationAfter to true, replication will be running.
	if startReplicationAfter {
		// Wait for database to be available after restart
		if err := pm.waitForDatabaseConnection(ctx); err != nil {
			pm.logger.ErrorContext(ctx, "Failed to reconnect to database after restart", "error", err)
			return mterrors.Wrap(err, "failed to reconnect to database")
		}

		pm.logger.InfoContext(ctx, "Starting replication after setting primary_conninfo")
		if err := pm.resumeWALReplay(ctx); err != nil {
			return err
		}
	}

	pm.logger.InfoContext(ctx, "SetPrimaryConnInfo completed successfully",
		"host", host,
		"port", port,
		"stop_replication_before", stopReplicationBefore,
		"start_replication_after", startReplicationAfter)

	return nil
}

// StartReplication starts WAL replay on standby (calls pg_wal_replay_resume)
func (pm *MultiPoolerManager) StartReplication(ctx context.Context) error {
	if err := pm.checkReady(); err != nil {
		return err
	}

	// Acquire the action lock to ensure only one mutation runs at a time
	ctx, err := pm.actionLock.Acquire(ctx, "StartReplication")
	if err != nil {
		return err
	}
	defer pm.actionLock.Release(ctx)

	// Check REPLICA guardrails (pooler type and recovery mode)
	if err = pm.checkReplicaGuardrails(ctx); err != nil {
		return err
	}

	// Resume WAL replay on the standby
	if err := pm.resumeWALReplay(ctx); err != nil {
		return err
	}

	return nil
}

// StopReplication stops replication based on the specified mode
func (pm *MultiPoolerManager) StopReplication(ctx context.Context, mode multipoolermanagerdatapb.ReplicationPauseMode, wait bool) error {
	if err := pm.checkReady(); err != nil {
		return err
	}

	// Acquire the action lock to ensure only one mutation runs at a time
	ctx, err := pm.actionLock.Acquire(ctx, "StopReplication")
	if err != nil {
		return err
	}
	defer pm.actionLock.Release(ctx)

	// Check REPLICA guardrails (pooler type and recovery mode)
	if err = pm.checkReplicaGuardrails(ctx); err != nil {
		return err
	}

	_, err = pm.pauseReplication(ctx, mode, wait)
	if err != nil {
		return err
	}

	return nil
}

// StandbyReplicationStatus gets the current replication status of the standby
func (pm *MultiPoolerManager) StandbyReplicationStatus(ctx context.Context) (*multipoolermanagerdatapb.StandbyReplicationStatus, error) {
	if err := pm.checkReady(); err != nil {
		return nil, err
	}

	// Check REPLICA guardrails (pooler type and recovery mode)
	if err := pm.checkReplicaGuardrails(ctx); err != nil {
		return nil, err
	}

	// Query all replication status fields
	status, err := pm.queryReplicationStatus(ctx)
	if err != nil {
		pm.logger.ErrorContext(ctx, "Failed to get replication status", "error", err)
		return nil, err
	}

	return status, nil
}

// Status gets unified status that works for both PRIMARY and REPLICA poolers.
// This RPC works even when the database connection is unavailable - fields that require
// database access will be nil/empty in that case. This allows callers to always get
// initialization status without needing a separate RPC.
func (pm *MultiPoolerManager) Status(ctx context.Context) (*multipoolermanagerdatapb.Status, error) {
	// Build status with initialization fields (always available)
	poolerStatus := &multipoolermanagerdatapb.Status{
		PoolerType:       pm.getPoolerType(),
		IsInitialized:    pm.isInitialized(ctx),
		HasDataDirectory: pm.hasDataDirectory(),
		PostgresReady:    pm.isPostgresReady(ctx),
		PostgresRunning:  pm.isPostgresRunning(ctx),
		PostgresRole:     pm.getRole(ctx),
		ShardId:          pm.getShardID(),
	}

	if action, duration := pm.actionLock.ActiveAction(); action != multipoolermanagerdatapb.PostgresAction_POSTGRES_ACTION_UNSPECIFIED {
		poolerStatus.PostgresAction = action
		poolerStatus.PostgresActionDuration = durationpb.New(duration)
	}

	// Get term revocation (use inconsistent read for monitoring)
	if revocation, err := pm.consensusState.GetInconsistentRevocation(); err == nil {
		poolerStatus.TermRevocation = revocation
	}

	// Get WAL position (ignore errors, just return empty string)
	walPosition, _ := pm.getWALPosition(ctx)
	poolerStatus.WalPosition = walPosition

	// Get cohort members from the current rule (best-effort).
	if pos, err := pm.rules.observePosition(ctx); err != nil {
		pm.logger.WarnContext(ctx, "Failed to read current rule for status", "error", err)
	} else if pos != nil && pos.Rule != nil {
		poolerStatus.CohortMembers = pos.Rule.CohortMembers
	}

	// Try to get detailed status based on PostgreSQL role
	isPrimary, err := pm.isPrimary(ctx)
	if err != nil {
		// Can't determine role - return what we have
		pm.logger.WarnContext(ctx, "Failed to check PostgreSQL role, returning partial status", "error", err)
		return poolerStatus, nil
	}

	// Populate role-specific status
	if isPrimary {
		// Acting as primary - get primary status (skip guardrails since we already checked isPrimary)
		primaryStatus, err := pm.getPrimaryStatusInternal(ctx)
		if err != nil {
			pm.logger.WarnContext(ctx, "Failed to get primary status", "error", err)
			// Return partial status instead of error
			return poolerStatus, nil
		}
		poolerStatus.PrimaryStatus = primaryStatus
		return poolerStatus, nil
	}
	// Acting as standby - get replication status (skip guardrails since we already checked isPrimary)
	replStatus, err := pm.getStandbyStatusInternal(ctx)
	if err != nil {
		pm.logger.WarnContext(ctx, "Failed to get standby replication status", "error", err)
		// Return partial status instead of error
		return poolerStatus, nil
	}
	poolerStatus.ReplicationStatus = replStatus
	return poolerStatus, nil
}

// ResetReplication resets the standby's connection to its primary by clearing primary_conninfo
// and reloading PostgreSQL configuration. This effectively disconnects the replica from the primary
// and prevents it from acknowledging commits, making it unavailable for synchronous replication
// until reconfigured.
func (pm *MultiPoolerManager) ResetReplication(ctx context.Context) error {
	if err := pm.checkReady(); err != nil {
		return err
	}

	// Acquire the action lock to ensure only one mutation runs at a time
	ctx, err := pm.actionLock.Acquire(ctx, "ResetReplication")
	if err != nil {
		return err
	}
	defer pm.actionLock.Release(ctx)

	// Check REPLICA guardrails (pooler type and recovery mode)
	if err = pm.checkReplicaGuardrails(ctx); err != nil {
		return err
	}

	// Pause the receiver (clear primary_conninfo) and wait for disconnect
	_, err = pm.pauseReplication(ctx, multipoolermanagerdatapb.ReplicationPauseMode_REPLICATION_PAUSE_MODE_RECEIVER_ONLY, true /* wait */)
	if err != nil {
		return err
	}

	return nil
}

// UpdateSynchronousStandbyList updates PostgreSQL synchronous_standby_names by adding,
// removing, or replacing members. It derives the correct GUC value from the committed
// rule's durability policy rather than re-reading the current GUC format from postgres.
func (pm *MultiPoolerManager) UpdateSynchronousStandbyList(ctx context.Context, operation multipoolermanagerdatapb.StandbyUpdateOperation, standbyIDs []*clustermetadatapb.ID, reloadConfig bool, consensusTerm int64, force bool, coordinatorID *clustermetadatapb.ID) error {
	if err := pm.checkReady(); err != nil {
		return err
	}

	if operation == multipoolermanagerdatapb.StandbyUpdateOperation_STANDBY_UPDATE_OPERATION_UNSPECIFIED {
		return mterrors.New(mtrpcpb.Code_INVALID_ARGUMENT, "operation must be specified")
	}

	requestedPoolerIDs, err := validateStandbyIDs(standbyIDs)
	if err != nil {
		return err
	}

	leaderID := pm.servicePoolerID

	ctx, err = pm.actionLock.Acquire(ctx, "UpdateSynchronousStandbyList")
	if err != nil {
		return err
	}
	defer pm.actionLock.Release(ctx)

	if err = pm.checkPrimaryGuardrails(ctx); err != nil {
		return err
	}

	// Use the committed rule as the source of truth for the current cohort and policy.
	currentRule := pm.rules.cachedPosition().GetRule()
	if len(currentRule.GetCohortMembers()) == 0 {
		return mterrors.New(mtrpcpb.Code_FAILED_PRECONDITION,
			"synchronous replication is not configured")
	}

	policy, err := consensus.NewPolicyFromProto(currentRule.GetDurabilityPolicy())
	if err != nil {
		return mterrors.Wrap(err, "failed to parse durability policy from current rule")
	}

	currentPoolerIDs, err := toPoolerIDs(currentRule.GetCohortMembers())
	if err != nil {
		return mterrors.Wrap(err, "invalid current cohort")
	}

	var updatedPoolerIDs []poolerID
	switch operation {
	case multipoolermanagerdatapb.StandbyUpdateOperation_STANDBY_UPDATE_OPERATION_ADD:
		updatedPoolerIDs = applyAddOperation(currentPoolerIDs, requestedPoolerIDs)
	case multipoolermanagerdatapb.StandbyUpdateOperation_STANDBY_UPDATE_OPERATION_REMOVE:
		updatedPoolerIDs = applyRemoveOperation(currentPoolerIDs, requestedPoolerIDs)
	default:
		return mterrors.New(mtrpcpb.Code_INVALID_ARGUMENT, "unsupported operation: "+operation.String())
	}

	if len(updatedPoolerIDs) == 0 {
		return mterrors.New(mtrpcpb.Code_INVALID_ARGUMENT, "resulting standby list cannot be empty after operation")
	}

	updatedIDs := make([]*clustermetadatapb.ID, len(updatedPoolerIDs))
	for i, pid := range updatedPoolerIDs {
		updatedIDs[i] = pid.id
	}

	// Derive the new GUC string from the policy.
	currentValue, err := policy.StandbyNames(currentRule.GetCohortMembers(), idToAppName)
	if err != nil {
		return mterrors.Wrap(err, "failed to compute current standby names")
	}
	newValue, err := policy.StandbyNames(updatedIDs, idToAppName)
	if err != nil {
		return mterrors.Wrap(err, "failed to compute new standby names")
	}

	if currentValue == newValue {
		return nil
	}

	operationName := standbyUpdateOperationName(operation)

	// Insert history before applying GUCs so the new cohort is advertised
	// before this primary can accept ACKs from it.
	coordID := coordinatorID
	if coordID == nil {
		coordID = pm.serviceID
	}
	standbyUpdate := newRuleUpdate(
		consensusTerm,
		coordID,
		"replication_config",
		"UpdateSynchronousStandbyList: "+operationName,
		time.Now()).
		withLeader(leaderID.id).
		withCohort(updatedIDs).
		withOperation(operationName)
	if force {
		standbyUpdate.withForce()
	}
	if _, err := pm.rules.updateRule(ctx, standbyUpdate); err != nil {
		return mterrors.Wrap(err, "failed to record replication config history")
	}

	if err = pm.applySynchronousStandbyNames(ctx, newValue); err != nil {
		return err
	}

	if reloadConfig {
		if err := pm.reloadPostgresConfig(ctx); err != nil {
			return err
		}
	}

	pm.logger.InfoContext(ctx, "UpdateSynchronousStandbyList completed successfully",
		"operation", operation,
		"old_value", currentValue,
		"new_value", newValue,
		"reload_config", reloadConfig,
		"consensus_term", consensusTerm,
		"force", force)

	pm.broadcastHealth()
	return nil
}

// getPrimaryStatusInternal gets primary status without guardrail checks.
// Called by Status() which has already verified the PostgreSQL role.
func (pm *MultiPoolerManager) getPrimaryStatusInternal(ctx context.Context) (*multipoolermanagerdatapb.PrimaryStatus, error) {
	status := &multipoolermanagerdatapb.PrimaryStatus{}

	// Get current LSN
	lsn, err := pm.getPrimaryLSN(ctx)
	if err != nil {
		return nil, err
	}
	status.Lsn = lsn
	status.Ready = true

	// Get connected followers from pg_stat_replication
	followers, err := pm.getConnectedFollowerIDs(ctx)
	if err != nil {
		return nil, err
	}
	status.ConnectedFollowers = followers

	// Get synchronous replication configuration
	syncConfig, err := pm.getSynchronousReplicationConfig(ctx)
	if err != nil {
		return nil, err
	}
	status.SyncReplicationConfig = syncConfig

	return status, nil
}

// getStandbyStatusInternal gets standby replication status without guardrail checks.
// Called by Status() which has already verified the PostgreSQL role.
func (pm *MultiPoolerManager) getStandbyStatusInternal(ctx context.Context) (*multipoolermanagerdatapb.StandbyReplicationStatus, error) {
	return pm.queryReplicationStatus(ctx)
}

// PrimaryStatus gets the status of the leader server
func (pm *MultiPoolerManager) PrimaryStatus(ctx context.Context) (*multipoolermanagerdatapb.PrimaryStatus, error) {
	if err := pm.checkReady(); err != nil {
		return nil, err
	}

	// Check PRIMARY guardrails (pooler type and non-recovery mode)
	if err := pm.checkPrimaryGuardrails(ctx); err != nil {
		return nil, err
	}

	status, err := pm.getPrimaryStatusInternal(ctx)
	if err != nil {
		return nil, err
	}

	return status, nil
}

// PrimaryPosition gets the current LSN position of the leader
func (pm *MultiPoolerManager) PrimaryPosition(ctx context.Context) (string, error) {
	if err := pm.checkReady(); err != nil {
		return "", err
	}

	// Check PRIMARY guardrails (pooler type and non-recovery mode)
	if err := pm.checkPrimaryGuardrails(ctx); err != nil {
		return "", err
	}

	// Get current primary LSN position
	return pm.getPrimaryLSN(ctx)
}

// StopReplicationAndGetStatus stops PostgreSQL replication (replay and/or receiver based on mode) and returns the status
func (pm *MultiPoolerManager) StopReplicationAndGetStatus(ctx context.Context, mode multipoolermanagerdatapb.ReplicationPauseMode, wait bool) (*multipoolermanagerdatapb.StandbyReplicationStatus, error) {
	if err := pm.checkReady(); err != nil {
		return nil, err
	}

	// Acquire the action lock to ensure only one mutation runs at a time
	ctx, err := pm.actionLock.Acquire(ctx, "StopReplicationAndGetStatus")
	if err != nil {
		return nil, err
	}
	defer pm.actionLock.Release(ctx)

	// Check REPLICA guardrails (pooler type and recovery mode)
	if err = pm.checkReplicaGuardrails(ctx); err != nil {
		return nil, err
	}

	status, err := pm.pauseReplication(ctx, mode, wait)
	if err != nil {
		return nil, err
	}

	pm.logger.InfoContext(ctx, "StopReplicationAndGetStatus completed",
		"last_replay_lsn", status.LastReplayLsn,
		"last_receive_lsn", status.LastReceiveLsn,
		"is_paused", status.IsWalReplayPaused,
		"pause_state", status.WalReplayPauseState,
		"primary_conn_info", status.PrimaryConnInfo)

	return status, nil
}

// changeTypeLocked updates the pooler type without acquiring the action lock.
// The caller MUST already hold the action lock.
func (pm *MultiPoolerManager) changeTypeLocked(ctx context.Context, poolerType clustermetadatapb.PoolerType) error {
	if err := AssertActionLockHeld(ctx); err != nil {
		return err
	}

	pm.logger.InfoContext(ctx, "changeTypeLocked called", "pooler_type", poolerType.String(), "service_id", pm.serviceID.String())

	// Use the serving state manager to transition components and update the multipooler record.
	// The serving status stays SERVING during type changes (the node remains available).
	if err := pm.servingState.SetState(ctx, poolerType, clustermetadatapb.PoolerServingStatus_SERVING); err != nil {
		return mterrors.Wrap(err, "failed to set serving state")
	}

	// Notify the topology publisher of the new state. The write to etcd happens
	// asynchronously so that a temporarily unreachable etcd does not block type changes.
	if err := pm.topoPublisher.Notify(ctx, pm.multipooler); err != nil {
		pm.logger.ErrorContext(ctx, "topoPublisher.Notify called without action lock", "error", err)
	}

	pm.logger.InfoContext(ctx, "Pooler type updated successfully", "new_type", poolerType.String(), "service_id", pm.serviceID.String())
	return nil
}

// ChangeType changes the pooler type (PRIMARY/REPLICA)
func (pm *MultiPoolerManager) ChangeType(ctx context.Context, poolerType string) error {
	if err := pm.checkReady(); err != nil {
		return err
	}

	// Acquire the action lock to ensure only one mutation runs at a time
	ctx, err := pm.actionLock.Acquire(ctx, "ChangeType")
	if err != nil {
		return err
	}
	defer pm.actionLock.Release(ctx)

	// Validate pooler type
	var newType clustermetadatapb.PoolerType
	// TODO: For now allow to change type to PRIMARY, this is to make it easier
	// to perform tests while we are still developing HA. Once, we have multiorch
	// fully implemented, we shouldn't allow to change the type to Primary.
	// This would happen organically as part of Promote workflow.
	switch poolerType {
	case "PRIMARY":
		newType = clustermetadatapb.PoolerType_PRIMARY
	case "REPLICA":
		newType = clustermetadatapb.PoolerType_REPLICA
	default:
		return mterrors.New(mtrpcpb.Code_INVALID_ARGUMENT,
			fmt.Sprintf("invalid pooler type: %s, must be PRIMARY or REPLICA", poolerType))
	}

	// Call the locked version
	return pm.changeTypeLocked(ctx, newType)
}

// EmergencyDemote demotes the current primary server
// This can be called for any of the following use cases:
// - By orchestrator when fixing a broken shard.
// - When performing a Planned demotion.
// - When receiving a SIGTERM and the pooler needs to shutdown.
func (pm *MultiPoolerManager) EmergencyDemote(ctx context.Context, consensusTerm int64, drainTimeout time.Duration, force bool) (_ *multipoolermanagerdatapb.EmergencyDemoteResponse, retErr error) {
	if err := pm.checkReady(); err != nil {
		return nil, err
	}

	// Acquire the action lock to ensure only one mutation runs at a time
	ctx, err := pm.actionLock.Acquire(ctx, "Demote")
	if err != nil {
		return nil, err
	}
	defer pm.actionLock.Release(ctx)

	// Validate the term but DON'T update yet. We only update the term AFTER
	// successful demotion to avoid a race where a failed demote (e.g., postgres
	// not ready) updates the term, causing subsequent detection to see equal
	// terms and skip demotion.
	if err := pm.validateTerm(ctx, consensusTerm, force); err != nil {
		return nil, err
	}

	nodeName := pm.serviceID.GetName()
	eventlog.Emit(ctx, pm.logger, eventlog.Started, eventlog.PrimaryDemotion{NodeName: nodeName, Reason: "emergency"})
	defer func() {
		if retErr == nil {
			eventlog.Emit(ctx, pm.logger, eventlog.Success, eventlog.PrimaryDemotion{NodeName: nodeName, Reason: "emergency"})
		} else {
			eventlog.Emit(ctx, pm.logger, eventlog.Failed, eventlog.PrimaryDemotion{NodeName: nodeName, Reason: "emergency"}, "error", retErr)
		}
	}()

	// Perform the actual demotion
	resp, err := pm.emergencyDemoteLocked(ctx, consensusTerm, drainTimeout)
	if err != nil {
		return nil, err
	}

	// Only update term AFTER successful demotion
	// This ensures the stale primary keeps its lower term until it's actually demoted,
	// allowing subsequent detection to continue flagging it as stale.
	if err := pm.updateTermIfNewer(ctx, consensusTerm); err != nil {
		// Log but don't fail - demotion succeeded, term update is secondary
		pm.logger.WarnContext(ctx, "Failed to update term after demotion",
			"error", err,
			"consensus_term", consensusTerm)
	}

	return resp, nil
}

// emergencyDemoteLocked performs the core demotion logic.
// REQUIRES: action lock must already be held by the caller.
// This is used for emergency demote operations.
// We won't try to perform a graceful switchover in this case.
// We will drain this pooler and stop postgres.
// This should only be called during ungraceful shutdown.
// MultiOrch will try to contact all nodes in the cohort.
// In the case that the dead primary received the RPC, it should just
// shut down itself.
func (pm *MultiPoolerManager) emergencyDemoteLocked(ctx context.Context, consensusTerm int64, drainTimeout time.Duration) (*multipoolermanagerdatapb.EmergencyDemoteResponse, error) {
	// Verify action lock is held
	if err := AssertActionLockHeld(ctx); err != nil {
		return nil, err
	}

	// === Validation & State Check ===

	// Guard rail: Demote can only be called on a PRIMARY
	if err := pm.checkPrimaryGuardrails(ctx); err != nil {
		return nil, err
	}

	// Check current demotion state
	state, err := pm.checkDemotionState(ctx)
	if err != nil {
		return nil, err
	}

	// If everything is already complete, return early (fully idempotent)
	if state.isNotServing && state.isReplicaInTopology && state.isReadOnly {
		return &multipoolermanagerdatapb.EmergencyDemoteResponse{
			WasAlreadyDemoted:     true,
			ConsensusTerm:         consensusTerm,
			LsnPosition:           state.finalLSN,
			ConnectionsTerminated: 0,
		}, nil
	}

	// Transition to NOT_SERVING — rejects all queries and stops heartbeat.
	// This ensures no new writes arrive while we drain existing connections.
	if err := pm.setNotServing(ctx, state); err != nil {
		return nil, err
	}

	// Drain write connections

	if err := pm.drainWriteActivity(ctx, drainTimeout); err != nil {
		return nil, err
	}

	// Terminate Remaining Write Connections

	connectionsTerminated, err := pm.terminateWriteConnections(ctx)
	if err != nil {
		// Log but don't fail - connections will eventually timeout
		pm.logger.WarnContext(ctx, "Failed to terminate write connections", "error", err)
	}

	// Capture State & Make PostgreSQL Read-Only
	finalLSN, err := pm.getPrimaryLSN(ctx)
	if err != nil {
		pm.logger.ErrorContext(ctx, "Failed to capture final LSN", "error", err)
		return nil, err
	}

	// Signal voluntary resignation so the coordinator can trigger an immediate
	// election without waiting for a heartbeat timeout.
	if term, err := pm.consensusState.GetRevocation(ctx); err == nil && term.GetRevokedBelowTerm() != 0 {
		if err := pm.setResignedPrimaryAtTerm(ctx, term.GetRevokedBelowTerm()); err != nil {
			return nil, mterrors.Wrap(err, "failed to set resigned primary term")
		}
	}

	// Restart PostgreSQL as standby. Unlike the old stop-only path, this keeps
	// the node in the cluster as a replication target, avoiding timeline divergence
	// in most cases. The coordinator still uses pg_rewind for nodes that diverged.
	if err := pm.restartPostgresAsStandby(ctx, state); err != nil {
		return nil, err
	}

	pm.healthStreamer.UpdatePrimaryObservation(nil)

	// Suppress the postgres monitor until a rewind completes; the monitor would
	// otherwise restart postgres on this demoted node.
	pm.rewindPending.Store(true)

	pm.logger.InfoContext(ctx, "Demote completed successfully",
		"final_lsn", finalLSN,
		"consensus_term", consensusTerm,
		"connections_terminated", connectionsTerminated)

	return &multipoolermanagerdatapb.EmergencyDemoteResponse{
		WasAlreadyDemoted:     false,
		ConsensusTerm:         consensusTerm,
		LsnPosition:           finalLSN,
		ConnectionsTerminated: connectionsTerminated,
	}, nil
}

// UndoDemote undoes a demotion
func (pm *MultiPoolerManager) UndoDemote(ctx context.Context) error {
	if err := pm.checkReady(); err != nil {
		return err
	}

	// Acquire the action lock to ensure only one mutation runs at a time
	ctx, err := pm.actionLock.Acquire(ctx, "UndoDemote")
	if err != nil {
		return err
	}
	defer pm.actionLock.Release(ctx)

	pm.logger.InfoContext(ctx, "UndoDemote called")
	return mterrors.New(mtrpcpb.Code_UNIMPLEMENTED, "method UndoDemote not implemented")
}

// promoteResult holds the outcome of a successful Promote operation.
type promoteResult struct {
	finalLSN          string
	wasAlreadyPrimary bool
}

// promote promotes a standby to primary as the proposal leader during Propose().
// The action lock must already be held by the caller.
func (pm *MultiPoolerManager) promote(ctx context.Context, proposal *consensusdatapb.CoordinatorProposal, standbyNamesStr string, force bool) (*promoteResult, error) {
	consensusTerm := proposal.GetTermRevocation().GetRevokedBelowTerm()
	expectedLSN := proposal.GetRecruitmentPosition().GetLsn()
	coordinatorID := proposal.GetTermRevocation().GetAcceptedCoordinatorId()
	cohortMemberIDs := proposal.GetProposedRule().GetCohortMembers()
	reason := "propose"

	// Detect a hung cohort change: the proposed rule_number matches what is already
	// committed in local WAL. In this case the rule_history entry already exists;
	// we must not write a new one but instead do a fencing write to force the
	// existing WAL to replicate to the required standbys.
	proposedRN := proposal.GetProposedRule().GetRuleNumber()
	currentPos := pm.rules.cachedPosition()
	isHungRule := currentPos != nil &&
		consensus.CompareRuleNumbers(proposedRN, currentPos.GetRule().GetRuleNumber()) == 0

	// Check current promotion state to determine what needs to be done
	state, err := pm.checkPromotionState(ctx)
	if err != nil {
		return nil, err
	}

	// Guard rail: Check topology type and validate state consistency
	if state.isPrimaryInTopology {
		pm.logger.InfoContext(ctx, "Promote called but topology already shows PRIMARY - validating state consistency")
		if state.isPrimaryInPostgres {
			pm.logger.InfoContext(ctx, "Promotion already complete (idempotent)", "lsn", state.currentLSN)
			return &promoteResult{finalLSN: state.currentLSN, wasAlreadyPrimary: true}, nil
		}
		if !force {
			return nil, mterrors.New(mtrpcpb.Code_FAILED_PRECONDITION,
				fmt.Sprintf("inconsistent state: topology is PRIMARY but PostgreSQL is not primary. Use force=true. (pg_primary=%v)",
					state.isPrimaryInPostgres))
		}
	}

	// Validate expected LSN before promotion if not yet promoted
	if !state.isPrimaryInPostgres {
		if err := pm.validateExpectedLSN(ctx, expectedLSN); err != nil {
			return nil, err
		}
	}

	if err := pm.promoteStandbyToPrimary(ctx, state); err != nil {
		return nil, err
	}

	if err := pm.configureReplicationAfterPromotion(ctx, state, standbyNamesStr); err != nil {
		return nil, err
	}

	finalLSN, err := pm.getPrimaryLSN(ctx)
	if err != nil {
		return nil, err
	}

	// Clear any outstanding resignation signal.
	if err := pm.clearResignedPrimaryAtTerm(ctx); err != nil {
		return nil, mterrors.Wrap(err, "failed to clear resigned primary term")
	}

	pm.healthStreamer.UpdatePrimaryObservation(&poolerserver.PrimaryObservation{
		PrimaryID:   pm.serviceID,
		PrimaryTerm: consensusTerm,
	})

	if isHungRule {
		// The rule_history entry already exists in local WAL. Perform a fencing
		// write so the standbys acknowledge all WAL up to and including that entry.
		if err := pm.rules.fenceRule(ctx); err != nil {
			return nil, mterrors.Wrap(err, "promotion failed: fencing write could not reach standbys")
		}
		pm.logger.InfoContext(ctx, "hung rule re-propose fencing write complete",
			"coordinator_term", proposedRN.GetCoordinatorTerm(),
			"leader_subterm", proposedRN.GetLeaderSubterm())
	} else {
		promoteCoordID := coordinatorID
		if promoteCoordID == nil {
			promoteCoordID = pm.serviceID
		}
		promoteUpdate := newRuleUpdate(
			consensusTerm,
			promoteCoordID,
			"promotion",
			reason,
			time.Now()).
			withLeader(pm.serviceID).
			withCohort(cohortMemberIDs).
			withWALPosition(finalLSN)
		if force {
			promoteUpdate.withForce()
		}
		if _, err = pm.rules.updateRule(ctx, promoteUpdate); err != nil {
			return nil, mterrors.Wrap(err, "promotion failed: could not write rule history (sync replication may not be functioning)")
		}
	}

	if err := pm.updateTopologyAfterPromotion(ctx, state); err != nil {
		pm.logger.WarnContext(ctx, "Failed to update topology after promotion", "error", err)
	}

	pm.logger.InfoContext(ctx, "promote completed successfully",
		"final_lsn", finalLSN, "consensus_term", consensusTerm, "is_hung_rule", isHungRule)

	return &promoteResult{
		finalLSN:          finalLSN,
		wasAlreadyPrimary: state.isPrimaryInPostgres && state.isPrimaryInTopology,
	}, nil
}

// SetPostgresRestartsEnabled enables or disables automatic PostgreSQL restarts by the monitor.
// When disabled, the monitor continues to run and detect problems but will not auto-restart
// a stopped PostgreSQL instance. Used by tests and demos during controlled failovers.
func (pm *MultiPoolerManager) SetPostgresRestartsEnabled(ctx context.Context, req *multipoolermanagerdatapb.SetPostgresRestartsEnabledRequest) (*multipoolermanagerdatapb.SetPostgresRestartsEnabledResponse, error) {
	pm.postgresRestartsDisabled.Store(!req.Enabled)
	pm.logger.InfoContext(ctx, "SetPostgresRestartsEnabled RPC called", "enabled", req.Enabled)
	return &multipoolermanagerdatapb.SetPostgresRestartsEnabledResponse{}, nil
}

// ====================================================================================
// Stale primary recovery — triggered asynchronously by the Inform handler
// ====================================================================================

// performStalePrimaryRecovery runs when an Inform RPC reveals that this node is
// still in the postgres-primary role but the committed rule names a different primary.
// It stops postgres, runs pg_rewind against the new primary, and restarts as standby.
// Called as a goroutine; logs errors but does not return them.
func (pm *MultiPoolerManager) performStalePrimaryRecovery(ctx context.Context, rule *consensusdatapb.ShardRule) {
	ctx, err := pm.actionLock.Acquire(ctx, "performStalePrimaryRecovery")
	if err != nil {
		pm.logger.ErrorContext(ctx, "Stale primary recovery: failed to acquire action lock", "error", err)
		return
	}
	defer pm.actionLock.Release(ctx)

	// Re-check after acquiring the lock; another goroutine may have already demoted.
	if pm.getPoolerType() != clustermetadatapb.PoolerType_PRIMARY {
		pm.logger.InfoContext(ctx, "Stale primary recovery: already demoted, skipping")
		return
	}

	pm.logger.InfoContext(ctx, "Stale primary recovery: starting",
		"new_primary", rule.GetPrimaryId().GetName())

	if err := pm.stopPostgresIfRunning(ctx); err != nil {
		pm.logger.ErrorContext(ctx, "Stale primary recovery: failed to stop postgres", "error", err)
		return
	}

	source, err := pm.topoClient.GetMultiPooler(ctx, rule.GetPrimaryId())
	if err != nil {
		pm.logger.ErrorContext(ctx, "Stale primary recovery: failed to look up new primary", "error", err)
		return
	}

	port, ok := source.PortMap["postgres"]
	if !ok {
		pm.logger.ErrorContext(ctx, "Stale primary recovery: postgres port not found in new primary's port map")
		return
	}

	if _, err := pm.runPgRewind(ctx, source.Hostname, port); err != nil {
		pm.logger.ErrorContext(ctx, "Stale primary recovery: pg_rewind failed", "error", err)
		return
	}

	if err := pm.fixPgBackRestPaths(ctx); err != nil {
		pm.logger.WarnContext(ctx, "Stale primary recovery: failed to fix pgbackrest paths", "error", err)
	}

	if err := pm.restartAsStandbyAfterRewind(ctx); err != nil {
		pm.logger.ErrorContext(ctx, "Stale primary recovery: failed to restart as standby", "error", err)
		return
	}

	if err := pm.resetSynchronousReplication(ctx); err != nil {
		pm.logger.WarnContext(ctx, "Stale primary recovery: failed to reset synchronous replication", "error", err)
	}

	pm.mu.Lock()
	pm.primaryPoolerID = source.Id
	pm.primaryHost = source.Hostname
	pm.primaryPort = port
	pm.mu.Unlock()

	if err := pm.setPrimaryConnInfoLocked(ctx, source.Hostname, port, false, false); err != nil {
		pm.logger.ErrorContext(ctx, "Stale primary recovery: failed to configure replication", "error", err)
		return
	}

	if err := pm.changeTypeLocked(ctx, clustermetadatapb.PoolerType_REPLICA); err != nil {
		pm.logger.ErrorContext(ctx, "Stale primary recovery: failed to update topology", "error", err)
		return
	}

	pm.logger.InfoContext(ctx, "Stale primary recovery: completed successfully",
		"new_primary", source.Id.GetName())
}

// ====================================================================================
// Postgres stop/rewind/restart helpers (used by stale primary recovery and Inform)
// ====================================================================================

// stopPostgresIfRunning stops postgres if it's currently running.
func (pm *MultiPoolerManager) stopPostgresIfRunning(ctx context.Context) error {
	if pm.pgctldClient == nil {
		return mterrors.New(mtrpcpb.Code_FAILED_PRECONDITION, "pgctld client not initialized")
	}

	pm.logger.InfoContext(ctx, "Stopping postgres if running")

	// Close ONLY connection pools to release database connections.
	// This allows postgres to stop cleanly without waiting for connections,
	// but keeps the manager operational for subsequent operations.
	pm.mu.Lock()
	pm.closeConnectionsLocked()
	pm.mu.Unlock()

	// Stop postgres (no-op if already stopped)
	stopReq := &pgctldpb.StopRequest{Mode: "fast"}
	if _, err := pm.pgctldClient.Stop(ctx, stopReq); err != nil {
		// Treat "already stopped" errors as success to make this truly idempotent.
		// This handles race conditions where postgres was stopped between our check and stop call.
		errMsg := err.Error()
		if strings.Contains(errMsg, "not running") ||
			strings.Contains(errMsg, "no child processes") ||
			strings.Contains(errMsg, "no such process") {
			pm.logger.InfoContext(ctx, "Postgres already stopped, continuing", "error", errMsg)
			return nil
		}
		return mterrors.Wrap(err, "failed to stop postgres")
	}

	return nil
}

// runPgRewind runs pg_rewind to sync with source.
// Returns true if rewind was performed, false if not needed.
func (pm *MultiPoolerManager) runPgRewind(ctx context.Context, sourceHost string, sourcePort int32) (bool, error) {
	if pm.pgctldClient == nil {
		return false, mterrors.New(mtrpcpb.Code_FAILED_PRECONDITION, "pgctld client not initialized")
	}

	// Get application name for replication connection
	pid := pm.servicePoolerID

	pm.logger.InfoContext(ctx, "Running pg_rewind dry-run (may do crash recovery)",
		"source_host", sourceHost, "source_port", sourcePort)

	// Dry-run to check if rewind is needed
	dryRunReq := &pgctldpb.PgRewindRequest{
		SourceHost:      sourceHost,
		SourcePort:      sourcePort,
		DryRun:          true,
		ApplicationName: pid.appName,
	}
	dryRunResp, err := pm.pgctldClient.PgRewind(ctx, dryRunReq)
	if err != nil {
		if dryRunResp != nil {
			pm.logger.ErrorContext(ctx, "pg_rewind dry-run failed", "error", err, "output", dryRunResp.Output)
		}
		return false, mterrors.Wrap(err, "pg_rewind dry-run failed")
	}

	// Check if servers diverged
	if dryRunResp.Output != "" && strings.Contains(dryRunResp.Output, "servers diverged at") {
		pm.logger.InfoContext(ctx, "Servers diverged, running pg_rewind with -R flag")

		rewindReq := &pgctldpb.PgRewindRequest{
			SourceHost:      sourceHost,
			SourcePort:      sourcePort,
			DryRun:          false,
			ApplicationName: pid.appName,
			ExtraArgs:       []string{"-R"},
		}
		rewindResp, err := pm.pgctldClient.PgRewind(ctx, rewindReq)
		if err != nil {
			if rewindResp != nil {
				pm.logger.ErrorContext(ctx, "pg_rewind failed", "error", err, "output", rewindResp.Output)
			}
			return false, mterrors.Wrap(err, "pg_rewind failed")
		}

		pm.logger.InfoContext(ctx, "pg_rewind completed")
		pm.rewindPending.Store(false)
		return true, nil
	}

	// No divergence: the node is already in sync with the source. The rewind is
	// effectively complete; clear the flag so the monitor resumes.
	pm.rewindPending.Store(false)
	pm.logger.InfoContext(ctx, "No divergence, skipping rewind")
	return false, nil
}

// fixPgBackRestPaths fixes the pgbackrest paths in postgresql.auto.conf
// After pg_rewind, the restore_command and archive_command may have paths from another pooler
// This function updates them to point to the current pooler's directories
func (pm *MultiPoolerManager) fixPgBackRestPaths(ctx context.Context) error {
	pm.mu.Lock()
	poolerDir := pm.multipooler.PoolerDir
	pm.mu.Unlock()

	if poolerDir == "" {
		return mterrors.New(mtrpcpb.Code_FAILED_PRECONDITION, "pooler directory not set")
	}

	autoConfPath := filepath.Join(postgresDataDir(), "postgresql.auto.conf")

	pm.logger.InfoContext(ctx, "Fixing pgbackrest paths in postgresql.auto.conf", "file", autoConfPath)

	// Read the file
	content, err := os.ReadFile(autoConfPath)
	if err != nil {
		return mterrors.Wrap(err, "failed to read postgresql.auto.conf")
	}

	// Replace all occurrences of old pooler paths with current pooler paths
	// We need to fix: --config, --lock-path, --log-path, --pg1-path
	// These paths follow the pattern: /some/path/pooler-X/data/...
	// We want to replace them with: /some/path/pooler-current/data/...

	// Extract current pooler dir path pattern
	// poolerDir is like: /tmp/test_12345/pooler-1/data
	// We want to match patterns like: /tmp/test_12345/pooler-X/data
	baseDir := filepath.Dir(filepath.Dir(poolerDir)) // Go up two levels to get base directory

	// Use regex to replace pooler-X paths with current pooler paths
	// Pattern matches: /path/to/pooler-<anything>/data
	re := regexp.MustCompile(regexp.QuoteMeta(baseDir) + `/pooler-[^/]+/data`)
	newContent := re.ReplaceAllString(string(content), poolerDir)

	// Write the file back
	if err := os.WriteFile(autoConfPath, []byte(newContent), 0o600); err != nil {
		return mterrors.Wrap(err, "failed to write postgresql.auto.conf")
	}

	pm.logger.InfoContext(ctx, "Successfully fixed pgbackrest paths in postgresql.auto.conf")
	return nil
}

// restartAsStandbyAfterRewind restarts postgres as standby after rewind.
func (pm *MultiPoolerManager) restartAsStandbyAfterRewind(ctx context.Context) error {
	// Use existing restartPostgresAsStandby with a state that indicates postgres is not running
	state := &demotionState{
		isReadOnly: false, // Postgres was stopped, not in standby mode yet
	}
	return pm.restartPostgresAsStandby(ctx, state)
}
