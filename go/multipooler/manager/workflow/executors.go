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

package workflow

import (
	"context"
	"database/sql"
	"strings"
	"sync"
	"time"

	"github.com/multigres/multigres/go/mterrors"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	pgctldpb "github.com/multigres/multigres/go/pb/pgctldservice"
)

// ManagerDependencies provides access to MultiPoolerManager functionality.
// This interface allows executors to interact with the manager without tight coupling.
type ManagerDependencies interface {
	GetDB() *sql.DB
	GetServiceID() *clustermetadatapb.ID
	GetCurrentTermNumber() int64
	GetMultiPooler() *clustermetadatapb.MultiPooler
	SetMultiPooler(mp *clustermetadatapb.MultiPooler)
	SetQueryServingState(status clustermetadatapb.PoolerServingStatus)
	ValidateAndUpdateTerm(ctx context.Context, term int64, force bool) error
	ConnectDB() error
	CheckPrimaryGuardrails(ctx context.Context) error
}

// TopologyClient provides access to the topology store.
type TopologyClient interface {
	UpdateMultiPoolerFields(ctx context.Context, serviceID *clustermetadatapb.ID,
		updateFunc func(*clustermetadatapb.MultiPooler) error) (*clustermetadatapb.MultiPooler, error)
}

// ReplTracker manages heartbeat writer/reader for replication tracking.
type ReplTracker interface {
	MakePrimary()
	MakeNonPrimary()
}

// PgctldClient controls PostgreSQL lifecycle.
type PgctldClient interface {
	Restart(ctx context.Context, req *pgctldpb.RestartRequest) (*pgctldpb.RestartResponse, error)
}

// ====================
// ValidationExecutor
// ====================

// ValidationExecutor validates preconditions for demotion.
type ValidationExecutor struct {
	manager ManagerDependencies
	logger  Logger
}

// NewValidationExecutor creates a new validation executor.
func NewValidationExecutor(manager ManagerDependencies, logger Logger) *ValidationExecutor {
	return &ValidationExecutor{
		manager: manager,
		logger:  logger,
	}
}

func (e *ValidationExecutor) Name() string {
	return "ValidationExecutor"
}

func (e *ValidationExecutor) Phases() []DemotePhase {
	return []DemotePhase{DemotePhaseValidate}
}

func (e *ValidationExecutor) Execute(ctx context.Context, phaseCtx *PhaseContext[DemotePhase, DemoteInput, DemoteState]) error {
	// Validate and update consensus term
	if err := e.manager.ValidateAndUpdateTerm(ctx, phaseCtx.Input.ConsensusTerm, phaseCtx.Input.Force); err != nil {
		return err
	}

	// Ensure database connection
	if err := e.manager.ConnectDB(); err != nil {
		e.logger.ErrorContext(ctx, "Failed to connect to database", "error", err)
		return mterrors.Wrap(err, "database connection failed")
	}

	// Guard rail: Demote can only be called on a PRIMARY
	if err := e.manager.CheckPrimaryGuardrails(ctx); err != nil {
		return err
	}

	// Check current demotion state
	state, err := e.checkDemotionState(ctx)
	if err != nil {
		return err
	}

	// Update phase state
	phaseCtx.State.IsServingReadOnly = state.isServingReadOnly
	phaseCtx.State.IsReplicaInTopology = state.isReplicaInTopology
	phaseCtx.State.IsReadOnly = state.isReadOnly
	phaseCtx.State.FinalLSN = state.finalLSN

	// Check if already demoted (idempotency)
	if state.isServingReadOnly && state.isReplicaInTopology && state.isReadOnly {
		e.logger.InfoContext(ctx, "Demotion already complete (idempotent)", "lsn", state.finalLSN)
		phaseCtx.State.WasAlreadyDemoted = true
	}

	return nil
}

func (e *ValidationExecutor) CanFail(phase DemotePhase) bool {
	return false // Validation failures are always fatal
}

// checkDemotionState checks the current state to determine what work remains.
func (e *ValidationExecutor) checkDemotionState(ctx context.Context) (*demotionState, error) {
	db := e.manager.GetDB()
	mp := e.manager.GetMultiPooler()

	state := &demotionState{}

	// Check serving status in topology
	state.isServingReadOnly = mp.ServingStatus == clustermetadatapb.PoolerServingStatus_SERVING_RDONLY

	// Check pooler type in topology
	state.isReplicaInTopology = mp.Type == clustermetadatapb.PoolerType_REPLICA

	// Check if PostgreSQL is in recovery mode
	var inRecovery bool
	err := db.QueryRowContext(ctx, "SELECT pg_is_in_recovery()").Scan(&inRecovery)
	if err != nil {
		return nil, mterrors.Wrap(err, "failed to check recovery status")
	}
	state.isReadOnly = inRecovery

	// Get current LSN (if still primary) or replay LSN (if replica)
	if !inRecovery {
		var lsn string
		err = db.QueryRowContext(ctx, "SELECT pg_current_wal_lsn()::text").Scan(&lsn)
		if err != nil {
			return nil, mterrors.Wrap(err, "failed to get current LSN")
		}
		state.finalLSN = lsn
	} else {
		var lsn string
		err = db.QueryRowContext(ctx, "SELECT pg_last_wal_replay_lsn()::text").Scan(&lsn)
		if err == nil {
			state.finalLSN = lsn
		}
		// Ignore error if LSN unavailable
	}

	return state, nil
}

type demotionState struct {
	isServingReadOnly   bool
	isReplicaInTopology bool
	isReadOnly          bool
	finalLSN            string
}

// ====================
// TopologyExecutor
// ====================

// TopologyExecutor manages topology updates during demotion.
type TopologyExecutor struct {
	manager    ManagerDependencies
	topoClient TopologyClient
	logger     Logger
}

// NewTopologyExecutor creates a new topology executor.
func NewTopologyExecutor(manager ManagerDependencies, topoClient TopologyClient, logger Logger) *TopologyExecutor {
	return &TopologyExecutor{
		manager:    manager,
		topoClient: topoClient,
		logger:     logger,
	}
}

func (e *TopologyExecutor) Name() string {
	return "TopologyExecutor"
}

func (e *TopologyExecutor) Phases() []DemotePhase {
	return []DemotePhase{DemotePhaseStopWrites, DemotePhaseCleanup}
}

func (e *TopologyExecutor) Execute(ctx context.Context, phaseCtx *PhaseContext[DemotePhase, DemoteInput, DemoteState]) error {
	switch phaseCtx.Phase {
	case DemotePhaseStopWrites:
		return e.updateToServingReadOnly(ctx, phaseCtx)
	case DemotePhaseCleanup:
		return e.updateToReplica(ctx, phaseCtx)
	}
	return nil
}

func (e *TopologyExecutor) CanFail(phase DemotePhase) bool {
	// StopWrites is critical; Cleanup is best-effort
	return phase == DemotePhaseCleanup
}

func (e *TopologyExecutor) updateToServingReadOnly(ctx context.Context, phaseCtx *PhaseContext[DemotePhase, DemoteInput, DemoteState]) error {
	if phaseCtx.State.IsServingReadOnly {
		e.logger.InfoContext(ctx, "Already in SERVING_RDONLY state, skipping")
		return nil
	}

	e.logger.InfoContext(ctx, "Transitioning to SERVING_RDONLY")

	// Update serving status in topology
	updatedMultipooler, err := e.topoClient.UpdateMultiPoolerFields(ctx, e.manager.GetServiceID(), func(mp *clustermetadatapb.MultiPooler) error {
		mp.ServingStatus = clustermetadatapb.PoolerServingStatus_SERVING_RDONLY
		return nil
	})
	if err != nil {
		e.logger.ErrorContext(ctx, "Failed to update serving status in topology", "error", err)
		return mterrors.Wrap(err, "failed to transition to SERVING_RDONLY")
	}

	e.manager.SetMultiPooler(updatedMultipooler)
	e.manager.SetQueryServingState(clustermetadatapb.PoolerServingStatus_SERVING_RDONLY)

	e.logger.InfoContext(ctx, "Transitioned to SERVING_RDONLY successfully")
	return nil
}

func (e *TopologyExecutor) updateToReplica(ctx context.Context, phaseCtx *PhaseContext[DemotePhase, DemoteInput, DemoteState]) error {
	if phaseCtx.State.IsReplicaInTopology {
		e.logger.InfoContext(ctx, "Topology already updated to REPLICA, skipping")
		return nil
	}

	e.logger.InfoContext(ctx, "Updating pooler type in topology to REPLICA")
	updatedMultipooler, err := e.topoClient.UpdateMultiPoolerFields(ctx, e.manager.GetServiceID(), func(mp *clustermetadatapb.MultiPooler) error {
		mp.Type = clustermetadatapb.PoolerType_REPLICA
		return nil
	})
	if err != nil {
		e.logger.ErrorContext(ctx, "Failed to update pooler type in topology", "error", err)
		return mterrors.Wrap(err, "demotion succeeded but failed to update topology")
	}

	e.manager.SetMultiPooler(updatedMultipooler)

	e.logger.InfoContext(ctx, "Topology updated to REPLICA successfully")
	return nil
}

// ====================
// ReplTrackerExecutor
// ====================

// ReplTrackerExecutor manages the heartbeat writer state.
type ReplTrackerExecutor struct {
	replTracker ReplTracker
	logger      Logger
}

// NewReplTrackerExecutor creates a new replication tracker executor.
func NewReplTrackerExecutor(replTracker ReplTracker, logger Logger) *ReplTrackerExecutor {
	return &ReplTrackerExecutor{
		replTracker: replTracker,
		logger:      logger,
	}
}

func (e *ReplTrackerExecutor) Name() string {
	return "ReplTrackerExecutor"
}

func (e *ReplTrackerExecutor) Phases() []DemotePhase {
	return []DemotePhase{DemotePhaseStopWrites}
}

func (e *ReplTrackerExecutor) Execute(ctx context.Context, phaseCtx *PhaseContext[DemotePhase, DemoteInput, DemoteState]) error {
	e.logger.InfoContext(ctx, "Stopping heartbeat writer")
	e.replTracker.MakeNonPrimary()
	return nil
}

func (e *ReplTrackerExecutor) CanFail(phase DemotePhase) bool {
	return false // Always succeeds
}

// ====================
// DrainExecutor
// ====================

// DrainExecutor handles connection draining and checkpointing.
type DrainExecutor struct {
	manager ManagerDependencies
	logger  Logger
}

// NewDrainExecutor creates a new drain executor.
func NewDrainExecutor(manager ManagerDependencies, logger Logger) *DrainExecutor {
	return &DrainExecutor{
		manager: manager,
		logger:  logger,
	}
}

func (e *DrainExecutor) Name() string {
	return "DrainExecutor"
}

func (e *DrainExecutor) Phases() []DemotePhase {
	return []DemotePhase{DemotePhaseDrain}
}

func (e *DrainExecutor) Execute(ctx context.Context, phaseCtx *PhaseContext[DemotePhase, DemoteInput, DemoteState]) error {
	db := e.manager.GetDB()

	// Run drain and checkpoint in parallel
	var wg sync.WaitGroup
	checkpointErr := make(chan error, 1)

	// Start checkpoint in background
	wg.Add(1)
	go func() {
		defer wg.Done()
		e.logger.InfoContext(ctx, "Starting checkpoint")
		_, err := db.ExecContext(ctx, "CHECKPOINT")
		if err != nil {
			e.logger.WarnContext(ctx, "Checkpoint failed", "error", err)
			checkpointErr <- err
		} else {
			e.logger.InfoContext(ctx, "Checkpoint completed")
			checkpointErr <- nil
		}
	}()

	// Monitor for write activity during drain
	e.logger.InfoContext(ctx, "Monitoring for write activity during drain", "duration", phaseCtx.Input.DrainTimeout)
	drainCtx, cancel := context.WithTimeout(ctx, phaseCtx.Input.DrainTimeout)
	defer cancel()

	monitorTicker := time.NewTicker(100 * time.Millisecond)
	defer monitorTicker.Stop()

	consecutiveNoWrites := 0
	drainComplete := false

	for !drainComplete {
		select {
		case <-drainCtx.Done():
			e.logger.InfoContext(ctx, "Drain timeout completed")
			drainComplete = true

		case err := <-checkpointErr:
			if err != nil {
				e.logger.WarnContext(ctx, "Checkpoint completed with error during drain", "error", err)
			} else {
				e.logger.InfoContext(ctx, "Checkpoint completed during drain")
			}

		case <-monitorTicker.C:
			// Check for write activity
			pids, err := e.getActiveWriteConnections(ctx)
			if err != nil {
				e.logger.WarnContext(ctx, "Failed to check write activity", "error", err)
				continue
			}

			if len(pids) == 0 {
				consecutiveNoWrites++
				if consecutiveNoWrites >= 2 {
					e.logger.InfoContext(ctx, "No write activity detected (2 consecutive checks), drain complete")
					drainComplete = true
				}
			} else {
				e.logger.InfoContext(ctx, "Active writes detected during drain",
					"count", len(pids),
					"consecutive_no_writes_reset", true)
				consecutiveNoWrites = 0
			}
		}
	}

	// Wait for checkpoint to complete
	wg.Wait()

	// Terminate remaining write connections
	connectionsTerminated, err := e.terminateWriteConnections(ctx)
	if err != nil {
		// Log but don't fail - connections will eventually timeout
		e.logger.WarnContext(ctx, "Failed to terminate write connections", "error", err)
	}

	phaseCtx.State.ConnectionsTerminated = connectionsTerminated

	return nil
}

func (e *DrainExecutor) CanFail(phase DemotePhase) bool {
	return false // Drain failures are critical
}

func (e *DrainExecutor) getActiveWriteConnections(ctx context.Context) ([]int32, error) {
	db := e.manager.GetDB()

	query := `
		SELECT COALESCE(array_agg(pid), ARRAY[]::integer[])
		FROM pg_stat_activity
		WHERE pid != pg_backend_pid()
		  AND state = 'active'
		  AND query !~* '^(SELECT|SHOW|SET|CHECKPOINT)'
	`

	var pids []int32
	err := db.QueryRowContext(ctx, query).Scan(&pids)
	if err != nil {
		return nil, mterrors.Wrap(err, "failed to query active write connections")
	}

	return pids, nil
}

func (e *DrainExecutor) terminateWriteConnections(ctx context.Context) (int32, error) {
	db := e.manager.GetDB()

	pids, err := e.getActiveWriteConnections(ctx)
	if err != nil {
		e.logger.ErrorContext(ctx, "Failed to get active write connections", "error", err)
		return 0, mterrors.Wrap(err, "failed to get active write connections")
	}

	if len(pids) == 0 {
		e.logger.InfoContext(ctx, "No active write connections to terminate")
		return 0, nil
	}

	e.logger.WarnContext(ctx, "Terminating connections still performing writes after drain",
		"count", len(pids),
		"pids", pids)

	// Terminate each write connection
	for _, pid := range pids {
		_, err := db.ExecContext(ctx, "SELECT pg_terminate_backend($1)", pid)
		if err != nil {
			e.logger.WarnContext(ctx, "Failed to terminate write connection", "pid", pid, "error", err)
		}
	}

	return int32(len(pids)), nil
}

// ====================
// RestartExecutor
// ====================

// RestartExecutor captures LSN and restarts PostgreSQL as standby.
type RestartExecutor struct {
	manager      ManagerDependencies
	pgctldClient PgctldClient
	logger       Logger
}

// NewRestartExecutor creates a new restart executor.
func NewRestartExecutor(manager ManagerDependencies, pgctldClient PgctldClient, logger Logger) *RestartExecutor {
	return &RestartExecutor{
		manager:      manager,
		pgctldClient: pgctldClient,
		logger:       logger,
	}
}

func (e *RestartExecutor) Name() string {
	return "RestartExecutor"
}

func (e *RestartExecutor) Phases() []DemotePhase {
	return []DemotePhase{DemotePhaseCapture, DemotePhaseRestart}
}

func (e *RestartExecutor) Execute(ctx context.Context, phaseCtx *PhaseContext[DemotePhase, DemoteInput, DemoteState]) error {
	switch phaseCtx.Phase {
	case DemotePhaseCapture:
		return e.captureLSN(ctx, phaseCtx)
	case DemotePhaseRestart:
		return e.restartAsStandby(ctx, phaseCtx)
	}
	return nil
}

func (e *RestartExecutor) CanFail(phase DemotePhase) bool {
	return false // Both capture and restart are critical
}

func (e *RestartExecutor) captureLSN(ctx context.Context, phaseCtx *PhaseContext[DemotePhase, DemoteInput, DemoteState]) error {
	db := e.manager.GetDB()

	// Get current LSN while still primary
	var lsn string
	err := db.QueryRowContext(ctx, "SELECT pg_current_wal_lsn()::text").Scan(&lsn)
	if err != nil {
		e.logger.ErrorContext(ctx, "Failed to capture final LSN", "error", err)
		return mterrors.Wrap(err, "failed to capture final LSN")
	}

	e.logger.InfoContext(ctx, "Captured final LSN", "lsn", lsn)
	phaseCtx.State.FinalLSN = lsn

	return nil
}

func (e *RestartExecutor) restartAsStandby(ctx context.Context, phaseCtx *PhaseContext[DemotePhase, DemoteInput, DemoteState]) error {
	if phaseCtx.State.IsReadOnly {
		e.logger.InfoContext(ctx, "PostgreSQL already running as standby, skipping")
		return nil
	}

	if e.pgctldClient == nil {
		return mterrors.New(mtrpcpb.Code_FAILED_PRECONDITION, "pgctld client not initialized")
	}

	e.logger.InfoContext(ctx, "Restarting PostgreSQL as standby")

	// Call pgctld to restart as standby
	req := &pgctldpb.RestartRequest{
		Mode:      "fast",
		Timeout:   nil, // Use default timeout
		Port:      0,   // Use default port
		ExtraArgs: nil,
		AsStandby: true, // Create standby.signal before restart
	}

	resp, err := e.pgctldClient.Restart(ctx, req)
	if err != nil {
		e.logger.ErrorContext(ctx, "Failed to restart PostgreSQL as standby", "error", err)
		return mterrors.Wrap(err, "failed to restart as standby")
	}

	e.logger.InfoContext(ctx, "PostgreSQL restarted as standby",
		"pid", resp.Pid,
		"message", resp.Message)

	// Reconnect to PostgreSQL
	if err := e.manager.ConnectDB(); err != nil {
		e.logger.ErrorContext(ctx, "Failed to reconnect to database after restart", "error", err)
		return mterrors.Wrap(err, "failed to reconnect to database")
	}

	// Verify server is in recovery mode (standby)
	db := e.manager.GetDB()
	var inRecovery bool
	err = db.QueryRowContext(ctx, "SELECT pg_is_in_recovery()").Scan(&inRecovery)
	if err != nil {
		e.logger.ErrorContext(ctx, "Failed to verify recovery status", "error", err)
		return mterrors.Wrap(err, "failed to verify standby status")
	}

	if !inRecovery {
		e.logger.ErrorContext(ctx, "PostgreSQL not in recovery mode after restart")
		return mterrors.New(mtrpcpb.Code_INTERNAL, "server not in recovery mode after restart as standby")
	}

	e.logger.InfoContext(ctx, "PostgreSQL is now running as a standby")
	return nil
}

// ====================
// CleanupExecutor
// ====================

// CleanupExecutor resets synchronous replication configuration.
type CleanupExecutor struct {
	manager ManagerDependencies
	logger  Logger
}

// NewCleanupExecutor creates a new cleanup executor.
func NewCleanupExecutor(manager ManagerDependencies, logger Logger) *CleanupExecutor {
	return &CleanupExecutor{
		manager: manager,
		logger:  logger,
	}
}

func (e *CleanupExecutor) Name() string {
	return "CleanupExecutor"
}

func (e *CleanupExecutor) Phases() []DemotePhase {
	return []DemotePhase{DemotePhaseCleanup}
}

func (e *CleanupExecutor) Execute(ctx context.Context, phaseCtx *PhaseContext[DemotePhase, DemoteInput, DemoteState]) error {
	db := e.manager.GetDB()

	e.logger.InfoContext(ctx, "Resetting synchronous replication configuration")

	// Get current settings to check if reset is needed
	var syncStandbyNames string
	err := db.QueryRowContext(ctx, "SHOW synchronous_standby_names").Scan(&syncStandbyNames)
	if err != nil {
		return mterrors.Wrap(err, "failed to check synchronous_standby_names")
	}

	// Only reset if non-empty
	if syncStandbyNames != "" && !strings.EqualFold(syncStandbyNames, "''") {
		_, err = db.ExecContext(ctx, "ALTER SYSTEM SET synchronous_standby_names = ''")
		if err != nil {
			e.logger.WarnContext(ctx, "Failed to reset synchronous_standby_names", "error", err)
			return mterrors.Wrap(err, "failed to reset synchronous replication")
		}

		_, err = db.ExecContext(ctx, "SELECT pg_reload_conf()")
		if err != nil {
			e.logger.WarnContext(ctx, "Failed to reload configuration", "error", err)
			return mterrors.Wrap(err, "failed to reload configuration")
		}

		e.logger.InfoContext(ctx, "Reset synchronous replication configuration successfully")
	} else {
		e.logger.InfoContext(ctx, "Synchronous replication already empty, skipping reset")
	}

	return nil
}

func (e *CleanupExecutor) CanFail(phase DemotePhase) bool {
	return true // Cleanup is best-effort
}
