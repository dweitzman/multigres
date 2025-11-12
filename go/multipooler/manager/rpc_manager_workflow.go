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
	"database/sql"
	"time"

	"github.com/multigres/multigres/go/multipooler/manager/workflow"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
	pgctldpb "github.com/multigres/multigres/go/pb/pgctldservice"
)

// managerAdapter adapts MultiPoolerManager to workflow.ManagerDependencies interface.
// This allows executors to access manager functionality without tight coupling.
type managerAdapter struct {
	pm *MultiPoolerManager
}

func (ma *managerAdapter) GetDB() *sql.DB {
	return ma.pm.db
}

func (ma *managerAdapter) GetServiceID() *clustermetadatapb.ID {
	return ma.pm.serviceID
}

func (ma *managerAdapter) GetCurrentTermNumber() int64 {
	return ma.pm.getCurrentTermNumber()
}

func (ma *managerAdapter) GetMultiPooler() *clustermetadatapb.MultiPooler {
	ma.pm.mu.Lock()
	defer ma.pm.mu.Unlock()
	if ma.pm.multipooler == nil {
		return nil
	}
	return ma.pm.multipooler.MultiPooler
}

func (ma *managerAdapter) SetMultiPooler(mp *clustermetadatapb.MultiPooler) {
	ma.pm.mu.Lock()
	defer ma.pm.mu.Unlock()
	ma.pm.multipooler.MultiPooler = mp
}

func (ma *managerAdapter) SetQueryServingState(status clustermetadatapb.PoolerServingStatus) {
	ma.pm.mu.Lock()
	defer ma.pm.mu.Unlock()
	ma.pm.queryServingState = status
}

func (ma *managerAdapter) ValidateAndUpdateTerm(ctx context.Context, term int64, force bool) error {
	return ma.pm.validateAndUpdateTerm(ctx, term, force)
}

func (ma *managerAdapter) ConnectDB() error {
	return ma.pm.connectDB()
}

func (ma *managerAdapter) CheckPrimaryGuardrails(ctx context.Context) error {
	return ma.pm.checkPrimaryGuardrails(ctx)
}

// topoClientAdapter adapts the manager's topoClient to workflow.TopologyClient interface.
type topoClientAdapter struct {
	pm *MultiPoolerManager
}

func (tca *topoClientAdapter) UpdateMultiPoolerFields(
	ctx context.Context,
	serviceID *clustermetadatapb.ID,
	updateFunc func(*clustermetadatapb.MultiPooler) error,
) (*clustermetadatapb.MultiPooler, error) {
	return tca.pm.topoClient.UpdateMultiPoolerFields(ctx, serviceID, updateFunc)
}

// replTrackerAdapter adapts the manager's replTracker to workflow.ReplTracker interface.
type replTrackerAdapter struct {
	pm *MultiPoolerManager
}

func (rta *replTrackerAdapter) MakePrimary() {
	if rta.pm.replTracker != nil {
		rta.pm.replTracker.MakePrimary()
	}
}

func (rta *replTrackerAdapter) MakeNonPrimary() {
	if rta.pm.replTracker != nil {
		rta.pm.replTracker.MakeNonPrimary()
	}
}

// pgctldClientAdapter adapts the manager's pgctldClient to workflow.PgctldClient interface.
type pgctldClientAdapter struct {
	pm *MultiPoolerManager
}

func (pca *pgctldClientAdapter) Restart(ctx context.Context, req *pgctldpb.RestartRequest) (*pgctldpb.RestartResponse, error) {
	return pca.pm.pgctldClient.Restart(ctx, req)
}

// DemoteWithWorkflow performs demotion using the phase-based workflow system.
//
// This is an alternative implementation of Demote() that uses declarative
// phase-based orchestration instead of imperative sequencing.
//
// The workflow is identical to Demote() but with better:
// - Component decoupling (executors register for phases)
// - Reordering protection (phases have fixed order)
// - Extensibility (add new executors without modifying core logic)
// - Testability (test executors independently)
func (pm *MultiPoolerManager) DemoteWithWorkflow(
	ctx context.Context,
	consensusTerm int64,
	drainTimeout time.Duration,
	force bool,
) (*multipoolermanagerdatapb.DemoteResponse, error) {
	if err := pm.checkReady(); err != nil {
		return nil, err
	}

	// Acquire the action lock to ensure only one mutation runs at a time
	if err := pm.lock(ctx); err != nil {
		return nil, err
	}
	defer pm.unlock()

	pm.logger.InfoContext(ctx, "DemoteWithWorkflow called",
		"consensus_term", consensusTerm,
		"drain_timeout", drainTimeout,
		"force", force)

	// Create workflow
	wf := workflow.NewDemoteWorkflow(pm.logger)

	// Create adapters for dependencies
	managerDeps := &managerAdapter{pm: pm}
	topoDeps := &topoClientAdapter{pm: pm}
	replDeps := &replTrackerAdapter{pm: pm}
	pgctldDeps := &pgctldClientAdapter{pm: pm}

	// Register executors
	wf.Register(workflow.NewValidationExecutor(managerDeps, pm.logger))
	wf.Register(workflow.NewTopologyExecutor(managerDeps, topoDeps, pm.logger))
	wf.Register(workflow.NewReplTrackerExecutor(replDeps, pm.logger))
	wf.Register(workflow.NewDrainExecutor(managerDeps, pm.logger))
	wf.Register(workflow.NewRestartExecutor(managerDeps, pgctldDeps, pm.logger))
	wf.Register(workflow.NewCleanupExecutor(managerDeps, pm.logger))

	// Prepare input
	input := &workflow.DemoteInput{
		ConsensusTerm: consensusTerm,
		DrainTimeout:  drainTimeout,
		Force:         force,
	}

	// Prepare initial state
	initialState := &workflow.DemoteState{}

	// Execute workflow
	result, err := wf.Execute(ctx, input, initialState, workflow.BuildDemoteResult, &workflow.ExecuteOptions[workflow.DemotePhase, workflow.DemoteInput, workflow.DemoteState, workflow.DemoteResult]{
		EarlyExitChecker: workflow.CheckDemoteEarlyExit,
	})
	if err != nil {
		return nil, err
	}

	// Log non-fatal errors if any
	if len(result.NonFatalErrors) > 0 {
		pm.logger.WarnContext(ctx, "Demotion completed with non-fatal errors",
			"error_count", len(result.NonFatalErrors),
			"errors", result.NonFatalErrors)
	}

	return result.ToProto(), nil
}
