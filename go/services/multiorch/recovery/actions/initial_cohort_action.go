// Copyright 2026 Supabase, Inc.
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

package actions

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/rpcclient"
	commontypes "github.com/multigres/multigres/go/common/types"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	multiorchdatapb "github.com/multigres/multigres/go/pb/multiorchdata"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
	"github.com/multigres/multigres/go/services/multiorch/config"
	"github.com/multigres/multigres/go/services/multiorch/consensus"
	"github.com/multigres/multigres/go/services/multiorch/recovery/types"
	"github.com/multigres/multigres/go/services/multiorch/store"
)

// Compile-time assertion that InitialCohortAction implements types.RecoveryAction.
var _ types.RecoveryAction = (*InitialCohortAction)(nil)

// InitialCohortAction handles Phase 2 of shard bootstrap: establishing the initial
// cohort for a freshly bootstrapped shard. It runs after all poolers have completed
// Phase 1 (initdb, schema creation, pgBackRest backup+restore) and are running as
// hot standbys with a 0-member leadership_history record.
//
// This action:
//  1. Re-verifies via fresh RPCs that no cohort has been established yet and that
//     enough initialized poolers are reachable to satisfy the durability policy.
//  2. Calls AppointInitialLeader (BeginTerm(term=1) + EstablishLeadership) to elect
//     a primary and write the initial cohort into leadership_history.
type InitialCohortAction struct {
	config      *config.Config
	coordinator *consensus.Coordinator
	poolerStore *store.PoolerStore
	rpcClient   rpcclient.MultiPoolerClient
	logger      *slog.Logger
}

// NewInitialCohortAction creates a new initial cohort action.
func NewInitialCohortAction(
	cfg *config.Config,
	coordinator *consensus.Coordinator,
	poolerStore *store.PoolerStore,
	rpcClient rpcclient.MultiPoolerClient,
	logger *slog.Logger,
) *InitialCohortAction {
	return &InitialCohortAction{
		config:      cfg,
		coordinator: coordinator,
		poolerStore: poolerStore,
		rpcClient:   rpcClient,
		logger:      logger,
	}
}

// Execute performs initial cohort establishment for a freshly bootstrapped shard.
func (a *InitialCohortAction) Execute(ctx context.Context, problem types.Problem) error {
	a.logger.InfoContext(ctx, "executing initial cohort action",
		"database", problem.ShardKey.Database,
		"tablegroup", problem.ShardKey.TableGroup,
		"shard", problem.ShardKey.Shard)

	cohort := a.getCohort(problem.ShardKey)
	if len(cohort) == 0 {
		return fmt.Errorf("no poolers found for shard %s", problem.ShardKey)
	}

	// Re-verify: skip if a primary already exists and is healthy.
	for _, pooler := range cohort {
		if pooler.MultiPooler != nil &&
			pooler.MultiPooler.Type == clustermetadatapb.PoolerType_PRIMARY &&
			pooler.IsLastCheckValid &&
			pooler.IsPostgresRunning {
			a.logger.InfoContext(ctx, "primary already exists, skipping initial cohort action",
				"primary", pooler.MultiPooler.Id.Name,
				"shard_key", problem.ShardKey.String())
			return nil
		}
	}

	// Re-verify via fresh RPCs: check each reachable pooler's status.
	// We need to confirm:
	//  1. No pooler has an established cohort (cohort members would mean Phase 2 already ran).
	//  2. At least some poolers are initialized (have completed Phase 1 backup+restore).
	initializedCount := 0
	for _, pooler := range cohort {
		resp, err := a.rpcClient.Status(ctx, pooler.MultiPooler, &multipoolermanagerdatapb.StatusRequest{})
		if err != nil {
			a.logger.WarnContext(ctx, "pooler unreachable during initial cohort re-verify",
				"pooler", pooler.MultiPooler.Id.Name, "error", err)
			continue
		}
		if resp.Status == nil {
			continue
		}

		if len(resp.Status.CohortMembers) > 0 {
			a.logger.InfoContext(ctx, "pooler already has established cohort, skipping initial cohort action",
				"pooler", pooler.MultiPooler.Id.Name,
				"cohort_members", resp.Status.CohortMembers)
			return nil
		}

		if resp.Status.IsInitialized {
			initializedCount++
		}
	}

	if initializedCount == 0 {
		return mterrors.Errorf(mtrpcpb.Code_FAILED_PRECONDITION,
			"no initialized poolers found for shard %s — Phase 1 may not have completed", problem.ShardKey)
	}

	a.logger.InfoContext(ctx, "re-verified shard needs initial cohort, proceeding",
		"shard_key", problem.ShardKey.String(),
		"initialized_count", initializedCount,
		"cohort_size", len(cohort))

	return a.coordinator.AppointInitialLeader(ctx, problem.ShardKey.Shard, cohort, problem.ShardKey.Database)
}

// getCohort fetches all poolers in the shard from the pooler store.
func (a *InitialCohortAction) getCohort(shardKey commontypes.ShardKey) []*multiorchdatapb.PoolerHealthState {
	var cohort []*multiorchdatapb.PoolerHealthState
	a.poolerStore.Range(func(_ string, pooler *multiorchdatapb.PoolerHealthState) bool {
		if pooler == nil || pooler.MultiPooler == nil || pooler.MultiPooler.Id == nil {
			return true
		}
		if pooler.MultiPooler.Database == shardKey.Database &&
			pooler.MultiPooler.TableGroup == shardKey.TableGroup &&
			pooler.MultiPooler.Shard == shardKey.Shard {
			cohort = append(cohort, pooler)
		}
		return true
	})
	return cohort
}

func (a *InitialCohortAction) RequiresHealthyPrimary() bool { return false }

func (a *InitialCohortAction) Metadata() types.RecoveryMetadata {
	return types.RecoveryMetadata{
		Name:        "InitialCohort",
		Description: "Establish initial cohort for a freshly bootstrapped shard",
		Timeout:     60 * time.Second,
		LockTimeout: 15 * time.Second,
		Retryable:   true,
	}
}

func (a *InitialCohortAction) Priority() types.Priority { return types.PriorityShardBootstrap }

func (a *InitialCohortAction) GracePeriod() *types.GracePeriodConfig { return nil }
