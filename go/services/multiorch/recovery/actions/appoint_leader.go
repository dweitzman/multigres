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

package actions

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/rpcclient"
	"github.com/multigres/multigres/go/common/timeouts"
	"github.com/multigres/multigres/go/common/topoclient"
	commontypes "github.com/multigres/multigres/go/common/types"
	"github.com/multigres/multigres/go/services/multiorch/config"
	"github.com/multigres/multigres/go/services/multiorch/consensus"
	"github.com/multigres/multigres/go/services/multiorch/recovery/types"
	"github.com/multigres/multigres/go/services/multiorch/store"

	multiorchdatapb "github.com/multigres/multigres/go/pb/multiorchdata"
)

// Compile-time assertion that AppointLeaderAction implements types.RecoveryAction.
var _ types.RecoveryAction = (*AppointLeaderAction)(nil)

// Compile-time assertion that AppointLeaderAction implements types.PreRecheckRefresher.
var _ types.PreRecheckRefresher = (*AppointLeaderAction)(nil)

// AppointLeaderAction handles leader appointment using the coordinator's consensus protocol.
// This action is used for both repair (mixed initialized/empty nodes) and reelect
// (all nodes initialized) scenarios. The consensus.AppointLeader method handles
// both cases by selecting the most advanced node based on WAL position and running
// the full consensus protocol to establish a new primary.
type AppointLeaderAction struct {
	config      *config.Config
	consensus   *consensus.Coordinator
	rpcClient   rpcclient.MultipoolerClient
	poolerStore *store.PoolerCache
	topoStore   topoclient.Store
	logger      *slog.Logger
}

// NewAppointLeaderAction creates a new leader appointment action
func NewAppointLeaderAction(
	cfg *config.Config,
	consensus *consensus.Coordinator,
	rpcClient rpcclient.MultipoolerClient,
	poolerStore *store.PoolerCache,
	topoStore topoclient.Store,
	logger *slog.Logger,
) *AppointLeaderAction {
	return &AppointLeaderAction{
		config:      cfg,
		consensus:   consensus,
		rpcClient:   rpcClient,
		poolerStore: poolerStore,
		topoStore:   topoStore,
		logger:      logger,
	}
}

// Execute performs leader appointment by running the coordinator's consensus protocol.
//
// Unlike an earlier version of this action, Execute trusts rechecked as-is and
// does not re-poll or re-judge the leader itself: Engine.attemptRecovery
// opportunistically refreshes reachable-looking shard members via a live RPC
// before its subsequent full re-analysis, which already confirmed the problem
// is still real using the same two-axis judgment the analyzer itself uses —
// not a partial reimplementation of it.
func (a *AppointLeaderAction) Execute(ctx context.Context, rechecked types.RecheckedProblem) error {
	problem := rechecked.Problem
	a.logger.InfoContext(ctx, "executing appoint leader action",
		"shard_key", commontypes.FormatShardKey(problem.ShardKey))

	shard := store.FindShardMembers(a.poolerStore, problem.ShardKey)
	if len(shard.Poolers) == 0 {
		return fmt.Errorf("no poolers found for shard %s", commontypes.FormatShardKey(problem.ShardKey))
	}

	// Use the coordinator's AppointLeader to handle the election.
	// Use the problem code as the reason for the election.
	reason := string(problem.Code)
	cohort := make([]*multiorchdatapb.PoolerHealthState, len(shard.Poolers))
	for i, p := range shard.Poolers {
		cohort[i] = p.Health()
	}
	if err := a.consensus.AppointLeader(ctx, problem.ShardKey, cohort, reason); err != nil {
		return mterrors.Wrap(err, "failed to appoint leader")
	}

	a.logger.InfoContext(ctx, "appoint leader action completed successfully",
		"shard_key", commontypes.FormatShardKey(problem.ShardKey))

	return nil
}

// RefreshBeforeRecheck opportunistically refreshes, in parallel, every shard
// member this action already knows about, via a live RPC, before the engine
// re-verifies the problem still exists. Appointing a new leader is
// disruptive enough to warrant extra confidence beyond the streamed cache
// across the whole cohort it's about to judge -- not just whichever pooler
// happened to trigger detection.
func (a *AppointLeaderAction) RefreshBeforeRecheck(ctx context.Context, problem types.Problem) {
	shard := store.FindShardMembers(a.poolerStore, problem.ShardKey)
	refreshRecentPoolersInParallel(ctx, a.rpcClient, shard.Poolers, a.logger)
}

// RecoveryAction interface implementation

func (a *AppointLeaderAction) RequiresHealthyLeader() bool {
	return false // leader appointment doesn't need existing primary
}

func (a *AppointLeaderAction) Metadata() types.RecoveryMetadata {
	return types.RecoveryMetadata{
		Name:        "AppointLeader",
		Description: "Elect a new primary for the shard using consensus",
		// Two sequential phases (Recruit, then concurrent Promote/SetPrimary),
		// each using the action context directly as their deadline, plus margin
		// so the action context does not race its own phases to the deadline.
		Timeout:     2*timeouts.RuleWriteTimeout + 5*time.Second,
		LockTimeout: 15 * time.Second,
		Retryable:   true, // can retry if it fails
	}
}

// GracePeriod's return value is never consulted: every problem this action is
// attached to is gated by Engine.readyToExecute on collective recruitment
// backoff instead. Reconcile still calls this every cycle for harmless
// eviction bookkeeping, and RecoveryAction requires the method regardless.
//
// TODO: once each recovery action fully owns its "may I act now?" gate (see
// the TODO on readyToExecute), remove this and its backing
// leader-failover-grace-period-base/-max-jitter config entirely.
func (a *AppointLeaderAction) GracePeriod() *types.GracePeriodConfig {
	return &types.GracePeriodConfig{
		BaseDelay: a.config.GetLeaderFailoverGracePeriodBase(),
		MaxJitter: a.config.GetLeaderFailoverGracePeriodMaxJitter(),
	}
}
