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

	"github.com/multigres/multigres/go/common/eventlog"
	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/rpcclient"
	"github.com/multigres/multigres/go/common/topoclient"
	"github.com/multigres/multigres/go/services/multiorch/recovery/types"
	"github.com/multigres/multigres/go/services/multiorch/store"

	consensusdatapb "github.com/multigres/multigres/go/pb/consensusdata"
)

// Compile-time assertion that SetPrimaryAction implements types.RecoveryAction.
var _ types.RecoveryAction = (*SetPrimaryAction)(nil)

// SetPrimaryAction delivers consensus information to a pooler that is not
// following the current leader's rule.
//
// It addresses ProblemNeedsSetPrimary: the pooler doesn't know the current
// leader's rule (missing/wrong/stale ReplicationPrimary, including a stale
// self-believed leader). A single SetPrimary RPC tells it who the leader is;
// the pooler-side handler points primary_conninfo at the leader and restarts
// as a standby (rewinding internally if its timeline is still compatible).
//
// The action does nothing more than that one RPC. SetPrimary is idempotent and
// position-fenced, so re-issuing it against a pooler that already knows the
// leader is a harmless no-op — there is no need to re-verify the problem first
// or poll for replication afterwards. If the pooler ends up knowing the right
// leader but still can't stream (e.g. timeline divergence), that is a distinct
// ProblemNeedsRewind handled by RewindAction; the next recovery cycle detects it.
//
// Cohort membership (synchronous_standby_names) is managed separately by
// ReconcileCohortAction.
type SetPrimaryAction struct {
	rpcClient   rpcclient.MultiPoolerClient
	poolerStore *store.PoolerStore
	logger      *slog.Logger
}

// NewSetPrimaryAction creates a new SetPrimary action.
func NewSetPrimaryAction(
	rpcClient rpcclient.MultiPoolerClient,
	poolerStore *store.PoolerStore,
	logger *slog.Logger,
) *SetPrimaryAction {
	return &SetPrimaryAction{
		rpcClient:   rpcClient,
		poolerStore: poolerStore,
		logger:      logger,
	}
}

// Execute tells the affected pooler the current leader's rule via SetPrimary.
func (a *SetPrimaryAction) Execute(ctx context.Context, problem types.Problem) (retErr error) {
	a.logger.InfoContext(ctx, "executing set primary action",
		"shard_key", problem.ShardKey.String(),
		"pooler", problem.PoolerID.Name,
		"problem_code", string(problem.Code))

	target, err := a.poolerStore.FindPoolerByID(problem.PoolerID)
	if err != nil {
		return mterrors.Wrap(err, "failed to find affected pooler")
	}

	poolers := a.poolerStore.FindPoolersInShard(problem.ShardKey)
	if len(poolers) == 0 {
		return fmt.Errorf("no poolers found for shard %s", problem.ShardKey)
	}

	primary, err := a.poolerStore.FindHealthyPrimary(ctx, poolers)
	if err != nil {
		return mterrors.Wrap(err, "failed to find primary")
	}

	a.logger.InfoContext(ctx, "delivering leader rule via SetPrimary",
		"primary", primary.MultiPooler.Id.Name,
		"pooler", target.MultiPooler.Id.Name)

	eventlog.Emit(ctx, a.logger, eventlog.Started, eventlog.NodeJoin{
		NodeName: target.MultiPooler.Id.Name,
	})
	defer func() {
		if retErr == nil {
			eventlog.Emit(ctx, a.logger, eventlog.Success, eventlog.NodeJoin{
				NodeName: target.MultiPooler.Id.Name,
			})
		} else {
			eventlog.Emit(ctx, a.logger, eventlog.Failed, eventlog.NodeJoin{
				NodeName: target.MultiPooler.Id.Name,
			}, "error", retErr)
		}
	}()

	setPrimaryReq := &consensusdatapb.SetPrimaryRequest{
		Leader: topoclient.PoolerAddressFor(primary.MultiPooler),
		Rule:   primary.GetConsensusStatus().GetCurrentPosition().GetRule(),
	}
	if _, err := a.rpcClient.SetPrimary(ctx, target.MultiPooler, setPrimaryReq); err != nil {
		return mterrors.Wrap(err, "SetPrimary RPC failed")
	}

	a.logger.InfoContext(ctx, "set primary action completed",
		"pooler", target.MultiPooler.Id.Name,
		"primary", primary.MultiPooler.Id.Name)
	return nil
}

// RecoveryAction interface implementation.

func (a *SetPrimaryAction) RequiresHealthyLeader() bool {
	return true // We need a healthy leader whose rule we can deliver.
}

func (a *SetPrimaryAction) Metadata() types.RecoveryMetadata {
	return types.RecoveryMetadata{
		Name:        "SetPrimary",
		Timeout:     45 * time.Second,
	}
}

func (a *SetPrimaryAction) Priority() types.Priority {
	return types.PriorityHigh
}

func (a *SetPrimaryAction) GracePeriod() *types.GracePeriodConfig {
	// SetPrimary is position-fenced and idempotent, so a spurious detection
	// costs one harmless RPC. No grace period needed.
	return nil
}
