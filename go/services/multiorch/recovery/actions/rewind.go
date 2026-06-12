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

	"github.com/multigres/multigres/go/common/eventlog"
	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/rpcclient"
	"github.com/multigres/multigres/go/services/multiorch/recovery/types"
	"github.com/multigres/multigres/go/services/multiorch/store"

	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
)

// Compile-time assertion that RewindAction implements types.RecoveryAction.
var _ types.RecoveryAction = (*RewindAction)(nil)

// RewindAction unsticks a pooler that already knows the current leader's rule
// but still can't stream from it — typically because its timeline diverged from
// the leader's (a former leader's unreplicated WAL).
//
// It addresses ProblemNeedsRewind. SetPrimary alone cannot fix this: the pooler
// has the right information and still can't follow the leader, so its data
// directory has to be rewound to the leader's history before replication can
// resume. RewindToSource handles that atomically on the pooler side (stop
// postgres → dry-run → rewind if needed → restart as standby).
//
// If pg_rewind is not feasible (e.g. the required WAL has been recycled), the
// pooler cannot rejoin and needs replacement. We emit a drain event for
// observability but deliberately do NOT write the pooler's topology Type: a
// pooler owns its own record and keeps republishing it, so an external write
// from orch would be clobbered. Durable "needs replacement" signaling will go
// through the pooler itself (future work).
type RewindAction struct {
	rpcClient   rpcclient.MultiPoolerClient
	poolerStore *store.PoolerStore
	logger      *slog.Logger
}

// NewRewindAction creates a new rewind action.
func NewRewindAction(
	rpcClient rpcclient.MultiPoolerClient,
	poolerStore *store.PoolerStore,
	logger *slog.Logger,
) *RewindAction {
	return &RewindAction{
		rpcClient:   rpcClient,
		poolerStore: poolerStore,
		logger:      logger,
	}
}

// Execute rewinds the affected pooler to the current leader's history.
func (a *RewindAction) Execute(ctx context.Context, problem types.Problem) (retErr error) {
	a.logger.InfoContext(ctx, "executing rewind action",
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

	a.logger.InfoContext(ctx, "attempting pg_rewind",
		"pooler", target.MultiPooler.Id.Name,
		"source", primary.MultiPooler.Id.Name)

	rewindResp, err := a.rpcClient.RewindToSource(ctx, target.MultiPooler,
		&multipoolermanagerdatapb.RewindToSourceRequest{Source: primary.MultiPooler})
	if err != nil {
		// RPC failure (e.g. source postgres unreachable) is transient — do not
		// drain. Return an error so the next recovery cycle retries.
		a.logger.WarnContext(ctx, "pg_rewind RPC failed, will retry next cycle",
			"pooler", target.MultiPooler.Id.Name, "error", err)
		return mterrors.Wrap(err, "pg_rewind RPC failed")
	}
	if !rewindResp.Success {
		a.logger.WarnContext(ctx, "pg_rewind not feasible; draining pooler",
			"pooler", target.MultiPooler.Id.Name, "error", rewindResp.ErrorMessage)
		eventlog.Emit(ctx, a.logger, eventlog.Success, eventlog.NodeDrain{
			NodeName: target.MultiPooler.Id.Name,
			Reason:   "rewind_not_feasible",
		})
		return nil
	}

	if rewindResp.RewindPerformed {
		a.logger.InfoContext(ctx, "pg_rewind completed - servers were diverged",
			"pooler", target.MultiPooler.Id.Name)
	} else {
		a.logger.InfoContext(ctx, "pg_rewind not needed - timelines are compatible",
			"pooler", target.MultiPooler.Id.Name)
	}
	return nil
}

// RecoveryAction interface implementation.

func (a *RewindAction) RequiresHealthyLeader() bool {
	return true // We rewind to the current leader's history.
}

func (a *RewindAction) Metadata() types.RecoveryMetadata {
	return types.RecoveryMetadata{
		Name:    "Rewind",
		Timeout: 60 * time.Second,
	}
}

func (a *RewindAction) Priority() types.Priority {
	return types.PriorityHigh
}

func (a *RewindAction) GracePeriod() *types.GracePeriodConfig {
	return nil
}
