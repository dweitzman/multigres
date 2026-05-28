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
	"errors"
	"log/slog"

	"github.com/multigres/multigres/go/common/eventlog"
	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/rpcclient"
	"github.com/multigres/multigres/go/common/topoclient"
	"github.com/multigres/multigres/go/services/multiorch/config"
	"github.com/multigres/multigres/go/services/multiorch/store"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multiorchdatapb "github.com/multigres/multigres/go/pb/multiorchdata"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
)

// errPoolerDrained is returned by RewindAction.Run when pg_rewind is not
// feasible and the pooler has been successfully marked DRAINED. The caller
// should stop attempting to verify replication and treat the recovery as
// complete (the pooler is intentionally out of service).
var errPoolerDrained = errors.New("pooler marked as DRAINED: replication cannot be established")

// RewindAction repairs a divergent standby by running pg_rewind against a
// source primary. The pooler-side RewindToSource handler does the full
// sequence atomically: stop postgres, dry-run rewind, run real rewind if
// needed, restart as standby. If pg_rewind is infeasible (e.g. missing WAL
// on the source), the pooler is marked DRAINED in topology — it is
// intentionally taken out of service.
//
// This action is currently invoked as a fallback from FixReplicationAction
// when SetTermPrimary completes but the WAL receiver still won't stream.
// In the future it should be triggered directly by a "replica stuck despite
// correct configuration" analyzer; the bundled fallback exists only to keep
// divergent-replica recovery working until that analyzer lands.
type RewindAction struct {
	config      *config.Config
	rpcClient   rpcclient.MultiPoolerClient
	poolerStore *store.PoolerStore
	topoStore   topoclient.Store
	logger      *slog.Logger
}

// NewRewindAction creates a new RewindAction.
func NewRewindAction(
	cfg *config.Config,
	rpcClient rpcclient.MultiPoolerClient,
	poolerStore *store.PoolerStore,
	topoStore topoclient.Store,
	logger *slog.Logger,
) *RewindAction {
	return &RewindAction{
		config:      cfg,
		rpcClient:   rpcClient,
		poolerStore: poolerStore,
		topoStore:   topoStore,
		logger:      logger,
	}
}

// Run executes pg_rewind on `target` against `source`. Returns errPoolerDrained
// when the pooler has been marked DRAINED because pg_rewind is infeasible;
// callers should treat that as a non-error terminal state.
func (a *RewindAction) Run(
	ctx context.Context,
	source *multiorchdatapb.PoolerHealthState,
	target *multiorchdatapb.PoolerHealthState,
) error {
	a.logger.InfoContext(ctx, "attempting pg_rewind",
		"target", target.MultiPooler.Id.Name,
		"source", source.MultiPooler.Id.Name)

	rewindReq := &multipoolermanagerdatapb.RewindToSourceRequest{
		Source: source.MultiPooler,
	}
	rewindResp, err := a.rpcClient.RewindToSource(ctx, target.MultiPooler, rewindReq)
	if err != nil {
		// RPC failure (e.g. source postgres unreachable) is transient — do not
		// drain the pooler. Return an error so the next recovery cycle retries.
		a.logger.WarnContext(ctx, "pg_rewind RPC failed, will retry next cycle",
			"target", target.MultiPooler.Id.Name,
			"error", err)
		return mterrors.Wrap(err, "pg_rewind RPC failed")
	}
	if !rewindResp.Success {
		a.logger.WarnContext(ctx, "pg_rewind not feasible, marking as DRAINED",
			"target", target.MultiPooler.Id.Name,
			"error", rewindResp.ErrorMessage)
		if drainErr := a.markPoolerDrained(ctx, target); drainErr != nil {
			return drainErr
		}
		return errPoolerDrained
	}

	if rewindResp.RewindPerformed {
		a.logger.InfoContext(ctx, "pg_rewind completed successfully - servers were diverged",
			"target", target.MultiPooler.Id.Name)
	} else {
		a.logger.InfoContext(ctx, "pg_rewind not needed - timelines are compatible",
			"target", target.MultiPooler.Id.Name)
	}

	return nil
}

// markPoolerDrained marks a pooler as DRAINED in the topology.
func (a *RewindAction) markPoolerDrained(ctx context.Context, pooler *multiorchdatapb.PoolerHealthState) (retErr error) {
	nodeName := pooler.MultiPooler.Id.Name
	a.logger.InfoContext(ctx, "marking pooler as DRAINED", "pooler", nodeName)
	eventlog.Emit(ctx, a.logger, eventlog.Started, eventlog.NodeDrain{NodeName: nodeName, Reason: "rewind_not_feasible"})
	defer func() {
		if retErr == nil {
			eventlog.Emit(ctx, a.logger, eventlog.Success, eventlog.NodeDrain{NodeName: nodeName, Reason: "rewind_not_feasible"})
		} else {
			eventlog.Emit(ctx, a.logger, eventlog.Failed, eventlog.NodeDrain{NodeName: nodeName, Reason: "rewind_not_feasible"}, "error", retErr)
		}
	}()
	_, err := a.topoStore.UpdateMultiPoolerFields(ctx, pooler.MultiPooler.Id, func(mp *clustermetadatapb.MultiPooler) error {
		mp.Type = clustermetadatapb.PoolerType_DRAINED
		return nil
	})
	if err != nil {
		return mterrors.Wrap(err, "failed to mark pooler as DRAINED")
	}
	return nil
}
