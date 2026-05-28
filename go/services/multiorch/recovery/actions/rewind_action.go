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
	"log/slog"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/rpcclient"
	"github.com/multigres/multigres/go/services/multiorch/config"
	"github.com/multigres/multigres/go/services/multiorch/store"

	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	multiorchdatapb "github.com/multigres/multigres/go/pb/multiorchdata"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
)

// RewindAction repairs a divergent standby by running pg_rewind against a
// source primary. The pooler-side RewindToSource handler does the full
// sequence atomically: stop postgres, dry-run rewind, run real rewind if
// needed, restart as standby.
//
// Orch does NOT mark the pooler DRAINED when rewind is infeasible: a pooler
// republishes its own MultiPooler record (including Type) every cycle, and
// an orch-side Type write would be overwritten on the pooler's next publish.
// Persistent self-drain is the pooler's responsibility — the postgres
// monitor should detect prolonged inability to replicate and write
// Type=DRAINED to its own record. See the TODO in multipooler manager.go.
//
// This action is currently invoked as a fallback from FixReplicationAction
// when SetTermPrimary completes but the WAL receiver still won't stream.
type RewindAction struct {
	config      *config.Config
	rpcClient   rpcclient.MultiPoolerClient
	poolerStore *store.PoolerStore
	logger      *slog.Logger
}

// NewRewindAction creates a new RewindAction.
func NewRewindAction(
	cfg *config.Config,
	rpcClient rpcclient.MultiPoolerClient,
	poolerStore *store.PoolerStore,
	logger *slog.Logger,
) *RewindAction {
	return &RewindAction{
		config:      cfg,
		rpcClient:   rpcClient,
		poolerStore: poolerStore,
		logger:      logger,
	}
}

// Run executes pg_rewind on `target` against `source`. Returns an error on
// any failure (RPC failure, rewind infeasible, etc.); the next recovery
// cycle retries. Persistent infeasibility is recognized and self-marked
// DRAINED by the pooler's postgres monitor, not by orch.
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
		a.logger.WarnContext(ctx, "pg_rewind RPC failed, will retry next cycle",
			"target", target.MultiPooler.Id.Name,
			"error", err)
		return mterrors.Wrap(err, "pg_rewind RPC failed")
	}
	if !rewindResp.Success {
		a.logger.WarnContext(ctx, "pg_rewind not feasible",
			"target", target.MultiPooler.Id.Name,
			"error", rewindResp.ErrorMessage)
		return mterrors.Errorf(mtrpcpb.Code_FAILED_PRECONDITION,
			"pg_rewind not feasible on %s: %s",
			target.MultiPooler.Id.Name, rewindResp.ErrorMessage)
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
