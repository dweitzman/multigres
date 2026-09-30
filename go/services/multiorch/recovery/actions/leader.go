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
	"sync"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/rpcclient"
	"github.com/multigres/multigres/go/common/topoclient"
	"github.com/multigres/multigres/go/services/multiorch/store"

	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
)

// RefreshHealthNow issues a live Status RPC against pooler right now -- not
// cached state -- and writes the confirmed fields back into its cached rider
// via Pooler.ApplyStatusResponse, the same field-writing logic
// HealthStream.applySnapshot uses for a regular gossip snapshot -- the two
// can't drift out of sync on which fields a "fresh observation" updates.
// Callers re-derive whatever shard-wide view they need (e.g.
// store.FindShardMembers, or a fresh ShardAnalysis) after this returns,
// rather than juggling a separate live-RPC-shaped snapshot alongside the
// cache: there's only ever one shape, and it's now fresh.
//
// Returns an error, leaving the cache untouched, if the pooler is unreachable
// or the RPC came back with no Status payload.
func RefreshHealthNow(ctx context.Context, rpcClient rpcclient.MultipoolerClient, pooler *store.Pooler) error {
	statusResp, err := rpcClient.Status(ctx, pooler.Health().GetMultipooler(), &multipoolermanagerdatapb.StatusRequest{})
	if err != nil {
		return mterrors.Wrap(err, "pooler unreachable during health refresh")
	}
	if !pooler.ApplyStatusResponse(statusResp, timestamppb.Now()) {
		return mterrors.Errorf(mtrpcpb.Code_INTERNAL, "pooler %s returned an empty status response",
			topoclient.ComponentIDString(pooler.Health().GetMultipooler().GetId()))
	}
	return nil
}

// refreshRecentPoolersInParallel opportunistically refreshes, concurrently,
// every pooler that already looks reachable (a fresh cached observation) --
// skipping ones that already look stale/unreachable, since another attempt
// is unlikely to succeed within the bounded timeout below and an unreachable
// pooler is itself valid evidence for the judgment that follows. Best-effort:
// failures are logged and otherwise ignored, never surfaced as an error.
//
// Only used by AppointLeaderAction today (see types.PreRecheckRefresher):
// appointing a new leader is disruptive enough to warrant refreshing the
// whole cohort it's judging, not just whichever pooler triggered detection.
func refreshRecentPoolersInParallel(ctx context.Context, rpcClient rpcclient.MultipoolerClient, poolers []*store.Pooler, logger *slog.Logger) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	now := time.Now()
	var wg sync.WaitGroup
	for _, pooler := range poolers {
		age, ok := pooler.ObservationAge(now)
		if !ok || age > store.DefaultObservationFreshness {
			continue
		}
		wg.Add(1)
		go func(pooler *store.Pooler) {
			defer wg.Done()
			if err := RefreshHealthNow(ctx, rpcClient, pooler); err != nil {
				logger.DebugContext(ctx, "best-effort pre-recheck refresh failed, continuing with cached state",
					"pooler", topoclient.ComponentIDString(pooler.Health().GetMultipooler().GetId()), "error", err)
			}
		}(pooler)
	}
	wg.Wait()
}
