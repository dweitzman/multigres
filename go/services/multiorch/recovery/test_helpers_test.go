// Copyright 2025 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package recovery

import (
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/rpcclient"
	"github.com/multigres/multigres/go/common/topoclient"
	"github.com/multigres/multigres/go/pb/clustermetadata"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	"github.com/multigres/multigres/go/services/multiorch/consensus"
	"github.com/multigres/multigres/go/services/multiorch/internal/snapshot"
	"github.com/multigres/multigres/go/services/multiorch/recovery/types"
	"github.com/multigres/multigres/go/services/multiorch/store"
)

// newTestCoordinator creates a coordinator for tests that need one but manage their own Engine creation.
func newTestCoordinator(ts topoclient.Store, rpcClient rpcclient.MultipoolerClient, cell string) *consensus.Coordinator {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	coordID := &clustermetadata.ID{
		Component: clustermetadata.ID_MULTIORCH,
		Cell:      cell,
		Name:      "test-coordinator",
	}
	return consensus.NewCoordinator(coordID, ts, rpcClient, logger)
}

// testShard freezes the engine's cache for shardKey, as the recovery cycle does before
// detecting problems. A shard with no poolers gives an empty snapshot.
func testShard(t *testing.T, engine *Engine, shardKey *clustermetadata.ShardKey) *snapshot.Shard {
	t.Helper()
	now := time.Now()
	shard, err := store.SnapshotShard(engine.poolerCache, shardKey, store.SnapshotOptions{Now: now})
	if mterrors.Code(err) == mtrpcpb.Code_NOT_FOUND {
		return &snapshot.Shard{ShardKey: shardKey, Now: now}
	}
	require.NoError(t, err)
	return shard
}

// shardForProblems freezes the shard that problems belong to.
func shardForProblems(t *testing.T, engine *Engine, problems []types.Problem) *snapshot.Shard {
	t.Helper()
	var shardKey *clustermetadata.ShardKey
	if len(problems) > 0 {
		shardKey = problems[0].ShardKey
	}
	return testShard(t, engine, shardKey)
}
