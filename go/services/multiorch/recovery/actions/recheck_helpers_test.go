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
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/mterrors"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	"github.com/multigres/multigres/go/services/multiorch/internal/snapshot"
	"github.com/multigres/multigres/go/services/multiorch/recovery/types"
	"github.com/multigres/multigres/go/services/multiorch/store"
)

// recheck snapshots the problem's shard from cache, as the engine's recheck does
// just before it runs an action.
func recheck(t *testing.T, cache *store.PoolerCache, problem types.Problem) types.RecheckedProblem {
	t.Helper()
	now := time.Now()
	shard, err := store.SnapshotShard(cache, problem.ShardKey, store.SnapshotOptions{Now: now})
	if mterrors.Code(err) == mtrpcpb.Code_NOT_FOUND {
		// No pooler for the shard: the engine would fail the recheck, but actions
		// are tested against an empty view.
		shard, err = &snapshot.Shard{ShardKey: problem.ShardKey, Now: now}, nil
	}
	require.NoError(t, err)
	return types.RecheckedProblem{Problem: problem, Shard: shard}
}
