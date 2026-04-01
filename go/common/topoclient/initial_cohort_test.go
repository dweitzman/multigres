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

package topoclient_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/common/types"
)

func TestClaimInitialCohort(t *testing.T) {
	shardKey := types.ShardKey{
		Database:   "testdb",
		TableGroup: constants.DefaultTableGroup,
		Shard:      "0-inf",
	}

	t.Run("first claim wins and returns proposed IDs sorted", func(t *testing.T) {
		ts := newTestStore(t)
		ctx := t.Context()

		proposed := []string{"zone1_pooler-3", "zone1_pooler-1", "zone1_pooler-2"}
		committed, err := ts.ClaimInitialCohort(ctx, shardKey, proposed)
		require.NoError(t, err)
		assert.Equal(t, []string{"zone1_pooler-1", "zone1_pooler-2", "zone1_pooler-3"}, committed)
	})

	t.Run("second caller gets back first caller's committed list", func(t *testing.T) {
		ts := newTestStore(t)
		ctx := t.Context()

		// First orch sees 3 poolers
		first, err := ts.ClaimInitialCohort(ctx, shardKey, []string{"zone1_pooler-1", "zone1_pooler-2", "zone1_pooler-3"})
		require.NoError(t, err)
		require.Equal(t, []string{"zone1_pooler-1", "zone1_pooler-2", "zone1_pooler-3"}, first)

		// Second orch sees only 2 poolers (different view)
		second, err := ts.ClaimInitialCohort(ctx, shardKey, []string{"zone1_pooler-1", "zone1_pooler-2"})
		require.NoError(t, err)
		// Gets back the first orch's committed list, not its own proposal
		assert.Equal(t, first, second, "second orch should use the committed list, not its own proposal")
	})

	t.Run("retry after crash gets back same committed list", func(t *testing.T) {
		ts := newTestStore(t)
		ctx := t.Context()

		proposed := []string{"zone1_pooler-1", "zone1_pooler-2"}
		first, err := ts.ClaimInitialCohort(ctx, shardKey, proposed)
		require.NoError(t, err)

		// Simulate crash + retry with same proposal
		retry, err := ts.ClaimInitialCohort(ctx, shardKey, proposed)
		require.NoError(t, err)
		assert.Equal(t, first, retry)
	})

	t.Run("different shards are independent", func(t *testing.T) {
		ts := newTestStore(t)
		ctx := t.Context()

		shard1 := shardKey
		shard2 := types.ShardKey{Database: "testdb", TableGroup: constants.DefaultTableGroup, Shard: "1-inf"}

		ids1, err := ts.ClaimInitialCohort(ctx, shard1, []string{"zone1_pooler-1"})
		require.NoError(t, err)

		ids2, err := ts.ClaimInitialCohort(ctx, shard2, []string{"zone1_pooler-2"})
		require.NoError(t, err)

		assert.Equal(t, []string{"zone1_pooler-1"}, ids1)
		assert.Equal(t, []string{"zone1_pooler-2"}, ids2)
	})
}
