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
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/topoclient"
	"github.com/multigres/multigres/go/common/topoclient/memorytopo"
	"github.com/multigres/multigres/go/common/types"
)

func TestInitialBackup(t *testing.T) {
	ctx := context.Background()
	shardKey := types.ShardKey{
		Database:   "mydb",
		TableGroup: "default",
		Shard:      "shard-1",
	}

	newStore := func(t *testing.T) topoclient.Store {
		t.Helper()
		s, _ := memorytopo.NewServerAndFactory(ctx, "test-cell")
		t.Cleanup(func() { _ = s.Close() })
		return s
	}

	t.Run("GetInitialBackup returns nil when not yet claimed", func(t *testing.T) {
		ts := newStore(t)
		backup, err := ts.GetInitialBackup(ctx, shardKey)
		require.NoError(t, err)
		assert.Nil(t, backup)
	})

	t.Run("ClaimInitialBackup wins on first call", func(t *testing.T) {
		ts := newStore(t)
		won, err := ts.ClaimInitialBackup(ctx, shardKey, "20250104-100000F")
		require.NoError(t, err)
		assert.True(t, won, "first caller should win the CAS")

		// Value should now be retrievable.
		backup, err := ts.GetInitialBackup(ctx, shardKey)
		require.NoError(t, err)
		require.NotNil(t, backup)
		assert.Equal(t, "20250104-100000F", backup.BackupId)
	})

	t.Run("ClaimInitialBackup loses when already claimed", func(t *testing.T) {
		ts := newStore(t)

		won, err := ts.ClaimInitialBackup(ctx, shardKey, "20250104-100000F")
		require.NoError(t, err)
		require.True(t, won)

		// Second caller (different backup ID) should lose.
		won, err = ts.ClaimInitialBackup(ctx, shardKey, "20250104-200000F")
		require.NoError(t, err)
		assert.False(t, won, "second caller should lose the CAS race")

		// The original backup ID must still be canonical.
		backup, err := ts.GetInitialBackup(ctx, shardKey)
		require.NoError(t, err)
		require.NotNil(t, backup)
		assert.Equal(t, "20250104-100000F", backup.BackupId)
	})

	t.Run("ClaimInitialBackup is idempotent for same backup ID", func(t *testing.T) {
		ts := newStore(t)

		won, err := ts.ClaimInitialBackup(ctx, shardKey, "20250104-100000F")
		require.NoError(t, err)
		require.True(t, won)

		// Same caller retrying with the same backup ID should lose the CAS
		// (the key already exists) but not be treated as an error.
		won, err = ts.ClaimInitialBackup(ctx, shardKey, "20250104-100000F")
		require.NoError(t, err)
		assert.False(t, won, "retry with same backup_id returns won=false (key already exists)")
	})

	t.Run("different shards are independent", func(t *testing.T) {
		ts := newStore(t)
		shard2 := types.ShardKey{Database: "mydb", TableGroup: "default", Shard: "shard-2"}

		won1, err := ts.ClaimInitialBackup(ctx, shardKey, "backup-for-shard1")
		require.NoError(t, err)
		assert.True(t, won1)

		won2, err := ts.ClaimInitialBackup(ctx, shard2, "backup-for-shard2")
		require.NoError(t, err)
		assert.True(t, won2, "each shard has its own independent key")

		b1, err := ts.GetInitialBackup(ctx, shardKey)
		require.NoError(t, err)
		assert.Equal(t, "backup-for-shard1", b1.BackupId)

		b2, err := ts.GetInitialBackup(ctx, shard2)
		require.NoError(t, err)
		assert.Equal(t, "backup-for-shard2", b2.BackupId)
	})
}
