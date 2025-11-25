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
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/clustermetadata/topo"
	"github.com/multigres/multigres/go/clustermetadata/topo/memorytopo"
	"github.com/multigres/multigres/go/common/rpcclient"
	"github.com/multigres/multigres/go/multiorch/config"
	"github.com/multigres/multigres/go/pb/clustermetadata"
)

// poolerKey creates the store key for a pooler
func poolerKey(cell, name string) string {
	return topo.MultiPoolerIDString(clustermetadata.ID_builder{
		Component: clustermetadata.ID_MULTIPOOLER,
		Cell:      cell,
		Name:      name,
	}.Build())
}

func TestDiscovery_DatabaseLevelWatch(t *testing.T) {
	ctx := context.Background()
	ts, _ := memorytopo.NewServerAndFactory(ctx, "zone1")
	defer ts.Close()

	cfg := config.NewTestConfig(
		config.WithCell("zone1"),
		config.WithClusterMetadataRefreshTimeout(5*time.Second),
	)

	engine := NewEngine(
		ts,
		slog.Default(),
		cfg,
		[]config.WatchTarget{{Database: "mydb"}},
		&rpcclient.FakeClient{},
	)

	// Initial state: 2 poolers in different tablegroups
	require.NoError(t, ts.CreateMultiPooler(ctx, clustermetadata.MultiPooler_builder{
		Id:       clustermetadata.ID_builder{Component: clustermetadata.ID_MULTIPOOLER, Cell: "zone1", Name: "pooler1"}.Build(),
		Database: "mydb", TableGroup: "tg1", Shard: "0",
	}.Build()))
	require.NoError(t, ts.CreateMultiPooler(ctx, clustermetadata.MultiPooler_builder{
		Id:       clustermetadata.ID_builder{Component: clustermetadata.ID_MULTIPOOLER, Cell: "zone1", Name: "pooler2"}.Build(),
		Database: "mydb", TableGroup: "tg2", Shard: "0",
	}.Build()))

	// First refresh - should discover both
	engine.refreshClusterMetadata()
	require.Equal(t, 2, engine.poolerStore.Len())

	_, ok := engine.poolerStore.Get(poolerKey("zone1", "pooler1"))
	require.True(t, ok, "pooler1 should be discovered")
	_, ok = engine.poolerStore.Get(poolerKey("zone1", "pooler2"))
	require.True(t, ok, "pooler2 should be discovered")

	// Add new tablegroup with pooler
	require.NoError(t, ts.CreateMultiPooler(ctx, clustermetadata.MultiPooler_builder{
		Id:       clustermetadata.ID_builder{Component: clustermetadata.ID_MULTIPOOLER, Cell: "zone1", Name: "pooler3"}.Build(),
		Database: "mydb", TableGroup: "tg3", Shard: "0",
	}.Build()))

	// Second refresh - should discover new tablegroup's pooler
	engine.refreshClusterMetadata()
	require.Equal(t, 3, engine.poolerStore.Len())

	_, ok = engine.poolerStore.Get(poolerKey("zone1", "pooler3"))
	require.True(t, ok, "pooler3 in new tablegroup should be discovered")

	// Add new shard in existing tablegroup
	require.NoError(t, ts.CreateMultiPooler(ctx, clustermetadata.MultiPooler_builder{
		Id:       clustermetadata.ID_builder{Component: clustermetadata.ID_MULTIPOOLER, Cell: "zone1", Name: "pooler4"}.Build(),
		Database: "mydb", TableGroup: "tg1", Shard: "1",
	}.Build()))

	// Third refresh - should discover new shard's pooler
	engine.refreshClusterMetadata()
	require.Equal(t, 4, engine.poolerStore.Len())

	_, ok = engine.poolerStore.Get(poolerKey("zone1", "pooler4"))
	require.True(t, ok, "pooler4 in new shard should be discovered")

	// Remove a pooler from topology
	require.NoError(t, ts.UnregisterMultiPooler(ctx, clustermetadata.ID_builder{
		Component: clustermetadata.ID_MULTIPOOLER, Cell: "zone1", Name: "pooler2",
	}.Build()))

	// Fourth refresh - removed pooler still in store (bookkeeping removes it later)
	engine.refreshClusterMetadata()
	require.Equal(t, 4, engine.poolerStore.Len(), "store should still contain all poolers, bookkeeping handles removal")
}

func TestDiscovery_TablegroupLevelWatch(t *testing.T) {
	ctx := context.Background()
	ts, _ := memorytopo.NewServerAndFactory(ctx, "zone1")
	defer ts.Close()

	cfg := config.NewTestConfig(
		config.WithCell("zone1"),
		config.WithClusterMetadataRefreshTimeout(5*time.Second),
	)

	engine := NewEngine(
		ts,
		slog.Default(),
		cfg,
		[]config.WatchTarget{{Database: "mydb", TableGroup: "tg1"}},
		&rpcclient.FakeClient{},
	)

	// Initial state: poolers in tg1 and tg2
	require.NoError(t, ts.CreateMultiPooler(ctx, clustermetadata.MultiPooler_builder{
		Id:       clustermetadata.ID_builder{Component: clustermetadata.ID_MULTIPOOLER, Cell: "zone1", Name: "pooler1"}.Build(),
		Database: "mydb", TableGroup: "tg1", Shard: "0",
	}.Build()))
	require.NoError(t, ts.CreateMultiPooler(ctx, clustermetadata.MultiPooler_builder{
		Id:       clustermetadata.ID_builder{Component: clustermetadata.ID_MULTIPOOLER, Cell: "zone1", Name: "pooler2"}.Build(),
		Database: "mydb", TableGroup: "tg2", Shard: "0",
	}.Build()))

	// First refresh - should only discover tg1
	engine.refreshClusterMetadata()
	require.Equal(t, 1, engine.poolerStore.Len())

	_, ok := engine.poolerStore.Get(poolerKey("zone1", "pooler1"))
	require.True(t, ok, "pooler1 in tg1 should be discovered")
	_, ok = engine.poolerStore.Get(poolerKey("zone1", "pooler2"))
	require.False(t, ok, "pooler2 in tg2 should NOT be discovered")

	// Add new shard in tg1
	require.NoError(t, ts.CreateMultiPooler(ctx, clustermetadata.MultiPooler_builder{
		Id:       clustermetadata.ID_builder{Component: clustermetadata.ID_MULTIPOOLER, Cell: "zone1", Name: "pooler3"}.Build(),
		Database: "mydb", TableGroup: "tg1", Shard: "1",
	}.Build()))

	// Second refresh - should discover new shard in tg1
	engine.refreshClusterMetadata()
	require.Equal(t, 2, engine.poolerStore.Len())

	_, ok = engine.poolerStore.Get(poolerKey("zone1", "pooler3"))
	require.True(t, ok, "pooler3 in new shard of tg1 should be discovered")

	// Add new tablegroup tg3 with pooler
	require.NoError(t, ts.CreateMultiPooler(ctx, clustermetadata.MultiPooler_builder{
		Id:       clustermetadata.ID_builder{Component: clustermetadata.ID_MULTIPOOLER, Cell: "zone1", Name: "pooler4"}.Build(),
		Database: "mydb", TableGroup: "tg3", Shard: "0",
	}.Build()))

	// Third refresh - should NOT discover new tablegroup
	engine.refreshClusterMetadata()
	require.Equal(t, 2, engine.poolerStore.Len())

	_, ok = engine.poolerStore.Get(poolerKey("zone1", "pooler4"))
	require.False(t, ok, "pooler4 in tg3 should NOT be discovered (only watching tg1)")
}

func TestDiscovery_ShardLevelWatch(t *testing.T) {
	ctx := context.Background()
	ts, _ := memorytopo.NewServerAndFactory(ctx, "zone1")
	defer ts.Close()

	cfg := config.NewTestConfig(
		config.WithCell("zone1"),
		config.WithClusterMetadataRefreshTimeout(5*time.Second),
	)

	engine := NewEngine(
		ts,
		slog.Default(),
		cfg,
		[]config.WatchTarget{{Database: "mydb", TableGroup: "tg1", Shard: "0"}},
		&rpcclient.FakeClient{},
	)

	// Initial state: poolers in different shards and tablegroups
	require.NoError(t, ts.CreateMultiPooler(ctx, clustermetadata.MultiPooler_builder{
		Id:       clustermetadata.ID_builder{Component: clustermetadata.ID_MULTIPOOLER, Cell: "zone1", Name: "pooler1"}.Build(),
		Database: "mydb", TableGroup: "tg1", Shard: "0",
	}.Build()))
	require.NoError(t, ts.CreateMultiPooler(ctx, clustermetadata.MultiPooler_builder{
		Id:       clustermetadata.ID_builder{Component: clustermetadata.ID_MULTIPOOLER, Cell: "zone1", Name: "pooler2"}.Build(),
		Database: "mydb", TableGroup: "tg1", Shard: "1",
	}.Build()))
	require.NoError(t, ts.CreateMultiPooler(ctx, clustermetadata.MultiPooler_builder{
		Id:       clustermetadata.ID_builder{Component: clustermetadata.ID_MULTIPOOLER, Cell: "zone1", Name: "pooler3"}.Build(),
		Database: "mydb", TableGroup: "tg2", Shard: "0",
	}.Build()))

	// First refresh - should only discover tg1/shard0
	engine.refreshClusterMetadata()
	require.Equal(t, 1, engine.poolerStore.Len())

	_, ok := engine.poolerStore.Get(poolerKey("zone1", "pooler1"))
	require.True(t, ok, "pooler1 in tg1/shard0 should be discovered")
	_, ok = engine.poolerStore.Get(poolerKey("zone1", "pooler2"))
	require.False(t, ok, "pooler2 in tg1/shard1 should NOT be discovered")
	_, ok = engine.poolerStore.Get(poolerKey("zone1", "pooler3"))
	require.False(t, ok, "pooler3 in tg2/shard0 should NOT be discovered")

	// Add another pooler to the watched shard
	require.NoError(t, ts.CreateMultiPooler(ctx, clustermetadata.MultiPooler_builder{
		Id:       clustermetadata.ID_builder{Component: clustermetadata.ID_MULTIPOOLER, Cell: "zone1", Name: "pooler4"}.Build(),
		Database: "mydb", TableGroup: "tg1", Shard: "0",
	}.Build()))

	// Second refresh - should discover new pooler in same shard
	engine.refreshClusterMetadata()
	require.Equal(t, 2, engine.poolerStore.Len())

	_, ok = engine.poolerStore.Get(poolerKey("zone1", "pooler4"))
	require.True(t, ok, "pooler4 in tg1/shard0 should be discovered")

	// Add new shard in same tablegroup
	require.NoError(t, ts.CreateMultiPooler(ctx, clustermetadata.MultiPooler_builder{
		Id:       clustermetadata.ID_builder{Component: clustermetadata.ID_MULTIPOOLER, Cell: "zone1", Name: "pooler5"}.Build(),
		Database: "mydb", TableGroup: "tg1", Shard: "2",
	}.Build()))

	// Third refresh - should NOT discover new shard
	engine.refreshClusterMetadata()
	require.Equal(t, 2, engine.poolerStore.Len())

	_, ok = engine.poolerStore.Get(poolerKey("zone1", "pooler5"))
	require.False(t, ok, "pooler5 in tg1/shard2 should NOT be discovered (only watching shard0)")

	// Add new tablegroup
	require.NoError(t, ts.CreateMultiPooler(ctx, clustermetadata.MultiPooler_builder{
		Id:       clustermetadata.ID_builder{Component: clustermetadata.ID_MULTIPOOLER, Cell: "zone1", Name: "pooler6"}.Build(),
		Database: "mydb", TableGroup: "tg3", Shard: "0",
	}.Build()))

	// Fourth refresh - should NOT discover new tablegroup
	engine.refreshClusterMetadata()
	require.Equal(t, 2, engine.poolerStore.Len())

	_, ok = engine.poolerStore.Get(poolerKey("zone1", "pooler6"))
	require.False(t, ok, "pooler6 in tg3 should NOT be discovered (only watching tg1/shard0)")
}

func TestDiscovery_PreservesTimestamps(t *testing.T) {
	ctx := context.Background()
	ts, _ := memorytopo.NewServerAndFactory(ctx, "zone1")
	defer ts.Close()

	cfg := config.NewTestConfig(
		config.WithCell("zone1"),
		config.WithClusterMetadataRefreshTimeout(5*time.Second),
	)

	engine := NewEngine(
		ts,
		slog.Default(),
		cfg,
		[]config.WatchTarget{{Database: "mydb"}},
		&rpcclient.FakeClient{},
	)

	// Add initial pooler
	require.NoError(t, ts.CreateMultiPooler(ctx, clustermetadata.MultiPooler_builder{
		Id:       clustermetadata.ID_builder{Component: clustermetadata.ID_MULTIPOOLER, Cell: "zone1", Name: "pooler1"}.Build(),
		Database: "mydb", TableGroup: "tg1", Shard: "0",
		Hostname: "host1",
	}.Build()))

	// First refresh - discover pooler
	engine.refreshClusterMetadata()
	require.Equal(t, 1, engine.poolerStore.Len())

	poolerInfo, ok := engine.poolerStore.Get(poolerKey("zone1", "pooler1"))
	require.True(t, ok)
	require.Equal(t, "host1", poolerInfo.MultiPooler.GetHostname())
	require.True(t, poolerInfo.LastSeen.IsZero(), "LastSeen should be zero (not yet health checked)")
	require.False(t, poolerInfo.IsUpToDate, "IsUpToDate should be false")

	// Simulate health check by updating timestamps
	now := time.Now()
	poolerInfo.LastSeen = now
	poolerInfo.LastCheckAttempted = now
	poolerInfo.LastCheckSuccessful = now
	poolerInfo.IsUpToDate = true
	poolerInfo.IsLastCheckValid = true
	engine.poolerStore.Set(poolerKey("zone1", "pooler1"), poolerInfo)

	// Update topology record (hostname changed)
	retrieved, err := ts.GetMultiPooler(ctx, clustermetadata.ID_builder{
		Component: clustermetadata.ID_MULTIPOOLER, Cell: "zone1", Name: "pooler1",
	}.Build())
	require.NoError(t, err)
	retrieved.MultiPooler.SetHostname("host2")
	require.NoError(t, ts.UpdateMultiPooler(ctx, retrieved))

	// Second refresh - should update MultiPooler but preserve timestamps
	engine.refreshClusterMetadata()
	require.Equal(t, 1, engine.poolerStore.Len())

	updatedInfo, ok := engine.poolerStore.Get(poolerKey("zone1", "pooler1"))
	require.True(t, ok)

	// MultiPooler record should be updated
	require.Equal(t, "host2", updatedInfo.MultiPooler.GetHostname(), "hostname should be updated")

	// Timestamps and computed fields should be preserved
	require.Equal(t, now, updatedInfo.LastSeen, "LastSeen should be preserved")
	require.Equal(t, now, updatedInfo.LastCheckAttempted, "LastCheckAttempted should be preserved")
	require.Equal(t, now, updatedInfo.LastCheckSuccessful, "LastCheckSuccessful should be preserved")
	require.True(t, updatedInfo.IsUpToDate, "IsUpToDate should be preserved")
	require.True(t, updatedInfo.IsLastCheckValid, "IsLastCheckValid should be preserved")
}

func TestDiscovery_MultipleWatchTargets(t *testing.T) {
	ctx := context.Background()
	ts, _ := memorytopo.NewServerAndFactory(ctx, "zone1")
	defer ts.Close()

	cfg := config.NewTestConfig(
		config.WithCell("zone1"),
		config.WithClusterMetadataRefreshTimeout(5*time.Second),
	)

	engine := NewEngine(
		ts,
		slog.Default(),
		cfg,
		[]config.WatchTarget{
			{Database: "db1"},                                // Watch entire database
			{Database: "db2", TableGroup: "tg1"},             // Watch specific tablegroup
			{Database: "db3", TableGroup: "tg1", Shard: "0"}, // Watch specific shard
		},
		&rpcclient.FakeClient{},
	)

	// Add poolers for different watch targets
	require.NoError(t, ts.CreateMultiPooler(ctx, clustermetadata.MultiPooler_builder{
		Id:       clustermetadata.ID_builder{Component: clustermetadata.ID_MULTIPOOLER, Cell: "zone1", Name: "pooler1"}.Build(),
		Database: "db1", TableGroup: "tg1", Shard: "0",
	}.Build()))
	require.NoError(t, ts.CreateMultiPooler(ctx, clustermetadata.MultiPooler_builder{
		Id:       clustermetadata.ID_builder{Component: clustermetadata.ID_MULTIPOOLER, Cell: "zone1", Name: "pooler2"}.Build(),
		Database: "db1", TableGroup: "tg2", Shard: "1", // Should be discovered (db1 watch)
	}.Build()))
	require.NoError(t, ts.CreateMultiPooler(ctx, clustermetadata.MultiPooler_builder{
		Id:       clustermetadata.ID_builder{Component: clustermetadata.ID_MULTIPOOLER, Cell: "zone1", Name: "pooler3"}.Build(),
		Database: "db2", TableGroup: "tg1", Shard: "0", // Should be discovered (db2/tg1 watch)
	}.Build()))
	require.NoError(t, ts.CreateMultiPooler(ctx, clustermetadata.MultiPooler_builder{
		Id:       clustermetadata.ID_builder{Component: clustermetadata.ID_MULTIPOOLER, Cell: "zone1", Name: "pooler4"}.Build(),
		Database: "db2", TableGroup: "tg2", Shard: "0", // Should NOT be discovered
	}.Build()))
	require.NoError(t, ts.CreateMultiPooler(ctx, clustermetadata.MultiPooler_builder{
		Id:       clustermetadata.ID_builder{Component: clustermetadata.ID_MULTIPOOLER, Cell: "zone1", Name: "pooler5"}.Build(),
		Database: "db3", TableGroup: "tg1", Shard: "0", // Should be discovered (db3/tg1/0 watch)
	}.Build()))
	require.NoError(t, ts.CreateMultiPooler(ctx, clustermetadata.MultiPooler_builder{
		Id:       clustermetadata.ID_builder{Component: clustermetadata.ID_MULTIPOOLER, Cell: "zone1", Name: "pooler6"}.Build(),
		Database: "db3", TableGroup: "tg1", Shard: "1", // Should NOT be discovered
	}.Build()))

	engine.refreshClusterMetadata()

	// Should discover: pooler1, pooler2, pooler3, pooler5 (4 total)
	require.Equal(t, 4, engine.poolerStore.Len())

	_, ok := engine.poolerStore.Get(poolerKey("zone1", "pooler1"))
	require.True(t, ok)
	_, ok = engine.poolerStore.Get(poolerKey("zone1", "pooler2"))
	require.True(t, ok)
	_, ok = engine.poolerStore.Get(poolerKey("zone1", "pooler3"))
	require.True(t, ok)
	_, ok = engine.poolerStore.Get(poolerKey("zone1", "pooler4"))
	require.False(t, ok, "pooler4 should NOT be discovered (wrong tablegroup)")
	_, ok = engine.poolerStore.Get(poolerKey("zone1", "pooler5"))
	require.True(t, ok)
	_, ok = engine.poolerStore.Get(poolerKey("zone1", "pooler6"))
	require.False(t, ok, "pooler6 should NOT be discovered (wrong shard)")
}

func TestDiscovery_EmptyTopology(t *testing.T) {
	ctx := context.Background()
	ts, _ := memorytopo.NewServerAndFactory(ctx, "zone1")
	defer ts.Close()

	cfg := config.NewTestConfig(
		config.WithCell("zone1"),
		config.WithClusterMetadataRefreshTimeout(5*time.Second),
	)

	engine := NewEngine(
		ts,
		slog.Default(),
		cfg,
		[]config.WatchTarget{{Database: "mydb"}},
		&rpcclient.FakeClient{},
	)

	// Refresh with empty topology
	engine.refreshClusterMetadata()
	require.Equal(t, 0, engine.poolerStore.Len())

	// Add some poolers
	require.NoError(t, ts.CreateMultiPooler(ctx, clustermetadata.MultiPooler_builder{
		Id:       clustermetadata.ID_builder{Component: clustermetadata.ID_MULTIPOOLER, Cell: "zone1", Name: "pooler1"}.Build(),
		Database: "mydb", TableGroup: "tg1", Shard: "0",
	}.Build()))
	engine.refreshClusterMetadata()
	require.Equal(t, 1, engine.poolerStore.Len())
}
