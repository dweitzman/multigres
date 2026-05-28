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

package store

import (
	"context"
	"errors"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/rpcclient"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multiorchdatapb "github.com/multigres/multigres/go/pb/multiorchdata"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
)

func TestPoolerStore_FindPoolersInShard(t *testing.T) {
	poolerStore := NewPoolerStore(nil, slog.Default())

	// Add poolers to different shards
	poolerStore.Set("pooler1", &multiorchdatapb.PoolerHealthState{
		MultiPooler: &clustermetadatapb.MultiPooler{
			Id: &clustermetadatapb.ID{Cell: "cell1", Name: "pooler1"},
			ShardKey: &clustermetadatapb.ShardKey{
				Database:   "db1",
				TableGroup: "tg1",
				Shard:      "0",
			},
		},
	})
	poolerStore.Set("pooler2", &multiorchdatapb.PoolerHealthState{
		MultiPooler: &clustermetadatapb.MultiPooler{
			Id: &clustermetadatapb.ID{Cell: "cell1", Name: "pooler2"},
			ShardKey: &clustermetadatapb.ShardKey{
				Database:   "db1",
				TableGroup: "tg1",
				Shard:      "0",
			},
		},
	})
	poolerStore.Set("pooler3", &multiorchdatapb.PoolerHealthState{
		MultiPooler: &clustermetadatapb.MultiPooler{
			Id: &clustermetadatapb.ID{Cell: "cell2", Name: "pooler3"},
			ShardKey: &clustermetadatapb.ShardKey{
				Database:   "db1",
				TableGroup: "tg1",
				Shard:      "1",
			}, // different shard
		},
	})

	t.Run("finds poolers in shard", func(t *testing.T) {
		shardKey := &clustermetadatapb.ShardKey{Database: "db1", TableGroup: "tg1", Shard: "0"}
		poolers := poolerStore.FindPoolersInShard(shardKey)

		assert.Len(t, poolers, 2)
		names := []string{poolers[0].MultiPooler.Id.Name, poolers[1].MultiPooler.Id.Name}
		assert.Contains(t, names, "pooler1")
		assert.Contains(t, names, "pooler2")
	})

	t.Run("returns empty for non-existent shard", func(t *testing.T) {
		shardKey := &clustermetadatapb.ShardKey{Database: "db1", TableGroup: "tg1", Shard: "999"}
		poolers := poolerStore.FindPoolersInShard(shardKey)

		assert.Empty(t, poolers)
	})

	t.Run("skips nil entries", func(t *testing.T) {
		poolerStore.Set("nil-pooler", nil)
		poolerStore.Set("nil-multipooler", &multiorchdatapb.PoolerHealthState{MultiPooler: nil})

		shardKey := &clustermetadatapb.ShardKey{Database: "db1", TableGroup: "tg1", Shard: "0"}
		poolers := poolerStore.FindPoolersInShard(shardKey)

		// Should still find the 2 valid poolers
		assert.Len(t, poolers, 2)
	})
}

func TestPoolerStore_FindPoolerByID(t *testing.T) {
	poolerStore := NewPoolerStore(nil, slog.Default())

	poolerStore.Set("pooler1", &multiorchdatapb.PoolerHealthState{
		MultiPooler: &clustermetadatapb.MultiPooler{
			Id: &clustermetadatapb.ID{Cell: "cell1", Name: "pooler1"},
		},
	})
	poolerStore.Set("pooler2", &multiorchdatapb.PoolerHealthState{
		MultiPooler: &clustermetadatapb.MultiPooler{
			Id: &clustermetadatapb.ID{Cell: "cell2", Name: "pooler2"},
		},
	})

	t.Run("finds pooler by ID", func(t *testing.T) {
		id := &clustermetadatapb.ID{Cell: "cell1", Name: "pooler1"}
		found, err := poolerStore.FindPoolerByID(id)

		require.NoError(t, err)
		assert.Equal(t, "pooler1", found.MultiPooler.Id.Name)
		assert.Equal(t, "cell1", found.MultiPooler.Id.Cell)
	})

	t.Run("returns error for non-existent pooler", func(t *testing.T) {
		id := &clustermetadatapb.ID{Cell: "cell1", Name: "non-existent"}
		found, err := poolerStore.FindPoolerByID(id)

		assert.Error(t, err)
		assert.Nil(t, found)
		assert.Contains(t, err.Error(), "not found")
	})

	t.Run("matches both cell and name", func(t *testing.T) {
		// Same name, different cell - should not match
		id := &clustermetadatapb.ID{Cell: "cell2", Name: "pooler1"}
		found, err := poolerStore.FindPoolerByID(id)

		assert.Error(t, err)
		assert.Nil(t, found)
	})
}

func TestPoolerStore_FindHealthyPrimary(t *testing.T) {
	ctx := context.Background()

	// withLeadership returns a MultiPooler that publishes CurrentLeadership
	// pointing to leaderID at the given coordinator term — the etcd-side
	// observation that ShardLeader uses to identify the consensus leader.
	withLeadership := func(id *clustermetadatapb.ID, leaderID *clustermetadatapb.ID, term int64) *clustermetadatapb.MultiPooler {
		var obs *clustermetadatapb.LeaderObservation
		if leaderID != nil {
			obs = &clustermetadatapb.LeaderObservation{
				LeaderId:         leaderID,
				LeaderRuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: term},
			}
		}
		return &clustermetadatapb.MultiPooler{
			Id:                id,
			CurrentLeadership: obs,
		}
	}

	t.Run("finds healthy primary", func(t *testing.T) {
		fakeClient := rpcclient.NewFakeClient()
		fakeClient.SetStatusResponse("multipooler-cell1-primary", &multipoolermanagerdatapb.StatusResponse{
			Status: &multipoolermanagerdatapb.Status{PoolerType: clustermetadatapb.PoolerType_PRIMARY},
		})
		poolerStore := NewPoolerStore(fakeClient, slog.Default())

		primaryPoolerID := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "cell1", Name: "primary"}
		replicaPoolerID := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "cell1", Name: "replica"}
		poolers := []*multiorchdatapb.PoolerHealthState{
			{MultiPooler: withLeadership(replicaPoolerID, nil, 0)},
			{MultiPooler: withLeadership(primaryPoolerID, primaryPoolerID, 1)},
		}

		primary, err := poolerStore.FindHealthyPrimary(ctx, poolers)

		require.NoError(t, err)
		assert.Equal(t, "primary", primary.MultiPooler.Id.Name)
	})

	t.Run("returns error when no consensus leader observation", func(t *testing.T) {
		poolerStore := NewPoolerStore(&rpcclient.FakeClient{}, slog.Default())

		// No pooler publishes a LeaderObservation — nothing for ShardLeader to pick.
		poolers := []*multiorchdatapb.PoolerHealthState{
			{MultiPooler: withLeadership(&clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "cell1", Name: "replica1"}, nil, 0)},
			{MultiPooler: withLeadership(&clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "cell1", Name: "replica2"}, nil, 0)},
		}

		primary, err := poolerStore.FindHealthyPrimary(ctx, poolers)

		assert.Error(t, err)
		assert.Nil(t, primary)
		assert.Contains(t, err.Error(), "no consensus leader observed")
	})

	t.Run("returns FAILED_PRECONDITION when consensus leader's postgres is in standby mode", func(t *testing.T) {
		// Demoted-but-not-yet-cleared scenario: pooler still self-claims as leader
		// in etcd, but its postgres has been restarted as standby. The Status
		// RPC will report PoolerType=REPLICA, which is rejected.
		fakeClient := rpcclient.NewFakeClient()
		primaryPoolerID := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "cell1", Name: "primary"}
		fakeClient.SetStatusResponse("multipooler-cell1-primary", &multipoolermanagerdatapb.StatusResponse{
			Status: &multipoolermanagerdatapb.Status{PoolerType: clustermetadatapb.PoolerType_REPLICA},
		})
		poolerStore := NewPoolerStore(fakeClient, slog.Default())

		poolers := []*multiorchdatapb.PoolerHealthState{
			{MultiPooler: withLeadership(primaryPoolerID, primaryPoolerID, 1)},
		}

		primary, err := poolerStore.FindHealthyPrimary(ctx, poolers)

		assert.Error(t, err)
		assert.Nil(t, primary)
		assert.Contains(t, err.Error(), "not PRIMARY")
	})

	t.Run("picks highest-term observation when poolers disagree", func(t *testing.T) {
		// primary1 still claims leadership at term 1, primary2 at term 2 — newer rule wins.
		fakeClient := rpcclient.NewFakeClient()
		primary1ID := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "cell1", Name: "primary1"}
		primary2ID := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "cell2", Name: "primary2"}
		fakeClient.SetStatusResponse("multipooler-cell2-primary2", &multipoolermanagerdatapb.StatusResponse{
			Status: &multipoolermanagerdatapb.Status{PoolerType: clustermetadatapb.PoolerType_PRIMARY},
		})
		poolerStore := NewPoolerStore(fakeClient, slog.Default())

		poolers := []*multiorchdatapb.PoolerHealthState{
			{MultiPooler: withLeadership(primary1ID, primary1ID, 1)},
			{MultiPooler: withLeadership(primary2ID, primary2ID, 2)},
		}

		primary, err := poolerStore.FindHealthyPrimary(ctx, poolers)

		require.NoError(t, err)
		assert.Equal(t, "primary2", primary.MultiPooler.Id.Name)
	})

	t.Run("returns FAILED_PRECONDITION when consensus leader is unreachable", func(t *testing.T) {
		// ShardLeader picks the named leader but its Status RPC fails.
		fakeClient := rpcclient.NewFakeClient()
		primaryPoolerID := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "cell1", Name: "primary"}
		fakeClient.Errors["multipooler-cell1-primary"] = errors.New("connection refused")
		poolerStore := NewPoolerStore(fakeClient, slog.Default())

		poolers := []*multiorchdatapb.PoolerHealthState{
			{MultiPooler: withLeadership(primaryPoolerID, primaryPoolerID, 1)},
		}

		primary, err := poolerStore.FindHealthyPrimary(ctx, poolers)

		assert.Error(t, err)
		assert.Nil(t, primary)
		assert.Contains(t, err.Error(), "unreachable")
	})

	t.Run("uses health-stream observation when etcd CurrentLeadership is stale/empty", func(t *testing.T) {
		// promoted-replica has not yet published its CurrentLeadership to etcd
		// (newly elected), but its health-stream ConsensusStatus already reports
		// it as the leader at the new term. ShardLeader merges both sources and
		// finds the live observation.
		fakeClient := rpcclient.NewFakeClient()
		promotedID := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "cell1", Name: "promoted-replica"}
		staleID := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "cell1", Name: "stale-primary"}
		fakeClient.SetStatusResponse("multipooler-cell1-promoted-replica", &multipoolermanagerdatapb.StatusResponse{
			Status: &multipoolermanagerdatapb.Status{PoolerType: clustermetadatapb.PoolerType_PRIMARY},
		})
		poolerStore := NewPoolerStore(fakeClient, slog.Default())

		poolers := []*multiorchdatapb.PoolerHealthState{
			{
				// stale-primary still publishes itself at the old term in etcd
				MultiPooler: withLeadership(staleID, staleID, 1),
			},
			{
				// promoted-replica's etcd record hasn't refreshed, but its
				// health-stream ConsensusStatus advertises the new rule
				MultiPooler: withLeadership(promotedID, nil, 0),
				ConsensusStatus: &clustermetadatapb.ConsensusStatus{
					ReplicationPrimary: &clustermetadatapb.ReplicationPrimary{
						Rule: &clustermetadatapb.ShardRule{
							RuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: 2},
							LeaderId:   promotedID,
						},
					},
				},
			},
		}

		primary, err := poolerStore.FindHealthyPrimary(ctx, poolers)

		require.NoError(t, err)
		assert.Equal(t, "promoted-replica", primary.MultiPooler.Id.Name)
	})
}

// TestPoolerStore_DoUpdateRange verifies that DoUpdateRange atomically resets fields
// on qualifying poolers while leaving others unchanged — mirroring the
// queuePoolersHealthCheck use case.
func TestPoolerStore_DoUpdateRange(t *testing.T) {
	store := NewPoolerStore(nil, slog.Default())

	// pooler1: IsUpToDate=true — should be reset to false
	store.Set("pooler1", &multiorchdatapb.PoolerHealthState{
		MultiPooler: &clustermetadatapb.MultiPooler{
			Id: &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "pooler1"},
		},
		IsUpToDate: true,
	})
	// pooler2: IsUpToDate=false — should remain false and not be written back
	store.Set("pooler2", &multiorchdatapb.PoolerHealthState{
		MultiPooler: &clustermetadatapb.MultiPooler{
			Id: &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "pooler2"},
		},
		IsUpToDate: false,
	})

	writeCount := 0
	store.DoUpdateRange(func(key string, value *multiorchdatapb.PoolerHealthState) (*multiorchdatapb.PoolerHealthState, bool) {
		if value.IsUpToDate {
			value.IsUpToDate = false
			writeCount++
			return value, true // write and continue
		}
		return nil, true // no write, continue
	})

	// Only pooler1 should have triggered a write-back
	require.Equal(t, 1, writeCount)

	p1, ok := store.Get("pooler1")
	require.True(t, ok)
	require.False(t, p1.IsUpToDate, "pooler1 IsUpToDate should have been reset to false")

	p2, ok := store.Get("pooler2")
	require.True(t, ok)
	require.False(t, p2.IsUpToDate, "pooler2 IsUpToDate should remain false")
}
