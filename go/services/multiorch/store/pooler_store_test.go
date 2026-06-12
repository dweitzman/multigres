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

// consensusLeaderPooler builds a pooler health state whose consensus status
// names leaderName as the shard leader at the given rule term. FindHealthyPrimary
// derives the leader from consensus, not the topology Type label, so tests drive
// it through the rule a pooler is positioned at.
func consensusLeaderPooler(name, leaderName string, ruleTerm int64) *multiorchdatapb.PoolerHealthState {
	return &multiorchdatapb.PoolerHealthState{
		MultiPooler: &clustermetadatapb.MultiPooler{
			Id: &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "cell1", Name: name},
		},
		ConsensusStatus: &clustermetadatapb.ConsensusStatus{
			CurrentPosition: &clustermetadatapb.PoolerPosition{
				Rule: &clustermetadatapb.ShardRule{
					RuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: ruleTerm},
					LeaderId:   &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "cell1", Name: leaderName},
				},
			},
		},
	}
}

// leaderStatusResp builds a live Status RPC response whose consensus status
// names the pooler itself as the leader — what FindHealthyPrimary verifies.
func leaderStatusResp(name string) *multipoolermanagerdatapb.StatusResponse {
	id := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "cell1", Name: name}
	return &multipoolermanagerdatapb.StatusResponse{
		ConsensusStatus: &clustermetadatapb.ConsensusStatus{
			Id: id,
			CurrentPosition: &clustermetadatapb.PoolerPosition{
				Rule: &clustermetadatapb.ShardRule{
					RuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: 1},
					LeaderId:   id,
				},
			},
		},
	}
}

func TestPoolerStore_FindHealthyPrimary(t *testing.T) {
	ctx := context.Background()

	t.Run("returns the consensus leader when it is running as primary", func(t *testing.T) {
		fakeClient := rpcclient.NewFakeClient()
		fakeClient.SetStatusResponse("multipooler-cell1-primary", leaderStatusResp("primary"))
		poolerStore := NewPoolerStore(fakeClient, slog.Default())

		// Both poolers' consensus names "primary" as the leader at rule 1.
		poolers := []*multiorchdatapb.PoolerHealthState{
			consensusLeaderPooler("replica", "primary", 1),
			consensusLeaderPooler("primary", "primary", 1),
		}

		primary, err := poolerStore.FindHealthyPrimary(ctx, poolers)

		require.NoError(t, err)
		assert.Equal(t, "primary", primary.MultiPooler.Id.Name)
	})

	t.Run("returns error when no consensus leader is known", func(t *testing.T) {
		poolerStore := NewPoolerStore(&rpcclient.FakeClient{}, slog.Default())

		// No consensus status on any pooler — nothing names a leader.
		poolers := []*multiorchdatapb.PoolerHealthState{
			{MultiPooler: &clustermetadatapb.MultiPooler{Id: &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "cell1", Name: "replica1"}}},
			{MultiPooler: &clustermetadatapb.MultiPooler{Id: &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "cell1", Name: "replica2"}}},
		}

		primary, err := poolerStore.FindHealthyPrimary(ctx, poolers)

		assert.Error(t, err)
		assert.Nil(t, primary)
		assert.Contains(t, err.Error(), "no consensus leader known")
	})

	t.Run("returns error when the consensus leader is not running as primary", func(t *testing.T) {
		fakeClient := rpcclient.NewFakeClient()
		fakeClient.Errors["multipooler-cell1-primary"] = errors.New("operation not allowed: the PostgreSQL instance is in standby mode")
		poolerStore := NewPoolerStore(fakeClient, slog.Default())

		poolers := []*multiorchdatapb.PoolerHealthState{
			consensusLeaderPooler("primary", "primary", 1),
		}

		primary, err := poolerStore.FindHealthyPrimary(ctx, poolers)

		assert.Error(t, err)
		assert.Nil(t, primary)
		assert.Contains(t, err.Error(), "consensus leader unreachable")
	})

	t.Run("returns error when the consensus leader is not in the pooler set", func(t *testing.T) {
		poolerStore := NewPoolerStore(rpcclient.NewFakeClient(), slog.Default())

		// The only pooler reports a leader ("ghost") that is not present.
		poolers := []*multiorchdatapb.PoolerHealthState{
			consensusLeaderPooler("replica", "ghost", 1),
		}

		primary, err := poolerStore.FindHealthyPrimary(ctx, poolers)

		assert.Error(t, err)
		assert.Nil(t, primary)
		assert.Contains(t, err.Error(), "not in the pooler set")
	})

	t.Run("resolves split-brain by choosing the highest-rule leader", func(t *testing.T) {
		fakeClient := rpcclient.NewFakeClient()
		fakeClient.SetStatusResponse("multipooler-cell1-primary2", leaderStatusResp("primary2"))
		poolerStore := NewPoolerStore(fakeClient, slog.Default())

		// primary1 still believes it leads at rule 1; primary2 leads at the
		// higher rule 2. Consensus resolves to primary2 — no error.
		poolers := []*multiorchdatapb.PoolerHealthState{
			consensusLeaderPooler("primary1", "primary1", 1),
			consensusLeaderPooler("primary2", "primary2", 2),
		}

		primary, err := poolerStore.FindHealthyPrimary(ctx, poolers)

		require.NoError(t, err)
		assert.Equal(t, "primary2", primary.MultiPooler.Id.Name)
	})

	t.Run("derives the leader from consensus regardless of stale topology", func(t *testing.T) {
		// Post-failover: promoted-replica leads at the higher rule 2; the
		// demoted old leader is still at rule 1. FindHealthyPrimary follows
		// consensus to promoted-replica, never the topology Type label.
		fakeClient := rpcclient.NewFakeClient()
		fakeClient.SetStatusResponse("multipooler-cell1-promoted-replica", leaderStatusResp("promoted-replica"))
		poolerStore := NewPoolerStore(fakeClient, slog.Default())

		poolers := []*multiorchdatapb.PoolerHealthState{
			consensusLeaderPooler("old-leader", "old-leader", 1),
			consensusLeaderPooler("promoted-replica", "promoted-replica", 2),
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
