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

package recovery

import (
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonconsensus "github.com/multigres/multigres/go/common/consensus"
	"github.com/multigres/multigres/go/common/rpcclient"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/services/multiorch/internal/snapshot"
	"github.com/multigres/multigres/go/services/multiorch/store"
)

func leaderInfoTestRule(term int64) *clustermetadatapb.ShardRule {
	return &clustermetadatapb.ShardRule{RuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: term}}
}

func leaderInfoTestAddress(name string) *clustermetadatapb.PoolerAddress {
	return &clustermetadatapb.PoolerAddress{
		Id:           &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: name},
		Host:         "host-" + name,
		PostgresPort: 5432,
	}
}

func TestShouldPropagateLeaderInfo(t *testing.T) {
	now := time.Unix(1700000000, 0)
	fresh := timestamppb.New(now.Add(-10 * time.Second))
	stale := timestamppb.New(now.Add(-2 * time.Minute))

	leaderAddress := leaderInfoTestAddress("leader")
	currentPosition := &clustermetadatapb.RulePosition{Decision: leaderInfoTestRule(5)}
	olderPosition := &clustermetadatapb.RulePosition{Decision: leaderInfoTestRule(3)}

	tests := []struct {
		name              string
		rp                *clustermetadatapb.IntendedPrimary
		lastSeen          *timestamppb.Timestamp
		leaderRewindReady bool
		want              bool
	}{
		{
			name:              "NeverToldAnything_Propagates",
			rp:                nil,
			lastSeen:          fresh,
			leaderRewindReady: false,
			want:              true,
		},
		{
			name:              "StaleHealth_Skipped_EvenIfBehind",
			rp:                nil,
			lastSeen:          stale,
			leaderRewindReady: true,
			want:              false,
		},
		{
			name: "PositionBehind_Propagates",
			rp: &clustermetadatapb.IntendedPrimary{
				Position: olderPosition,
				Primary:  leaderAddress,
			},
			lastSeen:          fresh,
			leaderRewindReady: false,
			want:              true,
		},
		{
			name: "PositionMatches_BothRewindReadyFalse_NoOp",
			rp: &clustermetadatapb.IntendedPrimary{
				Position:    currentPosition,
				Primary:     leaderAddress,
				RewindReady: false,
			},
			lastSeen:          fresh,
			leaderRewindReady: false,
			want:              false,
		},
		{
			name: "PositionMatches_BothRewindReadyTrue_NoOp",
			rp: &clustermetadatapb.IntendedPrimary{
				Position:    currentPosition,
				Primary:     leaderAddress,
				RewindReady: true,
			},
			lastSeen:          fresh,
			leaderRewindReady: true,
			want:              false,
		},
		{
			name: "PositionMatches_FollowerAlreadyTrue_LeaderViewStaleFalse_NeverRegress",
			rp: &clustermetadatapb.IntendedPrimary{
				Position:    currentPosition,
				Primary:     leaderAddress,
				RewindReady: true,
			},
			lastSeen:          fresh,
			leaderRewindReady: false,
			want:              false,
		},
		{
			name: "PositionMatches_FollowerFalse_LeaderTrue_PropagatesGoodNews",
			rp: &clustermetadatapb.IntendedPrimary{
				Position:    currentPosition,
				Primary:     leaderAddress,
				RewindReady: false,
			},
			lastSeen:          fresh,
			leaderRewindReady: true,
			want:              true,
		},
		{
			name: "PositionMatches_DifferentPrimaryContact_Propagates",
			rp: &clustermetadatapb.IntendedPrimary{
				Position:    currentPosition,
				Primary:     leaderInfoTestAddress("stale-leader"),
				RewindReady: true,
			},
			lastSeen:          fresh,
			leaderRewindReady: true,
			want:              true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := shouldPropagateLeaderInfo(tt.rp, tt.lastSeen, now, leaderAddress, tt.leaderRewindReady, commonconsensus.NewRuleTransition(currentPosition))
			assert.Equal(t, tt.want, got)
		})
	}
}

// TestPropagateLeaderInfoToPooler_CachesSuccessfulSetPrimary verifies that a
// successful SetPrimary call optimistically updates the pooler's cached
// IntendedPrimary, so a reconcile tick immediately after doesn't
// needlessly re-send the same SetPrimary before the pooler's own streamed
// health update reports it back (see shouldPropagateLeaderInfo).
func TestReconcileFollowerSlots(t *testing.T) {
	leader := &snapshot.PoolerHealthState{
		Multipooler: &clustermetadatapb.Multipooler{
			Id:   &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "p0"},
			Type: clustermetadatapb.PoolerType_PRIMARY,
		},
		LastSeen: timestamppb.Now(),
	}

	t.Run("calls ReconcileFollowers on the leader when the set is non-empty", func(t *testing.T) {
		fake := rpcclient.NewFakeClient()
		re := &Engine{rpcClient: fake, logger: slog.Default()}
		ids := []*clustermetadatapb.ID{
			{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "s1"},
			{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "s2"},
		}
		re.reconcileFollowerSlots(t.Context(), leader, ids)
		require.Equal(t, []string{"ReconcileFollowers(multipooler-zone1-p0)"}, fake.GetCallLog())
	})

	t.Run("no-op when the eligible set is empty", func(t *testing.T) {
		fake := rpcclient.NewFakeClient()
		re := &Engine{rpcClient: fake, logger: slog.Default()}
		re.reconcileFollowerSlots(t.Context(), leader, nil)
		require.Empty(t, fake.GetCallLog(), "must not call ReconcileFollowers with an empty set")
	})
}

func TestPropagateLeaderInfoToPooler_CachesSuccessfulSetPrimary(t *testing.T) {
	fake := rpcclient.NewFakeClient()
	cache := store.NewTestCache(t)
	re := &Engine{rpcClient: fake, logger: slog.Default(), poolerCache: cache}

	poolerID := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "p1"}
	pooler := store.NewPooler(&snapshot.PoolerHealthState{
		Multipooler: &clustermetadatapb.Multipooler{Id: poolerID},
		LastSeen:    timestamppb.Now(),
	}, nil)
	store.SeedCache(t, cache, pooler)

	leaderAddress := leaderInfoTestAddress("leader")
	position := &clustermetadatapb.RulePosition{Decision: leaderInfoTestRule(5)}

	re.propagateLeaderInfoToPooler(t.Context(), pooler.Health(), time.Now(), leaderAddress, true, commonconsensus.NewRuleTransition(position))

	require.Equal(t, []string{"SetPrimary(multipooler-zone1-p1)"}, fake.GetCallLog(), "SetPrimary must actually have been called")
	got := pooler.Health().GetConsensusStatus().GetIntendedPrimary()
	require.NotNil(t, got)
	assert.True(t, proto.Equal(leaderAddress, got.GetPrimary()))
	assert.True(t, got.GetRewindReady())
	assert.True(t, proto.Equal(position, got.GetPosition()))
}

// TestPropagateLeaderInfoToPooler_NeverRegressesCachedPrimary verifies the
// optimistic cache update never overwrites a fresher view with a stale one —
// e.g. a real streamed health update landed while an older SetPrimary call
// (still in flight from an earlier tick) was completing.
func TestPropagateLeaderInfoToPooler_NeverRegressesCachedPrimary(t *testing.T) {
	fake := rpcclient.NewFakeClient()
	cache := store.NewTestCache(t)
	re := &Engine{rpcClient: fake, logger: slog.Default(), poolerCache: cache}

	poolerID := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "p1"}
	newerPrimary := &clustermetadatapb.IntendedPrimary{
		Position: &clustermetadatapb.RulePosition{Decision: leaderInfoTestRule(10)},
		Primary:  leaderInfoTestAddress("newer-leader"),
	}
	pooler := store.NewPooler(&snapshot.PoolerHealthState{
		Multipooler:     &clustermetadatapb.Multipooler{Id: poolerID},
		ConsensusStatus: &clustermetadatapb.ConsensusStatus{IntendedPrimary: newerPrimary},
		LastSeen:        timestamppb.Now(),
	}, nil)
	store.SeedCache(t, cache, pooler)

	stalePosition := &clustermetadatapb.RulePosition{Decision: leaderInfoTestRule(3)}
	re.propagateLeaderInfoToPooler(t.Context(), pooler.Health(), time.Now(), leaderInfoTestAddress("stale-leader"), false, commonconsensus.NewRuleTransition(stalePosition))

	require.Equal(t, []string{"SetPrimary(multipooler-zone1-p1)"}, fake.GetCallLog(), "SetPrimary must actually have been called with the stale info")
	got := pooler.Health().GetConsensusStatus().GetIntendedPrimary()
	assert.True(t, proto.Equal(newerPrimary, got), "must not regress a fresher cached view with a stale optimistic update")
}

func TestPropagateLeaderInfoForShard(t *testing.T) {
	shardKey := &clustermetadatapb.ShardKey{Database: "db", TableGroup: "tg", Shard: "0"}
	id := func(name string) *clustermetadatapb.ID {
		return &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: name}
	}
	// pooler reports the leader's rule as its own position, so the shard's intended leader is "leader".
	pooler := func(name string) *store.Pooler {
		rule := &clustermetadatapb.RulePosition{Decision: &clustermetadatapb.ShardRule{
			RuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: 5}, LeaderId: id("leader"),
		}}
		return store.NewPooler(&snapshot.PoolerHealthState{
			Multipooler:     &clustermetadatapb.Multipooler{Id: id(name), ShardKey: shardKey},
			ConsensusStatus: &clustermetadatapb.ConsensusStatus{Id: id(name), CurrentPosition: &clustermetadatapb.PoolerPosition{Position: rule}},
			LastSeen:        timestamppb.Now(),
		}, nil)
	}
	engine := func(t *testing.T, names ...string) (*Engine, *rpcclient.FakeClient, *store.PoolerCache) {
		cache := store.NewTestCache(t)
		for _, n := range names {
			store.SeedCache(t, cache, pooler(n))
		}
		fake := rpcclient.NewFakeClient()
		return &Engine{rpcClient: fake, logger: slog.Default(), poolerCache: cache}, fake, cache
	}
	snap := func(t *testing.T, cache *store.PoolerCache) *snapshot.Shard {
		shard, err := store.SnapshotShard(cache, shardKey, store.SnapshotOptions{Now: time.Now()})
		require.NoError(t, err)
		return shard
	}

	t.Run("points followers at the leader and reconciles its follower slots", func(t *testing.T) {
		re, fake, cache := engine(t, "leader", "f1", "f2")
		re.propagateLeaderInfoForShard(t.Context(), snap(t, cache))
		log := fake.GetCallLog()
		assert.Contains(t, log, "ReconcileFollowers(multipooler-zone1-leader)")
		assert.Contains(t, log, "SetPrimary(multipooler-zone1-f1)")
		assert.Contains(t, log, "SetPrimary(multipooler-zone1-f2)")
		assert.NotContains(t, log, "SetPrimary(multipooler-zone1-leader)", "the leader is never pointed at itself")
	})

	t.Run("does nothing when the intended leader is not in the snapshot", func(t *testing.T) {
		re, fake, cache := engine(t, "f1", "f2")
		re.propagateLeaderInfoForShard(t.Context(), snap(t, cache))
		assert.Empty(t, fake.GetCallLog())
	})
}

// A follower evicted from the cache while SetPrimary was in flight still gets its
// call, and the missing cache entry is skipped rather than dereferenced.
func TestPropagateLeaderInfoToPooler_FollowerEvictedFromCache(t *testing.T) {
	fake := rpcclient.NewFakeClient()
	re := &Engine{rpcClient: fake, logger: slog.Default(), poolerCache: store.NewTestCache(t)}
	follower := &snapshot.PoolerHealthState{
		Multipooler: &clustermetadatapb.Multipooler{Id: &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "gone"}},
		LastSeen:    timestamppb.Now(),
	}

	re.propagateLeaderInfoToPooler(t.Context(), follower, time.Now(), leaderInfoTestAddress("leader"), true,
		commonconsensus.NewRuleTransition(&clustermetadatapb.RulePosition{Decision: leaderInfoTestRule(5)}))

	require.Equal(t, []string{"SetPrimary(multipooler-zone1-gone)"}, fake.GetCallLog())
}
