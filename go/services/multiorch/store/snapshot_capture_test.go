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

package store

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/durationpb"

	commonconsensus "github.com/multigres/multigres/go/common/consensus"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
	"github.com/multigres/multigres/go/services/multiorch/internal/snapshot"
)

// testCapture freezes shards from a test cache the way the engine does.
type testCapture struct {
	cache *PoolerCache
	now   time.Time
}

func newTestCapture(cache *PoolerCache) *testCapture {
	return &testCapture{cache: cache, now: time.Now()}
}

func (c *testCapture) all() []*snapshot.Shard {
	return SnapshotShards(c.cache, SnapshotOptions{Now: c.now})
}

func (c *testCapture) shard(key *clustermetadatapb.ShardKey) (*snapshot.Shard, error) {
	return SnapshotShard(c.cache, key, SnapshotOptions{Now: c.now})
}

func flattenPoolers(shards []*snapshot.Shard) []*snapshot.PoolerHealthState {
	var poolers []*snapshot.PoolerHealthState
	for _, sa := range shards {
		poolers = append(poolers, sa.Poolers...)
	}
	return poolers
}

// primaryConsensusStatus builds a ConsensusStatus that names id as the leader
// in its current rule with the given coordinator term. This is the minimal
// fixture required for SelfConsensusRole to report ConsensusRoleLeader for a given pooler.
func primaryConsensusStatus(id *clustermetadatapb.ID, term int64) *clustermetadatapb.ConsensusStatus {
	return &clustermetadatapb.ConsensusStatus{
		Id: id,
		CurrentPosition: &clustermetadatapb.PoolerPosition{
			Position: &clustermetadatapb.RulePosition{
				Decision: &clustermetadatapb.ShardRule{
					RuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: term},
					LeaderId:   id,
				},
			},
		},
	}
}

func TestSnapshotShards_EmptyStore(t *testing.T) {
	generator := newTestCapture(NewTestCache(t))

	analyses := flattenPoolers(generator.all())

	assert.Empty(t, analyses, "should return empty slice for empty store")
}

func TestSnapshotShards_SinglePrimary(t *testing.T) {
	ps := NewTestCache(t)

	// Add a single primary pooler
	primaryID := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      "cell1",
		Name:      "primary-1",
	}

	primary := &snapshot.PoolerHealthState{
		Multipooler: &clustermetadatapb.Multipooler{
			Id: primaryID,
			ShardKey: &clustermetadatapb.ShardKey{
				Database:   "testdb",
				TableGroup: "testtg",
				Shard:      "0",
			},
			Type: clustermetadatapb.PoolerType_PRIMARY,
		},
		LastSeen:        time.Now(),
		ConsensusStatus: primaryConsensusStatus(primaryID, 1),
		Status: &multipoolermanagerdatapb.Status{
			PoolerType: clustermetadatapb.PoolerType_PRIMARY,
			PrimaryStatus: &multipoolermanagerdatapb.PrimaryStatus{
				Lsn:   "0/1234567",
				Ready: true,
			},
		},
	}
	SeedCache(t, ps, NewPooler(primary, nil))

	generator := newTestCapture(ps)
	analyses := flattenPoolers(generator.all())

	require.Len(t, analyses, 1, "should generate one analysis")

	analysis := analyses[0]
	assert.Equal(t, "testdb", analysis.GetMultipooler().GetShardKey().GetDatabase())
	assert.Equal(t, "testtg", analysis.GetMultipooler().GetShardKey().GetTableGroup())
	assert.Equal(t, "0", analysis.GetMultipooler().GetShardKey().GetShard())
	assert.True(t, commonconsensus.SelfConsensusRole(analysis.GetConsensusStatus()) == commonconsensus.ConsensusRoleLeader)
}

func TestSnapshotShard_IsFrozenAtCapture(t *testing.T) {
	ps := NewTestCache(t)
	shardKey := &clustermetadatapb.ShardKey{Database: "testdb", TableGroup: "testtg", Shard: "0"}
	rider := NewPooler(&snapshot.PoolerHealthState{
		Multipooler: &clustermetadatapb.Multipooler{
			Id:       &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "cell1", Name: "pooler-1"},
			ShardKey: shardKey,
		},
		Status: &multipoolermanagerdatapb.Status{PostgresReady: true},
	}, nil)
	SeedCache(t, ps, rider)

	sa, err := newTestCapture(ps).shard(shardKey)
	require.NoError(t, err)

	// A later health update must not show through a snapshot already taken.
	rider.Mutate(func(h *snapshot.PoolerHealthState) { h.Status.PostgresReady = false })

	require.Len(t, sa.Poolers, 1)
	assert.True(t, sa.Poolers[0].GetStatus().GetPostgresReady())
}

func TestSnapshotShards_PrimaryWithReplicas(t *testing.T) {
	ps := NewTestCache(t)

	primaryID := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      "cell1",
		Name:      "primary-1",
	}

	replica1ID := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      "cell1",
		Name:      "replica-1",
	}

	replica2ID := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      "cell1",
		Name:      "replica-2",
	}

	// Add primary
	primary := &snapshot.PoolerHealthState{
		Multipooler: &clustermetadatapb.Multipooler{
			Id: primaryID,
			ShardKey: &clustermetadatapb.ShardKey{
				Database:   "testdb",
				TableGroup: "testtg",
				Shard:      "0",
			},
			Type:     clustermetadatapb.PoolerType_PRIMARY,
			Hostname: "primary.example.com",
		},
		LastSeen:        time.Now(),
		ConsensusStatus: primaryConsensusStatus(primaryID, 1),
		Status: &multipoolermanagerdatapb.Status{
			PoolerType: clustermetadatapb.PoolerType_PRIMARY,
			PrimaryStatus: &multipoolermanagerdatapb.PrimaryStatus{
				Lsn:                "0/1234567",
				Ready:              true,
				ConnectedFollowers: []*clustermetadatapb.ID{replica1ID, replica2ID},
			},
		},
	}
	SeedCache(t, ps, NewPooler(primary, nil))

	// Add replica 1 (replicating)
	replica1 := &snapshot.PoolerHealthState{
		Multipooler: &clustermetadatapb.Multipooler{
			Id: replica1ID,
			ShardKey: &clustermetadatapb.ShardKey{
				Database:   "testdb",
				TableGroup: "testtg",
				Shard:      "0",
			},
			Type: clustermetadatapb.PoolerType_REPLICA,
		},
		LastSeen: time.Now(),
		Status: &multipoolermanagerdatapb.Status{
			PoolerType: clustermetadatapb.PoolerType_REPLICA,
			ReplicationStatus: &multipoolermanagerdatapb.StandbyReplicationStatus{
				IsWalReplayPaused: false,
				Lag:               durationpb.New(100 * time.Millisecond), // 100ms lag
			},
		},
	}
	SeedCache(t, ps, NewPooler(replica1, nil))

	// Add replica 2 (lagging)
	replica2 := &snapshot.PoolerHealthState{
		Multipooler: &clustermetadatapb.Multipooler{
			Id: replica2ID,
			ShardKey: &clustermetadatapb.ShardKey{
				Database:   "testdb",
				TableGroup: "testtg",
				Shard:      "0",
			},
			Type: clustermetadatapb.PoolerType_REPLICA,
		},
		LastSeen: time.Now(),
		Status: &multipoolermanagerdatapb.Status{
			PoolerType: clustermetadatapb.PoolerType_REPLICA,
			ReplicationStatus: &multipoolermanagerdatapb.StandbyReplicationStatus{
				IsWalReplayPaused: false,
				Lag:               durationpb.New(15 * time.Second), // 15s lag (> 10s threshold)
			},
		},
	}
	SeedCache(t, ps, NewPooler(replica2, nil))

	generator := newTestCapture(ps)
	analyses := flattenPoolers(generator.all())

	require.Len(t, analyses, 3, "should generate three analyses")

	// Find the primary analysis
	var primaryAnalysis *snapshot.PoolerHealthState
	for _, a := range analyses {
		if commonconsensus.SelfConsensusRole(a.GetConsensusStatus()) == commonconsensus.ConsensusRoleLeader {
			primaryAnalysis = a
			break
		}
	}

	require.NotNil(t, primaryAnalysis, "should find primary analysis")
	assert.True(t, commonconsensus.SelfConsensusRole(primaryAnalysis.GetConsensusStatus()) == commonconsensus.ConsensusRoleLeader)
}

func TestSnapshotShards_Replica(t *testing.T) {
	ps := NewTestCache(t)

	primaryID := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      "cell1",
		Name:      "primary-1",
	}

	replicaID := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      "cell1",
		Name:      "replica-1",
	}

	// Add primary
	primary := &snapshot.PoolerHealthState{
		Multipooler: &clustermetadatapb.Multipooler{
			Id: primaryID,
			ShardKey: &clustermetadatapb.ShardKey{
				Database:   "testdb",
				TableGroup: "testtg",
				Shard:      "0",
			},
			Type: clustermetadatapb.PoolerType_PRIMARY,
		},
		LastSeen:        time.Now(),
		ConsensusStatus: primaryConsensusStatus(primaryID, 1),
		Status: &multipoolermanagerdatapb.Status{
			PoolerType:    clustermetadatapb.PoolerType_PRIMARY,
			PostgresReady: true,
		},
	}
	SeedCache(t, ps, NewPooler(primary, nil))

	// Add replica
	replica := &snapshot.PoolerHealthState{
		Multipooler: &clustermetadatapb.Multipooler{
			Id: replicaID,
			ShardKey: &clustermetadatapb.ShardKey{
				Database:   "testdb",
				TableGroup: "testtg",
				Shard:      "0",
			},
			Type: clustermetadatapb.PoolerType_REPLICA,
		},
		LastSeen: time.Now(),
		Status: &multipoolermanagerdatapb.Status{
			PoolerType: clustermetadatapb.PoolerType_REPLICA,
			ReplicationStatus: &multipoolermanagerdatapb.StandbyReplicationStatus{
				IsWalReplayPaused: false,
				Lag:               durationpb.New(500 * time.Millisecond),
				LastReplayLsn:     "0/1234567",
			},
		},
	}
	SeedCache(t, ps, NewPooler(replica, nil))

	generator := newTestCapture(ps)
	shards := generator.all()

	require.Len(t, shards, 1, "should snapshot one shard")
	sa := shards[0]
	require.Len(t, sa.Poolers, 2, "should generate two pooler analyses")

	// Exactly one of the two poolers claims leadership.
	leaders := 0
	for _, p := range sa.Poolers {
		if commonconsensus.SelfConsensusRole(p.GetConsensusStatus()) == commonconsensus.ConsensusRoleLeader {
			leaders++
		}
	}
	assert.Equal(t, 1, leaders)

	// Primary health is now a shard-level field
	assert.NotNil(t, sa.HighestIntendedRule().PossiblyUndecidedRule().GetLeaderId(), "should have topology primary ID populated")
}

func TestSnapshotShards_MultipleTableGroups(t *testing.T) {
	ps := NewTestCache(t)

	// Add poolers from two different table groups
	tg1Primary := &snapshot.PoolerHealthState{
		Multipooler: &clustermetadatapb.Multipooler{
			Id: &clustermetadatapb.ID{
				Component: clustermetadatapb.ID_MULTIPOOLER,
				Cell:      "cell1",
				Name:      "tg1-primary",
			},
			ShardKey: &clustermetadatapb.ShardKey{
				Database:   "testdb",
				TableGroup: "tg1",
				Shard:      "0",
			},
			Type: clustermetadatapb.PoolerType_PRIMARY,
		},
		LastSeen: time.Now(),
		Status: &multipoolermanagerdatapb.Status{
			PoolerType: clustermetadatapb.PoolerType_PRIMARY,
		},
	}
	SeedCache(t, ps, NewPooler(tg1Primary, nil))

	tg2Primary := &snapshot.PoolerHealthState{
		Multipooler: &clustermetadatapb.Multipooler{
			Id: &clustermetadatapb.ID{
				Component: clustermetadatapb.ID_MULTIPOOLER,
				Cell:      "cell1",
				Name:      "tg2-primary",
			},
			ShardKey: &clustermetadatapb.ShardKey{
				Database:   "testdb",
				TableGroup: "tg2",
				Shard:      "0",
			},
			Type: clustermetadatapb.PoolerType_PRIMARY,
		},
		LastSeen: time.Now(),
		Status: &multipoolermanagerdatapb.Status{
			PoolerType: clustermetadatapb.PoolerType_PRIMARY,
		},
	}
	SeedCache(t, ps, NewPooler(tg2Primary, nil))

	generator := newTestCapture(ps)
	analyses := flattenPoolers(generator.all())

	require.Len(t, analyses, 2, "should generate two analyses")

	// Verify both table groups are present
	tableGroups := make(map[string]bool)
	for _, a := range analyses {
		tableGroups[a.GetMultipooler().GetShardKey().GetTableGroup()] = true
	}

	assert.True(t, tableGroups["tg1"])
	assert.True(t, tableGroups["tg2"])
}

// Task 6: Test for skipping nil entries
func TestSnapshotShards_SkipsNilEntries(t *testing.T) {
	ps := NewTestCache(t)

	// Add a nil entry
	SeedCache(t, ps, nil)

	// Add a valid pooler
	SeedCache(t, ps, NewPooler(&snapshot.PoolerHealthState{
		Multipooler: &clustermetadatapb.Multipooler{
			Id: &clustermetadatapb.ID{
				Component: clustermetadatapb.ID_MULTIPOOLER,
				Cell:      "cell1",
				Name:      "valid",
			},
			ShardKey: &clustermetadatapb.ShardKey{
				Database:   "db1",
				TableGroup: "tg1",
				Shard:      "shard1",
			},
		},
		Status: &multipoolermanagerdatapb.Status{
			PoolerType: clustermetadatapb.PoolerType_REPLICA,
		},
	}, nil))

	gen := newTestCapture(ps)
	analyses := flattenPoolers(gen.all())

	// Should only generate one analysis for the valid pooler, skipping the nil entry
	assert.Len(t, analyses, 1)
	assert.Equal(t, "db1", analyses[0].GetMultipooler().GetShardKey().GetDatabase())
}

func TestSnapshotShard_NoRuleMeansNoLeader(t *testing.T) {
	ps := NewTestCache(t)

	SeedCache(t, ps, NewPooler(&snapshot.PoolerHealthState{
		Multipooler: &clustermetadatapb.Multipooler{
			Id: &clustermetadatapb.ID{
				Component: clustermetadatapb.ID_MULTIPOOLER,
				Cell:      "cell1",
				Name:      "replica",
			},
			ShardKey: &clustermetadatapb.ShardKey{
				Database:   "db1",
				TableGroup: "tg1",
				Shard:      "shard1",
			},
		},
		Status: &multipoolermanagerdatapb.Status{
			PoolerType: clustermetadatapb.PoolerType_REPLICA,
			ReplicationStatus: &multipoolermanagerdatapb.StandbyReplicationStatus{
				LastReplayLsn: "0/1234",
			},
		},
	}, nil))

	gen := newTestCapture(ps)
	sa, err := gen.shard(&clustermetadatapb.ShardKey{Database: "db1", TableGroup: "tg1", Shard: "shard1"})
	require.NoError(t, err)

	// When no primary exists in the shard, topology primary fields should be nil.
	assert.Nil(t, sa.HighestIntendedRule().PossiblyUndecidedRule().GetLeaderId())
}

// TestSnapshotShard_StaleLeaderSupersededViaFollowerRule is a regression
// test for the post-failover window where the newly elected leader has not yet
// reported a direct health update. A stale leader still self-claims leadership
// at the old term, while a follower already replicates from the new leader at a
// higher term. Leader identity must come from who the poolers say the leader is
// (the rule's LeaderId, including via the intended primary), not from which
// pooler claims leadership for itself — otherwise the stale leader would be
// mistaken for the primary and failover suppressed.
func TestSnapshotShard_StaleLeaderSupersededViaFollowerRule(t *testing.T) {
	shardKey := &clustermetadatapb.ShardKey{Database: "db1", TableGroup: "tg1", Shard: "shard1"}

	staleLeaderID := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "cell1", Name: "stale-leader"}
	newLeaderID := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "cell1", Name: "new-leader"}
	followerID := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "cell1", Name: "follower"}

	ps := NewTestCache(t)
	// Stale leader: still self-claims leadership at the old term (term 5) and is
	// reachable and ready. Under the old self-claim-only logic this would be
	// picked as the leader.
	SeedCache(t, ps, NewPooler(&snapshot.PoolerHealthState{
		Multipooler:     &clustermetadatapb.Multipooler{Id: staleLeaderID, ShardKey: shardKey, Type: clustermetadatapb.PoolerType_PRIMARY},
		ConsensusStatus: primaryConsensusStatus(staleLeaderID, 5),
		Status:          &multipoolermanagerdatapb.Status{PoolerType: clustermetadatapb.PoolerType_PRIMARY, PostgresReady: true, PostgresRunning: true},
	}, nil))
	// Follower: already replicating from the new leader at the higher term (6).
	// The new leader itself has not reported health yet (absent from the store).
	SeedCache(t, ps, NewPooler(&snapshot.PoolerHealthState{
		Multipooler: &clustermetadatapb.Multipooler{Id: followerID, ShardKey: shardKey, Type: clustermetadatapb.PoolerType_REPLICA},
		ConsensusStatus: &clustermetadatapb.ConsensusStatus{
			Id: followerID,
			IntendedPrimary: &clustermetadatapb.IntendedPrimary{
				Position: &clustermetadatapb.RulePosition{
					Decision: &clustermetadatapb.ShardRule{
						RuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: 6},
						LeaderId:   newLeaderID,
					},
				},
			},
		},
	}, nil))

	gen := newTestCapture(ps)
	sa, err := gen.shard(shardKey)
	require.NoError(t, err)

	require.NotNil(t, sa.HighestIntendedRule().PossiblyUndecidedRule().GetLeaderId())
	assert.Equal(t, "new-leader", sa.HighestIntendedRule().PossiblyUndecidedRule().GetLeaderId().Name,
		"the leader named by the follower's higher-term rule must win over the stale self-claiming leader")
	assert.Equal(t, int64(6), sa.HighestIntendedRule().PossiblyUndecidedRule().GetRuleNumber().GetCoordinatorTerm())
	assert.Nil(t, sa.IntendedLeaderHealth(), "the new leader has no health state yet, so Leader is nil")
}

func TestDetectOtherPrimary(t *testing.T) {
	shardKey := &clustermetadatapb.ShardKey{Database: "testdb", TableGroup: "default", Shard: "0"}

	// leaderName returns the shard leader's name and rule term from the generated
	// analysis. Leadership is the highest intended consensus rule, regardless of
	// reachability.
	leaderOf := func(sa *snapshot.Shard) (string, int64) {
		return sa.HighestIntendedRule().PossiblyUndecidedRule().GetLeaderId().GetName(), sa.HighestIntendedRule().PossiblyUndecidedRule().GetRuleNumber().GetCoordinatorTerm()
	}

	t.Run("highest-rule primary wins among two", func(t *testing.T) {
		store := setupMultiplePrimariesStore(t, []primaryConfig{
			{id: "primary-1", primaryTerm: 5, consensusTerm: 10},
			{id: "primary-2", primaryTerm: 6, consensusTerm: 11},
		})
		sa, err := newTestCapture(store).shard(shardKey)
		require.NoError(t, err)
		name, term := leaderOf(sa)
		assert.Equal(t, "primary-2", name)
		assert.Equal(t, int64(6), term)
	})

	t.Run("highest-rule primary wins among many (rule, not consensus term)", func(t *testing.T) {
		store := setupMultiplePrimariesStore(t, []primaryConfig{
			{id: "primary-1", primaryTerm: 5, consensusTerm: 11},
			{id: "primary-2", primaryTerm: 4, consensusTerm: 10},
			{id: "primary-3", primaryTerm: 6, consensusTerm: 9},
		})
		sa, err := newTestCapture(store).shard(shardKey)
		require.NoError(t, err)
		// primary-3 has the highest rule term (6) even though primary-1 has the
		// highest consensus term (11) — ranking is by rule, not consensus term.
		name, term := leaderOf(sa)
		assert.Equal(t, "primary-3", name)
		assert.Equal(t, int64(6), term)
	})

	t.Run("single primary is the leader", func(t *testing.T) {
		store := setupMultiplePrimariesStore(t, []primaryConfig{
			{id: "primary-1", primaryTerm: 5, consensusTerm: 10},
		})
		sa, err := newTestCapture(store).shard(shardKey)
		require.NoError(t, err)
		name, term := leaderOf(sa)
		assert.Equal(t, "primary-1", name)
		assert.Equal(t, int64(5), term)
	})

	t.Run("highest-rule leader wins even when unreachable", func(t *testing.T) {
		// The highest-rule leader (primary-2) is unreachable; we must NOT fall back
		// to the lower-rule reachable primary-1. The leader is still primary-2, but
		// its health is absent/unreachable.
		store := setupMultiplePrimariesStoreWithReachability(t, []primaryConfigWithReachability{
			{primaryConfig: primaryConfig{id: "primary-1", primaryTerm: 5, consensusTerm: 10}, reachable: true},
			{primaryConfig: primaryConfig{id: "primary-2", primaryTerm: 6, consensusTerm: 11}, reachable: false},
		})
		sa, err := newTestCapture(store).shard(shardKey)
		require.NoError(t, err)
		name, term := leaderOf(sa)
		assert.Equal(t, "primary-2", name)
		assert.Equal(t, int64(6), term)
	})
}

// Helper types and functions for multiple primaries tests

type primaryConfig struct {
	id            string
	primaryTerm   int64
	consensusTerm int64
}

type primaryConfigWithReachability struct {
	primaryConfig
	reachable bool
}

func setupMultiplePrimariesStore(t *testing.T, primaries []primaryConfig) *PoolerCache {
	configs := make([]primaryConfigWithReachability, len(primaries))
	for i, p := range primaries {
		configs[i] = primaryConfigWithReachability{
			primaryConfig: p,
			reachable:     true,
		}
	}
	return setupMultiplePrimariesStoreWithReachability(t, configs)
}

func setupMultiplePrimariesStoreWithReachability(t *testing.T, primaries []primaryConfigWithReachability) *PoolerCache {
	ps := NewTestCache(t)

	for _, p := range primaries {
		id := &clustermetadatapb.ID{
			Component: clustermetadatapb.ID_MULTIPOOLER,
			Cell:      "cell1",
			Name:      p.id,
		}
		poolerState := &snapshot.PoolerHealthState{
			Multipooler: &clustermetadatapb.Multipooler{
				Id: id,
				ShardKey: &clustermetadatapb.ShardKey{
					Database:   "testdb",
					TableGroup: "default",
					Shard:      "0",
				},
				Type:     clustermetadatapb.PoolerType_PRIMARY,
				Hostname: "localhost",
			},
			StreamConnected: p.reachable,
			ConsensusStatus: &clustermetadatapb.ConsensusStatus{
				Id:             id,
				TermRevocation: &clustermetadatapb.TermRevocation{RevokedBelowTerm: p.consensusTerm},
				CurrentPosition: &clustermetadatapb.PoolerPosition{
					Position: &clustermetadatapb.RulePosition{Decision: &clustermetadatapb.ShardRule{
						RuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: p.primaryTerm},
						LeaderId:   id,
					}},
				},
			},
			Status: &multipoolermanagerdatapb.Status{
				PoolerType: clustermetadatapb.PoolerType_PRIMARY,
			},
		}
		SeedCache(t, ps, NewPooler(poolerState, nil))
	}

	return ps
}

func TestSnapshotShard_ReturnsAllPoolersInShard(t *testing.T) {
	ps := NewTestCache(t)

	SeedCache(t, ps, NewPooler(&snapshot.PoolerHealthState{
		Multipooler: &clustermetadatapb.Multipooler{
			Id: &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "c1", Name: "primary"},
			ShardKey: &clustermetadatapb.ShardKey{
				Database:   "db",
				TableGroup: "tg",
				Shard:      "0",
			},
			Type: clustermetadatapb.PoolerType_PRIMARY,
		},
		Status: &multipoolermanagerdatapb.Status{
			PoolerType: clustermetadatapb.PoolerType_PRIMARY,
		},
	}, nil))
	SeedCache(t, ps, NewPooler(&snapshot.PoolerHealthState{
		Multipooler: &clustermetadatapb.Multipooler{
			Id: &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "c1", Name: "replica"},
			ShardKey: &clustermetadatapb.ShardKey{
				Database:   "db",
				TableGroup: "tg",
				Shard:      "0",
			},
			Type: clustermetadatapb.PoolerType_REPLICA,
		},
		Status: &multipoolermanagerdatapb.Status{
			PoolerType: clustermetadatapb.PoolerType_REPLICA,
		},
	}, nil))

	gen := newTestCapture(ps)
	sa, err := gen.shard(&clustermetadatapb.ShardKey{Database: "db", TableGroup: "tg", Shard: "0"})
	require.NoError(t, err)
	assert.Len(t, sa.Poolers, 2)
}
