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

package store

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multiorchdatapb "github.com/multigres/multigres/go/pb/multiorchdata"
)

// shard returns the ShardKey used across this file's fixtures.
func shard() *clustermetadatapb.ShardKey {
	return &clustermetadatapb.ShardKey{Database: "db", TableGroup: "tg", Shard: "0"}
}

// poolerID builds a stable component ID for a fixture pooler.
func poolerID(cell, name string) *clustermetadatapb.ID {
	return &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: cell, Name: name}
}

func TestFindPoolersInShard_EmptyCache(t *testing.T) {
	cache := NewTestCache(t)
	defer cache.Shutdown()
	assert.Empty(t, FindPoolersInShard(cache, shard()))
}

func TestFindPoolersInShard_OnlyShardScoped(t *testing.T) {
	cache := NewTestCache(t)
	defer cache.Shutdown()

	inShard := NewPooler(&multiorchdatapb.PoolerHealthState{
		Multipooler: &clustermetadatapb.Multipooler{Id: poolerID("zone1", "in"), ShardKey: shard()},
	}, nil)
	otherShard := NewPooler(&multiorchdatapb.PoolerHealthState{
		Multipooler: &clustermetadatapb.Multipooler{
			Id:       poolerID("zone1", "other"),
			ShardKey: &clustermetadatapb.ShardKey{Database: "db", TableGroup: "tg", Shard: "other"},
		},
	}, nil)
	SeedCache(t, cache, inShard)
	SeedCache(t, cache, otherShard)

	poolers := FindPoolersInShard(cache, shard())
	require.Len(t, poolers, 1)
	assert.Equal(t, "in", poolers[0].Health().Multipooler.Id.Name)
}
