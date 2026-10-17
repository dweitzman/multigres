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

// Package store provides multiorch's typed view of the topology pooler
// cache. The cache itself lives in topoclient/poolerwatch and is generic
// over a rider type; this package fixes the rider to *Pooler (which bundles
// the proto health state with the per-pooler stream handle) and supplies
// orch-specific helpers (FindPoolersInShard).
package store

import (
	"github.com/multigres/multigres/go/common/topoclient/poolerwatch"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
)

// PoolerCache is the orch-side type alias for the lifecycle-aware pooler
// cache keyed by *Pooler riders.
type PoolerCache = poolerwatch.PoolerCache[*Pooler]

// FindPoolersInShard returns every pooler the cache holds for the given
// shard. The returned slice is empty if no poolers match.
func FindPoolersInShard(cache *PoolerCache, shardKey *clustermetadatapb.ShardKey) []*Pooler {
	entries := cache.GetByShard(shardKey.GetDatabase(), shardKey.GetTableGroup(), shardKey.GetShard())
	out := make([]*Pooler, 0, len(entries))
	for _, e := range entries {
		out = append(out, e.Rider)
	}
	return out
}
