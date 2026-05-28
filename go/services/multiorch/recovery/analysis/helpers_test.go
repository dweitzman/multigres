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

package analysis

// flattenShardAnalyses collects all ReplicationAnalysis entries across all ShardAnalysis groups.
// Used in tests to assert on individual analyses without caring about shard grouping.
func flattenShardAnalyses(shards []*ShardAnalysis) []*PoolerAnalysis {
	var analyses []*PoolerAnalysis
	for _, sa := range shards {
		analyses = append(analyses, sa.Analyses...)
	}
	return analyses
}

// findPoolerByName returns the PoolerAnalysis with the given name from a ShardAnalysis,
// or nil if not found. Used in tests that need to assert on a specific pooler's analysis
// within a shard.
func findPoolerByName(sa *ShardAnalysis, name string) *PoolerAnalysis {
	for _, pa := range sa.Analyses {
		if pa.PoolerID.Name == name {
			return pa
		}
	}
	return nil
}
