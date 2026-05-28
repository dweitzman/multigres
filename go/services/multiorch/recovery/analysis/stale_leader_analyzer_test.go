// Copyright 2026 Supabase, Inc.
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

package analysis

// TODO: rewrite StaleLeaderAnalyzer tests for the new model.
//
// The pre-refactor tests built a ShardAnalysis with Leaders=[multiple
// PoolerAnalysis with IsLeader=true at different terms] and
// HighestTermReachableLeader=the chosen one. That shape no longer
// exists — ShardLeader produces a single cluster-authoritative
// observation by construction, and sa.Leader is the matched
// PoolerAnalysis (at most one).
//
// New tests should construct ShardAnalysis with:
//   - sa.LeaderObservation naming the consensus leader at some rule
//   - sa.Analyses containing the cluster leader (IsLeader=true) plus
//     one or more poolers whose SelfLeaderObservation() names
//     themselves (the stale claimants)
// and assert the analyzer emits a ProblemStaleLeader for each
// stale claimant, sorted most-stale-first.
