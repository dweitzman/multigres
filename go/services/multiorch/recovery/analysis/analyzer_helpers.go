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

import (
	commonconsensus "github.com/multigres/multigres/go/common/consensus"
	"github.com/multigres/multigres/go/services/multiorch/internal/snapshot"
	"github.com/multigres/multigres/go/services/multiorch/recovery/types"
)

// Helpers shared by the analyzers. Judgments about a single pooler live on
// snapshot.PoolerHealthState.

// compareLeaderTimeline compares two leader snapshots by rule position. LSN is
// intentionally excluded from the comparison (CompareRulePosition already
// stops at decision-then-proposal): for leaders, the coordinator term must be
// unique per promotion, so equal terms indicate a consensus bug rather than a
// resolvable tie.
func compareLeaderTimeline(a, b *snapshot.PoolerHealthState) int {
	return commonconsensus.CompareRulePosition(
		a.GetConsensusStatus().GetCurrentPosition().GetPosition(),
		b.GetConsensusStatus().GetCurrentPosition().GetPosition(),
	)
}

// analyzeAllPoolers runs fn against each pooler in sa, collecting all problems.
// Both the shard snapshot and the pooler's snapshot are passed so callbacks can
// access shard-level context alongside pooler-specific state.
// Errors are accumulated — the first error encountered is returned alongside any problems collected.
func analyzeAllPoolers(sa *snapshot.Shard, fn func(*snapshot.Shard, *snapshot.PoolerHealthState) (*types.Problem, error)) ([]types.Problem, error) {
	var problems []types.Problem
	var firstErr error
	for _, poolerAnalysis := range sa.Poolers {
		p, err := fn(sa, poolerAnalysis)
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		if p != nil {
			problems = append(problems, *p)
		}
	}
	return problems, firstErr
}
