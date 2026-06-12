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

package analysis

import (
	"errors"
	"fmt"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/multigres/multigres/go/services/multiorch/recovery/types"
)

// SetPrimaryAnalyzer detects poolers that are not following the current leader's
// rule and therefore need a SetPrimary RPC to learn who the leader is.
//
// This covers two cases that reduce to the same fix:
//   - an orphan replica whose ReplicationPrimary is missing, names a different
//     leader, or names the leader at an older rule; and
//   - a stale leader: a pooler that still believes it is the leader but is not
//     the highest-term reachable leader. SetPrimary demotes it by pointing it at
//     the real leader and restarting it as a standby.
//
// It does NOT cover "replication appears stuck despite knowing the right leader"
// (e.g. timeline divergence). SetPrimary wouldn't fix that — that is
// ProblemNeedsRewind, handled by NeedsRewindAnalyzer.
type SetPrimaryAnalyzer struct {
	factory *RecoveryActionFactory
}

func (a *SetPrimaryAnalyzer) Name() types.CheckName {
	return "SetPrimary"
}

func (a *SetPrimaryAnalyzer) ProblemCode() types.ProblemCode {
	return types.ProblemNeedsSetPrimary
}

func (a *SetPrimaryAnalyzer) RecoveryAction() types.RecoveryAction {
	return a.factory.NewSetPrimaryAction()
}

func (a *SetPrimaryAnalyzer) Analyze(sa *ShardAnalysis) ([]types.Problem, error) {
	return analyzeAllPoolers(sa, a.analyzePooler)
}

func (a *SetPrimaryAnalyzer) analyzePooler(sa *ShardAnalysis, poolerAnalysis *PoolerAnalysis) (*types.Problem, error) {
	if a.factory == nil {
		return nil, errors.New("recovery action factory not initialized")
	}

	// Skip if the pooler is not initialized (ShardNeedsInitialization handles that).
	if !poolerAnalysis.IsInitialized {
		return nil, nil
	}

	// Skip if there's no usable leader yet. HighestTermReachableLeader is non-nil
	// only when the leader is reachable AND has published its rule (the generator
	// filters out unobserved-rule leaders). Without a rule the recovery action
	// can't populate SetPrimaryRequest.Rule — firing the problem now would produce
	// a guaranteed-fail SetPrimary on the next cycle. LeaderIsDead handles the
	// unreachable case separately.
	if sa.HighestTermReachableLeader == nil {
		return nil, nil
	}

	// Don't tell the current leader to follow itself. Every other pooler —
	// including a stale self-believed leader — is a candidate.
	if proto.Equal(poolerAnalysis.PoolerID, sa.HighestTermReachableLeader.PoolerID) {
		return nil, nil
	}

	// Check whether the pooler is following the current leader.
	if !a.needsSetPrimary(poolerAnalysis, sa.HighestTermReachableLeader) {
		return nil, nil
	}

	return &types.Problem{
		Code:           types.ProblemNeedsSetPrimary,
		CheckName:      "SetPrimary",
		PoolerID:       poolerAnalysis.PoolerID,
		ShardKey:       poolerAnalysis.ShardKey,
		Description:    fmt.Sprintf("Pooler %s is not following the current leader's rule", poolerAnalysis.PoolerID.Name),
		Priority:       types.PriorityHigh,
		Scope:          types.ScopePooler,
		DetectedAt:     time.Now(),
		RecoveryAction: a.factory.NewSetPrimaryAction(),
	}, nil
}

// needsSetPrimary reports whether the pooler is not following the current
// leader's rule and therefore needs a SetPrimary re-point. It is the inverse of
// followsLeaderRule: a pooler that does not already name the current leader at
// its current rule needs to be told who the leader is.
//
// A stale leader records itself as its own ReplicationPrimary (by convention),
// so it names a different leader than the real one and is correctly flagged.
//
// This is deliberately scoped to the "unaware of the latest rule → SetPrimary"
// problem. It does NOT cover "replication appears stuck" while already pointed at
// the right leader; that is ProblemNeedsRewind, handled by NeedsRewindAnalyzer.
func (a *SetPrimaryAnalyzer) needsSetPrimary(analysis, leader *PoolerAnalysis) bool {
	return !followsLeaderRule(analysis, leader)
}
