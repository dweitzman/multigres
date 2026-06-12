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
	"errors"
	"fmt"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/multigres/multigres/go/services/multiorch/recovery/types"
)

// NeedsRewindAnalyzer detects a pooler that already knows the current leader's
// rule but still can't stream from it — typically because its timeline diverged
// from the leader's (a former leader's unreplicated WAL). SetPrimary can't fix
// this: the pooler has the right information and still can't follow, so its data
// directory has to be rewound to the leader's history.
//
// The signal is: the pooler follows the current leader's rule (so SetPrimary
// would be a no-op) yet its WAL receiver is not streaming. A pooler that doesn't
// know the leader yet is SetPrimaryAnalyzer's job and is skipped here.
type NeedsRewindAnalyzer struct {
	factory *RecoveryActionFactory
}

func (a *NeedsRewindAnalyzer) Name() types.CheckName {
	return "NeedsRewind"
}

func (a *NeedsRewindAnalyzer) ProblemCode() types.ProblemCode {
	return types.ProblemNeedsRewind
}

func (a *NeedsRewindAnalyzer) RecoveryAction() types.RecoveryAction {
	return a.factory.NewRewindAction()
}

func (a *NeedsRewindAnalyzer) Analyze(sa *ShardAnalysis) ([]types.Problem, error) {
	return analyzeAllPoolers(sa, a.analyzePooler)
}

func (a *NeedsRewindAnalyzer) analyzePooler(sa *ShardAnalysis, poolerAnalysis *PoolerAnalysis) (*types.Problem, error) {
	if a.factory == nil {
		return nil, errors.New("recovery action factory not initialized")
	}

	if !poolerAnalysis.IsInitialized {
		return nil, nil
	}
	if sa.HighestTermReachableLeader == nil {
		return nil, nil
	}
	// The leader doesn't rewind to itself.
	if proto.Equal(poolerAnalysis.PoolerID, sa.HighestTermReachableLeader.PoolerID) {
		return nil, nil
	}

	// Only act once the pooler already knows the current leader's rule. If it
	// doesn't, SetPrimaryAnalyzer handles it first; rewinding before the pooler
	// even knows who to follow would be premature.
	if !followsLeaderRule(poolerAnalysis, sa.HighestTermReachableLeader) {
		return nil, nil
	}

	// It knows the leader but isn't streaming — stuck and needs a rewind.
	if poolerAnalysis.ReplicationStatus.GetWalReceiverStatus() == walReceiverStatusStreaming {
		return nil, nil
	}

	return &types.Problem{
		Code:           types.ProblemNeedsRewind,
		CheckName:      "NeedsRewind",
		PoolerID:       poolerAnalysis.PoolerID,
		ShardKey:       poolerAnalysis.ShardKey,
		Description:    fmt.Sprintf("Pooler %s knows the leader but is not streaming; needs rewind", poolerAnalysis.PoolerID.Name),
		Priority:       types.PriorityHigh,
		Scope:          types.ScopePooler,
		DetectedAt:     time.Now(),
		RecoveryAction: a.factory.NewRewindAction(),
	}, nil
}

// followsLeaderRule reports whether the pooler's ReplicationPrimary names the
// current leader at its current rule (or newer). It is the inverse of "needs
// SetPrimary": a pooler that follows the leader's rule already has the
// information SetPrimary would deliver.
func followsLeaderRule(analysis, leader *PoolerAnalysis) bool {
	rp := analysis.ConsensusStatus.GetReplicationPrimary().GetRule()

	if proto.Equal(rp.GetLeaderId(), leader.PoolerID) {
		return true
	}
	return false
}
