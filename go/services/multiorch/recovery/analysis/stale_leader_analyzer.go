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
	"slices"
	"time"

	"google.golang.org/protobuf/proto"

	commonconsensus "github.com/multigres/multigres/go/common/consensus"
	"github.com/multigres/multigres/go/services/multiorch/recovery/types"
)

// StaleLeaderAnalyzer detects stale primaries — poolers whose own
// health-stream view still names themselves as leader, but at a rule
// strictly older than the cluster-wide authoritative observation.
// This happens when an old primary restarts without being properly
// demoted, or when SetTermPrimary hasn't yet reached the demoted node.
//
// Problems are sorted most-stale-first with descending priorities so
// the recovery system addresses the most out-of-date primary first.
//
// Note: This is NOT true split-brain. True split-brain means both
// primaries can accept writes. In this scenario, the new primary
// cannot accept writes because it cannot recruit standbys while the
// stale leader exists.
type StaleLeaderAnalyzer struct {
	factory *RecoveryActionFactory
}

func (a *StaleLeaderAnalyzer) Name() types.CheckName {
	return "StaleLeader"
}

func (a *StaleLeaderAnalyzer) ProblemCode() types.ProblemCode {
	return types.ProblemStaleLeader
}

func (a *StaleLeaderAnalyzer) RecoveryAction() types.RecoveryAction {
	return a.factory.NewDemoteStaleLeaderAction()
}

func (a *StaleLeaderAnalyzer) Analyze(sa *ShardAnalysis) ([]types.Problem, error) {
	if a.factory == nil {
		return nil, errors.New("recovery action factory not initialized")
	}

	// Need a cluster-authoritative leader observation to compare against.
	if sa.LeaderObservation.GetLeaderId() == nil {
		return nil, nil
	}

	// A stale primary is a pooler whose own health-stream observation
	// still names itself as leader, but the cluster authoritative view
	// names someone else. The rule number on the self-observation
	// doesn't matter here — if pooler-A claims to be leader at 5.1 and
	// the cluster also names pooler-A (at any rule, possibly newer),
	// A is still the leader, not stale.
	clusterLeaderID := sa.LeaderObservation.GetLeaderId()
	var staleLeaders []*PoolerAnalysis
	for _, pa := range sa.Analyses {
		if pa.IsLeader {
			continue
		}
		selfObs := pa.SelfLeaderObservation()
		if !proto.Equal(selfObs.GetLeaderId(), pa.PoolerID) {
			continue // pooler doesn't think it's the leader
		}
		// Pooler thinks IT is leader; cluster says someone else is. Stale.
		if proto.Equal(selfObs.GetLeaderId(), clusterLeaderID) {
			// Sanity: self-named ID matches cluster-named ID but IsLeader
			// is false. Shouldn't happen given generator.go's IsLeader
			// derivation. Skip rather than misclassify.
			continue
		}
		staleLeaders = append(staleLeaders, pa)
	}

	if len(staleLeaders) == 0 {
		return nil, nil
	}

	// Sort most stale first (lowest rule coordinator term first) so the
	// recovery system processes the most out-of-date leader at highest
	// priority.
	slices.SortFunc(staleLeaders, compareLeaderTimeline)

	clusterLeaderName := sa.LeaderObservation.GetLeaderId().GetName()
	clusterLeaderTerm := sa.LeaderObservation.GetLeaderRuleNumber().GetCoordinatorTerm()

	// Assign descending priorities so the most stale leader (sorted first)
	// gets PriorityEmergency, the next gets PriorityEmergency-1, etc.
	problems := make([]types.Problem, 0, len(staleLeaders))
	for i, stale := range staleLeaders {
		problems = append(problems, types.Problem{
			Code:      types.ProblemStaleLeader,
			CheckName: "StaleLeader",
			PoolerID:  stale.PoolerID,
			ShardKey:  sa.ShardKey,
			Description: fmt.Sprintf("Stale leader detected: %s (stale_leader_term %d) is stale, most advanced leader %s (most_advanced_leader_term %d)",
				stale.PoolerID.Name,
				commonconsensus.LeaderTerm(stale.ConsensusStatus),
				clusterLeaderName,
				clusterLeaderTerm),
			Priority:       types.PriorityEmergency - types.Priority(i),
			Scope:          types.ScopeShard,
			DetectedAt:     time.Now(),
			RecoveryAction: a.factory.NewDemoteStaleLeaderAction(),
		})
	}
	return problems, nil
}
