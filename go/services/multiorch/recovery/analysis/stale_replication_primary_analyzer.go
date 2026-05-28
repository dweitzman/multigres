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
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/services/multiorch/recovery/types"
)

// StaleReplicationPrimaryAnalyzer looks at one thing: the pooler's
// self-reported ReplicationPrimary. A pooler is in a healthy steady state
// when its ReplicationPrimary names the same LeaderId as the cluster
// authoritative LeaderObservation. Two failure shapes:
//
//   - The pooler's ReplicationPrimary names ITSELF (and the cluster has
//     moved on to a different leader). This is a stale primary — a
//     serious condition because the node may still be accepting writes
//     locally. Emitted as ProblemStaleLeader at PriorityEmergency.
//
//   - The pooler's ReplicationPrimary is unset OR names a non-leader.
//     The pooler is a follower but isn't pointed at the right source.
//     Emitted as ProblemReplicaNotReplicating at PriorityHigh.
//
// Both reduce to a single SetTermPrimary RPC against the misbehaving
// pooler with the current leader's rule. FixReplicationAction handles
// either problem code with the same code path; the pooler-side
// SetTermPrimary handler dispatches between the stale-primary demote
// branch and the standby retarget branch based on local postgres state.
//
// Separating the codes here lets recovery prioritize stale-primary
// remediation ahead of follower retargeting, and lets integration tests
// assert on the right event shape (primary.demotion vs node.join).
//
// Other replication anomalies (WAL replay paused by operator, WAL
// receiver stuck despite correct configuration, etc.) belong to
// different analyzers — they aren't fixed by SetTermPrimary.
type StaleReplicationPrimaryAnalyzer struct {
	factory *RecoveryActionFactory
}

func (a *StaleReplicationPrimaryAnalyzer) Name() types.CheckName {
	return "StaleReplicationPrimary"
}

func (a *StaleReplicationPrimaryAnalyzer) ProblemCode() types.ProblemCode {
	// The analyzer emits two codes; ProblemStaleLeader is the more
	// serious one and the documented "primary" code for the check.
	return types.ProblemStaleLeader
}

func (a *StaleReplicationPrimaryAnalyzer) RecoveryAction() types.RecoveryAction {
	return a.factory.NewSetReplicationPrimaryAction()
}

func (a *StaleReplicationPrimaryAnalyzer) Analyze(sa *ShardAnalysis) ([]types.Problem, error) {
	if a.factory == nil {
		return nil, errors.New("recovery action factory not initialized")
	}

	// Need a cluster-authoritative leader observation to compare against.
	// Without one, we can't classify any pooler as "stale relative to the
	// cluster" and we don't yet have a rule to put in SetTermPrimary.
	clusterLeaderID := sa.LeaderObservation.GetLeaderId()
	if clusterLeaderID == nil {
		return nil, nil
	}

	var problems []types.Problem
	var staleLeaders []*PoolerAnalysis

	for _, pa := range sa.Analyses {
		if pa.IsLeader {
			continue
		}
		if !pa.IsInitialized {
			// ShardNeedsInitialization handles uninitialized poolers.
			continue
		}

		selfLeaderID := pa.SelfLeaderObservation().GetLeaderId()
		// In agreement with the cluster — nothing to do.
		if selfLeaderID != nil && proto.Equal(selfLeaderID, clusterLeaderID) {
			continue
		}

		// Self-claiming pooler: a stale primary at an older rule. Most
		// dangerous case — its postgres may still accept writes locally.
		// Collected and sorted before emitting so descending priorities
		// reflect "most stale first".
		if selfLeaderID != nil && proto.Equal(selfLeaderID, pa.PoolerID) {
			staleLeaders = append(staleLeaders, pa)
			continue
		}

		// Replica path: ReplicationPrimary is nil (never told about a
		// primary, or in-memory state was lost on restart) or names a
		// non-self pooler that isn't the current cluster leader. Both
		// need SetTermPrimary against the cluster leader. We require a
		// reachable cluster leader so the recovery action has somewhere
		// to point at.
		if sa.Leader == nil || !sa.LeaderReachable {
			continue
		}
		problems = append(problems, types.Problem{
			Code:           types.ProblemReplicaNotReplicating,
			CheckName:      a.Name(),
			PoolerID:       pa.PoolerID,
			ShardKey:       pa.ShardKey,
			Description:    fmt.Sprintf("Replica %s replicating from %s, not cluster leader %s", pa.PoolerID.Name, leaderIDName(selfLeaderID), clusterLeaderID.GetName()),
			Priority:       types.PriorityHigh,
			Scope:          types.ScopePooler,
			DetectedAt:     time.Now(),
			RecoveryAction: a.factory.NewSetReplicationPrimaryAction(),
		})
	}

	// Sort most-stale-first by self-observation rule, then assign descending
	// priorities so the most out-of-date claim is processed first.
	slices.SortFunc(staleLeaders, compareLeaderTimeline)
	clusterLeaderName := clusterLeaderID.GetName()
	clusterLeaderTerm := sa.LeaderObservation.GetLeaderRuleNumber().GetCoordinatorTerm()
	for i, stale := range staleLeaders {
		problems = append(problems, types.Problem{
			Code:      types.ProblemStaleLeader,
			CheckName: a.Name(),
			PoolerID:  stale.PoolerID,
			ShardKey:  sa.ShardKey,
			Description: fmt.Sprintf("Stale leader detected: %s (stale_leader_term %d) is stale, most advanced leader %s (most_advanced_leader_term %d)",
				stale.PoolerID.Name,
				commonconsensus.LeaderTerm(stale.ConsensusStatus),
				clusterLeaderName,
				clusterLeaderTerm),
			Priority:       types.PriorityEmergency - types.Priority(i),
			Scope:          types.ScopePooler,
			DetectedAt:     time.Now(),
			RecoveryAction: a.factory.NewSetReplicationPrimaryAction(),
		})
	}

	return problems, nil
}

// leaderIDName returns the leader's name or a placeholder when the
// ReplicationPrimary observation is empty — useful for human-readable
// problem descriptions.
func leaderIDName(id *clustermetadatapb.ID) string {
	if id == nil {
		return "<no observation>"
	}
	return id.GetName()
}
