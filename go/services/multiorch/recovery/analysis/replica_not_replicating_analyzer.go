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

// ReplicaNotReplicatingAnalyzer detects when a replica's ReplicationPrimary
// is out of step with the cluster-authoritative leader: no primary_conninfo
// configured, replication explicitly stopped, or replication pointed at a
// pooler other than the current cluster leader. All three cases reduce to
// the same fix — SetTermPrimary against the current leader — but they
// happen at different stages of a pooler's lifecycle.
type ReplicaNotReplicatingAnalyzer struct {
	factory *RecoveryActionFactory
}

func (a *ReplicaNotReplicatingAnalyzer) Name() types.CheckName {
	return "ReplicaNotReplicating"
}

func (a *ReplicaNotReplicatingAnalyzer) ProblemCode() types.ProblemCode {
	return types.ProblemReplicaNotReplicating
}

func (a *ReplicaNotReplicatingAnalyzer) RecoveryAction() types.RecoveryAction {
	return a.factory.NewFixReplicationAction()
}

func (a *ReplicaNotReplicatingAnalyzer) Analyze(sa *ShardAnalysis) ([]types.Problem, error) {
	return analyzeAllPoolers(sa, a.analyzePooler)
}

func (a *ReplicaNotReplicatingAnalyzer) analyzePooler(sa *ShardAnalysis, poolerAnalysis *PoolerAnalysis) (*types.Problem, error) {
	if a.factory == nil {
		return nil, errors.New("recovery action factory not initialized")
	}

	// Only analyze replicas
	if poolerAnalysis.IsLeader {
		return nil, nil
	}

	// Skip if replica is not initialized (ShardNeedsInitialization handles that)
	if !poolerAnalysis.IsInitialized {
		return nil, nil
	}

	// Skip if there's no usable primary yet. sa.Leader is non-nil only when
	// the cluster-authoritative LeaderObservation names a pooler in our
	// known set. Reachability is a separate concern (sa.LeaderReachable);
	// PrimaryIsDead handles the unreachable case. Without a leader the
	// recovery action can't populate SetTermPrimaryRequest.Rule — firing
	// the problem now would produce a guaranteed-fail SetTermPrimary.
	if sa.Leader == nil || !sa.LeaderReachable {
		return nil, nil
	}

	// Check if replication is not configured, stopped, or stale
	if !a.needsReplicationFix(sa, poolerAnalysis) {
		return nil, nil
	}

	return &types.Problem{
		Code:           types.ProblemReplicaNotReplicating,
		CheckName:      "ReplicaNotReplicating",
		PoolerID:       poolerAnalysis.PoolerID,
		ShardKey:       poolerAnalysis.ShardKey,
		Description:    fmt.Sprintf("Replica %s has no replication configured", poolerAnalysis.PoolerID.Name),
		Priority:       types.PriorityHigh,
		Scope:          types.ScopePooler,
		DetectedAt:     time.Now(),
		RecoveryAction: a.factory.NewFixReplicationAction(),
	}, nil
}

// needsReplicationFix returns true if the pooler's ReplicationPrimary is out
// of step with the cluster-authoritative leader.
func (a *ReplicaNotReplicatingAnalyzer) needsReplicationFix(sa *ShardAnalysis, analysis *PoolerAnalysis) bool {
	// No primary_conninfo configured — pooler hasn't been told about a
	// primary yet, or had its config wiped (e.g. fresh standby).
	if analysis.PrimaryConnInfoHost == "" {
		return true
	}

	// Replication explicitly stopped — WAL replay paused, WAL receiver
	// in non-streaming state, etc. The pooler is configured but isn't
	// making progress.
	if analysis.ReplicationStopped {
		return true
	}

	// Pooler's self-reported ReplicationPrimary names a leader other
	// than the cluster's authoritative one — stale. The StaleLeader
	// analyzer covers the special case where the stale claim names the
	// pooler itself; here we cover everyone else.
	selfObs := analysis.SelfLeaderObservation()
	if selfObs.GetLeaderId() != nil &&
		!proto.Equal(selfObs.GetLeaderId(), analysis.PoolerID) &&
		!proto.Equal(selfObs.GetLeaderId(), sa.LeaderObservation.GetLeaderId()) {
		return true
	}

	return false
}
