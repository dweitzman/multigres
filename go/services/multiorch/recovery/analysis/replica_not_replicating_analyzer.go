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

	"github.com/multigres/multigres/go/services/multiorch/recovery/types"
)

// ReplicaNotReplicatingAnalyzer detects replicas that have no replication configured
// or have replication explicitly stopped.
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

func (a *ReplicaNotReplicatingAnalyzer) Analyze(shard *ShardAnalysis) ([]*types.Problem, error) {
	if a.factory == nil {
		return nil, errors.New("recovery action factory not initialized")
	}

	primary := shard.FindPrimary()
	primaryReachable := primary != nil &&
		primary.LastCheckValid &&
		primary.Health != nil && primary.Health.IsPostgresRunning

	var problems []*types.Problem
	for _, ps := range shard.Poolers {
		if ps.IsPrimary {
			continue
		}
		// ShardNeedsBootstrap handles uninitialized replicas
		if !ps.IsInitialized {
			continue
		}
		// PrimaryIsDead handles replicas when the primary is unreachable
		if primary != nil && !primaryReachable {
			continue
		}
		if !needsReplicationFix(ps) {
			continue
		}
		problems = append(problems, &types.Problem{
			Code:           types.ProblemReplicaNotReplicating,
			CheckName:      "ReplicaNotReplicating",
			PoolerID:       ps.ID,
			ShardKey:       shard.ShardKey,
			Description:    fmt.Sprintf("Replica %s has no replication configured", ps.ID.Name),
			Priority:       types.PriorityHigh,
			Scope:          types.ScopePooler,
			DetectedAt:     time.Now(),
			RecoveryAction: a.factory.NewFixReplicationAction(),
		})
	}
	return problems, nil
}

// needsReplicationFix returns true if replication is not configured or stopped.
func needsReplicationFix(ps *PoolerState) bool {
	if ps.Health == nil || ps.Health.ReplicationStatus == nil {
		return true // no replication status at all
	}
	rs := ps.Health.ReplicationStatus
	if rs.PrimaryConnInfo == nil || rs.PrimaryConnInfo.Host == "" {
		return true // primary_conninfo not configured
	}
	return rs.IsWalReplayPaused // replication explicitly stopped
}
