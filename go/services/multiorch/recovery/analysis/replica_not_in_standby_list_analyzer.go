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

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/services/multiorch/recovery/types"
)

// ReplicaNotInStandbyListAnalyzer detects when a replica is not in the primary's
// synchronous_standby_names list. This can happen if:
// - The replica was added but never registered with the primary
// - The standby list was cleared/modified manually
// - There was a failure during the fix replication process
type ReplicaNotInStandbyListAnalyzer struct {
	factory *RecoveryActionFactory
}

func (a *ReplicaNotInStandbyListAnalyzer) Name() types.CheckName {
	return "ReplicaNotInStandbyList"
}

func (a *ReplicaNotInStandbyListAnalyzer) ProblemCode() types.ProblemCode {
	return types.ProblemReplicaNotInStandbyList
}

func (a *ReplicaNotInStandbyListAnalyzer) RecoveryAction() types.RecoveryAction {
	return a.factory.NewFixReplicationAction()
}

func (a *ReplicaNotInStandbyListAnalyzer) Analyze(shard *ShardAnalysis) ([]*types.Problem, error) {
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
		if !ps.IsInitialized {
			continue
		}
		// Can't update standby list if primary is unreachable
		if primary != nil && !primaryReachable {
			continue
		}
		// Only fully-typed REPLICA poolers (not UNKNOWN)
		if ps.Type != clustermetadatapb.PoolerType_REPLICA {
			continue
		}
		// ReplicaNotReplicating handles replicas with no primary_conninfo
		if ps.Health == nil || ps.Health.ReplicationStatus == nil ||
			ps.Health.ReplicationStatus.PrimaryConnInfo == nil ||
			ps.Health.ReplicationStatus.PrimaryConnInfo.Host == "" {
			continue
		}
		if primary != nil && isInStandbyList(ps, primary) {
			continue
		}
		problems = append(problems, &types.Problem{
			Code:           types.ProblemReplicaNotInStandbyList,
			CheckName:      "ReplicaNotInStandbyList",
			PoolerID:       ps.ID,
			ShardKey:       shard.ShardKey,
			Description:    fmt.Sprintf("Replica %s is not in primary's synchronous standby list", ps.ID.Name),
			Priority:       types.PriorityNormal,
			Scope:          types.ScopePooler,
			DetectedAt:     time.Now(),
			RecoveryAction: a.factory.NewFixReplicationAction(),
		})
	}
	return problems, nil
}

// isInStandbyList checks if the replica is in the primary's synchronous standby list.
func isInStandbyList(replica *PoolerState, primary *PoolerState) bool {
	if primary.Health == nil || primary.Health.PrimaryStatus == nil ||
		primary.Health.PrimaryStatus.SyncReplicationConfig == nil {
		return false
	}
	for _, standbyID := range primary.Health.PrimaryStatus.SyncReplicationConfig.StandbyIds {
		if standbyID.Cell == replica.ID.Cell && standbyID.Name == replica.ID.Name {
			return true
		}
	}
	return false
}
