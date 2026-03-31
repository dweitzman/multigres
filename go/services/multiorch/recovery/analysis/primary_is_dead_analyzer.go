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

// PrimaryIsDeadAnalyzer detects when the shard's primary is unhealthy or unreachable.
// This is a shard-scoped analyzer: it evaluates the full shard state once and returns
// at most one problem. PoolerID is nil because the problem belongs to the shard, not
// any individual pooler — the recovery action determines the new leader independently.
type PrimaryIsDeadAnalyzer struct {
	factory *RecoveryActionFactory
}

func (a *PrimaryIsDeadAnalyzer) Name() types.CheckName {
	return "PrimaryIsDead"
}

func (a *PrimaryIsDeadAnalyzer) ProblemCode() types.ProblemCode {
	return types.ProblemPrimaryIsDead
}

func (a *PrimaryIsDeadAnalyzer) RecoveryAction() types.RecoveryAction {
	return a.factory.NewAppointLeaderAction()
}

func (a *PrimaryIsDeadAnalyzer) Analyze(shard *ShardAnalysis) ([]*types.Problem, error) {
	if a.factory == nil {
		return nil, errors.New("recovery action factory not initialized")
	}

	primary := shard.FindPrimary()
	if primary == nil {
		return nil, nil // no primary — ShardNeedsBootstrap handles that
	}

	// Check if any initialized replica exists. If no replicas are initialized, we
	// cannot determine primary health from their perspective.
	hasInitializedReplica := false
	for _, ps := range shard.Poolers {
		if !ps.IsPrimary && ps.IsInitialized {
			hasInitializedReplica = true
			break
		}
	}
	if !hasInitializedReplica {
		return nil, nil
	}

	primaryReachable := primary.LastCheckValid && primary.Health != nil && primary.Health.IsPostgresRunning

	if primaryReachable {
		return nil, nil
	}

	// Primary is not fully reachable. Distinguish two cases:
	// 1. Primary pooler unreachable but Postgres still running (pooler process crashed):
	//    → do NOT failover; operator should restart the pooler.
	// 2. Primary Postgres is down:
	//    → failover needed.
	if !primary.LastCheckValid && shard.AllReplicasConnectedToPrimary(primary) {
		a.factory.Logger().Warn("primary pooler unreachable but postgres still running",
			"shard_key", shard.ShardKey.String(),
			"primary_pooler_id", primary.ID.Name,
			"action", "operator should restart pooler process")
		return nil, nil
	}

	return []*types.Problem{{
		Code:           types.ProblemPrimaryIsDead,
		CheckName:      "PrimaryIsDead",
		PoolerID:       primary.ID, // the specific primary we believe is dead
		ShardKey:       shard.ShardKey,
		Description:    fmt.Sprintf("Primary for shard %s is dead/unreachable", shard.ShardKey),
		Priority:       types.PriorityEmergency,
		Scope:          types.ScopeShard,
		DetectedAt:     time.Now(),
		RecoveryAction: a.factory.NewAppointLeaderAction(),
	}}, nil
}
