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

	"github.com/multigres/multigres/go/services/multiorch/recovery/types"
	"github.com/multigres/multigres/go/services/multiorch/store"
)

// ShardNeedsInitialCohortAnalyzer detects when poolers in a shard have completed
// Phase 1 bootstrap (initdb, schema creation, pgBackRest backup+restore) but Phase 2
// (establishing the initial cohort via multiorch) has not yet happened.
//
// The signal is: a reachable, initialized pooler with no primary in the shard and an
// empty CohortMembers list. Since IsInitialized=true guarantees a leadership_history
// record exists (written by the multipooler before its first backup), a nil/empty
// CohortMembers means the 0-member bootstrap record is present but no cohort has been
// established yet.
//
// The InitialCohortAction re-verifies the full shard state via fresh RPCs before acting,
// including confirming that no node has an established cohort and that enough initialized
// poolers are reachable to satisfy the durability policy.
type ShardNeedsInitialCohortAnalyzer struct {
	factory *RecoveryActionFactory
}

func (a *ShardNeedsInitialCohortAnalyzer) Name() types.CheckName {
	return "ShardNeedsInitialCohort"
}

func (a *ShardNeedsInitialCohortAnalyzer) ProblemCode() types.ProblemCode {
	return types.ProblemShardNeedsInitialCohort
}

func (a *ShardNeedsInitialCohortAnalyzer) RecoveryAction() types.RecoveryAction {
	return a.factory.NewInitialCohortAction()
}

func (a *ShardNeedsInitialCohortAnalyzer) Analyze(poolerAnalysis *store.ReplicationAnalysis) (*types.Problem, error) {
	// Skip unreachable nodes — we can't determine their true state.
	if !poolerAnalysis.LastCheckValid {
		return nil, nil
	}

	// Skip primary nodes — they have no PrimaryPoolerID by design (they ARE the primary).
	if poolerAnalysis.IsPrimary {
		return nil, nil
	}

	// Only fire when Phase 1 bootstrap is complete on this pooler.
	if !poolerAnalysis.IsInitialized {
		return nil, nil
	}

	// Only fire when no primary exists in the shard yet.
	if poolerAnalysis.PrimaryPoolerID != nil {
		return nil, nil
	}

	// len(nil)==0 and len([]string{})==0 both indicate no cohort established yet.
	// Since IsInitialized=true guarantees a leadership_history record, an empty
	// CohortMembers means the 0-member bootstrap record is present — Phase 2 is needed.
	if len(poolerAnalysis.CohortMembers) > 0 {
		return nil, nil
	}

	if a.factory == nil {
		return nil, errors.New("recovery action factory not initialized")
	}

	return &types.Problem{
		Code:           types.ProblemShardNeedsInitialCohort,
		CheckName:      "ShardNeedsInitialCohort",
		PoolerID:       poolerAnalysis.PoolerID,
		ShardKey:       poolerAnalysis.ShardKey,
		Description:    fmt.Sprintf("Shard %s has initialized poolers with no established cohort", poolerAnalysis.ShardKey),
		Priority:       types.PriorityShardBootstrap,
		Scope:          types.ScopeShard,
		DetectedAt:     time.Now(),
		RecoveryAction: a.factory.NewInitialCohortAction(),
	}, nil
}
