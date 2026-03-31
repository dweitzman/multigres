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

// ShardNeedsBootstrapAnalyzer detects when a shard has no initialized nodes and no primary.
// This is a shard-scoped analyzer: it evaluates the full shard once and returns at most
// one problem. PoolerID is nil because the problem belongs to the shard as a whole.
type ShardNeedsBootstrapAnalyzer struct {
	factory *RecoveryActionFactory
}

func (a *ShardNeedsBootstrapAnalyzer) Name() types.CheckName {
	return "ShardNeedsBootstrap"
}

func (a *ShardNeedsBootstrapAnalyzer) ProblemCode() types.ProblemCode {
	return types.ProblemShardNeedsBootstrap
}

func (a *ShardNeedsBootstrapAnalyzer) RecoveryAction() types.RecoveryAction {
	return a.factory.NewBootstrapShardAction()
}

func (a *ShardNeedsBootstrapAnalyzer) Analyze(shard *ShardAnalysis) ([]*types.Problem, error) {
	if a.factory == nil {
		return nil, errors.New("recovery action factory not initialized")
	}

	// If the shard has a primary, bootstrap is not needed.
	// A dead primary is handled by PrimaryIsDead; a crashed primary's postgres
	// appearing as IsPrimary=true / IsInitialized=false is not a bootstrap case.
	if shard.FindPrimary() != nil {
		return nil, nil
	}

	// Check whether any reachable replica needs bootstrap. A replica needs bootstrap when:
	// - Its health check is valid (we can trust what it reports)
	// - It has no data directory (PG_VERSION absent — never been initialized)
	// - It is not initialized (postgres not running and reporting state)
	//
	// HasDataDirectory is the canonical "was ever initialized" signal regardless of
	// pooler type, so we skip replicas that have data even if IsInitialized is false.
	for _, ps := range shard.Poolers {
		if ps.IsPrimary {
			continue
		}
		if !ps.LastCheckValid {
			continue // can't trust the state of unreachable nodes
		}
		if ps.HasDataDirectory || ps.IsInitialized {
			continue
		}
		return []*types.Problem{{
			Code:           types.ProblemShardNeedsBootstrap,
			CheckName:      "ShardNeedsBootstrap",
			PoolerID:       nil, // shard-scoped: no single pooler owns this problem
			ShardKey:       shard.ShardKey,
			Description:    fmt.Sprintf("Shard %s has no initialized nodes and needs bootstrap", shard.ShardKey),
			Priority:       types.PriorityShardBootstrap,
			Scope:          types.ScopeShard,
			DetectedAt:     time.Now(),
			RecoveryAction: a.factory.NewBootstrapShardAction(),
		}}, nil
	}

	return nil, nil
}
