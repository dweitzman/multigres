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

	"github.com/multigres/multigres/go/multiorch/recovery/types"
	"github.com/multigres/multigres/go/multiorch/store"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
)

// TopologyTypeMismatchAnalyzer detects when topology Type doesn't match postgres role.
// This can happen when:
// - multipooler restarts and overwrites its Type field (the actual bug observed)
// - network partitions cause split-brain scenarios
//
// Following the Vitess pattern (primary_term_start_time), we use primary_term for CAS semantics.
// The recovery action uses the pooler's own consensus term (from its health state) and checks
// if there's a PRIMARY in the shard with a higher term before making changes.
type TopologyTypeMismatchAnalyzer struct {
	factory *RecoveryActionFactory
}

func (a *TopologyTypeMismatchAnalyzer) Name() types.CheckName {
	return "TopologyTypeMismatch"
}

func (a *TopologyTypeMismatchAnalyzer) Analyze(poolerAnalysis *store.ReplicationAnalysis) ([]types.Problem, error) {
	// Skip unreachable nodes - we can't determine their true postgres role
	if poolerAnalysis.IsUnreachable {
		return nil, nil
	}

	// Skip uninitialized nodes - they don't have a meaningful postgres role yet
	if !poolerAnalysis.IsInitialized {
		return nil, nil
	}

	// Get topology type and actual postgres role
	topologyType := poolerAnalysis.PoolerType
	postgresIsPrimary := poolerAnalysis.IsPrimary

	// Check for mismatch
	var mismatchDetected bool
	var description string

	if topologyType == clustermetadatapb.PoolerType_PRIMARY && !postgresIsPrimary {
		mismatchDetected = true
		description = "Topology says PRIMARY but postgres is in recovery (replica)"
	} else if topologyType == clustermetadatapb.PoolerType_REPLICA && postgresIsPrimary {
		mismatchDetected = true
		description = "Topology says REPLICA but postgres is primary (not in recovery)"
	}

	if !mismatchDetected {
		return nil, nil
	}

	if a.factory == nil {
		return nil, errors.New("recovery action factory not initialized")
	}

	// Create problem with recovery action
	// Pass the pooler's own consensus term - this is the term it already has from consensus participation
	return []types.Problem{{
		Code:           types.ProblemPrimaryTypeMismatch,
		CheckName:      "TopologyTypeMismatch",
		PoolerID:       poolerAnalysis.PoolerID,
		ShardKey:       poolerAnalysis.ShardKey,
		Description:    fmt.Sprintf("Pooler %s: %s", poolerAnalysis.PoolerID.Name, description),
		Priority:       types.PriorityNormal,
		Scope:          types.ScopePooler, // Type mismatch affects just this pooler's metadata
		DetectedAt:     time.Now(),
		RecoveryAction: a.factory.NewSyncTopologyTypeAction(poolerAnalysis.ConsensusTerm),
	}}, nil
}
