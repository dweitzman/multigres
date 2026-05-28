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

// stuckReplicationThreshold is how long a replica's WAL receiver may be
// in a non-"streaming" state before this analyzer surfaces the
// condition. The pooler itself self-tracks the same condition with a
// shorter threshold (30s today) and self-rewinds; orch's threshold sits
// above that so the analyzer fires only when self-recovery hasn't
// resolved it within a reasonable window.
const stuckReplicationThreshold = 90 * time.Second

// StuckReplicationAnalyzer surfaces poolers whose ReplicationPrimary is
// pointing at the right leader but whose WAL receiver hasn't been in
// "streaming" state for long enough that the local self-rewind path
// should have had a chance to clear it.
//
// This analyzer is observability-only: the problem it emits carries no
// RecoveryAction. The recovery loop logs the condition (at debug level)
// and records the event for metrics, but takes no further action.
// Remediation lives on the pooler — see remedialActionSelfRewind /
// remedialActionSelfDrain in multipooler manager.go.
//
// The signal source is the pooler-published
// StandbyReplicationStatus.wal_receiver_not_streaming_since timestamp.
type StuckReplicationAnalyzer struct {
	factory *RecoveryActionFactory
}

func (a *StuckReplicationAnalyzer) Name() types.CheckName {
	return "StuckReplication"
}

func (a *StuckReplicationAnalyzer) ProblemCode() types.ProblemCode {
	return types.ProblemReplicaStuck
}

func (a *StuckReplicationAnalyzer) RecoveryAction() types.RecoveryAction {
	// Observability-only: no remediation. The Problem objects we emit
	// also carry RecoveryAction=nil; the recovery loop early-skips them.
	return nil
}

func (a *StuckReplicationAnalyzer) Analyze(sa *ShardAnalysis) ([]types.Problem, error) {
	if a.factory == nil {
		return nil, errors.New("recovery action factory not initialized")
	}

	clusterLeaderID := sa.LeaderObservation.GetLeaderId()
	if clusterLeaderID == nil {
		// No cluster leader observed → no expectation about who any
		// pooler should be streaming from. Don't flag.
		return nil, nil
	}

	var problems []types.Problem
	for _, pa := range sa.Analyses {
		if pa.IsLeader {
			continue
		}
		if pa.WalReceiverNotStreamingSince.IsZero() {
			// Currently streaming, no primary_conninfo configured, or
			// the pooler hasn't published the field yet (older version).
			continue
		}
		stuckFor := time.Since(pa.WalReceiverNotStreamingSince)
		if stuckFor < stuckReplicationThreshold {
			continue
		}
		// Only complain when the pooler IS pointed at the right leader.
		// Mismatched ReplicationPrimary is handled by
		// StaleReplicationPrimaryAnalyzer (with a real recovery action),
		// and we'd otherwise double-emit on the same pooler.
		selfLeaderID := pa.SelfLeaderObservation().GetLeaderId()
		if selfLeaderID == nil || !proto.Equal(selfLeaderID, clusterLeaderID) {
			continue
		}

		problems = append(problems, types.Problem{
			Code:      types.ProblemReplicaStuck,
			CheckName: a.Name(),
			PoolerID:  pa.PoolerID,
			ShardKey:  pa.ShardKey,
			Description: fmt.Sprintf(
				"Replica %s WAL receiver has been non-streaming for %s (since %s) despite pointing at cluster leader %s",
				pa.PoolerID.GetName(),
				stuckFor.Truncate(time.Second),
				pa.WalReceiverNotStreamingSince.Format(time.RFC3339),
				clusterLeaderID.GetName(),
			),
			Priority:       types.PriorityNormal,
			Scope:          types.ScopePooler,
			DetectedAt:     time.Now(),
			RecoveryAction: nil, // observability-only
		})
	}
	return problems, nil
}
