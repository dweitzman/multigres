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

	commonconsensus "github.com/multigres/multigres/go/common/consensus"
	"github.com/multigres/multigres/go/services/multiorch/recovery/types"
	"github.com/multigres/multigres/go/services/multiorch/store"
)

// ReplicaNotReplicatingAnalyzer detects a replica that's correctly pointed at
// the current leader but isn't actually streaming (paused replay, a WAL
// receiver that connected and got FATAL, or an explicit StopReplication) —
// the gap leader-info propagation can't see, since it only compares recorded
// leader identity, not live streaming state. See needsReplicationFix.
type ReplicaNotReplicatingAnalyzer struct {
	factory *RecoveryActionFactory
}

func (a *ReplicaNotReplicatingAnalyzer) Name() types.CheckName {
	return "ReplicaNotReplicating"
}

func (a *ReplicaNotReplicatingAnalyzer) RecoveryAction() types.RecoveryAction {
	return a.factory.NewFixReplicationAction()
}

func (a *ReplicaNotReplicatingAnalyzer) Analyze(sa *ShardAnalysis) ([]types.Problem, error) {
	return analyzeAllPoolers(sa, a.analyzePooler)
}

func (a *ReplicaNotReplicatingAnalyzer) analyzePooler(sa *ShardAnalysis, pa *store.Pooler) (*types.Problem, error) {
	if a.factory == nil {
		return nil, errors.New("recovery action factory not initialized")
	}

	// Only analyze replicas
	if commonconsensus.SelfConsensusRole(pa.Health().GetConsensusStatus()) == commonconsensus.ConsensusRoleLeader {
		return nil, nil
	}

	// Skip if replica is not initialized (ShardNeedsInitialization handles that)
	if !pa.IsInitialized() {
		return nil, nil
	}

	// Skip unless we know where to point the replica: the shard must have a known
	// consensus leader (HighestShardRule) whose host/port we actually have (Leader
	// health present). A leader we have no address for is not actionable.
	//
	// TODO(temporary): we also require the leader to be reachable because today's
	// FixReplication still runs pg_rewind against the leader, which needs it live.
	// Once rewind is separated from SetPrimary (SetPrimary just delivers the
	// leader's rule + address), leader reachability no longer matters here — an
	// unreachable-but-known leader is still the official term leader worth telling
	// replicas about, and only knowing where to point them matters.
	if !leaderServing(sa) || sa.Leader.Health().GetMultipooler().GetHostname() == "" {
		return nil, nil
	}

	// Check if replication is not configured or stopped
	if !a.needsReplicationFix(pa) {
		return nil, nil
	}

	return &types.Problem{
		Code:           types.ProblemReplicaNotReplicating,
		CheckName:      "ReplicaNotReplicating",
		PoolerID:       poolerID(pa),
		ShardKey:       sa.ShardKey,
		Description:    fmt.Sprintf("Replica %s has no replication configured", poolerID(pa).Name),
		Priority:       types.PriorityHigh,
		Scope:          types.ScopePooler,
		DetectedAt:     time.Now(),
		RecoveryAction: a.factory.NewFixReplicationAction(),
	}, nil
}

// needsReplicationFix returns true if replication is configured but not
// actually flowing.
//
// Deliberately does NOT check for a missing/wrong primary_conninfo: that leg
// is now fully covered, faster, by propagateLeaderInfoToPooler
// (leader_info_propagation.go), which re-sends SetPrimary every
// leaderInfoPropagationInterval (1s) whenever a pooler's recorded primary
// doesn't match the current leader — versus this analyzer's full recovery
// cycle. An empty primary_conninfo also has no WAL receiver, so it's still
// caught below via walReceiverActive without needing its own check.
//
// What propagation structurally can't detect: it only compares the pooler's
// *recorded* leader identity/position, never live WAL-receiver state. A
// replica whose primary_conninfo already correctly names the current leader
// but isn't actually streaming (paused replay, a WAL receiver that connected
// and got FATAL, or an explicit StopReplication) looks "already up to date"
// to propagation and is never re-sent. This analyzer exists for that gap.
func (a *ReplicaNotReplicatingAnalyzer) needsReplicationFix(pa *store.Pooler) bool {
	// Replication not running (e.g. WAL replay paused)
	if !walReplayNotPaused(pa) {
		return true
	}

	// primary_conninfo is set but the WAL receiver is not active. This covers
	// timeline divergence: the WAL receiver connects, gets FATAL, and exits,
	// leaving primary_conninfo on disk but no active streaming. Also covers
	// primary_conninfo being unset entirely (no receiver, no "streaming").
	if !walReceiverActive(pa) {
		return true
	}

	return false
}
