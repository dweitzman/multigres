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
	"google.golang.org/protobuf/types/known/timestamppb"

	commonconsensus "github.com/multigres/multigres/go/common/consensus"
	"github.com/multigres/multigres/go/common/topoclient"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/services/multiorch/store"
)

// freshestQuorumCommitTs returns the most recent quorum_commit_ts known for
// this shard. quorum_commit_ts names one leader-authored fact, not
// independent per-member values, so any source that has a fresh copy is
// valid proof — including the leader's own (always >= any follower's replica,
// since replication only adds delay) and a non-cohort observer's.
//
// TODO: verify a report's heartbeat leader_id matches leaderID (fencing-gap misattribution risk).
func freshestQuorumCommitTs(sa *ShardAnalysis) *timestamppb.Timestamp {
	var freshest *timestamppb.Timestamp
	if sa.Leader != nil {
		if ts := sa.Leader.Health().GetStatus().GetPrimaryStatus().GetQuorumCommitTs(); ts != nil {
			freshest = ts
		}
	}
	for _, pa := range sa.Analyses {
		if pa == nil {
			continue
		}
		ts := pa.Health().GetStatus().GetReplicationStatus().GetQuorumCommitTs()
		if ts == nil {
			continue
		}
		if freshest == nil || ts.AsTime().After(freshest.AsTime()) {
			freshest = ts
		}
	}
	return freshest
}

// receiveLsnStillAdvancing reports whether a durability-sufficient set of the
// cohort has a recent last_receive_lsn_advance_time from the candidate leader
// specifically — evidence, during an undecided promotion (see
// quorumCommitStuckCause), that a quorum-commit stall is backlog-draining
// rather than a genuine halt. Unlike raw LSN, last_receive_lsn_advance_time
// only moves via live streaming (never restore_command replay), so it can't
// be spoofed by archive replay. Gated on replicaConfiguredForLeader so WAL
// advance from an unrelated primary can't stand in as evidence of this
// leader's health.
func (a *LeaderNeedsReplacementAnalyzer) receiveLsnStillAdvancing(sa *ShardAnalysis, cohort []*clustermetadatapb.ID, leaderID *clustermetadatapb.ID, policy commonconsensus.DurabilityPolicy) bool {
	if sa.Leader == nil {
		return false
	}
	primaryHost := sa.Leader.Health().GetMultipooler().GetHostname()
	primaryPort := sa.Leader.Health().GetMultipooler().GetPortMap()["postgres"]

	leaderKey := topoclient.ComponentIDString(leaderID)
	byID := make(map[topoclient.ComponentID]*store.Pooler, len(sa.Analyses))
	for _, pa := range sa.Analyses {
		if pa != nil {
			byID[topoclient.ComponentIDString(poolerID(pa))] = pa
		}
	}
	var vouching []*clustermetadatapb.ID
	for _, member := range cohort {
		if topoclient.ComponentIDString(member) == leaderKey {
			continue
		}
		pa, ok := byID[topoclient.ComponentIDString(member)]
		if !ok || !replicaConfiguredForLeader(pa, primaryHost, primaryPort) {
			continue
		}
		ts := pa.Health().GetStatus().GetReplicationStatus().GetLastReceiveLsnAdvanceTime()
		if ts != nil && sa.Now.Sub(ts.AsTime()) <= sa.Policy.FollowerStreamFreshness {
			vouching = append(vouching, member)
		}
	}
	if len(vouching) == 0 {
		return false
	}
	// A quorum-sufficient set actively receiving fresh WAL proves the leader
	// itself is generating and streaming it right now, so it vouches too —
	// same self-vouching inference as classifyFollowerReachability.
	vouching = append(vouching, leaderID)
	return policy.SatisfiedBy(vouching) == nil
}
