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
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
)

// consensusNamingLeader builds a ConsensusStatus for `self` whose current rule
// names `leader` as the shard leader at the given coordinator term. The pooler
// is treated as the leader (commonconsensus.IsLeader) exactly when self == leader.
// Both current_position and replication_primary carry the rule, so leader
// identity and "following the leader" predicates resolve consistently.
func consensusNamingLeader(self, leader *clustermetadatapb.ID, term int64) *clustermetadatapb.ConsensusStatus {
	rule := &clustermetadatapb.ShardRule{
		RuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: term},
		LeaderId:   leader,
	}
	return &clustermetadatapb.ConsensusStatus{
		Id:                 self,
		CurrentPosition:    &clustermetadatapb.PoolerPosition{Rule: rule},
		ReplicationPrimary: &clustermetadatapb.ReplicationPrimary{Rule: rule},
	}
}

// streamingReplicationStatus builds a healthy standby replication status:
// actively streaming, not paused, and caught up (zero lag). Use for cohort
// addition-fitness fixtures that should pass isAdditionCandidate.
func streamingReplicationStatus() *multipoolermanagerdatapb.StandbyReplicationStatus {
	return &multipoolermanagerdatapb.StandbyReplicationStatus{
		WalReceiverStatus:  walReceiverStatusStreaming,
		IsWalReplayPaused:  false,
		Lag:                durationpb.New(0),
		LastMsgReceiveTime: timestamppb.Now(),
	}
}
