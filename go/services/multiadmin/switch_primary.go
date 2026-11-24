// Copyright 2026 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package multiadmin

import (
	"context"
	"time"

	"github.com/multigres/multigres/go/common/consensus"
	"github.com/multigres/multigres/go/common/topoclient"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multiadminpb "github.com/multigres/multigres/go/pb/multiadmin"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
	"github.com/multigres/multigres/go/tools/retry"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func (s *MultiadminServer) findShardPrimary(ctx context.Context, shardRef *clustermetadatapb.ShardRef) (*clustermetadatapb.Multipooler, bool, error) {
	allPoolers, err := s.findShardPoolers(ctx, shardRef)
	if err != nil {
		return nil, false, err
	}

	var leader *clustermetadatapb.Multipooler
	var hasStandby bool

	for _, p := range allPoolers {
		if p.GetRoutingState().GetRole() == clustermetadatapb.RoutingRole_ROUTING_ROLE_PRIMARY {
			leader = p
		} else {
			hasStandby = true
		}
	}

	if leader != nil {
		return leader, hasStandby, nil
	}

	return nil, false, status.Errorf(codes.NotFound, "no PRIMARY pooler found for shard %s/%s/%s",
		shardRef.GetDatabase(), shardRef.GetTableGroup(), shardRef.GetShard())
}

// SwitchPrimary performs a graceful switchover: it quiesces writes on the
// current leader, which then prefers not to lead and reports NOT_SERVING so
// multiorch replaces it through the normal consensus flow. It returns once a
// decided rule names another leader, and fails if the old leader is named
// again or no new leader appears in time.
func (s *MultiadminServer) SwitchPrimary(ctx context.Context, req *multiadminpb.SwitchPrimaryRequest) (*multiadminpb.SwitchPrimaryResponse, error) {
	if req.GetShardRef() == nil {
		return nil, status.Error(codes.InvalidArgument, "shard_ref is required")
	}
	shardRef := req.GetShardRef()

	// 1. Find the primary pooler for this shard.
	leader, hasStandby, err := s.findShardPrimary(ctx, shardRef)
	if err != nil {
		return nil, err
	}

	if !hasStandby {
		return nil, status.Error(codes.FailedPrecondition, "no standby poolers available for promotion")
	}

	// 3. Pre-check: wait for the coordinator backoff window to clear before
	// quiescing writes. The 4-second guard in checkRecentTermAcceptance rejects
	// a new Recruit if a previous one completed too recently (e.g. bootstrap).
	// Waiting here — before writes are frozen — avoids the pause that would
	// otherwise sit while the old primary's postgres is still running and
	// generating WAL.
	// The leader's decided rule is also the baseline a new leader must beat.
	const coordinatorBackoffWindow = 4 * time.Second
	var baseline *clustermetadatapb.RuleNumber
	if statusResp, err := s.rpcClient.Status(ctx, leader, &multipoolermanagerdatapb.StatusRequest{}); err == nil {
		baseline = statusResp.GetConsensusStatus().GetCurrentPosition().GetPosition().GetDecision().GetRuleNumber()
		if rev := statusResp.GetConsensusStatus().GetTermRevocation(); rev != nil {
			if initAt := rev.GetCoordinatorInitiatedAt(); initAt != nil {
				age := time.Since(initAt.AsTime())
				if age < coordinatorBackoffWindow {
					waitDur := coordinatorBackoffWindow - age + 500*time.Millisecond
					s.logger.InfoContext(ctx, "switch_primary: coordinator backoff active, waiting before quiescing",
						"wait", waitDur)
					select {
					case <-ctx.Done():
						return nil, ctx.Err()
					case <-time.After(waitDur):
					}
				}
			}
		}
	}

	// 4. Quiesce writes and restart postgres as standby. ResignLeadership
	// transitions to DRAINING (rejecting new queries with MTF01 so the
	// gateway buffers and retries), drains existing write connections, stops
	// postgres cleanly (sending the shutdown-checkpoint WAL to connected
	// standbys), and reports NOT_SERVING for multiorch to act on.
	s.logger.InfoContext(ctx, "switch_primary: resigning current leader",
		"leader", topoclient.ClusterIDString(leader.Id),
		"reason", req.GetReason())

	resignResp, err := s.rpcClient.ResignLeadership(ctx, leader, &multipoolermanagerdatapb.ResignLeadershipRequest{})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "ResignLeadership failed: %v", err)
	}

	s.logger.InfoContext(ctx, "switch_primary: old leader resigned; waiting for a new leader",
		"old_leader", topoclient.ClusterIDString(leader.Id),
		"flush_lsn", resignResp.GetFlushLsn())

	// 5. Wait for multiorch to name a new leader.
	newLeader, err := s.waitForNewLeader(ctx, shardRef, leader.Id, baseline)
	if err != nil {
		return nil, err
	}
	s.logger.InfoContext(ctx, "switch_primary: complete",
		"old_leader", topoclient.ClusterIDString(leader.Id),
		"new_leader", topoclient.ClusterIDString(newLeader))

	return &multiadminpb.SwitchPrimaryResponse{
		OldLeaderId: proto.Clone(leader.Id).(*clustermetadatapb.ID),
		NewLeaderId: newLeader,
	}, nil
}

// waitForNewLeader polls the shard's poolers until a decided rule at a higher
// coordinator term than baseline names a leader. It returns that leader, or an
// error if the rule names oldLeader again or none appears within 30s. Only the
// coordinator term counts: a leader-led rule (a subterm bump, such as a cohort
// update) is not a new election. With no baseline, only another leader counts.
func (s *MultiadminServer) waitForNewLeader(ctx context.Context, shardRef *clustermetadatapb.ShardRef, oldLeader *clustermetadatapb.ID, baseline *clustermetadatapb.RuleNumber) (*clustermetadatapb.ID, error) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	for _, err := range retry.New(100*time.Millisecond, time.Second).Attempts(ctx) {
		if err != nil {
			return nil, status.Errorf(codes.DeadlineExceeded,
				"old leader %s resigned, but no new leader was decided: %v",
				topoclient.ClusterIDString(oldLeader), err)
		}
		rule := consensus.HighestDecidedShardRule(s.shardConsensusStatuses(ctx, shardRef))
		term := rule.GetRuleNumber().GetCoordinatorTerm()
		if rule == nil || term <= baseline.GetCoordinatorTerm() {
			continue
		}
		if !proto.Equal(rule.GetLeaderId(), oldLeader) {
			return proto.Clone(rule.GetLeaderId()).(*clustermetadatapb.ID), nil
		}
		if baseline != nil {
			return nil, status.Errorf(codes.Aborted,
				"switchover did not move leadership: %s was named leader again at term %d",
				topoclient.ClusterIDString(oldLeader), term)
		}
	}
	return nil, ctx.Err() // unreachable: Attempts only stops after yielding an error
}

// shardConsensusStatuses returns the consensus status of every reachable pooler
// in the shard.
func (s *MultiadminServer) shardConsensusStatuses(ctx context.Context, shardRef *clustermetadatapb.ShardRef) []*clustermetadatapb.ConsensusStatus {
	poolers, err := s.findShardPoolers(ctx, shardRef)
	if err != nil {
		return nil
	}
	var statuses []*clustermetadatapb.ConsensusStatus
	for _, p := range poolers {
		if resp, err := s.rpcClient.Status(ctx, p, &multipoolermanagerdatapb.StatusRequest{}); err == nil {
			statuses = append(statuses, resp.GetConsensusStatus())
		}
	}
	return statuses
}

// findShardPoolers returns the poolers multigres manages for the given shard
// across all cells. An unmanaged pooler's leadership is decided outside
// multigres, so a switchover must never pick it as the leader to resign.
func (s *MultiadminServer) findShardPoolers(ctx context.Context, shardRef *clustermetadatapb.ShardRef) ([]*clustermetadatapb.Multipooler, error) {
	cellNames, err := s.ts.GetCellNames(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to list cells: %v", err)
	}

	var poolers []*clustermetadatapb.Multipooler
	for _, cell := range cellNames {
		opts := &topoclient.GetMultipoolersByCellOptions{
			DatabaseShard: &topoclient.DatabaseShard{
				Database:   shardRef.GetDatabase(),
				TableGroup: shardRef.GetTableGroup(),
				Shard:      shardRef.GetShard(),
			},
		}
		infos, err := s.ts.GetMultipoolersByCell(ctx, cell, opts)
		if err != nil {
			s.logger.WarnContext(ctx, "failed to list poolers in cell", "cell", cell, "error", err)
			continue
		}
		for _, info := range infos {
			if info.Multipooler != nil && topoclient.IsManagedPooler(info.Multipooler) {
				poolers = append(poolers, info.Multipooler)
			}
		}
	}

	if len(poolers) == 0 {
		return nil, status.Errorf(codes.NotFound, "no managed poolers found for shard %s/%s/%s",
			shardRef.GetDatabase(), shardRef.GetTableGroup(), shardRef.GetShard())
	}
	return poolers, nil
}
