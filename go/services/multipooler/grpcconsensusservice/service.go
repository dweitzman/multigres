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

// Package grpcconsensusservice implements the gRPC server for consensus operations
package grpcconsensusservice

import (
	"context"
	"time"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/servenv"
	consensuspb "github.com/multigres/multigres/go/pb/consensus"
	consensusdata "github.com/multigres/multigres/go/pb/consensusdata"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
	"github.com/multigres/multigres/go/services/multipooler/manager"
)

// consensusService is the gRPC wrapper for consensus operations
type consensusService struct {
	consensuspb.UnimplementedMultiPoolerConsensusServer
	manager *manager.MultiPoolerManager
}

func RegisterConsensusServices(senv *servenv.ServEnv, grpc *servenv.GrpcServer) {
	// Register ourselves to be invoked when the manager starts
	manager.RegisterPoolerManagerServices = append(manager.RegisterPoolerManagerServices, func(pm *manager.MultiPoolerManager) {
		if grpc.CheckServiceMap("consensus", senv) {
			srv := &consensusService{
				manager: pm,
			}
			consensuspb.RegisterMultiPoolerConsensusServer(grpc.Server, srv)
		}
	})
}

// Recruit handles Phase 1 of the Paxos-inspired consensus protocol.
// The coordinator sends a TermRevocation; this pooler accepts it if the
// revoked_below_term is >= the current value, then freezes its WAL position.
func (s *consensusService) Recruit(ctx context.Context, req *consensusdata.RecruitRequest) (*consensusdata.RecruitResponse, error) {
	resp, err := s.manager.Recruit(ctx, req)
	if err != nil {
		return resp, mterrors.ToGRPC(err)
	}
	return resp, nil
}

// Propose handles Phase 2 of the Paxos-inspired consensus protocol.
// Each pooler self-identifies: if proposal_leader_id == self it promotes to primary;
// otherwise it configures replication to the proposal leader.
func (s *consensusService) Propose(ctx context.Context, req *consensusdata.ProposeRequest) (*consensusdata.ProposeResponse, error) {
	resp, err := s.manager.Propose(ctx, req)
	if err != nil {
		return resp, mterrors.ToGRPC(err)
	}
	return resp, nil
}

// Inform notifies this pooler of a committed shard rule decision.
func (s *consensusService) Inform(ctx context.Context, req *consensusdata.InformRequest) (*consensusdata.InformResponse, error) {
	resp, err := s.manager.Inform(ctx, req)
	if err != nil {
		return nil, mterrors.ToGRPC(err)
	}
	return resp, nil
}

// Status returns the current status of this node
func (s *consensusService) Status(ctx context.Context, req *consensusdata.StatusRequest) (*consensusdata.StatusResponse, error) {
	resp, err := s.manager.ConsensusStatus(ctx, req)
	if err != nil {
		return nil, mterrors.ToGRPC(err)
	}
	return resp, nil
}

// EmergencyDemote demotes the current leader server
func (s *consensusService) EmergencyDemote(ctx context.Context, req *multipoolermanagerdatapb.EmergencyDemoteRequest) (*multipoolermanagerdatapb.EmergencyDemoteResponse, error) {
	drainTimeout := 5 * time.Second
	if req.DrainTimeout != nil {
		drainTimeout = req.DrainTimeout.AsDuration()
	}
	resp, err := s.manager.EmergencyDemote(ctx, req.ConsensusTerm, drainTimeout, req.Force)
	if err != nil {
		return nil, mterrors.ToGRPC(err)
	}
	return resp, nil
}

// DemoteStalePrimary demotes a stale primary that came back after failover
func (s *consensusService) DemoteStalePrimary(ctx context.Context, req *multipoolermanagerdatapb.DemoteStalePrimaryRequest) (*multipoolermanagerdatapb.DemoteStalePrimaryResponse, error) {
	resp, err := s.manager.DemoteStalePrimary(ctx, req.Source, req.ConsensusTerm, req.Force)
	if err != nil {
		return nil, mterrors.ToGRPC(err)
	}
	return resp, nil
}

// UpdateConsensusRule updates the synchronous standby list (quorum membership)
func (s *consensusService) UpdateConsensusRule(ctx context.Context, req *multipoolermanagerdatapb.UpdateSynchronousStandbyListRequest) (*multipoolermanagerdatapb.UpdateSynchronousStandbyListResponse, error) {
	err := s.manager.UpdateSynchronousStandbyList(ctx,
		req.Operation,
		req.StandbyIds,
		req.ReloadConfig,
		req.ConsensusTerm,
		req.Force,
		req.CoordinatorId)
	if err != nil {
		return nil, mterrors.ToGRPC(err)
	}
	return &multipoolermanagerdatapb.UpdateSynchronousStandbyListResponse{}, nil
}

// RewindToSource performs pg_rewind to synchronize this server with a source
func (s *consensusService) RewindToSource(ctx context.Context, req *multipoolermanagerdatapb.RewindToSourceRequest) (*multipoolermanagerdatapb.RewindToSourceResponse, error) {
	resp, err := s.manager.RewindToSource(ctx, req.Source)
	if err != nil {
		return nil, mterrors.ToGRPC(err)
	}
	return resp, nil
}
