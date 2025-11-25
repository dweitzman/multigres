// Copyright 2025 Supabase, Inc.
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

// Package grpcmanagerservice implements the gRPC server for MultiPoolerManager
package grpcmanagerservice

import (
	"context"
	"time"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/multipooler/manager"
	multipoolermanagerpb "github.com/multigres/multigres/go/pb/multipoolermanager"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
	"github.com/multigres/multigres/go/servenv"
)

// managerService is the gRPC wrapper for MultiPoolerManager
type managerService struct {
	multipoolermanagerpb.UnimplementedMultiPoolerManagerServer
	manager *manager.MultiPoolerManager
}

func RegisterPoolerManagerServices(senv *servenv.ServEnv, grpc *servenv.GrpcServer) {
	// Register ourselves to be invoked when the manager starts
	manager.RegisterPoolerManagerServices = append(manager.RegisterPoolerManagerServices, func(pm *manager.MultiPoolerManager) {
		if grpc.CheckServiceMap("poolermanager", senv) {
			srv := &managerService{
				manager: pm,
			}
			multipoolermanagerpb.RegisterMultiPoolerManagerServer(grpc.Server, srv)
		}
	})
}

// WaitForLSN waits for PostgreSQL server to reach a specific LSN position
func (s *managerService) WaitForLSN(ctx context.Context, req *multipoolermanagerdatapb.WaitForLSNRequest) (*multipoolermanagerdatapb.WaitForLSNResponse, error) {
	err := s.manager.WaitForLSN(ctx, req.GetTargetLsn())
	if err != nil {
		return nil, mterrors.ToGRPC(err)
	}
	return &multipoolermanagerdatapb.WaitForLSNResponse{}, nil
}

// SetPrimaryConnInfo sets the primary connection info for a standby server
func (s *managerService) SetPrimaryConnInfo(ctx context.Context, req *multipoolermanagerdatapb.SetPrimaryConnInfoRequest) (*multipoolermanagerdatapb.SetPrimaryConnInfoResponse, error) {
	err := s.manager.SetPrimaryConnInfo(ctx,
		req.GetHost(),
		req.GetPort(),
		req.GetStopReplicationBefore(),
		req.GetStartReplicationAfter(),
		req.GetCurrentTerm(),
		req.GetForce())
	if err != nil {
		return nil, mterrors.ToGRPC(err)
	}
	return &multipoolermanagerdatapb.SetPrimaryConnInfoResponse{}, nil
}

// StartReplication starts WAL replay on standby (calls pg_wal_replay_resume)
func (s *managerService) StartReplication(ctx context.Context, req *multipoolermanagerdatapb.StartReplicationRequest) (*multipoolermanagerdatapb.StartReplicationResponse, error) {
	err := s.manager.StartReplication(ctx)
	if err != nil {
		return nil, mterrors.ToGRPC(err)
	}
	return &multipoolermanagerdatapb.StartReplicationResponse{}, nil
}

// StopReplication stops replication based on the specified mode
func (s *managerService) StopReplication(ctx context.Context, req *multipoolermanagerdatapb.StopReplicationRequest) (*multipoolermanagerdatapb.StopReplicationResponse, error) {
	err := s.manager.StopReplication(ctx, req.GetMode(), req.GetWait())
	if err != nil {
		return nil, mterrors.ToGRPC(err)
	}
	return &multipoolermanagerdatapb.StopReplicationResponse{}, nil
}

// StandbyReplicationStatus gets the current replication status of the standby
func (s *managerService) StandbyReplicationStatus(ctx context.Context, req *multipoolermanagerdatapb.StandbyReplicationStatusRequest) (*multipoolermanagerdatapb.StandbyReplicationStatusResponse, error) {
	status, err := s.manager.StandbyReplicationStatus(ctx)
	if err != nil {
		return nil, mterrors.ToGRPC(err)
	}
	return multipoolermanagerdatapb.StandbyReplicationStatusResponse_builder{
		Status: status,
	}.Build(), nil
}

// Status gets unified status that works for both PRIMARY and REPLICA poolers
func (s *managerService) Status(ctx context.Context, req *multipoolermanagerdatapb.StatusRequest) (*multipoolermanagerdatapb.StatusResponse, error) {
	status, err := s.manager.Status(ctx)
	if err != nil {
		return nil, mterrors.ToGRPC(err)
	}
	return multipoolermanagerdatapb.StatusResponse_builder{
		Status: status,
	}.Build(), nil
}

// ResetReplication resets the standby's connection to its primary
func (s *managerService) ResetReplication(ctx context.Context, req *multipoolermanagerdatapb.ResetReplicationRequest) (*multipoolermanagerdatapb.ResetReplicationResponse, error) {
	err := s.manager.ResetReplication(ctx)
	if err != nil {
		return nil, mterrors.ToGRPC(err)
	}
	return &multipoolermanagerdatapb.ResetReplicationResponse{}, nil
}

// ConfigureSynchronousReplication configures PostgreSQL synchronous replication settings
func (s *managerService) ConfigureSynchronousReplication(ctx context.Context, req *multipoolermanagerdatapb.ConfigureSynchronousReplicationRequest) (*multipoolermanagerdatapb.ConfigureSynchronousReplicationResponse, error) {
	err := s.manager.ConfigureSynchronousReplication(ctx,
		req.GetSynchronousCommit(),
		req.GetSynchronousMethod(),
		req.GetNumSync(),
		req.GetStandbyIds(),
		req.GetReloadConfig())
	if err != nil {
		return nil, mterrors.ToGRPC(err)
	}
	return &multipoolermanagerdatapb.ConfigureSynchronousReplicationResponse{}, nil
}

// UpdateSynchronousStandbyList updates the synchronous standby list
func (s *managerService) UpdateSynchronousStandbyList(ctx context.Context, req *multipoolermanagerdatapb.UpdateSynchronousStandbyListRequest) (*multipoolermanagerdatapb.UpdateSynchronousStandbyListResponse, error) {
	err := s.manager.UpdateSynchronousStandbyList(ctx,
		req.GetOperation(),
		req.GetStandbyIds(),
		req.GetReloadConfig(),
		req.GetConsensusTerm(),
		req.GetForce())
	if err != nil {
		return nil, mterrors.ToGRPC(err)
	}
	return &multipoolermanagerdatapb.UpdateSynchronousStandbyListResponse{}, nil
}

// PrimaryStatus gets the status of the leader server
func (s *managerService) PrimaryStatus(ctx context.Context, req *multipoolermanagerdatapb.PrimaryStatusRequest) (*multipoolermanagerdatapb.PrimaryStatusResponse, error) {
	status, err := s.manager.PrimaryStatus(ctx)
	if err != nil {
		return nil, mterrors.ToGRPC(err)
	}
	return multipoolermanagerdatapb.PrimaryStatusResponse_builder{
		Status: status,
	}.Build(), nil
}

// PrimaryPosition gets the current LSN position of the leader
func (s *managerService) PrimaryPosition(ctx context.Context, req *multipoolermanagerdatapb.PrimaryPositionRequest) (*multipoolermanagerdatapb.PrimaryPositionResponse, error) {
	position, err := s.manager.PrimaryPosition(ctx)
	if err != nil {
		return nil, mterrors.ToGRPC(err)
	}
	return multipoolermanagerdatapb.PrimaryPositionResponse_builder{
		LsnPosition: position,
	}.Build(), nil
}

// StopReplicationAndGetStatus stops PostgreSQL replication and returns the status
func (s *managerService) StopReplicationAndGetStatus(ctx context.Context, req *multipoolermanagerdatapb.StopReplicationAndGetStatusRequest) (*multipoolermanagerdatapb.StopReplicationAndGetStatusResponse, error) {
	status, err := s.manager.StopReplicationAndGetStatus(ctx, req.GetMode(), req.GetWait())
	if err != nil {
		return nil, mterrors.ToGRPC(err)
	}
	return multipoolermanagerdatapb.StopReplicationAndGetStatusResponse_builder{
		Status: status,
	}.Build(), nil
}

// ChangeType changes the pooler type (LEADER/FOLLOWER)
func (s *managerService) ChangeType(ctx context.Context, req *multipoolermanagerdatapb.ChangeTypeRequest) (*multipoolermanagerdatapb.ChangeTypeResponse, error) {
	err := s.manager.ChangeType(ctx, req.GetPoolerType().String())
	if err != nil {
		return nil, mterrors.ToGRPC(err)
	}
	return &multipoolermanagerdatapb.ChangeTypeResponse{}, nil
}

// GetFollowers gets the list of follower servers
func (s *managerService) GetFollowers(ctx context.Context, req *multipoolermanagerdatapb.GetFollowersRequest) (*multipoolermanagerdatapb.GetFollowersResponse, error) {
	response, err := s.manager.GetFollowers(ctx)
	if err != nil {
		return nil, mterrors.ToGRPC(err)
	}
	return response, nil
}

// Demote demotes the current leader server
func (s *managerService) Demote(ctx context.Context, req *multipoolermanagerdatapb.DemoteRequest) (*multipoolermanagerdatapb.DemoteResponse, error) {
	// Default drain timeout if not specified
	drainTimeout := 5 * time.Second
	if req.HasDrainTimeout() {
		drainTimeout = req.GetDrainTimeout().AsDuration()
	}

	resp, err := s.manager.Demote(ctx,
		req.GetConsensusTerm(),
		drainTimeout,
		req.GetForce())
	if err != nil {
		return nil, mterrors.ToGRPC(err)
	}
	return resp, nil
}

// UndoDemote undoes a demotion
func (s *managerService) UndoDemote(ctx context.Context, req *multipoolermanagerdatapb.UndoDemoteRequest) (*multipoolermanagerdatapb.UndoDemoteResponse, error) {
	err := s.manager.UndoDemote(ctx)
	if err != nil {
		return nil, mterrors.ToGRPC(err)
	}
	return &multipoolermanagerdatapb.UndoDemoteResponse{}, nil
}

// Promote promotes a replica to leader (Multigres-level operation)
func (s *managerService) Promote(ctx context.Context, req *multipoolermanagerdatapb.PromoteRequest) (*multipoolermanagerdatapb.PromoteResponse, error) {
	resp, err := s.manager.Promote(ctx,
		req.GetConsensusTerm(),
		req.GetExpectedLsn(),
		req.GetSyncReplicationConfig(),
		req.GetForce())
	if err != nil {
		return nil, mterrors.ToGRPC(err)
	}
	return resp, nil
}

// State gets the current status of the manager
func (s *managerService) State(ctx context.Context, req *multipoolermanagerdatapb.StateRequest) (*multipoolermanagerdatapb.StateResponse, error) {
	return s.manager.State(ctx)
}

// SetTerm sets the consensus term information
func (s *managerService) SetTerm(ctx context.Context, req *multipoolermanagerdatapb.SetTermRequest) (*multipoolermanagerdatapb.SetTermResponse, error) {
	if err := s.manager.SetTerm(ctx, req.GetTerm()); err != nil {
		return nil, mterrors.ToGRPC(err)
	}
	return &multipoolermanagerdatapb.SetTermResponse{}, nil
}

// Backup performs a backup
func (s *managerService) Backup(ctx context.Context, req *multipoolermanagerdatapb.BackupRequest) (*multipoolermanagerdatapb.BackupResponse, error) {
	backupID, err := s.manager.Backup(ctx, req.GetForcePrimary(), req.GetType())
	if err != nil {
		return nil, mterrors.ToGRPC(err)
	}

	return multipoolermanagerdatapb.BackupResponse_builder{
		BackupId: backupID,
	}.Build(), nil
}

// RestoreFromBackup restores from a backup
func (s *managerService) RestoreFromBackup(ctx context.Context, req *multipoolermanagerdatapb.RestoreFromBackupRequest) (*multipoolermanagerdatapb.RestoreFromBackupResponse, error) {
	err := s.manager.RestoreFromBackup(ctx, req.GetBackupId())
	if err != nil {
		return nil, mterrors.ToGRPC(err)
	}

	return &multipoolermanagerdatapb.RestoreFromBackupResponse{}, nil
}

// GetBackups retrieves backup information
func (s *managerService) GetBackups(ctx context.Context, req *multipoolermanagerdatapb.GetBackupsRequest) (*multipoolermanagerdatapb.GetBackupsResponse, error) {
	backups, err := s.manager.GetBackups(ctx, req.GetLimit())
	if err != nil {
		return nil, mterrors.ToGRPC(err)
	}

	return multipoolermanagerdatapb.GetBackupsResponse_builder{
		Backups: backups,
	}.Build(), nil
}

// InitializationStatus returns the initialization status of this pooler
func (s *managerService) InitializationStatus(ctx context.Context, req *multipoolermanagerdatapb.InitializationStatusRequest) (*multipoolermanagerdatapb.InitializationStatusResponse, error) {
	resp, err := s.manager.InitializationStatus(ctx, req)
	if err != nil {
		return nil, mterrors.ToGRPC(err)
	}
	return resp, nil
}

// InitializeEmptyPrimary initializes an empty PostgreSQL instance as a primary
func (s *managerService) InitializeEmptyPrimary(ctx context.Context, req *multipoolermanagerdatapb.InitializeEmptyPrimaryRequest) (*multipoolermanagerdatapb.InitializeEmptyPrimaryResponse, error) {
	resp, err := s.manager.InitializeEmptyPrimary(ctx, req)
	if err != nil {
		return nil, mterrors.ToGRPC(err)
	}
	return resp, nil
}

// InitializeAsStandby initializes an empty PostgreSQL instance as a standby
func (s *managerService) InitializeAsStandby(ctx context.Context, req *multipoolermanagerdatapb.InitializeAsStandbyRequest) (*multipoolermanagerdatapb.InitializeAsStandbyResponse, error) {
	resp, err := s.manager.InitializeAsStandby(ctx, req)
	if err != nil {
		return nil, mterrors.ToGRPC(err)
	}
	return resp, nil
}

// CreateDurabilityPolicy creates a new durability policy in the local database
func (s *managerService) CreateDurabilityPolicy(ctx context.Context, req *multipoolermanagerdatapb.CreateDurabilityPolicyRequest) (*multipoolermanagerdatapb.CreateDurabilityPolicyResponse, error) {
	resp, err := s.manager.CreateDurabilityPolicy(ctx, req)
	if err != nil {
		return nil, mterrors.ToGRPC(err)
	}
	return resp, nil
}
