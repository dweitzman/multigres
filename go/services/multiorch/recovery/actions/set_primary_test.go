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

package actions

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/rpcclient"
	"github.com/multigres/multigres/go/services/multiorch/recovery/types"
	"github.com/multigres/multigres/go/services/multiorch/store"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multiorchdatapb "github.com/multigres/multigres/go/pb/multiorchdata"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
)

const setPrimaryTestPrimaryKey = "multipooler-cell1-primary"
const setPrimaryTestReplicaKey = "multipooler-cell1-replica1"

func setPrimaryTestShardKey() *clustermetadatapb.ShardKey {
	return &clustermetadatapb.ShardKey{Database: "testdb", TableGroup: "default", Shard: "0"}
}

// setPrimaryFixture seeds a store with a consensus leader ("primary") and a
// replica ("replica1"), plus the leader's live Status response so
// FindHealthyPrimary's health check passes. It returns the store, a fake client,
// and the replica's ID.
func setPrimaryFixture() (*store.PoolerStore, *rpcclient.FakeClient, *clustermetadatapb.ID) {
	leaderID := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "cell1", Name: "primary"}
	replicaID := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "cell1", Name: "replica1"}

	leaderRule := &clustermetadatapb.ShardRule{
		RuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: 1},
		LeaderId:   leaderID,
	}
	leaderCS := &clustermetadatapb.ConsensusStatus{
		Id:              leaderID,
		CurrentPosition: &clustermetadatapb.PoolerPosition{Rule: leaderRule},
	}

	fakeClient := &rpcclient.FakeClient{
		StatusResponses: map[string]*rpcclient.ResponseWithDelay[*multipoolermanagerdatapb.StatusResponse]{
			setPrimaryTestPrimaryKey: {
				Response: &multipoolermanagerdatapb.StatusResponse{ConsensusStatus: leaderCS},
			},
		},
	}
	poolerStore := store.NewPoolerStore(fakeClient, slog.Default())

	poolerStore.Set(setPrimaryTestReplicaKey, &multiorchdatapb.PoolerHealthState{
		MultiPooler: &clustermetadatapb.MultiPooler{Id: replicaID, ShardKey: setPrimaryTestShardKey()},
	})
	poolerStore.Set(setPrimaryTestPrimaryKey, &multiorchdatapb.PoolerHealthState{
		MultiPooler: &clustermetadatapb.MultiPooler{
			Id:       leaderID,
			ShardKey: setPrimaryTestShardKey(),
			Hostname: "primary.example.com",
			PortMap:  map[string]int32{"postgres": 5432},
		},
		ConsensusStatus: leaderCS,
	})

	return poolerStore, fakeClient, replicaID
}

func TestSetPrimaryAction_Metadata(t *testing.T) {
	action := NewSetPrimaryAction(nil, nil, slog.Default())
	metadata := action.Metadata()
	assert.Equal(t, "SetPrimary", metadata.Name)
	assert.Equal(t, 45*time.Second, metadata.Timeout)
}

func TestSetPrimaryAction_RequiresHealthyLeader(t *testing.T) {
	action := NewSetPrimaryAction(nil, nil, slog.Default())
	assert.True(t, action.RequiresHealthyLeader())
}

func TestSetPrimaryAction_Priority(t *testing.T) {
	action := NewSetPrimaryAction(nil, nil, slog.Default())
	assert.Equal(t, types.PriorityHigh, action.Priority())
}

func TestSetPrimaryAction_GracePeriod(t *testing.T) {
	action := NewSetPrimaryAction(nil, nil, slog.Default())
	assert.Nil(t, action.GracePeriod())
}

func TestSetPrimaryAction_ExecutePoolerNotFound(t *testing.T) {
	ctx := context.Background()
	fakeClient := &rpcclient.FakeClient{}
	poolerStore := store.NewPoolerStore(fakeClient, slog.Default())

	action := NewSetPrimaryAction(fakeClient, poolerStore, slog.Default())
	err := action.Execute(ctx, types.Problem{
		Code:     types.ProblemNeedsSetPrimary,
		ShardKey: setPrimaryTestShardKey(),
		PoolerID: &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "cell1", Name: "replica1"},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to find affected pooler")
}

func TestSetPrimaryAction_ExecuteNoPrimary(t *testing.T) {
	ctx := context.Background()
	fakeClient := &rpcclient.FakeClient{}
	poolerStore := store.NewPoolerStore(fakeClient, slog.Default())

	replicaID := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "cell1", Name: "replica1"}
	poolerStore.Set(setPrimaryTestReplicaKey, &multiorchdatapb.PoolerHealthState{
		MultiPooler: &clustermetadatapb.MultiPooler{Id: replicaID, ShardKey: setPrimaryTestShardKey()},
	})

	action := NewSetPrimaryAction(fakeClient, poolerStore, slog.Default())
	err := action.Execute(ctx, types.Problem{
		Code:     types.ProblemNeedsSetPrimary,
		ShardKey: setPrimaryTestShardKey(),
		PoolerID: replicaID,
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to find primary")
}

func TestSetPrimaryAction_ExecuteSuccess(t *testing.T) {
	ctx := context.Background()
	poolerStore, fakeClient, replicaID := setPrimaryFixture()

	action := NewSetPrimaryAction(fakeClient, poolerStore, slog.Default())
	err := action.Execute(ctx, types.Problem{
		Code:     types.ProblemNeedsSetPrimary,
		ShardKey: setPrimaryTestShardKey(),
		PoolerID: replicaID,
	})
	require.NoError(t, err)

	// SetPrimary issued against the replica, carrying the leader's contact info
	// and rule.
	assert.Contains(t, fakeClient.GetCallLog(), "SetPrimary("+setPrimaryTestReplicaKey+")")
	req := fakeClient.SetPrimaryRequests[setPrimaryTestReplicaKey]
	require.NotNil(t, req)
	require.NotNil(t, req.Leader)
	assert.Equal(t, "primary", req.Leader.Id.Name)
	assert.Equal(t, "primary.example.com", req.Leader.GetHost())
	require.NotNil(t, req.Rule)
	assert.Equal(t, int64(1), req.Rule.GetRuleNumber().GetCoordinatorTerm())
}

func TestSetPrimaryAction_ExecuteSetPrimaryRPCError(t *testing.T) {
	ctx := context.Background()
	poolerStore, fakeClient, replicaID := setPrimaryFixture()
	rpcErr := errors.New("boom")
	fakeClient.Errors = map[string]error{setPrimaryTestReplicaKey: rpcErr}

	action := NewSetPrimaryAction(fakeClient, poolerStore, slog.Default())
	err := action.Execute(ctx, types.Problem{
		Code:     types.ProblemNeedsSetPrimary,
		ShardKey: setPrimaryTestShardKey(),
		PoolerID: replicaID,
	})
	require.Error(t, err)
	assert.ErrorIs(t, err, rpcErr)
	assert.Contains(t, err.Error(), "SetPrimary RPC failed")
}
