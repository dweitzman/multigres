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

package actions

import (
	"context"
	"errors"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/rpcclient"
	"github.com/multigres/multigres/go/common/topoclient/memorytopo"
	"github.com/multigres/multigres/go/services/multiorch/config"
	"github.com/multigres/multigres/go/services/multiorch/recovery/types"
	"github.com/multigres/multigres/go/services/multiorch/store"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	consensusdatapb "github.com/multigres/multigres/go/pb/consensusdata"
	multiorchdatapb "github.com/multigres/multigres/go/pb/multiorchdata"
)

func TestSetReplicationPrimaryAction_Metadata(t *testing.T) {
	action := NewSetReplicationPrimaryAction(config.NewTestConfig(), nil, nil, nil, slog.Default())
	md := action.Metadata()
	assert.Equal(t, "SetReplicationPrimary", md.Name)
	assert.True(t, md.Retryable)
}

func TestSetReplicationPrimaryAction_RequiresHealthyLeader(t *testing.T) {
	action := NewSetReplicationPrimaryAction(config.NewTestConfig(), nil, nil, nil, slog.Default())
	assert.True(t, action.RequiresHealthyLeader())
}

func TestSetReplicationPrimaryAction_Priority(t *testing.T) {
	action := NewSetReplicationPrimaryAction(config.NewTestConfig(), nil, nil, nil, slog.Default())
	assert.Equal(t, types.PriorityHigh, action.Priority())
}

// shardKey + IDs shared across the Execute tests.
var (
	srpShardKey  = &clustermetadatapb.ShardKey{Database: "db", TableGroup: "tg", Shard: "0"}
	srpLeaderID  = &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "cell1", Name: "leader"}
	srpReplicaID = &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "cell1", Name: "replica"}
)

// makeLeaderHealthState builds a PoolerHealthState representing the cluster
// leader. ShardLeader uses CurrentLeadership to identify it; the current
// position rule is what SetTermPrimary will carry in its request.
func makeLeaderHealthState(t *testing.T, term int64) *multiorchdatapb.PoolerHealthState {
	t.Helper()
	rule := &clustermetadatapb.ShardRule{
		RuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: term},
		LeaderId:   srpLeaderID,
	}
	return &multiorchdatapb.PoolerHealthState{
		MultiPooler: &clustermetadatapb.MultiPooler{
			Id:       srpLeaderID,
			ShardKey: srpShardKey,
			Type:     clustermetadatapb.PoolerType_PRIMARY,
			Hostname: "leader.example.com",
			PortMap:  map[string]int32{"postgres": 5432},
			CurrentLeadership: &clustermetadatapb.LeaderObservation{
				LeaderId:         srpLeaderID,
				LeaderRuleNumber: rule.RuleNumber,
			},
		},
		IsLastCheckValid: true,
		ConsensusStatus: &clustermetadatapb.ConsensusStatus{
			CurrentPosition: &clustermetadatapb.PoolerPosition{Rule: rule},
			ReplicationPrimary: &clustermetadatapb.ReplicationPrimary{
				Rule: rule,
			},
		},
	}
}

func makeReplicaHealthState() *multiorchdatapb.PoolerHealthState {
	return &multiorchdatapb.PoolerHealthState{
		MultiPooler: &clustermetadatapb.MultiPooler{
			Id:       srpReplicaID,
			ShardKey: srpShardKey,
			Type:     clustermetadatapb.PoolerType_REPLICA,
		},
		IsLastCheckValid: true,
	}
}

// newSetReplicationPrimaryTest spins up the test fixture and returns the
// configured action plus the fake client (for asserting RPC behaviour).
func newSetReplicationPrimaryTest(t *testing.T) (*SetReplicationPrimaryAction, *rpcclient.FakeClient, *store.PoolerStore) {
	t.Helper()
	ctx := context.Background()
	ts, _ := memorytopo.NewServerAndFactory(ctx, "cell1")
	t.Cleanup(func() { _ = ts.Close() })

	fake := rpcclient.NewFakeClient()
	ps := store.NewPoolerStore(fake, slog.Default())
	action := NewSetReplicationPrimaryAction(config.NewTestConfig(), fake, ps, ts, slog.Default())
	return action, fake, ps
}

func TestSetReplicationPrimaryAction_Execute_UnsupportedProblemCode(t *testing.T) {
	action, _, _ := newSetReplicationPrimaryTest(t)
	err := action.Execute(context.Background(), types.Problem{
		Code:     types.ProblemReplicaLagging,
		ShardKey: srpShardKey,
		PoolerID: srpReplicaID,
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported problem code")
}

func TestSetReplicationPrimaryAction_Execute_TargetNotInStore(t *testing.T) {
	action, _, _ := newSetReplicationPrimaryTest(t)
	err := action.Execute(context.Background(), types.Problem{
		Code:     types.ProblemReplicaNotReplicating,
		ShardKey: srpShardKey,
		PoolerID: srpReplicaID,
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to find affected pooler")
}

func TestSetReplicationPrimaryAction_Execute_NoConsensusLeader(t *testing.T) {
	action, _, ps := newSetReplicationPrimaryTest(t)
	// Replica is present but no pooler publishes CurrentLeadership or
	// ConsensusStatus.ReplicationPrimary, so ShardLeader returns nil.
	ps.Set("multipooler-cell1-replica", makeReplicaHealthState())
	err := action.Execute(context.Background(), types.Problem{
		Code:     types.ProblemReplicaNotReplicating,
		ShardKey: srpShardKey,
		PoolerID: srpReplicaID,
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no consensus leader observed")
}

func TestSetReplicationPrimaryAction_Execute_ReplicaPath_Success(t *testing.T) {
	action, fake, ps := newSetReplicationPrimaryTest(t)
	ps.Set("multipooler-cell1-leader", makeLeaderHealthState(t, 1))
	ps.Set("multipooler-cell1-replica", makeReplicaHealthState())
	fake.SetTermPrimaryResponses = map[string]*consensusdatapb.SetTermPrimaryResponse{
		"multipooler-cell1-replica": {},
	}

	err := action.Execute(context.Background(), types.Problem{
		Code:     types.ProblemReplicaNotReplicating,
		ShardKey: srpShardKey,
		PoolerID: srpReplicaID,
	})
	require.NoError(t, err)
	assert.Contains(t, fake.CallLog, "SetTermPrimary(multipooler-cell1-replica)")

	// Request carries the leader's address + rule and does NOT set
	// AwaitStreaming — this action is fire-and-forget by design.
	req := fake.SetTermPrimaryRequests["multipooler-cell1-replica"]
	require.NotNil(t, req)
	assert.Equal(t, srpLeaderID.Name, req.GetLeader().GetId().GetName())
	assert.Equal(t, int64(1), req.GetRule().GetRuleNumber().GetCoordinatorTerm())
	assert.False(t, req.GetAwaitStreaming())
}

func TestSetReplicationPrimaryAction_Execute_StaleLeaderPath_Success(t *testing.T) {
	action, fake, ps := newSetReplicationPrimaryTest(t)
	ps.Set("multipooler-cell1-leader", makeLeaderHealthState(t, 2))
	// "replica" is actually the stale primary in this scenario — the
	// pooler-side handler will demote it. From orch's view the
	// distinction is just the problem code.
	ps.Set("multipooler-cell1-replica", makeReplicaHealthState())
	fake.SetTermPrimaryResponses = map[string]*consensusdatapb.SetTermPrimaryResponse{
		"multipooler-cell1-replica": {},
	}

	err := action.Execute(context.Background(), types.Problem{
		Code:     types.ProblemStaleLeader,
		ShardKey: srpShardKey,
		PoolerID: srpReplicaID,
	})
	require.NoError(t, err)
	assert.Contains(t, fake.CallLog, "SetTermPrimary(multipooler-cell1-replica)")
}

func TestSetReplicationPrimaryAction_Execute_RPCFailure(t *testing.T) {
	action, fake, ps := newSetReplicationPrimaryTest(t)
	ps.Set("multipooler-cell1-leader", makeLeaderHealthState(t, 1))
	ps.Set("multipooler-cell1-replica", makeReplicaHealthState())
	fake.Errors["multipooler-cell1-replica"] = errors.New("connection refused")

	err := action.Execute(context.Background(), types.Problem{
		Code:     types.ProblemReplicaNotReplicating,
		ShardKey: srpShardKey,
		PoolerID: srpReplicaID,
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "SetTermPrimary RPC failed")
}

func TestSetReplicationPrimaryAction_Execute_GracePeriodReturnsNil(t *testing.T) {
	action := NewSetReplicationPrimaryAction(config.NewTestConfig(), nil, nil, nil, slog.Default())
	assert.Nil(t, action.GracePeriod())
}
