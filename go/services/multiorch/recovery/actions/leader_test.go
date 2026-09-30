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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/multigres/multigres/go/common/rpcclient"
	"github.com/multigres/multigres/go/services/multiorch/store"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multiorchdatapb "github.com/multigres/multigres/go/pb/multiorchdata"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
)

func TestRefreshHealthNow(t *testing.T) {
	ctx := context.Background()
	leaderID := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "cell1", Name: "primary"}

	newPooler := func() *store.Pooler {
		return store.NewPooler(&multiorchdatapb.PoolerHealthState{
			Multipooler: &clustermetadatapb.Multipooler{Id: leaderID, Type: clustermetadatapb.PoolerType_PRIMARY},
		}, nil)
	}

	t.Run("writes the fresh status into the cache on success", func(t *testing.T) {
		pooler := newPooler()
		servingStatus := &clustermetadatapb.ConsensusStatus{Id: leaderID}
		fakeClient := rpcclient.NewFakeClient()
		fakeClient.SetStatusResponse("multipooler-cell1-primary", &multipoolermanagerdatapb.StatusResponse{
			ConsensusStatus: servingStatus,
			Status: &multipoolermanagerdatapb.Status{
				PostgresReady:  true,
				PostgresStatus: multipoolermanagerdatapb.PostgresStatus_POSTGRES_STATUS_STANDBY,
			},
		})

		err := RefreshHealthNow(ctx, fakeClient, pooler)

		require.NoError(t, err)
		health := pooler.Health()
		assert.True(t, health.GetStatus().GetPostgresReady())
		assert.Equal(t, multipoolermanagerdatapb.PostgresStatus_POSTGRES_STATUS_STANDBY, health.GetStatus().GetPostgresStatus())
		assert.True(t, proto.Equal(health.GetConsensusStatus(), servingStatus))
		assert.NotNil(t, health.GetLastSeen())
		assert.NotNil(t, health.GetLastPostgresReadyTime())
		// Identity fields the Status RPC doesn't carry must survive untouched.
		assert.Equal(t, "primary", health.GetMultipooler().GetId().GetName())
	})

	t.Run("leaves LastPostgresReadyTime alone when postgres is not ready", func(t *testing.T) {
		pooler := newPooler()
		fakeClient := rpcclient.NewFakeClient()
		fakeClient.SetStatusResponse("multipooler-cell1-primary", &multipoolermanagerdatapb.StatusResponse{
			Status: &multipoolermanagerdatapb.Status{PostgresReady: false},
		})

		err := RefreshHealthNow(ctx, fakeClient, pooler)

		require.NoError(t, err)
		assert.False(t, pooler.Health().GetStatus().GetPostgresReady())
		assert.Nil(t, pooler.Health().GetLastPostgresReadyTime())
	})

	t.Run("leaves the cache untouched when the pooler is unreachable", func(t *testing.T) {
		pooler := newPooler()
		fakeClient := rpcclient.NewFakeClient()
		fakeClient.Errors["multipooler-cell1-primary"] = errors.New("connection refused")

		err := RefreshHealthNow(ctx, fakeClient, pooler)

		require.Error(t, err)
		assert.Contains(t, err.Error(), "unreachable during health refresh")
		assert.Nil(t, pooler.Health().GetStatus())
		assert.Nil(t, pooler.Health().GetLastSeen())
	})
}
