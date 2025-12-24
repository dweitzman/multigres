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

package poolergateway

import (
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/common/topoclient"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/pb/query"
)

// poolerID returns the expected ID format for a pooler
func poolerID(pooler *clustermetadatapb.MultiPooler) string {
	return topoclient.MultiPoolerIDString(pooler.Id)
}

func createTestMultiPooler(name, cell, tableGroup, shard string, poolerType clustermetadatapb.PoolerType) *clustermetadatapb.MultiPooler {
	return &clustermetadatapb.MultiPooler{
		Id: &clustermetadatapb.ID{
			Component: clustermetadatapb.ID_MULTIPOOLER,
			Cell:      cell,
			Name:      name,
		},
		Hostname:   name + ".example.com",
		TableGroup: tableGroup,
		Shard:      shard,
		Type:       poolerType,
		PortMap: map[string]int32{
			"grpc": 50051,
		},
	}
}

func TestLoadBalancer_AddRemovePooler(t *testing.T) {
	logger := slog.Default()
	lb := NewLoadBalancer("zone1", logger)

	// Initially empty
	assert.Equal(t, 0, lb.ConnectionCount())

	// Add a pooler
	pooler := createTestMultiPooler("pooler1", "zone1", constants.DefaultTableGroup, "0", clustermetadatapb.PoolerType_PRIMARY)
	err := lb.AddPooler(pooler)
	require.NoError(t, err)
	assert.Equal(t, 1, lb.ConnectionCount())

	// Adding same pooler again is a no-op
	err = lb.AddPooler(pooler)
	require.NoError(t, err)
	assert.Equal(t, 1, lb.ConnectionCount())

	// Remove the pooler
	lb.RemovePooler("zone1/pooler1")
	assert.Equal(t, 0, lb.ConnectionCount())

	// Removing non-existent pooler is a no-op
	lb.RemovePooler("zone1/nonexistent")
	assert.Equal(t, 0, lb.ConnectionCount())
}

func TestLoadBalancer_GetConnection_Primary(t *testing.T) {
	logger := slog.Default()
	lb := NewLoadBalancer("zone1", logger)

	// Add a primary
	primary := createTestMultiPooler("primary1", "zone1", constants.DefaultTableGroup, "0", clustermetadatapb.PoolerType_PRIMARY)
	require.NoError(t, lb.AddPooler(primary))

	// Should find the primary
	target := &query.Target{
		TableGroup: constants.DefaultTableGroup,
		PoolerType: clustermetadatapb.PoolerType_PRIMARY,
	}
	conn, err := lb.GetConnection(target, nil)
	require.NoError(t, err)
	assert.Equal(t, poolerID(primary), conn.ID())
}

func TestLoadBalancer_GetConnection_ReplicaPreferLocalCell(t *testing.T) {
	logger := slog.Default()
	lb := NewLoadBalancer("zone1", logger)

	// Add replicas in both cells
	localReplica := createTestMultiPooler("local-replica", "zone1", constants.DefaultTableGroup, "0", clustermetadatapb.PoolerType_REPLICA)
	remoteReplica := createTestMultiPooler("remote-replica", "zone2", constants.DefaultTableGroup, "0", clustermetadatapb.PoolerType_REPLICA)
	require.NoError(t, lb.AddPooler(localReplica))
	require.NoError(t, lb.AddPooler(remoteReplica))

	// Should prefer local cell for replicas
	target := &query.Target{
		TableGroup: constants.DefaultTableGroup,
		PoolerType: clustermetadatapb.PoolerType_REPLICA,
	}
	conn, err := lb.GetConnection(target, nil)
	require.NoError(t, err)
	assert.Equal(t, poolerID(localReplica), conn.ID(), "Should prefer local cell for replicas")
}

func TestLoadBalancer_GetConnection_CrossCellPrimary(t *testing.T) {
	logger := slog.Default()
	lb := NewLoadBalancer("zone1", logger)

	// Add primary only in remote cell
	remotePrimary := createTestMultiPooler("remote-primary", "zone2", constants.DefaultTableGroup, "0", clustermetadatapb.PoolerType_PRIMARY)
	require.NoError(t, lb.AddPooler(remotePrimary))

	// Should find primary in remote cell
	target := &query.Target{
		TableGroup: constants.DefaultTableGroup,
		PoolerType: clustermetadatapb.PoolerType_PRIMARY,
	}
	conn, err := lb.GetConnection(target, nil)
	require.NoError(t, err)
	assert.Equal(t, poolerID(remotePrimary), conn.ID(), "Should find primary in remote cell")
}

func TestLoadBalancer_GetConnection_NoMatch(t *testing.T) {
	logger := slog.Default()
	lb := NewLoadBalancer("zone1", logger)

	// Add a primary
	primary := createTestMultiPooler("primary1", "zone1", constants.DefaultTableGroup, "0", clustermetadatapb.PoolerType_PRIMARY)
	require.NoError(t, lb.AddPooler(primary))

	// Request a replica - should not find one
	target := &query.Target{
		TableGroup: constants.DefaultTableGroup,
		PoolerType: clustermetadatapb.PoolerType_REPLICA,
	}
	_, err := lb.GetConnection(target, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no pooler found")
}

func TestLoadBalancer_GetConnection_ShardMatch(t *testing.T) {
	logger := slog.Default()
	lb := NewLoadBalancer("zone1", logger)

	// Add primaries for different shards
	shard0 := createTestMultiPooler("primary-shard0", "zone1", constants.DefaultTableGroup, "0", clustermetadatapb.PoolerType_PRIMARY)
	shard1 := createTestMultiPooler("primary-shard1", "zone1", constants.DefaultTableGroup, "1", clustermetadatapb.PoolerType_PRIMARY)
	require.NoError(t, lb.AddPooler(shard0))
	require.NoError(t, lb.AddPooler(shard1))

	// Request specific shard
	target := &query.Target{
		TableGroup: constants.DefaultTableGroup,
		Shard:      "1",
		PoolerType: clustermetadatapb.PoolerType_PRIMARY,
	}
	conn, err := lb.GetConnection(target, nil)
	require.NoError(t, err)
	assert.Equal(t, poolerID(shard1), conn.ID())
}

func TestLoadBalancer_GetConnection_ExcludePoolers(t *testing.T) {
	logger := slog.Default()
	lb := NewLoadBalancer("zone1", logger)

	// Add two replicas
	replica1 := createTestMultiPooler("replica1", "zone1", constants.DefaultTableGroup, "0", clustermetadatapb.PoolerType_REPLICA)
	replica2 := createTestMultiPooler("replica2", "zone1", constants.DefaultTableGroup, "0", clustermetadatapb.PoolerType_REPLICA)
	require.NoError(t, lb.AddPooler(replica1))
	require.NoError(t, lb.AddPooler(replica2))

	target := &query.Target{
		TableGroup: constants.DefaultTableGroup,
		PoolerType: clustermetadatapb.PoolerType_REPLICA,
	}

	// First request returns one of them
	conn1, err := lb.GetConnection(target, nil)
	require.NoError(t, err)

	// Exclude the first one, should get the other
	opts := &GetConnectionOptions{
		ExcludePoolers: []string{conn1.ID()},
	}
	conn2, err := lb.GetConnection(target, opts)
	require.NoError(t, err)
	assert.NotEqual(t, conn1.ID(), conn2.ID(), "Should return different pooler when first is excluded")

	// Exclude both, should error
	opts.ExcludePoolers = []string{poolerID(replica1), poolerID(replica2)}
	_, err = lb.GetConnection(target, opts)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no pooler found")
}

func TestLoadBalancer_GetConnection_DefaultsToPrimary(t *testing.T) {
	logger := slog.Default()
	lb := NewLoadBalancer("zone1", logger)

	// Add both primary and replica
	primary := createTestMultiPooler("primary1", "zone1", constants.DefaultTableGroup, "0", clustermetadatapb.PoolerType_PRIMARY)
	replica := createTestMultiPooler("replica1", "zone1", constants.DefaultTableGroup, "0", clustermetadatapb.PoolerType_REPLICA)
	require.NoError(t, lb.AddPooler(primary))
	require.NoError(t, lb.AddPooler(replica))

	// Request with UNKNOWN type should default to PRIMARY
	target := &query.Target{
		TableGroup: constants.DefaultTableGroup,
		PoolerType: clustermetadatapb.PoolerType_UNKNOWN,
	}
	conn, err := lb.GetConnection(target, nil)
	require.NoError(t, err)
	assert.Equal(t, clustermetadatapb.PoolerType_PRIMARY, conn.Type(), "Should default to PRIMARY")
}

func TestLoadBalancer_Close(t *testing.T) {
	logger := slog.Default()
	lb := NewLoadBalancer("zone1", logger)

	// Add some poolers
	pooler1 := createTestMultiPooler("pooler1", "zone1", constants.DefaultTableGroup, "0", clustermetadatapb.PoolerType_PRIMARY)
	pooler2 := createTestMultiPooler("pooler2", "zone1", constants.DefaultTableGroup, "0", clustermetadatapb.PoolerType_REPLICA)
	require.NoError(t, lb.AddPooler(pooler1))
	require.NoError(t, lb.AddPooler(pooler2))
	assert.Equal(t, 2, lb.ConnectionCount())

	// Close should remove all connections
	err := lb.Close()
	require.NoError(t, err)
	assert.Equal(t, 0, lb.ConnectionCount())
}

func TestLoadBalancerListener(t *testing.T) {
	logger := slog.Default()
	lb := NewLoadBalancer("zone1", logger)
	listener := NewLoadBalancerListener(lb)

	// OnPoolerChanged should add pooler
	pooler := createTestMultiPooler("pooler1", "zone1", constants.DefaultTableGroup, "0", clustermetadatapb.PoolerType_PRIMARY)
	listener.OnPoolerChanged(pooler)
	assert.Equal(t, 1, lb.ConnectionCount())

	// OnPoolerRemoved should remove pooler
	listener.OnPoolerRemoved(pooler)
	assert.Equal(t, 0, lb.ConnectionCount())
}
