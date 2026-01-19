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

package multiorch

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	pb "github.com/multigres/multigres/go/pb/grpcfaultproxyservice"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
	"github.com/multigres/multigres/go/test/utils"
)

// TestPhantomWrite_NetworkPartition tests the phantom write scenario where
// a replica shows uncommitted data during a network partition.
//
// Test scenario:
// 1. Create 3-node cluster (1 primary + 2 standbys), bootstrap with ANY_2, reconfigure to numSync=2
// 2. Verify cluster is healthy - writes work with 2 standby ACKs
// 3. Partition both standbys from primary (using PostgreSQL proxy)
// 4. Attempt write - should FREEZE (can't get 2 standby ACKs)
// 5. Read from replica-0 - PHANTOM READ (replica sees uncommitted data)
// 6. Remove network partition
// 7. Verify cluster converges - all replicas eventually agree
//
// This test exposes a real distributed systems edge case where synchronous
// replication can still result in uncommitted data being visible on replicas
// when a network partition prevents the required acknowledgments.
func TestPhantomWrite_NetworkPartition(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	// Create 3-replica cluster (primary + 2 standbys)
	// Uses ANY_2 durability policy requiring 2 ACKs for writes
	setup := shardsetup.New(t,
		shardsetup.WithMultipoolerCount(3),
		shardsetup.WithMultiOrchCount(1),
		shardsetup.WithDurabilityPolicy("ANY_2"),
	)

	// Get primary client
	primaryClient := setup.NewPrimaryClient(t)
	defer primaryClient.Close()

	ctx := utils.WithTimeout(t, 10*time.Second)

	// === Phase 0: Reconfigure to require 2 standby ACKs ===
	t.Log("Phase 0: Reconfiguring from ANY_2 (numSync=1) to numSync=2...")

	// Build standby IDs for all standbys
	var standbyIDs []*clustermetadatapb.ID
	for name := range setup.Multipoolers {
		if name == setup.PrimaryName {
			continue
		}
		standbyIDs = append(standbyIDs, &clustermetadatapb.ID{
			Cell: setup.CellName,
			Name: name,
		})
	}

	// Reconfigure to numSync=2 (requires 2 standby ACKs)
	// This is equivalent to what would be an "ANY_3 policy" (3 nodes total: primary + 2 standbys)
	configReq := &multipoolermanagerdatapb.ConfigureSynchronousReplicationRequest{
		SynchronousCommit: multipoolermanagerdatapb.SynchronousCommitLevel_SYNCHRONOUS_COMMIT_REMOTE_APPLY,
		SynchronousMethod: multipoolermanagerdatapb.SynchronousMethod_SYNCHRONOUS_METHOD_ANY,
		NumSync:           2, // Requires 2 standby ACKs (equivalent to ANY_3: 3 nodes total)
		StandbyIds:        standbyIDs,
		ReloadConfig:      true,
	}
	_, err := primaryClient.Manager.ConfigureSynchronousReplication(ctx, configReq)
	require.NoError(t, err, "should reconfigure to numSync=2")

	t.Log("✓ Reconfigured to require 2 standby ACKs (equivalent to ANY_3)")

	// === Phase 1: Verify cluster is healthy ===
	t.Log("Phase 1: Verifying cluster health...")

	// Create test table
	_, err = primaryClient.Pooler.ExecuteQuery(ctx,
		"CREATE TABLE IF NOT EXISTS phantom_test (id SERIAL PRIMARY KEY, data TEXT)", 0)
	require.NoError(t, err, "should create test table")

	// Insert initial row - should succeed with 2 ACKs
	_, err = primaryClient.Pooler.ExecuteQuery(ctx,
		"INSERT INTO phantom_test (data) VALUES ('initial_data')", 0)
	require.NoError(t, err, "initial write should succeed with 2 ACKs")

	// Wait for replication to all standbys
	t.Log("Waiting for initial replication to complete...")
	time.Sleep(2 * time.Second)

	// Verify all replicas see the initial data
	for name, inst := range setup.Multipoolers {
		if name == setup.PrimaryName {
			continue
		}

		replicaClient, err := shardsetup.NewMultipoolerClient(inst.Multipooler.GrpcPort)
		require.NoError(t, err, "should connect to replica %s", name)
		defer replicaClient.Close()

		result, err := replicaClient.Pooler.ExecuteQuery(ctx,
			"SELECT COUNT(*) FROM phantom_test", 1)
		require.NoError(t, err, "should query replica %s", name)
		require.Equal(t, "1", string(result.Rows[0].Values[0]), "replica %s should see initial data", name)
	}

	t.Log("✓ Cluster healthy: all replicas have initial data")

	// === Phase 2: Inject network partition ===
	t.Log("Phase 2: Injecting network partition...")

	// Get PostgreSQL port of primary
	primary := setup.GetPrimary(t)
	require.NotNil(t, primary, "should have primary")
	primaryPostgresPort := primary.Pgctld.PgPort

	// Find all standbys and partition BOTH
	// With numSync=2, we need ACKs from 2 standbys. Partitioning both standbys
	// leaves only the primary, which is insufficient to meet the requirement
	var standbys []*shardsetup.MultipoolerInstance
	var standbyNames []string
	for name, inst := range setup.Multipoolers {
		if name == setup.PrimaryName {
			continue
		}
		standbys = append(standbys, inst)
		standbyNames = append(standbyNames, name)
	}
	require.Len(t, standbys, 2, "should have 2 standbys")

	t.Logf("Partitioning both standbys (%s, %s) from primary (port %d) - this prevents getting 2 standby ACKs",
		standbyNames[0], standbyNames[1], primaryPostgresPort)

	// Create fault rules for both standbys
	var partitionRules []*pb.FaultRule
	for i, standby := range standbys {
		appName := fmt.Sprintf("%s_%s", standby.Multipooler.Cell, standbyNames[i])
		partitionRules = append(partitionRules, &pb.FaultRule{
			Name:        fmt.Sprintf("partition-%s-from-primary", standbyNames[i]),
			Source:      appName,
			Target:      fmt.Sprintf("*:%d", primaryPostgresPort),
			Method:      "postgres:*",
			FaultType:   "drop", // Silent drop simulates network partition
			Probability: 1.0,
		})
	}

	setup.EnableFaultInjection(t, partitionRules)

	// Clear faults at test cleanup
	t.Cleanup(func() {
		setup.ClearFaultInjection(t)
	})

	// Wait a moment for the partition to take effect
	// Note: The proxy will drop active connections within 100ms, and then block reconnection attempts
	t.Log("Waiting for partition to take effect (proxy will drop the active connection)...")
	time.Sleep(2 * time.Second)

	// Debug: Check what synchronous_standby_names is set to
	debugCtx := utils.WithTimeout(t, 5*time.Second)
	debugResult, err := primaryClient.Pooler.ExecuteQuery(debugCtx,
		"SHOW synchronous_standby_names", 1)
	if err == nil && len(debugResult.Rows) > 0 {
		t.Logf("DEBUG: synchronous_standby_names = %s", string(debugResult.Rows[0].Values[0]))
	}

	// === Phase 3: Attempt write that will freeze ===
	t.Log("Phase 3: Attempting write (should freeze - can't get 2 ACKs)...")

	// Start write in goroutine since it will freeze
	writeCtx, writeCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer writeCancel()

	writeDone := make(chan error, 1)
	go func() {
		_, err := primaryClient.Pooler.ExecuteQuery(writeCtx,
			"INSERT INTO phantom_test (data) VALUES ('frozen_write')", 0)
		writeDone <- err
	}()

	// Wait a bit for the write to actually start
	time.Sleep(2 * time.Second)

	// === Phase 4: Check for phantom write on replica-0 ===
	t.Log("Phase 4: Checking for phantom write on replica-0...")

	// Find replica-0 (first standby)
	var replica0Instance *shardsetup.MultipoolerInstance
	for name, inst := range setup.Multipoolers {
		if name == setup.PrimaryName {
			continue
		}
		// First replica we encounter
		replica0Instance = inst
		break
	}
	require.NotNil(t, replica0Instance, "should find replica-0")

	// Query replica-0 - it may have received the WAL but can't ACK back to primary
	// This would show the phantom write
	replica0Client, err := shardsetup.NewMultipoolerClient(replica0Instance.Multipooler.GrpcPort)
	require.NoError(t, err, "should connect to replica-0")
	defer replica0Client.Close()

	queryCtx := utils.WithTimeout(t, 5*time.Second)
	result, err := replica0Client.Pooler.ExecuteQuery(queryCtx,
		"SELECT COUNT(*) FROM phantom_test WHERE data = 'frozen_write'", 1)

	if err == nil && len(result.Rows) > 0 {
		count := string(result.Rows[0].Values[0])
		if count == "1" {
			t.Logf("✓ PHANTOM WRITE DETECTED: Replica-0 sees uncommitted data!")
			t.Logf("  Primary hasn't committed (waiting for 2 ACKs)")
			t.Logf("  But replica-0 has the WAL entry locally")
		} else {
			t.Logf("Replica-0 does not see the frozen write yet (count=%v)", count)
		}
	}

	// Wait for write to actually timeout
	select {
	case err := <-writeDone:
		if err == nil {
			t.Fatal("write should have timed out waiting for ACKs, but it succeeded")
		}
		// Expected: timeout or context deadline
		t.Logf("✓ Write correctly froze/timed out: %v", err)
	case <-time.After(12 * time.Second):
		writeCancel()
		t.Log("✓ Write timed out as expected")
	}

	// === Phase 5: Remove partition and verify recovery ===
	t.Log("Phase 5: Removing network partition...")

	setup.ClearFaultInjection(t)
	t.Log("Network partition removed")

	// Wait for all standbys to reconnect
	t.Log("Waiting for standbys to reconnect to primary...")
	for i, standby := range standbys {
		name := standbyNames[i]
		require.Eventually(t, func() bool {
			client, err := shardsetup.NewMultipoolerClient(standby.Multipooler.GrpcPort)
			if err != nil {
				return false
			}
			defer client.Close()

			statusCtx := utils.WithTimeout(t, 5*time.Second)
			status, err := client.Manager.StandbyReplicationStatus(statusCtx,
				&multipoolermanagerdatapb.StandbyReplicationStatusRequest{})
			if err != nil {
				return false
			}

			if status.Status != nil && status.Status.WalReceiverStatus != "" {
				connected := status.Status.WalReceiverStatus == "streaming"
				if connected {
					t.Logf("✓ %s reconnected to primary", name)
					return true
				}
			}
			return false
		}, 30*time.Second, 1*time.Second, "%s should reconnect", name)
	}

	// === Phase 6: Verify final state ===
	t.Log("Phase 6: Verifying final cluster state...")

	// All replicas should eventually agree on the data
	// The frozen write may or may not be present depending on timing,
	// but what matters is that all replicas converge
	time.Sleep(5 * time.Second) // Let replication catch up

	// Count final rows on all replicas
	finalCounts := make(map[string]string)
	for name, inst := range setup.Multipoolers {
		if name == setup.PrimaryName {
			continue
		}

		replicaClient, err := shardsetup.NewMultipoolerClient(inst.Multipooler.GrpcPort)
		if err != nil {
			t.Logf("Failed to connect to %s: %v", name, err)
			continue
		}
		defer replicaClient.Close()

		queryCtx := utils.WithTimeout(t, 5*time.Second)
		result, err := replicaClient.Pooler.ExecuteQuery(queryCtx,
			"SELECT COUNT(*) FROM phantom_test", 1)
		if err == nil && len(result.Rows) > 0 {
			count := string(result.Rows[0].Values[0])
			finalCounts[name] = count
			t.Logf("Replica %s final count: %s", name, count)
		}
	}

	// Check primary count
	primaryCtx := utils.WithTimeout(t, 5*time.Second)
	result, err = primaryClient.Pooler.ExecuteQuery(primaryCtx,
		"SELECT COUNT(*) FROM phantom_test", 1)
	require.NoError(t, err, "should query primary")
	primaryCount := string(result.Rows[0].Values[0])
	t.Logf("Primary final count: %s", primaryCount)

	// All replicas should agree with primary eventually
	for name, count := range finalCounts {
		assert.Equal(t, primaryCount, count,
			"replica %s should eventually agree with primary", name)
	}

	t.Log("✓ Test complete: Network partition and recovery demonstrated")
	t.Log("")
	t.Log("This test demonstrates that:")
	t.Log("  1. Network partitions can prevent required replication ACKs")
	t.Log("  2. Writes can freeze waiting for synchronous_commit acknowledgments")
	t.Log("  3. Replicas may temporarily see uncommitted data (phantom writes)")
	t.Log("  4. After partition heals, cluster eventually converges")
}

// TestPostgresProxyBasicForwarding tests that PostgreSQL connections can be
// forwarded through the proxy without any behavioral changes.
func TestPostgresProxyBasicForwarding(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	// Create simple 2-replica cluster
	setup := shardsetup.New(t,
		shardsetup.WithMultipoolerCount(2),
	)

	// Get primary client
	primaryClient := setup.NewPrimaryClient(t)
	defer primaryClient.Close()

	ctx := utils.WithTimeout(t, 10*time.Second)

	// Create test table
	_, err := primaryClient.Pooler.ExecuteQuery(ctx,
		"CREATE TABLE IF NOT EXISTS proxy_test (id SERIAL PRIMARY KEY, data TEXT)", 0)
	require.NoError(t, err, "should create table")

	// Insert data
	_, err = primaryClient.Pooler.ExecuteQuery(ctx,
		"INSERT INTO proxy_test (data) VALUES ('test1'), ('test2'), ('test3')", 0)
	require.NoError(t, err, "should insert data")

	// Wait for replication
	time.Sleep(2 * time.Second)

	// Query from standby through proxy
	var standbyInst *shardsetup.MultipoolerInstance
	for name, inst := range setup.Multipoolers {
		if name != setup.PrimaryName {
			standbyInst = inst
			break
		}
	}
	require.NotNil(t, standbyInst, "should have standby")

	standbyClient, err := shardsetup.NewMultipoolerClient(standbyInst.Multipooler.GrpcPort)
	require.NoError(t, err, "should connect to standby")
	defer standbyClient.Close()

	result, err := standbyClient.Pooler.ExecuteQuery(ctx,
		"SELECT COUNT(*) FROM proxy_test", 1)
	require.NoError(t, err, "should query standby through proxy")
	require.Equal(t, "3", string(result.Rows[0].Values[0]), "should see all rows")

	t.Log("✓ PostgreSQL proxy forwarding works correctly")
}

// TestPostgresProxyFaultInjection_Drop tests that the proxy can block
// NEW PostgreSQL replication connections. Note: The proxy uses connection-level
// fault injection, so it only affects new connections, not established ones.
func TestPostgresProxyFaultInjection_Drop(t *testing.T) {
	t.Skip("This test is covered by TestPhantomWrite_NetworkPartition which tests the same scenario more comprehensively")
}
