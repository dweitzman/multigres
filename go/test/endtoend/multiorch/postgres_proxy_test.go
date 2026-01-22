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
	pgctldpb "github.com/multigres/multigres/go/pb/pgctldservice"
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

	// Wait for replication to catch up
	time.Sleep(5 * time.Second)

	// Verify primary: frozen write should have been aborted (not committed)
	// The write timed out waiting for 2 standby ACKs, so it was rolled back
	primaryCtx := utils.WithTimeout(t, 5*time.Second)
	result, err = primaryClient.Pooler.ExecuteQuery(primaryCtx,
		"SELECT COUNT(*) FROM phantom_test", 1)
	require.NoError(t, err, "should query primary")
	primaryCount := string(result.Rows[0].Values[0])
	t.Logf("Primary final count: %s", primaryCount)

	// CRITICAL: Primary should have count=1 (only initial_data, frozen_write was aborted)
	require.Equal(t, "1", primaryCount,
		"primary should have count=1 (frozen write was aborted, not committed)")
	t.Log("✓ Primary correctly aborted the frozen write (count=1)")

	// Verify all replicas: should converge to count=1
	// If any replica temporarily had phantom data (count=2), pg-rewind should have cleaned it up
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

	// CRITICAL: All replicas should agree with primary at count=1
	// This proves that any phantom writes were cleaned up (not that the write eventually succeeded)
	for name, count := range finalCounts {
		assert.Equal(t, "1", count,
			"replica %s should have count=1 (phantom writes cleaned up, not committed)", name)
	}

	t.Log("✓ All replicas converged to count=1 (phantom writes cleaned up)")
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

// TestPhantomWrite_WithFailover_PgRewind tests phantom write cleanup via pg-rewind after failover.
//
// Test scenario:
// 1. Create 3-node cluster, bootstrap with ANY_2, reconfigure to numSync=2
// 2. Start multiorch to monitor cluster and perform failover
// 3. Partition ONE standby's PostgreSQL replication (standby-A), keep other connected (standby-B)
// 4. Trigger frozen write on primary (can't get 2 ACKs)
// 5. standby-B may receive phantom WAL, standby-A definitely doesn't
// 6. Force failover to standby-A by partitioning multiorch gRPC from primary and standby-B
// 7. multiorch promotes standby-A (the clean node) as new primary
// 8. Remove all partitions
// 9. Old primary and standby-B rejoin via pg-rewind (cleaning up divergent WAL)
// 10. Verify all nodes converged to count=1 (phantom writes cleaned up via pg-rewind)
// 11. Verify cluster is fully operational with new primary
func TestPhantomWrite_WithFailover_PgRewind(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	// Create 3-node cluster with multiorch for failover
	setup := shardsetup.New(t,
		shardsetup.WithMultipoolerCount(3),
		shardsetup.WithMultiOrchCount(1),
		shardsetup.WithDurabilityPolicy("ANY_2"),
	)

	// Start multiorch to monitor cluster
	t.Log("Starting multiorch to monitor cluster...")
	setup.StartMultiOrchs(t)
	time.Sleep(5 * time.Second) // Let multiorch start monitoring

	// Get primary client
	primaryClient := setup.NewPrimaryClient(t)
	defer primaryClient.Close()

	ctx := utils.WithTimeout(t, 10*time.Second)

	// === Phase 0: Reconfigure to require 2 standby ACKs ===
	t.Log("Phase 0: Reconfiguring to numSync=2...")

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

	configReq := &multipoolermanagerdatapb.ConfigureSynchronousReplicationRequest{
		SynchronousCommit: multipoolermanagerdatapb.SynchronousCommitLevel_SYNCHRONOUS_COMMIT_REMOTE_APPLY,
		SynchronousMethod: multipoolermanagerdatapb.SynchronousMethod_SYNCHRONOUS_METHOD_ANY,
		NumSync:           2, // Requires 2 standby ACKs
		StandbyIds:        standbyIDs,
		ReloadConfig:      true,
	}
	_, err := primaryClient.Manager.ConfigureSynchronousReplication(ctx, configReq)
	require.NoError(t, err, "should reconfigure to numSync=2")
	t.Log("✓ Reconfigured to require 2 standby ACKs")

	// === Phase 1: Verify cluster is healthy ===
	t.Log("Phase 1: Verifying cluster health...")

	_, err = primaryClient.Pooler.ExecuteQuery(ctx,
		"CREATE TABLE IF NOT EXISTS phantom_test (id SERIAL PRIMARY KEY, data TEXT)", 0)
	require.NoError(t, err, "should create test table")

	_, err = primaryClient.Pooler.ExecuteQuery(ctx,
		"INSERT INTO phantom_test (data) VALUES ('initial_data')", 0)
	require.NoError(t, err, "initial write should succeed with 2 ACKs")

	time.Sleep(2 * time.Second) // Let replication complete
	t.Log("✓ Cluster healthy with initial data")

	// === Phase 2: Partition ONE standby's PostgreSQL replication ===
	t.Log("Phase 2: Partitioning ONE standby's PostgreSQL replication...")

	primary := setup.GetPrimary(t)
	require.NotNil(t, primary, "should have primary")
	originalPrimaryName := primary.Name
	primaryPostgresPort := primary.Pgctld.PgPort

	// Find standbys: partition standby-A (first one), keep standby-B connected
	var standbyA, standbyB *shardsetup.MultipoolerInstance
	var standbyAName, standbyBName string

	for name, inst := range setup.Multipoolers {
		if name == setup.PrimaryName {
			continue
		}
		if standbyA == nil {
			standbyA = inst
			standbyAName = name
		} else {
			standbyB = inst
			standbyBName = name
			break
		}
	}
	require.NotNil(t, standbyA, "should have standby-A")
	require.NotNil(t, standbyB, "should have standby-B")

	t.Logf("Partitioning %s's PostgreSQL replication, keeping %s connected", standbyAName, standbyBName)

	// Partition only standby-A's PostgreSQL connection
	postgresPartitionRules := []*pb.FaultRule{
		{
			Name:        fmt.Sprintf("partition-%s-postgres", standbyAName),
			Source:      fmt.Sprintf("%s_%s", standbyA.Multipooler.Cell, standbyAName),
			Target:      fmt.Sprintf("*:%d", primaryPostgresPort),
			Method:      "postgres:*",
			FaultType:   "drop",
			Probability: 1.0,
		},
	}

	setup.EnableFaultInjection(t, postgresPartitionRules)
	time.Sleep(2 * time.Second) // Let partition take effect
	t.Logf("✓ Partitioned %s (clean), %s still connected (may get phantom)", standbyAName, standbyBName)

	// === Phase 3: Trigger frozen write ===
	t.Log("Phase 3: Triggering frozen write...")

	writeCtx, writeCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer writeCancel()

	writeDone := make(chan error, 1)
	go func() {
		_, err := primaryClient.Pooler.ExecuteQuery(writeCtx,
			"INSERT INTO phantom_test (data) VALUES ('frozen_write')", 0)
		writeDone <- err
	}()

	time.Sleep(2 * time.Second) // Let write attempt to replicate

	// Check if standby-B has phantom write (it's still connected)
	standbyBClient, err := shardsetup.NewMultipoolerClient(standbyB.Multipooler.GrpcPort)
	require.NoError(t, err, "should connect to standby-B")
	defer standbyBClient.Close()

	queryCtx := utils.WithTimeout(t, 5*time.Second)
	result, err := standbyBClient.Pooler.ExecuteQuery(queryCtx,
		"SELECT COUNT(*) FROM phantom_test WHERE data = 'frozen_write'", 1)
	if err == nil && len(result.Rows) > 0 {
		count := string(result.Rows[0].Values[0])
		if count == "1" {
			t.Logf("✓ PHANTOM WRITE DETECTED on %s (count=1 for frozen_write)", standbyBName)
		} else {
			t.Logf("%s does not see frozen write yet (count=%s)", standbyBName, count)
		}
	}

	// === Phase 3.5: Stop standby-B's PostgreSQL to make it ineligible for promotion ===
	t.Logf("Phase 3.5: Stopping %s's PostgreSQL to make it ineligible for promotion...", standbyBName)

	// Stop PostgreSQL on standby-B via pgctld
	// This makes its walPosition empty, and multiorch will skip it when selecting a candidate
	// (see leader_appointment.go:205-209)
	pgctldClient, err := shardsetup.NewPgctldClient(standbyB.Pgctld.GrpcPort)
	require.NoError(t, err, "should connect to standby-B's pgctld")
	defer pgctldClient.Close()

	stopCtx := utils.WithTimeout(t, 30*time.Second)
	_, err = pgctldClient.Stop(stopCtx, &pgctldpb.StopRequest{Mode: "fast"})
	require.NoError(t, err, "should stop standby-B's PostgreSQL")

	// Wait for PostgreSQL to fully stop and for multiorch to detect it
	// Multiorch health checks run every 500ms (configured in startMultiOrch)
	time.Sleep(5 * time.Second)
	t.Logf("✓ Stopped %s's PostgreSQL (now ineligible for promotion)", standbyBName)

	// === Phase 4: Force failover by partitioning primary from multiorch ===
	t.Log("Phase 4: Forcing failover by partitioning multiorch from primary...")

	// Only partition the primary from multiorch, keep both standbys visible
	// This ensures multiorch sees a majority (2/3 nodes) and can trigger failover
	// standby-A should be preferred because it's still clean (no phantom data)
	grpcPartitionRules := []*pb.FaultRule{
		{
			Name:        "partition-multiorch-from-primary",
			Source:      "multiorch",
			Target:      fmt.Sprintf("*:%d", primary.Multipooler.GrpcPort),
			Method:      "*",
			FaultType:   "error",
			ErrorCode:   14, // Unavailable
			Probability: 1.0,
		},
	}

	// Add gRPC partition to existing PostgreSQL partition
	allRules := append(postgresPartitionRules, grpcPartitionRules...)
	setup.EnableFaultInjection(t, allRules)

	t.Logf("✓ Partitioned multiorch from primary, both standbys visible for quorum")

	// Wait for write to timeout
	select {
	case err := <-writeDone:
		if err == nil {
			t.Fatal("write should have timed out waiting for ACKs")
		}
		t.Logf("✓ Write correctly froze/timed out: %v", err)
	case <-time.After(12 * time.Second):
		writeCancel()
		t.Log("✓ Write timed out as expected")
	}

	// === Phase 5: Wait for failover ===
	t.Log("Phase 5: Waiting for multiorch to promote a standby...")

	var newPrimary *shardsetup.MultipoolerInstance
	var newPrimaryName string

	require.Eventually(t, func() bool {
		// Check which standby became primary
		// Multiorch can see both standbys and will choose based on health/replication state
		for name, inst := range setup.Multipoolers {
			if name == setup.PrimaryName {
				continue // Skip the partitioned original primary
			}

			client, err := shardsetup.NewMultipoolerClient(inst.Multipooler.GrpcPort)
			if err != nil {
				continue
			}

			statusCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			statusResp, err := client.Manager.Status(statusCtx, &multipoolermanagerdatapb.StatusRequest{})
			client.Close()
			cancel()

			if err != nil || statusResp.Status == nil {
				continue
			}

			if statusResp.Status.PoolerType == clustermetadatapb.PoolerType_PRIMARY {
				newPrimary = inst
				newPrimaryName = name
				t.Logf("✓ Failover complete: %s promoted to primary", name)
				return true
			}
		}
		return false
	}, 90*time.Second, 3*time.Second, "multiorch should promote a standby as new primary")

	require.NotNil(t, newPrimary, "expected a standby to become new primary")
	require.NotEqual(t, originalPrimaryName, newPrimaryName, "new primary should be different from original")

	t.Logf("New primary is %s (original was %s)", newPrimaryName, originalPrimaryName)

	// === Phase 6: Remove all partitions and let multiorch handle pg-rewind ===
	t.Log("Phase 6: Removing all partitions...")

	setup.ClearFaultInjection(t)
	t.Log("✓ All partitions removed")

	// Leave standby-B's PostgreSQL stopped
	// Multiorch will detect split-brain (old primary still running vs new primary)
	// and call DemoteStalePrimary on the old primary, which will:
	// 1. Stop PostgreSQL on old primary using fast shutdown
	// 2. Run pg_rewind against new primary
	// 3. Restart as standby
	// 4. Configure replication
	//
	// For standby-B (also needs pg-rewind), multiorch will similarly handle it
	t.Logf("Leaving %s's PostgreSQL stopped - multiorch will handle split-brain demotion and pg-rewind...", standbyBName)

	// Wait for multiorch to detect nodes, run pg-rewind, and reconfigure
	// Similar to demote_stale_primary_test.go, wait for both standbys to become REPLICA and streaming
	t.Log("Waiting for multiorch to run pg-rewind and rejoin nodes...")

	// Wait for old primary (pooler-1) to become standby
	t.Logf("Waiting for old primary (%s) to become REPLICA...", originalPrimaryName)
	waitForNodeToBecomeStandby(t, setup, originalPrimaryName, 180*time.Second)
	t.Logf("✓ Old primary (%s) is now REPLICA", originalPrimaryName)

	// Wait for standby-B (pooler-3) to complete pg-rewind and become standby
	t.Logf("Waiting for %s to complete pg-rewind and become REPLICA...", standbyBName)
	waitForNodeToBecomeStandby(t, setup, standbyBName, 180*time.Second)
	t.Logf("✓ %s is now REPLICA after pg-rewind", standbyBName)

	// === Phase 7: Verify all nodes converged - phantom row removed ===
	t.Log("Phase 7: Verifying pg-rewind cleaned up phantom writes...")

	// Connect to new primary
	newPrimaryClient, err := shardsetup.NewMultipoolerClient(newPrimary.Multipooler.GrpcPort)
	require.NoError(t, err, "should connect to new primary")
	defer newPrimaryClient.Close()

	// Verify new primary: count=1 (only initial_data), no frozen_write
	queryCtx = utils.WithTimeout(t, 5*time.Second)
	result, err = newPrimaryClient.Pooler.ExecuteQuery(queryCtx,
		"SELECT COUNT(*) FROM phantom_test", 1)
	require.NoError(t, err, "should query new primary total count")
	require.Equal(t, "1", string(result.Rows[0].Values[0]),
		"new primary should have count=1 (only initial_data)")

	// Explicitly verify frozen_write row doesn't exist on new primary
	result, err = newPrimaryClient.Pooler.ExecuteQuery(queryCtx,
		"SELECT COUNT(*) FROM phantom_test WHERE data = 'frozen_write'", 1)
	require.NoError(t, err, "should query new primary for frozen_write")
	require.Equal(t, "0", string(result.Rows[0].Values[0]),
		"new primary should NOT have frozen_write row")
	t.Logf("✓ New primary (%s): count=1, frozen_write row absent", newPrimaryName)

	// Verify ALL nodes converged - phantom row removed everywhere
	t.Log("Verifying phantom row removed from ALL nodes...")
	for name, inst := range setup.Multipoolers {
		if name == newPrimaryName {
			continue // Already checked
		}

		client, err := shardsetup.NewMultipoolerClient(inst.Multipooler.GrpcPort)
		if err != nil {
			t.Logf("Failed to connect to %s: %v", name, err)
			continue
		}
		defer client.Close()

		queryCtx := utils.WithTimeout(t, 5*time.Second)

		// Check total count
		result, err := client.Pooler.ExecuteQuery(queryCtx,
			"SELECT COUNT(*) FROM phantom_test", 1)
		require.NoError(t, err, "should query %s total count", name)
		totalCount := string(result.Rows[0].Values[0])

		// Check frozen_write specifically
		result, err = client.Pooler.ExecuteQuery(queryCtx,
			"SELECT COUNT(*) FROM phantom_test WHERE data = 'frozen_write'", 1)
		require.NoError(t, err, "should query %s for frozen_write", name)
		frozenCount := string(result.Rows[0].Values[0])

		t.Logf("Node %s: total=%s, frozen_write=%s", name, totalCount, frozenCount)

		assert.Equal(t, "1", totalCount,
			"node %s should have total count=1 after pg-rewind", name)
		assert.Equal(t, "0", frozenCount,
			"node %s should NOT have frozen_write row after pg-rewind", name)
	}

	t.Log("✓ ALL nodes converged: total count=1, phantom row (frozen_write) removed everywhere")
	t.Log("✓ pg-rewind successfully cleaned up divergent WAL on old primary and standby-B")

	// === Phase 8: Verify cluster is fully operational with synchronous replication ===
	t.Log("Phase 8: Verifying synchronous replication works after recovery...")

	// Wait for both standbys to start streaming replication
	// After multiorch completes pg-rewind and configures replication, this should be fast
	t.Log("Waiting for both standbys to connect and stream...")
	require.Eventually(t, func() bool {
		statusCtx := utils.WithTimeout(t, 5*time.Second)

		// Query pg_stat_replication to count connected standbys
		result, err := newPrimaryClient.Pooler.ExecuteQuery(statusCtx,
			"SELECT COUNT(*) FROM pg_stat_replication WHERE state = 'streaming'", 1)
		if err != nil {
			t.Logf("Failed to query replication status: %v", err)
			return false
		}

		if len(result.Rows) == 0 {
			return false
		}

		connectedCount := string(result.Rows[0].Values[0])
		if connectedCount == "2" {
			t.Logf("✓ 2 standbys connected and streaming")
			return true
		}
		t.Logf("Waiting for standbys: %s/2 connected", connectedCount)
		return false
	}, 60*time.Second, 3*time.Second, "standbys should reconnect to new primary")

	// Write to new primary with synchronous replication (should succeed with 2 standby ACKs)
	queryCtx = utils.WithTimeout(t, 15*time.Second)
	_, err = newPrimaryClient.Pooler.ExecuteQuery(queryCtx,
		"INSERT INTO phantom_test (data) VALUES ('post_failover_write')", 0)
	require.NoError(t, err, "should be able to write to new primary with synchronous replication")

	time.Sleep(2 * time.Second) // Let replication complete

	// Verify write exists on new primary
	queryCtx = utils.WithTimeout(t, 5*time.Second)
	result, err = newPrimaryClient.Pooler.ExecuteQuery(queryCtx,
		"SELECT COUNT(*) FROM phantom_test WHERE data = 'post_failover_write'", 1)
	require.NoError(t, err, "should query new primary")
	require.Equal(t, "1", string(result.Rows[0].Values[0]), "new write should exist on new primary")

	t.Log("✓ Cluster fully operational: synchronous replication working")
	t.Log("")
	t.Log("Test complete! This test demonstrates:")
	t.Log("  1. Network partitions can create phantom writes on connected replicas")
	t.Log("  2. Stopping a standby with phantom data forces failover to clean replica")
	t.Log("  3. pg-rewind successfully cleans up divergent WAL on rejoining nodes")
	t.Log("  4. Cluster recovers after partition and failover")
}

// waitForNodeToBecomeStandby waits for a node to become a REPLICA and have replication configured.
// This is used to wait for multiorch to complete pg-rewind and configure replication after a failover.
func waitForNodeToBecomeStandby(t *testing.T, setup *shardsetup.ShardSetup, nodeName string, timeout time.Duration) {
	t.Helper()

	node := setup.GetMultipoolerInstance(nodeName)
	require.NotNil(t, node, "node %s should exist", nodeName)

	require.Eventually(t, func() bool {
		client, err := shardsetup.NewMultipoolerClient(node.Multipooler.GrpcPort)
		if err != nil {
			t.Logf("Cannot connect to %s: %v", nodeName, err)
			return false
		}
		defer client.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		resp, err := client.Manager.Status(ctx, &multipoolermanagerdatapb.StatusRequest{})
		if err != nil {
			t.Logf("%s Status call failed: %v", nodeName, err)
			return false
		}

		// Check if it's now a REPLICA
		if resp.Status.PoolerType != clustermetadatapb.PoolerType_REPLICA {
			t.Logf("%s type is %s, waiting for REPLICA...", nodeName, resp.Status.PoolerType)
			return false
		}

		// Check if replication is configured
		if resp.Status.ReplicationStatus == nil || resp.Status.ReplicationStatus.PrimaryConnInfo == nil {
			t.Logf("%s is REPLICA but replication not yet configured", nodeName)
			return false
		}

		t.Logf("%s is now REPLICA, replicating from %s:%d",
			nodeName,
			resp.Status.ReplicationStatus.PrimaryConnInfo.Host,
			resp.Status.ReplicationStatus.PrimaryConnInfo.Port)
		return true
	}, timeout, 2*time.Second, "%s should become REPLICA after pg_rewind", nodeName)
}
