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

package multipooler

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/multigres/multigres/go/test/utils"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multipoolermanagerpb "github.com/multigres/multigres/go/pb/multipoolermanager"
	multipoolermanagerdata "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
)

// TestMultipoolerPrimaryPosition tests the replication API functionality
func TestMultipoolerPrimaryPosition(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping end-to-end tests in short mode")
	}

	setup := getSharedTestSetup(t)

	// Wait for both managers to be ready before running tests
	waitForManagerReady(t, setup, setup.PrimaryMultipooler)
	waitForManagerReady(t, setup, setup.StandbyMultipooler)

	t.Run("PrimaryPosition_Primary", func(t *testing.T) {
		// Connect to primary multipooler
		conn, err := grpc.NewClient(
			fmt.Sprintf("localhost:%d", setup.PrimaryMultipooler.GrpcPort),
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
		require.NoError(t, err)
		defer conn.Close()

		client := multipoolermanagerpb.NewMultiPoolerManagerClient(conn)
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()

		req := &multipoolermanagerdata.PrimaryPositionRequest{}
		resp, err := client.PrimaryPosition(ctx, req)
		if err != nil {
			st, ok := status.FromError(err)
			if ok && st.Message() == "unknown service multipoolermanager.MultiPoolerManager" {
				t.Logf("Got 'unknown service' error - checking multipooler logs:")
				setup.PrimaryMultipooler.logRecentOutput(t, "Debug - unknown service error")
			}
		}

		// Assert that it succeeds and returns a valid LSN
		require.NoError(t, err)
		require.NotNil(t, resp)
		assert.NotEmpty(t, resp.GetLsnPosition(), "LSN should not be empty")

		// PostgreSQL LSN format is typically like "0/1234ABCD"
		assert.Contains(t, resp.GetLsnPosition(), "/", "LSN should be in PostgreSQL format (e.g., 0/1234ABCD)")
	})

	t.Run("PrimaryPosition_Standby", func(t *testing.T) {
		// Connect to standby multipooler
		conn, err := grpc.NewClient(
			fmt.Sprintf("localhost:%d", setup.StandbyMultipooler.GrpcPort),
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
		require.NoError(t, err)
		defer conn.Close()

		client := multipoolermanagerpb.NewMultiPoolerManagerClient(conn)
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()

		req := &multipoolermanagerdata.PrimaryPositionRequest{}
		_, err = client.PrimaryPosition(ctx, req)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "operation not allowed")
	})
}

func TestPrimaryStatus(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping end-to-end tests in short mode")
	}

	setup := getSharedTestSetup(t)

	// Wait for both managers to be ready before running tests
	waitForManagerReady(t, setup, setup.PrimaryMultipooler)
	waitForManagerReady(t, setup, setup.StandbyMultipooler)

	// Create shared clients for all subtests
	primaryConn, err := grpc.NewClient(
		fmt.Sprintf("localhost:%d", setup.PrimaryMultipooler.GrpcPort),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	t.Cleanup(func() { primaryConn.Close() })
	primaryManagerClient := multipoolermanagerpb.NewMultiPoolerManagerClient(primaryConn)

	standbyConn, err := grpc.NewClient(
		fmt.Sprintf("localhost:%d", setup.StandbyMultipooler.GrpcPort),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	t.Cleanup(func() { standbyConn.Close() })
	standbyManagerClient := multipoolermanagerpb.NewMultiPoolerManagerClient(standbyConn)

	t.Run("PrimaryStatus_NoSyncReplication", func(t *testing.T) {
		setupPoolerTest(t, setup, WithoutReplication())

		t.Log("Testing PrimaryStatus without synchronous replication configured...")

		// Clear any existing sync replication configuration
		clearReq := multipoolermanagerdata.ConfigureSynchronousReplicationRequest_builder{
			SynchronousCommit: multipoolermanagerdata.SynchronousCommitLevel_SYNCHRONOUS_COMMIT_ON,
			SynchronousMethod: multipoolermanagerdata.SynchronousMethod_SYNCHRONOUS_METHOD_FIRST,
			NumSync:           0,
			StandbyIds:        []*clustermetadatapb.ID{},
			ReloadConfig:      true,
		}.Build()
		_, err := primaryManagerClient.ConfigureSynchronousReplication(utils.WithShortDeadline(t), clearReq)
		require.NoError(t, err)

		// Get primary status
		statusResp, err := primaryManagerClient.PrimaryStatus(utils.WithShortDeadline(t), &multipoolermanagerdata.PrimaryStatusRequest{})
		require.NoError(t, err, "PrimaryStatus should succeed")
		require.NotNil(t, statusResp.GetStatus(), "Status should not be nil")

		// Verify LSN is present and valid format
		assert.NotEmpty(t, statusResp.GetStatus().GetLsn(), "LSN should be present")
		assert.Regexp(t, `^[0-9A-F]+/[0-9A-F]+$`, statusResp.GetStatus().GetLsn(), "LSN should be in PostgreSQL format (X/XXXXXXXX)")
		t.Logf("Primary LSN: %s", statusResp.GetStatus().GetLsn())

		// Verify ready status
		assert.True(t, statusResp.GetStatus().GetReady(), "Primary should be ready")

		// Verify sync replication config is present but with empty standby list
		require.NotNil(t, statusResp.GetStatus().GetSyncReplicationConfig(), "Sync replication config should be present")
		assert.Equal(t, multipoolermanagerdata.SynchronousCommitLevel_SYNCHRONOUS_COMMIT_ON,
			statusResp.GetStatus().GetSyncReplicationConfig().GetSynchronousCommit(), "Should have synchronous_commit level")
		assert.Empty(t, statusResp.GetStatus().GetSyncReplicationConfig().GetStandbyIds(), "StandbyIds should be empty when not configured")
		assert.Equal(t, int32(0), statusResp.GetStatus().GetSyncReplicationConfig().GetNumSync(), "NumSync should be 0")

		t.Log("PrimaryStatus without sync replication verified successfully")
	})

	t.Run("PrimaryStatus_WithSyncReplication", func(t *testing.T) {
		setupPoolerTest(t, setup, WithoutReplication())

		t.Log("Testing PrimaryStatus with synchronous replication configured...")

		// Configure synchronous replication
		standbyIDs := []*clustermetadatapb.ID{
			makeMultipoolerID("test-cell", "standby1"),
			makeMultipoolerID("test-cell", "standby2"),
		}
		configReq := multipoolermanagerdata.ConfigureSynchronousReplicationRequest_builder{
			SynchronousCommit: multipoolermanagerdata.SynchronousCommitLevel_SYNCHRONOUS_COMMIT_REMOTE_APPLY,
			SynchronousMethod: multipoolermanagerdata.SynchronousMethod_SYNCHRONOUS_METHOD_ANY,
			NumSync:           2,
			StandbyIds:        standbyIDs,
			ReloadConfig:      true,
		}.Build()
		_, err := primaryManagerClient.ConfigureSynchronousReplication(utils.WithShortDeadline(t), configReq)
		require.NoError(t, err)

		// Wait for configuration to converge - pg_reload_conf() is asynchronous
		t.Log("Waiting for configuration to converge...")
		waitForSyncConfigConvergenceWithClient(t, primaryManagerClient, func(config *multipoolermanagerdata.SynchronousReplicationConfiguration) bool {
			return config != nil &&
				config.GetSynchronousCommit() == multipoolermanagerdata.SynchronousCommitLevel_SYNCHRONOUS_COMMIT_REMOTE_APPLY &&
				config.GetSynchronousMethod() == multipoolermanagerdata.SynchronousMethod_SYNCHRONOUS_METHOD_ANY &&
				config.GetNumSync() == 2 &&
				len(config.GetStandbyIds()) == 2
		}, "Configuration should converge to expected values")

		// Get primary status and verify
		statusResp, err := primaryManagerClient.PrimaryStatus(utils.WithShortDeadline(t), &multipoolermanagerdata.PrimaryStatusRequest{})
		require.NoError(t, err, "PrimaryStatus should succeed")
		require.NotNil(t, statusResp.GetStatus(), "Status should not be nil")

		// Verify sync replication config is present and correct
		require.NotNil(t, statusResp.GetStatus().GetSyncReplicationConfig(), "Sync replication config should be present")
		assert.Equal(t, multipoolermanagerdata.SynchronousCommitLevel_SYNCHRONOUS_COMMIT_REMOTE_APPLY,
			statusResp.GetStatus().GetSyncReplicationConfig().GetSynchronousCommit(), "Synchronous commit level should match")
		assert.Equal(t, multipoolermanagerdata.SynchronousMethod_SYNCHRONOUS_METHOD_ANY,
			statusResp.GetStatus().GetSyncReplicationConfig().GetSynchronousMethod(), "Synchronous method should match")
		assert.Equal(t, int32(2), statusResp.GetStatus().GetSyncReplicationConfig().GetNumSync(), "NumSync should match")
		assert.Len(t, statusResp.GetStatus().GetSyncReplicationConfig().GetStandbyIds(), 2, "Should have 2 standby IDs")

		// Verify standby IDs
		standbyIDMap := make(map[string]bool)
		for _, id := range statusResp.GetStatus().GetSyncReplicationConfig().GetStandbyIds() {
			key := fmt.Sprintf("%s_%s", id.GetCell(), id.GetName())
			standbyIDMap[key] = true
		}
		assert.True(t, standbyIDMap["test-cell_standby1"], "standby1 should be in the list")
		assert.True(t, standbyIDMap["test-cell_standby2"], "standby2 should be in the list")

		t.Log("PrimaryStatus with sync replication verified successfully")

		// Cleanup
		clearReq := multipoolermanagerdata.ConfigureSynchronousReplicationRequest_builder{
			SynchronousCommit: multipoolermanagerdata.SynchronousCommitLevel_SYNCHRONOUS_COMMIT_ON,
			SynchronousMethod: multipoolermanagerdata.SynchronousMethod_SYNCHRONOUS_METHOD_FIRST,
			NumSync:           0,
			StandbyIds:        []*clustermetadatapb.ID{},
			ReloadConfig:      true,
		}.Build()
		_, err = primaryManagerClient.ConfigureSynchronousReplication(utils.WithShortDeadline(t), clearReq)
		require.NoError(t, err)
	})

	t.Run("PrimaryStatus_WithConnectedFollower", func(t *testing.T) {
		setupPoolerTest(t, setup, WithoutReplication())

		t.Log("Testing PrimaryStatus with connected follower...")

		// Ensure standby is connected and replicating
		setPrimaryReq := multipoolermanagerdata.SetPrimaryConnInfoRequest_builder{
			Host:                  "localhost",
			Port:                  int32(setup.PrimaryPgctld.PgPort),
			StartReplicationAfter: true,
			StopReplicationBefore: false,
			CurrentTerm:           1,
			Force:                 false,
		}.Build()
		_, err := standbyManagerClient.SetPrimaryConnInfo(utils.WithShortDeadline(t), setPrimaryReq)
		require.NoError(t, err)

		// Wait for replication to be established
		require.Eventually(t, func() bool {
			statusResp, err := standbyManagerClient.StandbyReplicationStatus(utils.WithShortDeadline(t), &multipoolermanagerdata.StandbyReplicationStatusRequest{})
			if err != nil {
				return false
			}
			return statusResp.GetStatus().HasPrimaryConnInfo() &&
				statusResp.GetStatus().GetPrimaryConnInfo().GetHost() != "" &&
				!statusResp.GetStatus().GetIsWalReplayPaused()
		}, 5*time.Second, 200*time.Millisecond, "Replication should be established")

		// Wait for primary to register the follower in pg_stat_replication
		// There can be a delay between standby connection and primary's view update
		t.Log("Waiting for primary to register the follower...")
		var statusResp *multipoolermanagerdata.PrimaryStatusResponse
		require.Eventually(t, func() bool {
			var err error
			statusResp, err = primaryManagerClient.PrimaryStatus(utils.WithShortDeadline(t), &multipoolermanagerdata.PrimaryStatusRequest{})
			if err != nil {
				t.Logf("PrimaryStatus error: %v", err)
				return false
			}
			return statusResp.HasStatus() && len(statusResp.GetStatus().GetConnectedFollowers()) > 0
		}, 10*time.Second, 200*time.Millisecond, "Primary should register the follower")

		// Verify followers list contains the standby
		require.NotEmpty(t, statusResp.GetStatus().GetConnectedFollowers(), "Should have at least one follower")

		// Find our standby in the followers list
		expectedAppName := fmt.Sprintf("test-cell_%s", setup.StandbyMultipooler.ServiceID)
		foundStandby := false
		for _, follower := range statusResp.GetStatus().GetConnectedFollowers() {
			if follower.GetCell() == "test-cell" && follower.GetName() == setup.StandbyMultipooler.ServiceID {
				foundStandby = true
				break
			}
		}
		assert.True(t, foundStandby, "Standby should be in followers list with application_name: %s", expectedAppName)
		t.Logf("Found %d connected follower(s)", len(statusResp.GetStatus().GetConnectedFollowers()))

		t.Log("PrimaryStatus with connected follower verified successfully")
	})

	t.Run("PrimaryStatus_Standby_Fails", func(t *testing.T) {
		setupPoolerTest(t, setup, WithoutReplication())

		t.Log("Testing PrimaryStatus on REPLICA pooler (should fail)...")

		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()

		_, err := standbyManagerClient.PrimaryStatus(ctx, &multipoolermanagerdata.PrimaryStatusRequest{})
		require.Error(t, err, "PrimaryStatus should fail on standby")
		assert.Contains(t, err.Error(), "operation not allowed", "Error should indicate operation not allowed on REPLICA")

		t.Log("Confirmed: PrimaryStatus correctly rejected on REPLICA pooler")
	})
}

func TestGetFollowers(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping end-to-end tests in short mode")
	}

	setup := getSharedTestSetup(t)

	// Wait for both managers to be ready before running tests
	waitForManagerReady(t, setup, setup.PrimaryMultipooler)
	waitForManagerReady(t, setup, setup.StandbyMultipooler)

	// Create shared clients for all subtests
	primaryConn, err := grpc.NewClient(
		fmt.Sprintf("localhost:%d", setup.PrimaryMultipooler.GrpcPort),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	t.Cleanup(func() { primaryConn.Close() })
	primaryManagerClient := multipoolermanagerpb.NewMultiPoolerManagerClient(primaryConn)

	standbyConn, err := grpc.NewClient(
		fmt.Sprintf("localhost:%d", setup.StandbyMultipooler.GrpcPort),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	t.Cleanup(func() { standbyConn.Close() })
	standbyManagerClient := multipoolermanagerpb.NewMultiPoolerManagerClient(standbyConn)

	t.Run("GetFollowers_NoSyncReplication", func(t *testing.T) {
		setupPoolerTest(t, setup, WithoutReplication())

		t.Log("Testing GetFollowers without synchronous replication configured...")

		// Clear any existing sync replication configuration
		clearReq := multipoolermanagerdata.ConfigureSynchronousReplicationRequest_builder{
			SynchronousCommit: multipoolermanagerdata.SynchronousCommitLevel_SYNCHRONOUS_COMMIT_ON,
			SynchronousMethod: multipoolermanagerdata.SynchronousMethod_SYNCHRONOUS_METHOD_FIRST,
			NumSync:           0,
			StandbyIds:        []*clustermetadatapb.ID{},
			ReloadConfig:      true,
		}.Build()
		_, err := primaryManagerClient.ConfigureSynchronousReplication(utils.WithShortDeadline(t), clearReq)
		require.NoError(t, err)

		// Get followers
		followersResp, err := primaryManagerClient.GetFollowers(utils.WithShortDeadline(t), &multipoolermanagerdata.GetFollowersRequest{})
		require.NoError(t, err, "GetFollowers should succeed")
		require.NotNil(t, followersResp, "Response should not be nil")

		// Verify empty followers list since no sync replication is configured
		assert.Empty(t, followersResp.GetFollowers(), "Followers list should be empty when no sync replication configured")

		// Verify sync config is present
		require.NotNil(t, followersResp.GetSyncConfig(), "Sync config should be present")
		assert.Empty(t, followersResp.GetSyncConfig().GetStandbyIds(), "StandbyIds should be empty")

		t.Log("GetFollowers without sync replication verified successfully")
	})

	t.Run("GetFollowers_WithConnectedFollower", func(t *testing.T) {
		// Default behavior: replication already configured and streaming
		setupPoolerTest(t, setup)

		t.Log("Testing GetFollowers with connected follower...")

		// Replication is already running (default behavior), verify standby is connected
		require.Eventually(t, func() bool {
			statusResp, err := standbyManagerClient.StandbyReplicationStatus(utils.WithShortDeadline(t), &multipoolermanagerdata.StandbyReplicationStatusRequest{})
			if err != nil {
				return false
			}
			return statusResp.HasStatus() && statusResp.GetStatus().HasPrimaryConnInfo()
		}, 10*time.Second, 500*time.Millisecond, "Standby should be connected (from default setup)")

		// Configure synchronous replication with the standby
		standbyID := makeMultipoolerID("test-cell", setup.StandbyMultipooler.ServiceID)
		configReq := multipoolermanagerdata.ConfigureSynchronousReplicationRequest_builder{
			SynchronousCommit: multipoolermanagerdata.SynchronousCommitLevel_SYNCHRONOUS_COMMIT_ON,
			SynchronousMethod: multipoolermanagerdata.SynchronousMethod_SYNCHRONOUS_METHOD_FIRST,
			NumSync:           1,
			StandbyIds:        []*clustermetadatapb.ID{standbyID},
			ReloadConfig:      true,
		}.Build()
		_, err = primaryManagerClient.ConfigureSynchronousReplication(utils.WithShortDeadline(t), configReq)
		require.NoError(t, err)

		// Wait for the standby to actually connect and appear in pg_stat_replication
		t.Log("Waiting for standby to connect to primary...")
		require.Eventually(t, func() bool {
			statusResp, err := primaryManagerClient.PrimaryStatus(utils.WithShortDeadline(t), &multipoolermanagerdata.PrimaryStatusRequest{})
			if err != nil {
				return false
			}
			// Check if any followers are connected
			return statusResp.HasStatus() && len(statusResp.GetStatus().GetConnectedFollowers()) > 0
		}, 10*time.Second, 500*time.Millisecond, "Standby should connect to primary")

		// Get followers
		followersResp, err := primaryManagerClient.GetFollowers(utils.WithShortDeadline(t), &multipoolermanagerdata.GetFollowersRequest{})
		require.NoError(t, err, "GetFollowers should succeed")
		require.NotNil(t, followersResp, "Response should not be nil")

		// Verify followers list
		require.Len(t, followersResp.GetFollowers(), 1, "Should have exactly 1 follower configured")

		follower := followersResp.GetFollowers()[0]
		assert.Equal(t, "test-cell", follower.GetFollowerId().GetCell(), "Follower cell should match")
		assert.Equal(t, setup.StandbyMultipooler.ServiceID, follower.GetFollowerId().GetName(), "Follower name should match")
		assert.True(t, follower.GetIsConnected(), "Follower should be connected")
		assert.NotEmpty(t, follower.GetApplicationName(), "Application name should be set")

		// Verify replication stats are present
		require.NotNil(t, follower.GetReplicationStats(), "Replication stats should be present for connected follower")
		assert.NotZero(t, follower.GetReplicationStats().GetPid(), "PID should be set")
		assert.NotEmpty(t, follower.GetReplicationStats().GetState(), "State should be set")
		assert.NotEmpty(t, follower.GetReplicationStats().GetSyncState(), "Sync state should be set")
		assert.NotEmpty(t, follower.GetReplicationStats().GetSentLsn(), "Sent LSN should be set")
		assert.NotEmpty(t, follower.GetReplicationStats().GetWriteLsn(), "Write LSN should be set")
		assert.NotEmpty(t, follower.GetReplicationStats().GetFlushLsn(), "Flush LSN should be set")
		assert.NotEmpty(t, follower.GetReplicationStats().GetReplayLsn(), "Replay LSN should be set")

		t.Logf("Follower stats: PID=%d, State=%s, SyncState=%s, SentLSN=%s",
			follower.GetReplicationStats().GetPid(),
			follower.GetReplicationStats().GetState(),
			follower.GetReplicationStats().GetSyncState(),
			follower.GetReplicationStats().GetSentLsn())
	})

	t.Run("GetFollowers_FollowerDisconnects", func(t *testing.T) {
		// Default behavior: replication already configured and streaming
		setupPoolerTest(t, setup)

		t.Log("Testing GetFollowers when connected follower disconnects...")

		// Replication is already running (default behavior), verify standby is connected
		require.Eventually(t, func() bool {
			statusResp, err := standbyManagerClient.StandbyReplicationStatus(utils.WithShortDeadline(t), &multipoolermanagerdata.StandbyReplicationStatusRequest{})
			if err != nil {
				return false
			}
			return statusResp.HasStatus() && statusResp.GetStatus().HasPrimaryConnInfo()
		}, 10*time.Second, 500*time.Millisecond, "Standby should be connected (from default setup)")

		// Configure synchronous replication with the standby
		standbyID := makeMultipoolerID("test-cell", setup.StandbyMultipooler.ServiceID)
		configReq := multipoolermanagerdata.ConfigureSynchronousReplicationRequest_builder{
			SynchronousCommit: multipoolermanagerdata.SynchronousCommitLevel_SYNCHRONOUS_COMMIT_ON,
			SynchronousMethod: multipoolermanagerdata.SynchronousMethod_SYNCHRONOUS_METHOD_FIRST,
			NumSync:           1,
			StandbyIds:        []*clustermetadatapb.ID{standbyID},
			ReloadConfig:      true,
		}.Build()
		_, err = primaryManagerClient.ConfigureSynchronousReplication(utils.WithShortDeadline(t), configReq)
		require.NoError(t, err)

		// Wait for the standby to actually connect and appear in pg_stat_replication
		t.Log("Waiting for standby to connect to primary...")
		require.Eventually(t, func() bool {
			statusResp, err := primaryManagerClient.PrimaryStatus(utils.WithShortDeadline(t), &multipoolermanagerdata.PrimaryStatusRequest{})
			if err != nil {
				return false
			}
			// Check if any followers are connected
			return statusResp.HasStatus() && len(statusResp.GetStatus().GetConnectedFollowers()) > 0
		}, 10*time.Second, 500*time.Millisecond, "Standby should connect to primary")

		// Verify follower is connected
		followersResp, err := primaryManagerClient.GetFollowers(utils.WithShortDeadline(t), &multipoolermanagerdata.GetFollowersRequest{})
		require.NoError(t, err, "GetFollowers should succeed")
		require.Len(t, followersResp.GetFollowers(), 1, "Should have exactly 1 follower configured")

		follower := followersResp.GetFollowers()[0]
		assert.True(t, follower.GetIsConnected(), "Follower should be connected initially")
		assert.NotNil(t, follower.GetReplicationStats(), "Replication stats should be present initially")
		t.Logf("Initial state: Follower connected with PID=%d, State=%s",
			follower.GetReplicationStats().GetPid(), follower.GetReplicationStats().GetState())

		// Now reset replication on the standby to disconnect it
		t.Log("Resetting replication on standby to disconnect...")
		_, err = standbyManagerClient.ResetReplication(utils.WithShortDeadline(t), &multipoolermanagerdata.ResetReplicationRequest{})
		require.NoError(t, err, "ResetReplication should succeed")

		// Wait for the disconnection to be reflected in pg_stat_replication
		// The replication connection should close within a few seconds
		require.Eventually(t, func() bool {
			followersResp, err := primaryManagerClient.GetFollowers(utils.WithShortDeadline(t), &multipoolermanagerdata.GetFollowersRequest{})
			if err != nil {
				t.Logf("GetFollowers failed: %v", err)
				return false
			}
			if len(followersResp.GetFollowers()) != 1 {
				t.Logf("Expected 1 follower, got %d", len(followersResp.GetFollowers()))
				return false
			}
			// Check if follower is now disconnected
			return !followersResp.GetFollowers()[0].GetIsConnected()
		}, 10*time.Second, 500*time.Millisecond, "Follower should be marked as disconnected after ResetReplication")

		// Verify the final state
		followersResp, err = primaryManagerClient.GetFollowers(utils.WithShortDeadline(t), &multipoolermanagerdata.GetFollowersRequest{})
		require.NoError(t, err, "GetFollowers should succeed")
		require.Len(t, followersResp.GetFollowers(), 1, "Should still have 1 follower configured")

		follower = followersResp.GetFollowers()[0]
		assert.False(t, follower.GetIsConnected(), "Follower should be disconnected")
		assert.Nil(t, follower.GetReplicationStats(), "Replication stats should be nil for disconnected follower")
		assert.Equal(t, "test-cell", follower.GetFollowerId().GetCell(), "Follower cell should still match")
		assert.Equal(t, setup.StandbyMultipooler.ServiceID, follower.GetFollowerId().GetName(), "Follower name should still match")

		t.Log("Verified: Follower disconnect is correctly reflected in GetFollowers response")
	})

	t.Run("GetFollowers_MixedConnectedDisconnected", func(t *testing.T) {
		// Default behavior: replication already configured and streaming
		setupPoolerTest(t, setup)

		t.Log("Testing GetFollowers with mix of connected and disconnected followers...")

		// Replication is already running (default behavior), verify standby is connected
		require.Eventually(t, func() bool {
			statusResp, err := standbyManagerClient.StandbyReplicationStatus(utils.WithShortDeadline(t), &multipoolermanagerdata.StandbyReplicationStatusRequest{})
			if err != nil {
				return false
			}
			return statusResp.HasStatus() && statusResp.GetStatus().HasPrimaryConnInfo()
		}, 10*time.Second, 500*time.Millisecond, "Standby should be connected (from default setup)")

		// Configure synchronous replication with real standby + fake standby
		connectedID := makeMultipoolerID("test-cell", setup.StandbyMultipooler.ServiceID)
		disconnectedID := makeMultipoolerID("test-cell", "missing-standby")
		configReq := multipoolermanagerdata.ConfigureSynchronousReplicationRequest_builder{
			SynchronousCommit: multipoolermanagerdata.SynchronousCommitLevel_SYNCHRONOUS_COMMIT_ON,
			SynchronousMethod: multipoolermanagerdata.SynchronousMethod_SYNCHRONOUS_METHOD_FIRST,
			NumSync:           2,
			StandbyIds:        []*clustermetadatapb.ID{connectedID, disconnectedID},
			ReloadConfig:      true,
		}.Build()
		_, err = primaryManagerClient.ConfigureSynchronousReplication(utils.WithShortDeadline(t), configReq)
		require.NoError(t, err)

		// Wait for configuration to converge
		waitForSyncConfigConvergenceWithClient(t, primaryManagerClient, func(config *multipoolermanagerdata.SynchronousReplicationConfiguration) bool {
			return config != nil && len(config.GetStandbyIds()) == 2
		}, "Configuration should converge")

		// Wait for the real standby to actually connect (the fake one won't)
		t.Log("Waiting for real standby to connect to primary...")
		require.Eventually(t, func() bool {
			statusResp, err := primaryManagerClient.PrimaryStatus(utils.WithShortDeadline(t), &multipoolermanagerdata.PrimaryStatusRequest{})
			if err != nil {
				return false
			}
			// Check if any followers are connected
			return statusResp.HasStatus() && len(statusResp.GetStatus().GetConnectedFollowers()) > 0
		}, 10*time.Second, 500*time.Millisecond, "Real standby should connect to primary")

		// Get followers
		followersResp, err := primaryManagerClient.GetFollowers(utils.WithShortDeadline(t), &multipoolermanagerdata.GetFollowersRequest{})
		require.NoError(t, err, "GetFollowers should succeed")
		require.NotNil(t, followersResp, "Response should not be nil")

		// Verify followers list
		require.Len(t, followersResp.GetFollowers(), 2, "Should have exactly 2 followers configured")

		// Count connected and disconnected
		connectedCount := 0
		disconnectedCount := 0
		for _, follower := range followersResp.GetFollowers() {
			if follower.GetIsConnected() {
				connectedCount++
				assert.NotNil(t, follower.GetReplicationStats(), "Connected follower should have stats")
				assert.Equal(t, setup.StandbyMultipooler.ServiceID, follower.GetFollowerId().GetName(), "Connected follower name should match")
			} else {
				disconnectedCount++
				assert.Nil(t, follower.GetReplicationStats(), "Disconnected follower should not have stats")
				assert.Equal(t, "missing-standby", follower.GetFollowerId().GetName(), "Disconnected follower name should match")
			}
		}

		assert.Equal(t, 1, connectedCount, "Should have 1 connected follower")
		assert.Equal(t, 1, disconnectedCount, "Should have 1 disconnected follower")

		t.Log("GetFollowers with mixed connected/disconnected followers verified successfully")
	})

	t.Run("GetFollowers_Standby_Fails", func(t *testing.T) {
		setupPoolerTest(t, setup, WithoutReplication())

		t.Log("Testing GetFollowers on REPLICA pooler (should fail)...")

		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()

		_, err := standbyManagerClient.GetFollowers(ctx, &multipoolermanagerdata.GetFollowersRequest{})
		require.Error(t, err, "GetFollowers should fail on standby")
		assert.Contains(t, err.Error(), "operation not allowed", "Error should indicate operation not allowed on REPLICA")

		t.Log("Confirmed: GetFollowers correctly rejected on REPLICA pooler")
	})
}
