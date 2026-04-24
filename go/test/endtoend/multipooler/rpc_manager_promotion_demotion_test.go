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
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
	"github.com/multigres/multigres/go/test/utils"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	consensuspb "github.com/multigres/multigres/go/pb/consensus"
	consensusdatapb "github.com/multigres/multigres/go/pb/consensusdata"
	multipoolermanagerpb "github.com/multigres/multigres/go/pb/multipoolermanager"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
)

// testCoordinatorID is the coordinator identity used in Recruit/Propose calls within tests.
var testCoordinatorID = &clustermetadatapb.ID{
	Component: clustermetadatapb.ID_MULTIPOOLER,
	Cell:      "test-cell",
	Name:      "test-coordinator",
}

// recruitBoth sends Recruit to both consensus clients with a new term (currentTerm+100).
// Returns the TermRevocation to pass to subsequent Propose calls.
func recruitBoth(
	t *testing.T,
	primaryConsensusClient, standbyConsensusClient consensuspb.MultiPoolerConsensusClient,
	currentTerm int64,
) *consensusdatapb.TermRevocation {
	t.Helper()
	termRevocation := &consensusdatapb.TermRevocation{
		RevokedBelowTerm:      currentTerm + 100,
		AcceptedCoordinatorId: testCoordinatorID,
	}
	recruitReq := &consensusdatapb.RecruitRequest{TermRevocation: termRevocation}
	_, err := primaryConsensusClient.Recruit(utils.WithTimeout(t, 5*time.Second), recruitReq)
	require.NoError(t, err, "Recruit primary should succeed")
	_, err = standbyConsensusClient.Recruit(utils.WithTimeout(t, 5*time.Second), recruitReq)
	require.NoError(t, err, "Recruit standby should succeed")
	return termRevocation
}

// proposeBothWithLeader sends a CoordinatorProposal to both consensus clients.
// The node whose ID matches leaderID will promote; the other will configure replication.
func proposeBothWithLeader(
	t *testing.T,
	primaryConsensusClient, standbyConsensusClient consensuspb.MultiPoolerConsensusClient,
	termRevocation *consensusdatapb.TermRevocation,
	leaderID *clustermetadatapb.ID, leaderHost string, leaderPgPort int32,
) {
	t.Helper()
	proposal := &consensusdatapb.CoordinatorProposal{
		TermRevocation: termRevocation,
		ProposalLeader: &consensusdatapb.ProposalLeader{
			Id:           leaderID,
			Host:         leaderHost,
			PostgresPort: leaderPgPort,
		},
	}
	proposeReq := &consensusdatapb.ProposeRequest{Proposal: proposal}
	_, err := primaryConsensusClient.Propose(utils.WithTimeout(t, 5*time.Second), proposeReq)
	require.NoError(t, err, "Propose to primary should succeed")
	_, err = standbyConsensusClient.Propose(utils.WithTimeout(t, 5*time.Second), proposeReq)
	require.NoError(t, err, "Propose to standby should succeed")
}

// TestEmergencyDemoteAndPromote tests the full EmergencyDemote and Recruit/Propose cycle.
// This ensures that emergency demoting a primary and promoting a standby work together correctly
// using the new consensus protocol.
func TestEmergencyDemoteAndPromote(t *testing.T) {
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
	primaryConsensusClient := consensuspb.NewMultiPoolerConsensusClient(primaryConn)

	standbyConn, err := grpc.NewClient(
		fmt.Sprintf("localhost:%d", setup.StandbyMultipooler.GrpcPort),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	t.Cleanup(func() { standbyConn.Close() })
	standbyManagerClient := multipoolermanagerpb.NewMultiPoolerManagerClient(standbyConn)
	standbyConsensusClient := consensuspb.NewMultiPoolerConsensusClient(standbyConn)

	standbyID := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      setup.StandbyMultipooler.Cell,
		Name:      setup.StandbyMultipooler.Name,
	}
	primaryID := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      setup.PrimaryMultipooler.Cell,
		Name:      setup.PrimaryMultipooler.Name,
	}

	t.Run("FullCycle_EmergencyDemoteAndPromote", func(t *testing.T) {
		setupPoolerTest(t, setup)

		t.Log("=== Testing full EmergencyDemote/Propose cycle ===")

		ctx := utils.WithShortDeadline(t)
		primaryTerm := shardsetup.MustGetCurrentTerm(t, ctx, primaryConsensusClient)
		t.Logf("Starting test with primary term: %d", primaryTerm)

		// Demote the original primary
		t.Log("Demoting original primary...")

		primaryStatusBefore, err := primaryConsensusClient.Status(utils.WithShortDeadline(t), &consensusdatapb.StatusRequest{})
		require.NoError(t, err, "Status should succeed before demotion")
		lsnBeforeDemotion := primaryStatusBefore.WalPosition.CurrentLsn
		t.Logf("LSN before demotion: %s", lsnBeforeDemotion)

		demoteReq := &multipoolermanagerdatapb.EmergencyDemoteRequest{
			ConsensusTerm: primaryTerm,
			DrainTimeout:  nil,
			Force:         true,
		}
		demoteResp, err := primaryConsensusClient.EmergencyDemote(utils.WithTimeout(t, 10*time.Second), demoteReq)
		require.NoError(t, err, "Demote should succeed")
		require.NotNil(t, demoteResp)

		assert.False(t, demoteResp.WasAlreadyDemoted, "Should not have been already demoted")
		assert.NotEmpty(t, demoteResp.LsnPosition)
		t.Logf("Demotion complete. LSN: %s, connections terminated: %d",
			demoteResp.LsnPosition, demoteResp.ConnectionsTerminated)

		// Restore demoted primary to working state (emergency demotion stops postgres)
		restoreAfterEmergencyDemotion(t, setup, setup.PrimaryPgctld, setup.PrimaryMultipooler, setup.PrimaryMultipooler.Name)

		// Use Recruit+Propose to configure demoted primary as replica and promote standby.
		t.Log("Recruiting nodes and proposing standby as new leader...")
		currentTerm := shardsetup.MustGetCurrentTerm(t, ctx, primaryConsensusClient)
		termRevocation := recruitBoth(t, primaryConsensusClient, standbyConsensusClient, currentTerm)
		proposeBothWithLeader(t, primaryConsensusClient, standbyConsensusClient,
			termRevocation, standbyID, "localhost", int32(setup.StandbyMultipooler.PgPort))

		// Verify standby.signal exists on the demoted primary (now replica)
		t.Log("Verifying standby.signal exists after replica configuration...")
		primaryStandbySignalPath := filepath.Join(setup.PrimaryPgctld.PoolerDir, "pg_data", "standby.signal")
		_, statErr := os.Stat(primaryStandbySignalPath)
		assert.NoError(t, statErr, "standby.signal should exist after replica configuration")

		// Verify demoted node is now in replica role
		primaryStatusAfter, err := primaryConsensusClient.Status(utils.WithShortDeadline(t), &consensusdatapb.StatusRequest{})
		require.NoError(t, err)
		assert.Equal(t, consensusdatapb.PostgresRole_POSTGRES_ROLE_REPLICA, primaryStatusAfter.Role,
			"Demoted primary should be in replica role after Propose")

		t.Log("Verifying standby promoted to primary...")
		standbyNowPrimaryStatus, err := standbyConsensusClient.Status(utils.WithShortDeadline(t), &consensusdatapb.StatusRequest{})
		require.NoError(t, err, "Status should work on new primary")
		assert.NotEmpty(t, standbyNowPrimaryStatus.WalPosition.GetCurrentLsn())

		// Verify signal files are removed after promotion
		standbySignalPath := filepath.Join(setup.StandbyPgctld.PoolerDir, "pg_data", "standby.signal")
		recoverySignalPath := filepath.Join(setup.StandbyPgctld.PoolerDir, "pg_data", "recovery.signal")
		_, standbyStatErr := os.Stat(standbySignalPath)
		assert.True(t, os.IsNotExist(standbyStatErr), "standby.signal should not exist after promotion")
		_, recoveryStatErr := os.Stat(recoverySignalPath)
		assert.True(t, os.IsNotExist(recoveryStatErr), "recovery.signal should not exist after promotion")

		t.Log("Original standby is now primary")
		t.Log("Restoring original state...")

		// Demote the new primary (original standby) with Force=true
		demoteReq2 := &multipoolermanagerdatapb.EmergencyDemoteRequest{
			ConsensusTerm: 0, // Ignored when Force=true
			DrainTimeout:  nil,
			Force:         true,
		}
		demoteResp2, err := standbyConsensusClient.EmergencyDemote(utils.WithTimeout(t, 10*time.Second), demoteReq2)
		require.NoError(t, err, "Demote should succeed on new primary")
		assert.False(t, demoteResp2.WasAlreadyDemoted)
		t.Logf("New primary demoted. LSN: %s", demoteResp2.LsnPosition)

		// Restore demoted standby to working state
		restoreAfterEmergencyDemotion(t, setup, setup.StandbyPgctld, setup.StandbyMultipooler, setup.StandbyMultipooler.Name)

		// Stop replication on original primary (now replica), get LSN
		stopReq2 := &multipoolermanagerdatapb.StopReplicationRequest{}
		_, err = primaryManagerClient.StopReplication(utils.WithShortDeadline(t), stopReq2)
		require.NoError(t, err, "StopReplication should succeed")

		primaryNowReplicaStatus, err := primaryManagerClient.Status(utils.WithShortDeadline(t), &multipoolermanagerdatapb.StatusRequest{})
		require.NoError(t, err, "Status should succeed")
		require.NotNil(t, primaryNowReplicaStatus.Status.ReplicationStatus, "primary (now replica) should have replication status")

		// Recruit+Propose with original primary as leader to restore original state
		t.Log("Restoring original primary via Recruit+Propose...")
		currentTerm2 := shardsetup.MustGetCurrentTerm(t, ctx, primaryConsensusClient)
		termRevocation2 := recruitBoth(t, primaryConsensusClient, standbyConsensusClient, currentTerm2)
		proposeBothWithLeader(t, primaryConsensusClient, standbyConsensusClient,
			termRevocation2, primaryID, "localhost", int32(setup.PrimaryMultipooler.PgPort))

		// Verify signal files are removed after restoring original primary
		t.Log("Verifying signal files removed from restored primary...")
		primaryStandbySignalPath = filepath.Join(setup.PrimaryPgctld.PoolerDir, "pg_data", "standby.signal")
		primaryRecoverySignalPath := filepath.Join(setup.PrimaryPgctld.PoolerDir, "pg_data", "recovery.signal")
		_, primaryStandbyStatErr := os.Stat(primaryStandbySignalPath)
		assert.True(t, os.IsNotExist(primaryStandbyStatErr), "standby.signal should not exist after promotion")
		_, primaryRecoveryStatErr := os.Stat(primaryRecoverySignalPath)
		assert.True(t, os.IsNotExist(primaryRecoveryStatErr), "recovery.signal should not exist after promotion")

		// Verify original primary works again
		restoredPrimaryStatus, err := primaryConsensusClient.Status(utils.WithShortDeadline(t), &consensusdatapb.StatusRequest{})
		require.NoError(t, err, "Status should work on restored primary")
		assert.NotEmpty(t, restoredPrimaryStatus.WalPosition.GetCurrentLsn())

		t.Log("Original state restored - primary is primary, standby is standby")
	})

	t.Run("Idempotency_EmergencyDemote", func(t *testing.T) {
		setupPoolerTest(t, setup)

		t.Log("Testing that EmergencyDemote cannot be called twice after completion...")

		// First demotion with Force=true
		demoteReq := &multipoolermanagerdatapb.EmergencyDemoteRequest{
			ConsensusTerm: 0, // Ignored when Force=true
			DrainTimeout:  nil,
			Force:         true,
		}
		demoteResp1, err := primaryConsensusClient.EmergencyDemote(utils.WithTimeout(t, 20*time.Second), demoteReq)
		require.NoError(t, err, "First demote should succeed")
		assert.False(t, demoteResp1.WasAlreadyDemoted)

		// Restore demoted primary to working state
		restoreAfterEmergencyDemotion(t, setup, setup.PrimaryPgctld, setup.PrimaryMultipooler, setup.PrimaryMultipooler.Name)

		// Use Recruit+Propose to configure the demoted primary as a replica of the standby.
		// This puts the primary's postgres into standby mode, so the next EmergencyDemote should fail.
		currentTerm := shardsetup.MustGetCurrentTerm(t, utils.WithShortDeadline(t), primaryConsensusClient)
		termRevocation := recruitBoth(t, primaryConsensusClient, standbyConsensusClient, currentTerm)
		// Propose only to primary (replica path: configure replication to standby).
		// We don't promote the standby — we just need the primary's postgres in standby mode.
		proposal := &consensusdatapb.CoordinatorProposal{
			TermRevocation: termRevocation,
			ProposalLeader: &consensusdatapb.ProposalLeader{
				Id:           standbyID,
				Host:         "localhost",
				PostgresPort: int32(setup.StandbyMultipooler.PgPort),
			},
		}
		_, err = primaryConsensusClient.Propose(utils.WithTimeout(t, 5*time.Second), &consensusdatapb.ProposeRequest{Proposal: proposal})
		require.NoError(t, err, "Propose (replica path) to primary should succeed")

		// Second demotion should fail — cannot demote a standby
		_, err = primaryConsensusClient.EmergencyDemote(utils.WithTimeout(t, 10*time.Second), demoteReq)
		require.Error(t, err, "Second emergency demote should fail - cannot demote a standby")
		assert.Contains(t, err.Error(), "standby mode")

		t.Log("EmergencyDemote guard rail verified - cannot demote a standby")
	})

	t.Run("Idempotency_Propose", func(t *testing.T) {
		setupPoolerTest(t, setup)

		t.Log("Testing Propose idempotency (promoting a demoted primary twice)...")

		// Demote the primary so we can test promote idempotency
		demoteReq := &multipoolermanagerdatapb.EmergencyDemoteRequest{
			ConsensusTerm: 0, // Ignored when Force=true
			DrainTimeout:  nil,
			Force:         true,
		}
		_, err = primaryConsensusClient.EmergencyDemote(utils.WithTimeout(t, 10*time.Second), demoteReq)
		require.NoError(t, err, "Demote should succeed")

		// Restore demoted primary to working state
		restoreAfterEmergencyDemotion(t, setup, setup.PrimaryPgctld, setup.PrimaryMultipooler, setup.PrimaryMultipooler.Name)

		// Stop replication on the demoted primary (now replica) so we can promote it back.
		stopReq := &multipoolermanagerdatapb.StopReplicationRequest{}
		_, err = primaryManagerClient.StopReplication(utils.WithShortDeadline(t), stopReq)
		require.NoError(t, err)

		// Recruit+Propose: promote the demoted primary back with primaryID as leader.
		currentTerm := shardsetup.MustGetCurrentTerm(t, utils.WithShortDeadline(t), primaryConsensusClient)
		termRevocation := recruitBoth(t, primaryConsensusClient, standbyConsensusClient, currentTerm)
		proposeBothWithLeader(t, primaryConsensusClient, standbyConsensusClient,
			termRevocation, primaryID, "localhost", int32(setup.PrimaryMultipooler.PgPort))

		t.Log("First Propose complete, verifying primary role...")
		restoredStatus, err := primaryConsensusClient.Status(utils.WithShortDeadline(t), &consensusdatapb.StatusRequest{})
		require.NoError(t, err)
		assert.Equal(t, consensusdatapb.PostgresRole_POSTGRES_ROLE_PRIMARY, restoredStatus.Role,
			"Node should be primary after first Propose")

		// Second Propose with same term revocation — tests idempotency.
		// The accepted term revocation is unchanged so the proposal is still valid.
		t.Log("Calling Propose a second time with the same term (idempotency check)...")
		proposeBothWithLeader(t, primaryConsensusClient, standbyConsensusClient,
			termRevocation, primaryID, "localhost", int32(setup.PrimaryMultipooler.PgPort))

		restoredStatus2, err := primaryConsensusClient.Status(utils.WithShortDeadline(t), &consensusdatapb.StatusRequest{})
		require.NoError(t, err)
		assert.Equal(t, consensusdatapb.PostgresRole_POSTGRES_ROLE_PRIMARY, restoredStatus2.Role,
			"Node should still be primary after second Propose")

		t.Log("Propose idempotency verified - second call succeeds and node remains primary")
	})

	t.Run("TermValidation_EmergencyDemote", func(t *testing.T) {
		setupPoolerTest(t, setup)

		t.Log("Testing EmergencyDemote term validation...")

		ctx := utils.WithShortDeadline(t)
		currentTerm := shardsetup.MustGetCurrentTerm(t, ctx, primaryConsensusClient)
		t.Logf("Current term: %d", currentTerm)

		staleTerm := currentTerm - 2
		if staleTerm < 1 {
			t.Skipf("Skipping test: current term %d is too low for stale term validation (need at least 3)", currentTerm)
		}

		demoteReq := &multipoolermanagerdatapb.EmergencyDemoteRequest{
			ConsensusTerm: staleTerm, // Less than current term
			DrainTimeout:  nil,
			Force:         false,
		}
		_, err = primaryConsensusClient.EmergencyDemote(utils.WithTimeout(t, 10*time.Second), demoteReq)
		require.Error(t, err, "EmergencyDemote with stale term should fail")
		assert.Contains(t, err.Error(), "term")

		// Try with force flag (should succeed even with stale term)
		demoteReq.Force = true
		_, err = primaryConsensusClient.EmergencyDemote(utils.WithTimeout(t, 10*time.Second), demoteReq)
		require.NoError(t, err, "EmergencyDemote with force should succeed")

		// Restore demoted primary to working state
		restoreAfterEmergencyDemotion(t, setup, setup.PrimaryPgctld, setup.PrimaryMultipooler, setup.PrimaryMultipooler.Name)

		t.Log("EmergencyDemote term validation verified")
	})

	t.Run("TermValidation_Propose", func(t *testing.T) {
		setupPoolerTest(t, setup)

		t.Log("Testing Propose term validation...")

		ctx := utils.WithShortDeadline(t)
		currentTerm := shardsetup.MustGetCurrentTerm(t, ctx, primaryConsensusClient)
		t.Logf("Current term: %d", currentTerm)

		// Recruit both nodes with term K+100.
		acceptedRevocation := recruitBoth(t, primaryConsensusClient, standbyConsensusClient, currentTerm)
		acceptedTerm := acceptedRevocation.GetRevokedBelowTerm()

		// Try Propose with a different term (stale relative to what was accepted) — should fail.
		wrongRevocation := &consensusdatapb.TermRevocation{
			RevokedBelowTerm:      acceptedTerm - 1,
			AcceptedCoordinatorId: testCoordinatorID,
		}
		wrongProposal := &consensusdatapb.CoordinatorProposal{
			TermRevocation: wrongRevocation,
			ProposalLeader: &consensusdatapb.ProposalLeader{
				Id:           standbyID,
				Host:         "localhost",
				PostgresPort: int32(setup.StandbyMultipooler.PgPort),
			},
		}
		_, err = primaryConsensusClient.Propose(utils.WithTimeout(t, 5*time.Second),
			&consensusdatapb.ProposeRequest{Proposal: wrongProposal})
		require.Error(t, err, "Propose with mismatched term should fail")
		assert.Contains(t, err.Error(), "term")

		// Propose with the correct term (matching Recruit) — should succeed.
		proposeBothWithLeader(t, primaryConsensusClient, standbyConsensusClient,
			acceptedRevocation, standbyID, "localhost", int32(setup.StandbyMultipooler.PgPort))

		t.Log("Propose term validation verified")
	})

	t.Run("ErrorCases_EmergencyDemoteOnStandby", func(t *testing.T) {
		setupPoolerTest(t, setup)

		t.Log("Testing EmergencyDemote on standby (should fail)...")

		// Use Force=true since we're testing error behavior for demote on standby,
		// not term validation. The demote should fail because PostgreSQL is in standby mode.
		demoteReq := &multipoolermanagerdatapb.EmergencyDemoteRequest{
			ConsensusTerm: 0, // Ignored when Force=true
			DrainTimeout:  nil,
			Force:         true,
		}
		_, err = standbyConsensusClient.EmergencyDemote(context.Background(), demoteReq)
		require.Error(t, err, "EmergencyDemote should fail on standby")
		assert.Contains(t, err.Error(), "standby mode")

		t.Log("Confirmed: EmergencyDemote correctly rejected on standby")
	})

	// Silence unused variable warnings for clients only used in some subtests.
	_ = standbyManagerClient
}
