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

package multiorch

import (
	"context"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	commonconsensus "github.com/multigres/multigres/go/common/consensus"
	topoclient "github.com/multigres/multigres/go/common/topoclient"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	consensusdatapb "github.com/multigres/multigres/go/pb/consensusdata"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
	"github.com/multigres/multigres/go/test/utils"
)

// TestPropagationRecovery tests that multiorch can recover from a stuck in-WAL
// proposal (one written by a leader who died before the sync-commit quorum gate
// returned). The scenario:
//
//  1. Four-node cluster bootstrapped normally (P primary, R1/R2/R3 standbys).
//  2. R1 and R2 are killed; Recruit is sent to P and R3, then Propose is sent to
//     P with a durability policy that needs two standby ACKs. Only R3 is alive
//     (one ACK), so the quorum gate times out — but P has already written the
//     proposal to WAL and R3 has replicated it.
//  3. P is killed. R1 and R2 are restarted.
//  4. Multiorch is enabled. It detects the in-WAL proposal on R3 and takes the
//     propagation path: Recruit all live nodes, send Propagate to R3, send
//     SetTermPrimary to R1 and R2. Once R1 and R2 connect and ACK, R3 becomes
//     the new primary.
func TestPropagationRecovery(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping TestPropagationRecovery in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("skipping: no postgres binaries available")
	}

	setup, cleanup := shardsetup.NewIsolated(t,
		shardsetup.WithMultipoolerCount(4),
		shardsetup.WithMultiOrchCount(1),
		shardsetup.WithDatabase("postgres"),
		shardsetup.WithCellName("test-cell"),
	)
	defer cleanup()

	setup.StartMultiOrchs(t.Context(), t)

	primaryName := waitForShardReady(t, setup, 3, 60*time.Second)
	t.Logf("Shard ready: primary=%s", primaryName)

	enableRecovery := setup.DisableRecovery(t, "multiorch")

	// --- Phase 1: kill two standbys ------------------------------------------------

	// Collect standby names. Sort for deterministic ordering so R3 (the survivor)
	// is always predictable.
	var standbyNames []string
	for name := range setup.Multipoolers {
		if name != primaryName {
			standbyNames = append(standbyNames, name)
		}
	}
	sort.Strings(standbyNames)
	// standbyNames[0] and [1] will be killed; standbyNames[2] survives as R3.
	require.Len(t, standbyNames, 3, "expected exactly 3 standbys")
	killName1, killName2, r3Name := standbyNames[0], standbyNames[1], standbyNames[2]

	t.Logf("Killing standbys %s and %s; keeping %s as propagation target", killName1, killName2, r3Name)
	resumeKill1 := setup.StopPostgres(t, killName1, "immediate")
	resumeKill2 := setup.StopPostgres(t, killName2, "immediate")

	// --- Phase 2: plant a stuck proposal -------------------------------------------

	pInst := setup.GetMultipoolerInstance(primaryName)
	r3Inst := setup.GetMultipoolerInstance(r3Name)
	require.NotNil(t, pInst)
	require.NotNil(t, r3Inst)

	pClient, err := shardsetup.NewMultipoolerClient(pInst.Multipooler.GrpcPort)
	require.NoError(t, err)
	defer pClient.Close()

	r3Client, err := shardsetup.NewMultipoolerClient(r3Inst.Multipooler.GrpcPort)
	require.NoError(t, err)
	defer r3Client.Close()

	// Get current consensus statuses from the two live nodes.
	pStatusResp, err := pClient.Consensus.Status(utils.WithShortDeadline(t), &consensusdatapb.StatusRequest{})
	require.NoError(t, err, "failed to get P consensus status")
	r3StatusResp, err := r3Client.Consensus.Status(utils.WithShortDeadline(t), &consensusdatapb.StatusRequest{})
	require.NoError(t, err, "failed to get R3 consensus status")

	testCoordinatorID := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIORCH,
		Cell:      setup.CellName,
		Name:      "test-coordinator",
	}
	statuses := []*clustermetadatapb.ConsensusStatus{
		pStatusResp.GetConsensusStatus(),
		r3StatusResp.GetConsensusStatus(),
	}
	revocation, err := commonconsensus.NewTermRevocation(statuses, testCoordinatorID)
	require.NoError(t, err, "failed to build term revocation")
	t.Logf("TermRevocation: term=%d, outgoing_decision=%v", revocation.GetRevokedBelowTerm(), revocation.GetOutgoingDecision())

	// Recruit P and R3 concurrently.
	type recruitResult struct {
		name string
		cs   *clustermetadatapb.ConsensusStatus
	}
	recruitCh := make(chan recruitResult, 2)
	for _, entry := range []struct {
		name   string
		client *shardsetup.MultipoolerClient
	}{
		{primaryName, pClient},
		{r3Name, r3Client},
	} {
		go func() {
			ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
			defer cancel()
			resp, err := entry.client.Consensus.Recruit(ctx, &consensusdatapb.RecruitRequest{
				TermRevocation: revocation,
			})
			if err != nil {
				t.Logf("Recruit failed for %s: %v", entry.name, err)
				recruitCh <- recruitResult{name: entry.name}
				return
			}
			recruitCh <- recruitResult{name: entry.name, cs: resp.GetConsensusStatus()}
		}()
	}

	var pCS, r3CS *clustermetadatapb.ConsensusStatus
	for range 2 {
		rr := <-recruitCh
		switch rr.name {
		case primaryName:
			pCS = rr.cs
		case r3Name:
			r3CS = rr.cs
		}
	}
	require.NotNil(t, pCS, "P must accept Recruit")
	require.NotNil(t, r3CS, "R3 must accept Recruit")
	t.Logf("Both P and R3 accepted Recruit at term %d", revocation.GetRevokedBelowTerm())

	// Recruit on R3 used RECEIVER_ONLY mode, which clears primary_conninfo and stops
	// the WAL receiver. Re-establish R3 streaming from P so R3 will receive the
	// proposal when P writes it during Propose.
	pMultiPooler := &clustermetadatapb.MultiPooler{
		Id:       pCS.GetId(),
		Hostname: "localhost",
		PortMap:  map[string]int32{"postgres": int32(pInst.Pgctld.PgPort)},
	}
	_, err = r3Client.Consensus.SetPrimaryConnInfo(utils.WithTimeout(t, 10*time.Second),
		&multipoolermanagerdatapb.SetPrimaryConnInfoRequest{
			Primary:               pMultiPooler,
			Force:                 true,
			StartReplicationAfter: true,
		})
	require.NoError(t, err, "failed to re-establish R3 streaming from P")
	t.Logf("Re-established R3 streaming from P")

	// Wait for R3's WAL receiver to connect to P.
	require.Eventually(t, func() bool {
		resp, err := r3Client.Manager.Status(utils.WithShortDeadline(t), &multipoolermanagerdatapb.StatusRequest{})
		if err != nil {
			return false
		}
		st := resp.GetStatus()
		return st.GetReplicationStatus().GetWalReceiverStatus() == "streaming"
	}, 15*time.Second, 500*time.Millisecond, "R3 WAL receiver should be streaming from P")
	t.Logf("R3 is streaming from P")

	// Build the CoordinatorProposal. The proposed rule uses the existing cohort
	// (from the bootstrapped decision) but AtLeastN(3) as durability policy.
	// AtLeastN(3) with 4 nodes sets NumSync=2, requiring two standbys to ACK the
	// sync commit. With only R3 alive and streaming, only 1 ACK is possible — the
	// quorum gate will block and the Propose call will time out, leaving an in-WAL
	// proposal on P and R3.
	existingDecision := pCS.GetCurrentPosition().GetDecision()
	require.NotNil(t, existingDecision, "P must have a committed decision after Recruit")

	proposedRuleNumber := &clustermetadatapb.RuleNumber{
		CoordinatorTerm: revocation.GetRevokedBelowTerm(),
	}
	proposedRule := &clustermetadatapb.ShardRule{
		RuleNumber:       proposedRuleNumber,
		CohortMembers:    existingDecision.GetCohortMembers(),
		DurabilityPolicy: topoclient.AtLeastN(3), // requires 2 standby ACKs
		LeaderId:         pCS.GetId(),
	}
	proposal := &consensusdatapb.CoordinatorProposal{
		TermRevocation: revocation,
		ProposalLeader: &clustermetadatapb.PoolerAddress{
			Id:           pCS.GetId(),
			Host:         "localhost",
			PostgresPort: int32(pInst.Pgctld.PgPort),
		},
		ProposedRule: proposedRule,
	}

	// Call Propose to P with a short deadline. P will promote, write the proposal
	// to WAL (COMMIT), then block on the sync-quorum gate waiting for two standby
	// ACKs. The gate will never complete because only R3 is streaming (one ACK,
	// but NumSync=2). The deadline fires, Propose returns an error — but the
	// COMMIT is already durably in P's and R3's WALs.
	t.Log("Sending Propose to P — expecting quorum timeout (this takes ~2s)...")
	proposeCtx, proposeCancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer proposeCancel()
	_, proposeErr := pClient.Consensus.Propose(proposeCtx, &consensusdatapb.ProposeRequest{
		Proposal: proposal,
		AcceptedNodeIds: []*clustermetadatapb.ID{
			pCS.GetId(),
			r3CS.GetId(),
		},
	})
	// The error is expected: context deadline exceeded while waiting for quorum.
	require.Error(t, proposeErr, "Propose should time out waiting for sync-quorum")
	t.Logf("Propose timed out as expected: %v", proposeErr)

	// --- Phase 3: kill the primary -------------------------------------------------

	// Verify that R3 has received the proposal before killing P.
	// R3 should now have a proposal in its WAL from the replication stream.
	require.Eventually(t, func() bool {
		resp, err := r3Client.Consensus.Status(utils.WithShortDeadline(t), &consensusdatapb.StatusRequest{})
		if err != nil {
			return false
		}
		cs := resp.GetConsensusStatus()
		return cs.GetCurrentPosition().GetProposal() != nil
	}, 15*time.Second, 500*time.Millisecond, "R3 should have received the proposal from P")
	t.Logf("R3 has the in-WAL proposal — safe to kill P")

	setup.StopPostgres(t, primaryName, "immediate")
	t.Logf("Killed postgres on primary %s", primaryName)

	// --- Phase 4: bring back killed standbys and enable recovery -------------------

	// Re-enable postgres restarts on the two killed nodes so pgctld restarts them.
	// They'll come up as standbys; their primary_conninfo still points to the old
	// primary — that's fine, orch will fix replication after propagation completes.
	resumeKill1()
	resumeKill2()

	// Wait for the two restarted nodes to have postgres running.
	var restartedInsts []*shardsetup.MultipoolerInstance
	for _, name := range []string{killName1, killName2} {
		restartedInsts = append(restartedInsts, setup.GetMultipoolerInstance(name))
	}
	shardsetup.EventuallyPoolerCondition(t, restartedInsts, 30*time.Second, 1*time.Second,
		func(r shardsetup.PoolerStatusResult) (bool, string) {
			if !r.Status.PostgresReady {
				return false, "postgres not yet running"
			}
			return true, ""
		},
		"killed standbys should have postgres running again within 30s",
	)
	t.Logf("Standbys %s and %s are back up", killName1, killName2)

	// Enable multiorch recovery. Orch will detect R3 carries an in-WAL proposal
	// (propagation_intent set in NewTermRevocation), recruit all live nodes, and
	// drive propagation: Propagate to R3 + SetTermPrimary to R1/R2.
	enableRecovery()
	t.Log("Recovery enabled — waiting for propagation to elect R3 as primary...")

	// --- Phase 5: verify propagation succeeded -------------------------------------

	newPrimaryName := shardsetup.WaitForNewPrimary(t, setup, primaryName, 5*time.Second)
	require.NotEmpty(t, newPrimaryName, "orch should elect a new primary via propagation")
	require.Equal(t, r3Name, newPrimaryName,
		"R3 (the only node carrying the in-WAL proposal) must become the new primary")
	t.Logf("Propagation succeeded: %s is the new primary", newPrimaryName)

	// Verify writes work on the new primary.
	t.Run("writes_work_on_new_primary", func(t *testing.T) {
		socketDir := r3Inst.Pgctld.PoolerDir + "/pg_sockets"
		db := connectToPostgres(t, socketDir, r3Inst.Pgctld.PgPort)
		defer db.Close()

		var result int
		err := db.QueryRow("SELECT 1").Scan(&result)
		require.NoError(t, err, "should be able to query new primary %s", r3Name)
		require.Equal(t, 1, result)
	})
}
