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
	"fmt"
	"path/filepath"
	"testing"
	"time"

	_ "github.com/lib/pq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	commonconsensus "github.com/multigres/multigres/go/common/consensus"
	"github.com/multigres/multigres/go/common/faultinject"
	"github.com/multigres/multigres/go/common/ha"
	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
	"github.com/multigres/multigres/go/test/utils"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
)

// TestPromotionAppointsExactlyOneLeader verifies that after a primary failure
// exactly one leader appointment completes — no cascade of re-appointments.
//
// If the multipooler prematurely clears POSTGRES_STATUS_PROMOTING before
// postgres is ready, multiorch sees a dead leader and triggers another
// appointment, creating a cascading loop. The rule_history row count is the
// observable invariant: one failover == one promotion record.
func TestPromotionAppointsExactlyOneLeader(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping TestPromotionAppointsExactlyOneLeader in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("skipping: no real postgres binaries available")
	}

	setup, cleanup := shardsetup.NewIsolated(t,
		shardsetup.WithMultipoolerCount(3),
		shardsetup.WithMultiorchCount(1),
		shardsetup.WithMultigateway(),
	)
	defer cleanup()

	setup.StartMultiorchs(t.Context(), t)
	setup.WaitForMultigatewayQueryServing(t)

	primary := setup.GetPrimary(t)
	require.NotNil(t, primary)
	oldPrimaryName := setup.PrimaryName

	// Record the promotion count before the failover so we can assert the delta.
	socketDir := filepath.Join(primary.Pgctld.PoolerDir, "pg_sockets")
	db := connectToPostgres(t, socketDir, primary.Pgctld.PgPort)
	defer db.Close()

	var promotionsBefore int
	err := db.QueryRow(
		`SELECT COUNT(*) FROM multigres.rule_history WHERE event_type = 'promotion'`,
	).Scan(&promotionsBefore)
	require.NoError(t, err, "should be able to query rule_history before failover")

	// Disable postgres auto-restarts before killing postgres. Without this, pgctld
	// would restart postgres within seconds of the SIGKILL — before the new election
	// completes and emergencyDemoteLocked sets rewindPending. A premature restart
	// would bring the old primary back up at term=1, creating split-brain and
	// preventing LeaderIsDead from ever firing.
	primaryManagerClient, err := shardsetup.NewMultipoolerClient(primary.Multipooler.GrpcPort)
	require.NoError(t, err)
	_, err = primaryManagerClient.Manager.SetPostgresRestartsEnabled(
		utils.WithShortDeadline(t),
		&multipoolermanagerdatapb.SetPostgresRestartsEnabledRequest{Enabled: false},
	)
	require.NoError(t, err)
	primaryManagerClient.Close()

	// Kill postgres on the primary to trigger a failover.
	setup.KillPostgres(t, oldPrimaryName)

	// Wait for a new primary to be elected.
	newPrimaryName := shardsetup.WaitForNewPrimary(t, setup, oldPrimaryName, 30*time.Second)
	require.NotEmpty(t, newPrimaryName, "multiorch should elect a new primary after primary death")

	// Re-enable postgres restarts now that the new election is complete.
	// emergencyDemoteLocked has set rewindPending on the old primary, so when
	// postgres restarts it will pg_rewind against the new primary rather than
	// coming back up as a competing primary. This is also required for stale-leader
	// demotion: SetTermPrimary needs postgres running to write the rule.
	primaryManagerClient2, err := shardsetup.NewMultipoolerClient(primary.Multipooler.GrpcPort)
	require.NoError(t, err)
	_, err = primaryManagerClient2.Manager.SetPostgresRestartsEnabled(
		utils.WithShortDeadline(t),
		&multipoolermanagerdatapb.SetPostgresRestartsEnabledRequest{Enabled: true},
	)
	require.NoError(t, err)
	primaryManagerClient2.Close()

	// Wait for the old primary's postgres to finish pg_rewind and restart as a
	// standby before calling RequireRecovery. Without this, the 5-second
	// RecoveryScenarioInitialSettle timeout can expire while pg_rewind is still
	// running, causing ReplicaNotReplicating to never resolve within the window.
	shardsetup.EventuallyPoolerCondition(t, []*shardsetup.MultipoolerInstance{primary}, 30*time.Second, 500*time.Millisecond,
		func(r shardsetup.PoolerStatusResult) (bool, string) {
			if r.Err != nil || r.Status == nil {
				return false, "fetch error"
			}
			if !r.Status.PostgresReady {
				return false, "postgres not ready yet"
			}
			return true, ""
		},
		"old primary postgres should restart and reach ready state within 30s",
	)

	// Let multiorch finish all follow-up work (stale-primary demotion, etc.)
	setup.RequireRecovery(t, "multiorch", shardsetup.RecoveryScenarioInitialSettle)

	// Count promotions on the new primary.
	newPrimary := setup.GetMultipoolerInstance(newPrimaryName)
	require.NotNil(t, newPrimary)
	newSocketDir := filepath.Join(newPrimary.Pgctld.PoolerDir, "pg_sockets")
	newDB := connectToPostgres(t, newSocketDir, newPrimary.Pgctld.PgPort)
	defer newDB.Close()

	var promotionsAfter int
	err = newDB.QueryRow(
		`SELECT COUNT(*) FROM multigres.rule_history WHERE event_type = 'promotion'`,
	).Scan(&promotionsAfter)
	require.NoError(t, err, "should be able to query rule_history after failover")

	// Exactly one new promotion should have been recorded. A cascade of
	// re-appointments would produce two or more rows.
	assert.Equal(t, promotionsBefore+1, promotionsAfter,
		"exactly one promotion should fire per failover (cascade would produce more)")
}

// TestPromotingStatusClearedOnlyWhenReady verifies that a newly promoted node
// reports POSTGRES_STATUS_PROMOTING until its postgres process is accepting
// connections, and transitions to a non-PROMOTING status only after that.
//
// If PROMOTING clears before PostgresReady=true, LeaderNeedsReplacementAnalyzer
// would fire a spurious re-appointment.
func TestPromotingStatusClearedOnlyWhenReady(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping TestPromotingStatusClearedOnlyWhenReady in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("skipping: no real postgres binaries available")
	}

	setup, cleanup := shardsetup.NewIsolated(t,
		shardsetup.WithMultipoolerCount(3),
		shardsetup.WithMultiorchCount(1),
		shardsetup.WithMultigateway(),
	)
	defer cleanup()

	setup.StartMultiorchs(t.Context(), t)
	setup.WaitForMultigatewayQueryServing(t)

	oldPrimaryName := setup.PrimaryName
	primary := setup.GetPrimary(t)
	require.NotNil(t, primary)

	// Disable auto-restarts before the kill so pgctld doesn't bounce postgres
	// before LeaderIsDead fires and the new appointment completes (see comment in
	// TestPromotionAppointsExactlyOneLeader for the full split-brain explanation).
	primaryManagerClient, err := shardsetup.NewMultipoolerClient(primary.Multipooler.GrpcPort)
	require.NoError(t, err)
	_, err = primaryManagerClient.Manager.SetPostgresRestartsEnabled(
		utils.WithShortDeadline(t),
		&multipoolermanagerdatapb.SetPostgresRestartsEnabledRequest{Enabled: false},
	)
	require.NoError(t, err)
	primaryManagerClient.Close()

	setup.KillPostgres(t, oldPrimaryName)

	newPrimaryName := shardsetup.WaitForNewPrimary(t, setup, oldPrimaryName, 30*time.Second)
	require.NotEmpty(t, newPrimaryName)

	// Poll the new primary's status. The invariant: whenever PROMOTING clears,
	// PostgresReady must already be true. We sample repeatedly to catch any
	// window where PROMOTING=false but Ready=false.
	newPrimary := setup.GetMultipoolerInstance(newPrimaryName)
	require.NotNil(t, newPrimary)

	newClient, err := shardsetup.NewMultipoolerClient(newPrimary.Multipooler.GrpcPort)
	require.NoError(t, err)
	defer newClient.Close()

	require.Eventually(t, func() bool {
		resp, err := newClient.Manager.Status(utils.WithShortDeadline(t), &multipoolermanagerdatapb.StatusRequest{})
		if err != nil {
			return false
		}
		promoting := resp.Status.PostgresStatus == clustermetadatapb.PostgresStatus_POSTGRES_STATUS_PROMOTING
		ready := resp.Status.PostgresReady
		// Once PROMOTING clears, postgres must be ready.
		if !promoting {
			assert.True(t, ready,
				"PROMOTING status cleared before PostgresReady=true — LeaderNeedsReplacementAnalyzer would fire a spurious re-appointment")
			return true
		}
		return false
	}, 30*time.Second, 200*time.Millisecond,
		"new primary should leave PROMOTING state within 30s")
}

// TestPromotionHeldBeforePgPromoteIsNotInterrupted holds the new leader's
// Promote after it records the proposed rule but before pg_promote: it believes
// it leads while its postgres is still a standby. No coordinator may take that
// for a demoted leader and recruit over it, so the promotion that started must
// be the one that finishes, at the same term.
func TestPromotionHeldBeforePgPromoteIsNotInterrupted(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping TestPromotionHeldBeforePgPromoteIsNotInterrupted in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("skipping: no real postgres binaries available")
	}

	// Two coordinators, so one can act while the other's Promote is held.
	setup, cleanup := shardsetup.NewIsolated(t,
		shardsetup.WithMultipoolerCount(3),
		shardsetup.WithMultiorchCount(2),
	)
	defer cleanup()
	setup.StartMultiorchs(t.Context(), t)

	oldPrimaryName := setup.PrimaryName
	primary := setup.GetPrimary(t)
	require.NotNil(t, primary)

	// Hold well past failure detection, within the AppointLeader action timeout.
	const hold = 20 * time.Second
	var standbys []*shardsetup.MultipoolerInstance
	for name, inst := range setup.Multipoolers {
		if name == oldPrimaryName {
			continue
		}
		standbys = append(standbys, inst)
		setup.SetFaults(t, name, fmt.Sprintf("%s delay=%s", faultinject.PromoteBeforePgPromote, hold))
	}
	t.Cleanup(func() {
		for _, inst := range standbys {
			setup.SetFaults(t, inst.Name, "")
		}
	})

	// See TestPromotionAppointsExactlyOneLeader for why restarts are disabled.
	setPostgresRestartsEnabled(t, primary, false)
	setup.KillPostgres(t, oldPrimaryName)

	// Catch a standby mid-promotion: it names itself leader, its postgres is a standby.
	var promoting *shardsetup.MultipoolerInstance
	var promotingTerm int64
	require.Eventually(t, func() bool {
		for _, inst := range standbys {
			client, err := shardsetup.NewMultipoolerClient(inst.Multipooler.GrpcPort)
			if err != nil {
				continue
			}
			resp, err := client.Manager.Status(utils.WithShortDeadline(t), &multipoolermanagerdatapb.StatusRequest{})
			client.Close()
			if err != nil {
				continue
			}
			cs := resp.GetConsensusStatus()
			if commonconsensus.SelfConsensusRole(cs) == commonconsensus.ConsensusRoleLeader &&
				resp.GetAvailabilityStatus().GetFacts().GetPostgresStatus() == clustermetadatapb.PostgresStatus_POSTGRES_STATUS_STANDBY {
				promoting = inst
				rule := commonconsensus.HighestIntendedRule([]*clustermetadatapb.ConsensusStatus{cs})
				promotingTerm = commonconsensus.PossiblyUndecidedRule(rule).GetRuleNumber().GetCoordinatorTerm()
				return true
			}
		}
		return false
	}, 30*time.Second, 100*time.Millisecond, "a standby should be held mid-promotion")
	t.Logf("%s is held mid-promotion at term %d", promoting.Name, promotingTerm)

	newPrimaryName := shardsetup.WaitForNewPrimary(t, setup, oldPrimaryName, hold+30*time.Second)
	assert.Equal(t, promoting.Name, newPrimaryName, "the held promotion should finish")

	client, err := shardsetup.NewMultipoolerClient(setup.GetMultipoolerInstance(newPrimaryName).Multipooler.GrpcPort)
	require.NoError(t, err)
	defer client.Close()
	resp, err := client.Manager.Status(utils.WithShortDeadline(t), &multipoolermanagerdatapb.StatusRequest{})
	require.NoError(t, err)
	assert.Equal(t, promotingTerm, leaderTerm(resp.GetConsensusStatus()),
		"no coordinator should recruit over the held promotion")

	setPostgresRestartsEnabled(t, primary, true)
}

// setPostgresRestartsEnabled turns the pooler's automatic postgres restarts on or off.
func setPostgresRestartsEnabled(t *testing.T, inst *shardsetup.MultipoolerInstance, enabled bool) {
	t.Helper()
	client, err := shardsetup.NewMultipoolerClient(inst.Multipooler.GrpcPort)
	require.NoError(t, err)
	defer client.Close()
	_, err = client.Manager.SetPostgresRestartsEnabled(utils.WithShortDeadline(t),
		&multipoolermanagerdatapb.SetPostgresRestartsEnabledRequest{Enabled: enabled})
	require.NoError(t, err)
}

// TestPromotionFailedAfterPgPromoteIsReplaced fails the new leader's Promote
// after pg_promote but before its rule is written: postgres is a primary, its
// WAL still names the old leader, and no Promote will finish. Multiorch must
// treat that as an abandoned promotion and appoint a leader at a higher term.
func TestPromotionFailedAfterPgPromoteIsReplaced(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping TestPromotionFailedAfterPgPromoteIsReplaced in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("skipping: no real postgres binaries available")
	}

	setup, cleanup := shardsetup.NewIsolated(t,
		shardsetup.WithMultipoolerCount(3),
		shardsetup.WithMultiorchCount(1),
	)
	defer cleanup()
	setup.StartMultiorchs(t.Context(), t)

	oldPrimaryName := setup.PrimaryName
	primary := setup.GetPrimary(t)
	require.NotNil(t, primary)

	var standbys []*shardsetup.MultipoolerInstance
	for name, inst := range setup.Multipoolers {
		if name == oldPrimaryName {
			continue
		}
		standbys = append(standbys, inst)
		setup.SetFaults(t, name, fmt.Sprintf("%s fail", faultinject.PromoteAfterPgPromote))
	}
	clearFaults := func() {
		for _, inst := range standbys {
			setup.SetFaults(t, inst.Name, "")
		}
	}
	t.Cleanup(clearFaults)

	setPostgresRestartsEnabled(t, primary, false)
	setup.KillPostgres(t, oldPrimaryName)

	// Wait for a standby left with an abandoned promotion.
	var abandoned *shardsetup.MultipoolerInstance
	var abandonedTerm int64
	require.Eventually(t, func() bool {
		for _, inst := range standbys {
			client, err := shardsetup.NewMultipoolerClient(inst.Multipooler.GrpcPort)
			if err != nil {
				continue
			}
			resp, err := client.Manager.Status(utils.WithShortDeadline(t), &multipoolermanagerdatapb.StatusRequest{})
			client.Close()
			if err != nil {
				continue
			}
			report := ha.PoolerReport{Consensus: resp.GetConsensusStatus(), Availability: resp.GetAvailabilityStatus()}
			if ha.LeaderPromotionPhase(report) == ha.PromotionAbandoned {
				abandoned = inst
				rule := commonconsensus.HighestIntendedRule([]*clustermetadatapb.ConsensusStatus{resp.GetConsensusStatus()})
				abandonedTerm = commonconsensus.PossiblyUndecidedRule(rule).GetRuleNumber().GetCoordinatorTerm()
				return true
			}
		}
		return false
	}, 30*time.Second, 100*time.Millisecond, "a standby's promotion should fail after pg_promote")
	t.Logf("%s abandoned its promotion at term %d", abandoned.Name, abandonedTerm)
	clearFaults()

	newPrimaryName := shardsetup.WaitForNewPrimary(t, setup, oldPrimaryName, 60*time.Second)
	client, err := shardsetup.NewMultipoolerClient(setup.GetMultipoolerInstance(newPrimaryName).Multipooler.GrpcPort)
	require.NoError(t, err)
	defer client.Close()
	require.Eventually(t, func() bool {
		resp, err := client.Manager.Status(utils.WithShortDeadline(t), &multipoolermanagerdatapb.StatusRequest{})
		return err == nil && leaderTerm(resp.GetConsensusStatus()) > abandonedTerm
	}, 60*time.Second, 500*time.Millisecond, "a leader should be appointed above the abandoned term %d", abandonedTerm)

	setPostgresRestartsEnabled(t, primary, true)
}
