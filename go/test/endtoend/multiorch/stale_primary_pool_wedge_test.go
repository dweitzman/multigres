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
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
	"github.com/multigres/multigres/go/test/utils"

	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
)

// TestStalePrimary_RecoversAfterRewindFailure exercises the failure mode
// observed in a k8s scale-up incident: when multiorch's DemoteStalePrimary
// RPC routes through demoteStalePrimaryLocked and pg_rewind fails partway
// through, the multipooler's connection pool can be left in a permanently
// closed state with no recovery path. The cluster is expected to heal on
// its own once the rewind source becomes available again.
//
// Sequence:
//
//  1. 3-pooler cluster with AT_LEAST_2 durability.
//  2. SIGKILL the primary (P1) → multiorch elects P2 as new primary.
//  3. Write data on P2 so P1's WAL has uncommitted records P2 doesn't.
//  4. Disable postgres restarts on P2, then SIGKILL P2. With restarts disabled
//     and postgres down, P2 cannot serve as the pg_rewind source.
//  5. Re-enable postgres restarts on P1. Its monitor revives postgres at the
//     stale term — P1 is now a stale primary.
//  6. Multiorch detects the stale primary and calls DemoteStalePrimary on P1.
//     The handler routes through demoteStalePrimaryLocked, which calls
//     stopPostgresIfRunning (closing P1's connection pool so postgres can stop
//     cleanly) and then runPgRewind. Because P2 is unreachable, runPgRewind
//     fails. demoteStalePrimaryLocked returns the error to its caller.
//  7. Re-enable postgres restarts on P2 and wait briefly for its monitor to
//     bring postgres back. With a working rewind source available, multiorch
//     should be able to retry DemoteStalePrimary and succeed.
//
// BUG (today): the pool close in stopPostgresIfRunning has no symmetric reopen
// on the error path; only the success path's restartPostgresAsStandby ->
// reopenConnections will reopen the pool. Once pg_rewind fails the pool stays
// closed, every subsequent SetTermPrimary on P1 fails immediately at the
// observePosition call with "manager is closed", and recovery never
// completes — even after P2 comes back. This test FAILS in that state.
//
// FIX: adding a defer in demoteStalePrimaryLocked (or equivalent) that
// reopens connections on the error path lets P1 self-heal. Once P2 is back,
// the next DemoteStalePrimary succeeds and recovery completes. This test
// PASSES in that state.
func TestStalePrimary_RecoversAfterRewindFailure(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stale-primary pool-wedge test in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("Skipping end-to-end stale primary pool-wedge test (no postgres binaries)")
	}

	setup, cleanup := shardsetup.NewIsolated(t,
		shardsetup.WithMultipoolerCount(3),
		shardsetup.WithMultiOrchCount(1),
		shardsetup.WithDatabase("postgres"),
		shardsetup.WithCellName("test-cell"),
	)
	defer cleanup()

	setup.StartMultiOrchs(t.Context(), t)

	oldPrimary := setup.GetPrimary(t)
	require.NotNil(t, oldPrimary, "primary should exist")
	oldPrimaryName := setup.PrimaryName
	t.Logf("Initial primary: %s", oldPrimaryName)

	oldPrimaryClient, err := shardsetup.NewMultipoolerClient(oldPrimary.Multipooler.GrpcPort)
	require.NoError(t, err)
	defer oldPrimaryClient.Close()

	// Disable postgres restarts on the old primary so its monitor doesn't
	// revive postgres before multiorch elects a new leader. Re-enabled after
	// the new primary is in place.
	_, err = oldPrimaryClient.Manager.SetPostgresRestartsEnabled(t.Context(),
		&multipoolermanagerdatapb.SetPostgresRestartsEnabledRequest{Enabled: false})
	require.NoError(t, err)
	defer func() {
		_, _ = oldPrimaryClient.Manager.SetPostgresRestartsEnabled(t.Context(),
			&multipoolermanagerdatapb.SetPostgresRestartsEnabledRequest{Enabled: true})
	}()

	t.Log("Killing postgres on initial primary to trigger failover...")
	setup.KillPostgres(t, oldPrimaryName)

	t.Log("Waiting for new primary election...")
	newPrimaryName := shardsetup.WaitForNewPrimary(t, setup, oldPrimaryName, 30*time.Second)
	require.NotEmpty(t, newPrimaryName, "new primary should be elected")
	t.Logf("New primary elected: %s", newPrimaryName)

	t.Log("Writing data to new primary to ensure timeline divergence...")
	writeDataToNewPrimary(t, setup, newPrimaryName)

	// Take the NEW primary down too, with restarts disabled, so it cannot
	// serve as a pg_rewind source for the soon-to-be-stale old primary.
	t.Logf("Disabling postgres restarts on new primary %s and killing postgres so pg_rewind has no source...", newPrimaryName)
	newPrimary := setup.GetMultipoolerInstance(newPrimaryName)
	require.NotNil(t, newPrimary, "new primary instance should exist")
	newPrimaryClient, err := shardsetup.NewMultipoolerClient(newPrimary.Multipooler.GrpcPort)
	require.NoError(t, err)
	defer newPrimaryClient.Close()
	_, err = newPrimaryClient.Manager.SetPostgresRestartsEnabled(t.Context(),
		&multipoolermanagerdatapb.SetPostgresRestartsEnabledRequest{Enabled: false})
	require.NoError(t, err)
	defer func() {
		_, _ = newPrimaryClient.Manager.SetPostgresRestartsEnabled(t.Context(),
			&multipoolermanagerdatapb.SetPostgresRestartsEnabledRequest{Enabled: true})
	}()
	setup.KillPostgres(t, newPrimaryName)

	// Now revive the old primary's postgres. Its monitor restarts postgres in
	// the original (stale) primary role, and multiorch's StaleLeaderAnalyzer
	// fires DemoteStalePrimary against it. That call enters
	// demoteStalePrimaryLocked, which closes the connection pool via
	// stopPostgresIfRunning and then tries pg_rewind against the new primary —
	// which is down. pg_rewind fails. Without the fix, the pool stays closed.
	t.Log("Re-enabling postgres restarts on old primary so its monitor revives postgres as stale primary...")
	_, err = oldPrimaryClient.Manager.SetPostgresRestartsEnabled(t.Context(),
		&multipoolermanagerdatapb.SetPostgresRestartsEnabledRequest{Enabled: true})
	require.NoError(t, err)

	// Drive multiorch deterministically. TriggerRecoveryOnce runs a single
	// recovery cycle, which will detect the stale primary and call
	// DemoteStalePrimary on it. That RPC enters demoteStalePrimaryLocked,
	// closes the connection pool via stopPostgresIfRunning, attempts
	// pg_rewind against the (down) new primary, fails, and returns. The
	// `remaining` set will include StaleLeader since we couldn't finish
	// the demote — that's expected here; we care about the side effect on
	// the connection pool, not about recovery converging while the source
	// is down.
	t.Log("Triggering recovery to drive the stale-primary demote attempt...")
	remaining := setup.TriggerRecoveryOnce(t, "multiorch", 30*time.Second)
	t.Logf("Recovery cycle returned with %d remaining problems: %v", len(remaining), remaining)

	// Restore the new primary so a subsequent pg_rewind can succeed and the
	// cluster can finish recovering. With the fix in place, multiorch's next
	// DemoteStalePrimary call observes a healthy local pool, runs pg_rewind
	// against the restored source, and completes the demote.
	_, err = newPrimaryClient.Manager.SetPostgresRestartsEnabled(t.Context(),
		&multipoolermanagerdatapb.SetPostgresRestartsEnabledRequest{Enabled: true})
	require.NoError(t, err)
	t.Logf("Re-enabled postgres restarts on new primary %s; waiting for recovery to complete...", newPrimaryName)

	setup.RequireRecovery(t, "multiorch", 90*time.Second)
	t.Log("Recovery completed: old primary rejoined the cluster after pg_rewind failure")

	// Assertion: no outside caller should ever have observed "manager is
	// closed" on the old primary. Today, stopPostgresIfRunning closes the
	// pool inside demoteStalePrimaryLocked and only the success path's
	// restartPostgresAsStandby reopens it via reopenConnections, so the
	// failure path leaks a closed pool until the postgres monitor's next
	// startPostgres cycle reopens it. During that window any RPC that
	// touches postgres (observePosition, heartbeat, role check) surfaces
	// "manager is closed". With the fix (defer reopenConnections in the
	// demote function, or equivalent symmetric cleanup), the pool is
	// reopened before the action lock is released and no caller ever sees
	// the closed state.
	const wedgeError = "manager is closed"
	logPath := oldPrimary.Multipooler.LogFile
	data, err := os.ReadFile(logPath)
	require.NoError(t, err, "should be able to read old primary multipooler log")
	require.NotContains(t, string(data), wedgeError,
		"old primary multipooler log (%s) contains %q — demoteStalePrimaryLocked closed the connection pool on its error path and the close was visible to outside callers. Expected: the function reopens the pool before releasing the action lock.",
		logPath, wedgeError)
}
