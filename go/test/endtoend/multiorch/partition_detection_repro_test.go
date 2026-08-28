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
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/eventlog"
	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
	"github.com/multigres/multigres/go/test/utils"

	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
)

// TestPartitionedCellFailoverDetection is a diagnostic repro for MUL-581
// ("speed up failover detection for partitioned cells"), not a committed
// regression test.
//
// 6 poolers span 3 cells, 2 per cell, under MULTI_CELL_AT_LEAST_2. Whichever
// pooler wins the initial election, its cell always has exactly one cell-mate
// — so after killing the leader and freezing that cell-mate's health stream
// (piggybacking SetPostgresRestartsEnabled(false) — see the TEMPORARY
// comments in go/services/multipooler/grpcmanagerservice/service.go and
// .../internal/manager/rpc_manager.go), the "missing" set is 2 poolers but
// spans only 1 cell, which MultiCellPolicy judges as unable to form its own
// quorum (durability requires spanning 2 cells) — so revocation should still
// be judged safe, and the remaining 4 poolers (spanning the other 2 cells)
// should still satisfy the policy going forward. This mirrors "a whole cell
// goes dark" more faithfully than a flat AT_LEAST_2 policy, where any 2
// simultaneously-unreachable poolers look indistinguishable from a live
// quorum and correctly block failover as infeasible (confirmed empirically
// while building this repro).
//
// We measure how long multiorch takes to start a leader appointment once the
// leader and its cell-mate go dark.
func TestPartitionedCellFailoverDetection(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping TestPartitionedCellFailoverDetection test in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("Skipping end-to-end partition detection repro (short mode or no postgres binaries)")
	}

	cells := []string{"cell-a", "cell-b", "cell-c"}
	setup, cleanup := shardsetup.NewIsolated(t,
		shardsetup.WithMultipoolerCount(6),
		shardsetup.WithMultipoolerCells(cells...),
		shardsetup.WithMultiorchCount(1),
		shardsetup.WithDatabase("postgres"),
		shardsetup.WithCellName(cells[0]),
		shardsetup.WithDurabilityPolicy("MULTI_CELL_AT_LEAST_2"),
		// No grace period: we're timing detection, not the deliberate delay
		// before acting on it.
		shardsetup.WithLeaderFailoverGracePeriod("0s", "0s"),
	)
	defer cleanup()

	setup.StartMultiorchs(t.Context(), t)

	primary := setup.GetPrimary(t)
	require.NotNil(t, primary, "primary instance should exist")
	primaryName := setup.PrimaryName
	primaryCell := primary.Multipooler.Cell
	t.Logf("Initial primary: %s (cell=%s)", primaryName, primaryCell)

	// Find the leader's cell-mate: the other pooler placed in the same cell.
	var cellMate string
	for name, inst := range setup.Multipoolers {
		if name != primaryName && inst.Multipooler.Cell == primaryCell {
			cellMate = name
			break
		}
	}
	require.NotEmpty(t, cellMate, "expected exactly one cell-mate for the leader's cell %q", primaryCell)
	t.Logf("Simulating partition for leader's cell-mate: %s", cellMate)

	mateInst := setup.GetMultipoolerInstance(cellMate)
	require.NotNil(t, mateInst, "cell-mate %s instance should exist", cellMate)
	mateClient, err := shardsetup.NewMultipoolerClient(mateInst.Multipooler.GrpcPort)
	require.NoError(t, err)
	_, err = mateClient.Manager.SetPostgresRestartsEnabled(utils.WithShortDeadline(t),
		&multipoolermanagerdatapb.SetPostgresRestartsEnabledRequest{Enabled: false})
	mateClient.Close()
	require.NoError(t, err, "should be able to freeze health streaming on %s", cellMate)

	// Let the freeze take effect (its last snapshot before going silent
	// should already show it healthy and streaming from the leader).
	time.Sleep(1 * time.Second)

	primaryClient, err := shardsetup.NewMultipoolerClient(primary.Multipooler.GrpcPort)
	require.NoError(t, err)
	_, err = primaryClient.Manager.SetPostgresRestartsEnabled(utils.WithShortDeadline(t),
		&multipoolermanagerdatapb.SetPostgresRestartsEnabledRequest{Enabled: false})
	require.NoError(t, err)
	primaryClient.Close()

	t.Logf("Killing postgres on leader %s to trigger failover", primaryName)
	setup.KillPostgres(t, primaryName)
	killedAt := time.Now()

	var mo *shardsetup.ProcessInstance
	for _, inst := range setup.MultiorchInstances {
		mo = inst
		break
	}
	require.NotNil(t, mo, "expected at least one multiorch instance")

	shardsetup.WaitForEvent(t, mo.LogFile, "primary.promotion", string(eventlog.Started), 60*time.Second)
	detectionLatency := time.Since(killedAt)

	t.Logf("MUL-581 repro: leader kill -> AppointLeader start took %v", detectionLatency)

	// TEMPORARY: force log preservation (setup.Cleanup only keeps logs on
	// failure) so we can inspect multiorch.log for the mechanism behind the
	// latency above. Remove once done diagnosing.
	t.Fail()
}
