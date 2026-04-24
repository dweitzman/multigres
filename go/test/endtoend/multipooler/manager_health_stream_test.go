// Copyright 2026 Supabase, Inc.
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

package multipooler

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/test/endtoend/shardsetup"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	consensusdatapb "github.com/multigres/multigres/go/pb/consensusdata"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
)

// TestManagerHealthStream_SnapshotOnRecruit verifies that a snapshot is pushed to
// an open ManagerHealthStream promptly after Recruit is called. Recruit calls
// broadcastHealth() at the end of its work, so the stream client should not need
// to wait for the periodic poll ticker.
func TestManagerHealthStream_SnapshotOnRecruit(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping end-to-end tests in short mode")
	}

	setup := getSharedTestSetup(t)
	setupPoolerTest(t, setup, WithoutReplication())

	waitForManagerReady(t, setup, setup.StandbyMultipooler)

	// Connect to the standby's manager service.
	standbyClient, err := shardsetup.NewMultipoolerClient(setup.StandbyMultipooler.GrpcPort)
	require.NoError(t, err)
	t.Cleanup(func() { standbyClient.Close() })

	// Open a ManagerHealthStream. Cancel it at test teardown.
	streamCtx, cancelStream := context.WithCancel(context.Background())
	t.Cleanup(cancelStream)

	stream, err := standbyClient.Manager.ManagerHealthStream(streamCtx)
	require.NoError(t, err)

	// Send the start message to open the stream.
	require.NoError(t, stream.Send(&multipoolermanagerdatapb.ManagerHealthStreamClientMessage{
		Message: &multipoolermanagerdatapb.ManagerHealthStreamClientMessage_Start{
			Start: &multipoolermanagerdatapb.ManagerHealthStreamStartRequest{},
		},
	}))

	// Drain the initial snapshot that the server sends immediately on connection.
	initial, err := stream.Recv()
	require.NoError(t, err, "expected initial snapshot on stream open")
	require.Equal(t, multipoolermanagerdatapb.SnapshotTrigger_SNAPSHOT_TRIGGER_INITIAL,
		initial.GetSnapshot().GetTrigger(), "first message should be an initial snapshot")

	// Call Recruit on the standby with a higher term. Recruit calls broadcastHealth()
	// at the end of its work, which should cause the stream to send a broadcast-triggered snapshot.
	_, err = standbyClient.Consensus.Recruit(t.Context(), &consensusdatapb.RecruitRequest{
		TermRevocation: &consensusdatapb.TermRevocation{
			RevokedBelowTerm: 2,
			AcceptedCoordinatorId: &clustermetadatapb.ID{
				Component: clustermetadatapb.ID_MULTIORCH,
				Cell:      setup.CellName,
				Name:      "test-coordinator",
			},
		},
	})
	require.NoError(t, err, "Recruit should succeed on standby")

	// Receive snapshots until we see one with SNAPSHOT_TRIGGER_BROADCAST.
	// We skip over any heartbeat snapshots that may arrive concurrently.
	// We use the test context so there is no arbitrary deadline shorter than the
	// overall test timeout.
	for {
		snapCh := make(chan *multipoolermanagerdatapb.ManagerHealthStreamResponse, 1)
		go func() {
			msg, err := stream.Recv()
			if err == nil {
				snapCh <- msg
			}
		}()

		select {
		case snap := <-snapCh:
			require.NotNil(t, snap.GetSnapshot(), "received message should contain a snapshot")
			if snap.GetSnapshot().GetTrigger() == multipoolermanagerdatapb.SnapshotTrigger_SNAPSHOT_TRIGGER_BROADCAST {
				return // test passed
			}
			// Not a broadcast snapshot (e.g. a concurrent heartbeat); keep waiting.
		case <-t.Context().Done():
			t.Fatal("no broadcast snapshot received before test timeout — SetPrimaryConnInfo may not be triggering a broadcast")
		}
	}
}
