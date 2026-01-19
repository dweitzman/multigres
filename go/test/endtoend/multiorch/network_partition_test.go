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

package multiorch

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	pb "github.com/multigres/multigres/go/pb/grpcfaultproxyservice"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
)

// TestNetworkPartition_MultiOrchFailover tests that multiorch correctly performs failover
// when the primary multipooler becomes unreachable due to a simulated network partition.
//
// Test scenario:
// 1. Set up cluster with 3 multipoolers and 1 multiorch
// 2. Proxy starts with no faults - let cluster bootstrap normally
// 3. Wait for multiorch to bootstrap and elect a primary
// 4. Inject network partition: all RPC requests from multiorch to primary fail immediately
// 5. Verify multiorch detects the partition and elects a new primary
// 6. Verify the new primary is promoted successfully
func TestNetworkPartition_MultiOrchFailover(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	// Create shard setup with 3 multipoolers and 1 multiorch
	// Note: GRPC_PROXY_SOURCE_ID is now set per-process in ProcessInstance.startMultiOrch()
	// The shared setup includes an always-on proxy with no initial faults
	// This allows failover: primary goes down, multiorch can elect one of the 2 standbys
	setup := shardsetup.New(t,
		shardsetup.WithMultipoolerCount(3),
		shardsetup.WithMultiOrchCount(1),
	)

	// Bootstrap is complete, but multiorch was stopped after bootstrap
	// Start multiorch now so it can monitor the cluster and detect the partition
	t.Log("Starting multiorch to monitor cluster...")
	setup.StartMultiOrchs(t)

	// Wait for multiorch to start monitoring
	time.Sleep(5 * time.Second)

	// Get the current primary
	primary := setup.GetPrimary(t)
	require.NotNil(t, primary, "expected a primary to be elected")
	t.Logf("Current primary: %s (gRPC port: %d)", primary.Name, primary.Multipooler.GrpcPort)

	// Save original primary name for later comparison
	originalPrimaryName := primary.Name

	// Inject network partition: multiorch → primary communication fails immediately
	// Match the target using the port number since that's what gRPC clients see
	partitionRules := []*pb.FaultRule{
		{
			Name:        "partition-multiorch-from-primary",
			Source:      "multiorch",
			Target:      fmt.Sprintf("*:%d", primary.Multipooler.GrpcPort),
			Method:      "*",
			FaultType:   "error",
			Probability: 1.0,
			ErrorCode:   14, // codes.Unavailable
			ErrorMsg:    "simulated network partition",
		},
	}

	t.Logf("Injecting network partition between multiorch and primary (port %d)...", primary.Multipooler.GrpcPort)
	setup.EnableFaultInjection(t, partitionRules)

	// Clear faults at test cleanup
	t.Cleanup(func() {
		setup.ClearFaultInjection(t)
	})

	// Wait for multiorch to detect the partition and trigger failover
	// Multiorch health checks run every few seconds, so failover should happen within ~60s
	t.Log("Waiting for multiorch to detect partition and elect new primary...")

	// Poll for primary change by querying cluster state
	var newPrimary *shardsetup.MultipoolerInstance
	require.Eventually(t, func() bool {
		// Query each pooler to see if it's primary
		for name, inst := range setup.Multipoolers {
			if name == originalPrimaryName {
				continue // Skip the partitioned original primary
			}

			// Check if this pooler is now primary
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			client, err := shardsetup.NewMultipoolerClient(inst.Multipooler.GrpcPort)
			if err != nil {
				cancel()
				continue
			}

			statusResp, err := client.Manager.Status(ctx, &multipoolermanagerdatapb.StatusRequest{})
			client.Close()
			cancel()

			if err != nil || statusResp.Status == nil {
				continue
			}

			if statusResp.Status.PoolerType == clustermetadatapb.PoolerType_PRIMARY {
				newPrimary = inst
				t.Logf("New primary elected: %s", name)
				return true
			}
		}
		return false
	}, 90*time.Second, 3*time.Second, "multiorch should elect a new primary after partition")

	// Verify the new primary is one of the standbys (not the partitioned primary)
	require.NotNil(t, newPrimary, "expected a new primary to be elected")
	require.NotEqual(t, originalPrimaryName, newPrimary.Name, "new primary should be different from partitioned primary")

	t.Logf("SUCCESS: Failover completed. Old primary: %s, New primary: %s", originalPrimaryName, newPrimary.Name)
}
