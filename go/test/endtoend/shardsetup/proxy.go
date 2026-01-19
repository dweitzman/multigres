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

package shardsetup

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "github.com/multigres/multigres/go/pb/grpcfaultproxyservice"
	"github.com/multigres/multigres/go/services/grpcfaultproxy"
	"github.com/multigres/multigres/go/test/utils"
)

// startProxy starts the gRPC fault injection proxy for the test cluster.
// The proxy is always running but starts with no fault injection rules.
// Tests can use EnableFaultInjection() to inject faults dynamically.
func (s *ShardSetup) startProxy(t *testing.T) error {
	t.Helper()

	// Get available ports for proxy and management API
	httpPort := utils.GetFreePort(t)
	managementPort := utils.GetFreePort(t)

	// Create logger
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))

	// Create proxy with no initial rules, separate management port
	config := grpcfaultproxy.Config{
		HTTPAddr:       fmt.Sprintf("127.0.0.1:%d", httpPort),
		ManagementAddr: fmt.Sprintf("127.0.0.1:%d", managementPort),
	}

	proxy := grpcfaultproxy.New(config, logger)

	// Start proxy
	if err := proxy.Start(); err != nil {
		return err
	}

	// Store proxy instance
	proxyProcess := &ProcessInstance{
		Name:   "grpcfaultproxy",
		Binary: "grpcfaultproxy",
	}

	s.GrpcFaultProxy = proxyProcess

	// Set FORCE_GRPC_HTTPS_PROXY environment variable for all gRPC clients
	// This forces localhost connections through the proxy (see grpccommon/test_options.go)
	proxyAddr := proxy.Addr()
	managementAddr := proxy.ManagementAddr()
	if err := os.Setenv("FORCE_GRPC_HTTPS_PROXY", proxyAddr); err != nil {
		return err
	}

	t.Logf("Started gRPC fault injection proxy at %s (management API at %s)", proxyAddr, managementAddr)

	// Wait for management API to be ready by calling GetRules
	healthConn, err := grpc.NewClient(managementAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return fmt.Errorf("management API connection failed: %w", err)
	}
	defer healthConn.Close()

	healthClient := pb.NewGrpcFaultProxyServiceClient(healthConn)

	// Retry health check for a few seconds
	healthCtx, healthCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer healthCancel()

	for {
		_, err = healthClient.GetRules(healthCtx, &pb.GetRulesRequest{})
		if err == nil {
			break
		}

		select {
		case <-healthCtx.Done():
			return fmt.Errorf("management API health check timed out: %w", err)
		case <-time.After(100 * time.Millisecond):
			// Retry
		}
	}

	t.Logf("Management API health check passed")

	// Register cleanup to stop proxy
	t.Cleanup(func() {
		_ = os.Unsetenv("FORCE_GRPC_HTTPS_PROXY")
		_ = proxy.Stop()
	})

	// Store management address for EnableFaultInjection
	s.ProxyRulesFile = managementAddr

	return nil
}

// EnableFaultInjection updates the proxy rules to enable fault injection.
// The rules are sent to the proxy's management API and take effect immediately.
// Call ClearFaultInjection() in test cleanup to restore normal behavior.
//
// Example:
//
//	rules := []*pb.FaultRule{
//	    {
//	        Name:        "partition-multiorch-from-primary",
//	        Source:      "multiorch",
//	        Target:      "*:16100",
//	        Method:      "*",
//	        FaultType:   "error",
//	        Probability: 1.0,
//	        ErrorCode:   14, // codes.Unavailable
//	        ErrorMsg:    "simulated network partition",
//	    },
//	}
//	setup.EnableFaultInjection(t, rules)
func (s *ShardSetup) EnableFaultInjection(t *testing.T, rules []*pb.FaultRule) {
	t.Helper()

	if s.ProxyRulesFile == "" {
		t.Fatal("proxy management address not set - proxy not started?")
	}

	// Connect to proxy management API
	conn, err := grpc.NewClient(
		s.ProxyRulesFile,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err, "failed to connect to proxy management API")
	defer conn.Close()

	client := pb.NewGrpcFaultProxyServiceClient(conn)

	// Set rules with a fresh context (not derived from test context to avoid cancellation)
	rpcCtx, rpcCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer rpcCancel()
	_, err = client.SetRules(rpcCtx, &pb.SetRulesRequest{
		Rules: rules,
	})
	require.NoError(t, err, "failed to set fault injection rules")

	t.Logf("Enabled fault injection with %d rules", len(rules))
}

// ClearFaultInjection removes all fault injection rules, restoring normal proxy behavior.
// Call this in test cleanup to ensure subsequent tests aren't affected.
func (s *ShardSetup) ClearFaultInjection(t *testing.T) {
	t.Helper()

	if s.ProxyRulesFile == "" {
		return // No proxy, nothing to clear
	}

	// Connect to proxy management API
	conn, err := grpc.NewClient(
		s.ProxyRulesFile,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err, "failed to connect to proxy management API")
	defer conn.Close()

	client := pb.NewGrpcFaultProxyServiceClient(conn)

	// Clear all rules with fresh context (not derived from test context)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err = client.SetRules(ctx, &pb.SetRulesRequest{
		Rules: []*pb.FaultRule{}, // Empty list = no faults
	})
	require.NoError(t, err, "failed to clear fault injection rules")

	t.Logf("Cleared all fault injection rules")
}
