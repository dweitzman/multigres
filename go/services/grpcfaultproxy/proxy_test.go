// Copyright 2025 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package grpcfaultproxy

import (
	"context"
	"log/slog"
	"net"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	pb "github.com/multigres/multigres/go/pb/grpcfaultproxyservice"
	"github.com/multigres/multigres/go/tools/grpccommon"
)

// TestProxyBasicForwarding tests that the proxy can forward a simple gRPC health check.
func TestProxyBasicForwarding(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))

	// Start a backend gRPC server
	backendAddr := startTestBackend(t, ctx)
	logger.Info("backend started", "addr", backendAddr)

	// Start the proxy
	proxyAddr := startTestProxy(t, ctx, logger)
	logger.Info("proxy started", "addr", proxyAddr)

	// Create a client configured to use the proxy
	client := createProxiedClient(t, ctx, backendAddr, proxyAddr, "test-client")
	defer client.Close()

	// Make a health check call through the proxy
	healthClient := healthpb.NewHealthClient(client)
	resp, err := healthClient.Check(ctx, &healthpb.HealthCheckRequest{})
	require.NoError(t, err, "health check through proxy should succeed")
	require.Equal(t, healthpb.HealthCheckResponse_SERVING, resp.Status)

	logger.Info("✓ Health check through proxy successful")
}

// startTestBackend starts a simple gRPC server with health service.
func startTestBackend(t *testing.T, ctx context.Context) string {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	server := grpc.NewServer()
	healthpb.RegisterHealthServer(server, &testHealthServer{})

	go func() {
		if err := server.Serve(listener); err != nil {
			t.Logf("backend server error: %v", err)
		}
	}()

	t.Cleanup(func() {
		server.GracefulStop()
	})

	return listener.Addr().String()
}

// startTestProxy starts the fault injection proxy.
func startTestProxy(t *testing.T, ctx context.Context, logger *slog.Logger) string {
	config := Config{
		HTTPAddr:       "127.0.0.1:0",
		ManagementAddr: "127.0.0.1:0",
	}

	proxy := New(config, logger)

	err := proxy.Start()
	require.NoError(t, err)

	t.Cleanup(func() {
		_ = proxy.Stop()
	})

	// Extract the actual bound address
	return proxy.listener.Addr().String()
}

// createProxiedClient creates a gRPC client configured to use the HTTP proxy.
func createProxiedClient(t *testing.T, ctx context.Context, targetAddr, proxyAddr, sourceID string) *grpc.ClientConn {
	// Use FORCE_GRPC_HTTPS_PROXY to force connections through proxy (even localhost)
	// This matches how the endtoend tests configure clients
	os.Setenv("FORCE_GRPC_HTTPS_PROXY", proxyAddr)
	os.Setenv("GRPC_PROXY_SOURCE_ID", sourceID)
	t.Cleanup(func() {
		os.Unsetenv("FORCE_GRPC_HTTPS_PROXY")
		os.Unsetenv("GRPC_PROXY_SOURCE_ID")
	})

	// Use grpccommon.NewClient which automatically picks up proxy environment variables
	conn, err := grpccommon.NewClient(
		targetAddr,
		grpccommon.WithDialOptions(grpc.WithTransportCredentials(insecure.NewCredentials())),
	)
	require.NoError(t, err)

	return conn
}

// TestProxyFaultInjection tests that the proxy can inject faults (return errors).
func TestProxyFaultInjection(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))

	// Start a backend gRPC server
	backendAddr := startTestBackend(t, ctx)
	logger.Info("backend started", "addr", backendAddr)

	// Start the proxy
	proxy := startTestProxyInstance(t, ctx, logger)
	proxyAddr := proxy.listener.Addr().String()
	managementAddr := proxy.managementListener.Addr().String()
	logger.Info("proxy started", "addr", proxyAddr, "management", managementAddr)

	// Create a management client to configure fault rules
	mgmtConn, err := grpc.NewClient(managementAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	defer mgmtConn.Close()

	// Use the protobuf types to configure fault injection
	mgmtClient := pb.NewGrpcFaultProxyServiceClient(mgmtConn)

	_, err = mgmtClient.SetRules(ctx, &pb.SetRulesRequest{
		Rules: []*pb.FaultRule{
			{
				Name:        "block-test-client",
				Source:      "test-client",
				Target:      "*",
				Method:      "*",
				FaultType:   "error",
				Probability: 1.0,
				ErrorCode:   14, // codes.Unavailable
				ErrorMsg:    "simulated network partition",
			},
		},
	})
	require.NoError(t, err, "failed to set fault injection rules")
	logger.Info("fault injection enabled: test-client -> UNAVAILABLE")

	// Create a client configured to use the proxy
	client := createProxiedClient(t, ctx, backendAddr, proxyAddr, "test-client")
	defer client.Close()

	// Make a health check call - should be blocked by fault injection
	healthClient := healthpb.NewHealthClient(client)
	resp, err := healthClient.Check(ctx, &healthpb.HealthCheckRequest{})

	// Verify we got the injected error
	require.Error(t, err, "expected error from fault injection")
	require.Contains(t, err.Error(), "Unavailable", "expected Unavailable error code")
	require.Contains(t, err.Error(), "simulated network partition", "expected custom error message")
	require.Nil(t, resp, "expected nil response")

	logger.Info("✓ Fault injection test successful - request blocked as expected")
}

// startTestProxyInstance starts the proxy and returns the instance (not just address).
func startTestProxyInstance(t *testing.T, ctx context.Context, logger *slog.Logger) *Proxy {
	config := Config{
		HTTPAddr:       "127.0.0.1:0",
		ManagementAddr: "127.0.0.1:0",
	}

	proxy := New(config, logger)

	err := proxy.Start()
	require.NoError(t, err)

	t.Cleanup(func() {
		_ = proxy.Stop()
	})

	return proxy
}

// testHealthServer implements a simple health check service.
type testHealthServer struct {
	healthpb.UnimplementedHealthServer
}

func (s *testHealthServer) Check(ctx context.Context, req *healthpb.HealthCheckRequest) (*healthpb.HealthCheckResponse, error) {
	return &healthpb.HealthCheckResponse{
		Status: healthpb.HealthCheckResponse_SERVING,
	}, nil
}
