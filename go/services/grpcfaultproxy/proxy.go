// Copyright 2025 Supabase, Inc.
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

// Package grpcfaultproxy implements a transparent gRPC proxy with fault injection capabilities.
//
// The proxy accepts HTTP CONNECT requests (for HTTPS_PROXY support) and terminates gRPC
// connections on both sides, allowing full visibility into gRPC methods and metadata.
// This enables targeted fault injection for testing failure scenarios.
package grpcfaultproxy

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"sync"

	"github.com/mwitkow/grpc-proxy/proxy"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"

	"github.com/multigres/multigres/go/common/mterrors"
	pb "github.com/multigres/multigres/go/pb/grpcfaultproxyservice"
)

// Proxy is a transparent gRPC proxy with fault injection capabilities.
type Proxy struct {
	config Config
	logger *slog.Logger

	// ctx is the root context for the proxy
	ctx context.Context

	// cancelFunc cancels the root context
	cancelFunc context.CancelFunc

	// httpServer handles HTTP CONNECT requests
	httpServer *http.Server

	// grpcServer handles gRPC traffic after CONNECT tunneling (transparent proxy)
	grpcServer *grpc.Server

	// managementServer is the separate gRPC server for the management API
	managementServer *grpc.Server

	// managementListener is the TCP listener for the management server
	managementListener net.Listener

	// conns is a cache of backend gRPC connections (target -> connection)
	conns  map[string]*grpc.ClientConn
	connMu sync.RWMutex

	// listener is the TCP listener for the HTTP server
	listener net.Listener

	// engine is the fault injection engine
	engine *Engine
}

// New creates a new Proxy instance with the given configuration.
func New(config Config, logger *slog.Logger) *Proxy {
	//nolint:gocritic // context.Background() is appropriate for top-level service initialization
	ctx, cancel := context.WithCancel(context.Background())

	// Load fault rules if configured
	var rules []FaultRule
	if config.RulesFile != "" {
		var err error
		rules, err = LoadRules(config.RulesFile)
		if err != nil {
			logger.Warn("failed to load fault rules, starting without rules",
				"rules_file", config.RulesFile,
				"error", err)
		} else {
			logger.Info("loaded fault rules",
				"rules_file", config.RulesFile,
				"rule_count", len(rules))
		}
	}

	return &Proxy{
		config:     config,
		logger:     logger,
		ctx:        ctx,
		cancelFunc: cancel,
		conns:      make(map[string]*grpc.ClientConn),
		engine:     NewEngine(rules),
	}
}

// Start starts the proxy server and management API server.
func (p *Proxy) Start() error {
	// Create gRPC server with transparent proxy handler
	p.grpcServer = grpc.NewServer(
		grpc.CustomCodec(proxy.Codec()), //nolint:staticcheck // Required for mwitkow/grpc-proxy transparent proxying
		grpc.UnknownServiceHandler(proxy.TransparentHandler(p.director)),
		grpc.StatsHandler(otelgrpc.NewServerHandler()),
	)

	p.logger.Info("starting gRPC fault injection proxy", "addr", p.config.HTTPAddr)

	// Create TCP listener for proxy traffic
	listener, err := net.Listen("tcp", p.config.HTTPAddr)
	if err != nil {
		return mterrors.Wrapf(err, "failed to listen on %s", p.config.HTTPAddr)
	}
	p.listener = listener

	// Create HTTP server for CONNECT handling
	p.httpServer = p.newHTTPServer(p.config.HTTPAddr, p.grpcServer)

	// Start serving proxy traffic in a goroutine
	go func() {
		if err := p.httpServer.Serve(p.listener); err != nil && err != http.ErrServerClosed {
			p.logger.Error("HTTP server error", "error", err)
		}
	}()

	p.logger.Info("proxy started", "addr", p.listener.Addr().String())

	// Start management API on separate port if configured
	if p.config.ManagementAddr != "" {
		if err := p.startManagementAPI(); err != nil {
			return err
		}
	}

	return nil
}

// startManagementAPI starts the management gRPC API on a separate port.
func (p *Proxy) startManagementAPI() error {
	// Create separate gRPC server for management API
	p.managementServer = grpc.NewServer(
		grpc.StatsHandler(otelgrpc.NewServerHandler()),
	)

	// Register management service
	service := NewService(p)
	pb.RegisterGrpcFaultProxyServiceServer(p.managementServer, service)

	// Create TCP listener for management API
	listener, err := net.Listen("tcp", p.config.ManagementAddr)
	if err != nil {
		return mterrors.Wrapf(err, "failed to listen on management addr %s", p.config.ManagementAddr)
	}
	p.managementListener = listener

	// Create channel to signal when server is ready
	ready := make(chan struct{})

	// Start serving management API in a goroutine
	go func() {
		// Signal that we're about to start serving
		close(ready)
		if err := p.managementServer.Serve(p.managementListener); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			p.logger.Error("management server error", "error", err)
		}
	}()

	// Wait for the server goroutine to start
	<-ready

	p.logger.Info("management API started", "addr", p.managementListener.Addr().String())
	return nil
}

// Stop gracefully stops the proxy server and management API.
func (p *Proxy) Stop() error {
	p.logger.Info("stopping proxy")

	// Cancel context
	p.cancelFunc()

	// Stop gRPC server gracefully
	if p.grpcServer != nil {
		p.grpcServer.GracefulStop()
	}

	// Stop management server gracefully
	if p.managementServer != nil {
		p.managementServer.GracefulStop()
	}

	// Stop HTTP server gracefully
	if p.httpServer != nil {
		//nolint:gocritic // context.Background() is appropriate for graceful shutdown
		if err := p.httpServer.Shutdown(context.Background()); err != nil {
			p.logger.Error("failed to shutdown HTTP server", "error", err)
		}
	}

	// Close all backend connections
	p.closeBackendConns()

	p.logger.Info("proxy stopped")
	return nil
}

// Addr returns the address the proxy is listening on for CONNECT requests.
// Returns empty string if not started.
func (p *Proxy) Addr() string {
	if p.listener == nil {
		return ""
	}
	return p.listener.Addr().String()
}

// ManagementAddr returns the address the management API is listening on.
// Returns empty string if management server is not started.
func (p *Proxy) ManagementAddr() string {
	if p.managementListener == nil {
		return ""
	}
	return p.managementListener.Addr().String()
}

// Wait blocks until the proxy context is cancelled.
func (p *Proxy) Wait() {
	<-p.ctx.Done()
}
