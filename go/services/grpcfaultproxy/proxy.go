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
	"log/slog"
	"net"
	"net/http"
	"sync"

	"github.com/mwitkow/grpc-proxy/proxy"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"

	"github.com/multigres/multigres/go/common/mterrors"
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

	// grpcServer handles gRPC traffic after CONNECT tunneling
	grpcServer *grpc.Server

	// conns is a cache of backend gRPC connections (target -> connection)
	conns  map[string]*grpc.ClientConn
	connMu sync.RWMutex

	// listener is the TCP listener for the HTTP server
	listener net.Listener
}

// New creates a new Proxy instance with the given configuration.
func New(config Config, logger *slog.Logger) *Proxy {
	//nolint:gocritic // context.Background() is appropriate for top-level service initialization
	ctx, cancel := context.WithCancel(context.Background())

	return &Proxy{
		config:     config,
		logger:     logger,
		ctx:        ctx,
		cancelFunc: cancel,
		conns:      make(map[string]*grpc.ClientConn),
	}
}

// Start starts the proxy server.
func (p *Proxy) Start() error {
	// Create gRPC server with transparent proxy handler
	p.grpcServer = grpc.NewServer(
		grpc.CustomCodec(proxy.Codec()), //nolint:staticcheck // Required for mwitkow/grpc-proxy transparent proxying
		grpc.UnknownServiceHandler(proxy.TransparentHandler(p.director)),
		grpc.StatsHandler(otelgrpc.NewServerHandler()),
	)

	p.logger.Info("starting gRPC fault injection proxy", "addr", p.config.HTTPAddr)

	// Create TCP listener
	listener, err := net.Listen("tcp", p.config.HTTPAddr)
	if err != nil {
		return mterrors.Wrapf(err, "failed to listen on %s", p.config.HTTPAddr)
	}
	p.listener = listener

	// Create HTTP server for CONNECT handling
	p.httpServer = p.newHTTPServer(p.config.HTTPAddr, p.grpcServer)

	// Start serving in a goroutine
	go func() {
		if err := p.httpServer.Serve(p.listener); err != nil && err != http.ErrServerClosed {
			p.logger.Error("HTTP server error", "error", err)
		}
	}()

	p.logger.Info("proxy started", "addr", p.listener.Addr().String())
	return nil
}

// Stop gracefully stops the proxy server.
func (p *Proxy) Stop() error {
	p.logger.Info("stopping proxy")

	// Cancel context
	p.cancelFunc()

	// Stop gRPC server gracefully
	if p.grpcServer != nil {
		p.grpcServer.GracefulStop()
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

// Addr returns the address the proxy is listening on.
// Returns empty string if not started.
func (p *Proxy) Addr() string {
	if p.listener == nil {
		return ""
	}
	return p.listener.Addr().String()
}

// Wait blocks until the proxy context is cancelled.
func (p *Proxy) Wait() {
	<-p.ctx.Done()
}
