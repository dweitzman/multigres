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

package grpcfaultproxy

import (
	"context"
	"log/slog"
	"net"
	"net/http"

	"github.com/mwitkow/grpc-proxy/proxy"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
	"google.golang.org/grpc"
)

type contextKey string

// ServeHTTP handles both HTTP CONNECT requests (for proxying) and direct gRPC requests (for management API).
// This allows the proxy to work with gRPC's native HTTPS_PROXY support while also serving the management API.
func (p *Proxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	// CONNECT method is for proxy tunneling
	// Non-CONNECT requests are direct gRPC calls (e.g., management API)
	if r.Method != http.MethodConnect {
		p.logger.InfoContext(ctx, "handling direct gRPC request",
			"method", r.Method,
			"url", r.URL.String(),
			"proto", r.Proto)
		// Forward to gRPC server (management API and other registered services)
		p.grpcServer.ServeHTTP(w, r)
		p.logger.InfoContext(ctx, "direct gRPC request completed")
		return
	}

	p.logger.DebugContext(ctx, "handling CONNECT request", "host", r.Host)

	// Hijack the connection to get raw TCP socket
	hijacker, ok := w.(http.Hijacker)
	if !ok {
		p.logger.ErrorContext(ctx, "response writer does not support hijacking")
		http.Error(w, "Hijacking not supported", http.StatusInternalServerError)
		return
	}

	clientConn, _, err := hijacker.Hijack()
	if err != nil {
		p.logger.ErrorContext(ctx, "failed to hijack connection", "error", err)
		http.Error(w, "Failed to hijack connection", http.StatusInternalServerError)
		return
	}

	// Important: Write "200 Connection Established" before hijacking completes
	// This tells the client the tunnel is ready
	_, err = clientConn.Write([]byte("HTTP/1.1 200 Connection Established\r\n\r\n"))
	if err != nil {
		p.logger.ErrorContext(ctx, "failed to write CONNECT response", "error", err)
		_ = clientConn.Close()
		return
	}

	p.logger.DebugContext(ctx, "CONNECT tunnel established", "host", r.Host)

	// Now handle the gRPC connection with forwarding to backend
	go p.handleGRPCConnection(ctx, clientConn, r.Host)
}

// handleGRPCConnection serves gRPC traffic over the hijacked connection.
//
// After the HTTP CONNECT handshake, the connection becomes a clean TCP socket
// ready for HTTP/2 negotiation. We create a single-connection listener and pass it
// to grpc.Server.Serve(), which will:
// 1. Accept the one connection
// 2. Negotiate HTTP/2 (client preface, SETTINGS frames)
// 3. Handle gRPC requests using our TransparentHandler
// 4. Call our director() for each request to route and apply faults
func (p *Proxy) handleGRPCConnection(ctx context.Context, clientConn net.Conn, targetAddr string) {
	// Store target address in context so director can access it
	ctx = context.WithValue(ctx, contextKey("targetAddr"), targetAddr)

	// Create a NEW grpc.Server instance for this connection
	// We cannot reuse p.grpcServer because it's already being served by the HTTP server
	connServer := grpc.NewServer(
		grpc.CustomCodec(proxy.Codec()), //nolint:staticcheck // Required by mwitkow/grpc-proxy
		grpc.UnknownServiceHandler(proxy.TransparentHandler(p.director)),
		grpc.StatsHandler(otelgrpc.NewServerHandler()),
	)

	// Create a listener that yields this one connection, then closes
	listener := &singleConnListener{
		ctx:  ctx,
		conn: clientConn,
		done: make(chan struct{}),
	}

	p.logger.DebugContext(ctx, "serving hijacked connection", "target", targetAddr)

	// Serve the connection. This will:
	// 1. Call Accept() once, get the connection, spawn goroutines to handle it
	// 2. Call Accept() again, get net.ErrClosed, return immediately
	// The spawned goroutines will continue handling the connection until the client closes it.
	if err := connServer.Serve(listener); err != nil {
		// Ignore the expected error when the listener closes after yielding one connection
		if !isClosedNetworkError(err) {
			p.logger.ErrorContext(ctx, "gRPC serve error", "error", err, "target", targetAddr)
		}
	}

	// Serve() has returned, but the connection is still being handled by goroutines it spawned.
	// They will run until the client closes the connection.
	p.logger.DebugContext(ctx, "serve loop exited for connection", "target", targetAddr)
}

// isClosedNetworkError checks if an error is the expected "closed network connection" error.
func isClosedNetworkError(err error) bool {
	if err == nil {
		return false
	}
	errStr := err.Error()
	return errStr == "accept tcp: use of closed network connection" ||
		errStr == "use of closed network connection"
}

// singleConnListener is a net.Listener that yields a single connection then closes.
// This allows us to use grpc.Server.Serve() with a hijacked connection.
type singleConnListener struct {
	ctx  context.Context
	conn net.Conn
	done chan struct{}
}

func (l *singleConnListener) Accept() (net.Conn, error) {
	select {
	case <-l.done:
		return nil, net.ErrClosed
	default:
		close(l.done)
		return l.conn, nil
	}
}

func (l *singleConnListener) Close() error {
	select {
	case <-l.done:
	default:
		close(l.done)
	}
	return nil
}

func (l *singleConnListener) Addr() net.Addr {
	return l.conn.LocalAddr()
}

// newHTTPServer creates an HTTP server configured to handle CONNECT requests and direct gRPC.
func (p *Proxy) newHTTPServer(addr string, grpcServer *grpc.Server) *http.Server {
	// Create HTTP/2 server using h2c (HTTP/2 cleartext)
	// This allows HTTP/2 without TLS, which is what we need for the proxy
	h2s := &http2.Server{}

	// Create a handler that can serve both CONNECT (proxying) and direct gRPC (management API)
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodConnect {
			// CONNECT requests go through our proxy handler
			p.ServeHTTP(w, r)
		} else {
			// Direct HTTP/2 requests go straight to gRPC server (management API)
			p.logger.InfoContext(r.Context(), "direct request to gRPC server",
				"method", r.Method,
				"url", r.URL.String())
			grpcServer.ServeHTTP(w, r)
		}
	})

	return &http.Server{ //nolint:gosec // ReadHeaderTimeout not needed - this is for testing only, not production
		Addr:    addr,
		Handler: h2c.NewHandler(handler, h2s),
		BaseContext: func(net.Listener) context.Context {
			return p.ctx
		},
	}
}

// LoggingHandler wraps an http.Handler and logs requests.
func LoggingHandler(logger *slog.Logger, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		logger.DebugContext(r.Context(), "http request",
			"method", r.Method,
			"url", r.URL.String(),
			"host", r.Host,
			"remote_addr", r.RemoteAddr)
		next.ServeHTTP(w, r)
	})
}
