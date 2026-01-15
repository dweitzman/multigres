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
	"io"
	"log/slog"
	"net"
	"net/http"

	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
	"google.golang.org/grpc"
)

// ServeHTTP handles HTTP CONNECT requests and upgrades them to gRPC connections.
// This allows the proxy to work with gRPC's native HTTPS_PROXY support.
func (p *Proxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	// Only handle CONNECT method for proxy tunneling
	if r.Method != http.MethodConnect {
		p.logger.WarnContext(ctx, "rejecting non-CONNECT request",
			"method", r.Method,
			"url", r.URL.String())
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
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

	// Now hand off the connection to the gRPC server
	// The gRPC server will handle HTTP/2 negotiation and stream the RPCs
	go p.handleGRPCConnection(ctx, clientConn)
}

// handleGRPCConnection serves gRPC traffic over the hijacked connection.
func (p *Proxy) handleGRPCConnection(ctx context.Context, conn net.Conn) {
	defer func() {
		if err := conn.Close(); err != nil {
			p.logger.DebugContext(ctx, "error closing connection", "error", err)
		}
	}()

	// Serve the connection using the gRPC server
	// The gRPC server is configured with UnknownServiceHandler to proxy all methods
	p.grpcServer.ServeHTTP(&connResponseWriter{conn: conn}, &http.Request{
		Method: "PRI",
		URL:    nil,
		Header: http.Header{},
		Body:   io.NopCloser(conn),
		Host:   "",
	})
}

// connResponseWriter implements http.ResponseWriter for a raw net.Conn.
// This is used to bridge the hijacked connection to the gRPC server's HTTP handler.
type connResponseWriter struct {
	conn          net.Conn
	headerWritten bool
	statusCode    int
	header        http.Header
}

func (w *connResponseWriter) Header() http.Header {
	if w.header == nil {
		w.header = make(http.Header)
	}
	return w.header
}

func (w *connResponseWriter) Write(b []byte) (int, error) {
	if !w.headerWritten {
		w.WriteHeader(http.StatusOK)
	}
	return w.conn.Write(b)
}

func (w *connResponseWriter) WriteHeader(statusCode int) {
	if w.headerWritten {
		return
	}
	w.statusCode = statusCode
	w.headerWritten = true
	// Don't write HTTP response headers - we're already in gRPC/HTTP2 mode
}

// newHTTPServer creates an HTTP server configured to handle CONNECT requests.
func (p *Proxy) newHTTPServer(addr string, grpcServer *grpc.Server) *http.Server {
	// Create HTTP/2 server using h2c (HTTP/2 cleartext)
	// This allows HTTP/2 without TLS, which is what we need for the proxy
	h2s := &http2.Server{}

	return &http.Server{ //nolint:gosec // ReadHeaderTimeout not needed - this is for testing only, not production
		Addr:    addr,
		Handler: h2c.NewHandler(p, h2s),
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
