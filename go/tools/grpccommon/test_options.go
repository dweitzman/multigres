// Copyright 2025 Supabase, Inc.

package grpccommon

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// withGrpcTestProxyOptions returns test-specific gRPC options when environment
// variables are set. This is called automatically by NewClient.
//
// Environment variables:
//   - GRPC_PROXY_SOURCE_ID: Adds x-multigres-source metadata for fault injection
//   - FORCE_GRPC_HTTPS_PROXY: Forces connections through proxy (even localhost)
//
// Returns empty slice if no test environment variables are set.
func withGrpcTestProxyOptions() []ClientOption {
	var opts []ClientOption

	// Add source ID metadata if configured
	if sourceID := os.Getenv("GRPC_PROXY_SOURCE_ID"); sourceID != "" {
		opts = append(opts, withSourceID(sourceID))
	}

	// Add forced proxy dialer if configured
	if proxyAddr := os.Getenv("FORCE_GRPC_HTTPS_PROXY"); proxyAddr != "" {
		opts = append(opts, withForceProxy(proxyAddr))
	}

	return opts
}

// withSourceID adds x-multigres-source metadata to all gRPC requests.
// The source ID identifies which service is making the request, allowing
// the fault injection proxy to apply rules based on the source service.
func withSourceID(sourceID string) ClientOption {
	return funcOption(func(c *clientConfig) {
		c.dialOptions = append(c.dialOptions,
			grpc.WithUnaryInterceptor(sourceIDUnaryInterceptor(sourceID)),
			grpc.WithStreamInterceptor(sourceIDStreamInterceptor(sourceID)),
		)
	})
}

// sourceIDUnaryInterceptor injects x-multigres-source metadata for unary RPCs.
func sourceIDUnaryInterceptor(sourceID string) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		ctx = metadata.AppendToOutgoingContext(ctx, "x-multigres-source", sourceID)
		return invoker(ctx, method, req, reply, cc, opts...)
	}
}

// sourceIDStreamInterceptor injects x-multigres-source metadata for streaming RPCs.
func sourceIDStreamInterceptor(sourceID string) grpc.StreamClientInterceptor {
	return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		ctx = metadata.AppendToOutgoingContext(ctx, "x-multigres-source", sourceID)
		return streamer(ctx, desc, cc, method, opts...)
	}
}

// withForceProxy forces all gRPC connections through a proxy, even for localhost.
// Unlike standard HTTPS_PROXY behavior (which excludes localhost), this uses a
// custom dialer that establishes HTTP CONNECT tunnels for all targets.
//
// Implementation follows grpc-go's internal proxy.go approach.
func withForceProxy(proxyAddr string) ClientOption {
	return funcOption(func(c *clientConfig) {
		c.dialOptions = append(c.dialOptions,
			grpc.WithContextDialer(createProxyDialer(proxyAddr)))
	})
}

// bufConn wraps a net.Conn and bufio.Reader to avoid losing buffered bytes.
// This is necessary because http.ReadResponse() uses a bufio.Reader which may
// read more than just the HTTP response headers.
type bufConn struct {
	net.Conn
	r io.Reader
}

func (c *bufConn) Read(b []byte) (int, error) {
	return c.r.Read(b)
}

// createProxyDialer returns a dialer function that establishes HTTP CONNECT
// tunnels through the specified proxy for all gRPC connections.
// Implementation follows grpc-go's doHTTPConnectHandshake in internal/transport/proxy.go.
func createProxyDialer(proxyAddr string) func(context.Context, string) (net.Conn, error) {
	return func(ctx context.Context, targetAddr string) (net.Conn, error) {
		// Connect to proxy
		var d net.Dialer
		proxyConn, err := d.DialContext(ctx, "tcp", proxyAddr)
		if err != nil {
			return nil, fmt.Errorf("failed to connect to proxy %s: %w", proxyAddr, err)
		}

		// Perform HTTP CONNECT handshake
		return doHTTPConnectHandshake(ctx, proxyConn, targetAddr)
	}
}

// doHTTPConnectHandshake performs the HTTP CONNECT handshake with the proxy.
// Based on grpc-go's internal/transport/proxy.go implementation.
func doHTTPConnectHandshake(ctx context.Context, conn net.Conn, targetAddr string) (_ net.Conn, err error) {
	defer func() {
		if err != nil {
			conn.Close()
		}
	}()

	// Create CONNECT request with context
	req := &http.Request{
		Method: http.MethodConnect,
		URL:    &url.URL{Host: targetAddr},
		Header: map[string][]string{"User-Agent": {"grpc-go"}},
	}
	req = req.WithContext(ctx)

	// Send CONNECT request
	if err := req.Write(conn); err != nil {
		return nil, fmt.Errorf("failed to write CONNECT request: %w", err)
	}

	// Read CONNECT response
	br := bufio.NewReader(conn)
	resp, err := http.ReadResponse(br, req)
	if err != nil {
		return nil, fmt.Errorf("failed to read CONNECT response: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("proxy CONNECT failed with status: %s", resp.Status)
	}

	// The buffer may contain extra bytes from the target server (e.g., TLS handshake data),
	// so we wrap the connection to avoid losing them. In many cases the buffer is empty
	// (when the server waits for the client to send first), so we avoid the wrapper overhead.
	if br.Buffered() != 0 {
		return &bufConn{Conn: conn, r: br}, nil
	}
	return conn, nil
}
