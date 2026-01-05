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

package server

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"

	"github.com/multigres/multigres/go/pgprotocol/bufpool"
	"github.com/multigres/multigres/go/tools/ctxutil"
)

// Listener listens for incoming PostgreSQL client connections.
type Listener struct {
	// listener is the network listener.
	listener net.Listener

	// handler processes queries for connections.
	handler Handler

	// logger for logging.
	logger *slog.Logger

	// readersPool pools bufio.Reader objects.
	readersPool *sync.Pool

	// writersPool pools bufio.Writer objects.
	writersPool *sync.Pool

	// bufPool pools byte buffers for packet I/O.
	bufPool *bufpool.Pool

	// nextConnectionID is an atomic counter for assigning connection IDs.
	nextConnectionID atomic.Uint32

	// wg tracks active connection handlers.
	wg sync.WaitGroup

	// acceptCtx is cancelled when we stop accepting new connections (shutdown begins).
	acceptCtx    context.Context
	acceptCancel context.CancelFunc

	// connCtx is cancelled after the grace period (forces remaining connections to close).
	// Connections inherit from this context.
	connCtx    context.Context
	connCancel context.CancelFunc

	// shutdownOnce ensures Shutdown is idempotent.
	shutdownOnce sync.Once
	shutdownErr  error
}

// ListenerConfig holds configuration for the listener.
type ListenerConfig struct {
	// Address to listen on (e.g., "localhost:5432").
	Address string

	// Handler processes queries.
	Handler Handler

	// Logger for logging (optional, defaults to slog.Default()).
	Logger *slog.Logger
}

// NewListener creates a new PostgreSQL protocol listener.
func NewListener(ctx context.Context, config ListenerConfig) (*Listener, error) {
	if config.Handler == nil {
		return nil, fmt.Errorf("handler is required")
	}

	netListener, err := net.Listen("tcp", config.Address)
	if err != nil {
		return nil, fmt.Errorf("failed to listen on %s: %w", config.Address, err)
	}

	logger := config.Logger
	if logger == nil {
		logger = slog.Default()
	}

	// Detach from parent context to preserve telemetry but control our own lifetime.
	// We create two contexts:
	// - acceptCtx: cancelled when shutdown begins (stops accepting new connections)
	// - connCtx: cancelled after grace period (forces connections to end)
	detachedCtx := ctxutil.Detach(ctx)
	acceptCtx, acceptCancel := context.WithCancel(detachedCtx)
	connCtx, connCancel := context.WithCancel(detachedCtx)

	l := &Listener{
		listener:     netListener,
		handler:      config.Handler,
		logger:       logger,
		acceptCtx:    acceptCtx,
		acceptCancel: acceptCancel,
		connCtx:      connCtx,
		connCancel:   connCancel,
	}

	// Initialize buffer pools.
	l.readersPool = &sync.Pool{
		New: func() any {
			return bufio.NewReaderSize(nil, connBufferSize)
		},
	}
	l.writersPool = &sync.Pool{
		New: func() any {
			return bufio.NewWriterSize(nil, connBufferSize)
		},
	}
	l.bufPool = bufpool.New(16*1024, 64*1024*1024) // 16 KB to 64 MB

	logger.InfoContext(ctx, "PostgreSQL listener started", "address", config.Address)

	return l, nil
}

// Serve accepts and handles incoming connections.
// This method blocks until the listener is closed or an error occurs.
func (l *Listener) Serve() error {
	for {
		netConn, err := l.listener.Accept()
		if err != nil {
			select {
			case <-l.acceptCtx.Done():
				// Listener is shutting down.
				return nil
			default:
				l.logger.Error("failed to accept connection", "error", err)
				continue
			}
		}

		// Assign connection ID and create connection.
		connID := l.nextConnectionID.Add(1)
		conn := newConn(netConn, l, connID)
		conn.handler = l.handler

		// Handle connection in a new goroutine.
		l.wg.Go(func() {
			l.handleConnection(conn)
		})
	}
}

// handleConnection handles a single client connection.
func (l *Listener) handleConnection(conn *Conn) {
	// Catch panics and ensure cleanup happens in all cases.
	defer func() {
		if x := recover(); x != nil {
			conn.logger.Error("panic in connection handler",
				"panic", x,
				"remote_addr", conn.RemoteAddr())
		}

		// Clean up connection resources.
		if err := conn.Close(); err != nil {
			conn.logger.Error("error closing connection", "error", err)
		}
	}()

	conn.logger.Info("connection accepted", "remote_addr", conn.RemoteAddr())

	// Serve the connection (startup + command loop).
	if err := conn.serve(); err != nil {
		if !errors.Is(err, io.EOF) {
			conn.logger.Error("connection error", "error", err)
		}
	}

	conn.logger.Info("connection closed")
}

// Shutdown gracefully shuts down the listener. It stops accepting new connections
// immediately, then waits for existing connections to finish or for the context
// deadline to expire.
//
// Shutdown is idempotent - subsequent calls return immediately with the original error.
//
// Graceful shutdown behavior:
//   - Connections check acceptCtx.Done() between commands (in serve loop)
//   - Idle connections (not in a transaction) exit gracefully after sending 57P01
//   - Connections in a transaction wait until the transaction completes
//   - If ctx deadline expires, connCtx is cancelled and Shutdown returns immediately
//
// This matches pgbouncer and supavisor's approach to graceful shutdown.
//
// Known limitation: Context cancellation doesn't close network connections.
// Connections blocked on Read() won't see the cancellation. When the deadline
// expires, we cancel connCtx but don't wait - the process will exit shortly
// and the OS will clean up orphaned connections. This matches Vitess behavior.
//
// Future enhancement options to force-close blocked connections:
//   - Per-connection goroutine that watches ctx.Done() and calls SetReadDeadline
//     to interrupt blocked reads
//   - Track connections in sync.Map and call Close() on each when force-closing
func (l *Listener) Shutdown(ctx context.Context) error {
	l.shutdownOnce.Do(func() {
		// Stop accepting new connections.
		l.acceptCancel()
		l.shutdownErr = l.listener.Close()

		// Wait for either all connections to finish or context deadline.
		done := make(chan struct{})
		go func() {
			l.wg.Wait()
			close(done)
		}()

		select {
		case <-done:
			l.logger.InfoContext(ctx, "PostgreSQL listener stopped gracefully")
		case <-ctx.Done():
			// Grace period expired, force-cancel remaining connections.
			// We don't wait for connections to finish - they may be blocked on network
			// reads and won't see the context cancellation. The process will exit
			// shortly and the OS will clean up. This matches Vitess's approach.
			l.connCancel()
			l.logger.WarnContext(ctx, "PostgreSQL listener force-closed, some connections may be orphaned")
		}
	})

	return l.shutdownErr
}

// Addr returns the listener's network address.
func (l *Listener) Addr() net.Addr {
	return l.listener.Addr()
}
