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
	"context"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/sqltypes"
	"github.com/multigres/multigres/go/pb/query"
	"github.com/multigres/multigres/go/pgprotocol/client"
	"github.com/multigres/multigres/go/pgprotocol/protocol"
)

// transactionAwareHandler is a handler that simulates transaction state changes.
// It sets txnStatus to TxnStatusInBlock on BEGIN and back to TxnStatusIdle on COMMIT/ROLLBACK.
type transactionAwareHandler struct {
	// queriesProcessed tracks how many queries have been processed.
	queriesProcessed atomic.Int32
}

func (h *transactionAwareHandler) HandleQuery(ctx context.Context, conn *Conn, queryStr string, callback func(ctx context.Context, result *sqltypes.Result) error) error {
	h.queriesProcessed.Add(1)

	upperQuery := strings.ToUpper(strings.TrimSpace(queryStr))
	switch {
	case strings.HasPrefix(upperQuery, "BEGIN"):
		conn.txnStatus = protocol.TxnStatusInBlock
	case strings.HasPrefix(upperQuery, "COMMIT"), strings.HasPrefix(upperQuery, "ROLLBACK"):
		conn.txnStatus = protocol.TxnStatusIdle
	}

	// Return a simple result.
	return callback(ctx, &sqltypes.Result{
		CommandTag: "SELECT 1",
	})
}

func (h *transactionAwareHandler) HandleParse(ctx context.Context, conn *Conn, name, queryStr string, paramTypes []uint32) error {
	return nil
}

func (h *transactionAwareHandler) HandleBind(ctx context.Context, conn *Conn, portalName, stmtName string, params [][]byte, paramFormats, resultFormats []int16) error {
	return nil
}

func (h *transactionAwareHandler) HandleExecute(ctx context.Context, conn *Conn, portalName string, maxRows int32, callback func(ctx context.Context, result *sqltypes.Result) error) error {
	return nil
}

func (h *transactionAwareHandler) HandleDescribe(ctx context.Context, conn *Conn, typ byte, name string) (*query.StatementDescription, error) {
	return nil, nil
}

func (h *transactionAwareHandler) HandleClose(ctx context.Context, conn *Conn, typ byte, name string) error {
	return nil
}

func (h *transactionAwareHandler) HandleSync(ctx context.Context, conn *Conn) error {
	return nil
}

// testListenerWithHandler creates a test listener with a custom handler.
func testListenerWithHandler(t *testing.T, handler Handler) *Listener {
	listener, err := NewListener(t.Context(), ListenerConfig{
		Address: "localhost:0", // Use random available port
		Handler: handler,
		Logger:  testLogger(t),
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = listener.Shutdown(ctx)
	})
	return listener
}

// parseHostPort extracts host and port from an address string like "127.0.0.1:5432".
func parseHostPort(addr string) (string, int, error) {
	var host string
	var port int
	for i := len(addr) - 1; i >= 0; i-- {
		if addr[i] == ':' {
			host = addr[:i]
			for j := i + 1; j < len(addr); j++ {
				port = port*10 + int(addr[j]-'0')
			}
			return host, port, nil
		}
	}
	return "", 0, nil
}

// TestGracefulShutdownIdleConnection tests that an idle connection receives
// a 57P01 error when the listener begins graceful shutdown.
func TestGracefulShutdownIdleConnection(t *testing.T) {
	handler := &transactionAwareHandler{}
	listener := testListenerWithHandler(t, handler)

	// Start serving in a goroutine.
	serveDone := make(chan error, 1)
	go func() {
		serveDone <- listener.Serve()
	}()

	// Get the address the listener is bound to.
	addr := listener.Addr()
	host, port, err := parseHostPort(addr.String())
	require.NoError(t, err)

	// Connect using the wire protocol client.
	conn, err := client.Connect(t.Context(), &client.Config{
		Host:     host,
		Port:     port,
		User:     "postgres",
		Database: "postgres",
	})
	require.NoError(t, err)
	defer conn.Close()

	// Begin graceful shutdown. The connection is blocked on read, waiting for
	// the next client message. When the client sends a query, the server will:
	// 1. Read the message (unblocks)
	// 2. Check acceptCtx.Done() - it's cancelled!
	// 3. Send 57P01 and close WITHOUT processing the query
	listener.acceptCancel()

	// Query should fail with 57P01 - server reads it but rejects before processing.
	_, err = conn.Query(t.Context(), "SELECT 1")
	require.Error(t, err, "query should fail with 57P01 during graceful shutdown")

	// Close the listener to make Serve() exit.
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	require.NoError(t, listener.Shutdown(ctx))

	// Serve should exit.
	require.Eventually(t, func() bool {
		select {
		case <-serveDone:
			return true
		default:
			return false
		}
	}, time.Second, 10*time.Millisecond, "Serve did not exit after shutdown")
}

// TestGracefulShutdownInTransaction tests that a connection in a transaction
// is NOT closed when graceful shutdown begins - it waits until the transaction ends.
func TestGracefulShutdownInTransaction(t *testing.T) {
	handler := &transactionAwareHandler{}
	listener := testListenerWithHandler(t, handler)

	// Start serving in a goroutine.
	serveDone := make(chan error, 1)
	go func() {
		serveDone <- listener.Serve()
	}()

	// Get the address the listener is bound to.
	addr := listener.Addr()
	host, port, err := parseHostPort(addr.String())
	require.NoError(t, err)

	// Connect using the wire protocol client.
	conn, err := client.Connect(t.Context(), &client.Config{
		Host:     host,
		Port:     port,
		User:     "postgres",
		Database: "postgres",
	})
	require.NoError(t, err)
	defer conn.Close()

	// Start a transaction.
	_, err = conn.Query(t.Context(), "BEGIN")
	require.NoError(t, err)

	// Begin graceful shutdown.
	listener.acceptCancel()

	// Execute queries while in transaction - should succeed because
	// the shutdown check only applies when txnStatus is Idle.
	_, err = conn.Query(t.Context(), "SELECT 1")
	require.NoError(t, err, "query should succeed while in transaction during graceful shutdown")

	_, err = conn.Query(t.Context(), "SELECT 2")
	require.NoError(t, err, "second query should also succeed while in transaction")

	// Verify we processed the queries (BEGIN + SELECT + SELECT).
	require.Equal(t, int32(3), handler.queriesProcessed.Load())

	// Commit the transaction - this makes the connection idle.
	// COMMIT itself succeeds because txnStatus is still InBlock when we read it.
	_, err = conn.Query(t.Context(), "COMMIT")
	require.NoError(t, err, "COMMIT should succeed - txnStatus is still InBlock when read")

	// Now the connection is idle. The next query should fail with 57P01
	// because the server reads it, checks shutdown (now idle), and rejects.
	_, err = conn.Query(t.Context(), "SELECT 3")
	require.Error(t, err, "query after COMMIT should fail with 57P01")

	// Close the listener to make Serve() exit.
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	require.NoError(t, listener.Shutdown(ctx))

	// Serve should exit.
	require.Eventually(t, func() bool {
		select {
		case <-serveDone:
			return true
		default:
			return false
		}
	}, time.Second, 10*time.Millisecond, "Serve did not exit after shutdown")
}

// TestForceCloseConnection tests that a connection is force-closed when
// connCtx is cancelled (grace period expired).
func TestForceCloseConnection(t *testing.T) {
	handler := &transactionAwareHandler{}
	listener := testListenerWithHandler(t, handler)

	// Start serving in a goroutine.
	serveDone := make(chan error, 1)
	go func() {
		serveDone <- listener.Serve()
	}()

	// Get the address the listener is bound to.
	addr := listener.Addr()
	host, port, err := parseHostPort(addr.String())
	require.NoError(t, err)

	// Connect using the wire protocol client.
	conn, err := client.Connect(t.Context(), &client.Config{
		Host:     host,
		Port:     port,
		User:     "postgres",
		Database: "postgres",
	})
	require.NoError(t, err)
	defer conn.Close()

	// Force-close by cancelling connCtx (simulates grace period expiry).
	// acceptCancel is also called, but the key is connCtx which triggers
	// the force-close check at the top of the serve loop.
	listener.acceptCancel()
	listener.connCancel()

	// Execute a query - the server reads it, checks ctx.Done() first,
	// sees it's cancelled, and calls Close() then returns.
	_, err = conn.Query(t.Context(), "SELECT 1")
	require.Error(t, err, "query should fail after force close")

	// Close the listener to make Serve() exit.
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	_ = listener.Shutdown(ctx) // May already be partially shutdown

	// Serve should exit.
	require.Eventually(t, func() bool {
		select {
		case <-serveDone:
			return true
		default:
			return false
		}
	}, time.Second, 10*time.Millisecond, "Serve did not exit after force close")
}

// TestShutdownWithTimeout tests the full Shutdown() flow with a timeout.
func TestShutdownWithTimeout(t *testing.T) {
	handler := &transactionAwareHandler{}
	listener := testListenerWithHandler(t, handler)

	// Start serving in a goroutine.
	serveDone := make(chan error, 1)
	go func() {
		serveDone <- listener.Serve()
	}()

	// Get the address the listener is bound to.
	addr := listener.Addr()
	host, port, err := parseHostPort(addr.String())
	require.NoError(t, err)

	// Connect using the wire protocol client.
	conn, err := client.Connect(t.Context(), &client.Config{
		Host:     host,
		Port:     port,
		User:     "postgres",
		Database: "postgres",
	})
	require.NoError(t, err)
	defer conn.Close()

	// Call Shutdown with a timeout - this should gracefully close the connection.
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()

	shutdownErr := listener.Shutdown(ctx)
	require.NoError(t, shutdownErr)

	// Serve should exit.
	require.Eventually(t, func() bool {
		select {
		case <-serveDone:
			return true
		default:
			return false
		}
	}, time.Second, 10*time.Millisecond, "Serve did not exit after Shutdown")
}
