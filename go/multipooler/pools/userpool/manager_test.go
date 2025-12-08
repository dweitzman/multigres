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

package userpool

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// MockConnection implements a simple mock for testing.
type MockConnection struct {
	username   string
	closed     atomic.Bool
	resetCount atomic.Int32
}

func (m *MockConnection) Close() error {
	m.closed.Store(true)
	return nil
}

func (m *MockConnection) IsClosed() bool {
	return m.closed.Load()
}

func (m *MockConnection) Reset(ctx context.Context) error {
	m.resetCount.Add(1)
	return nil
}

// MockConnector creates mock connections and tracks connection attempts.
type MockConnector struct {
	mu              sync.Mutex
	connectAttempts []string // usernames we tried to connect as
	shouldFail      bool
	failFor         map[string]bool // usernames that should fail
}

func NewMockConnector() *MockConnector {
	return &MockConnector{
		failFor: make(map[string]bool),
	}
}

func (m *MockConnector) Connect(ctx context.Context, username string, clientKey, serverKey []byte) (*MockConnection, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.connectAttempts = append(m.connectAttempts, username)

	if m.shouldFail || m.failFor[username] {
		return nil, assert.AnError
	}

	return &MockConnection{username: username}, nil
}

func (m *MockConnector) ConnectionCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.connectAttempts)
}

func (m *MockConnector) ConnectionsForUser(username string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	count := 0
	for _, u := range m.connectAttempts {
		if u == username {
			count++
		}
	}
	return count
}

func TestPoolCreatedOnFirstRequest(t *testing.T) {
	t.Run("pool is created when first connection requested for user", func(t *testing.T) {
		connector := NewMockConnector()
		manager := NewManager[*MockConnection](ManagerConfig{
			MaxPoolsPerManager:    10,
			MaxConnectionsPerPool: 5,
			IdlePoolTimeout:       time.Minute,
			GlobalConnectionCap:   100,
		})

		ctx := context.Background()
		keys := &SCRAMKeys{
			ClientKey: []byte("client-key-alice"),
			ServerKey: []byte("server-key-alice"),
		}

		// Request a connection for "alice"
		conn, err := manager.GetConnection(ctx, "alice", keys, connector.Connect)
		require.NoError(t, err)
		require.NotNil(t, conn)

		// Verify pool was created
		assert.Equal(t, 1, manager.PoolCount(), "should have exactly one pool")
		assert.True(t, manager.HasPoolForUser("alice"), "should have pool for alice")

		// Verify connector was called with correct username
		assert.Equal(t, 1, connector.ConnectionCount())
		assert.Equal(t, 1, connector.ConnectionsForUser("alice"))

		// Return the connection
		manager.ReturnConnection(ctx, conn)
	})

	t.Run("separate pools created for different users", func(t *testing.T) {
		connector := NewMockConnector()
		manager := NewManager[*MockConnection](ManagerConfig{
			MaxPoolsPerManager:    10,
			MaxConnectionsPerPool: 5,
			IdlePoolTimeout:       time.Minute,
			GlobalConnectionCap:   100,
		})

		ctx := context.Background()

		// Request connections for different users
		aliceKeys := &SCRAMKeys{ClientKey: []byte("ck-alice"), ServerKey: []byte("sk-alice")}
		bobKeys := &SCRAMKeys{ClientKey: []byte("ck-bob"), ServerKey: []byte("sk-bob")}

		connAlice, err := manager.GetConnection(ctx, "alice", aliceKeys, connector.Connect)
		require.NoError(t, err)

		connBob, err := manager.GetConnection(ctx, "bob", bobKeys, connector.Connect)
		require.NoError(t, err)

		// Verify separate pools were created
		assert.Equal(t, 2, manager.PoolCount(), "should have two pools")
		assert.True(t, manager.HasPoolForUser("alice"))
		assert.True(t, manager.HasPoolForUser("bob"))

		// Verify connector was called for each user
		assert.Equal(t, 1, connector.ConnectionsForUser("alice"))
		assert.Equal(t, 1, connector.ConnectionsForUser("bob"))

		manager.ReturnConnection(ctx, connAlice)
		manager.ReturnConnection(ctx, connBob)
	})
}

func TestConnectionReuse(t *testing.T) {
	t.Run("returned connection is reused for same user", func(t *testing.T) {
		connector := NewMockConnector()
		manager := NewManager[*MockConnection](ManagerConfig{
			MaxPoolsPerManager:    10,
			MaxConnectionsPerPool: 5,
			IdlePoolTimeout:       time.Minute,
			GlobalConnectionCap:   100,
		})

		ctx := context.Background()
		keys := &SCRAMKeys{ClientKey: []byte("ck"), ServerKey: []byte("sk")}

		// Get first connection
		conn1, err := manager.GetConnection(ctx, "alice", keys, connector.Connect)
		require.NoError(t, err)

		// Return it
		manager.ReturnConnection(ctx, conn1)

		// Get another connection for same user
		conn2, err := manager.GetConnection(ctx, "alice", keys, connector.Connect)
		require.NoError(t, err)

		// Should have only created one connection (reused)
		assert.Equal(t, 1, connector.ConnectionCount(), "should reuse connection")

		// Should be the same connection object
		assert.Same(t, conn1.Conn, conn2.Conn, "should return same connection")

		manager.ReturnConnection(ctx, conn2)
	})

	t.Run("multiple concurrent requests create multiple connections up to pool size", func(t *testing.T) {
		connector := NewMockConnector()
		manager := NewManager[*MockConnection](ManagerConfig{
			MaxPoolsPerManager:    10,
			MaxConnectionsPerPool: 3,
			IdlePoolTimeout:       time.Minute,
			GlobalConnectionCap:   100,
		})

		ctx := context.Background()
		keys := &SCRAMKeys{ClientKey: []byte("ck"), ServerKey: []byte("sk")}

		// Get 3 connections concurrently (pool max is 3)
		var connections []*PooledConnection[*MockConnection]
		for range 3 {
			conn, err := manager.GetConnection(ctx, "alice", keys, connector.Connect)
			require.NoError(t, err)
			connections = append(connections, conn)
		}

		// Should have created 3 connections
		assert.Equal(t, 3, connector.ConnectionCount())

		// Return all
		for _, conn := range connections {
			manager.ReturnConnection(ctx, conn)
		}

		// Get one more - should reuse
		conn, err := manager.GetConnection(ctx, "alice", keys, connector.Connect)
		require.NoError(t, err)
		assert.Equal(t, 3, connector.ConnectionCount(), "should reuse existing connection")

		manager.ReturnConnection(ctx, conn)
	})
}

func TestIdlePoolGarbageCollection(t *testing.T) {
	t.Run("idle pools are removed after timeout", func(t *testing.T) {
		connector := NewMockConnector()
		idleTimeout := 50 * time.Millisecond

		manager := NewManager[*MockConnection](ManagerConfig{
			MaxPoolsPerManager:    10,
			MaxConnectionsPerPool: 5,
			IdlePoolTimeout:       idleTimeout,
			GlobalConnectionCap:   100,
		})

		ctx := context.Background()
		keys := &SCRAMKeys{ClientKey: []byte("ck"), ServerKey: []byte("sk")}

		// Create a pool
		conn, err := manager.GetConnection(ctx, "alice", keys, connector.Connect)
		require.NoError(t, err)
		manager.ReturnConnection(ctx, conn)

		assert.Equal(t, 1, manager.PoolCount())

		// Wait for idle timeout + some buffer
		time.Sleep(idleTimeout * 3)

		// Trigger GC
		manager.CleanupIdlePools()

		// Pool should be removed
		assert.Equal(t, 0, manager.PoolCount(), "idle pool should be removed")
	})

	t.Run("active pools are not removed", func(t *testing.T) {
		connector := NewMockConnector()
		idleTimeout := 50 * time.Millisecond

		manager := NewManager[*MockConnection](ManagerConfig{
			MaxPoolsPerManager:    10,
			MaxConnectionsPerPool: 5,
			IdlePoolTimeout:       idleTimeout,
			GlobalConnectionCap:   100,
		})

		ctx := context.Background()
		keys := &SCRAMKeys{ClientKey: []byte("ck"), ServerKey: []byte("sk")}

		// Create a pool and keep connection checked out
		conn, err := manager.GetConnection(ctx, "alice", keys, connector.Connect)
		require.NoError(t, err)

		// Wait longer than idle timeout
		time.Sleep(idleTimeout * 3)

		// Trigger GC
		manager.CleanupIdlePools()

		// Pool should still exist (has active connection)
		assert.Equal(t, 1, manager.PoolCount(), "active pool should not be removed")

		manager.ReturnConnection(ctx, conn)
	})
}

func TestGlobalConnectionCap(t *testing.T) {
	t.Run("enforces global connection limit across all pools", func(t *testing.T) {
		connector := NewMockConnector()
		manager := NewManager[*MockConnection](ManagerConfig{
			MaxPoolsPerManager:    10,
			MaxConnectionsPerPool: 5,
			IdlePoolTimeout:       time.Minute,
			GlobalConnectionCap:   3, // Only 3 connections total
		})

		ctx := context.Background()

		// Try to get 4 connections across different users
		var connections []*PooledConnection[*MockConnection]
		users := []string{"alice", "bob", "charlie", "dave"}

		for _, user := range users[:3] {
			keys := &SCRAMKeys{ClientKey: []byte("ck-" + user), ServerKey: []byte("sk-" + user)}
			conn, err := manager.GetConnection(ctx, user, keys, connector.Connect)
			require.NoError(t, err, "should get connection for %s", user)
			connections = append(connections, conn)
		}

		// Fourth connection should block or error due to global cap
		ctx4, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
		defer cancel()

		keys4 := &SCRAMKeys{ClientKey: []byte("ck-dave"), ServerKey: []byte("sk-dave")}
		_, err := manager.GetConnection(ctx4, "dave", keys4, connector.Connect)
		assert.Error(t, err, "should fail when global cap reached")

		// Return a connection
		manager.ReturnConnection(ctx, connections[0])
		connections = connections[1:]

		// Now should be able to get a connection
		conn, err := manager.GetConnection(ctx, "dave", keys4, connector.Connect)
		require.NoError(t, err)
		connections = append(connections, conn)

		// Cleanup
		for _, c := range connections {
			manager.ReturnConnection(ctx, c)
		}
	})
}

func TestSCRAMKeysPassedToConnector(t *testing.T) {
	t.Run("SCRAM keys are passed to connector function", func(t *testing.T) {
		var capturedClientKey, capturedServerKey []byte
		var capturedUsername string

		connector := func(ctx context.Context, username string, clientKey, serverKey []byte) (*MockConnection, error) {
			capturedUsername = username
			capturedClientKey = clientKey
			capturedServerKey = serverKey
			return &MockConnection{username: username}, nil
		}

		manager := NewManager[*MockConnection](ManagerConfig{
			MaxPoolsPerManager:    10,
			MaxConnectionsPerPool: 5,
			IdlePoolTimeout:       time.Minute,
			GlobalConnectionCap:   100,
		})

		ctx := context.Background()
		keys := &SCRAMKeys{
			ClientKey: []byte("expected-client-key"),
			ServerKey: []byte("expected-server-key"),
		}

		conn, err := manager.GetConnection(ctx, "testuser", keys, connector)
		require.NoError(t, err)

		assert.Equal(t, "testuser", capturedUsername)
		assert.Equal(t, []byte("expected-client-key"), capturedClientKey)
		assert.Equal(t, []byte("expected-server-key"), capturedServerKey)

		manager.ReturnConnection(ctx, conn)
	})
}

func TestManagerClose(t *testing.T) {
	t.Run("closing manager closes all pools and connections", func(t *testing.T) {
		connector := NewMockConnector()
		manager := NewManager[*MockConnection](ManagerConfig{
			MaxPoolsPerManager:    10,
			MaxConnectionsPerPool: 5,
			IdlePoolTimeout:       time.Minute,
			GlobalConnectionCap:   100,
		})

		ctx := context.Background()

		// Create connections for multiple users
		var connections []*PooledConnection[*MockConnection]
		for _, user := range []string{"alice", "bob"} {
			keys := &SCRAMKeys{ClientKey: []byte("ck-" + user), ServerKey: []byte("sk-" + user)}
			conn, err := manager.GetConnection(ctx, user, keys, connector.Connect)
			require.NoError(t, err)
			connections = append(connections, conn)
		}

		// Return connections
		for _, conn := range connections {
			manager.ReturnConnection(ctx, conn)
		}

		// Close manager
		err := manager.Close()
		require.NoError(t, err)

		assert.Equal(t, 0, manager.PoolCount(), "all pools should be closed")

		// All underlying connections should be closed
		for _, conn := range connections {
			assert.True(t, conn.Conn.IsClosed(), "connection should be closed")
		}
	})
}
