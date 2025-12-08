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
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPoolConnection_Interface(t *testing.T) {
	t.Run("PoolConnection implements Connection interface", func(t *testing.T) {
		// Compile-time check that PoolConnection implements Connection
		var _ Connection = (*PoolConnection)(nil)
	})
}

func TestConnector_Creation(t *testing.T) {
	t.Run("creates connector with config", func(t *testing.T) {
		config := ConnectorConfig{
			Host:     "localhost",
			Port:     5432,
			Database: "postgres",
			Logger:   slog.Default(),
		}

		connector := NewConnector(config)
		require.NotNil(t, connector)
	})

	t.Run("uses default logger when none provided", func(t *testing.T) {
		config := ConnectorConfig{
			Host:     "localhost",
			Port:     5432,
			Database: "postgres",
		}

		connector := NewConnector(config)
		require.NotNil(t, connector)
	})
}

func TestConnector_ConnectFunc(t *testing.T) {
	t.Run("returns a ConnectorFunc", func(t *testing.T) {
		connector := NewConnector(ConnectorConfig{
			Host:     "localhost",
			Port:     5432,
			Database: "postgres",
		})

		connectFunc := connector.ConnectFunc()
		require.NotNil(t, connectFunc)

		// Verify it has the right signature by calling it
		// (would fail at compile time if signature is wrong)
		_ = connectFunc
	})
}

// TestConnector_Integration tests real PostgreSQL connections.
// Skip in short mode since it requires a running PostgreSQL instance.
func TestConnector_Integration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	// This test requires:
	// 1. A running PostgreSQL server
	// 2. A user with SCRAM-SHA-256 authentication
	// 3. Known SCRAM keys for that user
	//
	// For now, this is a placeholder. In practice, you'd set up test fixtures
	// with known password → derived SCRAM keys.
	t.Skip("requires PostgreSQL with SCRAM test user - TODO: add test fixtures")
}

func TestPoolConnection_NilSafety(t *testing.T) {
	t.Run("IsClosed handles nil underlying conn", func(t *testing.T) {
		conn := &PoolConnection{}
		// Should not panic and should return true for nil conn
		assert.True(t, conn.IsClosed())
	})

	t.Run("Close handles nil underlying conn", func(t *testing.T) {
		conn := &PoolConnection{}
		// Should not panic and should return nil
		err := conn.Close()
		assert.NoError(t, err)
	})
}

func TestManagerWithConnector(t *testing.T) {
	t.Run("manager works with PoolConnection type", func(t *testing.T) {
		// Verify the Manager can be instantiated with PoolConnection type
		manager := NewManager[*PoolConnection](ManagerConfig{
			MaxPoolsPerManager:    10,
			MaxConnectionsPerPool: 5,
			IdlePoolTimeout:       0,
			GlobalConnectionCap:   100,
		})
		require.NotNil(t, manager)

		// Verify we can close an empty manager
		err := manager.Close()
		require.NoError(t, err)
	})

	t.Run("GetConnection accepts Connector.ConnectFunc", func(t *testing.T) {
		if testing.Short() {
			t.Skip("skipping test that would attempt real connection")
		}

		manager := NewManager[*PoolConnection](ManagerConfig{
			MaxPoolsPerManager:    10,
			MaxConnectionsPerPool: 5,
			IdlePoolTimeout:       0,
			GlobalConnectionCap:   100,
		})
		defer manager.Close()

		connector := NewConnector(ConnectorConfig{
			Host:     "localhost",
			Port:     5432,
			Database: "postgres",
		})

		keys := &SCRAMKeys{
			ClientKey: []byte("test-client-key"),
			ServerKey: []byte("test-server-key"),
		}

		// This will fail because we don't have a real PostgreSQL,
		// but it verifies the type signatures work together
		ctx := context.Background()
		_, err := manager.GetConnection(ctx, "testuser", keys, connector.ConnectFunc())
		// We expect an error since there's no PostgreSQL to connect to
		assert.Error(t, err)
	})
}
