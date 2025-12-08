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
	"fmt"
	"log/slog"

	"github.com/multigres/multigres/go/pgprotocol/client"
)

// PoolConnection wraps a client.Conn to implement the Connection interface.
// It provides SCRAM-authenticated connections to PostgreSQL using key passthrough.
type PoolConnection struct {
	conn *client.Conn
}

// NewPoolConnection wraps an existing client.Conn.
func NewPoolConnection(conn *client.Conn) *PoolConnection {
	return &PoolConnection{conn: conn}
}

// Conn returns the underlying client.Conn for executing queries.
func (c *PoolConnection) Conn() *client.Conn {
	return c.conn
}

// Close closes the connection.
func (c *PoolConnection) Close() error {
	if c.conn == nil {
		return nil
	}
	return c.conn.Close()
}

// IsClosed returns true if the connection has been closed.
func (c *PoolConnection) IsClosed() bool {
	return c.conn == nil || c.conn.IsClosed()
}

// Reset cleans up connection state before returning to the pool.
// It runs RESET ROLE to ensure no SET ROLE changes persist across sessions.
func (c *PoolConnection) Reset(ctx context.Context) error {
	if c.conn == nil {
		return nil
	}
	_, err := c.conn.Query(ctx, "RESET ROLE")
	return err
}

// ConnectorConfig holds configuration for creating PostgreSQL connections.
type ConnectorConfig struct {
	// Host is the PostgreSQL server host (IP, hostname, or Unix socket directory).
	Host string

	// Port is the PostgreSQL server port.
	Port uint16

	// Database is the database name to connect to.
	Database string

	// Logger for connection events.
	Logger *slog.Logger
}

// Connector creates connections to PostgreSQL using SCRAM key passthrough.
// It implements ConnectorFunc[*PoolConnection] for use with the Manager.
type Connector struct {
	config ConnectorConfig
}

// NewConnector creates a new connector with the given configuration.
func NewConnector(config ConnectorConfig) *Connector {
	if config.Logger == nil {
		config.Logger = slog.Default()
	}
	return &Connector{config: config}
}

// Connect creates a new connection to PostgreSQL for the specified user,
// authenticating with the provided SCRAM keys.
//
// This is the core of SCRAM passthrough authentication:
// - clientKey and serverKey were extracted during client authentication
// - We use them to authenticate to PostgreSQL without knowing the password
// - PostgreSQL sees a normal SCRAM-SHA-256 handshake
func (c *Connector) Connect(ctx context.Context, username string, clientKey, serverKey []byte) (*PoolConnection, error) {
	c.config.Logger.DebugContext(ctx, "connecting to PostgreSQL with SCRAM passthrough",
		"user", username,
		"host", c.config.Host,
		"port", c.config.Port,
		"database", c.config.Database,
		"has_client_key", len(clientKey) > 0,
		"has_server_key", len(serverKey) > 0,
	)

	// Build client config with SCRAM keys
	connConfig := &client.Config{
		Host:           c.config.Host,
		Port:           int(c.config.Port),
		Database:       c.config.Database,
		User:           username,
		SCRAMClientKey: clientKey,
		SCRAMServerKey: serverKey,
		// No password needed - we have the SCRAM keys
	}

	// Connect using internal client with SCRAM key passthrough
	conn, err := client.Connect(ctx, connConfig)
	if err != nil {
		return nil, fmt.Errorf("connect with SCRAM keys: %w", err)
	}

	c.config.Logger.DebugContext(ctx, "connected to PostgreSQL",
		"user", username,
		"database", c.config.Database,
	)

	return NewPoolConnection(conn), nil
}

// ConnectFunc returns a ConnectorFunc suitable for use with Manager.GetConnection.
func (c *Connector) ConnectFunc() ConnectorFunc[*PoolConnection] {
	return c.Connect
}
