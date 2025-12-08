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
	"sync/atomic"

	"github.com/jackc/pgx/v5/pgconn"
)

// PgxConnection wraps a pgconn.PgConn to implement the Connection interface.
// It provides SCRAM-authenticated connections to PostgreSQL using key passthrough.
type PgxConnection struct {
	conn   *pgconn.PgConn
	closed atomic.Bool
}

// NewPgxConnection wraps an existing pgconn.PgConn.
func NewPgxConnection(conn *pgconn.PgConn) *PgxConnection {
	return &PgxConnection{conn: conn}
}

// Conn returns the underlying pgconn.PgConn for executing queries.
func (c *PgxConnection) Conn() *pgconn.PgConn {
	return c.conn
}

// Close closes the connection.
func (c *PgxConnection) Close() error {
	if c.closed.Swap(true) {
		return nil // Already closed
	}
	//nolint:gocritic // Close is called during cleanup when no context is available
	return c.conn.Close(context.Background())
}

// IsClosed returns true if the connection has been closed.
func (c *PgxConnection) IsClosed() bool {
	return c.closed.Load() || c.conn.IsClosed()
}

// PgxConnectorConfig holds configuration for creating PostgreSQL connections.
type PgxConnectorConfig struct {
	// Host is the PostgreSQL server host (IP, hostname, or Unix socket directory).
	Host string

	// Port is the PostgreSQL server port.
	Port uint16

	// Database is the database name to connect to.
	Database string

	// Logger for connection events.
	Logger *slog.Logger
}

// PgxConnector creates connections to PostgreSQL using SCRAM key passthrough.
// It implements ConnectorFunc[*PgxConnection] for use with the Manager.
type PgxConnector struct {
	config PgxConnectorConfig
}

// NewPgxConnector creates a new connector with the given configuration.
func NewPgxConnector(config PgxConnectorConfig) *PgxConnector {
	if config.Logger == nil {
		config.Logger = slog.Default()
	}
	return &PgxConnector{config: config}
}

// Connect creates a new connection to PostgreSQL for the specified user,
// authenticating with the provided SCRAM keys.
//
// This is the core of SCRAM passthrough authentication:
// - clientKey and serverKey were extracted during client authentication
// - We use them to authenticate to PostgreSQL without knowing the password
// - PostgreSQL sees a normal SCRAM-SHA-256 handshake
func (c *PgxConnector) Connect(ctx context.Context, username string, clientKey, serverKey []byte) (*PgxConnection, error) {
	// Build pgconn config with SCRAM keys
	connConfig := &pgconn.Config{
		Host:           c.config.Host,
		Port:           c.config.Port,
		Database:       c.config.Database,
		User:           username,
		SCRAMClientKey: clientKey,
		SCRAMServerKey: serverKey,
		// No password needed - we have the SCRAM keys
	}

	c.config.Logger.DebugContext(ctx, "connecting to PostgreSQL with SCRAM passthrough",
		"user", username,
		"host", c.config.Host,
		"port", c.config.Port,
		"database", c.config.Database,
		"has_client_key", len(clientKey) > 0,
		"has_server_key", len(serverKey) > 0,
	)

	// Connect using pgconn with SCRAM key passthrough
	conn, err := pgconn.ConnectConfig(ctx, connConfig)
	if err != nil {
		return nil, fmt.Errorf("pgx connect with SCRAM keys: %w", err)
	}

	c.config.Logger.DebugContext(ctx, "connected to PostgreSQL",
		"user", username,
		"database", c.config.Database,
	)

	return NewPgxConnection(conn), nil
}

// ConnectFunc returns a ConnectorFunc suitable for use with Manager.GetConnection.
func (c *PgxConnector) ConnectFunc() ConnectorFunc[*PgxConnection] {
	return c.Connect
}
