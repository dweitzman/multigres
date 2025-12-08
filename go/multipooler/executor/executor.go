// Copyright 2025 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package executor implements query execution for multipooler.
// It provides the QueryService interface implementation that executes queries
// against PostgreSQL and streams results back to clients.
package executor

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"path/filepath"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/multigres/multigres/go/common/queryservice"
	"github.com/multigres/multigres/go/multipooler/pools/userpool"
	"github.com/multigres/multigres/go/pb/query"
)

// DBConfig contains database connection parameters.
type DBConfig struct {
	SocketFilePath string
	PoolerDir      string
	Database       string
	PgPort         int
}

// Executor implements the QueryService interface for executing queries against PostgreSQL.
type Executor struct {
	logger   *slog.Logger
	dbConfig *DBConfig
	db       *sql.DB
	isOpen   atomic.Bool

	// userPoolManager manages per-user connection pools for SCRAM passthrough authentication.
	// When a caller provides SCRAM keys, we use their dedicated pool instead of SET SESSION AUTHORIZATION.
	userPoolManager *userpool.Manager[*userpool.PoolConnection]

	// connector creates connections for the user pool manager.
	connector *userpool.Connector
}

// NewExecutor creates a new Executor instance.
func NewExecutor(logger *slog.Logger, dbConfig *DBConfig) *Executor {
	return &Executor{
		logger:   logger,
		dbConfig: dbConfig,
	}
}

// Open creates the database connection and initializes the user pool manager.
func (e *Executor) Open() error {
	if e.isOpen.Load() {
		return nil
	}

	e.logger.Info("Executor: opening")

	if e.dbConfig == nil {
		return fmt.Errorf("database config not set")
	}

	// Create connection string using socket connection
	// PostgreSQL creates socket files as: {poolerDir}/pg_sockets/.s.PGSQL.{port}
	socketDir := filepath.Join(e.dbConfig.PoolerDir, "pg_sockets")
	port := fmt.Sprintf("%d", e.dbConfig.PgPort)

	dsn := fmt.Sprintf("user=postgres dbname=%s host=%s port=%s sslmode=disable",
		e.dbConfig.Database, socketDir, port)

	e.logger.Info("Executor: Unix socket connection",
		"pooler_dir", e.dbConfig.PoolerDir,
		"socket_dir", socketDir,
		"pg_port", e.dbConfig.PgPort)

	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return fmt.Errorf("failed to open database: %w", err)
	}

	// Test the connection
	if err := db.Ping(); err != nil {
		db.Close()
		return fmt.Errorf("failed to ping database: %w", err)
	}

	e.db = db

	// Initialize user pool manager for SCRAM passthrough authentication.
	// This allows per-user connection pools where each user's connections
	// are authenticated using their extracted SCRAM keys.
	e.userPoolManager = userpool.NewManager[*userpool.PoolConnection](userpool.ManagerConfig{
		MaxPoolsPerManager:    1000,            // Support many concurrent users
		MaxConnectionsPerPool: 10,              // Connections per user
		IdlePoolTimeout:       5 * time.Minute, // Clean up idle user pools
		GlobalConnectionCap:   500,             // Total connections across all users
	})

	// Create the connector for SCRAM passthrough connections.
	e.connector = userpool.NewConnector(userpool.ConnectorConfig{
		Host:     socketDir,
		Port:     uint16(e.dbConfig.PgPort),
		Database: e.dbConfig.Database,
		Logger:   e.logger,
	})

	e.isOpen.Store(true)
	e.logger.Info("Executor opened database connection and user pool manager")

	return nil
}

// ExecuteQuery implements queryservice.QueryService.
func (e *Executor) ExecuteQuery(ctx context.Context, target *query.Target, sql string, options *query.ExecuteOptions) (*query.QueryResult, error) {
	if target == nil {
		target = &query.Target{}
	}

	var maxRows uint64
	var callerID string
	var scramClientKey, scramServerKey []byte

	if options != nil {
		maxRows = options.MaxRows
		if options.CallerId != nil {
			if options.CallerId.Principal != "" {
				callerID = options.CallerId.Principal
			}
			scramClientKey = options.CallerId.ScramClientKey
			scramServerKey = options.CallerId.ScramServerKey
		}
	}

	e.logger.DebugContext(ctx, "executing query",
		"tablegroup", target.TableGroup,
		"shard", target.Shard,
		"pooler_type", target.PoolerType.String(),
		"caller_id", callerID,
		"has_scram_keys", len(scramClientKey) > 0 && len(scramServerKey) > 0,
		"query", sql)

	// If we have SCRAM keys, use the per-user connection pool with SCRAM passthrough.
	// This provides true per-user authentication to PostgreSQL.
	if callerID != "" && len(scramClientKey) > 0 && len(scramServerKey) > 0 {
		return e.executeQueryWithSCRAMPassthrough(ctx, sql, callerID, scramClientKey, scramServerKey, maxRows)
	}

	// Caller ID without SCRAM keys is not supported - clients must use SCRAM-SHA-256.
	if callerID != "" {
		return nil, fmt.Errorf("SCRAM keys required for user %q: client must use SCRAM-SHA-256 authentication", callerID)
	}

	// No caller ID, execute directly on the shared pool (administrative queries)
	return e.executeQuery(ctx, sql, maxRows)
}

// StreamExecute executes a query and streams results back via callback.
// This implements the queryservice.QueryService interface.
func (e *Executor) StreamExecute(
	ctx context.Context,
	target *query.Target,
	sql string,
	options *query.ExecuteOptions,
	callback func(context.Context, *query.QueryResult) error,
) error {
	// Execute the query and stream results
	// TODO(GuptaManan100): Actually stream the results from postgres.
	result, err := e.ExecuteQuery(ctx, target, sql, options)
	if err != nil {
		e.logger.ErrorContext(ctx, "query execution failed", "error", err, "query", sql)
		return fmt.Errorf("query execution failed: %w", err)
	}

	// Stream the result via callback
	if err := callback(ctx, result); err != nil {
		return err
	}

	return nil
}

// Close closes the executor and releases resources.
func (e *Executor) Close(ctx context.Context) error {
	if !e.isOpen.Swap(false) {
		return nil
	}

	// Close user pool manager first (closes all per-user connection pools)
	if e.userPoolManager != nil {
		if err := e.userPoolManager.Close(); err != nil {
			e.logger.WarnContext(ctx, "Executor: error closing user pool manager", "error", err)
		}
		e.userPoolManager = nil
	}

	if e.db != nil {
		if err := e.db.Close(); err != nil {
			// db.Close() can return "write: broken pipe" if the connection is broken,
			// because lib/pq tries to send a Postgres termination message during Close():
			// https://github.com/lib/pq/blob/b7ffbd3b47da4290a4af2ccd253c74c2c22bfabf/conn.go#L885
			//
			// This is safe to ignore.
			if errors.Is(err, syscall.EPIPE) {
				e.logger.WarnContext(ctx, "Executor: broken pipe error when closing database", "error", err)
				return nil
			}
			return fmt.Errorf("failed to close database: %w", err)
		}
		e.db = nil
	}

	e.logger.InfoContext(ctx, "Executor: closed")
	return nil
}

// PortalStreamExecute executes a portal and streams results back via callback.
// TODO: Implement this method.
func (e *Executor) PortalStreamExecute(
	ctx context.Context,
	target *query.Target,
	preparedStatement *query.PreparedStatement,
	portal *query.Portal,
	options *query.ExecuteOptions,
	callback func(context.Context, *query.QueryResult) error,
) (queryservice.ReservedState, error) {
	panic("PortalStreamExecute not implemented")
}

// Describe returns metadata about a prepared statement or portal.
// TODO: Implement this method.
func (e *Executor) Describe(
	ctx context.Context,
	target *query.Target,
	preparedStatement *query.PreparedStatement,
	portal *query.Portal,
	options *query.ExecuteOptions,
) (*query.StatementDescription, error) {
	panic("Describe not implemented")
}

// IsHealthy checks if the executor is healthy and can serve queries.
func (e *Executor) IsHealthy() error {
	if e.db == nil {
		return fmt.Errorf("database connection not initialized")
	}
	if err := e.db.Ping(); err != nil {
		return fmt.Errorf("database ping failed: %w", err)
	}
	return nil
}

// executeQueryWithSCRAMPassthrough executes a query using a per-user connection pool
// authenticated with SCRAM key passthrough. This is the preferred method when SCRAM keys
// are available from client authentication.
//
// Benefits over SET SESSION AUTHORIZATION:
// - Each user has their own authenticated connection pool
// - PostgreSQL sees actual user authentication, not superuser impersonation
// - Row-level security and audit logs correctly reflect the authenticated user
// - No need for superuser privileges on the pooler connection
func (e *Executor) executeQueryWithSCRAMPassthrough(ctx context.Context, queryStr string, username string, clientKey, serverKey []byte, maxRows uint64) (*query.QueryResult, error) {
	// Get a connection from the user's pool (creates pool if needed)
	keys := &userpool.SCRAMKeys{
		ClientKey: clientKey,
		ServerKey: serverKey,
	}

	pooledConn, err := e.userPoolManager.GetConnection(ctx, username, keys, e.connector.ConnectFunc())
	if err != nil {
		return nil, fmt.Errorf("failed to get SCRAM-authenticated connection for user %q: %w", username, err)
	}
	defer e.userPoolManager.ReturnConnection(ctx, pooledConn)

	e.logger.DebugContext(ctx, "using SCRAM passthrough connection", "username", username)

	// Execute the query using the pooled connection
	return e.executeQueryOnPoolConn(ctx, pooledConn.Conn, queryStr, maxRows)
}

// executeQueryOnPoolConn executes a query using the internal pgprotocol client.
func (e *Executor) executeQueryOnPoolConn(ctx context.Context, conn *userpool.PoolConnection, queryStr string, maxRows uint64) (*query.QueryResult, error) {
	// Use the internal client's Query method which handles all query types
	results, err := conn.Conn().Query(ctx, queryStr)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}

	if len(results) == 0 {
		return &query.QueryResult{
			Fields: []*query.Field{},
			Rows:   []*query.Row{},
		}, nil
	}

	// Return the first result (multi-statement queries return multiple results)
	result := results[0]

	// Apply maxRows limit if specified
	if maxRows > 0 && uint64(len(result.Rows)) > maxRows {
		result.Rows = result.Rows[:maxRows]
	}

	return result, nil
}

// executeQuery executes a SQL query and returns the result.
// This is the internal method that handles both SELECT and modification queries.
func (e *Executor) executeQuery(ctx context.Context, queryStr string, maxRows uint64) (*query.QueryResult, error) {
	// Determine if this is a SELECT query or a modification query
	trimmedQuery := strings.TrimSpace(strings.ToUpper(queryStr))
	isSelect := strings.HasPrefix(trimmedQuery, "SELECT") ||
		strings.HasPrefix(trimmedQuery, "WITH") ||
		strings.HasPrefix(trimmedQuery, "SHOW") ||
		strings.HasPrefix(trimmedQuery, "EXPLAIN")

	if isSelect {
		return e.executeSelectQuery(ctx, queryStr, maxRows)
	}
	return e.executeModifyQuery(ctx, queryStr)
}

// scanQueryRows scans rows from a query result into a QueryResult.
// This is shared by both executeSelectQuery and executeSelectQueryOnConn.
func (e *Executor) scanQueryRows(rows *sql.Rows, maxRows uint64) (*query.QueryResult, error) {
	// Get column information
	columns, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("failed to get columns: %w", err)
	}

	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, fmt.Errorf("failed to get column types: %w", err)
	}

	// Build field information
	fields := make([]*query.Field, len(columns))
	for i, col := range columns {
		fields[i] = &query.Field{
			Name: col,
			Type: columnTypes[i].DatabaseTypeName(),
		}
	}

	// Read rows
	var resultRows []*query.Row
	scanValues := make([]any, len(columns))
	scanPointers := make([]any, len(columns))

	for i := range scanValues {
		scanPointers[i] = &scanValues[i]
	}

	rowCount := uint64(0)
	for rows.Next() && (maxRows == 0 || rowCount < maxRows) {
		if err := rows.Scan(scanPointers...); err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}

		// Convert values to bytes
		values := make([][]byte, len(columns))
		for i, val := range scanValues {
			if val == nil {
				values[i] = nil
			} else if b, ok := val.([]byte); ok {
				// lib/pq returns TEXT as []byte - use it directly
				values[i] = b
			} else {
				values[i] = fmt.Appendf(nil, "%v", val)
			}
		}

		resultRows = append(resultRows, &query.Row{Values: values})
		rowCount++
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error reading rows: %w", err)
	}

	// Generate command tag for SELECT
	commandTag := fmt.Sprintf("SELECT %d", rowCount)

	return &query.QueryResult{
		Fields:       fields,
		RowsAffected: 0, // SELECT queries don't affect rows
		Rows:         resultRows,
		CommandTag:   commandTag,
	}, nil
}

// executeSelectQuery executes a SELECT query and returns rows.
func (e *Executor) executeSelectQuery(ctx context.Context, queryStr string, maxRows uint64) (*query.QueryResult, error) {
	rows, err := e.db.QueryContext(ctx, queryStr)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}
	defer rows.Close()

	return e.scanQueryRows(rows, maxRows)
}

// executeModifyQuery executes an INSERT, UPDATE, DELETE, or other modification query.
func (e *Executor) executeModifyQuery(ctx context.Context, queryStr string) (*query.QueryResult, error) {
	result, err := e.db.ExecContext(ctx, queryStr)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		// Some queries don't support RowsAffected, that's okay
		rowsAffected = 0
	}

	// Generate command tag based on query type
	commandTag := e.generateCommandTag(queryStr, uint64(rowsAffected))

	return &query.QueryResult{
		Fields:       []*query.Field{}, // No fields for modification queries
		RowsAffected: uint64(rowsAffected),
		Rows:         []*query.Row{}, // No rows for modification queries
		CommandTag:   commandTag,
	}, nil
}

// generateCommandTag generates a PostgreSQL command tag for the result.
func (e *Executor) generateCommandTag(queryStr string, rowsAffected uint64) string {
	trimmedQuery := strings.TrimSpace(strings.ToUpper(queryStr))

	switch {
	case strings.HasPrefix(trimmedQuery, "INSERT"):
		return fmt.Sprintf("INSERT 0 %d", rowsAffected)
	case strings.HasPrefix(trimmedQuery, "UPDATE"):
		return fmt.Sprintf("UPDATE %d", rowsAffected)
	case strings.HasPrefix(trimmedQuery, "DELETE"):
		return fmt.Sprintf("DELETE %d", rowsAffected)
	case strings.HasPrefix(trimmedQuery, "CREATE TABLE"):
		return "CREATE TABLE"
	case strings.HasPrefix(trimmedQuery, "DROP TABLE"):
		return "DROP TABLE"
	case strings.HasPrefix(trimmedQuery, "ALTER TABLE"):
		return "ALTER TABLE"
	case strings.HasPrefix(trimmedQuery, "CREATE INDEX"):
		return "CREATE INDEX"
	case strings.HasPrefix(trimmedQuery, "DROP INDEX"):
		return "DROP INDEX"
	default:
		// Generic command complete
		return "COMMAND"
	}
}

// Ensure Executor implements queryservice.QueryService
var _ queryservice.QueryService = (*Executor)(nil)
