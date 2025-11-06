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

package connpool

import (
	"context"
	"database/sql"
)

// Conn wraps a sql.Conn and provides methods to execute queries.
// Connections must be recycled after use by calling Recycle().
type Conn struct {
	conn *sql.Conn
}

// QueryContext executes a query that returns rows.
// The caller must close the returned rows when done.
func (c *Conn) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	return c.conn.QueryContext(ctx, query, args...)
}

// QueryRowContext executes a query that returns at most one row.
func (c *Conn) QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row {
	return c.conn.QueryRowContext(ctx, query, args...)
}

// ExecContext executes a query without returning any rows.
func (c *Conn) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	return c.conn.ExecContext(ctx, query, args...)
}

// Recycle returns the connection to the pool.
// The connection should not be used after calling this method.
func (c *Conn) Recycle() {
	c.conn.Close()
}
