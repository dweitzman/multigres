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

// Package connpool provides a minimal connection pool abstraction over sql.DB.
// This package wraps database connections to prepare for future custom pooling logic.
package connpool

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

// Pool wraps a sql.DB connection pool and provides connection management.
// It hides direct access to sql.DB to enable future custom pooling implementations.
type Pool struct {
	db  *sql.DB
	err error // Connection error if sql.Open failed
}

// NewPool creates a new connection pool by opening a database connection with the provided DSN.
// If the connection fails to open, the error is saved and returned by all Pool methods.
func NewPool(driverName, dsn string) *Pool {
	db, err := sql.Open(driverName, dsn)
	if err != nil {
		return &Pool{err: fmt.Errorf("failed to open database connection: %w", err)}
	}
	return &Pool{db: db}
}

// GetConn acquires a connection from the pool.
// The caller must call Recycle() on the returned connection when done.
func (p *Pool) GetConn(ctx context.Context) (*Conn, error) {
	if p.err != nil {
		return nil, p.err
	}
	conn, err := p.db.Conn(ctx)
	if err != nil {
		return nil, err
	}
	return &Conn{conn: conn}, nil
}

// QueryRowContext is a convenience method that acquires a connection,
// executes a query that returns at most one row, and returns the connection.
// The connection is automatically recycled after the query completes.
func (p *Pool) QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row {
	if p.err != nil {
		// Return a Row that will return the error when scanned
		return &sql.Row{}
	}
	return p.db.QueryRowContext(ctx, query, args...)
}

// ExecContext is a convenience method that acquires a connection,
// executes a query without returning rows, and returns the connection.
// The connection is automatically recycled after execution.
func (p *Pool) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	if p.err != nil {
		return nil, p.err
	}
	return p.db.ExecContext(ctx, query, args...)
}

// QueryContext is a convenience method that acquires a connection,
// executes a query that returns rows, and returns the rows.
// The caller must close the returned rows when done.
func (p *Pool) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	if p.err != nil {
		return nil, p.err
	}
	return p.db.QueryContext(ctx, query, args...)
}

// PingContext verifies the connection to the database is still alive.
func (p *Pool) PingContext(ctx context.Context) error {
	if p.err != nil {
		return p.err
	}
	return p.db.PingContext(ctx)
}

// Ping verifies the connection to the database is still alive.
func (p *Pool) Ping() error {
	if p.err != nil {
		return p.err
	}
	return p.db.Ping()
}

// Close closes the database connection pool.
func (p *Pool) Close() error {
	if p.err != nil {
		return p.err
	}
	if p.db != nil {
		return p.db.Close()
	}
	return nil
}

func (p *Pool) Reconnect() {
	// Hacky method to force replacing all connections:
	// - Allow a max of 1 connection
	// - Set a 1-nanosecond max conection lifetime
	// - Use a connection, forcing it to expire
	// - Reset everything

	p.db.SetMaxOpenConns(1)
	p.db.SetConnMaxLifetime(time.Nanosecond * 1)

	defer p.db.SetMaxOpenConns(0)
	defer p.db.SetConnMaxLifetime(0)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*2)
	defer cancel()

	c, err := p.db.Conn(ctx)
	if err == nil {
		defer c.Close()
	}
}
