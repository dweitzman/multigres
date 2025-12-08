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

// Package userpool provides per-user connection pool management for multipooler.
// Each user gets their own connection pool, with connections authenticated using
// SCRAM key passthrough (extracted during client authentication).
package userpool

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"
)

var (
	// ErrGlobalCapReached is returned when the global connection cap is reached.
	ErrGlobalCapReached = errors.New("global connection cap reached")

	// ErrPoolClosed is returned when trying to use a closed pool.
	ErrPoolClosed = errors.New("pool is closed")
)

// Connection is the interface that pooled connections must implement.
type Connection interface {
	Close() error
	IsClosed() bool
	// Reset cleans up connection state (e.g., RESET ROLE) before returning to pool.
	Reset(ctx context.Context) error
}

// ConnectorFunc creates a new connection for a specific user using SCRAM keys.
// This is called when a pool needs to create a new connection to PostgreSQL.
type ConnectorFunc[C Connection] func(ctx context.Context, username string, clientKey, serverKey []byte) (C, error)

// SCRAMKeys holds the SCRAM keys needed for authentication.
// These are extracted during client authentication and passed through for backend auth.
type SCRAMKeys struct {
	ClientKey []byte
	ServerKey []byte
}

// ManagerConfig holds configuration for the user pool manager.
type ManagerConfig struct {
	// MaxPoolsPerManager is the maximum number of user pools to maintain.
	// When exceeded, least recently used pools are evicted.
	MaxPoolsPerManager int

	// MaxConnectionsPerPool is the maximum connections per user pool.
	MaxConnectionsPerPool int

	// IdlePoolTimeout is how long an idle pool (no active connections) is kept
	// before being garbage collected.
	IdlePoolTimeout time.Duration

	// GlobalConnectionCap is the maximum total connections across all pools.
	// This prevents resource exhaustion when many users connect.
	GlobalConnectionCap int

	// Logger for pool manager events. If nil, slog.Default() is used.
	Logger *slog.Logger
}

// PooledConnection wraps a connection with pool management metadata.
type PooledConnection[C Connection] struct {
	Conn     C
	username string
	pool     *userPool[C]
}

// userPool represents a connection pool for a single user.
type userPool[C Connection] struct {
	mu        sync.Mutex
	username  string
	clientKey []byte
	serverKey []byte
	lastUsed  time.Time
	idle      []C // idle connections available for reuse
	active    int // number of connections currently checked out
	maxSize   int // maximum connections in this pool
	closed    bool
}

func newUserPool[C Connection](username string, clientKey, serverKey []byte, maxSize int) *userPool[C] {
	return &userPool[C]{
		username:  username,
		clientKey: clientKey,
		serverKey: serverKey,
		lastUsed:  time.Now(),
		idle:      make([]C, 0, maxSize),
		maxSize:   maxSize,
	}
}

func (p *userPool[C]) getOrCreate(ctx context.Context, connector ConnectorFunc[C]) (C, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	var zero C
	if p.closed {
		return zero, ErrPoolClosed
	}

	p.lastUsed = time.Now()

	// Try to get an idle connection
	if len(p.idle) > 0 {
		conn := p.idle[len(p.idle)-1]
		p.idle = p.idle[:len(p.idle)-1]
		p.active++
		return conn, nil
	}

	// Create a new connection if under pool limit
	if p.active < p.maxSize {
		conn, err := connector(ctx, p.username, p.clientKey, p.serverKey)
		if err != nil {
			return zero, err
		}
		p.active++
		return conn, nil
	}

	// Pool is at capacity - could wait or return error
	// For simplicity, return error; in production would use waitlist
	return zero, errors.New("pool at capacity")
}

func (p *userPool[C]) returnConn(conn C) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		conn.Close()
		return
	}

	p.lastUsed = time.Now()
	p.active--

	// Return to idle pool
	p.idle = append(p.idle, conn)
}

func (p *userPool[C]) isIdle() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.active == 0
}

func (p *userPool[C]) idleSince() time.Time {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.active > 0 {
		return time.Now() // Not idle
	}
	return p.lastUsed
}

func (p *userPool[C]) close() {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.closed = true
	for _, conn := range p.idle {
		conn.Close()
	}
	p.idle = nil
}

// Manager manages multiple per-user connection pools.
// It handles pool lifecycle, connection limits, and garbage collection.
type Manager[C Connection] struct {
	config ManagerConfig
	logger *slog.Logger

	mu              sync.RWMutex
	pools           map[string]*userPool[C]
	globalConnCount int
	closed          bool
}

// NewManager creates a new user pool manager.
func NewManager[C Connection](config ManagerConfig) *Manager[C] {
	logger := config.Logger
	if logger == nil {
		logger = slog.Default()
	}
	return &Manager[C]{
		config: config,
		logger: logger,
		pools:  make(map[string]*userPool[C]),
	}
}

// GetConnection returns a connection for the specified user.
// If no pool exists for the user, one is created using the provided SCRAM keys.
// The connector function is called to create new connections when needed.
func (m *Manager[C]) GetConnection(ctx context.Context, username string, keys *SCRAMKeys, connector ConnectorFunc[C]) (*PooledConnection[C], error) {
	pool, err := m.getOrCreatePool(username, keys)
	if err != nil {
		return nil, err
	}

	// Check global cap before creating connection
	if !m.tryReserveGlobalConnection() {
		return nil, ErrGlobalCapReached
	}

	conn, err := pool.getOrCreate(ctx, connector)
	if err != nil {
		m.releaseGlobalConnection()
		return nil, err
	}

	return &PooledConnection[C]{
		Conn:     conn,
		username: username,
		pool:     pool,
	}, nil
}

func (m *Manager[C]) getOrCreatePool(username string, keys *SCRAMKeys) (*userPool[C], error) {
	// Fast path: check if pool exists
	m.mu.RLock()
	if m.closed {
		m.mu.RUnlock()
		return nil, ErrPoolClosed
	}
	pool, exists := m.pools[username]
	m.mu.RUnlock()

	if exists {
		return pool, nil
	}

	// Slow path: create pool
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closed {
		return nil, ErrPoolClosed
	}

	// Double-check after acquiring write lock
	if pool, exists = m.pools[username]; exists {
		return pool, nil
	}

	// Create new pool
	pool = newUserPool[C](username, keys.ClientKey, keys.ServerKey, m.config.MaxConnectionsPerPool)
	m.pools[username] = pool

	return pool, nil
}

func (m *Manager[C]) tryReserveGlobalConnection() bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.globalConnCount >= m.config.GlobalConnectionCap {
		return false
	}
	m.globalConnCount++
	return true
}

func (m *Manager[C]) releaseGlobalConnection() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.globalConnCount--
}

// ReturnConnection returns a connection to its pool for reuse.
// It runs cleanup (RESET ROLE) to ensure connections have predictable state.
//
// In transaction-mode pooling, connections are returned after each query
// (outside transactions) or after commit/rollback (inside transactions).
// The cleanup ensures SET ROLE and other session state doesn't leak between
// different queries or transactions.
func (m *Manager[C]) ReturnConnection(ctx context.Context, conn *PooledConnection[C]) {
	if conn == nil || conn.pool == nil {
		return
	}

	// Reset connection state before returning to pool.
	// This ensures SET ROLE changes don't persist across queries/transactions.
	if err := conn.Conn.Reset(ctx); err != nil {
		m.logger.WarnContext(ctx, "failed to reset connection before returning to pool",
			"username", conn.username,
			"error", err)
		// Close the connection instead of returning one with potentially dirty state
		conn.Conn.Close()
		m.releaseGlobalConnection()
		return
	}

	conn.pool.returnConn(conn.Conn)
	m.releaseGlobalConnection()
}

// PoolCount returns the number of active user pools.
func (m *Manager[C]) PoolCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.pools)
}

// HasPoolForUser returns whether a pool exists for the given user.
func (m *Manager[C]) HasPoolForUser(username string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	_, exists := m.pools[username]
	return exists
}

// CleanupIdlePools removes pools that have been idle longer than IdlePoolTimeout.
func (m *Manager[C]) CleanupIdlePools() {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now()
	for username, pool := range m.pools {
		if pool.isIdle() && now.Sub(pool.idleSince()) > m.config.IdlePoolTimeout {
			pool.close()
			delete(m.pools, username)
		}
	}
}

// Close shuts down all pools and closes all connections.
func (m *Manager[C]) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.closed = true

	for username, pool := range m.pools {
		pool.close()
		delete(m.pools, username)
	}

	return nil
}
