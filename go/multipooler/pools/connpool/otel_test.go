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

package connpool

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/semconv/v1.37.0/dbconv"

	"github.com/multigres/multigres/go/multipooler/connstate"
)

// otelTestHelper captures OTel metrics for testing.
type otelTestHelper struct {
	reader    *metric.ManualReader
	provider  *metric.MeterProvider
	connCount ConnectionCount
	connMax   dbconv.ClientConnectionMax
}

// newOTelTestHelper creates a new test helper with a real OTel meter that captures metrics.
func newOTelTestHelper(t *testing.T) *otelTestHelper {
	t.Helper()

	reader := metric.NewManualReader()
	provider := metric.NewMeterProvider(metric.WithReader(reader))
	meter := provider.Meter("test")

	connCount, err := NewConnectionCount(meter)
	require.NoError(t, err)

	connMax, err := dbconv.NewClientConnectionMax(meter)
	require.NoError(t, err)

	return &otelTestHelper{
		reader:    reader,
		provider:  provider,
		connCount: connCount,
		connMax:   connMax,
	}
}

// shutdown cleans up the OTel provider.
func (h *otelTestHelper) shutdown(ctx context.Context) error {
	return h.provider.Shutdown(ctx)
}

// connectionCounts returns the current idle and used connection counts for the given pool.
func (h *otelTestHelper) connectionCounts(ctx context.Context, poolName string) (idle, used int64) {
	var rm metricdata.ResourceMetrics
	if err := h.reader.Collect(ctx, &rm); err != nil {
		return 0, 0
	}

	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name == "db.client.connection.count" {
				sum, ok := m.Data.(metricdata.Sum[int64])
				if !ok {
					continue
				}
				for _, dp := range sum.DataPoints {
					// Check pool name attribute
					var matchesPool bool
					var state string
					for _, attr := range dp.Attributes.ToSlice() {
						if string(attr.Key) == "db.client.connection.pool.name" && attr.Value.AsString() == poolName {
							matchesPool = true
						}
						if string(attr.Key) == "db.client.connection.state" {
							state = attr.Value.AsString()
						}
					}
					if matchesPool {
						switch state {
						case "idle":
							idle = dp.Value
						case "used":
							used = dp.Value
						}
					}
				}
			}
		}
	}
	return idle, used
}

// newTestPoolWithOTel creates a test pool with OTel instrumentation.
func newTestPoolWithOTel(t *testing.T, capacity int64, helper *otelTestHelper) *Pool[*mockConnection] {
	t.Helper()

	pool := NewPool[*mockConnection](&Config{
		Capacity:        capacity,
		MaxIdleCount:    capacity,
		ConnectionCount: &helper.connCount,
		ConnectionMax:   &helper.connMax,
	})
	pool.Name = "test-pool"
	pool.Open(context.Background(), func(ctx context.Context) (*mockConnection, error) {
		return newMockConnection(), nil
	}, nil)
	return pool
}

func TestOTelBasicGetPut(t *testing.T) {
	ctx := context.Background()
	helper := newOTelTestHelper(t)
	defer func() { _ = helper.shutdown(ctx) }()

	pool := newTestPoolWithOTel(t, 10, helper)
	defer pool.Close()

	// Initially no connections
	idle, used := helper.connectionCounts(ctx, "test-pool")
	assert.Equal(t, int64(0), idle, "initial idle should be 0")
	assert.Equal(t, int64(0), used, "initial used should be 0")

	// Get a connection: ∅→used
	conn1, err := pool.Get(ctx)
	require.NoError(t, err)

	idle, used = helper.connectionCounts(ctx, "test-pool")
	assert.Equal(t, int64(0), idle, "after get: idle should be 0")
	assert.Equal(t, int64(1), used, "after get: used should be 1")

	// Return connection: used→idle
	conn1.Recycle()

	idle, used = helper.connectionCounts(ctx, "test-pool")
	assert.Equal(t, int64(1), idle, "after recycle: idle should be 1")
	assert.Equal(t, int64(0), used, "after recycle: used should be 0")

	// Get again: idle→used
	conn2, err := pool.Get(ctx)
	require.NoError(t, err)

	idle, used = helper.connectionCounts(ctx, "test-pool")
	assert.Equal(t, int64(0), idle, "after second get: idle should be 0")
	assert.Equal(t, int64(1), used, "after second get: used should be 1")

	conn2.Recycle()
}

func TestOTelMultipleConnections(t *testing.T) {
	ctx := context.Background()
	helper := newOTelTestHelper(t)
	defer func() { _ = helper.shutdown(ctx) }()

	pool := newTestPoolWithOTel(t, 10, helper)
	defer pool.Close()

	// Get multiple connections
	var conns []*Pooled[*mockConnection]
	for range 5 {
		conn, err := pool.Get(ctx)
		require.NoError(t, err)
		conns = append(conns, conn)
	}

	idle, used := helper.connectionCounts(ctx, "test-pool")
	assert.Equal(t, int64(0), idle, "5 connections borrowed: idle should be 0")
	assert.Equal(t, int64(5), used, "5 connections borrowed: used should be 5")

	// Return some connections
	conns[0].Recycle()
	conns[1].Recycle()

	idle, used = helper.connectionCounts(ctx, "test-pool")
	assert.Equal(t, int64(2), idle, "2 returned: idle should be 2")
	assert.Equal(t, int64(3), used, "3 still borrowed: used should be 3")

	// Return the rest
	for i := 2; i < 5; i++ {
		conns[i].Recycle()
	}

	idle, used = helper.connectionCounts(ctx, "test-pool")
	assert.Equal(t, int64(5), idle, "all returned: idle should be 5")
	assert.Equal(t, int64(0), used, "all returned: used should be 0")
}

func TestOTelConnectionWithSettings(t *testing.T) {
	ctx := context.Background()
	helper := newOTelTestHelper(t)
	defer func() { _ = helper.shutdown(ctx) }()

	pool := newTestPoolWithOTel(t, 10, helper)
	defer pool.Close()

	settings := connstate.NewSettings(map[string]string{"timezone": "UTC"}, 1)

	// Get with settings: ∅→used
	conn, err := pool.GetWithSettings(ctx, settings)
	require.NoError(t, err)

	idle, used := helper.connectionCounts(ctx, "test-pool")
	assert.Equal(t, int64(0), idle)
	assert.Equal(t, int64(1), used)

	// Return: used→idle
	conn.Recycle()

	idle, used = helper.connectionCounts(ctx, "test-pool")
	assert.Equal(t, int64(1), idle)
	assert.Equal(t, int64(0), used)
}

func TestOTelWaiterHandoff(t *testing.T) {
	ctx := context.Background()
	helper := newOTelTestHelper(t)
	defer func() { _ = helper.shutdown(ctx) }()

	// Pool with capacity 1
	pool := newTestPoolWithOTel(t, 1, helper)
	defer pool.Close()

	// Get the only connection
	conn1, err := pool.Get(ctx)
	require.NoError(t, err)

	idle, used := helper.connectionCounts(ctx, "test-pool")
	assert.Equal(t, int64(0), idle)
	assert.Equal(t, int64(1), used)

	// Start a waiter
	done := make(chan *Pooled[*mockConnection])
	go func() {
		conn, _ := pool.Get(ctx)
		done <- conn
	}()

	// Give waiter time to register
	time.Sleep(50 * time.Millisecond)

	// Return connection - should go directly to waiter (stays used)
	conn1.Recycle()

	conn2 := <-done
	require.NotNil(t, conn2)

	// Connection should still be used (direct handoff)
	idle, used = helper.connectionCounts(ctx, "test-pool")
	assert.Equal(t, int64(0), idle, "direct handoff: idle should be 0")
	assert.Equal(t, int64(1), used, "direct handoff: used should be 1")

	conn2.Recycle()
}

func TestOTelCloseIdleConn(t *testing.T) {
	ctx := context.Background()
	helper := newOTelTestHelper(t)
	defer func() { _ = helper.shutdown(ctx) }()

	pool := newTestPoolWithOTel(t, 10, helper)
	defer pool.Close()

	// Get and return a connection to make it idle
	conn, err := pool.Get(ctx)
	require.NoError(t, err)
	conn.Recycle()

	idle, used := helper.connectionCounts(ctx, "test-pool")
	assert.Equal(t, int64(1), idle)
	assert.Equal(t, int64(0), used)

	// Pop the connection and close it using closeIdleConn
	idleConn := pool.pop(&pool.clean)
	require.NotNil(t, idleConn)
	pool.closeIdleConn(ctx, idleConn)

	idle, used = helper.connectionCounts(ctx, "test-pool")
	assert.Equal(t, int64(0), idle, "after closeIdleConn: idle should be 0")
	assert.Equal(t, int64(0), used, "after closeIdleConn: used should be 0")
}

func TestOTelCloseUsedConn(t *testing.T) {
	ctx := context.Background()
	helper := newOTelTestHelper(t)
	defer func() { _ = helper.shutdown(ctx) }()

	pool := newTestPoolWithOTel(t, 10, helper)
	defer pool.Close()

	// Get a connection (it's now in used state from OTel perspective)
	conn, err := pool.Get(ctx)
	require.NoError(t, err)

	idle, used := helper.connectionCounts(ctx, "test-pool")
	assert.Equal(t, int64(0), idle)
	assert.Equal(t, int64(1), used)

	// Simulate closing a used connection (bypassing normal recycle)
	// This decrements borrowed but we need to also clean up the slot
	pool.borrowed.Add(-1)
	pool.closeUsedConn(ctx, conn)

	idle, used = helper.connectionCounts(ctx, "test-pool")
	assert.Equal(t, int64(0), idle, "after closeUsedConn: idle should be 0")
	assert.Equal(t, int64(0), used, "after closeUsedConn: used should be 0")
}

func TestOTelReleaseUsedSlot(t *testing.T) {
	ctx := context.Background()
	helper := newOTelTestHelper(t)
	defer func() { _ = helper.shutdown(ctx) }()

	pool := newTestPoolWithOTel(t, 10, helper)
	defer pool.Close()

	// Manually increment active and record a used connection
	pool.active.Add(1)
	pool.otelAdd(ctx, dbconv.ClientConnectionStateUsed, 1)

	idle, used := helper.connectionCounts(ctx, "test-pool")
	assert.Equal(t, int64(0), idle)
	assert.Equal(t, int64(1), used)

	// Release the slot (simulating connection creation failure after OTel was recorded)
	pool.releaseUsedSlot(ctx)

	idle, used = helper.connectionCounts(ctx, "test-pool")
	assert.Equal(t, int64(0), idle, "after releaseUsedSlot: idle should be 0")
	assert.Equal(t, int64(0), used, "after releaseUsedSlot: used should be 0")
}

func TestOTelCloseOnIdleLimitReached(t *testing.T) {
	ctx := context.Background()
	helper := newOTelTestHelper(t)
	defer func() { _ = helper.shutdown(ctx) }()

	// Pool with low idle limit
	pool := NewPool[*mockConnection](&Config{
		Capacity:        10,
		MaxIdleCount:    1, // Only 1 idle connection allowed
		ConnectionCount: &helper.connCount,
		ConnectionMax:   &helper.connMax,
	})
	pool.Name = "test-pool"
	pool.Open(ctx, func(ctx context.Context) (*mockConnection, error) {
		return newMockConnection(), nil
	}, nil)
	defer pool.Close()

	// Get two connections
	conn1, err := pool.Get(ctx)
	require.NoError(t, err)
	conn2, err := pool.Get(ctx)
	require.NoError(t, err)

	idle, used := helper.connectionCounts(ctx, "test-pool")
	assert.Equal(t, int64(0), idle)
	assert.Equal(t, int64(2), used)

	// Return first connection - goes to idle
	conn1.Recycle()

	idle, used = helper.connectionCounts(ctx, "test-pool")
	assert.Equal(t, int64(1), idle)
	assert.Equal(t, int64(1), used)

	// Return second connection - should be closed due to idle limit
	conn2.Recycle()

	idle, used = helper.connectionCounts(ctx, "test-pool")
	assert.Equal(t, int64(1), idle, "idle limit reached: should still have 1 idle")
	assert.Equal(t, int64(0), used, "idle limit reached: used should be 0")
}

func TestOTelSetCapacity(t *testing.T) {
	ctx := context.Background()
	helper := newOTelTestHelper(t)
	defer func() { _ = helper.shutdown(ctx) }()

	pool := newTestPoolWithOTel(t, 10, helper)
	defer pool.Close()

	// Create some idle connections
	var conns []*Pooled[*mockConnection]
	for range 5 {
		conn, err := pool.Get(ctx)
		require.NoError(t, err)
		conns = append(conns, conn)
	}
	for _, conn := range conns {
		conn.Recycle()
	}

	idle, used := helper.connectionCounts(ctx, "test-pool")
	assert.Equal(t, int64(5), idle)
	assert.Equal(t, int64(0), used)

	// Reduce capacity to 2 - should close idle connections
	err := pool.SetCapacity(ctx, 2)
	require.NoError(t, err)

	idle, used = helper.connectionCounts(ctx, "test-pool")
	assert.Equal(t, int64(2), idle, "after reducing capacity: idle should be 2")
	assert.Equal(t, int64(0), used, "after reducing capacity: used should be 0")
}
