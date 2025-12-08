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

package auth

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/pgprotocol/auth"
)

// mockHashProvider tracks calls and returns configurable results.
type mockHashProvider struct {
	calls      atomic.Int32
	hash       *auth.ScramHash
	err        error
	callRecord []struct{ username, database string }
}

func (m *mockHashProvider) GetPasswordHash(_ context.Context, username, database string) (*auth.ScramHash, error) {
	m.calls.Add(1)
	m.callRecord = append(m.callRecord, struct{ username, database string }{username, database})
	return m.hash, m.err
}

func TestCredentialCache_CacheMiss(t *testing.T) {
	// First request should call underlying provider
	hash := &auth.ScramHash{
		StoredKey:  []byte("stored-key"),
		ServerKey:  []byte("server-key"),
		Salt:       []byte("salt"),
		Iterations: 4096,
	}
	mock := &mockHashProvider{hash: hash}
	cache := NewCredentialCache(mock, 1*time.Hour)

	ctx := context.Background()
	result, err := cache.GetPasswordHash(ctx, "testuser", "testdb")

	require.NoError(t, err)
	require.Equal(t, hash, result)
	require.Equal(t, int32(1), mock.calls.Load(), "should call underlying provider on cache miss")
}

func TestCredentialCache_CacheHit(t *testing.T) {
	// Second request should return cached value without calling provider
	hash := &auth.ScramHash{
		StoredKey:  []byte("stored-key"),
		ServerKey:  []byte("server-key"),
		Salt:       []byte("salt"),
		Iterations: 4096,
	}
	mock := &mockHashProvider{hash: hash}
	cache := NewCredentialCache(mock, 1*time.Hour)

	ctx := context.Background()

	// First call - cache miss
	_, err := cache.GetPasswordHash(ctx, "testuser", "testdb")
	require.NoError(t, err)
	require.Equal(t, int32(1), mock.calls.Load())

	// Second call - cache hit
	result, err := cache.GetPasswordHash(ctx, "testuser", "testdb")
	require.NoError(t, err)
	require.Equal(t, hash, result)
	require.Equal(t, int32(1), mock.calls.Load(), "should NOT call underlying provider on cache hit")
}

func TestCredentialCache_TTLExpiry(t *testing.T) {
	// After TTL expires, should call provider again
	hash := &auth.ScramHash{
		StoredKey:  []byte("stored-key"),
		ServerKey:  []byte("server-key"),
		Salt:       []byte("salt"),
		Iterations: 4096,
	}
	mock := &mockHashProvider{hash: hash}
	ttl := 50 * time.Millisecond
	cache := NewCredentialCache(mock, ttl)

	ctx := context.Background()

	// First call - cache miss
	_, err := cache.GetPasswordHash(ctx, "testuser", "testdb")
	require.NoError(t, err)
	require.Equal(t, int32(1), mock.calls.Load())

	// Wait for TTL to expire
	time.Sleep(ttl + 10*time.Millisecond)

	// Third call - cache expired, should fetch again
	_, err = cache.GetPasswordHash(ctx, "testuser", "testdb")
	require.NoError(t, err)
	require.Equal(t, int32(2), mock.calls.Load(), "should call provider after TTL expires")
}

func TestCredentialCache_NegativeCaching(t *testing.T) {
	// Non-existent users should be cached to prevent repeated lookups
	mock := &mockHashProvider{err: auth.ErrUserNotFound}
	cache := NewCredentialCache(mock, 1*time.Hour)

	ctx := context.Background()

	// First call - user not found
	_, err := cache.GetPasswordHash(ctx, "nonexistent", "testdb")
	require.ErrorIs(t, err, auth.ErrUserNotFound)
	require.Equal(t, int32(1), mock.calls.Load())

	// Second call - should return cached "not found" without calling provider
	_, err = cache.GetPasswordHash(ctx, "nonexistent", "testdb")
	require.ErrorIs(t, err, auth.ErrUserNotFound)
	require.Equal(t, int32(1), mock.calls.Load(), "should cache ErrUserNotFound to prevent repeated lookups")
}

func TestCredentialCache_Invalidate(t *testing.T) {
	// Invalidate should force next lookup to fetch from provider
	hash := &auth.ScramHash{
		StoredKey:  []byte("stored-key"),
		ServerKey:  []byte("server-key"),
		Salt:       []byte("salt"),
		Iterations: 4096,
	}
	mock := &mockHashProvider{hash: hash}
	cache := NewCredentialCache(mock, 1*time.Hour)

	ctx := context.Background()

	// First call - cache miss
	_, err := cache.GetPasswordHash(ctx, "testuser", "testdb")
	require.NoError(t, err)
	require.Equal(t, int32(1), mock.calls.Load())

	// Second call - cache hit
	_, err = cache.GetPasswordHash(ctx, "testuser", "testdb")
	require.NoError(t, err)
	require.Equal(t, int32(1), mock.calls.Load())

	// Invalidate the cache entry
	cache.Invalidate("testuser", "testdb")

	// Third call - should fetch from provider again
	_, err = cache.GetPasswordHash(ctx, "testuser", "testdb")
	require.NoError(t, err)
	require.Equal(t, int32(2), mock.calls.Load(), "should call provider after invalidation")
}

func TestCredentialCache_DifferentUsersSameDatabase(t *testing.T) {
	// Different users should have separate cache entries
	hash := &auth.ScramHash{
		StoredKey:  []byte("stored-key"),
		ServerKey:  []byte("server-key"),
		Salt:       []byte("salt"),
		Iterations: 4096,
	}
	mock := &mockHashProvider{hash: hash}
	cache := NewCredentialCache(mock, 1*time.Hour)

	ctx := context.Background()

	// User 1
	_, err := cache.GetPasswordHash(ctx, "user1", "testdb")
	require.NoError(t, err)
	require.Equal(t, int32(1), mock.calls.Load())

	// User 2 - different cache key
	_, err = cache.GetPasswordHash(ctx, "user2", "testdb")
	require.NoError(t, err)
	require.Equal(t, int32(2), mock.calls.Load(), "different users should have separate cache entries")
}

func TestCredentialCache_SameUserDifferentDatabases(t *testing.T) {
	// Same user in different databases should have separate cache entries
	hash := &auth.ScramHash{
		StoredKey:  []byte("stored-key"),
		ServerKey:  []byte("server-key"),
		Salt:       []byte("salt"),
		Iterations: 4096,
	}
	mock := &mockHashProvider{hash: hash}
	cache := NewCredentialCache(mock, 1*time.Hour)

	ctx := context.Background()

	// Database 1
	_, err := cache.GetPasswordHash(ctx, "testuser", "db1")
	require.NoError(t, err)
	require.Equal(t, int32(1), mock.calls.Load())

	// Database 2 - different cache key
	_, err = cache.GetPasswordHash(ctx, "testuser", "db2")
	require.NoError(t, err)
	require.Equal(t, int32(2), mock.calls.Load(), "same user in different databases should have separate cache entries")
}
