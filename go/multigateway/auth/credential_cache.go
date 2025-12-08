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
	"errors"
	"sync"
	"time"

	"github.com/multigres/multigres/go/pgprotocol/auth"
)

// credentialKey uniquely identifies a cached credential.
type credentialKey struct {
	username string
	database string
}

// credentialEntry holds a cached credential with metadata.
type credentialEntry struct {
	hash       *auth.ScramHash
	userExists bool // false means negative cache entry (user not found)
	fetchedAt  time.Time
}

// CredentialCache wraps a PasswordHashProvider with TTL-based caching.
// It caches successful lookups and also caches "user not found" results
// (negative caching) to prevent repeated lookups for non-existent users.
//
// Alternative approaches considered for future:
//
//   - No caching (always fetch): pgbouncer's auth_query fetches credentials
//     on every connection. Simpler but adds latency to each authentication.
//     Could be useful when password changes must take effect immediately.
//
//   - Streaming updates: multipooler could push credential changes via
//     StreamCredentialUpdates gRPC stream, eliminating TTL staleness entirely.
//     More complex but provides real-time invalidation when passwords change.
//
// The current TTL-based approach balances simplicity with performance.
// Auth failures can call Invalidate() to handle password changes gracefully.
type CredentialCache struct {
	provider auth.PasswordHashProvider
	ttl      time.Duration

	mu      sync.Mutex
	entries map[credentialKey]*credentialEntry
}

// NewCredentialCache creates a new CredentialCache wrapping the given provider.
// The TTL controls how long entries are cached before being refreshed.
func NewCredentialCache(provider auth.PasswordHashProvider, ttl time.Duration) *CredentialCache {
	return &CredentialCache{
		provider: provider,
		ttl:      ttl,
		entries:  make(map[credentialKey]*credentialEntry),
	}
}

// GetPasswordHash retrieves a password hash, using the cache when possible.
// Returns cached results within TTL, otherwise fetches from the underlying provider.
func (c *CredentialCache) GetPasswordHash(ctx context.Context, username, database string) (*auth.ScramHash, error) {
	key := credentialKey{username: username, database: database}

	// Fast path: check cache
	c.mu.Lock()
	entry, exists := c.entries[key]
	c.mu.Unlock()

	if exists && time.Since(entry.fetchedAt) < c.ttl {
		// Cache hit - return cached result
		if !entry.userExists {
			return nil, auth.ErrUserNotFound
		}
		return entry.hash, nil
	}

	// Cache miss or expired - fetch from provider
	hash, err := c.provider.GetPasswordHash(ctx, username, database)

	// Update cache
	c.mu.Lock()
	defer c.mu.Unlock()

	if errors.Is(err, auth.ErrUserNotFound) {
		// Negative cache entry
		c.entries[key] = &credentialEntry{
			userExists: false,
			fetchedAt:  time.Now(),
		}
		return nil, auth.ErrUserNotFound
	}

	if err != nil {
		// Don't cache other errors
		return nil, err
	}

	// Successful lookup - cache the hash
	c.entries[key] = &credentialEntry{
		hash:       hash,
		userExists: true,
		fetchedAt:  time.Now(),
	}

	return hash, nil
}

// Invalidate removes an entry from the cache, forcing the next lookup
// to fetch from the underlying provider. This is useful when authentication
// fails and the cached hash may be stale (e.g., password changed).
func (c *CredentialCache) Invalidate(username, database string) {
	key := credentialKey{username: username, database: database}

	c.mu.Lock()
	defer c.mu.Unlock()

	delete(c.entries, key)
}

// Ensure CredentialCache implements auth.PasswordHashProvider
var _ auth.PasswordHashProvider = (*CredentialCache)(nil)
