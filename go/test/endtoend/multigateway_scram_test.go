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

package endtoend

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	_ "github.com/lib/pq" // PostgreSQL driver
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/test/utils"
)

// TestMultiGateway_SCRAMAuthentication tests SCRAM-SHA-256 authentication through multigateway.
// This verifies the full authentication flow: client → multigateway → multipooler → PostgreSQL.
func TestMultiGateway_SCRAMAuthentication(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping SCRAM authentication test in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("PostgreSQL binaries not found, skipping SCRAM authentication tests")
	}

	// Setup full test cluster
	cluster := setupTestCluster(t)
	t.Cleanup(cluster.Cleanup)

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	// Connect to multipooler via gRPC to create test users.
	// We use gRPC because multigateway now requires SCRAM authentication for all connections.
	poolerAddr := fmt.Sprintf("localhost:%d", cluster.PortConfig.Zones[0].MultipoolerGRPCPort)
	poolerClient, err := NewMultiPoolerTestClient(poolerAddr)
	require.NoError(t, err, "failed to connect to multipooler via gRPC")
	defer poolerClient.Close()

	// Create a test user with a SCRAM-SHA-256 password
	testUser := fmt.Sprintf("scramtest_%d", time.Now().UnixNano())
	testPassword := "test_scram_password_123"

	_, err = poolerClient.ExecuteQuery(ctx, fmt.Sprintf("CREATE USER %s WITH PASSWORD '%s'", testUser, testPassword), 0)
	require.NoError(t, err, "failed to create test user")

	// Cleanup user after test
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = poolerClient.ExecuteQuery(cleanupCtx, fmt.Sprintf("DROP USER IF EXISTS %s", testUser), 0)
	})

	// Grant connect privilege on postgres database
	_, err = poolerClient.ExecuteQuery(ctx, fmt.Sprintf("GRANT CONNECT ON DATABASE postgres TO %s", testUser), 0)
	require.NoError(t, err, "failed to grant connect privilege")

	t.Run("successful SCRAM authentication", func(t *testing.T) {
		// Connect as the test user with password - lib/pq will negotiate SCRAM-SHA-256
		userConnStr := fmt.Sprintf("host=localhost port=%d user=%s password=%s dbname=postgres sslmode=disable connect_timeout=5",
			cluster.PortConfig.Zones[0].MultigatewayPGPort, testUser, testPassword)
		userDB, err := sql.Open("postgres", userConnStr)
		require.NoError(t, err, "failed to open user database connection")
		defer userDB.Close()

		// Verify connection works - this confirms SCRAM authentication succeeded
		err = userDB.PingContext(ctx)
		require.NoError(t, err, "SCRAM authentication should succeed with correct password")

		// Execute a query to verify the connection is fully functional
		var result int
		err = userDB.QueryRowContext(ctx, "SELECT 1").Scan(&result)
		require.NoError(t, err, "query should succeed after SCRAM authentication")
		assert.Equal(t, 1, result)

		// Verify that current_user returns the authenticated user
		// This confirms identity propagation via SET SESSION AUTHORIZATION
		var currentUser string
		err = userDB.QueryRowContext(ctx, "SELECT current_user").Scan(&currentUser)
		require.NoError(t, err, "SELECT current_user should succeed")
		assert.Equal(t, testUser, currentUser, "current_user should match authenticated user")
	})

	t.Run("SCRAM authentication fails with wrong password", func(t *testing.T) {
		// Connect with wrong password
		userConnStr := fmt.Sprintf("host=localhost port=%d user=%s password=wrong_password dbname=postgres sslmode=disable connect_timeout=5",
			cluster.PortConfig.Zones[0].MultigatewayPGPort, testUser)
		userDB, err := sql.Open("postgres", userConnStr)
		require.NoError(t, err, "sql.Open should not fail")
		defer userDB.Close()

		// Ping should fail with authentication error
		err = userDB.PingContext(ctx)
		require.Error(t, err, "authentication should fail with wrong password")
		assert.Contains(t, err.Error(), "authentication failed",
			"error should indicate authentication failure")
	})

	t.Run("SCRAM authentication fails for non-existent user", func(t *testing.T) {
		// Connect as non-existent user
		userConnStr := fmt.Sprintf("host=localhost port=%d user=nonexistent_user_xyz password=anypassword dbname=postgres sslmode=disable connect_timeout=5",
			cluster.PortConfig.Zones[0].MultigatewayPGPort)
		userDB, err := sql.Open("postgres", userConnStr)
		require.NoError(t, err, "sql.Open should not fail")
		defer userDB.Close()

		// Ping should fail
		err = userDB.PingContext(ctx)
		require.Error(t, err, "authentication should fail for non-existent user")
	})
}

// TestMultiGateway_SCRAMMultipleConnections tests that multiple SCRAM-authenticated
// connections can be established concurrently.
func TestMultiGateway_SCRAMMultipleConnections(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping SCRAM multiple connections test in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("PostgreSQL binaries not found, skipping test")
	}

	// Setup full test cluster
	cluster := setupTestCluster(t)
	t.Cleanup(cluster.Cleanup)

	ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
	defer cancel()

	// Connect to multipooler via gRPC to create test users.
	// We use gRPC because multigateway now requires SCRAM authentication for all connections.
	poolerAddr := fmt.Sprintf("localhost:%d", cluster.PortConfig.Zones[0].MultipoolerGRPCPort)
	poolerClient, err := NewMultiPoolerTestClient(poolerAddr)
	require.NoError(t, err, "failed to connect to multipooler via gRPC")
	defer poolerClient.Close()

	// Create multiple test users
	const numUsers = 3
	testUsers := make([]string, numUsers)
	testPassword := "shared_password_123"

	for i := range numUsers {
		testUsers[i] = fmt.Sprintf("scramtest_multi_%d_%d", time.Now().UnixNano(), i)
		_, err = poolerClient.ExecuteQuery(ctx, fmt.Sprintf("CREATE USER %s WITH PASSWORD '%s'", testUsers[i], testPassword), 0)
		require.NoError(t, err, "failed to create test user %d", i)
		_, err = poolerClient.ExecuteQuery(ctx, fmt.Sprintf("GRANT CONNECT ON DATABASE postgres TO %s", testUsers[i]), 0)
		require.NoError(t, err, "failed to grant connect to user %d", i)
	}

	// Cleanup users after test
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		for _, user := range testUsers {
			_, _ = poolerClient.ExecuteQuery(cleanupCtx, fmt.Sprintf("DROP USER IF EXISTS %s", user), 0)
		}
	})

	// Connect as each user concurrently and verify SCRAM authentication succeeds
	type result struct {
		userIndex int
		err       error
	}
	results := make(chan result, numUsers)

	for i, user := range testUsers {
		go func(idx int, username string) {
			userConnStr := fmt.Sprintf("host=localhost port=%d user=%s password=%s dbname=postgres sslmode=disable connect_timeout=5",
				cluster.PortConfig.Zones[0].MultigatewayPGPort, username, testPassword)
			userDB, err := sql.Open("postgres", userConnStr)
			if err != nil {
				results <- result{userIndex: idx, err: err}
				return
			}
			defer userDB.Close()

			// Execute a simple query to verify the connection is fully functional
			var one int
			err = userDB.QueryRowContext(ctx, "SELECT 1").Scan(&one)
			results <- result{userIndex: idx, err: err}
		}(i, user)
	}

	// Collect results - all concurrent SCRAM authentications should succeed
	for range numUsers {
		r := <-results
		require.NoError(t, r.err, "user %d SCRAM authentication failed", r.userIndex)
	}
}

// TestMultiGateway_SessionAuthorizationSandbox tests that users cannot escape their
// sandboxed session by executing RESET SESSION AUTHORIZATION or SET SESSION AUTHORIZATION.
//
// SECURITY: This is a critical security test. Without proper SQL filtering, a malicious
// user could execute "RESET SESSION AUTHORIZATION" to escalate privileges to the
// superuser that the connection pool uses internally.
func TestMultiGateway_SessionAuthorizationSandbox(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping session authorization sandbox test in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("PostgreSQL binaries not found, skipping test")
	}

	// Setup full test cluster
	cluster := setupTestCluster(t)
	t.Cleanup(cluster.Cleanup)

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	// Connect to multipooler via gRPC to create test user
	poolerAddr := fmt.Sprintf("localhost:%d", cluster.PortConfig.Zones[0].MultipoolerGRPCPort)
	poolerClient, err := NewMultiPoolerTestClient(poolerAddr)
	require.NoError(t, err, "failed to connect to multipooler via gRPC")
	defer poolerClient.Close()

	// Create a test user
	testUser := fmt.Sprintf("sandbox_test_%d", time.Now().UnixNano())
	testPassword := "sandbox_password_123"

	_, err = poolerClient.ExecuteQuery(ctx, fmt.Sprintf("CREATE USER %s WITH PASSWORD '%s'", testUser, testPassword), 0)
	require.NoError(t, err, "failed to create test user")

	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = poolerClient.ExecuteQuery(cleanupCtx, fmt.Sprintf("DROP USER IF EXISTS %s", testUser), 0)
	})

	_, err = poolerClient.ExecuteQuery(ctx, fmt.Sprintf("GRANT CONNECT ON DATABASE postgres TO %s", testUser), 0)
	require.NoError(t, err, "failed to grant connect privilege")

	// Connect as test user
	userConnStr := fmt.Sprintf("host=localhost port=%d user=%s password=%s dbname=postgres sslmode=disable connect_timeout=5",
		cluster.PortConfig.Zones[0].MultigatewayPGPort, testUser, testPassword)
	userDB, err := sql.Open("postgres", userConnStr)
	require.NoError(t, err, "failed to open user database connection")
	defer userDB.Close()

	// Verify initial identity
	var currentUser string
	err = userDB.QueryRowContext(ctx, "SELECT current_user").Scan(&currentUser)
	require.NoError(t, err, "SELECT current_user should succeed")
	require.Equal(t, testUser, currentUser, "current_user should be test user initially")

	t.Run("RESET SESSION AUTHORIZATION should reset to authenticated user", func(t *testing.T) {
		// TODO(Phase4): Remove this skip once SQL filtering is implemented.
		// See docs/plans/2024-12-05-scram-authentication-design.md Phase 4.
		t.Skip("KNOWN SECURITY GAP: Phase 4 SQL filtering not yet implemented")

		// SECURITY: RESET SESSION AUTHORIZATION should be rewritten to:
		//   SET SESSION AUTHORIZATION '<authenticated_user>'
		// This makes RESET a no-op that keeps the user as the authenticated user,
		// rather than escalating to the superuser.
		//
		// Without this rewrite, an attacker could send multi-statement queries like:
		//   "RESET SESSION AUTHORIZATION; DROP TABLE users"
		// and the DROP would execute as the superuser.

		_, err := userDB.ExecContext(ctx, "RESET SESSION AUTHORIZATION")
		require.NoError(t, err, "RESET SESSION AUTHORIZATION should succeed (as a rewritten no-op)")

		// After RESET, current_user should still be the authenticated test user
		var userAfterReset string
		err = userDB.QueryRowContext(ctx, "SELECT current_user").Scan(&userAfterReset)
		require.NoError(t, err, "SELECT current_user should succeed")
		assert.Equal(t, testUser, userAfterReset,
			"SECURITY GAP: RESET SESSION AUTHORIZATION escalated to %q instead of staying as %q",
			userAfterReset, testUser)
	})

	t.Run("SET SESSION AUTHORIZATION should be blocked", func(t *testing.T) {
		// TODO(Phase4): Remove this skip once SQL filtering is implemented.
		// See docs/plans/2024-12-05-scram-authentication-design.md Phase 4.
		t.Skip("KNOWN SECURITY GAP: Phase 4 SQL filtering not yet implemented")

		// SECURITY: SET SESSION AUTHORIZATION should be blocked entirely.
		// It's a superuser-only command that allows impersonating other users.

		_, err := userDB.ExecContext(ctx, "SET SESSION AUTHORIZATION 'postgres'")

		// Once Phase 4 is implemented, this should return an error.
		if err != nil {
			// Good - SET was blocked
			t.Logf("SET SESSION AUTHORIZATION was blocked: %v", err)
		} else {
			// Bad - SET was allowed. This is the security gap.
			t.Error("SECURITY GAP: SET SESSION AUTHORIZATION 'postgres' was not blocked. " +
				"This allows direct privilege escalation.")
		}
	})
}
