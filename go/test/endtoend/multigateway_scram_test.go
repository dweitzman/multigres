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
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/lib/pq"
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
// SECURITY: With per-user connection pools using SCRAM passthrough authentication, each
// user authenticates directly to PostgreSQL as themselves. This means:
// - RESET SESSION AUTHORIZATION resets to the authenticated user (themselves) - harmless
// - SET SESSION AUTHORIZATION requires superuser, which regular users don't have - blocked by PostgreSQL
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

	t.Run("RESET SESSION AUTHORIZATION stays as authenticated user", func(t *testing.T) {
		// With per-user pools using SCRAM passthrough, each user authenticates directly
		// to PostgreSQL as themselves. RESET SESSION AUTHORIZATION resets to the
		// session's authenticated user, which is the test user - not superuser.
		// This is harmless and actually a no-op in our architecture.

		_, err := userDB.ExecContext(ctx, "RESET SESSION AUTHORIZATION")
		require.NoError(t, err, "RESET SESSION AUTHORIZATION should succeed")

		// After RESET, current_user should still be the authenticated test user
		// because the connection was authenticated as the test user (not superuser)
		var userAfterReset string
		err = userDB.QueryRowContext(ctx, "SELECT current_user").Scan(&userAfterReset)
		require.NoError(t, err, "SELECT current_user should succeed")
		assert.Equal(t, testUser, userAfterReset,
			"RESET SESSION AUTHORIZATION should stay as authenticated user %q, got %q",
			testUser, userAfterReset)
	})

	t.Run("SET SESSION AUTHORIZATION rejected by PostgreSQL", func(t *testing.T) {
		// With per-user pools, users authenticate as themselves (not superuser).
		// SET SESSION AUTHORIZATION requires superuser privileges, so PostgreSQL
		// will reject this with a permission error.

		_, err := userDB.ExecContext(ctx, "SET SESSION AUTHORIZATION 'postgres'")

		// PostgreSQL should reject this because the user is not a superuser
		require.Error(t, err, "SET SESSION AUTHORIZATION should be rejected for non-superuser")

		// The PostgreSQL error is in the Detail field of pq.Error.
		// lib/pq's err.Error() only returns Message, so we need to check Detail directly.
		var pqErr *pq.Error
		require.True(t, errors.As(err, &pqErr), "error should be a pq.Error")
		assert.Contains(t, pqErr.Detail, "permission denied",
			"Detail should contain permission denied, got: %q", pqErr.Detail)
	})
}

// TestMultiGateway_ConnectionPoolReuse tests that connections are reused within a user's pool.
// This verifies the per-user connection pooling is working correctly by connecting,
// disconnecting, and reconnecting - the backend PID should remain the same.
func TestMultiGateway_ConnectionPoolReuse(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping connection pool reuse test in short mode")
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
	testUser := fmt.Sprintf("poolreuse_%d", time.Now().UnixNano())
	testPassword := "poolreuse_password_123" //nolint:gosec // Test credentials for e2e test

	_, err = poolerClient.ExecuteQuery(ctx, fmt.Sprintf("CREATE USER %s WITH PASSWORD '%s'", testUser, testPassword), 0)
	require.NoError(t, err, "failed to create test user")

	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = poolerClient.ExecuteQuery(cleanupCtx, fmt.Sprintf("DROP USER IF EXISTS %s", testUser), 0)
	})

	_, err = poolerClient.ExecuteQuery(ctx, fmt.Sprintf("GRANT CONNECT ON DATABASE postgres TO %s", testUser), 0)
	require.NoError(t, err, "failed to grant connect privilege")

	userConnStr := fmt.Sprintf("host=localhost port=%d user=%s password=%s dbname=postgres sslmode=disable connect_timeout=5",
		cluster.PortConfig.Zones[0].MultigatewayPGPort, testUser, testPassword)

	// First connection - get backend PID
	userDB1, err := sql.Open("postgres", userConnStr)
	require.NoError(t, err, "failed to open first connection")
	userDB1.SetMaxOpenConns(1)

	var pid1 int
	err = userDB1.QueryRowContext(ctx, "SELECT pg_backend_pid()").Scan(&pid1)
	require.NoError(t, err, "first pg_backend_pid query should succeed")
	require.NotZero(t, pid1, "backend PID should not be zero")

	// Close the first connection - this returns it to multipooler's pool
	userDB1.Close()

	// Second connection - should reuse the same backend connection from multipooler's pool
	userDB2, err := sql.Open("postgres", userConnStr)
	require.NoError(t, err, "failed to open second connection")
	defer userDB2.Close()
	userDB2.SetMaxOpenConns(1)

	var pid2 int
	err = userDB2.QueryRowContext(ctx, "SELECT pg_backend_pid()").Scan(&pid2)
	require.NoError(t, err, "second pg_backend_pid query should succeed")

	// The backend PID should be the same - proving multipooler reused the connection
	assert.Equal(t, pid1, pid2,
		"backend PID should be the same after reconnect (got %d then %d), proving pool reuse", pid1, pid2)

	// Third connection - verify pool continues to work
	userDB2.Close()
	userDB3, err := sql.Open("postgres", userConnStr)
	require.NoError(t, err, "failed to open third connection")
	defer userDB3.Close()
	userDB3.SetMaxOpenConns(1)

	var pid3 int
	err = userDB3.QueryRowContext(ctx, "SELECT pg_backend_pid()").Scan(&pid3)
	require.NoError(t, err, "third pg_backend_pid query should succeed")
	assert.Equal(t, pid1, pid3,
		"backend PID should still be the same on third reconnect (got %d)", pid3)
}

// TestMultiGateway_UserIsolation tests that users cannot access each other's objects.
// This verifies that per-user pools maintain proper security isolation.
func TestMultiGateway_UserIsolation(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping user isolation test in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("PostgreSQL binaries not found, skipping test")
	}

	// Setup full test cluster
	cluster := setupTestCluster(t)
	t.Cleanup(cluster.Cleanup)

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	// Connect to multipooler via gRPC to create test users
	poolerAddr := fmt.Sprintf("localhost:%d", cluster.PortConfig.Zones[0].MultipoolerGRPCPort)
	poolerClient, err := NewMultiPoolerTestClient(poolerAddr)
	require.NoError(t, err, "failed to connect to multipooler via gRPC")
	defer poolerClient.Close()

	// Create two test users
	timestamp := time.Now().UnixNano()
	userAlice := fmt.Sprintf("alice_%d", timestamp)
	userBob := fmt.Sprintf("bob_%d", timestamp)
	password := "test_password_123"

	for _, user := range []string{userAlice, userBob} {
		_, err = poolerClient.ExecuteQuery(ctx, fmt.Sprintf("CREATE USER %s WITH PASSWORD '%s'", user, password), 0)
		require.NoError(t, err, "failed to create user %s", user)
		_, err = poolerClient.ExecuteQuery(ctx, fmt.Sprintf("GRANT CONNECT ON DATABASE postgres TO %s", user), 0)
		require.NoError(t, err, "failed to grant connect to %s", user)
	}

	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		// Drop tables first (as superuser)
		_, _ = poolerClient.ExecuteQuery(cleanupCtx, fmt.Sprintf("DROP TABLE IF EXISTS %s_secret", userAlice), 0)
		_, _ = poolerClient.ExecuteQuery(cleanupCtx, fmt.Sprintf("DROP TABLE IF EXISTS %s_secret", userBob), 0)
		// Then drop users
		_, _ = poolerClient.ExecuteQuery(cleanupCtx, fmt.Sprintf("DROP USER IF EXISTS %s", userAlice), 0)
		_, _ = poolerClient.ExecuteQuery(cleanupCtx, fmt.Sprintf("DROP USER IF EXISTS %s", userBob), 0)
	})

	// Grant CREATE permission on public schema to both users
	_, err = poolerClient.ExecuteQuery(ctx, fmt.Sprintf("GRANT CREATE ON SCHEMA public TO %s, %s", userAlice, userBob), 0)
	require.NoError(t, err, "failed to grant CREATE on public schema")

	// Connect as Alice and create a table with secret data
	aliceConnStr := fmt.Sprintf("host=localhost port=%d user=%s password=%s dbname=postgres sslmode=disable connect_timeout=5",
		cluster.PortConfig.Zones[0].MultigatewayPGPort, userAlice, password)
	aliceDB, err := sql.Open("postgres", aliceConnStr)
	require.NoError(t, err, "failed to open Alice's connection")
	defer aliceDB.Close()

	// Alice creates her secret table
	_, err = aliceDB.ExecContext(ctx, fmt.Sprintf("CREATE TABLE %s_secret (data TEXT)", userAlice))
	require.NoError(t, err, "Alice should be able to create her table")

	_, err = aliceDB.ExecContext(ctx, fmt.Sprintf("INSERT INTO %s_secret VALUES ('alice_secret_data')", userAlice))
	require.NoError(t, err, "Alice should be able to insert into her table")

	// Verify Alice can read her own data
	var aliceData string
	err = aliceDB.QueryRowContext(ctx, fmt.Sprintf("SELECT data FROM %s_secret", userAlice)).Scan(&aliceData)
	require.NoError(t, err, "Alice should be able to read her own table")
	assert.Equal(t, "alice_secret_data", aliceData)

	// Connect as Bob
	bobConnStr := fmt.Sprintf("host=localhost port=%d user=%s password=%s dbname=postgres sslmode=disable connect_timeout=5",
		cluster.PortConfig.Zones[0].MultigatewayPGPort, userBob, password)
	bobDB, err := sql.Open("postgres", bobConnStr)
	require.NoError(t, err, "failed to open Bob's connection")
	defer bobDB.Close()

	// Verify Bob's identity
	var currentUser string
	err = bobDB.QueryRowContext(ctx, "SELECT current_user").Scan(&currentUser)
	require.NoError(t, err, "Bob should be able to query current_user")
	assert.Equal(t, userBob, currentUser, "Bob should be connected as himself")

	// Bob should NOT be able to read Alice's table (permission denied)
	var bobReadAttempt string
	err = bobDB.QueryRowContext(ctx, fmt.Sprintf("SELECT data FROM %s_secret", userAlice)).Scan(&bobReadAttempt)
	require.Error(t, err, "Bob should NOT be able to read Alice's table")

	// Verify the error indicates permission denied (PostgreSQL returns this in Detail)
	var pqErr *pq.Error
	if errors.As(err, &pqErr) {
		assert.Contains(t, pqErr.Detail, "permission denied",
			"error detail should contain permission denied, got: %q", pqErr.Detail)
	}

	// Bob should NOT be able to modify Alice's table
	_, err = bobDB.ExecContext(ctx, fmt.Sprintf("INSERT INTO %s_secret VALUES ('bob_injection')", userAlice))
	require.Error(t, err, "Bob should NOT be able to insert into Alice's table")

	// Bob should NOT be able to drop Alice's table
	_, err = bobDB.ExecContext(ctx, fmt.Sprintf("DROP TABLE %s_secret", userAlice))
	require.Error(t, err, "Bob should NOT be able to drop Alice's table")

	// Verify Alice's data is still intact
	err = aliceDB.QueryRowContext(ctx, fmt.Sprintf("SELECT data FROM %s_secret", userAlice)).Scan(&aliceData)
	require.NoError(t, err, "Alice's table should still exist")
	assert.Equal(t, "alice_secret_data", aliceData, "Alice's data should be unchanged")
}

// TestMultiGateway_SetRole tests that SET ROLE and RESET ROLE work correctly
// within a user's role memberships. This verifies that PostgreSQL's native
// role system works correctly through the proxy.
func TestMultiGateway_SetRole(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping SET ROLE test in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("PostgreSQL binaries not found, skipping test")
	}

	// Setup full test cluster
	cluster := setupTestCluster(t)
	t.Cleanup(cluster.Cleanup)

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	// Connect to multipooler via gRPC to create test user and roles
	poolerAddr := fmt.Sprintf("localhost:%d", cluster.PortConfig.Zones[0].MultipoolerGRPCPort)
	poolerClient, err := NewMultiPoolerTestClient(poolerAddr)
	require.NoError(t, err, "failed to connect to multipooler via gRPC")
	defer poolerClient.Close()

	// Create test user, a role they're a member of, and a role they're NOT a member of
	timestamp := time.Now().UnixNano()
	testUser := fmt.Sprintf("roletest_%d", timestamp)
	grantedRole := fmt.Sprintf("granted_role_%d", timestamp)
	ungrantedRole := fmt.Sprintf("ungranted_role_%d", timestamp)
	testPassword := "roletest_password_123" //nolint:gosec // Test credentials

	// Create the roles first (use CREATE USER ... NOLOGIN which is equivalent to CREATE ROLE)
	_, err = poolerClient.ExecuteQuery(ctx, fmt.Sprintf("CREATE USER %s NOLOGIN", grantedRole), 0)
	require.NoError(t, err, "failed to create granted role")
	_, err = poolerClient.ExecuteQuery(ctx, fmt.Sprintf("CREATE USER %s NOLOGIN", ungrantedRole), 0)
	require.NoError(t, err, "failed to create ungranted role")

	// Create user and grant one role
	_, err = poolerClient.ExecuteQuery(ctx, fmt.Sprintf("CREATE USER %s WITH PASSWORD '%s'", testUser, testPassword), 0)
	require.NoError(t, err, "failed to create test user")
	_, err = poolerClient.ExecuteQuery(ctx, fmt.Sprintf("GRANT %s TO %s", grantedRole, testUser), 0)
	require.NoError(t, err, "failed to grant role to user")
	_, err = poolerClient.ExecuteQuery(ctx, fmt.Sprintf("GRANT CONNECT ON DATABASE postgres TO %s", testUser), 0)
	require.NoError(t, err, "failed to grant connect privilege")

	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		_, _ = poolerClient.ExecuteQuery(cleanupCtx, fmt.Sprintf("DROP USER IF EXISTS %s", testUser), 0)
		_, _ = poolerClient.ExecuteQuery(cleanupCtx, fmt.Sprintf("DROP ROLE IF EXISTS %s", grantedRole), 0)
		_, _ = poolerClient.ExecuteQuery(cleanupCtx, fmt.Sprintf("DROP ROLE IF EXISTS %s", ungrantedRole), 0)
	})

	// Connect as test user
	userConnStr := fmt.Sprintf("host=localhost port=%d user=%s password=%s dbname=postgres sslmode=disable connect_timeout=5",
		cluster.PortConfig.Zones[0].MultigatewayPGPort, testUser, testPassword)
	userDB, err := sql.Open("postgres", userConnStr)
	require.NoError(t, err, "failed to open user database connection")
	defer userDB.Close()

	// Force single connection to test role state on same connection
	userDB.SetMaxOpenConns(1)

	// Verify initial identity
	var currentUser, sessionUser string
	err = userDB.QueryRowContext(ctx, "SELECT current_user, session_user").Scan(&currentUser, &sessionUser)
	require.NoError(t, err, "initial identity query should succeed")
	assert.Equal(t, testUser, currentUser, "current_user should be test user initially")
	assert.Equal(t, testUser, sessionUser, "session_user should be test user")

	t.Run("SET ROLE to granted role succeeds", func(t *testing.T) {
		_, err := userDB.ExecContext(ctx, fmt.Sprintf("SET ROLE %s", grantedRole))
		require.NoError(t, err, "SET ROLE to granted role should succeed")

		// current_user changes, but session_user stays the same
		err = userDB.QueryRowContext(ctx, "SELECT current_user, session_user").Scan(&currentUser, &sessionUser)
		require.NoError(t, err, "identity query after SET ROLE should succeed")
		assert.Equal(t, grantedRole, currentUser, "current_user should be the granted role")
		assert.Equal(t, testUser, sessionUser, "session_user should still be test user")
	})

	t.Run("RESET ROLE returns to session user", func(t *testing.T) {
		_, err := userDB.ExecContext(ctx, "RESET ROLE")
		require.NoError(t, err, "RESET ROLE should succeed")

		err = userDB.QueryRowContext(ctx, "SELECT current_user, session_user").Scan(&currentUser, &sessionUser)
		require.NoError(t, err, "identity query after RESET ROLE should succeed")
		assert.Equal(t, testUser, currentUser, "current_user should return to test user after RESET ROLE")
		assert.Equal(t, testUser, sessionUser, "session_user should still be test user")
	})

	t.Run("SET ROLE to ungranted role fails", func(t *testing.T) {
		_, err := userDB.ExecContext(ctx, fmt.Sprintf("SET ROLE %s", ungrantedRole))
		require.Error(t, err, "SET ROLE to ungranted role should fail")

		// Verify current_user unchanged after failed SET ROLE
		err = userDB.QueryRowContext(ctx, "SELECT current_user").Scan(&currentUser)
		require.NoError(t, err, "identity query should succeed")
		assert.Equal(t, testUser, currentUser, "current_user should be unchanged after failed SET ROLE")
	})

	t.Run("SET ROLE NONE returns to session user", func(t *testing.T) {
		// First SET ROLE to granted role
		_, err := userDB.ExecContext(ctx, fmt.Sprintf("SET ROLE %s", grantedRole))
		require.NoError(t, err, "SET ROLE should succeed")

		// Then SET ROLE NONE
		_, err = userDB.ExecContext(ctx, "SET ROLE NONE")
		require.NoError(t, err, "SET ROLE NONE should succeed")

		err = userDB.QueryRowContext(ctx, "SELECT current_user").Scan(&currentUser)
		require.NoError(t, err, "identity query should succeed")
		assert.Equal(t, testUser, currentUser, "current_user should return to test user after SET ROLE NONE")
	})
}
