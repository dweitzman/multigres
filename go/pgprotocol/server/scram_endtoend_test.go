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

package server

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"

	_ "github.com/lib/pq" // PostgreSQL driver
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/pb/query"
	"github.com/multigres/multigres/go/pgprotocol/auth"
)

// selectOneHandler responds to "SELECT 1" queries for end-to-end testing.
type selectOneHandler struct{}

func (h *selectOneHandler) HandleQuery(ctx context.Context, conn *Conn, queryStr string, callback func(ctx context.Context, result *query.QueryResult) error) error {
	// Return a simple result for SELECT 1.
	result := &query.QueryResult{
		Fields: []*query.Field{
			{
				Name:         "?column?",
				Type:         "int4",
				DataTypeOid:  23, // INT4OID
				DataTypeSize: 4,
				TypeModifier: -1,
				Format:       0, // text
			},
		},
		Rows: []*query.Row{
			{Values: [][]byte{[]byte("1")}},
		},
		CommandTag: "SELECT 1",
	}
	return callback(ctx, result)
}

func (h *selectOneHandler) HandleParse(ctx context.Context, conn *Conn, name, queryStr string, paramTypes []uint32) error {
	return nil
}

func (h *selectOneHandler) HandleBind(ctx context.Context, conn *Conn, portalName, stmtName string, params [][]byte, paramFormats, resultFormats []int16) error {
	return nil
}

func (h *selectOneHandler) HandleExecute(ctx context.Context, conn *Conn, portalName string, maxRows int32, callback func(ctx context.Context, result *query.QueryResult) error) error {
	return nil
}

func (h *selectOneHandler) HandleDescribe(ctx context.Context, conn *Conn, typ byte, name string) (*query.StatementDescription, error) {
	return nil, nil
}

func (h *selectOneHandler) HandleClose(ctx context.Context, conn *Conn, typ byte, name string) error {
	return nil
}

func (h *selectOneHandler) HandleSync(ctx context.Context, conn *Conn) error {
	return nil
}

// e2eTestLogger creates a logger for end-to-end testing.
func e2eTestLogger(t *testing.T) *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))
}

// e2ePasswordHashProvider implements auth.PasswordHashProvider for end-to-end testing.
type e2ePasswordHashProvider struct {
	hashes map[string]*auth.ScramHash
}

func (p *e2ePasswordHashProvider) GetPasswordHash(_ context.Context, username string) (*auth.ScramHash, error) {
	hash, ok := p.hashes[username]
	if !ok {
		return nil, auth.ErrUserNotFound
	}
	return hash, nil
}

// createE2EScramHash creates a ScramHash for testing.
func createE2EScramHash(password string, salt []byte, iterations int) *auth.ScramHash {
	saltedPassword := auth.ComputeSaltedPassword(password, salt, iterations)
	clientKey := auth.ComputeClientKey(saltedPassword)
	storedKey := auth.ComputeStoredKey(clientKey)
	serverKey := auth.ComputeServerKey(saltedPassword)
	return &auth.ScramHash{
		Iterations: iterations,
		Salt:       salt,
		StoredKey:  storedKey,
		ServerKey:  serverKey,
	}
}

// TestSCRAMEndToEnd tests the full SCRAM authentication flow using lib/pq driver.
func TestSCRAMEndToEnd(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping end-to-end test in short mode")
	}

	testPassword := "testpassword123"
	testSalt := []byte("randomsalt123456") // 16 bytes
	testIterations := 4096

	// Create password hash provider.
	provider := &e2ePasswordHashProvider{
		hashes: map[string]*auth.ScramHash{
			"testuser": createE2EScramHash(testPassword, testSalt, testIterations),
		},
	}

	// Create listener with SCRAM authentication.
	listener, err := NewListener(ListenerConfig{
		Address:              "localhost:0", // Random available port
		Handler:              &selectOneHandler{},
		PasswordHashProvider: provider,
		Logger:               e2eTestLogger(t),
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		listener.Close()
	})

	// Start serving in background.
	go func() {
		_ = listener.Serve()
	}()

	// Get the actual port.
	addr := listener.Addr().String()

	// Connect using lib/pq with SCRAM-SHA-256.
	connStr := fmt.Sprintf(
		"host=localhost port=%s user=testuser password=%s dbname=testdb sslmode=disable connect_timeout=5",
		addr[strings.LastIndex(addr, ":")+1:],
		testPassword,
	)
	db, err := sql.Open("postgres", connStr)
	require.NoError(t, err)
	defer db.Close()

	// Set connection timeout.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Execute query.
	rows, err := db.QueryContext(ctx, "SELECT 1")
	require.NoError(t, err, "SCRAM authentication should succeed")
	defer rows.Close()

	// Verify result.
	require.True(t, rows.Next(), "should have a row")
	var result int
	err = rows.Scan(&result)
	require.NoError(t, err)
	assert.Equal(t, 1, result)
}

// TestSCRAMEndToEndWrongPassword tests that authentication fails with wrong password.
func TestSCRAMEndToEndWrongPassword(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping end-to-end test in short mode")
	}

	testPassword := "correctpassword"
	testSalt := []byte("randomsalt123456")
	testIterations := 4096

	provider := &e2ePasswordHashProvider{
		hashes: map[string]*auth.ScramHash{
			"testuser": createE2EScramHash(testPassword, testSalt, testIterations),
		},
	}

	listener, err := NewListener(ListenerConfig{
		Address:              "localhost:0",
		Handler:              &selectOneHandler{},
		PasswordHashProvider: provider,
		Logger:               e2eTestLogger(t),
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		listener.Close()
	})

	go func() {
		_ = listener.Serve()
	}()

	addr := listener.Addr().String()

	// Try to connect with wrong password.
	connStr := fmt.Sprintf(
		"host=localhost port=%s user=testuser password=wrongpassword dbname=testdb sslmode=disable connect_timeout=5",
		addr[strings.LastIndex(addr, ":")+1:],
	)
	db, err := sql.Open("postgres", connStr)
	require.NoError(t, err)
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Connection should fail.
	err = db.PingContext(ctx)
	require.Error(t, err, "authentication should fail with wrong password")
	assert.Contains(t, err.Error(), "authentication failed")
}

// TestSCRAMEndToEndUnknownUser tests that authentication fails for unknown users.
func TestSCRAMEndToEndUnknownUser(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping end-to-end test in short mode")
	}

	testPassword := "testpassword"
	testSalt := []byte("randomsalt123456")
	testIterations := 4096

	provider := &e2ePasswordHashProvider{
		hashes: map[string]*auth.ScramHash{
			"knownuser": createE2EScramHash(testPassword, testSalt, testIterations),
		},
	}

	listener, err := NewListener(ListenerConfig{
		Address:              "localhost:0",
		Handler:              &selectOneHandler{},
		PasswordHashProvider: provider,
		Logger:               e2eTestLogger(t),
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		listener.Close()
	})

	go func() {
		_ = listener.Serve()
	}()

	addr := listener.Addr().String()

	// Try to connect with unknown user.
	connStr := fmt.Sprintf(
		"host=localhost port=%s user=unknownuser password=%s dbname=testdb sslmode=disable connect_timeout=5",
		addr[strings.LastIndex(addr, ":")+1:],
		testPassword,
	)
	db, err := sql.Open("postgres", connStr)
	require.NoError(t, err)
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Connection should fail.
	err = db.PingContext(ctx)
	require.Error(t, err, "authentication should fail for unknown user")
	assert.Contains(t, err.Error(), "authentication failed")
}
