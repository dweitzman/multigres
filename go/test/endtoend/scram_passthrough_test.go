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
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/pgprotocol/auth"
	"github.com/multigres/multigres/go/test/utils"
)

// TestSCRAMPassthrough tests that extracted SCRAM keys can authenticate to PostgreSQL.
// This validates the SCRAM passthrough mechanism:
// 1. Get SCRAM hash for a user from PostgreSQL
// 2. Simulate client auth to proxy (extract ClientKey from proof)
// 3. Use extracted keys to authenticate directly to PostgreSQL via raw TCP
//
// This is the key validation for Phase 4 per-user pools with SCRAM key passthrough.
func TestSCRAMPassthrough(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping SCRAM passthrough test in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("PostgreSQL binaries not found, skipping SCRAM passthrough test")
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

	// Create a test user with SCRAM-SHA-256 password
	testUser := fmt.Sprintf("scram_passthrough_%d", time.Now().UnixNano())
	testPassword := "passthrough_test_password"

	_, err = poolerClient.ExecuteQuery(ctx, fmt.Sprintf("CREATE USER %s WITH PASSWORD '%s'", testUser, testPassword), 0)
	require.NoError(t, err, "failed to create test user")

	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = poolerClient.ExecuteQuery(cleanupCtx, fmt.Sprintf("DROP USER IF EXISTS %s", testUser), 0)
	})

	// Get the SCRAM hash from pg_authid via multipooler
	result, err := poolerClient.ExecuteQuery(ctx, fmt.Sprintf("SELECT rolpassword FROM pg_authid WHERE rolname = '%s'", testUser), 0)
	require.NoError(t, err, "failed to get SCRAM hash")
	require.Len(t, result.Rows, 1, "should have exactly one row")
	require.Len(t, result.Rows[0].Values, 1, "should have exactly one column")

	scramHash := string(result.Rows[0].Values[0])
	require.True(t, auth.IsScramSHA256Hash(scramHash), "hash should be SCRAM-SHA-256 format")

	parsedHash, err := auth.ParseScramSHA256Hash(scramHash)
	require.NoError(t, err)

	// === Phase 1: Simulate client auth to proxy ===
	originalClient := auth.NewSCRAMClientWithPassword(testUser, testPassword)

	clientFirst, err := originalClient.ClientFirstMessage()
	require.NoError(t, err)

	parsed, err := auth.ParseClientFirstMessage(clientFirst)
	require.NoError(t, err)

	serverFirst, _, err := auth.GenerateServerFirstMessage(parsed.ClientNonce, parsedHash.Salt, parsedHash.Iterations)
	require.NoError(t, err)

	clientFinal, err := originalClient.ProcessServerFirst(serverFirst)
	require.NoError(t, err)

	clientFinalParsed, err := auth.ParseClientFinalMessage(clientFinal)
	require.NoError(t, err)

	clientFirstBare := clientFirst[3:] // Remove "n,,"
	authMessage := auth.BuildAuthMessage(clientFirstBare, serverFirst, clientFinalParsed.ClientFinalMessageWithoutProof)

	extractedClientKey, ok := auth.ExtractAndVerifyClientProof(parsedHash.StoredKey, authMessage, clientFinalParsed.Proof)
	require.True(t, ok, "proxy should verify original client")

	// === Phase 2: Use extracted keys to authenticate to PostgreSQL ===
	// Connect directly to PostgreSQL (bypassing multigateway)
	pgPort := cluster.PortConfig.Zones[0].PgctldPGPort
	conn, err := net.DialTimeout("tcp", fmt.Sprintf("localhost:%d", pgPort), 5*time.Second)
	require.NoError(t, err)
	defer conn.Close()

	// Send startup message
	err = sendStartupMessage(conn, testUser, "postgres")
	require.NoError(t, err)

	// Read authentication request
	authType, _, err := readAuthResponse(conn)
	require.NoError(t, err)
	require.Equal(t, int32(10), authType, "expected AuthenticationSASL (10)")

	// Create passthrough client with extracted keys
	passthroughClient := auth.NewSCRAMClientWithKeys(testUser, extractedClientKey, parsedHash.ServerKey)

	// Send SASL initial response
	clientFirst2, err := passthroughClient.ClientFirstMessage()
	require.NoError(t, err)

	err = sendSASLInitialResponse(conn, "SCRAM-SHA-256", clientFirst2)
	require.NoError(t, err)

	// Read SASL continue
	var authData []byte
	authType, authData, err = readAuthResponse(conn)
	require.NoError(t, err)
	require.Equal(t, int32(11), authType, "expected AuthenticationSASLContinue (11)")

	serverFirst2 := string(authData)

	// Send SASL response
	clientFinal2, err := passthroughClient.ProcessServerFirst(serverFirst2)
	require.NoError(t, err)

	err = sendSASLResponse(conn, clientFinal2)
	require.NoError(t, err)

	// Read SASL final or OK
	authType, authData, err = readAuthResponse(conn)
	require.NoError(t, err)

	if authType == 12 { // AuthenticationSASLFinal
		serverFinal2 := string(authData)
		err = passthroughClient.VerifyServerFinal(serverFinal2)
		require.NoError(t, err, "server signature should be valid")

		authType, _, err = readAuthResponse(conn)
		require.NoError(t, err)
	}

	assert.Equal(t, int32(0), authType, "expected AuthenticationOK (0)")
	t.Log("SCRAM passthrough authentication succeeded!")
}

// Wire protocol helpers

func sendStartupMessage(conn net.Conn, user, database string) error {
	var buf bytes.Buffer
	_ = binary.Write(&buf, binary.BigEndian, uint32(196608)) // Protocol version 3.0; bytes.Buffer never fails

	buf.WriteString("user")
	buf.WriteByte(0)
	buf.WriteString(user)
	buf.WriteByte(0)
	buf.WriteString("database")
	buf.WriteByte(0)
	buf.WriteString(database)
	buf.WriteByte(0)
	buf.WriteByte(0)

	msgLen := uint32(buf.Len() + 4)
	lenBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(lenBuf, msgLen)

	if _, err := conn.Write(lenBuf); err != nil {
		return err
	}
	_, err := conn.Write(buf.Bytes())
	return err
}

func readAuthResponse(conn net.Conn) (int32, []byte, error) {
	typeBuf := make([]byte, 1)
	if _, err := io.ReadFull(conn, typeBuf); err != nil {
		return 0, nil, err
	}

	lenBuf := make([]byte, 4)
	if _, err := io.ReadFull(conn, lenBuf); err != nil {
		return 0, nil, err
	}
	length := binary.BigEndian.Uint32(lenBuf)

	payload := make([]byte, length-4)
	if len(payload) > 0 {
		if _, err := io.ReadFull(conn, payload); err != nil {
			return 0, nil, err
		}
	}

	msgType := typeBuf[0]
	if msgType == 'E' {
		return 0, nil, fmt.Errorf("PostgreSQL error: %s", string(payload))
	}
	if msgType != 'R' {
		return 0, nil, fmt.Errorf("expected Auth ('R'), got '%c'", msgType)
	}
	if len(payload) < 4 {
		return 0, nil, fmt.Errorf("payload too short")
	}

	authType := int32(binary.BigEndian.Uint32(payload[:4]))
	return authType, payload[4:], nil
}

func sendSASLInitialResponse(conn net.Conn, mechanism, response string) error {
	var buf bytes.Buffer
	buf.WriteByte('p')

	var payload bytes.Buffer
	payload.WriteString(mechanism)
	payload.WriteByte(0)
	_ = binary.Write(&payload, binary.BigEndian, int32(len(response))) // bytes.Buffer never fails
	payload.WriteString(response)

	_ = binary.Write(&buf, binary.BigEndian, int32(payload.Len()+4)) // bytes.Buffer never fails
	buf.Write(payload.Bytes())

	_, err := conn.Write(buf.Bytes())
	return err
}

func sendSASLResponse(conn net.Conn, response string) error {
	var buf bytes.Buffer
	buf.WriteByte('p')
	_ = binary.Write(&buf, binary.BigEndian, int32(len(response)+4)) // bytes.Buffer never fails
	buf.WriteString(response)
	_, err := conn.Write(buf.Bytes())
	return err
}
