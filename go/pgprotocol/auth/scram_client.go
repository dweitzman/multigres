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

package auth

import (
	"crypto/rand"
	"encoding/base64"
	"fmt"
)

// SCRAMClient implements client-side SCRAM-SHA-256 authentication.
// It supports two modes:
// 1. Password mode: authenticate using a plaintext password
// 2. Passthrough mode: authenticate using pre-extracted ClientKey/ServerKey
//
// Passthrough mode enables SCRAM key passthrough: after a proxy verifies a client,
// it can extract the ClientKey and use it to authenticate to the backend database
// without knowing the plaintext password.
type SCRAMClient struct {
	// Username for authentication.
	username string

	// Mode: either password or keys.
	password  string // For normal mode
	clientKey []byte // For passthrough mode
	serverKey []byte // For passthrough mode (to verify server signature)

	// State from the SCRAM exchange.
	clientNonce                 string
	serverFirstMessage          string
	combinedNonce               string
	salt                        []byte
	iterations                  int
	clientFirstMessageBare      string
	clientFinalMessageWithProof string
	authMessage                 string
}

// NewSCRAMClientWithPassword creates a SCRAM client that authenticates with a password.
// This is the standard mode where the password is used to derive SCRAM keys.
func NewSCRAMClientWithPassword(username, password string) *SCRAMClient {
	return &SCRAMClient{
		username: username,
		password: password,
	}
}

// NewSCRAMClientWithKeys creates a SCRAM client that authenticates with extracted SCRAM keys.
// This enables SCRAM passthrough: a proxy can verify a client, extract the ClientKey,
// and use it to authenticate to PostgreSQL without the plaintext password.
//
// The clientKey and serverKey should be extracted from a previous SCRAM authentication
// using ExtractAndVerifyClientProof and the hash's ServerKey.
func NewSCRAMClientWithKeys(username string, clientKey, serverKey []byte) *SCRAMClient {
	return &SCRAMClient{
		username:  username,
		clientKey: clientKey,
		serverKey: serverKey,
	}
}

// ClientFirstMessage generates the client-first-message to send to the server.
// This starts the SCRAM authentication handshake.
// Returns the full message including the GS2 header.
func (c *SCRAMClient) ClientFirstMessage() (string, error) {
	// Generate random client nonce (18 bytes, base64 encoded for printability).
	nonceBytes := make([]byte, 18)
	if _, err := rand.Read(nonceBytes); err != nil {
		return "", fmt.Errorf("failed to generate client nonce: %w", err)
	}
	c.clientNonce = base64.StdEncoding.EncodeToString(nonceBytes)

	// Build client-first-message-bare: n=<username>,r=<nonce>
	c.clientFirstMessageBare = "n=" + encodeSaslName(c.username) + ",r=" + c.clientNonce

	// Full client-first-message with GS2 header.
	// "n,," means: no channel binding, no authorization identity
	return "n,," + c.clientFirstMessageBare, nil
}

// ProcessServerFirst processes the server-first-message and generates the client-final-message.
// The serverFirst parameter is the server's response to the client-first-message.
// Returns the client-final-message to send to the server.
func (c *SCRAMClient) ProcessServerFirst(serverFirst string) (string, error) {
	c.serverFirstMessage = serverFirst

	// Parse server-first-message.
	var err error
	c.combinedNonce, c.salt, c.iterations, err = ParseServerFirstMessage(serverFirst)
	if err != nil {
		return "", fmt.Errorf("failed to parse server-first-message: %w", err)
	}

	// Verify the combined nonce starts with our client nonce.
	if len(c.combinedNonce) < len(c.clientNonce) || c.combinedNonce[:len(c.clientNonce)] != c.clientNonce {
		return "", fmt.Errorf("server nonce does not start with client nonce (possible attack)")
	}

	// Build client-final-message-without-proof.
	// Channel binding data for "n,," is "biws" (base64 of "n,,").
	channelBinding := base64.StdEncoding.EncodeToString([]byte("n,,"))
	clientFinalWithoutProof := "c=" + channelBinding + ",r=" + c.combinedNonce

	// Build AuthMessage.
	c.authMessage = c.clientFirstMessageBare + "," + c.serverFirstMessage + "," + clientFinalWithoutProof

	// Compute ClientProof.
	var clientProof []byte
	if c.clientKey != nil {
		// Passthrough mode: use pre-extracted ClientKey.
		storedKey := ComputeStoredKey(c.clientKey)
		clientSignature := ComputeClientSignature(storedKey, c.authMessage)
		clientProof = ComputeClientProof(c.clientKey, clientSignature)
	} else {
		// Password mode: derive keys from password.
		saltedPassword := ComputeSaltedPassword(c.password, c.salt, c.iterations)
		clientKey := ComputeClientKey(saltedPassword)
		storedKey := ComputeStoredKey(clientKey)
		clientSignature := ComputeClientSignature(storedKey, c.authMessage)
		clientProof = ComputeClientProof(clientKey, clientSignature)

		// Store derived keys for server signature verification.
		c.clientKey = clientKey
		c.serverKey = ComputeServerKey(saltedPassword)
	}

	// Build client-final-message.
	proofB64 := base64.StdEncoding.EncodeToString(clientProof)
	c.clientFinalMessageWithProof = clientFinalWithoutProof + ",p=" + proofB64

	return c.clientFinalMessageWithProof, nil
}

// VerifyServerFinal verifies the server-final-message for mutual authentication.
// The serverFinal parameter is the server's response to the client-final-message.
// Returns nil if the server signature is valid, or an error if verification fails.
func (c *SCRAMClient) VerifyServerFinal(serverFinal string) error {
	// Parse server-final-message: v=<server_signature_b64>
	if len(serverFinal) < 2 || serverFinal[:2] != "v=" {
		return fmt.Errorf("invalid server-final-message: expected v=...")
	}

	serverSigB64 := serverFinal[2:]
	serverSig, err := base64.StdEncoding.DecodeString(serverSigB64)
	if err != nil {
		return fmt.Errorf("invalid server signature: %w", err)
	}

	// Compute expected server signature.
	if c.serverKey == nil {
		return fmt.Errorf("server key not available for verification")
	}
	expectedServerSig := ComputeServerSignature(c.serverKey, c.authMessage)

	// Compare signatures.
	if len(serverSig) != len(expectedServerSig) {
		return fmt.Errorf("server signature length mismatch")
	}
	for i := range serverSig {
		if serverSig[i] != expectedServerSig[i] {
			return fmt.Errorf("server signature mismatch (server may not know the password)")
		}
	}

	return nil
}
