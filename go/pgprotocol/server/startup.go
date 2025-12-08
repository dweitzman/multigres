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
	"errors"
	"fmt"

	"github.com/multigres/multigres/go/pgprotocol/auth"
	"github.com/multigres/multigres/go/pgprotocol/protocol"
)

// StartupMessage represents a parsed startup message from the client.
type StartupMessage struct {
	ProtocolVersion uint32
	Parameters      map[string]string
}

// handleStartup handles the initial connection startup phase.
// This includes SSL negotiation and processing the startup message.
// Returns an error if the startup fails.
func (c *Conn) handleStartup() error {
	// Read the first startup packet (could be SSL request, startup message, etc.)
	buf, err := c.readStartupPacket()
	if err != nil {
		return fmt.Errorf("failed to read startup packet: %w", err)
	}
	defer c.returnReadBuffer(buf)

	// Parse the protocol version/code from the packet.
	reader := NewMessageReader(buf)
	protocolCode, err := reader.ReadUint32()
	if err != nil {
		return fmt.Errorf("failed to read protocol code: %w", err)
	}

	// Handle special protocol codes.
	switch protocolCode {
	case protocol.SSLRequestCode:
		// Client is requesting SSL. We don't support SSL yet, so decline.
		return c.handleSSLRequest()

	case protocol.GSSENCRequestCode:
		// Client is requesting GSSAPI encryption. We don't support it, so decline.
		return c.handleGSSENCRequest()

	case protocol.CancelRequestCode:
		// This is a cancel request, not a regular connection startup.
		return c.handleCancelRequest(reader)

	case protocol.ProtocolVersionNumber:
		// This is a normal startup message with protocol version 3.0.
		return c.handleStartupMessage(protocolCode, reader)

	default:
		return fmt.Errorf("unsupported protocol version: %d", protocolCode)
	}
}

// handleSSLRequest handles an SSL negotiation request.
// We currently don't support SSL, so we send 'N' (no SSL) and then
// wait for the client to send the actual startup message.
func (c *Conn) handleSSLRequest() error {
	c.logger.Debug("client requested SSL, declining")

	// Send 'N' to decline SSL.
	writer := c.getWriter()
	if err := c.writeByte(writer, 'N'); err != nil {
		return fmt.Errorf("failed to send SSL response: %w", err)
	}

	// Flush the response immediately.
	if err := c.flush(); err != nil {
		return fmt.Errorf("failed to flush SSL response: %w", err)
	}

	// Now read the actual startup message.
	buf, err := c.readStartupPacket()
	if err != nil {
		return fmt.Errorf("failed to read startup message after SSL: %w", err)
	}
	defer c.returnReadBuffer(buf)

	reader := NewMessageReader(buf)
	protocolCode, err := reader.ReadUint32()
	if err != nil {
		return fmt.Errorf("failed to read protocol code: %w", err)
	}

	if protocolCode != protocol.ProtocolVersionNumber {
		return fmt.Errorf("expected protocol version %d, got %d", protocol.ProtocolVersionNumber, protocolCode)
	}

	return c.handleStartupMessage(protocolCode, reader)
}

// handleGSSENCRequest handles a GSSAPI encryption request.
// We don't support GSSAPI encryption, so we send 'N' (no GSSENC) and then
// wait for the client to send the actual startup message.
func (c *Conn) handleGSSENCRequest() error {
	c.logger.Debug("client requested GSSAPI encryption, declining")

	// Send 'N' to decline GSSENC.
	writer := c.getWriter()
	if err := c.writeByte(writer, 'N'); err != nil {
		return fmt.Errorf("failed to send GSSENC response: %w", err)
	}

	// Flush the response immediately.
	if err := c.flush(); err != nil {
		return fmt.Errorf("failed to flush GSSENC response: %w", err)
	}

	// Now read the actual startup message.
	buf, err := c.readStartupPacket()
	if err != nil {
		return fmt.Errorf("failed to read startup message after GSSENC: %w", err)
	}
	defer c.returnReadBuffer(buf)

	reader := NewMessageReader(buf)
	protocolCode, err := reader.ReadUint32()
	if err != nil {
		return fmt.Errorf("failed to read protocol code: %w", err)
	}

	if protocolCode != protocol.ProtocolVersionNumber {
		return fmt.Errorf("expected protocol version %d, got %d", protocol.ProtocolVersionNumber, protocolCode)
	}

	return c.handleStartupMessage(protocolCode, reader)
}

// handleCancelRequest handles a query cancellation request.
// This is sent by clients to cancel a running query on another connection.
func (c *Conn) handleCancelRequest(reader *MessageReader) error {
	// Read the process ID (connection ID).
	processID, err := reader.ReadUint32()
	if err != nil {
		return fmt.Errorf("failed to read process ID: %w", err)
	}

	// Read the secret key.
	secretKey, err := reader.ReadUint32()
	if err != nil {
		return fmt.Errorf("failed to read secret key: %w", err)
	}

	c.logger.Info("received cancel request", "process_id", processID, "secret_key", secretKey)

	// TODO(GuptaManan100): Implement query cancellation.
	// For now, we just close the connection as per protocol spec.
	// The client should not expect a response to a cancel request.
	return c.Close()
}

// handleStartupMessage processes a startup message and extracts connection parameters.
func (c *Conn) handleStartupMessage(protocolVersion uint32, reader *MessageReader) error {
	c.logger.Debug("parsing startup message", "protocol_version", protocolVersion)

	// Store the protocol version.
	c.protocolVersion = protocol.ProtocolVersion(protocolVersion)

	// Parse key-value pairs until we hit a null byte.
	for reader.Remaining() > 0 {
		// Read the key.
		key, err := reader.ReadString()
		if err != nil {
			return fmt.Errorf("failed to read parameter key: %w", err)
		}

		// Empty key means we've reached the end.
		if key == "" {
			break
		}

		// Read the value.
		value, err := reader.ReadString()
		if err != nil {
			return fmt.Errorf("failed to read parameter value for key %q: %w", key, err)
		}

		// Store the parameter.
		c.params[key] = value

		c.logger.Debug("startup parameter", "key", key, "value", value)
	}

	// Extract required parameters.
	c.user = c.params["user"]
	c.database = c.params["database"]

	// Default database to user if not specified.
	if c.database == "" {
		c.database = c.user
	}

	c.logger.Info("startup message parsed", "user", c.user, "database", c.database)

	// Now perform authentication.
	return c.authenticate()
}

// authenticate performs the authentication handshake with the client.
// Uses SCRAM-SHA-256 if a PasswordHashProvider is configured, otherwise trust authentication.
//
// Note: We only support SCRAM-SHA-256 for password authentication. MD5 and cleartext
// password methods are intentionally not supported due to security concerns.
// SCRAM-SHA-256 is the recommended method as of PostgreSQL 10+ and provides:
// - No plaintext password transmission
// - Mutual authentication (server proves it knows the password too)
// - Protection against replay attacks via nonces
func (c *Conn) authenticate() error {
	// Check if SCRAM authentication is configured.
	if c.listener.passwordHashProvider != nil {
		c.logger.Debug("authenticating client", "method", "scram-sha-256")
		if err := c.authenticateSCRAM(); err != nil {
			return err
		}
	} else {
		c.logger.Debug("authenticating client", "method", "trust")
		// Send AuthenticationOk (trust authentication).
		if err := c.sendAuthenticationOk(); err != nil {
			return fmt.Errorf("failed to send AuthenticationOk: %w", err)
		}
	}

	// Send BackendKeyData for query cancellation.
	if err := c.sendBackendKeyData(); err != nil {
		return fmt.Errorf("failed to send BackendKeyData: %w", err)
	}

	// Send initial ParameterStatus messages.
	if err := c.sendParameterStatuses(); err != nil {
		return fmt.Errorf("failed to send ParameterStatus messages: %w", err)
	}

	// Send ReadyForQuery to indicate we're ready to receive commands.
	if err := c.sendReadyForQuery(); err != nil {
		return fmt.Errorf("failed to send ReadyForQuery: %w", err)
	}

	c.logger.Info("authentication complete", "user", c.user)
	return nil
}

// authenticateSCRAM performs SCRAM-SHA-256 authentication.
// This implements the server side of the SASL SCRAM handshake.
func (c *Conn) authenticateSCRAM() error {
	// Create SCRAM authenticator with the database from startup params.
	authenticator := auth.NewScramAuthenticator(c.listener.passwordHashProvider, c.database)

	// Step 1: Send AuthenticationSASL with mechanism list.
	mechanisms := authenticator.StartAuthentication()
	if err := c.sendAuthenticationSASL(mechanisms); err != nil {
		return fmt.Errorf("failed to send AuthenticationSASL: %w", err)
	}
	if err := c.flush(); err != nil {
		return fmt.Errorf("failed to flush AuthenticationSASL: %w", err)
	}

	// Step 2: Read SASLInitialResponse (client-first-message).
	mechanism, clientFirstMessage, err := c.readSASLInitialResponse()
	if err != nil {
		return fmt.Errorf("failed to read SASLInitialResponse: %w", err)
	}

	// Verify mechanism is SCRAM-SHA-256.
	if mechanism != auth.ScramSHA256Mechanism {
		return fmt.Errorf("unsupported SASL mechanism: %s", mechanism)
	}

	// Step 3: Process client-first-message and generate server-first-message.
	serverFirstMessage, err := authenticator.HandleClientFirst(c.ctx, clientFirstMessage)
	if err != nil {
		if errors.Is(err, auth.ErrUserNotFound) {
			// User not found - send error response.
			return c.sendAuthError("28000", "password authentication failed for user %q", c.user)
		}
		return fmt.Errorf("failed to handle client-first-message: %w", err)
	}

	// Step 4: Send AuthenticationSASLContinue (server-first-message).
	if err := c.sendAuthenticationSASLContinue(serverFirstMessage); err != nil {
		return fmt.Errorf("failed to send AuthenticationSASLContinue: %w", err)
	}
	if err := c.flush(); err != nil {
		return fmt.Errorf("failed to flush AuthenticationSASLContinue: %w", err)
	}

	// Step 5: Read SASLResponse (client-final-message).
	clientFinalMessage, err := c.readSASLResponse()
	if err != nil {
		return fmt.Errorf("failed to read SASLResponse: %w", err)
	}

	// Step 6: Verify client proof and generate server signature.
	serverFinalMessage, err := authenticator.HandleClientFinal(clientFinalMessage)
	if err != nil {
		if errors.Is(err, auth.ErrAuthenticationFailed) {
			// Wrong password - send error response.
			return c.sendAuthError("28P01", "password authentication failed for user %q", c.user)
		}
		return fmt.Errorf("failed to handle client-final-message: %w", err)
	}

	// Step 7: Send AuthenticationSASLFinal (server signature).
	if err := c.sendAuthenticationSASLFinal(serverFinalMessage); err != nil {
		return fmt.Errorf("failed to send AuthenticationSASLFinal: %w", err)
	}

	// Step 8: Send AuthenticationOk.
	if err := c.sendAuthenticationOk(); err != nil {
		return fmt.Errorf("failed to send AuthenticationOk: %w", err)
	}
	if err := c.flush(); err != nil {
		return fmt.Errorf("failed to flush AuthenticationOk: %w", err)
	}

	// Extract SCRAM keys for passthrough authentication to backends.
	// These keys allow multipooler to authenticate to PostgreSQL as this user
	// without knowing the plaintext password.
	c.scramClientKey, c.scramServerKey = authenticator.ExtractedKeys()

	c.logger.Debug("SCRAM authentication successful", "user", c.user,
		"has_scram_keys", c.scramClientKey != nil)
	return nil
}

// sendAuthenticationSASL sends AuthenticationSASL message with mechanism list.
// Message format: int32(10) + list of null-terminated mechanism names + extra null.
func (c *Conn) sendAuthenticationSASL(mechanisms []string) error {
	w := NewMessageWriter()
	w.WriteInt32(protocol.AuthSASL)
	for _, mech := range mechanisms {
		w.WriteString(mech)
	}
	w.WriteByte(0) // Extra null terminator for list end
	return c.writeMessage(protocol.MsgAuthenticationRequest, w.Bytes())
}

// sendAuthenticationSASLContinue sends AuthenticationSASLContinue message.
// Message format: int32(11) + server-first-message bytes.
func (c *Conn) sendAuthenticationSASLContinue(serverFirstMessage string) error {
	w := NewMessageWriter()
	w.WriteInt32(protocol.AuthSASLContinue)
	w.WriteBytes([]byte(serverFirstMessage))
	return c.writeMessage(protocol.MsgAuthenticationRequest, w.Bytes())
}

// sendAuthenticationSASLFinal sends AuthenticationSASLFinal message.
// Message format: int32(12) + server-final-message bytes.
func (c *Conn) sendAuthenticationSASLFinal(serverFinalMessage string) error {
	w := NewMessageWriter()
	w.WriteInt32(protocol.AuthSASLFinal)
	w.WriteBytes([]byte(serverFinalMessage))
	return c.writeMessage(protocol.MsgAuthenticationRequest, w.Bytes())
}

// readSASLInitialResponse reads a SASLInitialResponse message from the client.
// Message format: 'p' + length + mechanism-name + int32(response-length) + response-data.
// Returns the mechanism name and the initial response (client-first-message).
func (c *Conn) readSASLInitialResponse() (string, string, error) {
	// Read message type.
	msgType, err := c.readMessageType()
	if err != nil {
		return "", "", fmt.Errorf("failed to read message type: %w", err)
	}
	if msgType != protocol.MsgPasswordMsg {
		return "", "", fmt.Errorf("expected password message ('p'), got %c", msgType)
	}

	// Read message body.
	bodyLen, err := c.readMessageLength()
	if err != nil {
		return "", "", fmt.Errorf("failed to read message length: %w", err)
	}
	buf, err := c.readMessageBody(bodyLen)
	if err != nil {
		return "", "", fmt.Errorf("failed to read message body: %w", err)
	}
	defer c.returnReadBuffer(buf)

	// Parse the message.
	reader := NewMessageReader(buf)

	// Read mechanism name (null-terminated string).
	mechanism, err := reader.ReadString()
	if err != nil {
		return "", "", fmt.Errorf("failed to read mechanism name: %w", err)
	}

	// Read response length.
	responseLen, err := reader.ReadInt32()
	if err != nil {
		return "", "", fmt.Errorf("failed to read response length: %w", err)
	}

	// Read response data (client-first-message).
	if responseLen < 0 {
		// -1 means no initial response, which is invalid for SCRAM.
		return "", "", fmt.Errorf("SCRAM requires initial response data")
	}
	responseData, err := reader.ReadBytes(int(responseLen))
	if err != nil {
		return "", "", fmt.Errorf("failed to read response data: %w", err)
	}

	return mechanism, string(responseData), nil
}

// readSASLResponse reads a SASLResponse message from the client.
// Message format: 'p' + length + response-data (client-final-message).
func (c *Conn) readSASLResponse() (string, error) {
	// Read message type.
	msgType, err := c.readMessageType()
	if err != nil {
		return "", fmt.Errorf("failed to read message type: %w", err)
	}
	if msgType != protocol.MsgPasswordMsg {
		return "", fmt.Errorf("expected password message ('p'), got %c", msgType)
	}

	// Read message body.
	bodyLen, err := c.readMessageLength()
	if err != nil {
		return "", fmt.Errorf("failed to read message length: %w", err)
	}
	buf, err := c.readMessageBody(bodyLen)
	if err != nil {
		return "", fmt.Errorf("failed to read message body: %w", err)
	}
	defer c.returnReadBuffer(buf)

	// The entire body is the client-final-message.
	return string(buf), nil
}

// sendAuthError sends an authentication error to the client and returns an error.
func (c *Conn) sendAuthError(sqlstate string, format string, args ...any) error {
	msg := fmt.Sprintf(format, args...)
	_ = c.writeErrorResponse("FATAL", sqlstate, msg, "", "")
	_ = c.flush()
	return fmt.Errorf("authentication failed: %s", msg)
}

// sendAuthenticationOk sends an AuthenticationOk message to the client.
func (c *Conn) sendAuthenticationOk() error {
	w := NewMessageWriter()
	w.WriteInt32(protocol.AuthOk)
	return c.writeMessage(protocol.MsgAuthenticationRequest, w.Bytes())
}

// sendBackendKeyData sends the BackendKeyData message.
// This contains the process ID (connection ID) and secret key for query cancellation.
func (c *Conn) sendBackendKeyData() error {
	w := NewMessageWriter()
	w.WriteUint32(c.connectionID)   // Process ID
	w.WriteUint32(c.backendKeyData) // Secret key
	return c.writeMessage(protocol.MsgBackendKeyData, w.Bytes())
}

// sendParameterStatuses sends initial ParameterStatus messages to the client.
// These inform the client about server settings.
func (c *Conn) sendParameterStatuses() error {
	// Send standard parameters that clients expect.
	parameters := map[string]string{
		"server_version":              "17.0 (multigres)", // Pretend to be PostgreSQL 17
		"server_encoding":             "UTF8",
		"client_encoding":             "UTF8",
		"DateStyle":                   "ISO, MDY",
		"TimeZone":                    "UTC",
		"integer_datetimes":           "on",
		"standard_conforming_strings": "on",
	}

	for key, value := range parameters {
		if err := c.sendParameterStatus(key, value); err != nil {
			return err
		}
	}

	return nil
}

// sendParameterStatus sends a single ParameterStatus message.
func (c *Conn) sendParameterStatus(name, value string) error {
	w := NewMessageWriter()
	w.WriteString(name)
	w.WriteString(value)
	return c.writeMessage(protocol.MsgParameterStatus, w.Bytes())
}

// sendReadyForQuery sends a ReadyForQuery message to indicate the server is ready.
func (c *Conn) sendReadyForQuery() error {
	w := NewMessageWriter()
	w.WriteByte(c.txnStatus)
	if err := c.writeMessage(protocol.MsgReadyForQuery, w.Bytes()); err != nil {
		return err
	}
	// Flush to ensure the client receives the message immediately.
	return c.flush()
}
