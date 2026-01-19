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

package grpcfaultproxy

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"time"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/pgprotocol/protocol"
	"github.com/multigres/multigres/go/common/pgprotocol/server"
)

// StartPostgresProxy starts the PostgreSQL wire protocol proxy listener.
// Returns nil if PostgresAddr is empty (PostgreSQL proxy disabled).
//
// The PostgreSQL proxy provides fault injection for testing network partitions.
// It intercepts PostgreSQL connections at startup, extracts routing metadata,
// and either forwards the connection or applies a fault (latency, error, drop).
//
// NOTE: Fault injection is checked periodically (every 100ms) during active connections.
// When a fault rule matches, the connection is immediately terminated to simulate
// network partitions. This allows dynamic fault injection on established connections.
func (p *Proxy) StartPostgresProxy(ctx context.Context) error {
	if p.config.PostgresAddr == "" {
		p.logger.InfoContext(ctx, "PostgreSQL proxy disabled (no PostgresAddr configured)")
		return nil
	}

	listener, err := net.Listen("tcp", p.config.PostgresAddr)
	if err != nil {
		return mterrors.Wrapf(err, "failed to listen on %s", p.config.PostgresAddr)
	}

	p.postgresListener = listener

	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				if errors.Is(err, net.ErrClosed) {
					return
				}
				p.logger.Error("accept error", "error", err)
				continue
			}

			go p.handlePostgresConnection(ctx, conn)
		}
	}()

	p.logger.InfoContext(ctx, "PostgreSQL proxy listening", "addr", listener.Addr().String())
	return nil
}

// handlePostgresConnection proxies a single PostgreSQL connection.
//
// Connection flow:
// 1. Read startup message from client
// 2. Extract routing metadata (source from application_name, target from proxy_target)
// 3. Evaluate fault injection rules
// 4. If fault: apply fault (block, error, or delay) and close
// 5. If no fault: connect to backend, forward cleaned startup, then io.Copy bidirectionally
func (p *Proxy) handlePostgresConnection(ctx context.Context, clientConn net.Conn) {
	defer clientConn.Close()

	// Read startup message
	startupMsg, rawStartup, err := readStartupPacket(clientConn)
	if err != nil {
		p.logger.ErrorContext(ctx, "failed to read startup message",
			"remote_addr", clientConn.RemoteAddr(),
			"error", err)
		return
	}

	// Extract routing metadata
	source := startupMsg.Parameters["application_name"]
	target := extractProxyTarget(startupMsg.Parameters)

	if target == "" {
		p.logger.ErrorContext(ctx, "no proxy_target in connection parameters",
			"source", source,
			"params", startupMsg.Parameters,
			"remote_addr", clientConn.RemoteAddr())
		sendErrorResponse(clientConn, "FATAL", "proxy_target parameter required in options")
		return
	}

	p.logger.InfoContext(ctx, "PostgreSQL connection",
		"source", source,
		"target", target,
		"user", startupMsg.Parameters["user"],
		"database", startupMsg.Parameters["database"],
		"remote_addr", clientConn.RemoteAddr())

	// Evaluate fault injection rules
	req := RequestInfo{
		Source:   source,
		Target:   target,
		Method:   "postgres:startup",
		Protocol: "postgres",
	}

	if fault := p.engine.Evaluate(ctx, req); fault != nil {
		p.logger.InfoContext(ctx, "injecting fault on PostgreSQL connection",
			"source", source,
			"target", target,
			"fault_type", fault.Type,
			"remote_addr", clientConn.RemoteAddr())

		// Apply connection-level fault
		switch fault.Type {
		case "error":
			sendErrorResponse(clientConn, "FATAL", fault.ErrorMsg)
		case "drop":
			// Silent drop - just close without response
		case "latency":
			// Latency then drop (no ongoing connection)
			// For latency on an ongoing connection, we'd need per-message interception
		}
		return
	}

	// Connect to backend
	backendConn, err := net.Dial("tcp", target)
	if err != nil {
		p.logger.ErrorContext(ctx, "failed to connect to backend",
			"target", target,
			"error", err,
			"remote_addr", clientConn.RemoteAddr())
		sendErrorResponse(clientConn, "FATAL", fmt.Sprintf("backend connection failed: %v", err))
		return
	}
	defer backendConn.Close()

	// Forward cleaned startup message to backend
	cleanedStartup := removeProxyMetadata(startupMsg, rawStartup)
	if _, err := backendConn.Write(cleanedStartup); err != nil {
		p.logger.ErrorContext(ctx, "failed to write startup to backend",
			"error", err,
			"remote_addr", clientConn.RemoteAddr())
		return
	}

	// Bidirectional forwarding (opaque bytes)
	p.proxyPostgresMessages(ctx, clientConn, backendConn, source, target)
}

// proxyPostgresMessages forwards messages bidirectionally between client and backend.
// Periodically checks fault injection rules and closes the connection if a fault is active.
func (p *Proxy) proxyPostgresMessages(ctx context.Context, client, backend net.Conn, source, target string) {
	errChan := make(chan error, 2)
	doneChan := make(chan struct{})

	// Client → Backend
	go func() {
		_, err := io.Copy(backend, client)
		errChan <- err
	}()

	// Backend → Client
	go func() {
		_, err := io.Copy(client, backend)
		errChan <- err
	}()

	// Fault injection checker - periodically evaluate rules and kill connection if needed
	go func() {
		ticker := time.NewTicker(100 * time.Millisecond) // Check every 100ms
		defer ticker.Stop()

		for {
			select {
			case <-doneChan:
				return
			case <-ticker.C:
				// Check if fault injection should apply to this connection
				req := RequestInfo{
					Source:   source,
					Target:   target,
					Method:   "postgres:*",
					Protocol: "postgres",
				}

				if fault := p.engine.Evaluate(ctx, req); fault != nil {
					p.logger.Info("injecting fault on active PostgreSQL connection",
						"source", source,
						"target", target,
						"fault_type", fault.Type)

					// Close both connections to simulate network partition
					client.Close()
					backend.Close()
					return
				}
			}
		}
	}()

	// Wait for either direction to close
	err := <-errChan
	close(doneChan) // Stop the fault checker

	if err != nil && !isClosedNetworkError(err) {
		p.logger.DebugContext(ctx, "proxy connection closed",
			"source", source,
			"target", target,
			"error", err)
	}
}

// extractProxyTarget parses proxy_target from connection options.
// Example: options="-c proxy_target=primary:5432"
func extractProxyTarget(params map[string]string) string {
	opts := params["options"]
	if opts == "" {
		return ""
	}

	// Parse "-c proxy_target=host:port"
	parts := strings.Split(opts, "proxy_target=")
	if len(parts) < 2 {
		return ""
	}

	target := strings.TrimSpace(parts[1])
	// Remove any trailing options (space, quote, etc.)
	if idx := strings.IndexAny(target, " '\""); idx != -1 {
		target = target[:idx]
	}

	return target
}

// removeProxyMetadata removes proxy_target from options and reconstructs the startup packet.
func removeProxyMetadata(msg *server.StartupMessage, rawStartup []byte) []byte {
	// Strip proxy_target from options parameter
	cleaned := *msg
	cleaned.Parameters = make(map[string]string)

	for k, v := range msg.Parameters {
		if k == "options" {
			v = stripProxyTarget(v)
			if v != "" {
				cleaned.Parameters[k] = v
			}
		} else {
			cleaned.Parameters[k] = v
		}
	}

	// Rebuild startup packet
	w := server.NewMessageWriter()
	w.WriteUint32(cleaned.ProtocolVersion)

	for k, v := range cleaned.Parameters {
		w.WriteString(k)
		w.WriteString(v)
	}
	w.WriteByte(0) // Null terminator

	body := w.Bytes()
	length := uint32(4 + len(body))

	// Build full packet (length + body)
	result := make([]byte, 4+len(body))
	binary.BigEndian.PutUint32(result[0:4], length)
	copy(result[4:], body)

	return result
}

// stripProxyTarget removes proxy_target from options string.
func stripProxyTarget(opts string) string {
	parts := strings.Split(opts, "proxy_target=")
	if len(parts) < 2 {
		return opts
	}

	before := parts[0]
	after := parts[1]

	// Find end of proxy_target value
	endIdx := strings.IndexAny(after, " '\"")
	if endIdx != -1 {
		after = after[endIdx:]
	} else {
		after = ""
	}

	result := strings.TrimSpace(before + after)
	if result == "-c" || result == "" {
		return ""
	}
	return result
}

// readStartupPacket reads a PostgreSQL startup packet from a raw connection.
// Returns the parsed message and the raw bytes for potential re-transmission.
// Handles SSL negotiation (rejects with 'N' and reads the actual startup message).
func readStartupPacket(conn net.Conn) (*server.StartupMessage, []byte, error) {
	reader := bufio.NewReader(conn)

	// Read length (4 bytes)
	var lenBuf [4]byte
	if _, err := io.ReadFull(reader, lenBuf[:]); err != nil {
		return nil, nil, fmt.Errorf("failed to read startup length: %w", err)
	}

	length := binary.BigEndian.Uint32(lenBuf[:])
	if length < 4 {
		return nil, nil, fmt.Errorf("invalid startup message length: %d", length)
	}

	bodyLen := int(length - 4)
	if bodyLen > protocol.MaxStartupPacketLength {
		return nil, nil, fmt.Errorf("startup packet too large: %d bytes", bodyLen)
	}

	// Read body
	body := make([]byte, bodyLen)
	if _, err := io.ReadFull(reader, body); err != nil {
		return nil, nil, fmt.Errorf("failed to read startup body: %w", err)
	}

	// Parse protocol version
	msgReader := server.NewMessageReader(body)
	protocolVersion, err := msgReader.ReadUint32()
	if err != nil {
		return nil, nil, fmt.Errorf("failed to read protocol version: %w", err)
	}

	// Handle SSL request - reject and read actual startup message
	if protocolVersion == protocol.SSLRequestCode {
		// Send 'N' to decline SSL
		if _, err := conn.Write([]byte{'N'}); err != nil {
			return nil, nil, fmt.Errorf("failed to send SSL rejection: %w", err)
		}

		// Read the actual startup message after SSL rejection
		if _, err := io.ReadFull(reader, lenBuf[:]); err != nil {
			return nil, nil, fmt.Errorf("failed to read startup length after SSL: %w", err)
		}

		length = binary.BigEndian.Uint32(lenBuf[:])
		if length < 4 {
			return nil, nil, fmt.Errorf("invalid startup message length: %d", length)
		}

		bodyLen = int(length - 4)
		if bodyLen > protocol.MaxStartupPacketLength {
			return nil, nil, fmt.Errorf("startup packet too large: %d bytes", bodyLen)
		}

		body = make([]byte, bodyLen)
		if _, err := io.ReadFull(reader, body); err != nil {
			return nil, nil, fmt.Errorf("failed to read startup body after SSL: %w", err)
		}

		msgReader = server.NewMessageReader(body)
		protocolVersion, err = msgReader.ReadUint32()
		if err != nil {
			return nil, nil, fmt.Errorf("failed to read protocol version after SSL: %w", err)
		}
	}

	// Store raw packet for potential forwarding
	rawPacket := make([]byte, 4+bodyLen)
	copy(rawPacket[0:4], lenBuf[:])
	copy(rawPacket[4:], body)

	if protocolVersion != protocol.ProtocolVersionNumber {
		return nil, nil, fmt.Errorf("unsupported protocol version: %d", protocolVersion)
	}

	// Parse parameters
	params := make(map[string]string)
	for msgReader.Remaining() > 0 {
		key, err := msgReader.ReadString()
		if err != nil {
			return nil, nil, fmt.Errorf("failed to read parameter key: %w", err)
		}

		if key == "" {
			break
		}

		value, err := msgReader.ReadString()
		if err != nil {
			return nil, nil, fmt.Errorf("failed to read parameter value: %w", err)
		}

		params[key] = value
	}

	return &server.StartupMessage{
		ProtocolVersion: protocolVersion,
		Parameters:      params,
	}, rawPacket, nil
}

// sendErrorResponse sends a PostgreSQL ErrorResponse message.
func sendErrorResponse(conn net.Conn, severity, message string) {
	w := server.NewMessageWriter()

	w.WriteByte('S')
	w.WriteString(severity)
	w.WriteByte('M')
	w.WriteString(message)
	w.WriteByte(0)

	body := w.Bytes()
	length := uint32(4 + len(body))

	var buf [5]byte
	buf[0] = byte(protocol.MsgErrorResponse)
	binary.BigEndian.PutUint32(buf[1:], length)

	conn.Write(buf[:]) //nolint:errcheck
	conn.Write(body)   //nolint:errcheck
}
