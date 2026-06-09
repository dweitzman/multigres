// Copyright 2026 Supabase, Inc.
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

package pgquery

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/parser/ast"
	"github.com/multigres/multigres/go/services/multipooler/internal/executor"

	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
)

// ReadPrimaryConnInfo returns the current primary_conninfo setting as a raw string.
// Returns an empty string if primary_conninfo is not set.
func (e *Engine) ReadPrimaryConnInfo(ctx context.Context) (string, error) {
	queryCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()
	result, err := e.qs.Query(queryCtx, "SELECT current_setting('primary_conninfo', true)")
	if err != nil {
		return "", mterrors.Wrap(err, "failed to read primary_conninfo")
	}
	var connInfo *string
	if err := executor.ScanSingleRow(result, &connInfo); err != nil {
		return "", mterrors.Wrap(err, "failed to scan primary_conninfo")
	}
	if connInfo == nil {
		return "", nil
	}
	return *connInfo, nil
}

// SetPrimaryConnInfo sets the primary_conninfo connection string
func (e *Engine) SetPrimaryConnInfo(ctx context.Context, connInfo string) error {
	e.logger.InfoContext(ctx, "Setting primary_conninfo", "conninfo", connInfo)

	execCtx, execCancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer execCancel()
	sql := "ALTER SYSTEM SET primary_conninfo = " + ast.QuoteStringLiteral(connInfo)
	if _, err := e.qs.Query(execCtx, sql); err != nil {
		e.logger.ErrorContext(ctx, "Failed to set primary_conninfo", "error", err)
		return mterrors.Wrap(err, "failed to set primary_conninfo")
	}

	return nil
}

// ResetPrimaryConnInfo clears primary_conninfo and reloads PostgreSQL configuration.
// This effectively disconnects the replica from the primary.
func (e *Engine) ResetPrimaryConnInfo(ctx context.Context) error {
	// Clear primary_conninfo using ALTER SYSTEM (should be quick)
	e.logger.InfoContext(ctx, "Clearing primary_conninfo")

	execCtx, execCancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer execCancel()
	if _, err := e.qs.Query(execCtx, "ALTER SYSTEM RESET primary_conninfo"); err != nil {
		e.logger.ErrorContext(ctx, "Failed to clear primary_conninfo", "error", err)
		return mterrors.Wrap(err, "failed to clear primary_conninfo")
	}

	return e.ReloadConfig(ctx)
}

// ParseAndRedactPrimaryConnInfo parses a PostgreSQL primary_conninfo connection string into structured fields
// Example input: "host=localhost port=5432 user=postgres application_name=cell_name"
// Returns a PrimaryConnInfo message with parsed fields, or an error if parsing fails
// Note: Passwords are redacted in the raw field for security
func ParseAndRedactPrimaryConnInfo(connInfoStr string) (*multipoolermanagerdatapb.PrimaryConnInfo, error) {
	connInfo := &multipoolermanagerdatapb.PrimaryConnInfo{}

	// Simple space-based parsing of key=value pairs
	parts := strings.Split(connInfoStr, " ")
	redactedParts := make([]string, 0, len(parts))

	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}

		kv := strings.SplitN(part, "=", 2)
		if len(kv) != 2 {
			// Not a key=value pair - parsing failed
			return nil, fmt.Errorf("invalid key=value format in primary_conninfo: %q", part)
		}

		key := strings.TrimSpace(kv[0])
		value := strings.TrimSpace(kv[1])

		if key == "" {
			return nil, fmt.Errorf("empty key in primary_conninfo: %q", part)
		}

		// Redact sensitive fields in the raw string
		if key == "password" {
			redactedParts = append(redactedParts, key+"=[REDACTED]")
		} else {
			redactedParts = append(redactedParts, part)
		}

		// Parse specific fields we care about
		switch key {
		case "host":
			connInfo.Host = value
		case "port":
			if port, err := strconv.ParseInt(value, 10, 32); err == nil {
				connInfo.Port = int32(port)
			}
		case "user":
			connInfo.User = value
		case "application_name":
			connInfo.ApplicationName = value
		}
	}

	// Set the redacted raw string
	connInfo.Raw = strings.Join(redactedParts, " ")

	return connInfo, nil
}
