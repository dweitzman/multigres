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
	"time"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/services/multipooler/internal/executor"
)

// IsPrimary checks if the connected database is a primary (not in recovery)
func (e *Engine) IsPrimary(ctx context.Context) (bool, error) {
	inRecovery, err := e.IsInRecovery(ctx)
	return !inRecovery, err
}

// IsInRecovery checks if the connected database is in recovery mode (standby).
// Returns true if the database is a standby, false if it's a primary.
func (e *Engine) IsInRecovery(ctx context.Context) (bool, error) {
	queryCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()
	result, err := e.qs.Query(queryCtx, "SELECT pg_is_in_recovery()")
	if err != nil {
		return false, fmt.Errorf("failed to query pg_is_in_recovery: %w", err)
	}

	var inRecovery bool
	if err := executor.ScanSingleRow(result, &inRecovery); err != nil {
		return false, fmt.Errorf("failed to scan pg_is_in_recovery result: %w", err)
	}

	return inRecovery, nil
}

// SchemaExists checks if the multigres schema exists in the database
func (e *Engine) SchemaExists(ctx context.Context) (bool, error) {
	queryCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()
	sql := "SELECT EXISTS(SELECT 1 FROM information_schema.schemata WHERE schema_name = 'multigres')"
	result, err := e.qs.Query(queryCtx, sql)
	if err != nil {
		return false, mterrors.Wrap(err, "failed to check schema exists")
	}
	var exists bool
	if err := executor.ScanSingleRow(result, &exists); err != nil {
		return false, mterrors.Wrap(err, "failed to scan schema exists result")
	}
	return exists, nil
}
