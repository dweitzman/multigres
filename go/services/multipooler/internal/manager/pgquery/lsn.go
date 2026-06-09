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

	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
)

// PrimaryLSN gets the current WAL write location (primary only)
func (e *Engine) PrimaryLSN(ctx context.Context) (string, error) {
	queryCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()
	result, err := e.qs.Query(queryCtx, "SELECT pg_current_wal_lsn()::text")
	if err != nil {
		return "", mterrors.Wrap(err, "failed to get current WAL LSN")
	}
	var lsn string
	if err := executor.ScanSingleRow(result, &lsn); err != nil {
		return "", mterrors.Wrap(err, "failed to scan WAL LSN result")
	}
	return lsn, nil
}

// StandbyReplayLSN gets the last replayed WAL location (standby only)
func (e *Engine) StandbyReplayLSN(ctx context.Context) (string, error) {
	queryCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()
	result, err := e.qs.Query(queryCtx, "SELECT pg_last_wal_replay_lsn()::text")
	if err != nil {
		return "", mterrors.Wrap(err, "failed to get replay LSN")
	}
	var lsn string
	if err := executor.ScanSingleRow(result, &lsn); err != nil {
		return "", mterrors.Wrap(err, "failed to scan replay LSN result")
	}
	return lsn, nil
}

// CheckLSNReached checks if the standby has replayed up to or past the target LSN
func (e *Engine) CheckLSNReached(ctx context.Context, targetLsn string) (bool, error) {
	queryCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()
	result, err := e.qs.QueryArgs(queryCtx, "SELECT pg_last_wal_replay_lsn() >= $1::pg_lsn", targetLsn)
	if err != nil {
		return false, mterrors.Wrap(err, "failed to check if replay LSN reached target")
	}
	var reachedTarget bool
	if err := executor.ScanSingleRow(result, &reachedTarget); err != nil {
		return false, mterrors.Wrap(err, "failed to scan LSN comparison result")
	}
	return reachedTarget, nil
}

// ReplayState returns the current replay LSN and pause state.
// Returns FAILED_PRECONDITION if the server is not in recovery (replay LSN is NULL).
func (e *Engine) ReplayState(ctx context.Context) (replayLsn string, isPaused bool, err error) {
	queryCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()
	result, err := e.qs.Query(queryCtx, "SELECT pg_last_wal_replay_lsn(), pg_is_wal_replay_paused()")
	if err != nil {
		return "", false, mterrors.Wrap(err, "failed to query replay state")
	}

	var lsn *string
	if err := executor.ScanSingleRow(result, &lsn, &isPaused); err != nil {
		return "", false, mterrors.Wrap(err, "failed to scan replay state")
	}
	if lsn == nil {
		return "", false, mterrors.New(mtrpcpb.Code_FAILED_PRECONDITION,
			"pg_last_wal_replay_lsn is NULL (not in recovery) — unexpected during revoke")
	}
	return *lsn, isPaused, nil
}

// ValidateExpectedLSN validates that the current replay LSN matches the expected LSN
func (e *Engine) ValidateExpectedLSN(ctx context.Context, expectedLSN string) error {
	if expectedLSN == "" {
		return nil // No validation requested
	}

	queryCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()
	sql := "SELECT pg_last_wal_replay_lsn()::text, pg_is_wal_replay_paused()"
	result, err := e.qs.Query(queryCtx, sql)
	if err != nil {
		e.logger.ErrorContext(ctx, "Failed to get current replay LSN and pause state", "error", err)
		return mterrors.Wrap(err, "failed to get current replay LSN and pause state")
	}

	var currentLSN string
	var isPaused bool
	err = executor.ScanSingleRow(result, &currentLSN, &isPaused)
	if err != nil {
		return mterrors.Wrap(err, "failed to get current replay LSN")
	}

	// Best practice: WAL replay should be paused before promotion
	// The coordinator should have called StopReplication during Discovery stage
	if !isPaused {
		e.logger.WarnContext(ctx, "WAL replay is not paused before promotion - coordinator may have skipped Discovery stage",
			"current_lsn", currentLSN,
			"expected_lsn", expectedLSN)
		// Note: We don't fail here as this is a soft check, but it indicates
		// a potential issue in the consensus flow
	}

	if currentLSN != expectedLSN {
		e.logger.ErrorContext(ctx, "LSN mismatch - node does not have expected durable state",
			"expected_lsn", expectedLSN,
			"current_lsn", currentLSN)
		return mterrors.New(mtrpcpb.Code_FAILED_PRECONDITION,
			fmt.Sprintf("LSN mismatch: expected %s, current %s. "+
				"This indicates an error in an earlier consensus stage.",
				expectedLSN, currentLSN))
	}

	e.logger.InfoContext(ctx, "LSN validation passed",
		"lsn", currentLSN,
		"wal_replay_paused", isPaused)
	return nil
}
