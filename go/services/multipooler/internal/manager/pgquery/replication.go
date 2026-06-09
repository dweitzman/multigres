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
	"errors"
	"fmt"
	"time"

	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/services/multipooler/internal/executor"
	"github.com/multigres/multigres/go/tools/retry"

	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
)

// ============================================================================
// PostgreSQL Replication Operations
//
// This file contains methods for querying and configuring PostgreSQL
// replication settings. These are low-level operations that directly interact
// with the database.
// ============================================================================

// sqlGetReplicationStatus is the SQL query to retrieve all relevant replication
// status fields from pg_stat_wal_receiver. This is used by
// ReplicationStatus to get a comprehensive view of the replication state
// in one query. Note that some fields may be NULL depending on the state of the
// standby (e.g., if not in recovery or if no WAL has been received).
//
// Scalar subqueries are used for pg_stat_wal_receiver fields so that NULL is
// returned when the view is empty (e.g., on the primary or when the WAL
// receiver is not running), rather than returning zero rows.
const sqlGetReplicationStatus = `
SELECT	pg_last_wal_replay_lsn(),
		pg_last_wal_receive_lsn(),
		pg_is_wal_replay_paused(),
		pg_get_wal_replay_pause_state(),
		pg_last_xact_replay_timestamp(),
		current_setting('primary_conninfo'),
		(SELECT status FROM pg_stat_wal_receiver LIMIT 1),
		(SELECT last_msg_receipt_time FROM pg_stat_wal_receiver LIMIT 1),
		current_setting('wal_receiver_status_interval'),
		current_setting('wal_receiver_timeout')
`

// ReplicationStatus queries PostgreSQL for all replication status fields.
// This method handles NULL values properly for LSN fields that may be NULL
// when not in recovery mode or when no WAL has been received/replayed.
func (e *Engine) ReplicationStatus(ctx context.Context) (*multipoolermanagerdatapb.StandbyReplicationStatus, error) {
	queryCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()
	result, err := e.qs.Query(queryCtx, sqlGetReplicationStatus)
	if err != nil {
		return nil, mterrors.Wrap(err, "failed to query replication status")
	}

	var replayLsn *string
	var receiveLsn *string
	var isPaused bool
	var pauseState string
	var lastXactTime *string
	var primaryConnInfo string
	var walReceiverStatus *string
	var lastMsgReceiveTime *time.Time
	var walReceiverStatusInterval *string
	var walReceiverTimeout *string

	err = executor.ScanSingleRow(result, &replayLsn, &receiveLsn, &isPaused, &pauseState, &lastXactTime, &primaryConnInfo, &walReceiverStatus, &lastMsgReceiveTime, &walReceiverStatusInterval, &walReceiverTimeout)
	if err != nil {
		return nil, mterrors.Wrap(err, "failed to query replication status")
	}
	status := &multipoolermanagerdatapb.StandbyReplicationStatus{
		IsWalReplayPaused:   isPaused,
		WalReplayPauseState: pauseState,
	}
	if replayLsn != nil {
		status.LastReplayLsn = *replayLsn
	}
	if receiveLsn != nil {
		status.LastReceiveLsn = *receiveLsn
	}
	if lastXactTime != nil {
		status.LastXactReplayTimestamp = *lastXactTime
	}
	if walReceiverStatus != nil {
		status.WalReceiverStatus = *walReceiverStatus
	}

	if lastMsgReceiveTime != nil {
		status.LastMsgReceiveTime = timestamppb.New(*lastMsgReceiveTime)
	}

	if walReceiverStatusInterval != nil {
		// We can use ParseDuration here since PostgreSQL interval settings are
		// in a format compatible with Go durations (e.g., "10s", "500ms").
		if d, err := time.ParseDuration(*walReceiverStatusInterval); err == nil {
			status.WalReceiverStatusInterval = durationpb.New(d)
		}
	}

	if walReceiverTimeout != nil {
		// We can use ParseDuration here since PostgreSQL interval settings are
		// in a format compatible with Go durations (e.g., "10s", "500ms").
		if d, err := time.ParseDuration(*walReceiverTimeout); err == nil {
			status.WalReceiverTimeout = durationpb.New(d)
		}
	}

	// Parse primary_conninfo into structured format
	parsedConnInfo, err := ParseAndRedactPrimaryConnInfo(primaryConnInfo)
	if err != nil {
		return nil, mterrors.Wrap(err, "failed to parse primary_conninfo")
	}
	status.PrimaryConnInfo = parsedConnInfo

	return status, nil
}

// pollForReplicationStatus polls poll on a 10s budget until it reports done,
// returning the accompanying status. The first attempt runs immediately and
// subsequent attempts back off exponentially from a small base: a state that
// settles within milliseconds (the common case for these replication waits)
// returns promptly instead of paying a fixed poll interval on the failover
// critical path, while the backoff avoids hammering postgres when it takes
// longer.
//
// poll returns (status, done, err): done==true stops the loop and returns
// status; a non-nil err aborts immediately, except that an err observed after
// the context expired mid-poll is reported as the canonical timeout. On
// budget/context exhaustion, onTimeout (if non-nil) is invoked for diagnostics
// and a canonical DEADLINE_EXCEEDED (timeoutMsg) or wrapped cancellation
// (cancelMsg) is returned.
func (e *Engine) pollForReplicationStatus(
	ctx context.Context,
	timeoutMsg, cancelMsg string,
	onTimeout func(cause error),
	poll func(ctx context.Context) (status *multipoolermanagerdatapb.StandbyReplicationStatus, done bool, err error),
) (*multipoolermanagerdatapb.StandbyReplicationStatus, error) {
	waitCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	fail := func(cause error) (*multipoolermanagerdatapb.StandbyReplicationStatus, error) {
		if onTimeout != nil {
			onTimeout(cause)
		}
		if errors.Is(cause, context.DeadlineExceeded) {
			return nil, mterrors.New(mtrpcpb.Code_DEADLINE_EXCEEDED, timeoutMsg)
		}
		return nil, mterrors.Wrap(cause, cancelMsg)
	}

	r := retry.New(5*time.Millisecond, 500*time.Millisecond)
	for _, err := range r.Attempts(waitCtx) {
		if err != nil {
			return fail(err)
		}
		status, done, pErr := poll(waitCtx)
		if pErr != nil {
			// Surface the cleaner timeout cause if waitCtx expired mid-poll
			// instead of an opaque "pool ctx expired" wrapper.
			if waitErr := waitCtx.Err(); waitErr != nil {
				return fail(waitErr)
			}
			return nil, pErr
		}
		if done {
			return status, nil
		}
	}

	// Attempts only stops yielding when waitCtx is done, which is handled by the
	// err branch above; this is unreachable but required for the compiler.
	return fail(waitCtx.Err())
}

// WaitForReplicationPause polls until WAL replay is paused and returns the status at that moment.
// This ensures the LSN returned represents the exact point at which replication stopped.
func (e *Engine) WaitForReplicationPause(ctx context.Context) (*multipoolermanagerdatapb.StandbyReplicationStatus, error) {
	return e.pollForReplicationStatus(ctx,
		"timeout waiting for WAL replay to pause",
		"context cancelled while waiting for WAL replay to pause",
		func(cause error) {
			if errors.Is(cause, context.DeadlineExceeded) {
				e.logger.ErrorContext(ctx, "Timeout waiting for WAL replay to pause")
			} else {
				e.logger.ErrorContext(ctx, "Context cancelled while waiting for WAL replay to pause")
			}
		},
		func(waitCtx context.Context) (*multipoolermanagerdatapb.StandbyReplicationStatus, bool, error) {
			// pg_wal_replay_pause() is asynchronous, so poll until it takes effect.
			status, err := e.ReplicationStatus(waitCtx)
			if err != nil {
				e.logger.ErrorContext(ctx, "Failed to get replication status", "error", err)
				return nil, false, err
			}
			if !status.IsWalReplayPaused {
				return nil, false, nil
			}
			// Once paused, we have the exact state at the moment replication stopped.
			e.logger.InfoContext(ctx, "WAL replay is now paused",
				"last_replay_lsn", status.LastReplayLsn,
				"last_receive_lsn", status.LastReceiveLsn,
				"pause_state", status.WalReplayPauseState)
			return status, true, nil
		})
}

// WaitForReplayStabilize waits, best effort, for WAL replay to stop making
// observable progress. The intent is to approximate replay is idle given the WAL
// that is currently available to this standby.
//
// WARNING: This function is not perfect and has some theoretical limitations.
// See decision: 2026-02-12-wait-for-replay-stabilize-during-revoke.md for more context.
func (e *Engine) WaitForReplayStabilize(ctx context.Context) (*multipoolermanagerdatapb.StandbyReplicationStatus, error) {
	waitCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	// TODO: short-circuit when replay has provably caught up. On the recruit path
	// the receiver is stopped before this runs, so the received LSN is fixed; if
	// pg_last_wal_replay_lsn() == pg_last_wal_receive_lsn() we are maximally
	// applied and can return immediately instead of waiting out requiredStablePolls.
	// That needs ReplayState to also return the receive LSN. Until then the
	// stability heuristic below is a safe (if slightly slower) fallback.
	//
	// requiredStablePolls: number of consecutive polls showing the same replay_lsn
	// before we declare stability. At 10ms per tick, 3 polls = 30ms of stability.
	const requiredStablePolls = 3
	var prevReplayLsn string
	consecutive := 0

	for {
		select {
		case <-waitCtx.Done():
			if waitCtx.Err() == context.DeadlineExceeded {
				return nil, mterrors.New(mtrpcpb.Code_DEADLINE_EXCEEDED, "timeout waiting for WAL replay to stabilize")
			}
			return nil, mterrors.Wrap(waitCtx.Err(), "context cancelled while waiting for replay to stabilize")

		case <-ticker.C:
			replayLsn, isPaused, err := e.ReplayState(waitCtx)
			if err != nil {
				return nil, err
			}

			if isPaused {
				return nil, mterrors.New(mtrpcpb.Code_FAILED_PRECONDITION,
					"WAL replay is paused during revoke — unexpected state")
			}

			if replayLsn == prevReplayLsn {
				consecutive++
			} else {
				consecutive = 1
			}
			prevReplayLsn = replayLsn

			if consecutive >= requiredStablePolls {
				e.logger.InfoContext(ctx, "WAL replay stabilized (maximally applied)",
					"replay_lsn", replayLsn)

				status, err := e.ReplicationStatus(waitCtx)
				if err != nil {
					return nil, err
				}
				return status, nil
			}
		}
	}
}

// WaitForReceiverDisconnect waits for the WAL receiver to fully disconnect after clearing primary_conninfo.
// It polls pg_stat_wal_receiver to confirm the receiver has stopped.
func (e *Engine) WaitForReceiverDisconnect(ctx context.Context) (*multipoolermanagerdatapb.StandbyReplicationStatus, error) {
	// Track the latest poll snapshot so a timeout has diagnostic detail without
	// needing another query after the ctx has already expired.
	var (
		lastCount    int64 = -1 // -1 = not yet polled
		lastStatus   string
		lastConnInfo string
	)

	return e.pollForReplicationStatus(ctx,
		"timeout waiting for WAL receiver to disconnect",
		"context cancelled while waiting for WAL receiver to disconnect",
		func(cause error) {
			e.logger.ErrorContext(ctx, "WAL receiver did not disconnect",
				"cause", cause,
				"last_receiver_count", lastCount,
				"last_walreceiver_status", lastStatus,
				"last_primary_conninfo", lastConnInfo)
		},
		func(waitCtx context.Context) (*multipoolermanagerdatapb.StandbyReplicationStatus, bool, error) {
			// Pull the count, the walreceiver status, and the live primary_conninfo
			// in a single query so each poll is also a diagnostic snapshot.
			result, err := e.qs.Query(waitCtx, `SELECT
				(SELECT COUNT(*) FROM pg_stat_wal_receiver),
				coalesce((SELECT status FROM pg_stat_wal_receiver), ''),
				current_setting('primary_conninfo')`)
			if err != nil {
				e.logger.ErrorContext(ctx, "Failed to query pg_stat_wal_receiver", "error", err)
				return nil, false, mterrors.Wrap(err, "failed to query pg_stat_wal_receiver")
			}
			if err := executor.ScanSingleRow(result, &lastCount, &lastStatus, &lastConnInfo); err != nil {
				e.logger.ErrorContext(ctx, "Failed to scan pg_stat_wal_receiver row", "error", err)
				return nil, false, mterrors.Wrap(err, "failed to scan pg_stat_wal_receiver row")
			}

			// Done when either the walreceiver slot is gone, OR it's sitting
			// in WALRCV_WAITING with primary_conninfo empty. The latter is
			// safe because:
			//
			//   - We hold the action lock, so no other in-process path can
			//     write primary_conninfo during this wait.
			//   - WAITING → STREAMING requires the startup process to call
			//     RequestXLogStreaming, which only fires when primary_conninfo
			//     is non-empty.
			//   - We can't actively terminate a walreceiver from SQL —
			//     pg_terminate_backend only works on regular backends, not
			//     auxiliary processes like the walreceiver. The walreceiver
			//     only exits when its in-flight libpq call returns, which is
			//     bounded by connect_timeout (potentially tens of seconds).
			//     Treating WAITING+empty as done lets us proceed without
			//     waiting out that timeout.
			done := lastCount == 0 || (lastStatus == "waiting" && lastConnInfo == "")
			if !done {
				return nil, false, nil
			}
			e.logger.InfoContext(ctx, "WAL receiver has disconnected",
				"last_receiver_count", lastCount,
				"last_walreceiver_status", lastStatus)

			// Get the final replication status
			status, err := e.ReplicationStatus(waitCtx)
			if err != nil {
				e.logger.ErrorContext(ctx, "Failed to get replication status", "error", err)
				return nil, false, err
			}
			return status, true, nil
		})
}

// PauseReplication pauses replication based on the specified mode.
// If wait is true, it waits for the pause operation to complete before returning.
// Returns the replication status after pausing (if wait is true) or nil (if wait is false).
func (e *Engine) PauseReplication(ctx context.Context, mode multipoolermanagerdatapb.ReplicationPauseMode, wait bool) (*multipoolermanagerdatapb.StandbyReplicationStatus, error) {
	switch mode {
	case multipoolermanagerdatapb.ReplicationPauseMode_REPLICATION_PAUSE_MODE_REPLAY_ONLY:
		// Pause WAL replay on the standby
		e.logger.InfoContext(ctx, "Pausing WAL replay on standby")

		// Set tight timeout for the pause command itself (should be quick)
		execCtx, execCancel := context.WithTimeout(ctx, 500*time.Millisecond)
		defer execCancel()

		if _, err := e.qs.Query(execCtx, "SELECT pg_wal_replay_pause()"); err != nil {
			e.logger.ErrorContext(ctx, "Failed to pause WAL replay", "error", err)
			return nil, mterrors.Wrap(err, "failed to pause WAL replay")
		}

		if wait {
			// Wait for WAL replay to actually be paused
			// pg_wal_replay_pause() is asynchronous, so we need to wait for it to complete
			e.logger.InfoContext(ctx, "Waiting for WAL replay to complete pausing")
			status, err := e.WaitForReplicationPause(ctx)
			if err != nil {
				return nil, err
			}
			return status, nil
		}

		return nil, nil

	case multipoolermanagerdatapb.ReplicationPauseMode_REPLICATION_PAUSE_MODE_RECEIVER_ONLY:
		// Stop the WAL receiver by clearing primary_conninfo
		e.logger.InfoContext(ctx, "Stopping WAL receiver")

		if err := e.ResetPrimaryConnInfo(ctx); err != nil {
			return nil, err
		}

		if wait {
			// Wait for receiver to fully disconnect
			e.logger.InfoContext(ctx, "Waiting for WAL receiver to disconnect")
			status, err := e.WaitForReceiverDisconnect(ctx)
			if err != nil {
				return nil, err
			}
			return status, nil
		}

		return nil, nil

	case multipoolermanagerdatapb.ReplicationPauseMode_REPLICATION_PAUSE_MODE_REPLAY_AND_RECEIVER:
		// IMPORTANT: Must stop receiver BEFORE pausing replay
		// Reason: When replay is paused, the WAL receiver won't disconnect even if we clear primary_conninfo
		// So we must clear primary_conninfo while replay is still running
		e.logger.InfoContext(ctx, "Pausing both WAL replay and receiver")

		// First stop receiver (while replay is still running)
		if err := e.ResetPrimaryConnInfo(ctx); err != nil {
			return nil, err
		}

		// Wait for receiver to disconnect before pausing replay
		_, err := e.WaitForReceiverDisconnect(ctx)
		if err != nil {
			return nil, err
		}

		// Now that receiver is disconnected, pause replay
		execCtx, execCancel := context.WithTimeout(ctx, 500*time.Millisecond)
		defer execCancel()
		if _, err := e.qs.Query(execCtx, "SELECT pg_wal_replay_pause()"); err != nil {
			e.logger.ErrorContext(ctx, "Failed to pause WAL replay", "error", err)
			return nil, mterrors.Wrap(err, "failed to pause WAL replay")
		}

		if wait {
			// Wait for replay pause to complete
			e.logger.InfoContext(ctx, "Waiting for WAL replay to complete pausing")
			status, err := e.WaitForReplicationPause(ctx)
			if err != nil {
				return nil, err
			}
			return status, nil
		}

		return nil, nil

	default:
		return nil, mterrors.New(mtrpcpb.Code_INVALID_ARGUMENT,
			fmt.Sprintf("invalid replication pause mode: %d", mode))
	}
}

// ResumeWALReplay resumes WAL replay on a standby server
func (e *Engine) ResumeWALReplay(ctx context.Context) error {
	e.logger.InfoContext(ctx, "Resuming WAL replay")

	execCtx, execCancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer execCancel()

	if _, err := e.qs.Query(execCtx, "SELECT pg_wal_replay_resume()"); err != nil {
		e.logger.ErrorContext(ctx, "Failed to resume WAL replay", "error", err)
		return mterrors.Wrap(err, "failed to resume WAL replay")
	}

	return nil
}
