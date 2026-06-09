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
	"log/slog"
	"time"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/services/multipooler/internal/executor"
	"github.com/multigres/multigres/go/tools/retry"

	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
)

// ReloadConfig reloads PostgreSQL configuration bound to this engine's query
// service and logger.
func (e *Engine) ReloadConfig(ctx context.Context) error {
	return ReloadConfig(ctx, e.logger, e.qs)
}

// ReloadConfig reloads PostgreSQL configuration to apply changes made via
// ALTER SYSTEM, and waits for postmaster to finish re-reading the config files
// before returning.
//
// pg_reload_conf() returns immediately after sending SIGHUP to postmaster, well
// before any of that work has happened. We use pg_conf_load_time() — the
// timestamp of postmaster's most recent successful config load — as the
// completion signal: once a follow-up query observes it advance past the
// pre-reload value, postmaster has re-read postgresql.auto.conf and signalled
// its child processes.
//
// Caveat: this guarantees postmaster has processed the reload, not that every
// child process has. Backends (the walreceiver, individual query backends)
// each pick up SIGHUP at their own pace — typically within milliseconds, but
// not synchronously. Callers that need to observe a child's reaction (e.g.
// polling pg_stat_wal_receiver for the walreceiver to disconnect after
// clearing primary_conninfo) should still poll, but they can do so knowing
// the new config is loaded server-side rather than racing with postmaster's
// signal handler.
func ReloadConfig(ctx context.Context, logger *slog.Logger, qs executor.InternalQueryService) error {
	if qs == nil {
		return errors.New("internal query service not available")
	}

	loadTimeCtx, loadTimeCancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer loadTimeCancel()
	result, err := qs.Query(loadTimeCtx, "SELECT pg_conf_load_time()")
	if err != nil {
		return mterrors.Wrap(err, "failed to read pg_conf_load_time before reload")
	}
	var loadTimeBefore string
	if err := executor.ScanSingleRow(result, &loadTimeBefore); err != nil {
		return mterrors.Wrap(err, "failed to scan pg_conf_load_time before reload")
	}

	logger.InfoContext(ctx, "Reloading PostgreSQL configuration")
	reloadCtx, reloadCancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer reloadCancel()
	if _, err := qs.Query(reloadCtx, "SELECT pg_reload_conf()"); err != nil {
		logger.ErrorContext(ctx, "Failed to reload configuration", "error", err)
		return mterrors.Wrap(err, "failed to reload PostgreSQL configuration")
	}

	// Poll pg_conf_load_time() until it advances. retry.New uses "do work, then
	// back off" semantics, so the backoff timer starts after the previous query
	// finishes — a slow query under load doesn't cause back-to-back hammering.
	waitCtx, waitCancel := context.WithTimeout(ctx, 5*time.Second)
	defer waitCancel()
	r := retry.New(1*time.Millisecond, 20*time.Millisecond)
	for _, attemptErr := range r.Attempts(waitCtx) {
		if attemptErr != nil {
			return mterrors.New(mtrpcpb.Code_DEADLINE_EXCEEDED,
				"timeout waiting for pg_conf_load_time to advance after pg_reload_conf")
		}
		queryCtx, queryCancel := context.WithTimeout(waitCtx, 500*time.Millisecond)
		result, err := qs.Query(queryCtx, "SELECT pg_conf_load_time()")
		queryCancel()
		if err != nil {
			return mterrors.Wrap(err, "failed to poll pg_conf_load_time after reload")
		}
		var loadTimeAfter string
		if err := executor.ScanSingleRow(result, &loadTimeAfter); err != nil {
			return mterrors.Wrap(err, "failed to scan pg_conf_load_time after reload")
		}
		if loadTimeAfter != loadTimeBefore {
			return nil
		}
	}
	// Unreachable: r.Attempts only exits via the ctx-cancelled branch above.
	return mterrors.New(mtrpcpb.Code_INTERNAL, "reload polling loop exited unexpectedly")
}
