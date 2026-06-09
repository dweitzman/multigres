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
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/services/multipooler/internal/executor/mock"

	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
)

// newTestEngine creates a test Engine backed by a mock query service.
func newTestEngine() (*Engine, *mock.QueryService) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	mockQueryService := mock.NewQueryService()
	return NewEngine(logger, mockQueryService), mockQueryService
}

// expectReloadConfig sets up the mock query expectations for one successful call
// to Engine.ReloadConfig: read pg_conf_load_time (pre), run pg_reload_conf, then
// read pg_conf_load_time (post) returning a different value so the wait loop
// exits on the first poll.
func expectReloadConfig(m *mock.QueryService) {
	m.AddQueryPatternOnce("SELECT pg_conf_load_time",
		mock.MakeQueryResult([]string{"pg_conf_load_time"}, [][]any{{"2026-01-01 00:00:00+00"}}))
	m.AddQueryPatternOnce("SELECT pg_reload_conf", mock.MakeQueryResult(nil, nil))
	m.AddQueryPatternOnce("SELECT pg_conf_load_time",
		mock.MakeQueryResult([]string{"pg_conf_load_time"}, [][]any{{"2026-01-01 00:00:01+00"}}))
}

// expectReloadConfigFailure sets up the mock query expectations for a call to
// Engine.ReloadConfig where pg_reload_conf itself fails. The wait loop is never
// entered.
func expectReloadConfigFailure(m *mock.QueryService, reloadErr error) {
	m.AddQueryPatternOnce("SELECT pg_conf_load_time",
		mock.MakeQueryResult([]string{"pg_conf_load_time"}, [][]any{{"2026-01-01 00:00:00+00"}}))
	m.AddQueryPatternOnceWithError("SELECT pg_reload_conf", reloadErr)
}

func TestIsInRecovery(t *testing.T) {
	tests := []struct {
		name         string
		setupMock    func(*mock.QueryService)
		expectError  bool
		expectResult bool
	}{
		{
			name: "primary server - not in recovery",
			setupMock: func(m *mock.QueryService) {
				m.AddQueryPatternOnce("SELECT pg_is_in_recovery", mock.MakeQueryResult([]string{"pg_is_in_recovery"}, [][]any{{"f"}}))
			},
			expectError:  false,
			expectResult: false,
		},
		{
			name: "standby server - in recovery",
			setupMock: func(m *mock.QueryService) {
				m.AddQueryPatternOnce("SELECT pg_is_in_recovery", mock.MakeQueryResult([]string{"pg_is_in_recovery"}, [][]any{{"t"}}))
			},
			expectError:  false,
			expectResult: true,
		},
		{
			name: "query error",
			setupMock: func(m *mock.QueryService) {
				m.AddQueryPatternOnceWithError("SELECT pg_is_in_recovery", errors.New("connection done"))
			},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e, mockQueryService := newTestEngine()

			tt.setupMock(mockQueryService)

			ctx := context.Background()
			result, err := e.IsInRecovery(ctx)

			if tt.expectError {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expectResult, result)
			}
			assert.NoError(t, mockQueryService.ExpectationsWereMet())
		})
	}
}

func TestGetPrimaryLSN(t *testing.T) {
	tests := []struct {
		name        string
		setupMock   func(*mock.QueryService)
		expectError bool
		expectedLSN string
	}{
		{
			name: "successful query",
			setupMock: func(m *mock.QueryService) {
				m.AddQueryPatternOnce("SELECT pg_current_wal_lsn", mock.MakeQueryResult([]string{"pg_current_wal_lsn"}, [][]any{{"0/3000000"}}))
			},
			expectError: false,
			expectedLSN: "0/3000000",
		},
		{
			name: "different LSN format",
			setupMock: func(m *mock.QueryService) {
				m.AddQueryPatternOnce("SELECT pg_current_wal_lsn", mock.MakeQueryResult([]string{"pg_current_wal_lsn"}, [][]any{{"1/ABCD1234"}}))
			},
			expectError: false,
			expectedLSN: "1/ABCD1234",
		},
		{
			name: "query error",
			setupMock: func(m *mock.QueryService) {
				m.AddQueryPatternOnceWithError("SELECT pg_current_wal_lsn", errors.New("connection done"))
			},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e, mockQueryService := newTestEngine()

			tt.setupMock(mockQueryService)

			ctx := context.Background()
			result, err := e.PrimaryLSN(ctx)

			if tt.expectError {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expectedLSN, result)
			}
			assert.NoError(t, mockQueryService.ExpectationsWereMet())
		})
	}
}

func TestGetStandbyReplayLSN(t *testing.T) {
	tests := []struct {
		name        string
		setupMock   func(*mock.QueryService)
		expectError bool
		expectedLSN string
	}{
		{
			name: "successful query",
			setupMock: func(m *mock.QueryService) {
				m.AddQueryPatternOnce("pg_last_wal_replay_lsn", mock.MakeQueryResult([]string{"pg_last_wal_replay_lsn"}, [][]any{{"0/2000000"}}))
			},
			expectError: false,
			expectedLSN: "0/2000000",
		},
		{
			name: "different LSN format",
			setupMock: func(m *mock.QueryService) {
				m.AddQueryPatternOnce("pg_last_wal_replay_lsn", mock.MakeQueryResult([]string{"pg_last_wal_replay_lsn"}, [][]any{{"5/FFFF0000"}}))
			},
			expectError: false,
			expectedLSN: "5/FFFF0000",
		},
		{
			name: "query error",
			setupMock: func(m *mock.QueryService) {
				m.AddQueryPatternOnceWithError("SELECT pg_last_wal_replay_lsn", errors.New("connection done"))
			},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e, mockQueryService := newTestEngine()

			tt.setupMock(mockQueryService)

			ctx := context.Background()
			result, err := e.StandbyReplayLSN(ctx)

			if tt.expectError {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expectedLSN, result)
			}
			assert.NoError(t, mockQueryService.ExpectationsWereMet())
		})
	}
}

func TestValidateExpectedLSN(t *testing.T) {
	tests := []struct {
		name          string
		expectedLSN   string
		setupMock     func(*mock.QueryService)
		expectError   bool
		errorContains string
	}{
		{
			name:        "empty expectedLSN - no validation",
			expectedLSN: "",
			setupMock:   func(m *mock.QueryService) {},
			expectError: false,
		},
		{
			name:        "LSN match with paused replay",
			expectedLSN: "0/3000000",
			setupMock: func(m *mock.QueryService) {
				m.AddQueryPatternOnce("pg_last_wal_replay_lsn", mock.MakeQueryResult(
					[]string{"pg_last_wal_replay_lsn", "pg_is_wal_replay_paused"},
					[][]any{{"0/3000000", "t"}}))
			},
			expectError: false,
		},
		{
			name:        "LSN match with running replay (warning only)",
			expectedLSN: "0/3000000",
			setupMock: func(m *mock.QueryService) {
				m.AddQueryPatternOnce("pg_last_wal_replay_lsn", mock.MakeQueryResult(
					[]string{"pg_last_wal_replay_lsn", "pg_is_wal_replay_paused"},
					[][]any{{"0/3000000", "f"}}))
			},
			expectError: false,
		},
		{
			name:        "LSN mismatch",
			expectedLSN: "0/3000000",
			setupMock: func(m *mock.QueryService) {
				m.AddQueryPatternOnce("pg_last_wal_replay_lsn", mock.MakeQueryResult(
					[]string{"pg_last_wal_replay_lsn", "pg_is_wal_replay_paused"},
					[][]any{{"0/2000000", "t"}}))
			},
			expectError:   true,
			errorContains: "LSN mismatch",
		},
		{
			name:        "query error",
			expectedLSN: "0/3000000",
			setupMock: func(m *mock.QueryService) {
				m.AddQueryPatternOnceWithError("SELECT pg_last_wal_replay_lsn", errors.New("database error"))
			},
			expectError:   true,
			errorContains: "failed to get current replay LSN",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e, mockQueryService := newTestEngine()

			tt.setupMock(mockQueryService)

			ctx := context.Background()
			err := e.ValidateExpectedLSN(ctx, tt.expectedLSN)

			if tt.expectError {
				require.Error(t, err)
				if tt.errorContains != "" {
					assert.Contains(t, err.Error(), tt.errorContains)
				}
			} else {
				assert.NoError(t, err)
			}
			assert.NoError(t, mockQueryService.ExpectationsWereMet())
		})
	}
}

func TestPauseReplication(t *testing.T) {
	tests := []struct {
		name           string
		mode           multipoolermanagerdatapb.ReplicationPauseMode
		wait           bool
		setupMock      func(*mock.QueryService)
		expectError    bool
		errorContains  string
		expectStatus   bool // true if we expect a non-nil status to be returned
		validateResult func(t *testing.T, status *multipoolermanagerdatapb.StandbyReplicationStatus)
	}{
		{
			name: "PauseReplayOnly with wait=true",
			mode: multipoolermanagerdatapb.ReplicationPauseMode_REPLICATION_PAUSE_MODE_REPLAY_ONLY,
			wait: true,
			setupMock: func(m *mock.QueryService) {
				m.AddQueryPatternOnce("SELECT pg_wal_replay_pause", mock.MakeQueryResult(nil, nil))
				m.AddQueryPatternOnce("pg_last_wal_replay_lsn", mock.MakeQueryResult(
					[]string{"replay_lsn", "receive_lsn", "is_paused", "pause_state", "xact_time", "conninfo", "wal_receiver_status", "last_msg_receive_time", "wal_receiver_status_interval", "wal_receiver_timeout"},
					[][]any{{"0/3000000", "0/3000100", "t", "paused", "2025-01-15 10:00:00+00", "host=primary port=5432", "streaming", nil, nil, nil}}))
			},
			expectError:  false,
			expectStatus: true,
			validateResult: func(t *testing.T, status *multipoolermanagerdatapb.StandbyReplicationStatus) {
				assert.Equal(t, "0/3000000", status.LastReplayLsn)
				assert.True(t, status.IsWalReplayPaused)
			},
		},
		{
			name: "PauseReplayOnly with wait=false",
			mode: multipoolermanagerdatapb.ReplicationPauseMode_REPLICATION_PAUSE_MODE_REPLAY_ONLY,
			wait: false,
			setupMock: func(m *mock.QueryService) {
				m.AddQueryPatternOnce("SELECT pg_wal_replay_pause", mock.MakeQueryResult(nil, nil))
			},
			expectError:  false,
			expectStatus: false,
		},
		{
			name: "PauseReplayOnly fails on pause command",
			mode: multipoolermanagerdatapb.ReplicationPauseMode_REPLICATION_PAUSE_MODE_REPLAY_ONLY,
			wait: true,
			setupMock: func(m *mock.QueryService) {
				m.AddQueryPatternOnceWithError("SELECT pg_wal_replay_pause", errors.New("permission denied"))
			},
			expectError:   true,
			errorContains: "failed to pause WAL replay",
		},
		{
			name: "PauseReceiverOnly with wait=true",
			mode: multipoolermanagerdatapb.ReplicationPauseMode_REPLICATION_PAUSE_MODE_RECEIVER_ONLY,
			wait: true,
			setupMock: func(m *mock.QueryService) {
				m.AddQueryPatternOnce("ALTER SYSTEM RESET primary_conninfo", mock.MakeQueryResult(nil, nil))
				expectReloadConfig(m)
				m.AddQueryPatternOnce("SELECT COUNT", mock.MakeQueryResult([]string{"count", "status", "primary_conninfo"}, [][]any{{int64(0), "", ""}}))
				m.AddQueryPatternOnce("pg_last_wal_replay_lsn", mock.MakeQueryResult(
					[]string{"replay_lsn", "receive_lsn", "is_paused", "pause_state", "xact_time", "conninfo", "wal_receiver_status", "last_msg_receive_time", "wal_receiver_status_interval", "wal_receiver_timeout"},
					[][]any{{"0/4000000", "", "f", "not paused", "2025-01-15 11:00:00+00", "", "", nil, nil, nil}}))
			},
			expectError:  false,
			expectStatus: true,
			validateResult: func(t *testing.T, status *multipoolermanagerdatapb.StandbyReplicationStatus) {
				assert.Equal(t, "0/4000000", status.LastReplayLsn)
				assert.False(t, status.IsWalReplayPaused)
				assert.Empty(t, status.LastReceiveLsn)
			},
		},
		{
			name: "PauseReceiverOnly with wait=false",
			mode: multipoolermanagerdatapb.ReplicationPauseMode_REPLICATION_PAUSE_MODE_RECEIVER_ONLY,
			wait: false,
			setupMock: func(m *mock.QueryService) {
				m.AddQueryPatternOnce("ALTER SYSTEM RESET primary_conninfo", mock.MakeQueryResult(nil, nil))
				expectReloadConfig(m)
			},
			expectError:  false,
			expectStatus: false,
		},
		{
			name: "PauseReceiverOnly fails on ALTER SYSTEM",
			mode: multipoolermanagerdatapb.ReplicationPauseMode_REPLICATION_PAUSE_MODE_RECEIVER_ONLY,
			wait: false,
			setupMock: func(m *mock.QueryService) {
				m.AddQueryPatternOnceWithError("ALTER SYSTEM RESET primary_conninfo", errors.New("permission denied"))
			},
			expectError:   true,
			errorContains: "failed to clear primary_conninfo",
		},
		{
			name: "PauseReceiverOnly fails on reload",
			mode: multipoolermanagerdatapb.ReplicationPauseMode_REPLICATION_PAUSE_MODE_RECEIVER_ONLY,
			wait: false,
			setupMock: func(m *mock.QueryService) {
				m.AddQueryPatternOnce("ALTER SYSTEM RESET primary_conninfo", mock.MakeQueryResult(nil, nil))
				expectReloadConfigFailure(m, errors.New("reload failed"))
			},
			expectError:   true,
			errorContains: "failed to reload PostgreSQL configuration",
		},
		{
			name: "PauseReplayAndReceiver with wait=true",
			mode: multipoolermanagerdatapb.ReplicationPauseMode_REPLICATION_PAUSE_MODE_REPLAY_AND_RECEIVER,
			wait: true,
			setupMock: func(m *mock.QueryService) {
				m.AddQueryPatternOnce("ALTER SYSTEM RESET primary_conninfo", mock.MakeQueryResult(nil, nil))
				expectReloadConfig(m)
				m.AddQueryPatternOnce("SELECT COUNT", mock.MakeQueryResult([]string{"count", "status", "primary_conninfo"}, [][]any{{int64(0), "", ""}}))
				// First query for WaitForReceiverDisconnect - consumed after first match
				m.AddQueryPatternOnce("pg_last_wal_replay_lsn", mock.MakeQueryResult(
					[]string{"replay_lsn", "receive_lsn", "is_paused", "pause_state", "xact_time", "conninfo", "wal_receiver_status", "last_msg_receive_time", "wal_receiver_status_interval", "wal_receiver_timeout"},
					[][]any{{"0/5000000", "", "f", "not paused", "2025-01-15 12:00:00+00", "", "", nil, nil, nil}}))
				m.AddQueryPatternOnce("SELECT pg_wal_replay_pause", mock.MakeQueryResult(nil, nil))
				// Second query for WaitForReplicationPause
				m.AddQueryPatternOnce("pg_last_wal_replay_lsn", mock.MakeQueryResult(
					[]string{"replay_lsn", "receive_lsn", "is_paused", "pause_state", "xact_time", "conninfo", "wal_receiver_status", "last_msg_receive_time", "wal_receiver_status_interval", "wal_receiver_timeout"},
					[][]any{{"0/5000000", "", "t", "paused", "2025-01-15 12:00:00+00", "", "", nil, nil, nil}}))
			},
			expectError:  false,
			expectStatus: true,
			validateResult: func(t *testing.T, status *multipoolermanagerdatapb.StandbyReplicationStatus) {
				assert.Equal(t, "0/5000000", status.LastReplayLsn)
				assert.True(t, status.IsWalReplayPaused)
			},
		},
		{
			name: "PauseReplayAndReceiver with wait=false",
			mode: multipoolermanagerdatapb.ReplicationPauseMode_REPLICATION_PAUSE_MODE_REPLAY_AND_RECEIVER,
			wait: false,
			setupMock: func(m *mock.QueryService) {
				m.AddQueryPatternOnce("ALTER SYSTEM RESET primary_conninfo", mock.MakeQueryResult(nil, nil))
				expectReloadConfig(m)
				m.AddQueryPatternOnce("SELECT COUNT", mock.MakeQueryResult([]string{"count", "status", "primary_conninfo"}, [][]any{{int64(0), "", ""}}))
				m.AddQueryPatternOnce("pg_last_wal_replay_lsn", mock.MakeQueryResult(
					[]string{"replay_lsn", "receive_lsn", "is_paused", "pause_state", "xact_time", "conninfo", "wal_receiver_status", "last_msg_receive_time", "wal_receiver_status_interval", "wal_receiver_timeout"},
					[][]any{{"0/5000000", "", "f", "not paused", "2025-01-15 12:00:00+00", "", "", nil, nil, nil}}))
				m.AddQueryPatternOnce("SELECT pg_wal_replay_pause", mock.MakeQueryResult(nil, nil))
			},
			expectError:  false,
			expectStatus: false,
		},
		{
			name: "PauseReplayAndReceiver fails on clearing conninfo",
			mode: multipoolermanagerdatapb.ReplicationPauseMode_REPLICATION_PAUSE_MODE_REPLAY_AND_RECEIVER,
			wait: false,
			setupMock: func(m *mock.QueryService) {
				m.AddQueryPatternOnceWithError("ALTER SYSTEM RESET primary_conninfo", errors.New("reset failed"))
			},
			expectError:   true,
			errorContains: "failed to clear primary_conninfo",
		},
		{
			name: "PauseReplayAndReceiver fails on receiver disconnect wait",
			mode: multipoolermanagerdatapb.ReplicationPauseMode_REPLICATION_PAUSE_MODE_REPLAY_AND_RECEIVER,
			wait: false,
			setupMock: func(m *mock.QueryService) {
				m.AddQueryPatternOnce("ALTER SYSTEM RESET primary_conninfo", mock.MakeQueryResult(nil, nil))
				expectReloadConfig(m)
				m.AddQueryPatternOnceWithError("SELECT COUNT", errors.New("query failed"))
			},
			expectError:   true,
			errorContains: "failed to query pg_stat_wal_receiver",
		},
		{
			// count > 0 but status=waiting with empty primary_conninfo should be
			// treated as effectively disconnected: the walreceiver can't transition
			// out of WAITING without a non-empty primary_conninfo, and we hold the
			// action lock so nothing else can repopulate it.
			name: "PauseReceiverOnly accepts WALRCV_WAITING+empty primary_conninfo as disconnected",
			mode: multipoolermanagerdatapb.ReplicationPauseMode_REPLICATION_PAUSE_MODE_RECEIVER_ONLY,
			wait: true,
			setupMock: func(m *mock.QueryService) {
				m.AddQueryPatternOnce("ALTER SYSTEM RESET primary_conninfo", mock.MakeQueryResult(nil, nil))
				expectReloadConfig(m)
				m.AddQueryPatternOnce("SELECT COUNT", mock.MakeQueryResult(
					[]string{"count", "status", "primary_conninfo"},
					[][]any{{int64(1), "waiting", ""}}))
				m.AddQueryPatternOnce("pg_last_wal_replay_lsn", mock.MakeQueryResult(
					[]string{"replay_lsn", "receive_lsn", "is_paused", "pause_state", "xact_time", "conninfo", "wal_receiver_status", "last_msg_receive_time", "wal_receiver_status_interval", "wal_receiver_timeout"},
					[][]any{{"0/6000000", "", "f", "not paused", "2025-01-15 13:00:00+00", "", "waiting", nil, nil, nil}}))
			},
			expectError:  false,
			expectStatus: true,
			validateResult: func(t *testing.T, status *multipoolermanagerdatapb.StandbyReplicationStatus) {
				assert.Equal(t, "0/6000000", status.LastReplayLsn)
			},
		},
		{
			name: "PauseReplayAndReceiver fails on pause",
			mode: multipoolermanagerdatapb.ReplicationPauseMode_REPLICATION_PAUSE_MODE_REPLAY_AND_RECEIVER,
			wait: false,
			setupMock: func(m *mock.QueryService) {
				m.AddQueryPatternOnce("ALTER SYSTEM RESET primary_conninfo", mock.MakeQueryResult(nil, nil))
				expectReloadConfig(m)
				m.AddQueryPatternOnce("SELECT COUNT", mock.MakeQueryResult([]string{"count", "status", "primary_conninfo"}, [][]any{{int64(0), "", ""}}))
				m.AddQueryPatternOnce("pg_last_wal_replay_lsn", mock.MakeQueryResult(
					[]string{"replay_lsn", "receive_lsn", "is_paused", "pause_state", "xact_time", "conninfo", "wal_receiver_status", "last_msg_receive_time", "wal_receiver_status_interval", "wal_receiver_timeout"},
					[][]any{{"0/5000000", "", "f", "not paused", "2025-01-15 12:00:00+00", "", "", nil, nil, nil}}))
				m.AddQueryPatternOnceWithError("SELECT pg_wal_replay_pause", errors.New("pause failed"))
			},
			expectError:   true,
			errorContains: "failed to pause WAL replay",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e, mockQueryService := newTestEngine()

			tt.setupMock(mockQueryService)

			ctx := context.Background()
			status, err := e.PauseReplication(ctx, tt.mode, tt.wait)

			if tt.expectError {
				assert.Error(t, err)
				if tt.errorContains != "" {
					assert.Contains(t, err.Error(), tt.errorContains)
				}
				assert.Nil(t, status)
			} else {
				require.NoError(t, err)
				if tt.expectStatus {
					require.NotNil(t, status, "Expected non-nil status when wait=true")
					if tt.validateResult != nil {
						tt.validateResult(t, status)
					}
				} else {
					assert.Nil(t, status, "Expected nil status when wait=false")
				}
			}
			assert.NoError(t, mockQueryService.ExpectationsWereMet())
		})
	}
}

// TestWaitForReceiverDisconnect_Timeout covers the diagnostic timeout branch
// added with the better-diagnostics fix: when the wait budget expires, the
// function should return the canonical DEADLINE_EXCEEDED error rather than
// leaking a pool-context-expired wrapper. The 10s budget is hardcoded, but
// shrinking the parent context shortens the effective deadline via the
// min-deadline semantics of context.WithTimeout.
func TestWaitForReceiverDisconnect_Timeout(t *testing.T) {
	t.Run("deadline already expired surfaces timeout before polling", func(t *testing.T) {
		e, _ := newTestEngine()
		// Deadline already in the past: the first retry attempt observes the
		// expired context and returns the timeout without ever polling, so no
		// query mock is needed.
		ctx, cancel := context.WithDeadline(t.Context(), time.Now().Add(-time.Second))
		defer cancel()

		status, err := e.WaitForReceiverDisconnect(ctx)
		require.Error(t, err)
		assert.Nil(t, status)
		assert.Contains(t, err.Error(), "timeout waiting for WAL receiver to disconnect")
		assert.Equal(t, mtrpcpb.Code_DEADLINE_EXCEEDED, mterrors.Code(err))
	})

	t.Run("deadline expires while polls keep returning still-streaming", func(t *testing.T) {
		e, mockQueryService := newTestEngine()
		// Persistent (non-consumeOnce) pattern: the function polls repeatedly
		// (immediately, then with exponential backoff) until the deadline trips.
		mockQueryService.AddQueryPattern("SELECT COUNT", mock.MakeQueryResult(
			[]string{"count", "status", "primary_conninfo"},
			[][]any{{int64(1), "streaming", "host=primary port=5432"}}))

		ctx, cancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
		defer cancel()

		status, err := e.WaitForReceiverDisconnect(ctx)
		require.Error(t, err)
		assert.Nil(t, status)
		assert.Contains(t, err.Error(), "timeout waiting for WAL receiver to disconnect")
		assert.Equal(t, mtrpcpb.Code_DEADLINE_EXCEEDED, mterrors.Code(err))
	})

	t.Run("parent context cancelled surfaces cancellation, not timeout", func(t *testing.T) {
		e, _ := newTestEngine()
		ctx, cancel := context.WithCancel(t.Context())
		cancel() // cancel immediately

		status, err := e.WaitForReceiverDisconnect(ctx)
		require.Error(t, err)
		assert.Nil(t, status)
		assert.Contains(t, err.Error(), "context cancelled while waiting for WAL receiver to disconnect")
		assert.NotEqual(t, mtrpcpb.Code_DEADLINE_EXCEEDED, mterrors.Code(err))
	})
}

func TestResetPrimaryConnInfo(t *testing.T) {
	tests := []struct {
		name          string
		setupMock     func(*mock.QueryService)
		expectError   bool
		errorContains string
	}{
		{
			name: "successful reset",
			setupMock: func(m *mock.QueryService) {
				m.AddQueryPatternOnce("ALTER SYSTEM RESET primary_conninfo", mock.MakeQueryResult(nil, nil))
				expectReloadConfig(m)
			},
			expectError: false,
		},
		{
			name: "ALTER SYSTEM fails",
			setupMock: func(m *mock.QueryService) {
				m.AddQueryPatternOnceWithError("ALTER SYSTEM RESET primary_conninfo", errors.New("permission denied"))
			},
			expectError:   true,
			errorContains: "failed to clear primary_conninfo",
		},
		{
			name: "pg_reload_conf fails",
			setupMock: func(m *mock.QueryService) {
				m.AddQueryPatternOnce("ALTER SYSTEM RESET primary_conninfo", mock.MakeQueryResult(nil, nil))
				expectReloadConfigFailure(m, errors.New("reload failed"))
			},
			expectError:   true,
			errorContains: "failed to reload PostgreSQL configuration",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e, mockQueryService := newTestEngine()

			tt.setupMock(mockQueryService)

			ctx := context.Background()
			err := e.ResetPrimaryConnInfo(ctx)

			if tt.expectError {
				assert.Error(t, err)
				if tt.errorContains != "" {
					assert.Contains(t, err.Error(), tt.errorContains)
				}
			} else {
				assert.NoError(t, err)
			}
			assert.NoError(t, mockQueryService.ExpectationsWereMet())
		})
	}
}

func TestQueryReplicationStatus(t *testing.T) {
	tests := []struct {
		name           string
		setupMock      func(*mock.QueryService)
		expectError    bool
		validateResult func(t *testing.T, status *multipoolermanagerdatapb.StandbyReplicationStatus)
	}{
		{
			name: "All fields with valid values",
			setupMock: func(m *mock.QueryService) {
				m.AddQueryPatternOnce("pg_last_wal_replay_lsn", mock.MakeQueryResult(
					[]string{"replay_lsn", "receive_lsn", "is_paused", "pause_state", "xact_time", "conninfo", "wal_receiver_status", "last_msg_receive_time", "wal_receiver_status_interval", "wal_receiver_timeout"},
					[][]any{{"0/3000000", "0/3000100", "f", "not paused", "2025-01-15 10:00:00+00", "host=primary port=5432", "streaming", "2025-01-15 10:00:05+00", "10s", "60s"}}))
			},
			expectError: false,
			validateResult: func(t *testing.T, status *multipoolermanagerdatapb.StandbyReplicationStatus) {
				assert.Equal(t, "0/3000000", status.LastReplayLsn)
				assert.Equal(t, "0/3000100", status.LastReceiveLsn)
				assert.False(t, status.IsWalReplayPaused)
				assert.Equal(t, "not paused", status.WalReplayPauseState)
				assert.Equal(t, "2025-01-15 10:00:00+00", status.LastXactReplayTimestamp)
				assert.NotNil(t, status.PrimaryConnInfo)
				assert.Equal(t, "primary", status.PrimaryConnInfo.Host)
				assert.Equal(t, "streaming", status.WalReceiverStatus)
				assert.NotNil(t, status.LastMsgReceiveTime)
				assert.NotNil(t, status.WalReceiverStatusInterval)
				assert.Equal(t, 10*time.Second, status.WalReceiverStatusInterval.AsDuration())
				assert.NotNil(t, status.WalReceiverTimeout)
				assert.Equal(t, 60*time.Second, status.WalReceiverTimeout.AsDuration())
			},
		},
		{
			name: "NULL LSN values (primary server case)",
			setupMock: func(m *mock.QueryService) {
				m.AddQueryPatternOnce("pg_last_wal_replay_lsn", mock.MakeQueryResult(
					[]string{"replay_lsn", "receive_lsn", "is_paused", "pause_state", "xact_time", "conninfo", "wal_receiver_status", "last_msg_receive_time", "wal_receiver_status_interval", "wal_receiver_timeout"},
					[][]any{{"", "", "f", "not paused", "", "", "", nil, nil, nil}}))
			},
			expectError: false,
			validateResult: func(t *testing.T, status *multipoolermanagerdatapb.StandbyReplicationStatus) {
				assert.Empty(t, status.LastReplayLsn, "LastReplayLsn should be empty when NULL")
				assert.Empty(t, status.LastReceiveLsn, "LastReceiveLsn should be empty when NULL")
				assert.False(t, status.IsWalReplayPaused)
				assert.Equal(t, "not paused", status.WalReplayPauseState)
				assert.Empty(t, status.LastXactReplayTimestamp, "LastXactReplayTimestamp should be empty when NULL")
				assert.Empty(t, status.WalReceiverStatus, "WalReceiverStatus should be empty on primary")
				assert.Nil(t, status.LastMsgReceiveTime)
				assert.Nil(t, status.WalReceiverStatusInterval)
				assert.Nil(t, status.WalReceiverTimeout)
			},
		},
		{
			name: "Paused replication with valid LSNs",
			setupMock: func(m *mock.QueryService) {
				m.AddQueryPatternOnce("pg_last_wal_replay_lsn", mock.MakeQueryResult(
					[]string{"replay_lsn", "receive_lsn", "is_paused", "pause_state", "xact_time", "conninfo", "wal_receiver_status", "last_msg_receive_time", "wal_receiver_status_interval", "wal_receiver_timeout"},
					[][]any{{"0/4000000", "0/4000200", "t", "paused", "2025-01-15 11:00:00+00", "host=primary port=5432 user=replicator application_name=standby1", "streaming", "2025-01-15 11:00:05+00", "10s", "60s"}}))
			},
			expectError: false,
			validateResult: func(t *testing.T, status *multipoolermanagerdatapb.StandbyReplicationStatus) {
				assert.Equal(t, "0/4000000", status.LastReplayLsn)
				assert.Equal(t, "0/4000200", status.LastReceiveLsn)
				assert.True(t, status.IsWalReplayPaused)
				assert.Equal(t, "paused", status.WalReplayPauseState)
				assert.Equal(t, "2025-01-15 11:00:00+00", status.LastXactReplayTimestamp)
				assert.NotNil(t, status.PrimaryConnInfo)
				assert.Equal(t, "primary", status.PrimaryConnInfo.Host)
				assert.Equal(t, int32(5432), status.PrimaryConnInfo.Port)
				assert.Equal(t, "replicator", status.PrimaryConnInfo.User)
				assert.Equal(t, "standby1", status.PrimaryConnInfo.ApplicationName)
				assert.Equal(t, "streaming", status.WalReceiverStatus)
				assert.NotNil(t, status.WalReceiverStatusInterval)
				assert.Equal(t, 10*time.Second, status.WalReceiverStatusInterval.AsDuration())
				assert.NotNil(t, status.WalReceiverTimeout)
				assert.Equal(t, 60*time.Second, status.WalReceiverTimeout.AsDuration())
			},
		},
		{
			name: "Mixed NULL and valid values",
			setupMock: func(m *mock.QueryService) {
				m.AddQueryPatternOnce("pg_last_wal_replay_lsn", mock.MakeQueryResult(
					[]string{"replay_lsn", "receive_lsn", "is_paused", "pause_state", "xact_time", "conninfo", "wal_receiver_status", "last_msg_receive_time", "wal_receiver_status_interval", "wal_receiver_timeout"},
					[][]any{{"0/5000000", "", "f", "not paused", "", "host=primary port=5432", "", nil, nil, nil}}))
			},
			expectError: false,
			validateResult: func(t *testing.T, status *multipoolermanagerdatapb.StandbyReplicationStatus) {
				assert.Equal(t, "0/5000000", status.LastReplayLsn, "LastReplayLsn should be populated")
				assert.Empty(t, status.LastReceiveLsn, "LastReceiveLsn should be empty when NULL")
				assert.False(t, status.IsWalReplayPaused)
				assert.Empty(t, status.LastXactReplayTimestamp, "LastXactReplayTimestamp should be empty when NULL")
				assert.Empty(t, status.WalReceiverStatus)
				assert.Nil(t, status.LastMsgReceiveTime)
				assert.Nil(t, status.WalReceiverStatusInterval)
				assert.Nil(t, status.WalReceiverTimeout)
			},
		},
		{
			name: "Query error",
			setupMock: func(m *mock.QueryService) {
				m.AddQueryPatternOnceWithError("pg_last_wal_replay_lsn", errors.New("connection done"))
			},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e, mockQueryService := newTestEngine()

			tt.setupMock(mockQueryService)

			ctx := context.Background()
			status, err := e.ReplicationStatus(ctx)

			if tt.expectError {
				assert.Error(t, err)
				assert.Nil(t, status)
			} else {
				require.NoError(t, err)
				require.NotNil(t, status)
				if tt.validateResult != nil {
					tt.validateResult(t, status)
				}
			}
			assert.NoError(t, mockQueryService.ExpectationsWereMet())
		})
	}
}
