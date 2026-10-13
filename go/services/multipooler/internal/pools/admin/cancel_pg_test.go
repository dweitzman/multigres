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

package admin

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/pgprotocol/client"
	"github.com/multigres/multigres/go/common/sqltypes"
	testutils "github.com/multigres/multigres/go/test/utils"
)

// startTestPostgres starts a throwaway postgres listening on a unix socket and returns
// a function that opens a superuser connection to it.
func startTestPostgres(t *testing.T, extraOptions string) func(context.Context) (*client.Conn, error) {
	t.Helper()
	if testutils.ShouldSkipRealPostgres() {
		t.Skip("skipping: no real postgres binaries available")
	}

	// /tmp keeps the unix socket path under the macOS length limit.
	dataDir, err := os.MkdirTemp("/tmp", "admin_cancel_pg_test-*")
	require.NoError(t, err)
	pgDataDir := filepath.Join(dataDir, "pg_data")
	socketDir := filepath.Join(dataDir, "sockets")
	require.NoError(t, os.MkdirAll(socketDir, 0o700))
	env := append(os.Environ(), "LC_ALL=C")

	initdb := exec.Command("initdb", "-D", pgDataDir, "-U", "postgres", "--no-instructions", "-A", "trust")
	initdb.Env = env
	out, err := initdb.CombinedOutput()
	require.NoError(t, err, string(out))

	start := exec.Command("pg_ctl", "start", "-D", pgDataDir, "-w", "-t", "30",
		"-l", filepath.Join(dataDir, "postgres.log"),
		"-o", fmt.Sprintf("-c listen_addresses= -c unix_socket_directories=%s -c logging_collector=off %s", socketDir, extraOptions))
	start.Env = env
	out, err = start.CombinedOutput()
	require.NoError(t, err, string(out))
	t.Cleanup(func() {
		_ = exec.Command("pg_ctl", "stop", "-D", pgDataDir, "-m", "immediate").Run()
		_ = os.RemoveAll(dataDir)
	})

	return func(ctx context.Context) (*client.Conn, error) {
		return client.Connect(ctx, ctx, &client.Config{
			SocketFile: filepath.Join(socketDir, ".s.PGSQL.5432"),
			User:       "postgres",
			Database:   "postgres",
		})
	}
}

func scalar(t *testing.T, ctx context.Context, conn *client.Conn, sql string) string {
	t.Helper()
	results, err := conn.QueryArgs(ctx, sql)
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.Len(t, results[0].Rows, 1)
	return string(results[0].Rows[0].Values[0])
}

// TestExecQueryWithContextCancel_EndsBlockedBackend: a write whose commit is blocked
// waiting for a synchronous standby must not leave its server backend behind when the
// caller's context expires. Closing the socket alone doesn't end the wait.
func TestExecQueryWithContextCancel_EndsBlockedBackend(t *testing.T) {
	for name, tc := range map[string]struct {
		postgresOptions string
		exhaustSlots    bool
	}{
		"with free connection slots": {},
		// The cancel must not need a connection slot of its own.
		"with every connection slot in use": {
			postgresOptions: "-c max_connections=7 -c superuser_reserved_connections=3",
			exhaustSlots:    true,
		},
	} {
		t.Run(name, func(t *testing.T) {
			connect := startTestPostgres(t, tc.postgresOptions)
			ctx := t.Context()

			observer, err := connect(ctx)
			require.NoError(t, err)
			defer observer.Close()
			_, err = observer.Query(ctx, "CREATE TABLE t (i int)")
			require.NoError(t, err)

			// Commits now wait for a standby that doesn't exist.
			_, err = observer.Query(ctx, "ALTER SYSTEM SET synchronous_standby_names = 'nobody'")
			require.NoError(t, err)
			_, err = observer.Query(ctx, "SELECT pg_reload_conf()")
			require.NoError(t, err)
			require.Eventually(t, func() bool {
				return scalar(t, ctx, observer, "SHOW synchronous_standby_names") == "nobody"
			}, 5*time.Second, 50*time.Millisecond)

			victim, err := connect(ctx)
			require.NoError(t, err)
			defer victim.Close()

			if tc.exhaustSlots {
				var fillers []*client.Conn
				defer func() {
					for _, c := range fillers {
						c.Close()
					}
				}()
				for {
					c, err := connect(ctx)
					if err != nil {
						break
					}
					fillers = append(fillers, c)
				}
				_, err = connect(ctx)
				require.ErrorContains(t, err, "too many clients", "setup must leave no free slot")
			}

			queryCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
			defer cancel()
			_, err = execQueryWithContextCancel(queryCtx, victim, func() ([]*sqltypes.Result, error) {
				return victim.QueryArgs(queryCtx, "INSERT INTO t VALUES (1)")
			})
			assert.ErrorIs(t, err, context.DeadlineExceeded)
			assert.True(t, victim.IsClosed(), "the timed-out connection must not be reused")

			assert.Eventually(t, func() bool {
				return scalar(t, ctx, observer, "SELECT count(*) FROM pg_stat_activity WHERE wait_event = 'SyncRep'") == "0"
			}, 5*time.Second, 50*time.Millisecond, "the backend must stop waiting for the standby")
		})
	}
}
