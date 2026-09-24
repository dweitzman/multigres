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

package multipooler

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"testing"
	"time"

	_ "github.com/lib/pq"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
	"github.com/multigres/multigres/go/test/utils"
)

// TestPgIsReadyMissesCorruptedDataFiles is a regression test for the fact that
// pg_isready (a bare "can I open a connection" probe) says a primary is
// healthy even after its actual table data has been deleted from disk. It
// shrinks shared_buffers so the primary's data can't all live in cache, then
// deletes the contents of pg_data/base (leaving pg_wal, global, etc. intact)
// while postgres keeps running. A real query against the table starts
// failing immediately, but the multipooler's reported PostgresReady status
// stays true indefinitely, since it's derived from pg_isready rather than
// from actually reading data.
func TestPgIsReadyMissesCorruptedDataFiles(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping end-to-end tests in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("Skipping end-to-end test (no postgres binaries)")
	}

	// Shrink shared_buffers to the postgres minimum so the table we insert
	// below can't stay fully cached, forcing real reads from the (soon to be
	// deleted) underlying files.
	confDir := t.TempDir()
	confPath := filepath.Join(confDir, "tiny_shared_buffers.conf")
	require.NoError(t, os.WriteFile(confPath, []byte("shared_buffers = '128kB'\n"), 0o644))

	setup, cleanup := shardsetup.NewIsolated(t,
		shardsetup.WithPgInitdbExtraConfFiles(confPath),
	)
	defer cleanup()

	primary := setup.GetPrimary(t)
	require.NotNil(t, primary, "expected a primary multipooler")

	client, err := shardsetup.NewMultipoolerClient(primary.Multipooler.GrpcPort)
	require.NoError(t, err)
	defer client.Close()

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	_, err = client.Pooler.ExecuteQuery(ctx, "CREATE TABLE health_check_probe (id serial primary key, payload text)", 0)
	require.NoError(t, err, "create table")

	// Enough padded rows to exceed the 128kB buffer pool many times over.
	insert := "INSERT INTO health_check_probe (payload) SELECT repeat('x', 1024) FROM generate_series(1, 500)"
	_, err = client.Pooler.ExecuteQuery(ctx, insert, 0)
	require.NoError(t, err, "seed table")

	_, err = client.Pooler.ExecuteQuery(ctx, "SELECT count(*) FROM health_check_probe", 1)
	require.NoError(t, err, "baseline query should succeed before corruption")

	pgDataDir := filepath.Join(primary.Pgctld.PoolerDir, "pg_data")
	baseDir := filepath.Join(pgDataDir, "base")
	entries, err := os.ReadDir(baseDir)
	require.NoError(t, err, "read pg_data/base")
	require.NotEmpty(t, entries, "pg_data/base should not be empty")
	for _, e := range entries {
		require.NoError(t, os.RemoveAll(filepath.Join(baseDir, e.Name())))
	}
	t.Logf("deleted contents of %s while postgres keeps running", baseDir)

	// Query through a brand-new backend connection rather than multipooler's
	// pooled one: a backend that already had the relation file open keeps
	// reading/writing through that fd even after it's unlinked (standard
	// Unix delete-while-open semantics), masking the corruption. A fresh
	// backend has to open() the file by path and gets ENOENT immediately.
	directDB, err := sql.Open("postgres", shardsetup.GetPostgresDSN("localhost", primary.Pgctld.PgPort, "sslmode=disable"))
	require.NoError(t, err)
	_, err = directDB.ExecContext(ctx, "SELECT count(*) FROM health_check_probe")
	require.Error(t, err, "query should fail once the underlying table files are gone")
	t.Logf("query against corrupted data failed as expected: %v", err)
	require.NoError(t, directDB.Close())

	// This is the actual regression: PostgresReady should reflect that the
	// primary can no longer serve real data, but pg_isready-based health
	// checking never notices, so this currently times out.
	shardsetup.EventuallyPoolerCondition(t,
		[]*shardsetup.MultipoolerInstance{primary},
		15*time.Second, 2*time.Second,
		func(r shardsetup.PoolerStatusResult) (bool, string) {
			if r.Status == nil {
				return false, "no status"
			}
			if r.Status.PostgresReady {
				return false, "PostgresReady still true despite corrupted data files"
			}
			return true, ""
		},
		"expected PostgresReady to eventually go false after data corruption",
	)
}
