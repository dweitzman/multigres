// Copyright 2025 Supabase, Inc.
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

package executor

import (
	"context"
	"log/slog"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewExecutor(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	config := &DBConfig{
		PoolerDir: "/tmp/test",
		Database:  "testdb",
		PgPort:    5432,
	}

	exec := NewExecutor(logger, config)

	require.NotNil(t, exec)
	assert.Equal(t, config, exec.dbConfig)
	assert.Equal(t, logger, exec.logger)
	assert.False(t, exec.isOpen.Load())
}

func TestExecutor_Open_NilConfig(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	exec := NewExecutor(logger, nil)

	err := exec.Open()

	require.Error(t, err)
	assert.Contains(t, err.Error(), "database config not set")
}

func TestExecutor_Close_WhenNotOpen(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	exec := NewExecutor(logger, nil)

	// Close should be idempotent when not open
	err := exec.Close(context.Background())

	require.NoError(t, err)
}

func TestExecutor_Close_Idempotent(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	exec := NewExecutor(logger, nil)

	// Multiple closes should be safe
	err1 := exec.Close(context.Background())
	err2 := exec.Close(context.Background())

	require.NoError(t, err1)
	require.NoError(t, err2)
}

func TestExecutor_IsHealthy_NilDB(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	exec := NewExecutor(logger, nil)

	err := exec.IsHealthy()

	require.Error(t, err)
	assert.Contains(t, err.Error(), "database connection not initialized")
}

func TestExecutor_generateCommandTag(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	exec := NewExecutor(logger, nil)

	tests := []struct {
		name         string
		query        string
		rowsAffected uint64
		expected     string
	}{
		{
			name:         "INSERT with rows",
			query:        "INSERT INTO users (name) VALUES ('test')",
			rowsAffected: 1,
			expected:     "INSERT 0 1",
		},
		{
			name:         "INSERT multiple rows",
			query:        "INSERT INTO users (name) VALUES ('a'), ('b'), ('c')",
			rowsAffected: 3,
			expected:     "INSERT 0 3",
		},
		{
			name:         "UPDATE with rows",
			query:        "UPDATE users SET name = 'new' WHERE id = 1",
			rowsAffected: 1,
			expected:     "UPDATE 1",
		},
		{
			name:         "UPDATE no rows",
			query:        "UPDATE users SET name = 'new' WHERE id = -1",
			rowsAffected: 0,
			expected:     "UPDATE 0",
		},
		{
			name:         "DELETE with rows",
			query:        "DELETE FROM users WHERE id = 1",
			rowsAffected: 1,
			expected:     "DELETE 1",
		},
		{
			name:         "DELETE multiple rows",
			query:        "DELETE FROM users WHERE active = false",
			rowsAffected: 5,
			expected:     "DELETE 5",
		},
		{
			name:         "CREATE TABLE",
			query:        "CREATE TABLE test (id INT)",
			rowsAffected: 0,
			expected:     "CREATE TABLE",
		},
		{
			name:         "DROP TABLE",
			query:        "DROP TABLE test",
			rowsAffected: 0,
			expected:     "DROP TABLE",
		},
		{
			name:         "ALTER TABLE",
			query:        "ALTER TABLE users ADD COLUMN email TEXT",
			rowsAffected: 0,
			expected:     "ALTER TABLE",
		},
		{
			name:         "CREATE INDEX",
			query:        "CREATE INDEX idx_users_name ON users(name)",
			rowsAffected: 0,
			expected:     "CREATE INDEX",
		},
		{
			name:         "DROP INDEX",
			query:        "DROP INDEX idx_users_name",
			rowsAffected: 0,
			expected:     "DROP INDEX",
		},
		{
			name:         "unknown command",
			query:        "TRUNCATE users",
			rowsAffected: 0,
			expected:     "COMMAND",
		},
		{
			name:         "case insensitive - lowercase",
			query:        "insert into users (name) values ('test')",
			rowsAffected: 1,
			expected:     "INSERT 0 1",
		},
		{
			name:         "with leading whitespace",
			query:        "  UPDATE users SET name = 'new'",
			rowsAffected: 2,
			expected:     "UPDATE 2",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := exec.generateCommandTag(tt.query, tt.rowsAffected)
			assert.Equal(t, tt.expected, result)
		})
	}
}
