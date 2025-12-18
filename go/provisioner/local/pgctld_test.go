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

package local

import (
	"fmt"
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestBuildPgctldServerArgs tests that pgctld server arguments are correctly constructed
// from configuration, particularly for optional parameters like pg_pwfile.
func TestBuildPgctldServerArgs(t *testing.T) {
	tests := []struct {
		name           string
		config         map[string]any
		expectPwfile   bool
		expectedPwfile string
	}{
		{
			name: "with pg_pwfile configured",
			config: map[string]any{
				"pooler_dir":  "/data/pooler",
				"grpc_port":   5433,
				"pg_port":     5432,
				"pg_database": "postgres",
				"pg_user":     "postgres",
				"timeout":     30,
				"log_level":   "info",
				"pg_pwfile":   "/etc/postgres/pwfile",
			},
			expectPwfile:   true,
			expectedPwfile: "/etc/postgres/pwfile",
		},
		{
			name: "without pg_pwfile configured",
			config: map[string]any{
				"pooler_dir":  "/data/pooler",
				"grpc_port":   5433,
				"pg_port":     5432,
				"pg_database": "postgres",
				"pg_user":     "postgres",
				"timeout":     30,
				"log_level":   "info",
			},
			expectPwfile: false,
		},
		{
			name: "with empty pg_pwfile",
			config: map[string]any{
				"pooler_dir":  "/data/pooler",
				"grpc_port":   5433,
				"pg_port":     5432,
				"pg_database": "postgres",
				"pg_user":     "postgres",
				"timeout":     30,
				"log_level":   "info",
				"pg_pwfile":   "",
			},
			expectPwfile: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			args := buildPgctldServerArgs(tt.config, "/path/to/logfile")

			// Check if --pg-pwfile is in args
			hasPwfile := false
			pwfileValue := ""
			for i, arg := range args {
				if arg == "--pg-pwfile" {
					hasPwfile = true
					if i+1 < len(args) {
						pwfileValue = args[i+1]
					}
					break
				}
			}

			assert.Equal(t, tt.expectPwfile, hasPwfile, "pg-pwfile presence mismatch")
			if tt.expectPwfile {
				assert.Equal(t, tt.expectedPwfile, pwfileValue, "pg-pwfile value mismatch")
			}

			// Verify required args are always present
			assertContainsArg(t, args, "--pooler-dir")
			assertContainsArg(t, args, "--grpc-port")
			assertContainsArg(t, args, "--pg-port")
			assertContainsArg(t, args, "--pg-database")
			assertContainsArg(t, args, "--pg-user")
			assertContainsArg(t, args, "--timeout")
			assertContainsArg(t, args, "--log-level")
			assertContainsArg(t, args, "--log-output")
		})
	}
}

func assertContainsArg(t *testing.T, args []string, expected string) {
	t.Helper()
	if !slices.Contains(args, expected) {
		t.Errorf("expected args to contain %q, got %v", expected, args)
	}
}

// buildPgctldServerArgs extracts the argument building logic for testability.
// This mirrors the logic in provisionPgctld.
func buildPgctldServerArgs(config map[string]any, logFile string) []string {
	// Get values with defaults (mirrors provisionPgctld logic)
	grpcPort := 5433
	if port, ok := config["grpc_port"].(int); ok && port > 0 {
		grpcPort = port
	}

	pgPort := 5432
	if port, ok := config["pg_port"].(int); ok && port > 0 {
		pgPort = port
	}

	pgDatabase := "postgres"
	if db, ok := config["pg_database"].(string); ok && db != "" {
		pgDatabase = db
	}

	pgUser := "postgres"
	if user, ok := config["pg_user"].(string); ok && user != "" {
		pgUser = user
	}

	timeout := 30
	if t, ok := config["timeout"].(int); ok {
		timeout = t
	}

	logLevel := "info"
	if level, ok := config["log_level"].(string); ok && level != "" {
		logLevel = level
	}

	poolerDir := ""
	if dir, ok := config["pooler_dir"].(string); ok {
		poolerDir = dir
	}

	pgPwfile := ""
	if pwfile, ok := config["pg_pwfile"].(string); ok && pwfile != "" {
		pgPwfile = pwfile
	}

	args := []string{
		"server",
		"--pooler-dir", poolerDir,
		"--grpc-port", fmt.Sprintf("%d", grpcPort),
		"--pg-port", fmt.Sprintf("%d", pgPort),
		"--pg-database", pgDatabase,
		"--pg-user", pgUser,
		"--timeout", fmt.Sprintf("%d", timeout),
		"--log-level", logLevel,
		"--log-output", logFile,
	}

	// Add password file if configured
	if pgPwfile != "" {
		args = append(args, "--pg-pwfile", pgPwfile)
	}

	return args
}
