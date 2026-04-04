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

package command

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	"github.com/multigres/multigres/go/services/pgctld"
	"github.com/multigres/multigres/go/tools/executil"
)

type PgRewindResult struct {
	// Status message
	Message string
	Output  string
}

func PgRewindWithResult(ctx context.Context, logger *slog.Logger, sourceServer, password string, dryRun bool, extraArgs []string) (*PgRewindResult, error) {
	result := &PgRewindResult{}
	dataDir := pgctld.PostgresDataDir()

	args := []string{
		"--source-server", sourceServer,
		"--target-pgdata", dataDir,
	}
	if dryRun {
		args = append(args, "--dry-run")
	}
	args = append(args, extraArgs...)

	logger.InfoContext(ctx, "executing pg_rewind command",
		"command", "pg_rewind",
		"args", args,
		"source_server", sourceServer,
		"target_pgdata", dataDir,
		"dry_run", dryRun)

	cmd := executil.Command(ctx, "pg_rewind", args...)

	// Set PGPASSWORD environment variable for pg_rewind to use
	// pg_rewind doesn't reliably use passwords from connection strings
	if password != "" {
		cmd.AddEnv("PGPASSWORD=" + password)
	}

	// Capture both Stdout and Stderr
	output, err := cmd.CombinedOutput()
	result.Output = string(output)
	if err != nil {
		result.Message = "Rewind failed"
		logger.ErrorContext(ctx, "pg_rewind command failed",
			"error", err,
			"output", string(output))
		return result, fmt.Errorf("pg_rewind failed: %w", err)
	}

	result.Message = "Rewind completed successfully"
	logger.InfoContext(ctx, "pg_rewind command completed successfully",
		"output", string(output))

	if !dryRun {
		logControlDataAfterRewind(ctx, logger)
	}

	return result, nil
}

// logControlDataAfterRewind runs pg_controldata after a successful rewind and logs
// the recovery point fields. This makes it easy to verify that minRecoveryPointTLI
// matches the expected timeline — a mismatch here will cause PostgreSQL startup to fail.
func logControlDataAfterRewind(ctx context.Context, logger *slog.Logger) {
	cmd := executil.Command(ctx, "pg_controldata", pgctld.PostgresDataDir())
	output, err := cmd.CombinedOutput()
	if err != nil {
		logger.WarnContext(ctx, "pg_controldata after rewind failed", "error", err)
		return
	}

	fields := map[string]string{
		"Latest checkpoint's TimeLineID":     "",
		"Minimum recovery ending location":   "",
		"Min recovery ending loc's timeline": "",
	}
	for line := range strings.SplitSeq(string(output), "\n") {
		for key := range fields {
			if strings.HasPrefix(line, key) {
				parts := strings.SplitN(line, ":", 2)
				if len(parts) == 2 {
					fields[key] = strings.TrimSpace(parts[1])
				}
			}
		}
	}

	logger.InfoContext(ctx, "pg_controldata after rewind",
		"checkpoint_tli", fields["Latest checkpoint's TimeLineID"],
		"min_recovery_point", fields["Minimum recovery ending location"],
		"min_recovery_point_tli", fields["Min recovery ending loc's timeline"],
	)
}
