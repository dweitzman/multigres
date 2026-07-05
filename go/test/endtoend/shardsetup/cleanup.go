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

package shardsetup

import (
	"context"
	"fmt"
	"testing"
	"time"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multipoolermanagerpb "github.com/multigres/multigres/go/pb/multipoolermanager"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
)

// ValidatePoolerType checks that the pooler type in topology matches the expected value.
// Follows the pattern from multipooler/setup_test.go:validatePoolerType.
func ValidatePoolerType(ctx context.Context, client multipoolermanagerpb.MultiPoolerManagerClient, expectedType clustermetadatapb.PoolerType, nodeName string) error {
	status, err := client.Status(ctx, &multipoolermanagerdatapb.StatusRequest{})
	if err != nil {
		return fmt.Errorf("%s failed to get status: %w", nodeName, err)
	}

	if status.Status == nil {
		return fmt.Errorf("%s status response has nil Status field", nodeName)
	}

	if status.Status.PoolerType != expectedType {
		return fmt.Errorf("%s pooler type=%s (expected %s)", nodeName, status.Status.PoolerType.String(), expectedType.String())
	}

	return nil
}

// SaveGUCs queries multiple GUC values and saves them to a map.
// Returns a map of gucName -> value. Empty values are preserved.
func SaveGUCs(ctx context.Context, client *MultiPoolerTestClient, gucNames []string) map[string]string {
	saved := make(map[string]string)
	for _, gucName := range gucNames {
		value, err := QueryStringValue(ctx, client, "SHOW "+gucName)
		if err == nil {
			saved[gucName] = value
		}
	}
	return saved
}

// ReloadConfig calls pg_reload_conf() and waits for the reload to complete
// using pg_conf_load_time() as an event-based completion signal.
//
// pg_reload_conf() sends SIGHUP and returns immediately, before postgres has
// processed the signal. This function waits until pg_conf_load_time() advances
// past the pre-reload value, which happens atomically when postgres finishes
// processing the SIGHUP — at which point all GUC values are guaranteed to
// reflect the latest postgresql.auto.conf.
//
// ctx is used only for the pg_reload_conf() call. The reload-completion wait
// uses its own internal context so that a short caller deadline does not cut
// off the wait on a loaded system.
//
// Returns an error instead of failing t directly so callers running this
// concurrently across instances (e.g. via errgroup) can report failures from
// the goroutine running the test, as required by testing.T.
func ReloadConfig(ctx context.Context, client *MultiPoolerTestClient, instanceName string) error {
	loadTimeBefore, err := QueryStringValue(ctx, client, "SELECT pg_conf_load_time()")
	if err != nil {
		return fmt.Errorf("failed to get pg_conf_load_time on %s: %w", instanceName, err)
	}

	if _, err := client.ExecuteQuery(ctx, "SELECT pg_reload_conf()", 1); err != nil {
		return fmt.Errorf("failed to reload config on %s: %w", instanceName, err)
	}

	// Use a fresh context for polling so a short caller ctx does not cut off the wait.
	pollCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	deadline := time.Now().Add(30 * time.Second)
	for {
		loadTimeAfter, err := QueryStringValue(pollCtx, client, "SELECT pg_conf_load_time()")
		if err != nil {
			return fmt.Errorf("error calling pg_conf_load_time on %s: %w", instanceName, err)
		}
		if loadTimeAfter != loadTimeBefore {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("%s: pg_conf_load_time did not advance after pg_reload_conf()", instanceName)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// RestoreGUCs restores GUC values from a saved map using ALTER SYSTEM, then
// calls ReloadConfig to apply the changes and wait for the reload to complete.
// Empty values are treated as RESET (restore to default).
func RestoreGUCs(ctx context.Context, t *testing.T, client *MultiPoolerTestClient, savedGucs map[string]string, instanceName string) error {
	t.Helper()

	for gucName, gucValue := range savedGucs {
		var query string
		if gucValue == "" {
			query = "ALTER SYSTEM RESET " + gucName
		} else {
			query = fmt.Sprintf("ALTER SYSTEM SET %s = '%s'", gucName, gucValue)
		}
		_, err := client.ExecuteQuery(ctx, query, 1)
		if err != nil {
			t.Logf("Warning: Failed to restore %s on %s in cleanup: %v", gucName, instanceName, err)
		}
	}

	return ReloadConfig(ctx, client, instanceName)
}

// ValidateGUCValue queries a GUC and returns an error if it doesn't match the expected value.
// Follows the pattern from multipooler/setup_test.go:validateGUCValue.
func ValidateGUCValue(ctx context.Context, client *MultiPoolerTestClient, gucName, expected, instanceName string) error {
	value, err := QueryStringValue(ctx, client, "SHOW "+gucName)
	if err != nil {
		return fmt.Errorf("%s failed to query %s: %w", instanceName, gucName, err)
	}
	if value != expected {
		return fmt.Errorf("%s has %s='%s' (expected '%s')", instanceName, gucName, value, expected)
	}
	return nil
}
