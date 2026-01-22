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

package manager

import (
	"context"
	"errors"
	"fmt"

	pgctldpb "github.com/multigres/multigres/go/pb/pgctldservice"
)

// StateChanger is a chokepoint for all operations that modify PostgreSQL state via pgctld.
// All state-modifying methods enforce that the action lock is held by the caller.
//
// This pattern makes it structurally impossible to modify state without holding the lock.
// For SQL-based state changes, use pm.exec(ctx, sql, QueryIntentStateChange) which also enforces lock checks.
//
// Read-only operations (like PgctldStatus) can be called without the lock.
type StateChanger struct {
	// pgctldClient is used to control PostgreSQL lifecycle
	// This is owned by StateChanger to enforce that all state changes go through
	// the lock-checking methods. Read-only operations are exposed separately.
	pgctldClient pgctldpb.PgCtldClient
}

// newStateChanger creates a new StateChanger.
// This is called by MultiPoolerManager during initialization.
func newStateChanger(pgctldClient pgctldpb.PgCtldClient) *StateChanger {
	return &StateChanger{
		pgctldClient: pgctldClient,
	}
}

// PgctldRestart restarts PostgreSQL via pgctld.
// Caller must hold the action lock.
func (sc *StateChanger) PgctldRestart(ctx context.Context, req *pgctldpb.RestartRequest) (*pgctldpb.RestartResponse, error) {
	if err := AssertActionLockHeld(ctx); err != nil {
		return nil, fmt.Errorf("StateChanger.PgctldRestart requires action lock: %w", err)
	}
	if sc.pgctldClient == nil {
		return nil, errors.New("pgctld client not available")
	}
	return sc.pgctldClient.Restart(ctx, req)
}

// PgctldStop stops PostgreSQL via pgctld.
// Caller must hold the action lock.
func (sc *StateChanger) PgctldStop(ctx context.Context, req *pgctldpb.StopRequest) (*pgctldpb.StopResponse, error) {
	if err := AssertActionLockHeld(ctx); err != nil {
		return nil, fmt.Errorf("StateChanger.PgctldStop requires action lock: %w", err)
	}
	if sc.pgctldClient == nil {
		return nil, errors.New("pgctld client not available")
	}
	return sc.pgctldClient.Stop(ctx, req)
}

// PgctldStart starts PostgreSQL via pgctld.
// Caller must hold the action lock.
func (sc *StateChanger) PgctldStart(ctx context.Context, req *pgctldpb.StartRequest) (*pgctldpb.StartResponse, error) {
	if err := AssertActionLockHeld(ctx); err != nil {
		return nil, fmt.Errorf("StateChanger.PgctldStart requires action lock: %w", err)
	}
	if sc.pgctldClient == nil {
		return nil, errors.New("pgctld client not available")
	}
	return sc.pgctldClient.Start(ctx, req)
}

// PgctldInitDataDir initializes the PostgreSQL data directory via pgctld.
// Caller must hold the action lock.
func (sc *StateChanger) PgctldInitDataDir(ctx context.Context, req *pgctldpb.InitDataDirRequest) (*pgctldpb.InitDataDirResponse, error) {
	if err := AssertActionLockHeld(ctx); err != nil {
		return nil, fmt.Errorf("StateChanger.PgctldInitDataDir requires action lock: %w", err)
	}
	if sc.pgctldClient == nil {
		return nil, errors.New("pgctld client not available")
	}
	return sc.pgctldClient.InitDataDir(ctx, req)
}

// PgctldPgRewind performs pg_rewind via pgctld.
// Caller must hold the action lock.
func (sc *StateChanger) PgctldPgRewind(ctx context.Context, req *pgctldpb.PgRewindRequest) (*pgctldpb.PgRewindResponse, error) {
	if err := AssertActionLockHeld(ctx); err != nil {
		return nil, fmt.Errorf("StateChanger.PgctldPgRewind requires action lock: %w", err)
	}
	if sc.pgctldClient == nil {
		return nil, errors.New("pgctld client not available")
	}
	return sc.pgctldClient.PgRewind(ctx, req)
}

// PgctldStatus returns the current status of PostgreSQL via pgctld.
// This is a read-only operation and does NOT require the action lock.
func (sc *StateChanger) PgctldStatus(ctx context.Context, req *pgctldpb.StatusRequest) (*pgctldpb.StatusResponse, error) {
	if sc.pgctldClient == nil {
		return nil, errors.New("pgctld client not available")
	}
	return sc.pgctldClient.Status(ctx, req)
}
