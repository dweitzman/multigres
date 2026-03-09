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

// Package postgres sketches the production wiring that connects the pure consensus
// state machines to real PostgreSQL instances and gRPC transports.
//
// Three pieces:
//
//   - AtomicStateFile: crash-safe PoolerStorage (write-rename+fsync).
//   - PoolerDriver: runs a PoolerNode; serves inbound gRPC from coordinators;
//     drives the GUC/WAL pipeline for rule changes and the revocation sidecar.
//   - CoordDriver: runs a CoordNode; dials pooler gRPC endpoints for policy
//     writes and recruit calls; feeds discovery events from etcd.
//
// All gRPC call sites are documented as comments rather than wired to real proto
// stubs, so the sketch compiles and captures the design without requiring a full
// protobuf definition.
package postgres

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/multigres/multigres/go/common/consensus"
)

// Compile-time check: AtomicStateFile must satisfy the storage interface.
var _ consensus.PoolerStorage = (*AtomicStateFile)(nil)

// AtomicStateFile persists PoolerPersistentState to disk using write-rename+fsync
// so that a crash mid-write never leaves a partial or corrupt file on disk.
// This upholds the consensus invariant that committed state must survive restarts.
type AtomicStateFile struct {
	path string // e.g. /var/lib/pooler/consensus-state.json
}

// NewAtomicStateFile creates an AtomicStateFile that reads and writes to path.
func NewAtomicStateFile(path string) *AtomicStateFile {
	return &AtomicStateFile{path: path}
}

// Save atomically persists state to disk:
//  1. Marshal to JSON (see stateJSON for encoding details).
//  2. Write to a temp file in the same directory (same filesystem as path).
//  3. fsync the temp file — data is durable before the rename.
//  4. Rename temp → path (atomic on POSIX).
//  5. fsync the parent directory — directory entry is durable.
func (f *AtomicStateFile) Save(state consensus.PoolerPersistentState) error {
	data, err := marshalState(state)
	if err != nil {
		return err
	}

	dir := filepath.Dir(f.path)
	tmp, err := os.CreateTemp(dir, ".consensus-*.tmp")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName) // no-op after a successful rename

	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpName, f.path); err != nil {
		return err
	}

	dirFile, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer dirFile.Close()
	return dirFile.Sync()
}

// Load reads the state file written by Save and deserialises it.
// Called by NewPoolerNode on startup and by Restart() after crash recovery.
// Returns a zero state (not an error) if the file does not yet exist (first run).
func (f *AtomicStateFile) Load() (consensus.PoolerPersistentState, error) {
	data, err := os.ReadFile(f.path)
	if os.IsNotExist(err) {
		return consensus.PoolerPersistentState{}, nil // first run: no prior state
	}
	if err != nil {
		return consensus.PoolerPersistentState{}, err
	}
	return unmarshalState(data)
}

// ── JSON encoding ─────────────────────────────────────────────────────────────
//
// DurabilityRules embeds DurabilityPolicy, which is an interface. We flatten it for
// JSON: the production code only uses AtLeastPolicy so we store the threshold
// directly. If a non-AtLeast policy is ever encountered, Save returns an error to
// surface the gap rather than silently losing policy information.

type stateJSON struct {
	Role       consensus.PoolerRole             `json:"role"`
	Primary    consensus.NodeID                 `json:"primary"`
	Rules      *rulesJSON                       `json:"rules,omitempty"`
	Commitment *consensus.RecruitmentCommitment `json:"commitment,omitempty"`
}

type rulesJSON struct {
	Seq     int64                    `json:"seq"`
	Primary consensus.NodeID         `json:"primary"`
	Members []consensus.CohortMember `json:"members"`
	// AtLeast is the AtLeastThreshold for the AtLeast policy. Only AtLeastPolicy
	// is supported in production; if we ever need other policy shapes, add a
	// discriminant here.
	AtLeast int `json:"at_least"`
}

type atLeastThresholder interface {
	AtLeastThreshold() int
}

func marshalState(state consensus.PoolerPersistentState) ([]byte, error) {
	js := stateJSON{
		Role:       state.Role,
		Primary:    state.Primary,
		Commitment: state.Commitment,
	}
	if state.Rules != nil {
		at, ok := state.Rules.Policy.(atLeastThresholder)
		if !ok {
			return nil, fmt.Errorf("unsupported DurabilityPolicy type %T: only AtLeastPolicy is supported in production", state.Rules.Policy)
		}
		js.Rules = &rulesJSON{
			Seq:     state.Rules.Seq,
			Primary: state.Rules.Primary,
			Members: state.Rules.Members,
			AtLeast: at.AtLeastThreshold(),
		}
	}
	return json.Marshal(js)
}

func unmarshalState(data []byte) (consensus.PoolerPersistentState, error) {
	var js stateJSON
	if err := json.Unmarshal(data, &js); err != nil {
		return consensus.PoolerPersistentState{}, err
	}
	state := consensus.PoolerPersistentState{
		Role:       js.Role,
		Primary:    js.Primary,
		Commitment: js.Commitment,
	}
	if js.Rules != nil {
		state.Rules = &consensus.DurabilityRules{
			Seq:     js.Rules.Seq,
			Primary: js.Rules.Primary,
			Members: js.Rules.Members,
			Policy:  consensus.AtLeastPolicy(js.Rules.AtLeast),
		}
	}
	return state, nil
}
