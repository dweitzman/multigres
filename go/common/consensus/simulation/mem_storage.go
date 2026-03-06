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

// Package simulation provides simulation-only infrastructure for testing the
// consensus package. It is not imported by production code.
//
// The key components are:
//
//   - MemStorage: in-memory PoolerStorage for PoolerNode
//   - SimPooler: wraps a PoolerNode and acts as the local postgres driver
//   - WalReplicator: simulates WAL propagation from primary to replicas
//   - Handler: routes consensus Requests to Indicators
//   - DiscoveryNode: simulates an etcd watch stream for pooler membership
package simulation

import "github.com/multigres/multigres/go/common/consensus"

// MemStorage is an in-memory PoolerStorage implementation for tests.
// It simulates durable storage: Save writes to the struct field and Load reads
// it back, so a simulated crash-restart (which calls Restart() → storage.Load())
// correctly restores the last saved state.
type MemStorage struct {
	state consensus.PoolerPersistentState
}

// Save persists state to memory.
func (s *MemStorage) Save(state consensus.PoolerPersistentState) error {
	s.state = state
	return nil
}

// Load returns the last saved state.
func (s *MemStorage) Load() (consensus.PoolerPersistentState, error) {
	return s.state, nil
}
