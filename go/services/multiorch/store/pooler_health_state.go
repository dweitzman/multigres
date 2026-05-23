// Copyright 2025 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package store

import (
	"time"

	multiorchdata "github.com/multigres/multigres/go/pb/multiorchdata"
)

// PoolerLikelyUnreachable reports whether the coordinator has been out of
// contact with a pooler for long enough to treat it as unreachable. It is a
// pure staleness check against PoolerHealthState.LastSeen — intentionally
// NOT consulting IsLastCheckValid, which flips to false instantly on any
// health-stream disconnect including the brief window of an ordinary network
// blip. Decision sites (cohort exclusion, failover trigger, AppointLeader
// pre-check, etc.) should ALWAYS go through this helper rather than reading
// IsLastCheckValid directly.
//
// A missing LastSeen timestamp also returns true (we have no positive
// evidence the pooler exists). threshold is the maximum staleness tolerated
// before declaring the pooler unreachable; callers pick a value that matches
// their tolerance for transient flaps.
//
// TODO: audit existing IsLastCheckValid consumers (appoint_leader.go,
// generator.go, leader_appointment.go, store.IsInitialized, etc.) and route
// them through this helper too. They were all written before the debounce
// concern surfaced and are similarly vulnerable to stream-blip noise.
func PoolerLikelyUnreachable(p *multiorchdata.PoolerHealthState, threshold time.Duration) bool {
	if p == nil {
		return true
	}
	last := p.GetLastSeen()
	if last == nil {
		return true
	}
	return time.Since(last.AsTime()) > threshold
}

// poolerHealthStore is a thread-safe store for pooler health state.
// It provides clone-on-read/write semantics so callers always work with
// isolated copies, preventing concurrent mutation of shared state.
type poolerHealthStore struct {
	proto *ProtoStore[string, *multiorchdata.PoolerHealthState]
}

// newPoolerHealthStore creates a new store for pooler health state.
func newPoolerHealthStore() *poolerHealthStore {
	return &poolerHealthStore{
		proto: NewProtoStore[string, *multiorchdata.PoolerHealthState](),
	}
}

// get retrieves a pooler's health state by its ID string.
func (s *poolerHealthStore) get(poolerID string) (*multiorchdata.PoolerHealthState, bool) {
	return s.proto.Get(poolerID)
}

// set stores a deep clone of the pooler health state.
func (s *poolerHealthStore) set(poolerID string, state *multiorchdata.PoolerHealthState) {
	s.proto.Set(poolerID, state)
}

// delete removes a pooler from the store. Returns true if the pooler existed.
func (s *poolerHealthStore) delete(poolerID string) bool {
	return s.proto.Delete(poolerID)
}

// len returns the number of poolers in the store.
func (s *poolerHealthStore) len() int {
	return s.proto.Len()
}

// range iterates over all poolers. Each value passed to the callback is a deep
// clone safe to mutate. Iteration stops early if the callback returns false.
func (s *poolerHealthStore) rangeHealth(fn func(key string, value *multiorchdata.PoolerHealthState) bool) {
	s.proto.Range(fn)
}

// doUpdateRange iterates over all poolers while holding the lock and allows
// in-place updates. See ProtoStore.DoUpdateRange for full semantics.
func (s *poolerHealthStore) doUpdateRange(fn func(key string, value *multiorchdata.PoolerHealthState) (*multiorchdata.PoolerHealthState, bool)) {
	s.proto.DoUpdateRange(fn)
}

// doUpdate performs an atomic read-modify-write on a pooler's health state.
// See ProtoStore.DoUpdate for full semantics.
func (s *poolerHealthStore) doUpdate(key string, fn func(value *multiorchdata.PoolerHealthState) *multiorchdata.PoolerHealthState) {
	s.proto.DoUpdate(key, fn)
}

// findPoolersInShard returns all poolers belonging to the given shard.
// IsInitialized returns true if the pooler has been initialized.
// A pooler is considered initialized based on the IsInitialized field from
// the Status RPC, which is determined by the data directory state (not LSN).
// The node must also be reachable for us to trust this information.
func IsInitialized(p *multiorchdata.PoolerHealthState) bool {
	if !p.IsLastCheckValid {
		return false // unreachable nodes are considered uninitialized
	}

	if p.MultiPooler == nil {
		return false
	}

	// Use the IsInitialized field from Status RPC directly.
	// This is based on data directory state, not LSN.
	return p.GetStatus().GetIsInitialized()
}
