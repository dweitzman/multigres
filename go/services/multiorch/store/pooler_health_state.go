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
	"google.golang.org/protobuf/proto"

	"github.com/multigres/multigres/go/common/consensus"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multiorchdata "github.com/multigres/multigres/go/pb/multiorchdata"
)

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

// EtcdLeaderObservation returns the pooler's etcd-published view of who
// the consensus leader is. Non-nil only when this pooler currently
// considers itself the leader of its shard (replicas leave the field
// nil); see multipooler/manager/state_manager.go for the publication
// rule.
func EtcdLeaderObservation(p *multiorchdata.PoolerHealthState) *clustermetadatapb.LeaderObservation {
	return p.GetMultiPooler().GetCurrentLeadership()
}

// HealthLeaderObservation returns the pooler's health-stream view of
// who the consensus leader is, adapted from its ConsensusStatus snapshot.
// Distinct from EtcdLeaderObservation in source: this one came from a
// live RPC, that one came from etcd. Returns nil if the pooler has not
// reported a consensus rule yet.
func HealthLeaderObservation(p *multiorchdata.PoolerHealthState) *clustermetadatapb.LeaderObservation {
	rule := p.GetConsensusStatus().GetReplicationPrimary().GetRule()
	if rule == nil {
		return nil
	}
	return &clustermetadatapb.LeaderObservation{
		LeaderId:         rule.GetLeaderId(),
		LeaderRuleNumber: rule.GetRuleNumber(),
	}
}

// ShardLeader identifies the cluster-wide consensus leader for the
// shard from the given poolers' observations, and returns both the
// authoritative LeaderObservation and the matching pooler (if known).
//
// The observation is the highest-numbered across every pooler's
// etcd-published CurrentLeadership and health-stream ConsensusStatus
// — analogous to the gateway's `leaders[shard]` cache. The per-pooler
// accessors above answer the narrower "what does this pooler think?"
// question.
//
// Returns (nil, nil) if no pooler has reported an observation. Returns
// (obs, nil) if the observation names a pooler that isn't in the slice
// — possible when the leader has been observed via a peer's health
// stream but its own record hasn't been discovered yet (or has been
// removed).
func ShardLeader(poolers []*multiorchdata.PoolerHealthState) (*clustermetadatapb.LeaderObservation, *multiorchdata.PoolerHealthState) {
	obs := make([]*clustermetadatapb.LeaderObservation, 0, 2*len(poolers))
	for _, p := range poolers {
		obs = append(obs, EtcdLeaderObservation(p), HealthLeaderObservation(p))
	}
	best := consensus.MostAuthoritativeObservation(obs...)
	if best == nil {
		return nil, nil
	}
	for _, p := range poolers {
		if proto.Equal(p.GetMultiPooler().GetId(), best.GetLeaderId()) {
			return best, p
		}
	}
	return best, nil
}
