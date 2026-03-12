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

package store

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/rpcclient"
	commontypes "github.com/multigres/multigres/go/common/types"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	multiorchdatapb "github.com/multigres/multigres/go/pb/multiorchdata"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
)

// poolerOpState holds per-pooler operational metadata that is tracked in memory
// alongside health state but is not part of the persisted proto.
type poolerOpState struct {
	lastPollAttempt time.Time
}

// PoolerStore extends PoolerHealthStore with RPC-based domain queries and
// per-pooler operational state. It is used where both health state and live
// RPC calls are needed. For components that only need health state, prefer
// passing .Health() directly.
type PoolerStore struct {
	health    *PoolerHealthStore
	opState   map[string]*poolerOpState
	opStateMu sync.Mutex
	rpcClient rpcclient.MultiPoolerClient
	logger    *slog.Logger
}

// NewPoolerStore creates a new PoolerStore.
// rpcClient and logger are used by FindHealthyPrimary; they may be nil in tests
// that do not exercise that method.
func NewPoolerStore(rpcClient rpcclient.MultiPoolerClient, logger *slog.Logger) *PoolerStore {
	return &PoolerStore{
		health:    NewPoolerHealthStore(),
		opState:   make(map[string]*poolerOpState),
		rpcClient: rpcClient,
		logger:    logger,
	}
}

// Health returns the underlying PoolerHealthStore for passing to components
// that only need health state (generators, most recovery actions).
func (s *PoolerStore) Health() *PoolerHealthStore {
	return s.health
}

// Get retrieves a pooler's health state by its ID string.
// Returns a deep clone safe to mutate, and false if the key does not exist.
func (s *PoolerStore) Get(poolerID string) (*multiorchdatapb.PoolerHealthState, bool) {
	return s.health.Get(poolerID)
}

// Set stores a deep clone of the pooler health state.
func (s *PoolerStore) Set(poolerID string, state *multiorchdatapb.PoolerHealthState) {
	s.health.set(poolerID, state)
}

// Delete removes a pooler from the store, including its operational state.
// Returns true if the pooler existed.
func (s *PoolerStore) Delete(poolerID string) bool {
	s.opStateMu.Lock()
	delete(s.opState, poolerID)
	s.opStateMu.Unlock()
	return s.health.delete(poolerID)
}

// RecordPollAttempt records that a health poll was attempted for this pooler right now.
func (s *PoolerStore) RecordPollAttempt(poolerID string) {
	s.opStateMu.Lock()
	defer s.opStateMu.Unlock()
	op := s.opState[poolerID]
	if op == nil {
		op = &poolerOpState{}
		s.opState[poolerID] = op
	}
	op.lastPollAttempt = time.Now()
}

// WasRecentlyPolled returns true if a poll attempt was recorded for this pooler
// within the given interval.
func (s *PoolerStore) WasRecentlyPolled(poolerID string, interval time.Duration) bool {
	s.opStateMu.Lock()
	defer s.opStateMu.Unlock()
	if op, ok := s.opState[poolerID]; ok {
		return time.Since(op.lastPollAttempt) < interval
	}
	return false
}

// Len returns the number of poolers in the store.
func (s *PoolerStore) Len() int {
	return s.health.Len()
}

// Range iterates over all poolers. Each value passed to the callback is a deep
// clone safe to mutate. Iteration stops early if the callback returns false.
func (s *PoolerStore) Range(fn func(key string, value *multiorchdatapb.PoolerHealthState) bool) {
	s.health.Range(fn)
}

// FindPoolersInShard returns all poolers belonging to the given shard.
func (s *PoolerStore) FindPoolersInShard(shardKey commontypes.ShardKey) []*multiorchdatapb.PoolerHealthState {
	var poolers []*multiorchdatapb.PoolerHealthState

	s.health.Range(func(_ string, pooler *multiorchdatapb.PoolerHealthState) bool {
		if pooler == nil || pooler.MultiPooler == nil || pooler.MultiPooler.Id == nil {
			return true // continue
		}

		if pooler.MultiPooler.Database == shardKey.Database &&
			pooler.MultiPooler.TableGroup == shardKey.TableGroup &&
			pooler.MultiPooler.Shard == shardKey.Shard {
			poolers = append(poolers, pooler)
		}

		return true // continue
	})

	return poolers
}

// FindPoolerByID finds a pooler in the store by its cell and name.
func (s *PoolerStore) FindPoolerByID(id *clustermetadatapb.ID) (*multiorchdatapb.PoolerHealthState, error) {
	var found *multiorchdatapb.PoolerHealthState

	s.health.Range(func(_ string, pooler *multiorchdatapb.PoolerHealthState) bool {
		if pooler == nil || pooler.MultiPooler == nil || pooler.MultiPooler.Id == nil {
			return true // continue
		}

		if pooler.MultiPooler.Id.Name == id.Name &&
			pooler.MultiPooler.Id.Cell == id.Cell {
			found = pooler
			return false // stop iteration
		}

		return true // continue
	})

	if found == nil {
		return nil, mterrors.Errorf(mtrpcpb.Code_NOT_FOUND,
			"pooler %s/%s not found", id.Cell, id.Name)
	}

	return found, nil
}

// FindHealthyPrimary finds a healthy, initialized primary in the given pooler slice.
// It verifies health by making an RPC call to each candidate.
// Returns an error if multiple primaries are found (likely a stale primary that needs to be demoted).
func (s *PoolerStore) FindHealthyPrimary(
	ctx context.Context,
	poolers []*multiorchdatapb.PoolerHealthState,
) (*multiorchdatapb.PoolerHealthState, error) {
	var healthyPrimary *multiorchdatapb.PoolerHealthState

	for _, pooler := range poolers {
		if pooler.MultiPooler == nil ||
			pooler.MultiPooler.Type != clustermetadatapb.PoolerType_PRIMARY {
			continue
		}

		// Verify it's actually reachable and healthy via RPC
		statusResp, err := s.rpcClient.Status(ctx, pooler.MultiPooler,
			&multipoolermanagerdatapb.StatusRequest{})
		if err != nil {
			s.logger.WarnContext(ctx, "primary unreachable during health check",
				"pooler", pooler.MultiPooler.Id.Name,
				"error", err)
			continue
		}

		if statusResp == nil || statusResp.Status == nil {
			s.logger.WarnContext(ctx, "primary returned nil status",
				"pooler", pooler.MultiPooler.Id.Name)
			continue
		}

		if statusResp.Status.IsInitialized {
			if healthyPrimary != nil {
				return nil, mterrors.Errorf(mtrpcpb.Code_FAILED_PRECONDITION,
					"multiple primaries found: %s and %s (stale primary needs demotion)",
					healthyPrimary.MultiPooler.Id.Name, pooler.MultiPooler.Id.Name)
			}
			healthyPrimary = pooler
		}
	}

	if healthyPrimary == nil {
		return nil, mterrors.Errorf(mtrpcpb.Code_FAILED_PRECONDITION,
			"no healthy primary found")
	}

	return healthyPrimary, nil
}
