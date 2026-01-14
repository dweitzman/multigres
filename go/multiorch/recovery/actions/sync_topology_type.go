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

package actions

import (
	"context"
	"log/slog"
	"time"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/topoclient"
	"github.com/multigres/multigres/go/multiorch/recovery/types"
	"github.com/multigres/multigres/go/multiorch/store"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
)

// SyncTopologyTypeAction syncs the topology Type field to match postgres reality.
// This is a defense-in-depth mechanism following the Vitess pattern.
// The primary mechanism for Type management is multiorch's AppointLeader,
// but this action can fix mismatches caused by bugs or network partitions.
//
// Compare-and-swap semantics: uses the pooler's own consensus term and checks
// if there's a PRIMARY in the shard with a higher term before making changes.
// This prevents overriding decisions made at higher terms.
type SyncTopologyTypeAction struct {
	poolerStore *store.PoolerHealthStore
	topoStore   topoclient.Store
	logger      *slog.Logger
	poolerTerm  int64 // The consensus term of the pooler being fixed (from its health state)
}

// NewSyncTopologyTypeAction creates a new sync topology type action.
func NewSyncTopologyTypeAction(
	poolerStore *store.PoolerHealthStore,
	topoStore topoclient.Store,
	logger *slog.Logger,
	poolerTerm int64,
) types.RecoveryAction {
	return &SyncTopologyTypeAction{
		poolerStore: poolerStore,
		topoStore:   topoStore,
		logger:      logger,
		poolerTerm:  poolerTerm,
	}
}

// Execute syncs the topology Type to match postgres role.
func (a *SyncTopologyTypeAction) Execute(ctx context.Context, problem types.Problem) error {
	poolerID := problem.PoolerID
	if poolerID == nil {
		return mterrors.Errorf(mtrpcpb.Code_INTERNAL, "pooler ID is nil")
	}

	a.logger.InfoContext(ctx, "Syncing topology type to match postgres role",
		"pooler", poolerID.Name,
		"problem", problem.Description,
		"current_term", a.poolerTerm)

	// Get current topology entry
	mpi, err := a.topoStore.GetMultiPooler(ctx, poolerID)
	if err != nil {
		return mterrors.Wrapf(err, "failed to get topology entry for pooler %s", poolerID.Name)
	}

	mp := mpi.MultiPooler
	currentTopologyType := mp.Type
	storedTerm := mp.PrimaryTerm

	// Get postgres role from health store to determine correct type
	// PoolerType in PoolerHealthState is what the pooler reports itself as (from Status RPC),
	// which reflects the actual postgres role (pg_is_in_recovery())
	poolerHealth, exists := a.poolerStore.Get(topoclient.MultiPoolerIDString(poolerID))
	if !exists {
		return mterrors.Errorf(mtrpcpb.Code_NOT_FOUND, "pooler not found in health store: %s", poolerID.Name)
	}

	// The "correct" type is what postgres actually is
	correctType := poolerHealth.PoolerType

	// If topology already matches, nothing to do
	if currentTopologyType == correctType {
		a.logger.InfoContext(ctx, "Topology type already matches postgres role, no action needed",
			"pooler", poolerID.Name,
			"type", correctType)
		return nil
	}

	// Handle REPLICA→PRIMARY case: The actual bug we saw (multipooler restart overwrote Type)
	// Need to check if there's already a PRIMARY in the shard with higher term
	if currentTopologyType == clustermetadatapb.PoolerType_REPLICA && correctType == clustermetadatapb.PoolerType_PRIMARY {
		return a.handleReplicaToPrimaryPromotion(ctx, mp, poolerID)
	}

	// Handle PRIMARY→REPLICA case: Simple demotion
	// CAS check: only update if our term >= stored term
	if storedTerm > a.poolerTerm {
		a.logger.WarnContext(ctx, "Cannot sync topology - stored term is higher than our term",
			"pooler", poolerID.Name,
			"stored_term", storedTerm,
			"current_term", a.poolerTerm,
			"stored_type", mp.Type,
			"correct_type", correctType)
		return mterrors.Errorf(mtrpcpb.Code_FAILED_PRECONDITION,
			"stored term %d is higher than current term %d - a more recent multiorch made this decision",
			storedTerm, a.poolerTerm)
	}

	// Update topology with CAS semantics
	_, err = a.topoStore.UpdateMultiPoolerFields(ctx, poolerID, func(mp *clustermetadatapb.MultiPooler) error {
		// Double-check term in the update callback (race protection)
		if mp.GetPrimaryTerm() > a.poolerTerm {
			return mterrors.Errorf(mtrpcpb.Code_FAILED_PRECONDITION,
				"stored term %d is higher than current term %d",
				mp.GetPrimaryTerm(), a.poolerTerm)
		}

		mp.Type = correctType
		// If promoting to PRIMARY, use current term
		// If demoting to REPLICA, clear term
		if correctType == clustermetadatapb.PoolerType_PRIMARY {
			mp.PrimaryTerm = a.poolerTerm
		} else {
			mp.PrimaryTerm = 0
		}
		return nil
	})
	if err != nil {
		return mterrors.Wrapf(err, "failed to update topology type for pooler %s", poolerID.Name)
	}

	a.logger.InfoContext(ctx, "Successfully synced topology type with CAS",
		"pooler", poolerID.Name,
		"old_type", currentTopologyType,
		"new_type", correctType,
		"old_term", storedTerm,
		"new_term", a.poolerTerm)

	return nil
}

// Metadata returns information about this recovery action.
func (a *SyncTopologyTypeAction) Metadata() types.RecoveryMetadata {
	return types.RecoveryMetadata{
		Name:        "SyncTopologyType",
		Description: "Sync topology Type field to match postgres role (with CAS)",
		Timeout:     30 * time.Second,
		LockTimeout: 10 * time.Second,
		Retryable:   true,
	}
}

// RequiresHealthyPrimary returns false - we can sync type even if primary is unhealthy.
// This is defense-in-depth: fixing type mismatches helps recovery converge.
func (a *SyncTopologyTypeAction) RequiresHealthyPrimary() bool {
	return false
}

// Priority returns the priority of this recovery action.
func (a *SyncTopologyTypeAction) Priority() types.Priority {
	return types.PriorityNormal
}

// handleReplicaToPrimaryPromotion handles the REPLICA→PRIMARY case (the actual bug we saw).
// When topology says REPLICA but postgres is PRIMARY, we need to check if there's already
// a PRIMARY in the shard with a higher term before promoting.
func (a *SyncTopologyTypeAction) handleReplicaToPrimaryPromotion(ctx context.Context, mp *clustermetadatapb.MultiPooler, poolerID *clustermetadatapb.ID) error {
	// Find all poolers in the same shard
	shardPoolers, err := a.topoStore.GetMultiPoolersByCell(ctx, poolerID.Cell, &topoclient.GetMultiPoolersByCellOptions{
		DatabaseShard: &topoclient.DatabaseShard{
			Database:   mp.Database,
			TableGroup: mp.TableGroup,
			Shard:      mp.Shard,
		},
	})
	if err != nil {
		return mterrors.Wrapf(err, "failed to query shard poolers for %s", poolerID.Name)
	}

	// Find any existing PRIMARY in the shard
	var existingPrimary *clustermetadatapb.MultiPooler
	var existingPrimaryTerm int64
	for _, poolerInfo := range shardPoolers {
		if poolerInfo.MultiPooler.Type == clustermetadatapb.PoolerType_PRIMARY {
			// Skip ourselves
			if poolerInfo.MultiPooler.Id.Name == poolerID.Name {
				continue
			}
			existingPrimary = poolerInfo.MultiPooler
			existingPrimaryTerm = poolerInfo.MultiPooler.PrimaryTerm
			break
		}
	}

	// Case 1: No existing PRIMARY → safe to promote (bootstrap or all-died case)
	if existingPrimary == nil {
		a.logger.InfoContext(ctx, "No existing PRIMARY in shard, safe to promote",
			"pooler", poolerID.Name,
			"shard", mp.Shard)

		// Update topology to PRIMARY with CAS check
		_, err = a.topoStore.UpdateMultiPoolerFields(ctx, poolerID, func(mp *clustermetadatapb.MultiPooler) error {
			// CAS: only promote if still REPLICA and term hasn't changed
			if mp.Type == clustermetadatapb.PoolerType_PRIMARY {
				// Already promoted by someone else - this is OK
				return nil
			}
			// Check stored term hasn't increased (someone else might have updated it)
			if mp.GetPrimaryTerm() > a.poolerTerm {
				return mterrors.Errorf(mtrpcpb.Code_FAILED_PRECONDITION,
					"stored term %d is higher than current term %d",
					mp.GetPrimaryTerm(), a.poolerTerm)
			}
			mp.Type = clustermetadatapb.PoolerType_PRIMARY
			mp.PrimaryTerm = a.poolerTerm
			return nil
		})
		if err != nil {
			return mterrors.Wrapf(err, "failed to promote pooler %s to PRIMARY", poolerID.Name)
		}

		a.logger.InfoContext(ctx, "Successfully promoted to PRIMARY with CAS",
			"pooler", poolerID.Name,
			"term", a.poolerTerm)
		return nil
	}

	// Case 2: Existing PRIMARY has higher term → refuse (higher authority made that decision)
	if existingPrimaryTerm > a.poolerTerm {
		a.logger.WarnContext(ctx, "Cannot promote - existing PRIMARY has higher term",
			"pooler", poolerID.Name,
			"existing_primary", existingPrimary.Id.Name,
			"existing_term", existingPrimaryTerm,
			"current_term", a.poolerTerm)
		return mterrors.Errorf(mtrpcpb.Code_FAILED_PRECONDITION,
			"existing PRIMARY %s has higher term %d than our term %d",
			existingPrimary.Id.Name, existingPrimaryTerm, a.poolerTerm)
	}

	// Case 3: Existing PRIMARY has equal/lower term → we have equal or higher authority
	// This is the actual bug case: multipooler restart overwrote Type field
	a.logger.InfoContext(ctx, "Existing PRIMARY has lower/equal term, safe to update topology",
		"pooler", poolerID.Name,
		"existing_primary", existingPrimary.Id.Name,
		"existing_term", existingPrimaryTerm,
		"current_term", a.poolerTerm)

	// Update this node to PRIMARY with CAS check
	_, err = a.topoStore.UpdateMultiPoolerFields(ctx, poolerID, func(mp *clustermetadatapb.MultiPooler) error {
		// CAS: only promote if still REPLICA and term hasn't changed
		if mp.Type == clustermetadatapb.PoolerType_PRIMARY {
			// Already promoted by someone else - this is OK
			return nil
		}
		// Check stored term hasn't increased (someone else might have updated it)
		if mp.GetPrimaryTerm() > a.poolerTerm {
			return mterrors.Errorf(mtrpcpb.Code_FAILED_PRECONDITION,
				"stored term %d is higher than current term %d",
				mp.GetPrimaryTerm(), a.poolerTerm)
		}
		mp.Type = clustermetadatapb.PoolerType_PRIMARY
		mp.PrimaryTerm = a.poolerTerm
		return nil
	})
	if err != nil {
		return mterrors.Wrapf(err, "failed to promote pooler %s to PRIMARY", poolerID.Name)
	}

	a.logger.InfoContext(ctx, "Successfully fixed topology type mismatch with CAS",
		"pooler", poolerID.Name,
		"old_type", clustermetadatapb.PoolerType_REPLICA,
		"new_type", clustermetadatapb.PoolerType_PRIMARY,
		"term", a.poolerTerm,
		"note", "multipooler restart bug fixed")

	return nil
}
