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
	"fmt"
	"log/slog"
	"time"

	"github.com/multigres/multigres/go/common/eventlog"
	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/rpcclient"
	"github.com/multigres/multigres/go/common/topoclient"
	commontypes "github.com/multigres/multigres/go/common/types"
	"github.com/multigres/multigres/go/services/multiorch/config"
	"github.com/multigres/multigres/go/services/multiorch/recovery/types"
	"github.com/multigres/multigres/go/services/multiorch/store"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	consensusdatapb "github.com/multigres/multigres/go/pb/consensusdata"
	multiorchdatapb "github.com/multigres/multigres/go/pb/multiorchdata"
)

// Compile-time assertion that DemoteStalePrimaryAction implements types.RecoveryAction.
var _ types.RecoveryAction = (*DemoteStalePrimaryAction)(nil)

// StalePrimaryDrainTimeout is a shorter drain timeout for stale primaries.
// Stale primaries that just came back online typically have no active connections,
// so we use a shorter timeout to speed up demotion.
const StalePrimaryDrainTimeout = 5 * time.Second

// DemoteStalePrimaryAction demotes a stale primary that was detected after failover.
// It uses the DemoteStalePrimary RPC with the correct primary's term to force the stale primary
// to accept the term and demote, preventing further writes.
type DemoteStalePrimaryAction struct {
	config      *config.Config
	rpcClient   rpcclient.MultiPoolerClient
	poolerStore *store.PoolerStore
	topoStore   topoclient.Store
	logger      *slog.Logger
}

// NewDemoteStalePrimaryAction creates a new action to demote a stale primary.
func NewDemoteStalePrimaryAction(
	cfg *config.Config,
	rpcClient rpcclient.MultiPoolerClient,
	poolerStore *store.PoolerStore,
	topoStore topoclient.Store,
	logger *slog.Logger,
) *DemoteStalePrimaryAction {
	return &DemoteStalePrimaryAction{
		config:      cfg,
		rpcClient:   rpcClient,
		poolerStore: poolerStore,
		topoStore:   topoStore,
		logger:      logger,
	}
}

func (a *DemoteStalePrimaryAction) Metadata() types.RecoveryMetadata {
	return types.RecoveryMetadata{
		Name:        "DemoteStalePrimary",
		Description: "Demote a stale primary that came back online after failover",
		Timeout:     60 * time.Second,
		LockTimeout: 15 * time.Second,
		Retryable:   true,
	}
}

func (a *DemoteStalePrimaryAction) Priority() types.Priority {
	return types.PriorityHigh
}

func (a *DemoteStalePrimaryAction) RequiresHealthyPrimary() bool {
	// We're demoting a primary, so we can't require a healthy primary
	return false
}

func (a *DemoteStalePrimaryAction) GracePeriod() *types.GracePeriodConfig {
	return &types.GracePeriodConfig{
		BaseDelay: a.config.GetPrimaryFailoverGracePeriodBase(),
		MaxJitter: a.config.GetPrimaryFailoverGracePeriodMaxJitter(),
	}
}

// Execute notifies the stale primary of the committed rule via Inform. The stale
// primary's Inform handler detects that the committed rule names a different primary
// and triggers autonomous recovery (stop → pg_rewind → restart as standby).
func (a *DemoteStalePrimaryAction) Execute(ctx context.Context, problem types.Problem) (retErr error) {
	poolerIDStr := topoclient.MultiPoolerIDString(problem.PoolerID)

	a.logger.InfoContext(ctx, "executing demote stale primary action",
		"shard_key", problem.ShardKey.String(),
		"stale_primary", poolerIDStr)

	stalePrimary, ok := a.poolerStore.Get(poolerIDStr)
	if !ok {
		return fmt.Errorf("stale primary %s not found in store", poolerIDStr)
	}

	correctPrimary, _, err := a.findCorrectPrimary(problem.ShardKey, poolerIDStr)
	if err != nil {
		return mterrors.Wrap(err, "failed to find correct primary")
	}

	// Get the committed rule from the correct primary's known state.
	rule := correctPrimary.ConsensusStatus.GetConsensusStatus().GetHighestKnownDecision()
	if rule == nil {
		rule = correctPrimary.ConsensusStatus.GetConsensusStatus().GetCurrentPosition().GetRule()
	}
	if rule == nil {
		return fmt.Errorf("no committed rule found for correct primary %s", correctPrimary.MultiPooler.Id.Name)
	}

	eventlog.Emit(ctx, a.logger, eventlog.Started, eventlog.PrimaryDemotion{NodeName: poolerIDStr, Reason: "stale"})
	defer func() {
		if retErr == nil {
			eventlog.Emit(ctx, a.logger, eventlog.Success, eventlog.PrimaryDemotion{NodeName: poolerIDStr, Reason: "stale"})
		} else {
			eventlog.Emit(ctx, a.logger, eventlog.Failed, eventlog.PrimaryDemotion{NodeName: poolerIDStr, Reason: "stale"}, "error", retErr)
		}
	}()

	a.logger.InfoContext(ctx, "sending Inform to stale primary",
		"stale_primary", poolerIDStr,
		"correct_primary", correctPrimary.MultiPooler.Id.Name)

	// Inform the stale primary of the committed rule. Its handler will detect
	// that the committed rule names a different primary and trigger recovery.
	_, err = a.rpcClient.Inform(ctx, stalePrimary.MultiPooler, &consensusdatapb.InformRequest{
		Rule: rule,
	})
	if err != nil {
		return mterrors.Wrap(err, "Inform RPC failed")
	}

	a.logger.InfoContext(ctx, "demote stale primary action completed",
		"shard_key", problem.ShardKey.String(),
		"demoted_primary", poolerIDStr)

	return nil
}

// findCorrectPrimary finds the correct primary in the shard and returns it along with its term.
// The correct primary is the one with the highest PrimaryTerm.
func (a *DemoteStalePrimaryAction) findCorrectPrimary(shardKey commontypes.ShardKey, stalePrimaryIDStr string) (*multiorchdatapb.PoolerHealthState, int64, error) {
	var correctPrimary *multiorchdatapb.PoolerHealthState
	var maxPrimaryTerm int64

	// Iterate through all poolers to find the correct primary
	a.poolerStore.Range(func(key string, pooler *multiorchdatapb.PoolerHealthState) bool {
		if pooler == nil || pooler.MultiPooler == nil || pooler.MultiPooler.Id == nil {
			return true // continue
		}

		// Only consider poolers in the same shard
		if pooler.MultiPooler.Database != shardKey.Database ||
			pooler.MultiPooler.TableGroup != shardKey.TableGroup ||
			pooler.MultiPooler.Shard != shardKey.Shard {
			return true // continue
		}

		poolerIDStr := topoclient.MultiPoolerIDString(pooler.MultiPooler.Id)

		// Skip the stale primary
		if poolerIDStr == stalePrimaryIDStr {
			return true // continue
		}

		// Check if this pooler is a PRIMARY
		poolerType := pooler.GetStatus().GetPoolerType()
		if poolerType == clustermetadatapb.PoolerType_UNKNOWN && pooler.MultiPooler != nil {
			poolerType = pooler.MultiPooler.Type
		}

		if poolerType == clustermetadatapb.PoolerType_PRIMARY {
			primaryTerm := pooler.ConsensusStatus.GetConsensusStatus().GetTermRevocation().GetRevokedBelowTerm()
			if primaryTerm > maxPrimaryTerm {
				maxPrimaryTerm = primaryTerm
				correctPrimary = pooler
			}
		}

		return true // continue
	})

	if correctPrimary == nil {
		return nil, 0, fmt.Errorf("no correct primary found in shard %s", shardKey.String())
	}

	consensusTerm := correctPrimary.ConsensusStatus.GetConsensusStatus().GetTermRevocation().GetRevokedBelowTerm()

	return correctPrimary, consensusTerm, nil
}
