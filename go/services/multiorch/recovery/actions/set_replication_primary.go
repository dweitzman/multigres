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
	"github.com/multigres/multigres/go/services/multiorch/config"
	"github.com/multigres/multigres/go/services/multiorch/recovery/types"
	"github.com/multigres/multigres/go/services/multiorch/store"

	consensusdatapb "github.com/multigres/multigres/go/pb/consensusdata"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	multiorchdatapb "github.com/multigres/multigres/go/pb/multiorchdata"
)

// Compile-time assertion that SetReplicationPrimaryAction implements types.RecoveryAction.
var _ types.RecoveryAction = (*SetReplicationPrimaryAction)(nil)

// SetReplicationPrimaryAction tells one pooler about the current cluster
// leader via SetTermPrimary. The action does exactly that — no
// post-mutation streaming poll, no pg_rewind fallback. Divergent-replica
// recovery (rewind, eventual drain) is owned by the pooler's postgres
// monitor (see remedialActionSelfDrain TODO in multipooler manager).
//
// Detection of "pooler has been pointed at the right primary for a while
// but still isn't streaming" belongs to a future StuckReplicationAnalyzer.
// The intent there is for the pooler itself to track when WAL replication
// last advanced (so a healthy stream that then stops starts the clock at
// the stop, not at orch's first observation) and surface that timestamp
// in its health snapshot. Orch reads the timestamp and emits an
// observational problem after a threshold; remediation stays inside the
// pooler.
//
// The pooler-side SetTermPrimary handler dispatches based on its
// postgres recovery state:
//   - postgres in standby mode → ALTER SYSTEM SET primary_conninfo,
//     restart WAL receiver against the new leader
//   - postgres in primary mode (stale self-claim) → pg_rewind against
//     the new leader, restart as standby, clear sync config, update
//     topology
//
// Same RPC, same arguments — only the receiving pooler's branch differs.
//
// Addressed problem codes:
//   - ProblemReplicaNotReplicating: replica's ReplicationPrimary is unset
//     or names a non-leader
//   - ProblemStaleLeader: pooler still self-claims at an outdated rule
//
// Cohort membership (adding/removing the replica from the primary's
// synchronous standby list) is managed separately by ReconcileCohortAction.
//
// Idempotency:
// SetTermPrimary is idempotent at the pooler — sending it twice with
// the same rule no-ops the second time. Concurrent multiorch instances
// racing on the same problem produce identical end state.
type SetReplicationPrimaryAction struct {
	config      *config.Config
	rpcClient   rpcclient.MultiPoolerClient
	poolerStore *store.PoolerStore
	topoStore   topoclient.Store
	logger      *slog.Logger
}

// NewSetReplicationPrimaryAction creates a new action.
func NewSetReplicationPrimaryAction(
	cfg *config.Config,
	rpcClient rpcclient.MultiPoolerClient,
	poolerStore *store.PoolerStore,
	topoStore topoclient.Store,
	logger *slog.Logger,
) *SetReplicationPrimaryAction {
	return &SetReplicationPrimaryAction{
		config:      cfg,
		rpcClient:   rpcClient,
		poolerStore: poolerStore,
		topoStore:   topoStore,
		logger:      logger,
	}
}

// Execute retargets the affected pooler at the current cluster leader.
//
// The problem already identifies the target pooler. The leader to point
// it at comes from store.ShardLeader over the same in-store health
// snapshot the analyzer used — no additional Status RPC is needed to
// "verify the primary." SetTermPrimary is idempotent on the receiving
// pooler (it no-ops when the incoming rule isn't higher than the local
// rule), so we also skip any pre-flight verification of the target's
// current state.
func (a *SetReplicationPrimaryAction) Execute(ctx context.Context, problem types.Problem) error {
	a.logger.InfoContext(ctx, "executing set replication primary action",
		"shard_key", problem.ShardKey.String(),
		"pooler", problem.PoolerID.Name,
		"problem_code", string(problem.Code))

	switch problem.Code {
	case types.ProblemReplicaNotReplicating, types.ProblemStaleLeader:
		// Both reduce to "tell this pooler about the current leader". The
		// pooler-side SetTermPrimary handler dispatches between the standby
		// branch and the stale-primary demote branch.
	default:
		return mterrors.Errorf(mtrpcpb.Code_INVALID_ARGUMENT,
			"unsupported problem code for SetReplicationPrimary: %s", problem.Code)
	}

	target, err := a.poolerStore.FindPoolerByID(problem.PoolerID)
	if err != nil {
		return mterrors.Wrap(err, "failed to find affected pooler")
	}

	poolers := a.poolerStore.FindPoolersInShard(problem.ShardKey)
	if len(poolers) == 0 {
		return fmt.Errorf("no poolers found for shard %s", problem.ShardKey)
	}
	obs, leader := store.ShardLeader(poolers)
	if obs.GetLeaderId() == nil {
		return mterrors.Errorf(mtrpcpb.Code_FAILED_PRECONDITION,
			"no consensus leader observed across known poolers")
	}
	if leader == nil {
		return mterrors.Errorf(mtrpcpb.Code_FAILED_PRECONDITION,
			"consensus leader %s not present in known poolers", obs.GetLeaderId().GetName())
	}

	a.logger.InfoContext(ctx, "retargeting at cluster leader",
		"leader", leader.MultiPooler.Id.Name,
		"target", target.MultiPooler.Id.Name)

	return a.retargetReplication(ctx, target, leader, problem.Code)
}

// retargetReplication informs `target` about the current cluster leader
// via SetTermPrimary. Returns when the pooler acknowledges; doesn't wait
// for replication to actually start streaming.
func (a *SetReplicationPrimaryAction) retargetReplication(
	ctx context.Context,
	target *multiorchdatapb.PoolerHealthState,
	primary *multiorchdatapb.PoolerHealthState,
	problemCode types.ProblemCode,
) (retErr error) {
	a.logger.InfoContext(ctx, "retargeting replication via SetTermPrimary",
		"target", target.MultiPooler.Id.Name,
		"primary", primary.MultiPooler.Id.Name,
		"problem_code", string(problemCode))

	// Event shape depends on the problem we're addressing: a replica being
	// retargeted is a NodeJoin (new/recovering follower), a stale primary
	// being demoted is a PrimaryDemotion. Integration tests assert on these
	// event types to confirm which recovery path ran.
	var startEvent, successEvent, failEvent eventlog.Event
	if problemCode == types.ProblemStaleLeader {
		evt := eventlog.PrimaryDemotion{NodeName: target.MultiPooler.Id.Name, Reason: "stale"}
		startEvent, successEvent, failEvent = evt, evt, evt
	} else {
		evt := eventlog.NodeJoin{NodeName: target.MultiPooler.Id.Name}
		startEvent, successEvent, failEvent = evt, evt, evt
	}
	eventlog.Emit(ctx, a.logger, eventlog.Started, startEvent)
	defer func() {
		if retErr == nil {
			eventlog.Emit(ctx, a.logger, eventlog.Success, successEvent)
		} else {
			eventlog.Emit(ctx, a.logger, eventlog.Failed, failEvent, "error", retErr)
		}
	}()

	informReq := &consensusdatapb.SetTermPrimaryRequest{
		Leader: topoclient.PoolerAddressFor(primary.MultiPooler),
		Rule:   primary.GetConsensusStatus().GetCurrentPosition().GetRule(),
	}
	if _, err := a.rpcClient.SetTermPrimary(ctx, target.MultiPooler, informReq); err != nil {
		return mterrors.Wrap(err, "SetTermPrimary RPC failed")
	}

	a.logger.InfoContext(ctx, "SetReplicationPrimary completed successfully",
		"target", target.MultiPooler.Id.Name,
		"primary", primary.MultiPooler.Id.Name)
	return nil
}

// RecoveryAction interface implementation

func (a *SetReplicationPrimaryAction) RequiresHealthyLeader() bool {
	return true // Cannot point a pooler at a leader without one
}

func (a *SetReplicationPrimaryAction) Metadata() types.RecoveryMetadata {
	return types.RecoveryMetadata{
		Name:        "SetReplicationPrimary",
		Description: "Inform a pooler about the current cluster leader",
		Timeout:     15 * time.Second,
		LockTimeout: 15 * time.Second,
		Retryable:   true,
	}
}

func (a *SetReplicationPrimaryAction) Priority() types.Priority {
	return types.PriorityHigh
}

func (a *SetReplicationPrimaryAction) GracePeriod() *types.GracePeriodConfig {
	return nil
}
