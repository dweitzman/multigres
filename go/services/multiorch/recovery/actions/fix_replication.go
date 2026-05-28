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
	"github.com/multigres/multigres/go/services/multiorch/config"
	"github.com/multigres/multigres/go/services/multiorch/recovery/types"
	"github.com/multigres/multigres/go/services/multiorch/store"

	consensusdatapb "github.com/multigres/multigres/go/pb/consensusdata"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	multiorchdatapb "github.com/multigres/multigres/go/pb/multiorchdata"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
)

// Compile-time assertion that FixReplicationAction implements types.RecoveryAction.
var _ types.RecoveryAction = (*FixReplicationAction)(nil)

// FixReplicationAction retargets a pooler at the current cluster leader via
// SetTermPrimary. The pooler-side handler dispatches based on its postgres
// recovery state:
//   - postgres in standby mode → ALTER SYSTEM SET primary_conninfo, restart
//     WAL receiver against the new leader
//   - postgres in primary mode (stale self-claim) → pg_rewind against the
//     new leader, restart as standby, clear sync config, update topology
//
// Same RPC, same arguments — only the receiving pooler's branch differs.
//
// Addressed problem codes:
//   - ProblemReplicaNotReplicating: replica has no primary_conninfo
//   - ProblemStaleLeader: pooler still self-claims at an outdated rule
//
// Future problem codes (TODO):
//   - ProblemReplicaWrongPrimary: replica pointed at the wrong leader
//   - ProblemReplicaLagging: replication configured but lag is excessive
//
// Cohort membership (adding/removing the replica from the primary's
// synchronous standby list) is managed separately by ReconcileCohortAction.
//
// Idempotency:
// This action is fully idempotent. If multiple multiorch instances race to fix
// the same problem, the end result will be identical. The underlying RPC
// operations (SetTermPrimary) are implemented as idempotent operations
// at the pooler level and serialized by action locks on the poolers, so
// concurrent calls are safe and produce the same final state.

// Default polling parameters for verifyReplicationStarted.
const (
	DefaultVerifyMaxAttempts  = 10
	DefaultVerifyPollInterval = 500 * time.Millisecond
)

type FixReplicationAction struct {
	config      *config.Config
	rpcClient   rpcclient.MultiPoolerClient
	poolerStore *store.PoolerStore
	topoStore   topoclient.Store
	logger      *slog.Logger
	rewind      *RewindAction

	// Polling parameters for verifyReplicationStarted.
	verifyMaxAttempts  int
	verifyPollInterval time.Duration
}

// NewFixReplicationAction creates a new fix replication action.
func NewFixReplicationAction(
	cfg *config.Config,
	rpcClient rpcclient.MultiPoolerClient,
	poolerStore *store.PoolerStore,
	topoStore topoclient.Store,
	logger *slog.Logger,
) *FixReplicationAction {
	maxAttempts := DefaultVerifyMaxAttempts
	pollInterval := DefaultVerifyPollInterval
	if cfg != nil {
		timeout := cfg.GetVerifyReplicationTimeout()
		if timeout > 0 {
			maxAttempts = max(int(timeout/DefaultVerifyPollInterval), 1)
		}
	}
	return &FixReplicationAction{
		config:             cfg,
		rpcClient:          rpcClient,
		poolerStore:        poolerStore,
		topoStore:          topoStore,
		logger:             logger,
		rewind:             NewRewindAction(cfg, rpcClient, poolerStore, logger),
		verifyMaxAttempts:  maxAttempts,
		verifyPollInterval: pollInterval,
	}
}

// Execute retargets the affected pooler at the current cluster leader.
//
// The problem already identifies the target pooler. The leader to point it
// at comes from store.ShardLeader over the same in-store health snapshot
// the analyzer used — no additional Status RPC is needed to "verify the
// primary." SetTermPrimary is idempotent on the receiving pooler (it
// no-ops when the incoming rule isn't higher than the local rule), so we
// also skip any pre-flight verification of the target's current state.
func (a *FixReplicationAction) Execute(ctx context.Context, problem types.Problem) error {
	a.logger.InfoContext(ctx, "executing fix replication action",
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
			"unsupported problem code for fix replication: %s", problem.Code)
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

// retargetReplication informs `target` about the current cluster leader via
// SetTermPrimary. The pooler-side handler dispatches between the standby and
// stale-primary branches; both paths end with postgres streaming from
// `primary`. We then poll the WAL receiver to confirm streaming actually
// started. If it didn't (replica path only) and the target's WAL has diverged,
// fall back to pg_rewind — but never for a stale-leader demote, since
// SetTermPrimary already runs pg_rewind inside demoteStalePrimaryLocked and
// re-running it from here would be racy and redundant.
func (a *FixReplicationAction) retargetReplication(
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

	// Verify replication started
	err := a.verifyReplicationStarted(ctx, target)
	if err == nil {
		a.logger.InfoContext(ctx, "fix replication action completed successfully",
			"target", target.MultiPooler.Id.Name,
			"primary", primary.MultiPooler.Id.Name)
		return nil
	}

	a.logger.WarnContext(ctx, "replication did not start after configuration",
		"target", target.MultiPooler.Id.Name,
		"primary", primary.MultiPooler.Id.Name)

	if problemCode == types.ProblemStaleLeader {
		// Stale-primary demote already ran pg_rewind inside the pooler's
		// SetTermPrimary handler. If WAL receiver still isn't streaming,
		// something else is wrong (e.g. primary postgres went away mid-demote);
		// return the verify error so the next cycle re-detects.
		return mterrors.Wrap(err, "replication did not start after stale-leader demote")
	}

	// TODO: delete the rewind fallback below once the postgres monitor
	// self-rewinds on prolonged inability to replicate (see
	// remedialActionSelfDrain TODO in multipooler manager and the related
	// pass-1 plan to add remedialActionSelfRewind triggered by
	// rewindPending=true). The monitor has the freshest local view of WAL
	// receiver state; orch shouldn't be in the loop for this. Once the
	// monitor handles it, RewindAction loses its only caller and can
	// either be deleted or kept as an admin escape hatch.

	// Re-check the primary's latest health-stream state before running pg_rewind.
	// pg_rewind stops the target's postgres before contacting the source; if the
	// primary postgres is no longer running the stop will leave two nodes down.
	// Return an error for retry — the next cycle will detect PrimaryIsDead.
	primaryKey := topoclient.MultiPoolerIDString(primary.MultiPooler.Id)
	if latest, ok := a.poolerStore.Get(primaryKey); !ok || !latest.GetStatus().GetPostgresReady() {
		return mterrors.Errorf(mtrpcpb.Code_UNAVAILABLE,
			"primary postgres not running, skipping pg_rewind to avoid leaving two nodes down")
	}

	if rewindErr := a.rewind.Run(ctx, primary, target); rewindErr != nil {
		return mterrors.Wrap(rewindErr, "pg_rewind failed")
	}
	// Re-verify replication after rewind. RewindToSource restarts
	// PostgreSQL as a standby, and primary_conninfo in
	// postgresql.auto.conf is preserved (pg_rewind doesn't touch it).
	if verifyErr := a.verifyReplicationStarted(ctx, target); verifyErr != nil {
		return mterrors.Wrap(verifyErr, "replication did not start after pg_rewind")
	}

	// Cohort membership (adding the replica to synchronous_standby_names) is
	// managed by ReconcileCohortAction separately. By the time this action
	// returns, the target is replicating; the cohort analyzer will pick it up
	// on the next cycle and propose adding it to the cohort.

	a.logger.InfoContext(ctx, "fix replication action completed successfully",
		"target", target.MultiPooler.Id.Name,
		"primary", primary.MultiPooler.Id.Name)
	return nil
}

// verifyReplicationStarted checks that replication is actively streaming.
// It polls a few times to allow the WAL receiver to connect.
func (a *FixReplicationAction) verifyReplicationStarted(ctx context.Context, replica *multiorchdatapb.PoolerHealthState) error {
	ticker := time.NewTicker(a.verifyPollInterval)
	defer ticker.Stop()

	var lastErr error
	for attempt := 1; attempt <= a.verifyMaxAttempts; attempt++ {
		select {
		case <-ctx.Done():
			return mterrors.Wrap(ctx.Err(), "context cancelled while verifying replication")
		case <-ticker.C:
		}

		statusResp, err := a.rpcClient.Status(ctx, replica.MultiPooler, &multipoolermanagerdatapb.StatusRequest{})
		if err != nil {
			lastErr = mterrors.Wrap(err, "failed to get replication status after fix")
			continue
		}

		var status *multipoolermanagerdatapb.StandbyReplicationStatus
		if statusResp.Status != nil {
			status = statusResp.Status.ReplicationStatus
		}
		if status == nil {
			lastErr = mterrors.Errorf(mtrpcpb.Code_INTERNAL, "no replication status returned")
			continue
		}

		// Check WAL receiver status first - this is the live connection state
		if status.WalReceiverStatus != "streaming" {
			lastErr = mterrors.Errorf(mtrpcpb.Code_INTERNAL,
				"WAL receiver not streaming (status: %s)", status.WalReceiverStatus)
			continue
		}

		// Also verify we have a receive LSN (sanity check)
		if status.LastReceiveLsn == "" {
			lastErr = mterrors.Errorf(mtrpcpb.Code_INTERNAL,
				"WAL receiver streaming but no receive LSN")
			continue
		}

		a.logger.InfoContext(ctx, "verified replication is streaming",
			"replica", replica.MultiPooler.Id.Name,
			"wal_receiver_status", status.WalReceiverStatus,
			"last_receive_lsn", status.LastReceiveLsn,
			"last_replay_lsn", status.LastReplayLsn)

		return nil
	}

	return mterrors.Wrap(lastErr, "replication did not start after polling")
}

// RecoveryAction interface implementation

func (a *FixReplicationAction) RequiresHealthyLeader() bool {
	return true // Cannot fix replica replication without a healthy primary
}

func (a *FixReplicationAction) Metadata() types.RecoveryMetadata {
	return types.RecoveryMetadata{
		Name:        "FixReplication",
		Description: "Configure or repair replication on a replica",
		Timeout:     45 * time.Second,
		LockTimeout: 15 * time.Second,
		Retryable:   true,
	}
}

func (a *FixReplicationAction) Priority() types.Priority {
	return types.PriorityHigh
}

func (a *FixReplicationAction) GracePeriod() *types.GracePeriodConfig {
	// No grace period needed, execute immediately
	return nil
}

// =============================================================================
// TODO: Future replication problem handlers
// =============================================================================
//
// The following are replication problems we should handle in future PRs:
//
// ProblemReplicaWrongPrimary
//    - Replica is connected to a stale primary (e.g., after failover)
//    - Fix: Update primary_conninfo to point to new primary, restart streaming
//    - Consider: We need to handle timeline changes.
//
// ProblemReplicaLagging
//    - Replication is working but lag exceeds threshold
//    - Causes to investigate:
//      a) Network congestion between primary and replica
//      b) Replica CPU/IO saturation (can't keep up with replay)
//      c) Long-running queries on replica blocking replay
//      d) Checkpoint/vacuum activity on primary generating excessive WAL
//      e) Synchronous replication bottleneck
//    - Fix: Depends on root cause; short-term we might not fix them, automatically
//           should understand why replication is broken.
//
// ProblemWalReceiverCrashing
//    - WAL receiver process repeatedly crashing
//    - Causes: Bad WAL segment, memory issues, bugs
//    - Fix: May need to skip corrupted WAL or re-clone
//
// ProblemReplicaSlotMissing
//    - NOTE: We are not creating a replication slot right now, so we might need to revisit this.
//    - Replication slot on primary was dropped
//    - Symptoms: Replica can't stream, gets "replication slot does not exist"
//    - Fix: Create new slot, may need to re-clone if WAL recycled
//
// ProblemSynchronousStandbyMisconfigured
//    - synchronous_standby_names doesn't match actual standbys
//    - Symptoms: Primary waiting indefinitely for sync confirmation
//    - Fix: Update synchronous_standby_names to match reality
