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
	"log/slog"
	"time"

	commonconsensus "github.com/multigres/multigres/go/common/consensus"
	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/rpcclient"
	"github.com/multigres/multigres/go/common/topoclient"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	"github.com/multigres/multigres/go/services/multiorch/config"
	"github.com/multigres/multigres/go/services/multiorch/recovery/analysis/eligibility"
	"github.com/multigres/multigres/go/services/multiorch/recovery/types"
	"github.com/multigres/multigres/go/services/multiorch/store"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	consensusdatapb "github.com/multigres/multigres/go/pb/consensusdata"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
)

// Compile-time assertion that ReconcileCohortAction implements types.RecoveryAction.
var _ types.RecoveryAction = (*ReconcileCohortAction)(nil)

// ReconcileCohortAction applies a single cohort-membership change on the
// shard's leader.
//
// problem.Code only gets it dispatched here; it plays no role in deciding
// what to do. The actual decision — add, remove, or nothing — is re-derived
// from scratch against current state via eligibility.DecideAll, the same
// classifier Analyze() used, so a Problem that's gone stale by execution
// time (the member recovered, or someone else already fixed it) is a safe
// no-op instead of an incorrect action.
//
// The action mutates exactly one cohort member per execution; multiple
// drifting members produce multiple problems and run separately.
//
// TODO: future work will likely cap the cohort size based on the durability
// policy and require a fitness heuristic to choose the best-qualified
// candidates among many eligible poolers. Today the action adds every
// eligible non-cohort pooler unconditionally.
type ReconcileCohortAction struct {
	config      *config.Config
	rpcClient   rpcclient.MultipoolerClient
	poolerStore *store.PoolerCache
	topoStore   topoclient.Store
	logger      *slog.Logger
}

// NewReconcileCohortAction creates a new cohort reconciliation action.
func NewReconcileCohortAction(
	cfg *config.Config,
	rpcClient rpcclient.MultipoolerClient,
	poolerStore *store.PoolerCache,
	topoStore topoclient.Store,
	logger *slog.Logger,
) *ReconcileCohortAction {
	return &ReconcileCohortAction{
		config:      cfg,
		rpcClient:   rpcClient,
		poolerStore: poolerStore,
		topoStore:   topoStore,
		logger:      logger,
	}
}

// Execute applies the cohort change on the shard leader.
func (a *ReconcileCohortAction) Execute(ctx context.Context, problem types.Problem) error {
	a.logger.InfoContext(ctx, "executing reconcile cohort action",
		"shard_key", problem.ShardKey.String(),
		"pooler", problem.PoolerID.Name,
		"problem_code", string(problem.Code))

	members := store.FindShardMembers(a.poolerStore, problem.ShardKey)
	leader := members.Leader
	if leader == nil || members.HighestKnownPosition == nil {
		return mterrors.Errorf(mtrpcpb.Code_FAILED_PRECONDITION,
			"no consensus leader known for shard %s", problem.ShardKey)
	}
	// TODO: allow non-promotion rule changes to do propagation.
	if !commonconsensus.IsRuleDecided(members.HighestKnownPosition) {
		return mterrors.Errorf(mtrpcpb.Code_FAILED_PRECONDITION,
			"shard %s cannot update its cohort while it has an undecided proposal", problem.ShardKey)
	}
	// TODO: skip this cycle if there's no recent evidence the leader can
	// actually commit writes quickly (e.g. blocked on sync-replication ack
	// from a lagging standby) — no such signal exists yet to check against.
	rule := members.HighestKnownPosition.GetDecision()

	// problem.Code and problem.PoolerID only got us here; the pooler cache
	// updates asynchronously off each pooler's health stream, independent of
	// the recovery loop's cadence, so what Analyze() saw can be stale by now.
	// Re-derive every candidate in the shard from scratch with the same
	// classifier Analyze() used: a stale Problem then resolves to a safe
	// no-op instead of an incorrect action, and same-tier candidates can be
	// batched into the one RPC below instead of trickling out one per cycle.
	targetKey := topoclient.ComponentIDString(problem.PoolerID)
	decisions := eligibility.DecideAll(time.Now(), eligibility.DefaultThresholds(), rule, members.Poolers,
		func(id *clustermetadatapb.ID) bool { return isTombstoned(a.poolerStore, id) })

	var target *eligibility.Decision
	for i := range decisions {
		if topoclient.ComponentIDString(decisions[i].ID) == targetKey {
			target = &decisions[i]
			break
		}
	}
	if target == nil {
		// The situation already resolved since Analyze() looked: the member
		// recovered, or a non-member is correctly excluded. Nothing to do.
		a.logger.InfoContext(ctx, "reconcile cohort: recommendation no longer applies, nothing to do", "target", problem.PoolerID.Name)
		return nil
	}

	// Batch every other candidate at the same tier into this one RPC — safe
	// for OpAdd (more members never hurts durability) and for an Urgent
	// OpRemove (these were never really contributing anyway), but not for a
	// non-urgent OpRemove: two individually-safe removals aren't necessarily
	// safe together, so those still apply one at a time (see DecideAll).
	op := multipoolermanagerdatapb.CohortUpdateOperation_COHORT_UPDATE_OPERATION_ADD
	batchable := func(d eligibility.Decision) bool { return d.Op == eligibility.OpAdd }
	if target.Op == eligibility.OpRemove {
		op = multipoolermanagerdatapb.CohortUpdateOperation_COHORT_UPDATE_OPERATION_REMOVE
		urgent := target.Urgent
		batchable = func(d eligibility.Decision) bool { return d.Op == eligibility.OpRemove && d.Urgent == urgent && urgent }
	}
	standbyIDs := []*clustermetadatapb.ID{target.ID}
	for _, d := range decisions {
		if d.ID != target.ID && batchable(d) {
			standbyIDs = append(standbyIDs, d.ID)
		}
	}

	req := &multipoolermanagerdatapb.UpdateConsensusRuleRequest{
		Operation:            op,
		StandbyIds:           standbyIDs,
		ExpectedOutgoingRule: rule.GetRuleNumber(),
	}

	if _, err := a.rpcClient.UpdateConsensusRule(ctx, leader.Health().Multipooler, req); err != nil {
		return mterrors.Wrap(err, "UpdateConsensusRule failed")
	}

	// A member that joins an already-established cohort out-of-band (provisioned
	// after a failover, added here rather than through the promotion-time Recruit
	// wave) never received Recruit's synchronous restore_command clear. The ADD
	// above only amends the leader's rule + synchronous_standby_names; it runs on
	// the leader and cannot touch the joining member's restore_command. Left set,
	// a restart-as-standby can resolve recovery_target_timeline=latest through the
	// archive to a divergent timeline and FATAL at startup. Drive the member-side
	// clear synchronously by re-issuing SetPrimary carrying the post-ADD rule: the
	// member now sees itself named in that rule and clears restore_command before
	// the monitor's ~one-tick backstop would. Best-effort — the ADD (the action's
	// contract) already succeeded, and the monitor backstop still covers a failure
	// here — so a member-side hiccup does not fail cohort reconciliation.
	if op == multipoolermanagerdatapb.CohortUpdateOperation_COHORT_UPDATE_OPERATION_ADD {
		for _, id := range standbyIDs {
			if pa := findPooler(members.Poolers, id); pa != nil {
				a.clearJoiningMemberArchive(ctx, leader, pa)
			}
		}
	}

	a.logger.InfoContext(ctx, "reconcile cohort action completed",
		"targets", len(standbyIDs),
		"primary", leader.Health().Multipooler.Id.Name,
		"operation", op.String())
	return nil
}

// findPooler returns the rider named id within poolers, or nil.
func findPooler(poolers []*store.Pooler, id *clustermetadatapb.ID) *store.Pooler {
	key := topoclient.ComponentIDString(id)
	for _, pa := range poolers {
		if topoclient.ComponentIDString(pa.Health().GetMultipooler().GetId()) == key {
			return pa
		}
	}
	return nil
}

// isTombstoned reports whether id is currently a cache tombstone (known
// SHUTDOWN) — checked fresh, not carried over from whatever Analyze() saw.
func isTombstoned(cache *store.PoolerCache, id *clustermetadatapb.ID) bool {
	key := topoclient.ComponentIDString(id)
	for _, t := range cache.Tombstones() {
		if topoclient.ComponentIDString(t.ID) == key {
			return true
		}
	}
	return false
}

// clearJoiningMemberArchive re-issues SetPrimary to a pooler just added to the
// cohort so it clears restore_command synchronously (see the caller for why).
//
// It re-reads the leader's status to obtain the post-ADD rule — the rule that
// now names the member — because the member-side clear keys off cohort
// membership as asserted by the rule this SetPrimary delivers. The cached
// pre-ADD rule would not name the member, so relaying it would not trigger the
// clear. Failures are logged and swallowed: this is a best-effort hardening step
// layered on top of the pooler's own monitor backstop.
func (a *ReconcileCohortAction) clearJoiningMemberArchive(ctx context.Context, leader, target *store.Pooler) {
	statusResp, err := a.rpcClient.Status(ctx, leader.Health().Multipooler, &multipoolermanagerdatapb.StatusRequest{})
	if err != nil {
		a.logger.WarnContext(ctx, "reconcile cohort: could not read leader status to clear joining member's archive; relying on monitor backstop",
			"target", target.Health().Multipooler.Id.Name, "error", err)
		return
	}
	// The leader's own rule store reflects the ADD synchronously (UpdateConsensusRule
	// commits before returning), so HighestKnownRule here is the post-ADD rule.
	postAddRule := commonconsensus.HighestKnownRule([]*clustermetadatapb.ConsensusStatus{statusResp.GetConsensusStatus()})
	if postAddRule == nil {
		a.logger.WarnContext(ctx, "reconcile cohort: leader reported no rule after ADD; relying on monitor backstop",
			"target", target.Health().Multipooler.Id.Name)
		return
	}
	setPrimaryReq := &consensusdatapb.SetPrimaryRequest{
		ReplicationPrimary: &clustermetadatapb.ReplicationPrimary{
			Position:    postAddRule,
			Primary:     topoclient.PoolerAddressFor(leader.Health().Multipooler),
			RewindReady: commonconsensus.ReplicationPrimaryOrNil(statusResp.GetConsensusStatus()).GetRewindReady(),
		},
	}
	if _, err := a.rpcClient.SetPrimary(ctx, target.Health().Multipooler, setPrimaryReq); err != nil {
		a.logger.WarnContext(ctx, "reconcile cohort: SetPrimary to clear joining member's archive failed; relying on monitor backstop",
			"target", target.Health().Multipooler.Id.Name, "error", err)
	}
}

// RecoveryAction interface implementation

func (a *ReconcileCohortAction) RequiresHealthyLeader() bool {
	return true // UpdateConsensusRule must run on a healthy primary.
}

func (a *ReconcileCohortAction) Metadata() types.RecoveryMetadata {
	return types.RecoveryMetadata{
		Name:        "ReconcileCohort",
		Description: "Add or remove a single cohort member on the shard leader",
		Timeout:     30 * time.Second,
		LockTimeout: 15 * time.Second,
		Retryable:   true,
	}
}

func (a *ReconcileCohortAction) GracePeriod() *types.GracePeriodConfig {
	return nil
}
