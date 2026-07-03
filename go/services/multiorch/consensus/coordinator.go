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

package consensus

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	commonconsensus "github.com/multigres/multigres/go/common/consensus"
	"github.com/multigres/multigres/go/common/eventlog"
	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/rpcclient"
	"github.com/multigres/multigres/go/common/timeouts"
	"github.com/multigres/multigres/go/common/topoclient"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	consensusdatapb "github.com/multigres/multigres/go/pb/consensusdata"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	multiorchdatapb "github.com/multigres/multigres/go/pb/multiorchdata"
	"github.com/multigres/multigres/go/services/multiorch/recovery/types"
)

// Coordinator orchestrates consensus-based leader election for shards.
//
// TODO: PoolerStore should be reorganized to be shard-centric rather than pooler-centric.
type Coordinator struct {
	coordinatorID *clustermetadatapb.ID
	topoStore     topoclient.Store
	rpcClient     rpcclient.MultiPoolerClient
	logger        *slog.Logger

	// TODO: policyCache will go away when we start reading the policy from nodes instead of etcd.
	// This cache is a temporary way to avoid making failover depend on etcd.
	policyCache sync.Map // database name → *clustermetadatapb.DurabilityPolicy
}

// NewCoordinator creates a new coordinator instance.
func NewCoordinator(coordinatorID *clustermetadatapb.ID, topoStore topoclient.Store, rpcClient rpcclient.MultiPoolerClient, logger *slog.Logger) *Coordinator {
	return &Coordinator{
		coordinatorID: coordinatorID,
		topoStore:     topoStore,
		rpcClient:     rpcClient,
		logger:        logger,
	}
}

// AppointLeader orchestrates the full consensus protocol to appoint a new leader
// for the given shard. It operates on a cohort of nodes (all nodes in the shard).
//
// Returns an error if any stage fails. The operation is idempotent and can be
// retried safely.
func (c *Coordinator) AppointLeader(ctx context.Context, shardKey *clustermetadatapb.ShardKey, cohort []*multiorchdatapb.PoolerHealthState, reason string) error {
	c.logger.InfoContext(ctx, "Starting leader appointment",
		"database", shardKey.GetDatabase(),
		"tablegroup", shardKey.GetTableGroup(),
		"shard", shardKey.GetShard(),
		"cohort_size", len(cohort))

	if len(cohort) == 0 {
		return mterrors.Errorf(mtrpcpb.Code_INVALID_ARGUMENT, "cohort is empty for shard %s", shardKey.GetShard())
	}

	return c.runFailover(ctx, cohort, reason)
}

// runFailover wires the failover callbacks for a coordinatorLedRuleChange and
// runs it. All poolers in the cohort are recruited regardless of their current
// health state: a pooler that looks unreachable at recruit-start may become
// reachable during the fan-out, and the most-advanced pooler (highest WAL
// position) may be one whose heartbeat happens to be stale. Recruit RPCs to
// unresponsive poolers return nil status and are skipped by the
// proposal-building loop; the non-blocking Phase 2 drain in rule_change.go
// ensures slow Recruit RPCs do not stall Run() after quorum is reached.
//
// A leader that signaled REQUESTING_DEMOTION stays in the recruitment cohort
// and may be re-promoted at the new term if it remains the best candidate —
// the signal is "please replace me if you can," not "remove me." Excluding it
// dropped a 2-pooler AT_LEAST_2 cohort below quorum after a Recruit fan-out
// emergency-demoted the primary.
//
// TODO: during a rule change, if some recruitable replicas would not require
// a pg_rewind to follow the chosen leader, prefer them over candidates that
// would. Picking a rewind-free replica makes failover faster; picking one
// that needs rewinding is not fatal — the SetPrimary path runs pg_rewind
// before the node serves writes or replicates.
func (c *Coordinator) runFailover(ctx context.Context, cohort []*multiorchdatapb.PoolerHealthState, reason string) error {
	var cohortStatuses []*clustermetadatapb.ConsensusStatus
	for _, p := range cohort {
		if cs := p.GetConsensusStatus(); cs != nil {
			cohortStatuses = append(cohortStatuses, cs)
		}
	}
	revocation, err := commonconsensus.NewTermRevocation(cohortStatuses, c.coordinatorID, timestamppb.Now())
	if err != nil {
		return mterrors.Errorf(mtrpcpb.Code_FAILED_PRECONDITION, "%v", err)
	}

	if revocation.GetPropagationIntent() != nil {
		c.logger.InfoContext(ctx, "Detected in-flight proposal beyond the cohort's outgoing decision; using propagation path",
			"outgoing_decision", commonconsensus.FormatRuleNumber(revocation.GetOutgoingDecision()),
			"propagation_intent", commonconsensus.FormatRuleNumber(revocation.GetPropagationIntent()),
			"recruited_term", revocation.GetRevokedBelowTerm())
		return c.runPropagation(ctx, cohort, cohortStatuses, revocation, reason)
	}

	poolerByID, _ := buildCohortMaps(cohort)
	buildProposal := func(r commonconsensus.RecruitmentResult) (*consensusdatapb.CoordinatorProposal, error) {
		return buildFailoverProposal(r, poolerByID)
	}
	tryBuildProposal := func(rev *clustermetadatapb.TermRevocation, statuses []*clustermetadatapb.ConsensusStatus) (*consensusdatapb.CoordinatorProposal, error) {
		return commonconsensus.BuildSafeProposal(rev, statuses, buildProposal)
	}
	checkProposalPossible := func(rev *clustermetadatapb.TermRevocation, statuses []*clustermetadatapb.ConsensusStatus) error {
		return commonconsensus.CheckProposalPossible(rev, statuses, buildProposal)
	}
	return c.newRuleChange(reason, tryBuildProposal, checkProposalPossible).Run(ctx, cohort, revocation)
}

// runPropagation handles the dead-leader propagation path: when NewTermRevocation
// detects an in-flight proposal beyond the cohort's outgoing decision
// (propagation_intent set), we recruit all nodes with the propagation revocation
// and then drive the existing WAL entry to quorum via the Propagate RPC rather
// than writing a new rule.
//
// cachedStatuses are the same statuses that were passed to NewTermRevocation —
// they already show the matching proposal. We use them for a fast pre-vote
// check before committing to a recruitment round.
//
// The most-advanced recruited node whose proposal matches propagation_intent
// becomes the propagation leader. It receives Propagate (promote, drive the WAL
// entry to quorum, self-promote); all other recruited nodes receive SetPrimary
// pointing at that leader.
func (c *Coordinator) runPropagation(
	ctx context.Context,
	cohort []*multiorchdatapb.PoolerHealthState,
	cachedStatuses []*clustermetadatapb.ConsensusStatus,
	revocation *clustermetadatapb.TermRevocation,
	reason string,
) error {
	if err := checkRecentAcceptance(ctx, c.logger, cohort); err != nil {
		return mterrors.Errorf(mtrpcpb.Code_UNAVAILABLE, "%v", err)
	}

	if err := commonconsensus.CheckPropagationPossible(revocation, cachedStatuses); err != nil {
		return mterrors.Errorf(mtrpcpb.Code_UNAVAILABLE, "pre-vote failed: %v", err)
	}

	proposedTerm := revocation.GetRevokedBelowTerm()
	start := time.Now()
	primaryPromotion := eventlog.PrimaryPromotion{
		ProposedTerm: proposedTerm,
		Reason:       reason,
	}
	eventlog.Emit(ctx, c.logger, eventlog.Started, primaryPromotion)

	// Phase 1: recruit all nodes concurrently with the propagation revocation.
	type recruitResult struct {
		pooler *multiorchdatapb.PoolerHealthState
		cs     *clustermetadatapb.ConsensusStatus
	}
	results := make(chan recruitResult, len(cohort))
	for _, p := range cohort {
		go func() {
			rpcCtx, cancel := context.WithTimeout(ctx, timeouts.RemoteOperationTimeout)
			defer cancel()
			resp, err := c.rpcClient.Recruit(rpcCtx, p.MultiPooler, &consensusdatapb.RecruitRequest{
				TermRevocation: revocation,
			})
			if err != nil {
				c.logger.WarnContext(ctx, "Propagation recruit failed",
					"pooler", p.MultiPooler.Id.Name, "error", err)
				results <- recruitResult{pooler: p}
				return
			}
			c.logger.InfoContext(ctx, "Propagation recruit succeeded",
				"pooler", p.MultiPooler.Id.Name)
			results <- recruitResult{pooler: p, cs: resp.GetConsensusStatus()}
		}()
	}

	_, healthByID := buildCohortMaps(cohort)
	var recruitedStatuses []*clustermetadatapb.ConsensusStatus
	for range len(cohort) {
		if rr := <-results; rr.cs != nil {
			recruitedStatuses = append(recruitedStatuses, rr.cs)
		}
	}
	recruitMs := time.Since(start).Milliseconds()

	// Phase 2: find the propagation leader candidates — most-advanced recruited
	// nodes with the matching proposal — and pick the first eligible one
	// (arbitrary choice when tied).
	leaders, err := commonconsensus.FindPropagationLeaders(revocation, recruitedStatuses)
	if err != nil {
		promoteEvent := eventlog.PrimaryPromotion{ProposedTerm: proposedTerm, Reason: reason, RecruitMs: &recruitMs}
		wrapped := mterrors.Errorf(mtrpcpb.Code_FAILED_PRECONDITION, "recruitment failed: %v", err)
		eventlog.Emit(ctx, c.logger, eventlog.Failed, promoteEvent, "error", wrapped)
		return wrapped
	}
	var propLeaderCS *clustermetadatapb.ConsensusStatus
	for _, cs := range leaders {
		ph := healthByID[topoclient.ClusterIDString(cs.GetId())]
		if types.PoolerIsCohortIneligible(ph.GetAvailabilityStatus()) {
			c.logger.WarnContext(ctx, "Skipping ineligible propagation leader candidate",
				"pooler", cs.GetId().GetName())
			continue
		}
		propLeaderCS = cs
		break
	}
	if propLeaderCS == nil {
		promoteEvent := eventlog.PrimaryPromotion{ProposedTerm: proposedTerm, Reason: reason, RecruitMs: &recruitMs}
		wrapped := mterrors.New(mtrpcpb.Code_FAILED_PRECONDITION,
			"no eligible propagation leader: all candidates are cohort-ineligible")
		eventlog.Emit(ctx, c.logger, eventlog.Failed, promoteEvent, "error", wrapped)
		return wrapped
	}

	leaderKey := topoclient.ClusterIDString(propLeaderCS.GetId())
	mp := healthByID[leaderKey].MultiPooler
	propLeaderAddr := topoclient.PoolerAddressFor(mp)
	expectedProposal := propLeaderCS.GetCurrentPosition().GetProposal()
	selectedAt := time.Now()

	// Phase 3: dispatch Propagate to the leader and SetPrimary to all other
	// recruited nodes concurrently.
	type dispatchResult struct {
		poolerName string
		isLeader   bool
		err        error
	}
	total := len(recruitedStatuses)
	dispatched := make(chan dispatchResult, total)

	for _, cs := range recruitedStatuses {
		isLeader := topoclient.ClusterIDString(cs.GetId()) == leaderKey
		fHealth := healthByID[topoclient.ClusterIDString(cs.GetId())]
		go func() {
			rpcCtx, cancel := context.WithTimeout(ctx, timeouts.RemoteOperationTimeout)
			defer cancel()
			name := cs.GetId().GetName()
			if isLeader {
				_, err := c.rpcClient.Propagate(rpcCtx, mp, &consensusdatapb.PropagateRequest{
					TermRevocation:   revocation,
					ExpectedProposal: expectedProposal,
					Leader:           propLeaderAddr,
				})
				dispatched <- dispatchResult{poolerName: name, isLeader: true, err: err}
				return
			}
			// The propagation revocation establishes the authority under which
			// the propagation leader will finalize expectedProposal. Followers
			// accept the leader_id mismatch (the rule's authored leader_id is
			// the now-dead primary, while the leader contact info points at the
			// propagation leader finalizing it) because their own accepted
			// revocation — established via the Recruit above — carries a
			// matching propagation_intent.
			_, err := c.rpcClient.SetPrimary(rpcCtx, fHealth.MultiPooler, &consensusdatapb.SetPrimaryRequest{
				ReplicationPrimary: &clustermetadatapb.ReplicationPrimary{
					Rule:    expectedProposal,
					Primary: propLeaderAddr,
				},
			})
			dispatched <- dispatchResult{poolerName: name, isLeader: false, err: err}
		}()
	}

	var leaderErr error
	for range total {
		dr := <-dispatched
		if dr.err != nil {
			if dr.isLeader {
				leaderErr = dr.err
			} else {
				c.logger.WarnContext(ctx, "SetPrimary failed for follower during propagation",
					"pooler", dr.poolerName, "error", dr.err)
			}
		} else {
			c.logger.InfoContext(ctx, "Propagation dispatch succeeded",
				"pooler", dr.poolerName, "is_leader", dr.isLeader)
		}
	}

	promoteMs := time.Since(selectedAt).Milliseconds()
	promoteEvent := eventlog.PrimaryPromotion{
		NewPrimary:   mp.GetId().GetName(),
		ProposedTerm: proposedTerm,
		Reason:       reason,
		RecruitMs:    &recruitMs,
		PromoteMs:    &promoteMs,
	}
	if leaderErr != nil {
		eventlog.Emit(ctx, c.logger, eventlog.Failed, promoteEvent, "error", leaderErr)
		return mterrors.Wrapf(leaderErr, "propagation leader %s failed Propagate", mp.GetId().GetName())
	}
	eventlog.Emit(ctx, c.logger, eventlog.Success, promoteEvent)
	return nil
}

// AppointInitialLeader orchestrates consensus leader election for a freshly
// bootstrapped shard where all poolers start at term 0.
//
// Uses GetBootstrapPolicy (not AppointLeader's LoadQuorumRule) because freshly
// restored standbys report UNKNOWN pooler type, which causes LoadQuorumRule to
// fall back to majority quorum instead of the configured durability policy.
func (c *Coordinator) AppointInitialLeader(ctx context.Context, shardKey *clustermetadatapb.ShardKey, cohort []*multiorchdatapb.PoolerHealthState) error {
	if len(cohort) == 0 {
		return mterrors.Errorf(mtrpcpb.Code_INVALID_ARGUMENT, "cohort is empty for shard %s", shardKey.GetShard())
	}

	policy, err := c.GetBootstrapPolicy(ctx, shardKey.GetDatabase())
	if err != nil {
		return mterrors.Wrap(err, "failed to load durability policy from topology")
	}

	// Bootstrap has no outgoing cohort to recruit consent from, so we use
	// the externally-certified path. The "external" certification is the
	// most-advanced timeline observed across the cohort's cached statuses:
	// its rule number caps how far the outgoing cohort could have
	// progressed, and its LSN is the frozen point any new leader must
	// match. This handles partial bootstraps too — if any cohort member
	// already carries a rule, we surface its rule number rather than
	// falsely claiming term 0.
	var cohortStatuses []*clustermetadatapb.ConsensusStatus
	for _, p := range cohort {
		if cs := p.GetConsensusStatus(); cs != nil {
			cohortStatuses = append(cohortStatuses, cs)
		}
	}

	// This is the discovery phase of coordinator-led rule changes. For
	// externally-certified rule changes, the agent (this method) is
	// responsible for choosing the outgoing rule and authoring the
	// revocation; common/consensus consumes the cert without re-deriving.
	mostAdvanced := commonconsensus.MostAdvancedPosition(cohortStatuses)
	if mostAdvanced == nil {
		return mterrors.Errorf(mtrpcpb.Code_FAILED_PRECONDITION,
			"cannot bootstrap shard %s: no cohort member has a known WAL position", shardKey.GetShard())
	}

	revocation, err := commonconsensus.NewTermRevocation(cohortStatuses, c.coordinatorID, timestamppb.Now())
	if err != nil {
		return mterrors.Errorf(mtrpcpb.Code_FAILED_PRECONDITION, "%v", err)
	}

	cert := &clustermetadatapb.ExternallyCertifiedRevocation{
		TermRevocation: revocation,
		FrozenLsn:      mostAdvanced.GetLsn(),
	}

	poolerByID, _ := buildCohortMaps(cohort)
	cohortIDs := poolerIDs(cohort)
	buildProposalFn := func(result commonconsensus.RecruitmentResult) (*consensusdatapb.CoordinatorProposal, error) {
		return buildBootstrapProposal(result, cohortIDs, policy, poolerByID)
	}
	return c.newRuleChange(
		"ShardInit",
		func(_ *clustermetadatapb.TermRevocation, statuses []*clustermetadatapb.ConsensusStatus) (*consensusdatapb.CoordinatorProposal, error) {
			return commonconsensus.BuildExternallyCertifiedProposal(cert, statuses, buildProposalFn)
		},
		func(_ *clustermetadatapb.TermRevocation, statuses []*clustermetadatapb.ConsensusStatus) error {
			return commonconsensus.CheckExternallyCertifiedProposalPossible(cert, statuses, buildProposalFn)
		},
	).Run(ctx, cohort, revocation)
}

// GetCoordinatorID returns the coordinator's ID.
func (c *Coordinator) GetCoordinatorID() *clustermetadatapb.ID {
	return c.coordinatorID
}

// GetShardNodes retrieves all multipooler nodes for a given shard from the topology.
func (c *Coordinator) GetShardNodes(ctx context.Context, cell string, database string, tablegroup string, shardID string) ([]*multiorchdatapb.PoolerHealthState, error) {
	// Get all multipoolers in the cell for this specific shard
	poolers, err := c.topoStore.GetMultiPoolersByCell(ctx, cell, &topoclient.GetMultiPoolersByCellOptions{
		DatabaseShard: &topoclient.DatabaseShard{
			Database:   database,
			TableGroup: tablegroup,
			Shard:      shardID,
		},
	})
	if err != nil {
		return nil, mterrors.Wrap(err, "failed to get multipoolers from topology")
	}

	if len(poolers) == 0 {
		return nil, mterrors.Errorf(mtrpcpb.Code_NOT_FOUND,
			"no multipoolers found for shard %s in cell %s", shardID, cell)
	}

	// Convert topology poolers to PoolerHealthState instances
	poolerHealths := make([]*multiorchdatapb.PoolerHealthState, 0, len(poolers))
	for _, poolerInfo := range poolers {
		ph := &multiorchdatapb.PoolerHealthState{
			MultiPooler: poolerInfo.MultiPooler,
		}
		poolerHealths = append(poolerHealths, ph)
	}

	return poolerHealths, nil
}

// GetBootstrapPolicy returns the durability policy for the given database by reading
// bootstrap_durability_policy from the topology Database record. The result is cached
// in memory since the policy is assumed not to change for the lifetime of this process.
//
// TODO: Once pooler status updates carry policy information, this should be replaced
// with a live policy loaded from the shard's nodes rather than the bootstrap record.
func (c *Coordinator) GetBootstrapPolicy(ctx context.Context, database string) (*clustermetadatapb.DurabilityPolicy, error) {
	if cached, ok := c.policyCache.Load(database); ok {
		return cached.(*clustermetadatapb.DurabilityPolicy), nil
	}

	db, err := c.topoStore.GetDatabase(ctx, database)
	if err != nil {
		return nil, mterrors.Wrapf(err, "failed to get database %s from topology", database)
	}

	if db.BootstrapDurabilityPolicy == nil {
		return nil, mterrors.Errorf(mtrpcpb.Code_FAILED_PRECONDITION,
			"database %s has no bootstrap_durability_policy configured", database)
	}

	c.policyCache.Store(database, db.BootstrapDurabilityPolicy)
	return db.BootstrapDurabilityPolicy, nil
}

// poolerIDs extracts the clustermetadata IDs from a slice of PoolerHealthState.
// Used at the boundary where poolers cross into the durability-policy layer,
// which operates on bare *clustermetadatapb.ID values.
func poolerIDs(poolers []*multiorchdatapb.PoolerHealthState) []*clustermetadatapb.ID {
	out := make([]*clustermetadatapb.ID, len(poolers))
	for i, p := range poolers {
		out[i] = p.MultiPooler.Id
	}
	return out
}
