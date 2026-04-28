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

package consensus

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	commonconsensus "github.com/multigres/multigres/go/common/consensus"
	"github.com/multigres/multigres/go/common/eventlog"
	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/timeouts"
	"github.com/multigres/multigres/go/common/topoclient"
	"github.com/multigres/multigres/go/services/multiorch/recovery/types"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	consensusdatapb "github.com/multigres/multigres/go/pb/consensusdata"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	multiorchdatapb "github.com/multigres/multigres/go/pb/multiorchdata"
)

// coordinatorLedRuleChange orchestrates the recruit → check → propose workflow
// for a coordinator-initiated rule change. It is parameterized by action-specific
// callbacks so the same workflow serves both normal failover and bootstrap.
type coordinatorLedRuleChange struct {
	coordinator   *Coordinator
	reason        string
	tryBuild      func(*clustermetadatapb.TermRevocation, []*clustermetadatapb.ConsensusStatus) (*consensusdatapb.CoordinatorProposal, error)
	checkPossible func(*clustermetadatapb.TermRevocation, []*clustermetadatapb.ConsensusStatus) error
}

func (c *Coordinator) newRuleChange(
	reason string,
	tryBuild func(*clustermetadatapb.TermRevocation, []*clustermetadatapb.ConsensusStatus) (*consensusdatapb.CoordinatorProposal, error),
	checkPossible func(*clustermetadatapb.TermRevocation, []*clustermetadatapb.ConsensusStatus) error,
) *coordinatorLedRuleChange {
	return &coordinatorLedRuleChange{
		coordinator:   c,
		reason:        reason,
		tryBuild:      tryBuild,
		checkPossible: checkPossible,
	}
}

// Run executes the rule change: derive term, pre-validate, recruit, propose.
func (r *coordinatorLedRuleChange) Run(ctx context.Context, cohort []*multiorchdatapb.PoolerHealthState) error {
	// Extract consensus statuses from the cached health state to derive the term.
	var initialStatuses []*clustermetadatapb.ConsensusStatus
	for _, p := range cohort {
		if cs := p.GetConsensusStatus(); cs != nil {
			initialStatuses = append(initialStatuses, cs)
		}
	}

	revocation := commonconsensus.NewTermRevocation(initialStatuses, r.coordinator.coordinatorID)

	r.coordinator.logger.InfoContext(ctx, "Starting rule change",
		"proposed_term", revocation.GetRevokedBelowTerm(),
		"cohort_size", len(cohort))

	// Back off if any node recently accepted a revocation — another coordinator
	// may be running an election.
	if err := checkRecentAcceptance(ctx, r.coordinator.logger, cohort); err != nil {
		return mterrors.Errorf(mtrpcpb.Code_UNAVAILABLE, "%v", err)
	}

	// Pre-validate that a proposal would be feasible with current statuses before
	// committing to a recruitment round.
	if err := r.checkPossible(revocation, initialStatuses); err != nil {
		return mterrors.Errorf(mtrpcpb.Code_UNAVAILABLE, "pre-vote failed: %v", err)
	}

	// Recruit nodes in parallel; build a proposal as soon as quorum is achieved.
	proposal, statuses, err := r.recruit(ctx, cohort, revocation)
	if err != nil {
		return mterrors.Wrap(err, "recruitment failed")
	}

	newPrimary := proposal.GetProposalLeader().GetId().GetName()
	eventlog.Emit(ctx, r.coordinator.logger, eventlog.Started, eventlog.PrimaryPromotion{
		NewPrimary: newPrimary,
	})
	poolerByID, _ := buildCohortMaps(cohort)
	proposeErr := proposeAll(ctx, r.coordinator, r.reason, proposal, statuses, poolerByID)
	if proposeErr == nil {
		eventlog.Emit(ctx, r.coordinator.logger, eventlog.Success, eventlog.PrimaryPromotion{
			NewPrimary: newPrimary,
		})
	} else {
		eventlog.Emit(ctx, r.coordinator.logger, eventlog.Failed, eventlog.PrimaryPromotion{
			NewPrimary: newPrimary,
		}, "error", proposeErr)
	}
	return proposeErr
}

// recruit sends Recruit RPCs to all cohort members in parallel. After each
// successful response it calls r.tryBuild; on the first viable proposal it
// returns early without waiting for remaining goroutines. If all RPCs complete
// without a viable proposal, the accumulated statuses and the final tryBuild
// error are returned.
func (r *coordinatorLedRuleChange) recruit(
	ctx context.Context,
	cohort []*multiorchdatapb.PoolerHealthState,
	revocation *clustermetadatapb.TermRevocation,
) (*consensusdatapb.CoordinatorProposal, []*clustermetadatapb.ConsensusStatus, error) {
	type result struct {
		status *clustermetadatapb.ConsensusStatus
	}

	ch := make(chan result, len(cohort))
	for _, p := range cohort {
		go func(p *multiorchdatapb.PoolerHealthState) {
			rpcCtx, cancel := context.WithTimeout(ctx, timeouts.RemoteOperationTimeout)
			defer cancel()
			resp, err := r.coordinator.rpcClient.Recruit(rpcCtx, p.MultiPooler, &consensusdatapb.RecruitRequest{
				TermRevocation: revocation,
			})
			if err != nil {
				r.coordinator.logger.WarnContext(ctx, "Recruit failed",
					"pooler", p.MultiPooler.Id.Name, "error", err)
				ch <- result{}
				return
			}
			cs := resp.GetConsensusStatus()
			if cs == nil {
				r.coordinator.logger.WarnContext(ctx, "Recruit returned nil ConsensusStatus",
					"pooler", p.MultiPooler.Id.Name)
				ch <- result{}
				return
			}
			r.coordinator.logger.InfoContext(ctx, "Recruited pooler",
				"pooler", p.MultiPooler.Id.Name,
				"lsn", cs.GetCurrentPosition().GetLsn())
			ch <- result{status: cs}
		}(p)
	}

	var statuses []*clustermetadatapb.ConsensusStatus
	consumed := 0
	for range len(cohort) {
		res := <-ch
		consumed++
		if res.status != nil {
			statuses = append(statuses, res.status)
			if proposal, err := r.tryBuild(revocation, statuses); err == nil {
				remaining := len(cohort) - consumed
				if remaining > 0 {
					go func() {
						for range remaining {
							<-ch
						}
					}()
				}
				return proposal, statuses, nil
			}
		}
	}

	// All RPCs completed
	proposal, err := r.tryBuild(revocation, statuses)
	return proposal, statuses, err
}

// buildFailoverProposal constructs a CoordinatorProposal for normal failover.
// It picks the first non-resigned eligible leader from result.EligibleLeaders
// and derives the cohort and durability policy from result.BestRule.
func buildFailoverProposal(
	ctx context.Context,
	logger *slog.Logger,
	result commonconsensus.RecruitmentResult,
	poolerByID map[string]*clustermetadatapb.MultiPooler,
	healthByID map[string]*multiorchdatapb.PoolerHealthState,
) (*consensusdatapb.CoordinatorProposal, error) {
	if result.BestRule == nil {
		return nil, errors.New("no committed rule found; use bootstrap path for fresh clusters")
	}

	var leader *clustermetadatapb.ConsensusStatus
	for _, cs := range result.EligibleLeaders {
		key := topoclient.ClusterIDString(cs.GetId())
		if health, ok := healthByID[key]; ok && types.PrimaryNeedsReplacement(health) {
			logger.InfoContext(ctx, "Skipping resigned primary during leader selection",
				"pooler", cs.GetId().GetName())
			continue
		}
		leader = cs
		break
	}
	if leader == nil {
		return nil, errors.New("all eligible leaders have resigned")
	}

	mp, ok := poolerByID[topoclient.ClusterIDString(leader.GetId())]
	if !ok {
		return nil, fmt.Errorf("leader %s not found in cohort", leader.GetId().GetName())
	}

	return &consensusdatapb.CoordinatorProposal{
		TermRevocation: result.TermRevocation,
		ProposalLeader: &consensusdatapb.ProposalLeader{
			Id:           leader.GetId(),
			Host:         mp.GetHostname(),
			PostgresPort: mp.GetPortMap()["postgres"],
		},
		ProposedRule: &clustermetadatapb.ShardRule{
			CohortMembers:    result.BestRule.GetCohortMembers(),
			DurabilityPolicy: result.BestRule.GetDurabilityPolicy(),
			PrimaryId:        leader.GetId(),
		},
	}, nil
}

// buildBootstrapProposal constructs a CoordinatorProposal for bootstrap or
// forced recovery where no committed rule exists. It picks the first eligible
// leader and proposes the full provided cohort under the given durability policy.
func buildBootstrapProposal(
	result commonconsensus.RecruitmentResult,
	cohortIDs []*clustermetadatapb.ID,
	policy *clustermetadatapb.DurabilityPolicy,
	poolerByID map[string]*clustermetadatapb.MultiPooler,
) (*consensusdatapb.CoordinatorProposal, error) {
	if len(result.EligibleLeaders) == 0 {
		return nil, errors.New("no eligible leaders for bootstrap proposal")
	}
	leader := result.EligibleLeaders[0]
	mp, ok := poolerByID[topoclient.ClusterIDString(leader.GetId())]
	if !ok {
		return nil, fmt.Errorf("leader %s not found in cohort", leader.GetId().GetName())
	}
	return &consensusdatapb.CoordinatorProposal{
		TermRevocation: result.TermRevocation,
		ProposalLeader: &consensusdatapb.ProposalLeader{
			Id:           leader.GetId(),
			Host:         mp.GetHostname(),
			PostgresPort: mp.GetPortMap()["postgres"],
		},
		ProposedRule: &clustermetadatapb.ShardRule{
			CohortMembers:    cohortIDs,
			DurabilityPolicy: policy,
			PrimaryId:        leader.GetId(),
		},
	}, nil
}

// proposeAll sends a Propose RPC to every recruited node in parallel. Returns
// an error only if the proposed leader's Propose fails; non-leader failures are
// logged but do not abort the operation.
func proposeAll(
	ctx context.Context,
	c *Coordinator,
	reason string,
	proposal *consensusdatapb.CoordinatorProposal,
	statuses []*clustermetadatapb.ConsensusStatus,
	poolerByID map[string]*clustermetadatapb.MultiPooler,
) error {
	leaderKey := topoclient.ClusterIDString(proposal.GetProposalLeader().GetId())

	leaderFound := false
	for _, cs := range statuses {
		if topoclient.ClusterIDString(cs.GetId()) == leaderKey {
			leaderFound = true
			break
		}
	}
	if !leaderFound {
		return mterrors.Errorf(mtrpcpb.Code_INTERNAL,
			"proposed leader %s not found in recruited statuses", leaderKey)
	}

	acceptedIDs := make([]*clustermetadatapb.ID, 0, len(statuses))
	for _, cs := range statuses {
		acceptedIDs = append(acceptedIDs, cs.GetId())
	}

	type proposeResult struct {
		poolerName string
		isLeader   bool
		err        error
	}
	results := make(chan proposeResult, len(statuses))

	req := &consensusdatapb.ProposeRequest{
		Proposal:        proposal,
		Reason:          reason,
		AcceptedNodeIds: acceptedIDs,
	}
	for _, cs := range statuses {
		key := topoclient.ClusterIDString(cs.GetId())
		mp, ok := poolerByID[key]
		if !ok {
			c.logger.WarnContext(ctx, "Recruited node not found in cohort, skipping Propose", "id", key)
			results <- proposeResult{poolerName: key}
			continue
		}
		isLeader := key == leaderKey
		go func(mp *clustermetadatapb.MultiPooler, isLeader bool) {
			rpcCtx, cancel := context.WithTimeout(ctx, timeouts.RemoteOperationTimeout)
			defer cancel()
			_, err := c.rpcClient.Propose(rpcCtx, mp, req)
			results <- proposeResult{poolerName: mp.Id.Name, isLeader: isLeader, err: err}
		}(mp, isLeader)
	}

	var leaderErr error
	for range len(statuses) {
		r := <-results
		if r.err != nil {
			if r.isLeader {
				leaderErr = r.err
			} else {
				c.logger.WarnContext(ctx, "Propose failed for non-leader",
					"pooler", r.poolerName, "error", r.err)
			}
		} else if r.poolerName != "" {
			c.logger.InfoContext(ctx, "Propose succeeded",
				"pooler", r.poolerName, "is_leader", r.isLeader)
		}
	}

	if leaderErr != nil {
		leaderName := proposal.GetProposalLeader().GetId().GetName()
		return mterrors.Wrapf(leaderErr, "leader %s failed to accept proposal", leaderName)
	}
	return nil
}

// checkRecentAcceptance returns an error if any node in the cohort recently
// accepted a term revocation, which may indicate another coordinator is making
// a rule change.
func checkRecentAcceptance(ctx context.Context, logger *slog.Logger, cohort []*multiorchdatapb.PoolerHealthState) error {
	const backoffWindow = 4 * time.Second
	now := time.Now()
	for _, pooler := range cohort {
		rev := pooler.GetConsensusStatus().GetTermRevocation()
		if rev == nil || rev.CoordinatorInitiatedAt == nil {
			continue
		}
		timeSince := now.Sub(rev.CoordinatorInitiatedAt.AsTime())
		if timeSince >= 0 && timeSince < backoffWindow {
			logger.InfoContext(ctx, "Recent term acceptance detected, backing off",
				"pooler", pooler.MultiPooler.Id.Name,
				"accepted_term", rev.RevokedBelowTerm,
				"time_since_acceptance", timeSince)
			return fmt.Errorf("another coordinator started recruiting recently (%v ago), backing off",
				timeSince.Round(time.Millisecond))
		}
	}
	return nil
}

// buildCohortMaps returns pooler-by-ID and health-by-ID lookup maps built from
// a cohort slice. Keys are ClusterIDString values.
func buildCohortMaps(cohort []*multiorchdatapb.PoolerHealthState) (map[string]*clustermetadatapb.MultiPooler, map[string]*multiorchdatapb.PoolerHealthState) {
	poolerByID := make(map[string]*clustermetadatapb.MultiPooler, len(cohort))
	healthByID := make(map[string]*multiorchdatapb.PoolerHealthState, len(cohort))
	for _, p := range cohort {
		key := topoclient.ClusterIDString(p.MultiPooler.Id)
		poolerByID[key] = p.MultiPooler
		healthByID[key] = p
	}
	return poolerByID, healthByID
}
