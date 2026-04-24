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
	"sync"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	commonconsensus "github.com/multigres/multigres/go/common/consensus"
	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/timeouts"
	"github.com/multigres/multigres/go/common/topoclient"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	consensusdatapb "github.com/multigres/multigres/go/pb/consensusdata"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	multiorchdatapb "github.com/multigres/multigres/go/pb/multiorchdata"
)

// recruitAndPropose recruits all nodes in the cohort under a new term, validates
// that the recruited set satisfies consensus safety, and builds a CoordinatorProposal
// via CheckSufficientRecruitment. Returns the validated proposal and the health states
// of all nodes that accepted recruitment.
func (c *Coordinator) recruitAndPropose(
	ctx context.Context,
	shardID string,
	cohort []*multiorchdatapb.PoolerHealthState,
	policy *clustermetadatapb.DurabilityPolicy,
	proposedTerm int64,
) (*consensusdatapb.CoordinatorProposal, []*multiorchdatapb.PoolerHealthState, error) {
	termRevocation := &consensusdatapb.TermRevocation{
		RevokedBelowTerm:       proposedTerm,
		AcceptedCoordinatorId:  c.coordinatorID,
		CoordinatorInitiatedAt: timestamppb.Now(),
	}

	recruited, err := c.recruitNodes(ctx, cohort, termRevocation)
	if err != nil {
		return nil, nil, mterrors.Wrap(err, "failed to recruit poolers")
	}

	c.logger.InfoContext(ctx, "recruited poolers", "shard", shardID, "count", len(recruited))

	if len(recruited) == 0 {
		return nil, nil, mterrors.New(mtrpcpb.Code_UNAVAILABLE, "no poolers accepted the term")
	}

	statuses := make([]*consensusdatapb.ConsensusStatus, len(recruited))
	for i, r := range recruited {
		statuses[i] = r.consensusStatus
	}

	proposal, err := commonconsensus.CheckSufficientRecruitment(statuses,
		func(result commonconsensus.RecruitmentResult) (*consensusdatapb.CoordinatorProposal, error) {
			return c.buildProposal(ctx, result, recruited, policy)
		},
	)
	if err != nil {
		return nil, nil, mterrors.Wrapf(err, "recruitment validation failed for shard %s", shardID)
	}

	recruitedHealthStates := make([]*multiorchdatapb.PoolerHealthState, len(recruited))
	for i, r := range recruited {
		recruitedHealthStates[i] = r.pooler
	}
	return proposal, recruitedHealthStates, nil
}

// buildProposal is the CheckSufficientRecruitment callback. It selects the best
// eligible leader and constructs a CoordinatorProposal from the recruitment result.
func (c *Coordinator) buildProposal(
	ctx context.Context,
	result commonconsensus.RecruitmentResult,
	recruited []recruitmentResult,
	policy *clustermetadatapb.DurabilityPolicy,
) (*consensusdatapb.CoordinatorProposal, error) {
	leader, err := c.selectLeaderFromEligible(ctx, result.EligibleLeaders, recruited)
	if err != nil {
		return nil, err
	}

	// Proposed cohort: all recruited nodes. This preserves the existing behaviour
	// where nodes unreachable during recruitment are implicitly removed from the cohort.
	cohortMembers := make([]*clustermetadatapb.ID, 0, len(recruited))
	for _, r := range recruited {
		if r.pooler.MultiPooler != nil && r.pooler.MultiPooler.Id != nil {
			cohortMembers = append(cohortMembers, r.pooler.MultiPooler.Id)
		}
	}

	var ruleNumber *consensusdatapb.RuleNumber
	if result.HungProposal != nil {
		// Re-propose the hung rule with the same rule_number; only the leader changes.
		ruleNumber = result.HungProposal.GetRuleNumber()
	} else {
		ruleNumber = &consensusdatapb.RuleNumber{
			CoordinatorTerm: result.TermRevocation.GetRevokedBelowTerm(),
			LeaderSubterm:   0, // assigned by updateRule on the leader node
		}
	}

	candidatePort := int32(0)
	if leader.pooler.MultiPooler.PortMap != nil {
		candidatePort = leader.pooler.MultiPooler.PortMap["postgres"]
	}

	return &consensusdatapb.CoordinatorProposal{
		TermRevocation: result.TermRevocation,
		ProposalLeader: &consensusdatapb.ProposalLeader{
			Id:           leader.pooler.MultiPooler.Id,
			Host:         leader.pooler.MultiPooler.Hostname,
			PostgresPort: candidatePort,
		},
		ProposedRule: &consensusdatapb.ShardRule{
			RuleNumber:       ruleNumber,
			PrimaryId:        leader.pooler.MultiPooler.Id,
			CohortMembers:    cohortMembers,
			DurabilityPolicy: policy,
			CoordinatorId:    c.coordinatorID,
			CreationTime:     timestamppb.Now(),
		},
		RecruitmentPosition: &consensusdatapb.RecruitmentPosition{
			Lsn: leader.consensusStatus.GetCurrentPosition().GetLsn(),
			RuleNumber: leader.consensusStatus.GetCurrentPosition().GetRule().GetRuleNumber(),
		},
	}, nil
}

// selectLeaderFromEligible picks a leader from the nodes that CheckSufficientRecruitment
// identified as eligible. All eligible nodes are at the same BestRule rule_number so any
// of them can safely lead; the only filtering done here is skipping resigned candidates.
func (c *Coordinator) selectLeaderFromEligible(
	ctx context.Context,
	eligible []*consensusdatapb.ConsensusStatus,
	recruited []recruitmentResult,
) (*recruitmentResult, error) {
	recruitedByID := make(map[string]*recruitmentResult, len(recruited))
	for i := range recruited {
		key := topoclient.ClusterIDString(recruited[i].pooler.MultiPooler.Id)
		recruitedByID[key] = &recruited[i]
	}

	for _, cs := range eligible {
		key := topoclient.ClusterIDString(cs.GetId())
		r, ok := recruitedByID[key]
		if !ok {
			c.logger.WarnContext(ctx, "eligible leader not found in recruited set", "id", key)
			continue
		}
		if poolerRequestingDemotion(r.pooler) {
			c.logger.InfoContext(ctx, "skipping resigned candidate", "pooler", r.pooler.MultiPooler.Id.Name)
			continue
		}
		c.logger.InfoContext(ctx, "selected leader from eligible candidates",
			"pooler", r.pooler.MultiPooler.Id.Name)
		return r, nil
	}

	return nil, mterrors.New(mtrpcpb.Code_UNAVAILABLE,
		"no valid candidate found among eligible leaders")
}

// discoverMaxTerm finds the maximum consensus term from cached health state.
// This uses the TermRevocation data already populated by health checks, avoiding extra RPCs.
func (c *Coordinator) discoverMaxTerm(cohort []*multiorchdatapb.PoolerHealthState) (int64, error) {
	var maxTerm int64

	for _, pooler := range cohort {
		// Invariant: poolers in the cohort with successful health checks must have TermRevocation populated
		if pooler.IsLastCheckValid && pooler.GetStatus().GetTermRevocation() == nil {
			return 0, mterrors.Errorf(mtrpcpb.Code_INTERNAL,
				"healthy pooler %s in cohort missing term revocation data - health check invariant violated",
				pooler.MultiPooler.Id.Name)
		}

		if tr := pooler.GetStatus().GetTermRevocation(); tr != nil && tr.GetRevokedBelowTerm() > maxTerm {
			maxTerm = tr.GetRevokedBelowTerm()
		}
	}

	// Invariant: at least one pooler in the cohort must have a term > 0
	if maxTerm == 0 {
		return 0, mterrors.Errorf(mtrpcpb.Code_FAILED_PRECONDITION,
			"no poolers in cohort have initialized consensus term - cannot discover max term")
	}

	return maxTerm, nil
}

// recruitmentResult captures recruitment outcome and WAL position from Recruit response.
type recruitmentResult struct {
	pooler          *multiorchdatapb.PoolerHealthState
	consensusStatus *consensusdatapb.ConsensusStatus
}

// poolerRequestingDemotion reports whether the pooler's cached health state shows it has
// voluntarily requested to be replaced (REQUESTING_DEMOTION signal).
// This duplicates the logic in recovery/types.PrimaryNeedsReplacement to avoid a circular import
// (recovery imports consensus; consensus cannot import recovery).
func poolerRequestingDemotion(pooler *multiorchdatapb.PoolerHealthState) bool {
	ls := pooler.GetConsensusStatus().GetAvailabilityStatus().GetLeadershipStatus()
	return ls != nil &&
		ls.Signal == clustermetadatapb.LeadershipSignal_LEADERSHIP_SIGNAL_REQUESTING_DEMOTION &&
		ls.PrimaryTerm != 0
}

// recruitNodes sends Recruit RPC to all poolers in parallel and returns those that accepted.
func (c *Coordinator) recruitNodes(ctx context.Context, cohort []*multiorchdatapb.PoolerHealthState, termRevocation *consensusdatapb.TermRevocation) ([]recruitmentResult, error) {
	type result struct {
		pooler          *multiorchdatapb.PoolerHealthState
		consensusStatus *consensusdatapb.ConsensusStatus
		err             error
	}

	results := make(chan result, len(cohort))
	var wg sync.WaitGroup

	for _, pooler := range cohort {
		wg.Add(1)
		go func(n *multiorchdatapb.PoolerHealthState) {
			defer wg.Done()
			req := &consensusdatapb.RecruitRequest{
				TermRevocation: termRevocation,
			}
			rpcCtx, cancel := context.WithTimeout(ctx, timeouts.RemoteOperationTimeout)
			defer cancel()
			resp, err := c.rpcClient.Recruit(rpcCtx, n.MultiPooler, req)
			if err != nil {
				results <- result{pooler: n, err: err}
				return
			}
			results <- result{
				pooler:          n,
				consensusStatus: resp.GetConsensusStatus(),
			}
		}(pooler)
	}

	// Wait for all goroutines to complete
	go func() {
		wg.Wait()
		close(results)
	}()

	// Collect accepted poolers with their consensus status
	var recruited []recruitmentResult
	for r := range results {
		if r.err != nil {
			c.logger.WarnContext(ctx, "Recruit failed for pooler",
				"pooler", r.pooler.MultiPooler.Id.Name,
				"error", r.err)
			continue
		}

		lsn := r.consensusStatus.GetCurrentPosition().GetLsn()
		c.logger.InfoContext(ctx, "pooler accepted recruitment",
			"pooler", r.pooler.MultiPooler.Id.Name,
			"term", termRevocation.GetRevokedBelowTerm(),
			"lsn", lsn)

		recruited = append(recruited, recruitmentResult{
			pooler:          r.pooler,
			consensusStatus: r.consensusStatus,
		})
	}

	return recruited, nil
}

// sendPropose sends Propose RPC to all poolers in parallel.
// Returns an error if the candidate (leader) fails to accept the proposal.
func (c *Coordinator) sendPropose(ctx context.Context, poolers []*multiorchdatapb.PoolerHealthState, proposal *consensusdatapb.CoordinatorProposal) error {
	type result struct {
		pooler *multiorchdatapb.PoolerHealthState
		err    error
	}

	results := make(chan result, len(poolers))
	var wg sync.WaitGroup

	for _, pooler := range poolers {
		wg.Add(1)
		go func(p *multiorchdatapb.PoolerHealthState) {
			defer wg.Done()
			rpcCtx, cancel := context.WithTimeout(ctx, timeouts.RemoteOperationTimeout)
			defer cancel()
			_, err := c.rpcClient.Propose(rpcCtx, p.MultiPooler, &consensusdatapb.ProposeRequest{
				Proposal: proposal,
			})
			results <- result{pooler: p, err: err}
		}(pooler)
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	leaderName := proposal.GetProposalLeader().GetId().GetName()
	var candidateErr error

	for r := range results {
		if r.err != nil {
			if r.pooler.MultiPooler.Id.Name == leaderName {
				candidateErr = r.err
				c.logger.ErrorContext(ctx, "Propose failed on candidate",
					"pooler", r.pooler.MultiPooler.Id.Name,
					"error", r.err)
			} else {
				c.logger.WarnContext(ctx, "Propose failed on standby (non-fatal)",
					"pooler", r.pooler.MultiPooler.Id.Name,
					"error", r.err)
			}
		} else {
			c.logger.InfoContext(ctx, "Propose accepted",
				"pooler", r.pooler.MultiPooler.Id.Name)
		}
	}

	return candidateErr
}

// sendInform sends Inform RPC to all poolers in parallel. Errors are logged but not returned,
// since Inform is best-effort — poolers will learn the committed rule via health checks.
func (c *Coordinator) sendInform(ctx context.Context, poolers []*multiorchdatapb.PoolerHealthState, rule *consensusdatapb.ShardRule) {
	var wg sync.WaitGroup
	for _, pooler := range poolers {
		wg.Add(1)
		go func(p *multiorchdatapb.PoolerHealthState) {
			defer wg.Done()
			rpcCtx, cancel := context.WithTimeout(ctx, timeouts.RemoteOperationTimeout)
			defer cancel()
			if _, err := c.rpcClient.Inform(rpcCtx, p.MultiPooler, &consensusdatapb.InformRequest{
				Rule: rule,
			}); err != nil {
				c.logger.WarnContext(ctx, "Inform failed (non-fatal)",
					"pooler", p.MultiPooler.Id.Name,
					"error", err)
			}
		}(pooler)
	}
	wg.Wait()
}

// preVote performs a pre-election check to decide whether an election is
// likely to succeed. It prevents disruptive elections that would fail due to:
//  1. Not enough currently reachable poolers to achieve a valid recruitment
//     (candidacy + revocation) under the durability policy.
//  2. Another coordinator recently started an election (within last 10 seconds).
//
// Returns (canProceed, reason) where canProceed indicates if election should proceed.
func (c *Coordinator) preVote(ctx context.Context, cohort []*multiorchdatapb.PoolerHealthState, policy commonconsensus.DurabilityPolicy, proposedTerm int64) (bool, string) {
	now := time.Now()
	const recentAcceptanceWindow = 4 * time.Second

	// Filter cohort to poolers eligible to participate in recruitment right now.
	var eligiblePoolers []*multiorchdatapb.PoolerHealthState
	for _, pooler := range cohort {
		status := pooler.GetStatus()
		if pooler.IsLastCheckValid && status.GetIsInitialized() && status.GetTermRevocation() != nil && status.GetPostgresReady() {
			eligiblePoolers = append(eligiblePoolers, pooler)
		}
	}

	c.logger.InfoContext(ctx, "pre-vote health check",
		"eligible_poolers", len(eligiblePoolers),
		"total_poolers", len(cohort),
		"policy", policy.Description(),
		"proposed_term", proposedTerm)

	eligibleIDs := poolerIDs(eligiblePoolers)
	if err := policy.CheckSufficientRecruitment(poolerIDs(cohort), eligibleIDs); err != nil {
		return false, "not enough eligible poolers to achieve valid recruitment: " + err.Error()
	}
	if err := policy.CheckAchievable(eligibleIDs); err != nil {
		return false, "not enough eligible poolers to achieve valid recruitment: " + err.Error()
	}

	// Check if another coordinator recently started an election.
	for _, pooler := range eligiblePoolers {
		if tr := pooler.GetStatus().GetTermRevocation(); tr != nil && tr.GetCoordinatorInitiatedAt() != nil {
			lastAcceptanceTime := tr.GetCoordinatorInitiatedAt().AsTime()
			timeSinceAcceptance := now.Sub(lastAcceptanceTime)

			if timeSinceAcceptance < recentAcceptanceWindow && timeSinceAcceptance >= 0 {
				c.logger.InfoContext(ctx, "detected recent term acceptance, backing off to avoid disruption",
					"pooler", pooler.MultiPooler.Id.Name,
					"accepted_term", tr.GetRevokedBelowTerm(),
					"accepted_from", tr.GetAcceptedCoordinatorId().GetName(),
					"time_since_acceptance", timeSinceAcceptance,
					"backoff_window", recentAcceptanceWindow)

				return false, "another coordinator started election recently, backing off to avoid disruption"
			}
		}
	}

	c.logger.InfoContext(ctx, "pre-vote check passed",
		"proposed_term", proposedTerm,
		"eligible_poolers", len(eligiblePoolers))

	return true, ""
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
