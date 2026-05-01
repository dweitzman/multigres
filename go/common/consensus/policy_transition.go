// Copyright 2026 Supabase, Inc.
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

package consensus

import (
	"fmt"
	"log/slog"

	"github.com/multigres/multigres/go/common/topoclient"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
)

// TransitionPolicy represents a durability policy transition from Outgoing to Incoming.
//
// It is a DurabilityPolicy that models the window during which a rule change WAL record
// is being committed. Its BuildLeaderDurabilityPostgresConfig returns the "both" GUC:
// a Postgres sync-replication config whose standby set is the intersection of the outgoing
// and incoming standby sets, ensuring the WAL commit is acknowledged by standbys that are
// valid witnesses under both the old and new rules.
//
// IncomingCohortMode changes the policy's behavior:
//   - false (default): CheckAchievable requires both policies to be achievable;
//     CheckSufficientRecruitment uses the outgoing policy's requirements.
//   - true: both checks operate on the incoming policy and IncomingCohort only.
//     Used after the transition is decided (the rule has been committed and propagated).
type TransitionPolicy struct {
	Outgoing           DurabilityPolicy
	Incoming           DurabilityPolicy
	OutgoingCohort     []*clustermetadatapb.ID
	IncomingCohort     []*clustermetadatapb.ID
	IncomingCohortMode bool
}

// Compile-time check that TransitionPolicy implements DurabilityPolicy.
var _ DurabilityPolicy = TransitionPolicy{}

// CheckAchievable checks that the proposed cohort can achieve both the outgoing and
// incoming policies. In IncomingCohortMode, only the incoming policy is checked.
func (p TransitionPolicy) CheckAchievable(proposedCohort []*clustermetadatapb.ID) error {
	if p.IncomingCohortMode {
		return p.Incoming.CheckAchievable(p.IncomingCohort)
	}
	if err := p.Outgoing.CheckAchievable(proposedCohort); err != nil {
		return fmt.Errorf("outgoing policy not achievable: %w", err)
	}
	if err := p.Incoming.CheckAchievable(proposedCohort); err != nil {
		return fmt.Errorf("incoming policy not achievable: %w", err)
	}
	return nil
}

// CheckSufficientRecruitment delegates to the outgoing policy's requirements, since
// recruitment still operates under the old rule during a transition. In
// IncomingCohortMode, it delegates to the incoming policy instead.
func (p TransitionPolicy) CheckSufficientRecruitment(cohort, recruited []*clustermetadatapb.ID) error {
	if p.IncomingCohortMode {
		return p.Incoming.CheckSufficientRecruitment(p.IncomingCohort, recruited)
	}
	return p.Outgoing.CheckSufficientRecruitment(cohort, recruited)
}

// BuildLeaderDurabilityPostgresConfig returns the "both" GUC config for the transition.
//
// NOTE: The cohort parameter is unused for TransitionPolicy — both cohorts are stored in
// OutgoingCohort and IncomingCohort. Callers should pass nil.
//
// The "both" GUC must be acknowledged by standbys valid under both the outgoing and incoming
// policies simultaneously. The algorithm first checks whether either policy implements
// IntersectableDurabilityPolicy (tried outgoing-first); if so, the policy computes the
// result directly using type-specific heuristics. Otherwise, it falls back to a
// representative-sample approach: select the minimum number of standbys from each policy's
// standby set needed to satisfy both policies simultaneously and require all of them.
// OutgoingCohort and IncomingCohort are expected to be ordered by replication fitness
// (most caught-up first) so earlier standbys are preferred.
//
// Returns an error when the "both" GUC cannot be constructed (e.g. incompatible commit levels).
func (p TransitionPolicy) BuildLeaderDurabilityPostgresConfig(logger *slog.Logger, _ []*clustermetadatapb.ID, leader *clustermetadatapb.ID) (*LeaderDurabilityPostgresConfig, error) {
	if b, ok := p.Outgoing.(IntersectableDurabilityPolicy); ok {
		if result := b.BuildIntersectionGUC(logger, leader, p.Incoming, p.OutgoingCohort, p.IncomingCohort); result != nil {
			return result, nil
		}
	}
	if b, ok := p.Incoming.(IntersectableDurabilityPolicy); ok {
		if result := b.BuildIntersectionGUC(logger, leader, p.Outgoing, p.IncomingCohort, p.OutgoingCohort); result != nil {
			return result, nil
		}
	}

	outgoingGUC, err := p.Outgoing.BuildLeaderDurabilityPostgresConfig(logger, p.OutgoingCohort, leader)
	if err != nil {
		return nil, fmt.Errorf("outgoing GUC: %w", err)
	}
	afterGUC, err := p.Incoming.BuildLeaderDurabilityPostgresConfig(logger, p.IncomingCohort, leader)
	if err != nil {
		return nil, fmt.Errorf("incoming GUC: %w", err)
	}
	return representativeSampleBothGUC(outgoingGUC, afterGUC)
}

// representativeSampleBothGUC builds a "both" GUC by selecting the minimum number of
// standbys from each policy's standby set needed to satisfy both simultaneously.
//
// Shared standbys (in both sets) are counted toward both policies' NumSync requirements,
// reducing the total number of standbys that must acknowledge. Within each set, earlier
// entries are preferred (the caller is expected to pass cohorts ordered by fitness).
func representativeSampleBothGUC(outgoing, incoming *LeaderDurabilityPostgresConfig) (*LeaderDurabilityPostgresConfig, error) {
	if outgoing.SyncCommit != incoming.SyncCommit || outgoing.SyncMethod != incoming.SyncMethod {
		return nil, fmt.Errorf(
			"cannot build representative-sample GUC: incompatible commit levels (%v/%v vs %v/%v)",
			outgoing.SyncCommit, outgoing.SyncMethod,
			incoming.SyncCommit, incoming.SyncMethod,
		)
	}

	shared := intersectStandbys(outgoing.SyncStandbyIDs, incoming.SyncStandbyIDs)
	outOnly := excludeStandbys(outgoing.SyncStandbyIDs, incoming.SyncStandbyIDs)
	inOnly := excludeStandbys(incoming.SyncStandbyIDs, outgoing.SyncStandbyIDs)

	// Each shared standby satisfies both policies simultaneously; use as many as possible.
	sharedUsed := min(len(shared), min(outgoing.NumSync, incoming.NumSync))
	outNeed := outgoing.NumSync - sharedUsed
	inNeed := incoming.NumSync - sharedUsed

	if len(outOnly) < outNeed || len(inOnly) < inNeed {
		return nil, fmt.Errorf(
			"not enough standbys for representative sample (need %d outgoing-only and %d incoming-only, have %d and %d)",
			outNeed, inNeed, len(outOnly), len(inOnly),
		)
	}

	reps := make([]*clustermetadatapb.ID, 0, sharedUsed+outNeed+inNeed)
	reps = append(reps, shared[:sharedUsed]...)
	reps = append(reps, outOnly[:outNeed]...)
	reps = append(reps, inOnly[:inNeed]...)

	return &LeaderDurabilityPostgresConfig{
		SyncCommit:     outgoing.SyncCommit,
		SyncMethod:     outgoing.SyncMethod,
		NumSync:        len(reps),
		SyncStandbyIDs: reps,
	}, nil
}

// cohortIsSubsetOf reports whether every element of a appears in b.
func cohortIsSubsetOf(a, b []*clustermetadatapb.ID) bool {
	return len(intersectStandbys(a, b)) == len(a)
}

// excludeStandbys returns elements of a that do not appear in b.
func excludeStandbys(a, b []*clustermetadatapb.ID) []*clustermetadatapb.ID {
	bKeys := poolerKeysOf(b)
	result := make([]*clustermetadatapb.ID, 0, len(a))
	for _, id := range a {
		if _, ok := bKeys[topoclient.ClusterIDString(id)]; !ok {
			result = append(result, id)
		}
	}
	return result
}

// Description returns a human-readable summary of the transition policy.
func (p TransitionPolicy) Description() string {
	return fmt.Sprintf("Transition(%s → %s)", p.Outgoing.Description(), p.Incoming.Description())
}
