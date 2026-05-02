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

// StandbySelector is called during a policy transition when there is a genuine
// choice between using one shared standby (one ack that satisfies both the
// outgoing and incoming NumSync requirements simultaneously) versus using one
// outgoing-exclusive and one incoming-exclusive standby (two acks, drawn from
// the healthier end of each policy's standby list).
//
// shared is the candidate shared standby; outExclusive and inExclusive are the
// current heads of the outgoing-only and incoming-only lists, ordered by
// replication fitness (most caught-up first).
//
// Return true to use the shared standby (fewer total acks required), false to
// use the exclusive pair (more acks, but both drawn from healthier candidates).
//
// A nil StandbySelector defaults to always preferring the shared standby.
type StandbySelector func(shared, outExclusive, inExclusive *clustermetadatapb.ID) bool

// TransitionPolicy represents a durability policy transition from Outgoing to Incoming.
//
// It is a DurabilityPolicy that models the window during which a rule change WAL record
// is being committed. Its BuildPrimaryDurabilityPostgresConfig returns the "both" GUC:
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
	// StandbySelector is consulted at each step of the representative-sample
	// fallback when there is a choice between a shared standby and an exclusive
	// pair. nil defaults to always preferring the shared standby.
	StandbySelector StandbySelector
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

// BuildPrimaryDurabilityPostgresConfig returns the "both" GUC config for the transition.
//
// NOTE: The cohort parameter is unused for TransitionPolicy — both cohorts are stored in
// OutgoingCohort and IncomingCohort. Callers should pass nil.
//
// The "both" GUC must be acknowledged by standbys valid under both the outgoing and incoming
// policies simultaneously. The algorithm first checks whether either policy implements
// BothGUCPolicy (tried outgoing-first); if so, the policy computes the
// result directly using type-specific heuristics. Otherwise, it falls back to a
// representative-sample approach: at each step, shared standbys (valid under both policies)
// are preferred by default, but StandbySelector may override the choice when a healthier
// exclusive pair is available.
// OutgoingCohort and IncomingCohort are expected to be ordered by replication fitness
// (most caught-up first) so earlier standbys are preferred.
//
// Returns an error when the "both" GUC cannot be constructed (e.g. incompatible commit levels).
func (p TransitionPolicy) BuildPrimaryDurabilityPostgresConfig(logger *slog.Logger, _ []*clustermetadatapb.ID, leader *clustermetadatapb.ID) (*PrimaryDurabilityPostgresConfig, error) {
	if b, ok := p.Outgoing.(BothGUCPolicy); ok {
		if result := b.BuildBothGUC(logger, leader, p.Incoming, p.OutgoingCohort, p.IncomingCohort); result != nil {
			return result, nil
		}
	}
	if b, ok := p.Incoming.(BothGUCPolicy); ok {
		if result := b.BuildBothGUC(logger, leader, p.Outgoing, p.IncomingCohort, p.OutgoingCohort); result != nil {
			return result, nil
		}
	}

	outgoingGUC, err := p.Outgoing.BuildPrimaryDurabilityPostgresConfig(logger, p.OutgoingCohort, leader)
	if err != nil {
		return nil, fmt.Errorf("outgoing GUC: %w", err)
	}
	afterGUC, err := p.Incoming.BuildPrimaryDurabilityPostgresConfig(logger, p.IncomingCohort, leader)
	if err != nil {
		return nil, fmt.Errorf("incoming GUC: %w", err)
	}
	return representativeSampleBothGUC(outgoingGUC, afterGUC, p.StandbySelector)
}

// representativeSampleBothGUC builds a "both" GUC by stepping through the shared,
// outgoing-exclusive, and incoming-exclusive standby lists, selecting the minimum
// number of standbys needed to satisfy both policies simultaneously.
//
// At each step where both policies still need acks AND all three lists have remaining
// candidates, prefer is called to choose between:
//   - one shared standby (satisfies both with one ack), or
//   - one outgoing-exclusive + one incoming-exclusive (two acks, from the healthier
//     end of each policy's dedicated list).
//
// When prefer is nil or there is no real choice (one of the exclusive lists is exhausted),
// a shared standby is used whenever available, falling back to the exclusive pair.
// Once only one policy still needs acks, candidates are drawn in their natural list order
// (exclusive standbys first, shared as fallback).
func representativeSampleBothGUC(outgoing, incoming *PrimaryDurabilityPostgresConfig, prefer StandbySelector) (*PrimaryDurabilityPostgresConfig, error) {
	if outgoing.SyncCommit != incoming.SyncCommit || outgoing.SyncMethod != incoming.SyncMethod {
		return nil, fmt.Errorf(
			"cannot build representative-sample GUC: incompatible commit levels (%v/%v vs %v/%v)",
			outgoing.SyncCommit, outgoing.SyncMethod,
			incoming.SyncCommit, incoming.SyncMethod,
		)
	}

	// Split the outgoing and incoming standby lists into three ordered pools:
	//   shared  – standbys present in both lists (in outgoing order); each ack
	//             satisfies both outgoing and incoming NumSync simultaneously.
	//   outOnly – standbys only in the outgoing list (in outgoing order).
	//   inOnly  – standbys only in the incoming list (in incoming order).
	shared := intersectStandbys(outgoing.SyncStandbyIDs, incoming.SyncStandbyIDs)
	outOnly := excludeStandbys(outgoing.SyncStandbyIDs, incoming.SyncStandbyIDs)
	inOnly := excludeStandbys(incoming.SyncStandbyIDs, outgoing.SyncStandbyIDs)

	// Rank maps restore the original list order when only one policy still needs
	// acks and shared and exclusive candidates must be compared directly.
	outRank := rankOf(outgoing.SyncStandbyIDs)
	inRank := rankOf(incoming.SyncStandbyIDs)

	remOut, remIn := outgoing.NumSync, incoming.NumSync
	si, oi, ii := 0, 0, 0 // cursors into shared, outOnly, inOnly
	reps := make([]*clustermetadatapb.ID, 0, remOut+remIn)

	for remOut > 0 || remIn > 0 {
		switch {
		case remOut > 0 && remIn > 0:
			// Both policies still need acks. Prefer shared when available; offer the
			// caller a choice only when all three pools have remaining candidates.
			hasShared := si < len(shared)
			hasOutOnly := oi < len(outOnly)
			hasInOnly := ii < len(inOnly)
			useShared := hasShared && (!hasOutOnly || !hasInOnly || prefer == nil || prefer(shared[si], outOnly[oi], inOnly[ii]))
			if useShared {
				reps = append(reps, shared[si])
				si++
			} else {
				reps = append(reps, outOnly[oi], inOnly[ii])
				oi++
				ii++
			}
			remOut--
			remIn--

		case remOut > 0:
			// Only outgoing acks remain: pick the next candidate in outgoing order
			// across remaining outOnly and shared standbys.
			if oi < len(outOnly) && (si >= len(shared) || outRank[topoclient.ClusterIDString(outOnly[oi])] < outRank[topoclient.ClusterIDString(shared[si])]) {
				reps = append(reps, outOnly[oi])
				oi++
			} else {
				reps = append(reps, shared[si])
				si++
			}
			remOut--

		default: // remIn > 0
			// Only incoming acks remain: pick the next candidate in incoming order
			// across remaining inOnly and shared standbys.
			if ii < len(inOnly) && (si >= len(shared) || inRank[topoclient.ClusterIDString(inOnly[ii])] < inRank[topoclient.ClusterIDString(shared[si])]) {
				reps = append(reps, inOnly[ii])
				ii++
			} else {
				reps = append(reps, shared[si])
				si++
			}
			remIn--
		}
	}

	return &PrimaryDurabilityPostgresConfig{
		SyncCommit:     outgoing.SyncCommit,
		SyncMethod:     outgoing.SyncMethod,
		NumSync:        len(reps),
		SyncStandbyIDs: reps,
	}, nil
}

// rankOf returns a map from ClusterIDString to position in standbys, used to
// restore the original list order when merging shared and exclusive candidates
// during single-side selection.
func rankOf(standbys []*clustermetadatapb.ID) map[string]int {
	ranks := make(map[string]int, len(standbys))
	for i, s := range standbys {
		ranks[topoclient.ClusterIDString(s)] = i
	}
	return ranks
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
