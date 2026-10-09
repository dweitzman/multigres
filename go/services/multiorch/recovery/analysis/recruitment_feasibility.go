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

package analysis

import (
	commonconsensus "github.com/multigres/multigres/go/common/consensus"
	"github.com/multigres/multigres/go/common/topoclient"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/services/multiorch/store"
)

// recruitmentFeasible reports whether a failover could establish a new term from
// the reachable subset: a strict majority of the outgoing cohort reachable, with
// the unreachable remainder unable to satisfy the durability policy. Thin
// readable wrapper over CheckSufficientRecruitment's error return.
func recruitmentFeasible(policy commonconsensus.DurabilityPolicy, cohort, reachable []*clustermetadatapb.ID) bool {
	return commonconsensus.CheckSufficientRecruitment(policy, cohort, reachable) == nil
}

// cohortWithout returns the cohort members other than exclude (all of them,
// regardless of reachability) — used to ask what recruitment would be possible if
// every member were reachable.
func cohortWithout(cohort []*clustermetadatapb.ID, exclude *clustermetadatapb.ID) []*clustermetadatapb.ID {
	excludeKey := topoclient.ComponentIDString(exclude)
	out := make([]*clustermetadatapb.ID, 0, len(cohort))
	for _, m := range cohort {
		if topoclient.ComponentIDString(m) != excludeKey {
			out = append(out, m)
		}
	}
	return out
}

// cohortSatisfying returns the outgoing-cohort members (minus exclude, if
// non-nil) whose rider passes pred. Shared by freshInitializedCohort and
// recruitableCohort, which differ only in which question pred asks.
func cohortSatisfying(sa *ShardAnalysis, cohort []*clustermetadatapb.ID, exclude *clustermetadatapb.ID, pred func(*store.Pooler) bool) []*clustermetadatapb.ID {
	byID := make(map[topoclient.ComponentID]*store.Pooler, len(sa.Analyses)+1)
	for _, pa := range sa.Analyses {
		if pa != nil {
			byID[topoclient.ComponentIDString(poolerID(pa))] = pa
		}
	}
	// The leader's rider lives on sa.Leader, not necessarily in Analyses.
	if sa.Leader != nil {
		byID[topoclient.ComponentIDString(poolerID(sa.Leader))] = sa.Leader
	}

	excludeKey := topoclient.ComponentIDString(exclude)
	var satisfying []*clustermetadatapb.ID
	for _, m := range cohort {
		if exclude != nil && topoclient.ComponentIDString(m) == excludeKey {
			continue
		}
		pa, ok := byID[topoclient.ComponentIDString(m)]
		if !ok {
			continue
		}
		if pred(pa) {
			satisfying = append(satisfying, m)
		}
	}
	return satisfying
}

// freshInitializedCohort returns the outgoing-cohort members we currently
// have a fresh, initialized observation for — i.e. members whose report is
// usable evidence, regardless of whether they could actually be recruited
// (see freshAndInitialized's doc). Used only to judge "do we have any
// trustworthy signal at all," not recruitment feasibility.
func freshInitializedCohort(sa *ShardAnalysis, cohort []*clustermetadatapb.ID, exclude *clustermetadatapb.ID) []*clustermetadatapb.ID {
	return cohortSatisfying(sa, cohort, exclude, func(pa *store.Pooler) bool {
		return freshAndInitialized(pa, sa.Now, sa.Policy.ObservationFreshness)
	})
}

// recruitableCohort returns the outgoing-cohort members that are currently
// recruitable — the set we could use to establish a new rule. Recruitment
// forms the new term from the *outgoing cohort*, so membership is the rule's
// cohort intersected with recruitable poolers (CheckSufficientRecruitment
// also requires recruited ⊆ cohort). If exclude is non-nil that member is
// omitted — used to ask "could we recover if the leader were lost?" for the
// ShardAtRisk check.
func recruitableCohort(sa *ShardAnalysis, cohort []*clustermetadatapb.ID, exclude *clustermetadatapb.ID) []*clustermetadatapb.ID {
	return cohortSatisfying(sa, cohort, exclude, func(pa *store.Pooler) bool {
		return recruitable(pa, sa.Now, sa.Policy.ObservationFreshness)
	})
}

// hasUsableShardHealth reports whether orch has at least one fresh, valid,
// initialized observation of a shard pooler to reason from. Without one, orch is
// blind: its view of the rule/leader comes only from stale health, so it must not
// convict the leader (see emitFailover, which reports NoHealthyCohortMembers then).
// This asks freshness/initialization, not recruitability — a draining or
// recruit-blocked pooler's report is still real, trustworthy evidence; it just
// can't win a Recruit round (see freshAndInitialized vs recruitable).
//
// This is exactly freshInitializedCohort being non-empty: the leader is itself a
// cohort member (the rule's CohortMembers includes it, which is why emitFailover
// recruits with exclude=nil), so a fresh leader already counts here.
func hasUsableShardHealth(sa *ShardAnalysis, cohort []*clustermetadatapb.ID) bool {
	return len(freshInitializedCohort(sa, cohort, nil)) > 0
}
