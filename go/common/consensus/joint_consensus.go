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
	"errors"
	"fmt"

	"github.com/multigres/multigres/go/common/topoclient"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
)

// JointPolicy returns the DurabilityPolicy and cohort that must be used during
// a cohort-change transition so that writes are acknowledged by enough nodes to
// satisfy both the before and after policies simultaneously.
//
// It recognises two cases for AT_LEAST_N policies:
//   - One cohort is a strict or equal subset of the other: the joint policy is
//     AT_LEAST_N(max(N_before, N_after)) applied to the smaller cohort.
//   - Equal cohorts with different N values fall out as a special case above.
//
// Returns an error if no joint expression can be determined.
func JointPolicy(
	beforePolicy DurabilityPolicy, beforeCohort []*clustermetadatapb.ID,
	afterPolicy DurabilityPolicy, afterCohort []*clustermetadatapb.ID,
) (DurabilityPolicy, []*clustermetadatapb.ID, error) {
	beforeN, beforeIsAtLeastN := toAtLeastN(beforePolicy)
	afterN, afterIsAtLeastN := toAtLeastN(afterPolicy)
	if beforeIsAtLeastN && afterIsAtLeastN {
		return jointAtLeastN(beforeN, beforeCohort, afterN, afterCohort)
	}
	return nil, nil, fmt.Errorf("no known joint quorum expression for policies %s and %s",
		beforePolicy.Description(), afterPolicy.Description())
}

func toAtLeastN(p DurabilityPolicy) (int, bool) {
	a, ok := p.(AtLeastNPolicy)
	return a.N, ok
}

func jointAtLeastN(
	beforeN int, beforeCohort []*clustermetadatapb.ID,
	afterN int, afterCohort []*clustermetadatapb.ID,
) (DurabilityPolicy, []*clustermetadatapb.ID, error) {
	jointN := max(beforeN, afterN)
	// afterCohort ⊆ beforeCohort: the after-cohort is the smaller one.
	if isSubset(afterCohort, beforeCohort) {
		return AtLeastNPolicy{N: jointN}, afterCohort, nil
	}
	// beforeCohort ⊆ afterCohort: the before-cohort is the smaller one.
	if isSubset(beforeCohort, afterCohort) {
		return AtLeastNPolicy{N: jointN}, beforeCohort, nil
	}
	return nil, nil, errors.New("AT_LEAST_N joint quorum requires one cohort to be a subset of the other")
}

// isSubset returns true if every element of sub is present in super (by pooler key).
func isSubset(sub, super []*clustermetadatapb.ID) bool {
	superKeys := poolerKeysOf(super)
	for _, id := range sub {
		if _, ok := superKeys[topoclient.ClusterIDString(id)]; !ok {
			return false
		}
	}
	return true
}
