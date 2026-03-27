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

// Package consensus provides shared utilities for the Multigres consensus protocol.
package consensus

import (
	"fmt"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
)

// CompareRuleNumbers compares two RuleNumbers lexicographically.
// Returns a negative number if a < b, zero if a == b, positive if a > b.
// Nil is treated as the zero value (coordinator_term=0, rule_subterm=0).
func CompareRuleNumbers(a, b *clustermetadatapb.RuleNumber) int {
	aCoord, aSub := int64(0), int64(0)
	if a != nil {
		aCoord = a.GetCoordinatorTerm()
		aSub = a.GetRuleSubterm()
	}

	bCoord, bSub := int64(0), int64(0)
	if b != nil {
		bCoord = b.GetCoordinatorTerm()
		bSub = b.GetRuleSubterm()
	}

	if aCoord != bCoord {
		if aCoord < bCoord {
			return -1
		}
		return 1
	}
	if aSub != bSub {
		if aSub < bSub {
			return -1
		}
		return 1
	}
	return 0
}

// RuleNumberIsZero reports whether r is the zero value (both fields are 0 or r is nil).
func RuleNumberIsZero(r *clustermetadatapb.RuleNumber) bool {
	if r == nil {
		return true
	}
	return r.GetCoordinatorTerm() == 0 && r.GetRuleSubterm() == 0
}

// RuleNumberString returns a human-readable representation of r, e.g. "5.3".
// Returns "0.0" for nil or zero values.
func RuleNumberString(r *clustermetadatapb.RuleNumber) string {
	if r == nil {
		return "0.0"
	}
	return fmt.Sprintf("%d.%d", r.GetCoordinatorTerm(), r.GetRuleSubterm())
}
