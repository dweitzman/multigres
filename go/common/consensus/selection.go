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
	"github.com/jackc/pglogrepl"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
)

// CompareNodePositions compares two NodePositions.
// Returns -1 if a < b, 0 if a == b, 1 if a > b.
// Comparison is first by rule number (coordinator_term, then rule_subterm),
// then by LSN as a tiebreaker. A nil NodePosition is treated as a zero value
// (equal to a NodePosition with a nil rule and empty LSN).
func CompareNodePositions(a, b *clustermetadatapb.NodePosition) int {
	var aRule, bRule *clustermetadatapb.RuleNumber
	if a != nil {
		aRule = a.GetRule().GetRuleNumber()
	}
	if b != nil {
		bRule = b.GetRule().GetRuleNumber()
	}

	if cmp := CompareRuleNumbers(aRule, bRule); cmp != 0 {
		return cmp
	}

	// Rules are equal; break tie by LSN.
	aLSN := parseLSN(a.GetLsn())
	bLSN := parseLSN(b.GetLsn())
	if aLSN < bLSN {
		return -1
	}
	if aLSN > bLSN {
		return 1
	}
	return 0
}

// MostAdvancedIndex returns the index of the most advanced NodePosition in the
// slice. Returns -1 if the slice is empty, all positions are nil, or there is
// a tie for the most advanced position.
func MostAdvancedIndex(positions []*clustermetadatapb.NodePosition) int {
	bestIdx := -1
	tie := false

	for i, p := range positions {
		if p == nil {
			continue
		}
		if bestIdx == -1 {
			bestIdx = i
			tie = false
			continue
		}
		cmp := CompareNodePositions(positions[bestIdx], p)
		if cmp < 0 {
			bestIdx = i
			tie = false
		} else if cmp == 0 {
			tie = true
		}
	}

	if tie {
		return -1
	}
	return bestIdx
}

// parseLSN parses a PostgreSQL LSN string into a numeric value.
// Returns 0 for empty or unparseable strings.
func parseLSN(s string) pglogrepl.LSN {
	if s == "" {
		return 0
	}
	lsn, err := pglogrepl.ParseLSN(s)
	if err != nil {
		return 0
	}
	return lsn
}
