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

package consensus_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/multigres/multigres/go/common/consensus"
)

func TestRecruitmentCommitmentIsRevokedBy(t *testing.T) {
	const (
		coordA consensus.NodeID = "coord-a"
		coordB consensus.NodeID = "coord-b"
	)

	type tc struct {
		name     string
		existing consensus.RecruitmentCommitment
		proposed consensus.RecruitmentCommitment
		want     bool
	}

	tests := []tc{
		// Higher AtTermSeq always revokes, regardless of ProposedSeq or coordinator.
		{
			name:     "higher AtTermSeq same ProposedSeq same coord",
			existing: consensus.RecruitmentCommitment{CoordID: coordA, AtTermSeq: 1, ProposedSeq: 3},
			proposed: consensus.RecruitmentCommitment{CoordID: coordA, AtTermSeq: 2, ProposedSeq: 3},
			want:     true,
		},
		{
			name:     "higher AtTermSeq lower ProposedSeq different coord",
			existing: consensus.RecruitmentCommitment{CoordID: coordA, AtTermSeq: 1, ProposedSeq: 5},
			proposed: consensus.RecruitmentCommitment{CoordID: coordB, AtTermSeq: 2, ProposedSeq: 3},
			want:     true,
		},
		// Higher ProposedSeq revokes when AtTermSeq is equal.
		{
			name:     "same AtTermSeq higher ProposedSeq same coord",
			existing: consensus.RecruitmentCommitment{CoordID: coordA, AtTermSeq: 1, ProposedSeq: 3},
			proposed: consensus.RecruitmentCommitment{CoordID: coordA, AtTermSeq: 1, ProposedSeq: 4},
			want:     true,
		},
		{
			name:     "same AtTermSeq higher ProposedSeq different coord",
			existing: consensus.RecruitmentCommitment{CoordID: coordA, AtTermSeq: 1, ProposedSeq: 3},
			proposed: consensus.RecruitmentCommitment{CoordID: coordB, AtTermSeq: 1, ProposedSeq: 4},
			want:     true,
		},
		// Equal (AtTermSeq, ProposedSeq) does NOT revoke — first-write-wins.
		// The idempotent case (same coordinator) is handled by == in the caller.
		{
			name:     "equal seqs same coord — idempotent, not a revocation",
			existing: consensus.RecruitmentCommitment{CoordID: coordA, AtTermSeq: 1, ProposedSeq: 3},
			proposed: consensus.RecruitmentCommitment{CoordID: coordA, AtTermSeq: 1, ProposedSeq: 3},
			want:     false,
		},
		{
			name:     "equal seqs different coord — first-write-wins, not revoked",
			existing: consensus.RecruitmentCommitment{CoordID: coordA, AtTermSeq: 1, ProposedSeq: 3},
			proposed: consensus.RecruitmentCommitment{CoordID: coordB, AtTermSeq: 1, ProposedSeq: 3},
			want:     false,
		},
		// Lower seqs never revoke.
		{
			name:     "lower ProposedSeq same AtTermSeq",
			existing: consensus.RecruitmentCommitment{CoordID: coordA, AtTermSeq: 1, ProposedSeq: 3},
			proposed: consensus.RecruitmentCommitment{CoordID: coordB, AtTermSeq: 1, ProposedSeq: 2},
			want:     false,
		},
		{
			// AtTermSeq is lower even though ProposedSeq is higher: the proposing
			// coordinator has stale base knowledge and must not displace one that
			// knows more.
			name:     "lower AtTermSeq higher ProposedSeq — stale base, not revoked",
			existing: consensus.RecruitmentCommitment{CoordID: coordA, AtTermSeq: 2, ProposedSeq: 3},
			proposed: consensus.RecruitmentCommitment{CoordID: coordB, AtTermSeq: 1, ProposedSeq: 5},
			want:     false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, tt.existing.IsRevokedBy(tt.proposed))
		})
	}
}

// TestRecruitmentCommitmentEquality verifies that Go struct equality serves as
// the idempotency check: same coordinator and same seqs are equal; anything
// else is not.
func TestRecruitmentCommitmentEquality(t *testing.T) {
	const (
		coordA consensus.NodeID = "coord-a"
		coordB consensus.NodeID = "coord-b"
	)

	base := consensus.RecruitmentCommitment{CoordID: coordA, AtTermSeq: 1, ProposedSeq: 3}

	assert.Equal(t, base, consensus.RecruitmentCommitment{CoordID: coordA, AtTermSeq: 1, ProposedSeq: 3},
		"identical structs should be equal")
	assert.NotEqual(t, base, consensus.RecruitmentCommitment{CoordID: coordB, AtTermSeq: 1, ProposedSeq: 3},
		"different coordinator should not be equal")
	assert.NotEqual(t, base, consensus.RecruitmentCommitment{CoordID: coordA, AtTermSeq: 2, ProposedSeq: 3},
		"different AtTermSeq should not be equal")
	assert.NotEqual(t, base, consensus.RecruitmentCommitment{CoordID: coordA, AtTermSeq: 1, ProposedSeq: 4},
		"different ProposedSeq should not be equal")
}
