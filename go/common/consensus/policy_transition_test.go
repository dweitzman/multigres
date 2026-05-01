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
	"testing"

	"github.com/stretchr/testify/require"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
)

func TestTransitionPolicy_CheckAchievable(t *testing.T) {
	outCohort := []*clustermetadatapb.ID{id("p1", "c1"), id("p2", "c2")}
	inCohort := []*clustermetadatapb.ID{id("p2", "c2"), id("p3", "c3")}

	t.Run("both achievable returns nil", func(t *testing.T) {
		p := TransitionPolicy{
			Outgoing:       AtLeastNPolicy{N: 2},
			Incoming:       AtLeastNPolicy{N: 2},
			OutgoingCohort: outCohort,
			IncomingCohort: inCohort,
		}
		require.NoError(t, p.CheckAchievable([]*clustermetadatapb.ID{
			id("p1", "c1"), id("p2", "c2"), id("p3", "c3"),
		}))
	})

	t.Run("outgoing not achievable", func(t *testing.T) {
		p := TransitionPolicy{
			Outgoing:       AtLeastNPolicy{N: 5},
			Incoming:       AtLeastNPolicy{N: 2},
			OutgoingCohort: outCohort,
			IncomingCohort: inCohort,
		}
		err := p.CheckAchievable([]*clustermetadatapb.ID{id("p1", "c1"), id("p2", "c2")})
		require.EqualError(t, err, "outgoing policy not achievable: durability not achievable: proposed cohort has 2 poolers, required 5")
	})

	t.Run("incoming not achievable", func(t *testing.T) {
		p := TransitionPolicy{
			Outgoing:       AtLeastNPolicy{N: 2},
			Incoming:       AtLeastNPolicy{N: 5},
			OutgoingCohort: outCohort,
			IncomingCohort: inCohort,
		}
		err := p.CheckAchievable([]*clustermetadatapb.ID{id("p1", "c1"), id("p2", "c2")})
		require.EqualError(t, err, "incoming policy not achievable: durability not achievable: proposed cohort has 2 poolers, required 5")
	})

	t.Run("IncomingCohortMode only checks incoming against IncomingCohort", func(t *testing.T) {
		p := TransitionPolicy{
			Outgoing:           AtLeastNPolicy{N: 5}, // would fail if checked
			Incoming:           AtLeastNPolicy{N: 2},
			OutgoingCohort:     outCohort,
			IncomingCohort:     inCohort,
			IncomingCohortMode: true,
		}
		// IncomingCohort has 2 members, Incoming requires 2 → achievable
		require.NoError(t, p.CheckAchievable(nil))
	})
}

func TestTransitionPolicy_CheckSufficientRecruitment(t *testing.T) {
	outCohort := []*clustermetadatapb.ID{id("p1", "c1"), id("p2", "c2"), id("p3", "c3")}
	inCohort := []*clustermetadatapb.ID{id("p2", "c2"), id("p3", "c3"), id("p4", "c4")}

	t.Run("default mode delegates to outgoing policy", func(t *testing.T) {
		p := TransitionPolicy{
			Outgoing:       AtLeastNPolicy{N: 2},
			Incoming:       AtLeastNPolicy{N: 2},
			OutgoingCohort: outCohort,
			IncomingCohort: inCohort,
		}
		// 2 of 3 recruited — majority (need 2) and revocation (1 missing < N=2) both pass
		require.NoError(t, p.CheckSufficientRecruitment(outCohort, []*clustermetadatapb.ID{id("p1", "c1"), id("p2", "c2")}))
	})

	t.Run("default mode: outgoing policy failure surfaces", func(t *testing.T) {
		p := TransitionPolicy{
			Outgoing:       AtLeastNPolicy{N: 2},
			Incoming:       AtLeastNPolicy{N: 2},
			OutgoingCohort: outCohort,
			IncomingCohort: inCohort,
		}
		// 1 of 3 — fails majority
		err := p.CheckSufficientRecruitment(outCohort, []*clustermetadatapb.ID{id("p1", "c1")})
		require.EqualError(t, err, "majority not satisfied: recruited 1 of 3 cohort poolers, need at least 2")
	})

	t.Run("IncomingCohortMode delegates to incoming policy against IncomingCohort", func(t *testing.T) {
		p := TransitionPolicy{
			Outgoing:           AtLeastNPolicy{N: 5},
			Incoming:           AtLeastNPolicy{N: 2},
			OutgoingCohort:     outCohort,
			IncomingCohort:     inCohort,
			IncomingCohortMode: true,
		}
		// 2 of 3 in inCohort passes
		require.NoError(t, p.CheckSufficientRecruitment(nil, []*clustermetadatapb.ID{id("p2", "c2"), id("p3", "c3")}))
	})
}

func TestTransitionPolicy_BuildPrimaryDurabilityPostgresConfig(t *testing.T) {
	logger := testLogger()
	leader := id("primary", "cell-p")
	mcLeader := id("primary", "cell-a")

	commitON := multipoolermanagerdatapb.SynchronousCommitLevel_SYNCHRONOUS_COMMIT_ON
	methodANY := multipoolermanagerdatapb.SynchronousMethod_SYNCHRONOUS_METHOD_ANY

	tests := []struct {
		name         string
		leader       *clustermetadatapb.ID // nil = default leader
		policy       TransitionPolicy
		wantErr      string
		wantCommit   multipoolermanagerdatapb.SynchronousCommitLevel
		wantMethod   multipoolermanagerdatapb.SynchronousMethod
		wantNumSync  int
		wantStandbys []string
	}{
		// --- AtLeastN + AtLeastN: same-type intersection heuristic ---
		// When both policies are the same type, BuildBothGUC resolves the
		// transition without falling back to the representative-sample algorithm.
		// The binding constraint is whichever side has the smaller cohort (same N)
		// or the higher N (same cohort) — the "both" GUC must satisfy the stricter policy.
		{
			name: "same N, outgoing cohort ⊆ incoming: smaller cohort wins",
			policy: TransitionPolicy{
				Outgoing:       AtLeastNPolicy{N: 2},
				Incoming:       AtLeastNPolicy{N: 2},
				OutgoingCohort: []*clustermetadatapb.ID{leader, id("p1", "c1"), id("p2", "c2")},
				IncomingCohort: []*clustermetadatapb.ID{leader, id("p1", "c1"), id("p2", "c2"), id("p3", "c3")},
			},
			wantCommit:   commitON,
			wantMethod:   methodANY,
			wantNumSync:  1,
			wantStandbys: []string{"c1_p1", "c2_p2"},
		},
		{
			name: "same N, incoming cohort ⊆ outgoing: smaller cohort wins",
			policy: TransitionPolicy{
				Outgoing:       AtLeastNPolicy{N: 2},
				Incoming:       AtLeastNPolicy{N: 2},
				OutgoingCohort: []*clustermetadatapb.ID{leader, id("p1", "c1"), id("p2", "c2"), id("p3", "c3")},
				IncomingCohort: []*clustermetadatapb.ID{leader, id("p1", "c1"), id("p2", "c2")},
			},
			wantCommit:   commitON,
			wantMethod:   methodANY,
			wantNumSync:  1,
			wantStandbys: []string{"c1_p1", "c2_p2"},
		},
		{
			name: "same N, equal cohorts",
			policy: TransitionPolicy{
				Outgoing:       AtLeastNPolicy{N: 2},
				Incoming:       AtLeastNPolicy{N: 2},
				OutgoingCohort: []*clustermetadatapb.ID{leader, id("p1", "c1"), id("p2", "c2")},
				IncomingCohort: []*clustermetadatapb.ID{leader, id("p1", "c1"), id("p2", "c2")},
			},
			wantCommit:   commitON,
			wantMethod:   methodANY,
			wantNumSync:  1,
			wantStandbys: []string{"c1_p1", "c2_p2"},
		},
		{
			name: "same cohort, outgoing N larger: higher N wins",
			policy: TransitionPolicy{
				Outgoing:       AtLeastNPolicy{N: 3},
				Incoming:       AtLeastNPolicy{N: 2},
				OutgoingCohort: []*clustermetadatapb.ID{leader, id("p1", "c1"), id("p2", "c2"), id("p3", "c3")},
				IncomingCohort: []*clustermetadatapb.ID{leader, id("p1", "c1"), id("p2", "c2"), id("p3", "c3")},
			},
			wantCommit:   commitON,
			wantMethod:   methodANY,
			wantNumSync:  2,
			wantStandbys: []string{"c1_p1", "c2_p2", "c3_p3"},
		},
		{
			name: "same cohort, incoming N larger: higher N wins",
			policy: TransitionPolicy{
				Outgoing:       AtLeastNPolicy{N: 2},
				Incoming:       AtLeastNPolicy{N: 3},
				OutgoingCohort: []*clustermetadatapb.ID{leader, id("p1", "c1"), id("p2", "c2"), id("p3", "c3")},
				IncomingCohort: []*clustermetadatapb.ID{leader, id("p1", "c1"), id("p2", "c2"), id("p3", "c3")},
			},
			wantCommit:   commitON,
			wantMethod:   methodANY,
			wantNumSync:  2,
			wantStandbys: []string{"c1_p1", "c2_p2", "c3_p3"},
		},

		// --- MultiCell + MultiCell: same-type intersection heuristic ---
		// The MultiCellPolicy intersection heuristic works identically to
		// AtLeastN's: the smaller cohort's config satisfies both policies.
		// Unlike AtLeastN, MultiCell excludes standbys in the leader's cell.
		{
			name:   "same N, outgoing cohort ⊆ incoming: smaller cohort wins",
			leader: mcLeader,
			policy: TransitionPolicy{
				Outgoing:       MultiCellPolicy{N: 2},
				Incoming:       MultiCellPolicy{N: 2},
				OutgoingCohort: []*clustermetadatapb.ID{mcLeader, id("p1", "cell-b"), id("p2", "cell-c")},
				IncomingCohort: []*clustermetadatapb.ID{mcLeader, id("p1", "cell-b"), id("p2", "cell-c"), id("p3", "cell-d")},
			},
			wantCommit:   commitON,
			wantMethod:   methodANY,
			wantNumSync:  1,
			wantStandbys: []string{"cell-b_p1", "cell-c_p2"},
		},
		{
			name:   "same N, incoming cohort ⊆ outgoing: smaller cohort wins",
			leader: mcLeader,
			policy: TransitionPolicy{
				Outgoing:       MultiCellPolicy{N: 2},
				Incoming:       MultiCellPolicy{N: 2},
				OutgoingCohort: []*clustermetadatapb.ID{mcLeader, id("p1", "cell-b"), id("p2", "cell-c"), id("p3", "cell-d")},
				IncomingCohort: []*clustermetadatapb.ID{mcLeader, id("p1", "cell-b"), id("p2", "cell-c")},
			},
			wantCommit:   commitON,
			wantMethod:   methodANY,
			wantNumSync:  1,
			wantStandbys: []string{"cell-b_p1", "cell-c_p2"},
		},
		{
			name:   "same N, equal cohorts",
			leader: mcLeader,
			policy: TransitionPolicy{
				Outgoing:       MultiCellPolicy{N: 2},
				Incoming:       MultiCellPolicy{N: 2},
				OutgoingCohort: []*clustermetadatapb.ID{mcLeader, id("p1", "cell-b"), id("p2", "cell-c")},
				IncomingCohort: []*clustermetadatapb.ID{mcLeader, id("p1", "cell-b"), id("p2", "cell-c")},
			},
			wantCommit:   commitON,
			wantMethod:   methodANY,
			wantNumSync:  1,
			wantStandbys: []string{"cell-b_p1", "cell-c_p2"},
		},
		{
			name:   "same cohort, outgoing N larger: higher N wins",
			leader: mcLeader,
			policy: TransitionPolicy{
				Outgoing:       MultiCellPolicy{N: 3},
				Incoming:       MultiCellPolicy{N: 2},
				OutgoingCohort: []*clustermetadatapb.ID{mcLeader, id("p1", "cell-b"), id("p2", "cell-c"), id("p3", "cell-d")},
				IncomingCohort: []*clustermetadatapb.ID{mcLeader, id("p1", "cell-b"), id("p2", "cell-c"), id("p3", "cell-d")},
			},
			wantCommit:   commitON,
			wantMethod:   methodANY,
			wantNumSync:  2,
			wantStandbys: []string{"cell-b_p1", "cell-c_p2", "cell-d_p3"},
		},
		{
			name:   "same cohort, incoming N larger: higher N wins",
			leader: mcLeader,
			policy: TransitionPolicy{
				Outgoing:       MultiCellPolicy{N: 2},
				Incoming:       MultiCellPolicy{N: 3},
				OutgoingCohort: []*clustermetadatapb.ID{mcLeader, id("p1", "cell-b"), id("p2", "cell-c"), id("p3", "cell-d")},
				IncomingCohort: []*clustermetadatapb.ID{mcLeader, id("p1", "cell-b"), id("p2", "cell-c"), id("p3", "cell-d")},
			},
			wantCommit:   commitON,
			wantMethod:   methodANY,
			wantNumSync:  2,
			wantStandbys: []string{"cell-b_p1", "cell-c_p2", "cell-d_p3"},
		},

		// --- Representative-sample fallback ---
		// These cases cannot be resolved by the intersection heuristic (different
		// policy types, or same-type but cohorts are not in a subset relation with
		// different N). TransitionPolicy falls back to selecting the minimum
		// standbys from each policy's set needed to satisfy both simultaneously.
		{
			// Cohorts share only the leader (no standby overlap), so the intersection
			// heuristic returns nil and the fallback must pick one representative from
			// each side. leader + p1 satisfies outgoing N=2; leader + p3 satisfies
			// incoming N=2. Together they require two standby acks.
			name: "AtLeastN + AtLeastN, same N, fully disjoint standbys",
			policy: TransitionPolicy{
				Outgoing:       AtLeastNPolicy{N: 2},
				Incoming:       AtLeastNPolicy{N: 2},
				OutgoingCohort: []*clustermetadatapb.ID{leader, id("p1", "c1"), id("p2", "c2")},
				IncomingCohort: []*clustermetadatapb.ID{leader, id("p3", "c3"), id("p4", "c4")},
			},
			wantCommit:   commitON,
			wantMethod:   methodANY,
			wantNumSync:  2,
			wantStandbys: []string{"c1_p1", "c3_p3"},
		},
		{
			// With N=3 and no shared standbys, the "both" GUC must require 2 acks
			// from each side independently (4 standby acks total). There is no
			// cheaper option when the cohorts are completely disjoint.
			name: "AtLeastN + AtLeastN, same N=3, fully disjoint standbys: four reps needed",
			policy: TransitionPolicy{
				Outgoing:       AtLeastNPolicy{N: 3},
				Incoming:       AtLeastNPolicy{N: 3},
				OutgoingCohort: []*clustermetadatapb.ID{leader, id("p1", "c1"), id("p2", "c2"), id("p3", "c3")},
				IncomingCohort: []*clustermetadatapb.ID{leader, id("p4", "c4"), id("p5", "c5"), id("p6", "c6")},
			},
			wantCommit:   commitON,
			wantMethod:   methodANY,
			wantNumSync:  4,
			wantStandbys: []string{"c1_p1", "c2_p2", "c4_p4", "c5_p5"},
		},
		{
			// Neither cohort is a subset of the other, so the intersection heuristic
			// returns nil. outgoing has NumSync=1, incoming has NumSync=2; shared
			// standbys [p2, p3] are used first (up to max(1,2)=2), covering both
			// policies with no policy-exclusive standbys needed.
			name: "AtLeastN + AtLeastN, different N and non-subset cohorts",
			policy: TransitionPolicy{
				Outgoing:       AtLeastNPolicy{N: 2},
				Incoming:       AtLeastNPolicy{N: 3},
				OutgoingCohort: []*clustermetadatapb.ID{leader, id("p1", "c1"), id("p2", "c2"), id("p3", "c3")},
				IncomingCohort: []*clustermetadatapb.ID{leader, id("p2", "c2"), id("p3", "c3"), id("p4", "c4")},
			},
			wantCommit:   commitON,
			wantMethod:   methodANY,
			wantNumSync:  2,
			wantStandbys: []string{"c2_p2", "c3_p3"},
		},
		{
			// Neither cohort is a subset of the other and N values differ, so
			// BuildBothGUC returns nil. The fallback picks one shared standby
			// (satisfies outgoing N=2) and one incoming-only standby (contributes
			// the second ack for incoming N=3).
			name:   "MultiCell + MultiCell, different N and non-subset cohorts",
			leader: mcLeader,
			policy: TransitionPolicy{
				Outgoing:       MultiCellPolicy{N: 2},
				Incoming:       MultiCellPolicy{N: 3},
				OutgoingCohort: []*clustermetadatapb.ID{mcLeader, id("p1", "cell-b"), id("p2", "cell-c")},
				IncomingCohort: []*clustermetadatapb.ID{mcLeader, id("p2", "cell-c"), id("p3", "cell-d"), id("p4", "cell-e")},
			},
			wantCommit:   commitON,
			wantMethod:   methodANY,
			wantNumSync:  2,
			wantStandbys: []string{"cell-c_p2", "cell-d_p3"},
		},
		{
			// AtLeastN includes the leader in SyncStandbyIDs; MultiCell excludes the
			// leader's cell. The shared standbys (p1, p2) satisfy both policies'
			// NumSync=1, so one representative is sufficient.
			name: "AtLeastN + MultiCell: cross-type falls back to representative sample",
			policy: TransitionPolicy{
				Outgoing:       AtLeastNPolicy{N: 2},
				Incoming:       MultiCellPolicy{N: 2},
				OutgoingCohort: []*clustermetadatapb.ID{leader, id("p1", "c1"), id("p2", "c2")},
				IncomingCohort: []*clustermetadatapb.ID{leader, id("p1", "c1"), id("p2", "c2")},
			},
			wantCommit:   commitON,
			wantMethod:   methodANY,
			wantNumSync:  1,
			wantStandbys: []string{"c1_p1"},
		},
		{
			// The proposal leader is assisting recovery for a dead primary: it was
			// not part of the original outgoing cohort. The leader's WAL write still
			// counts, but since the leader is absent from outgoing, outgoing requires
			// N=2 standby acks (NumSync=2). Shared standbys [p1, p2] satisfy both
			// policies simultaneously, so both N=2 requirements are met with two acks.
			name: "leader assisting recovery: not in outgoing cohort",
			policy: TransitionPolicy{
				Outgoing:       AtLeastNPolicy{N: 2},
				Incoming:       AtLeastNPolicy{N: 2},
				OutgoingCohort: []*clustermetadatapb.ID{id("p1", "c1"), id("p2", "c2"), id("p3", "c3")},
				IncomingCohort: []*clustermetadatapb.ID{leader, id("p1", "c1"), id("p2", "c2")},
			},
			wantCommit:   commitON,
			wantMethod:   methodANY,
			wantNumSync:  2,
			wantStandbys: []string{"c1_p1", "c2_p2"},
		},
		{
			// The current leader is resigning: it is in the outgoing cohort but not
			// the incoming cohort. The leader's WAL write counts as one outgoing ack
			// (NumSync=1 outgoing), but incoming requires N=2 standby acks since the
			// leader is absent. Shared standbys [p1, p2] satisfy both simultaneously.
			name: "leader resigning: not in incoming cohort",
			policy: TransitionPolicy{
				Outgoing:       AtLeastNPolicy{N: 2},
				Incoming:       AtLeastNPolicy{N: 2},
				OutgoingCohort: []*clustermetadatapb.ID{leader, id("p1", "c1"), id("p2", "c2")},
				IncomingCohort: []*clustermetadatapb.ID{id("p1", "c1"), id("p2", "c2"), id("p3", "c3")},
			},
			wantCommit:   commitON,
			wantMethod:   methodANY,
			wantNumSync:  2,
			wantStandbys: []string{"c1_p1", "c2_p2"},
		},
		{
			// The proposal leader is a coordinator that does not appear in either
			// cohort (e.g. helping make a stuck rule change durable). Both policies
			// require N=2 standby acks (NumSync=2). The cohorts are fully disjoint,
			// so 2 reps from each side are needed — 4 acks total.
			name: "leader not in either cohort: two reps from each side",
			policy: TransitionPolicy{
				Outgoing:       AtLeastNPolicy{N: 2},
				Incoming:       AtLeastNPolicy{N: 2},
				OutgoingCohort: []*clustermetadatapb.ID{id("p1", "c1"), id("p2", "c2"), id("p3", "c3")},
				IncomingCohort: []*clustermetadatapb.ID{id("p4", "c4"), id("p5", "c5"), id("p6", "c6")},
			},
			wantCommit:   commitON,
			wantMethod:   methodANY,
			wantNumSync:  4,
			wantStandbys: []string{"c1_p1", "c2_p2", "c4_p4", "c5_p5"},
		},
		{
			// Strict outgoing policy (N=5, NumSync=4) with all standbys shared with
			// the incoming cohort. The shared standbys cover outgoing's stricter
			// NumSync requirement; incoming's NumSync=1 is satisfied by the same set.
			// No policy-exclusive standbys are needed.
			name: "strict outgoing N shares all standbys with incoming: shared standbys cover both",
			policy: TransitionPolicy{
				Outgoing:       AtLeastNPolicy{N: 5},
				Incoming:       MultiCellPolicy{N: 2},
				OutgoingCohort: []*clustermetadatapb.ID{leader, id("p1", "c1"), id("p2", "c2"), id("p3", "c3"), id("p4", "c4")},
				IncomingCohort: []*clustermetadatapb.ID{leader, id("p1", "c1"), id("p2", "c2"), id("p3", "c3"), id("p4", "c4")},
			},
			wantCommit:   commitON,
			wantMethod:   methodANY,
			wantNumSync:  4,
			wantStandbys: []string{"c1_p1", "c2_p2", "c3_p3", "c4_p4"},
		},
		{
			// N=1 uses SYNCHRONOUS_COMMIT_LOCAL; N=2 uses SYNCHRONOUS_COMMIT_ON.
			// With disjoint cohorts neither subset heuristic applies, and the
			// representative-sample fallback detects the commit-level mismatch.
			name: "incompatible commit levels: error",
			policy: TransitionPolicy{
				Outgoing:       AtLeastNPolicy{N: 1},
				Incoming:       AtLeastNPolicy{N: 2},
				OutgoingCohort: []*clustermetadatapb.ID{leader, id("p1", "c1")},
				IncomingCohort: []*clustermetadatapb.ID{leader, id("p2", "c2")},
			},
			wantErr: "cannot build representative-sample GUC: incompatible commit levels (SYNCHRONOUS_COMMIT_LOCAL/SYNCHRONOUS_METHOD_ANY vs SYNCHRONOUS_COMMIT_ON/SYNCHRONOUS_METHOD_ANY)",
		},
		{
			name: "incoming cohort too small for its N: error",
			policy: TransitionPolicy{
				Outgoing:       AtLeastNPolicy{N: 2},
				Incoming:       AtLeastNPolicy{N: 3},
				OutgoingCohort: []*clustermetadatapb.ID{leader, id("p1", "c1"), id("p2", "c2")},
				IncomingCohort: []*clustermetadatapb.ID{leader},
			},
			wantErr: "incoming GUC: Code: FAILED_PRECONDITION\ncannot establish synchronous replication: insufficient cohort members (required 2 standbys, available 0)\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l := tt.leader
			if l == nil {
				l = leader
			}
			cfg, err := tt.policy.BuildPrimaryDurabilityPostgresConfig(logger, nil, l)
			if tt.wantErr != "" {
				require.EqualError(t, err, tt.wantErr)
				require.Nil(t, cfg)
				return
			}
			require.NoError(t, err)
			require.NotNil(t, cfg)
			require.Equal(t, tt.wantCommit, cfg.SyncCommit)
			require.Equal(t, tt.wantMethod, cfg.SyncMethod)
			require.Equal(t, tt.wantNumSync, cfg.NumSync)
			require.Equal(t, tt.wantStandbys, clusterIDStrings(cfg.SyncStandbyIDs))
		})
	}
}

func TestTransitionPolicy_Description(t *testing.T) {
	p := TransitionPolicy{
		Outgoing: AtLeastNPolicy{N: 2},
		Incoming: MultiCellPolicy{N: 2},
	}
	require.Equal(t, "Transition(AT_LEAST_N(N=2) → MULTI_CELL_AT_LEAST_N(N=2))", p.Description())
}
