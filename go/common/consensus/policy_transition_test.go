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
		require.Error(t, err)
		require.Contains(t, err.Error(), "outgoing policy not achievable")
	})

	t.Run("incoming not achievable", func(t *testing.T) {
		p := TransitionPolicy{
			Outgoing:       AtLeastNPolicy{N: 2},
			Incoming:       AtLeastNPolicy{N: 5},
			OutgoingCohort: outCohort,
			IncomingCohort: inCohort,
		}
		err := p.CheckAchievable([]*clustermetadatapb.ID{id("p1", "c1"), id("p2", "c2")})
		require.Error(t, err)
		require.Contains(t, err.Error(), "incoming policy not achievable")
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
		require.Error(t, err)
		require.Contains(t, err.Error(), "majority not satisfied")
	})

	t.Run("IncomingCohortMode delegates to incoming policy against IncomingCohort", func(t *testing.T) {
		p := TransitionPolicy{
			Outgoing:           AtLeastNPolicy{N: 2},
			Incoming:           AtLeastNPolicy{N: 2},
			OutgoingCohort:     outCohort,
			IncomingCohort:     inCohort,
			IncomingCohortMode: true,
		}
		// 2 of 3 in inCohort passes
		require.NoError(t, p.CheckSufficientRecruitment(nil, []*clustermetadatapb.ID{id("p2", "c2"), id("p3", "c3")}))
	})
}

func TestTransitionPolicy_BuildLeaderDurabilityPostgresConfig(t *testing.T) {
	logger := testLogger()
	leader := id("primary", "cell-p")

	t.Run("same type, same N, outgoing cohort subset of incoming → outgoing GUC", func(t *testing.T) {
		// outCohort ⊆ inCohort: {p1,p2} ⊆ {p1,p2,p3}
		outCohort := []*clustermetadatapb.ID{leader, id("p1", "c1"), id("p2", "c2")}
		inCohort := []*clustermetadatapb.ID{leader, id("p1", "c1"), id("p2", "c2"), id("p3", "c3")}
		p := TransitionPolicy{
			Outgoing:       AtLeastNPolicy{N: 2},
			Incoming:       AtLeastNPolicy{N: 2},
			OutgoingCohort: outCohort,
			IncomingCohort: inCohort,
		}
		cfg, err := p.BuildLeaderDurabilityPostgresConfig(logger, nil, leader)
		require.NoError(t, err)
		require.NotNil(t, cfg)
		// Outgoing GUC uses outCohort (3 nodes, N=2, numSync=1)
		require.Equal(t, 1, cfg.NumSync)
		require.ElementsMatch(t, clusterIDStrings(outCohort), clusterIDStrings(cfg.SyncStandbyIDs))
	})

	t.Run("same type, same N, incoming cohort subset of outgoing → incoming GUC", func(t *testing.T) {
		outCohort := []*clustermetadatapb.ID{leader, id("p1", "c1"), id("p2", "c2"), id("p3", "c3")}
		inCohort := []*clustermetadatapb.ID{leader, id("p1", "c1"), id("p2", "c2")}
		p := TransitionPolicy{
			Outgoing:       AtLeastNPolicy{N: 2},
			Incoming:       AtLeastNPolicy{N: 2},
			OutgoingCohort: outCohort,
			IncomingCohort: inCohort,
		}
		cfg, err := p.BuildLeaderDurabilityPostgresConfig(logger, nil, leader)
		require.NoError(t, err)
		require.NotNil(t, cfg)
		// Incoming GUC uses inCohort (3 nodes, N=2, numSync=1)
		require.Equal(t, 1, cfg.NumSync)
		require.ElementsMatch(t, clusterIDStrings(inCohort), clusterIDStrings(cfg.SyncStandbyIDs))
	})

	t.Run("same type, same N, equal cohorts → outgoing GUC", func(t *testing.T) {
		cohort := []*clustermetadatapb.ID{leader, id("p1", "c1"), id("p2", "c2")}
		p := TransitionPolicy{
			Outgoing:       AtLeastNPolicy{N: 2},
			Incoming:       AtLeastNPolicy{N: 2},
			OutgoingCohort: cohort,
			IncomingCohort: cohort,
		}
		cfg, err := p.BuildLeaderDurabilityPostgresConfig(logger, nil, leader)
		require.NoError(t, err)
		require.NotNil(t, cfg)
		require.Equal(t, 1, cfg.NumSync)
		require.ElementsMatch(t, clusterIDStrings(cohort), clusterIDStrings(cfg.SyncStandbyIDs))
	})

	t.Run("same type, same cohort, smaller outgoing N → outgoing GUC", func(t *testing.T) {
		cohort := []*clustermetadatapb.ID{leader, id("p1", "c1"), id("p2", "c2"), id("p3", "c3")}
		p := TransitionPolicy{
			Outgoing:       AtLeastNPolicy{N: 2},
			Incoming:       AtLeastNPolicy{N: 3},
			OutgoingCohort: cohort,
			IncomingCohort: cohort,
		}
		cfg, err := p.BuildLeaderDurabilityPostgresConfig(logger, nil, leader)
		require.NoError(t, err)
		require.NotNil(t, cfg)
		// Outgoing N=2 → numSync=1
		require.Equal(t, 1, cfg.NumSync)
	})

	t.Run("same type, same cohort, smaller incoming N → incoming GUC", func(t *testing.T) {
		cohort := []*clustermetadatapb.ID{leader, id("p1", "c1"), id("p2", "c2"), id("p3", "c3")}
		p := TransitionPolicy{
			Outgoing:       AtLeastNPolicy{N: 3},
			Incoming:       AtLeastNPolicy{N: 2},
			OutgoingCohort: cohort,
			IncomingCohort: cohort,
		}
		cfg, err := p.BuildLeaderDurabilityPostgresConfig(logger, nil, leader)
		require.NoError(t, err)
		require.NotNil(t, cfg)
		// Incoming N=2 → numSync=1
		require.Equal(t, 1, cfg.NumSync)
	})

	t.Run("same type, different N, different cohorts → falls back to representative sample", func(t *testing.T) {
		// outCohort and inCohort overlap but neither is a subset of the other
		outCohort := []*clustermetadatapb.ID{leader, id("p1", "c1"), id("p2", "c2"), id("p3", "c3")}
		inCohort := []*clustermetadatapb.ID{leader, id("p2", "c2"), id("p3", "c3"), id("p4", "c4")}
		p := TransitionPolicy{
			Outgoing:       AtLeastNPolicy{N: 2},
			Incoming:       AtLeastNPolicy{N: 3},
			OutgoingCohort: outCohort,
			IncomingCohort: inCohort,
		}
		cfg, err := p.BuildLeaderDurabilityPostgresConfig(logger, nil, leader)
		require.NoError(t, err)
		require.NotNil(t, cfg)
		// Representative sample: outgoing needs numSync=1, incoming needs numSync=2.
		// Shared standbys (leader,p2,p3): sharedUsed=min(3,min(1,2))=1.
		// outNeed=0, inNeed=1. Result: 2 standbys total.
		require.Equal(t, 2, cfg.NumSync)
	})

	t.Run("cross-type AtLeastN → MultiCell falls back to representative sample", func(t *testing.T) {
		// AtLeastN uses full cohort; MultiCell excludes leader's cell.
		// Use different cells so MultiCell has eligible standbys.
		outCohort := []*clustermetadatapb.ID{leader, id("p1", "c1"), id("p2", "c2")}
		inCohort := []*clustermetadatapb.ID{leader, id("p1", "c1"), id("p2", "c2")}
		p := TransitionPolicy{
			Outgoing:       AtLeastNPolicy{N: 2},  // numSync=1, standbys={leader,p1,p2}
			Incoming:       MultiCellPolicy{N: 2}, // numSync=1, standbys={p1,p2} (leader's cell excluded)
			OutgoingCohort: outCohort,
			IncomingCohort: inCohort,
		}
		cfg, err := p.BuildLeaderDurabilityPostgresConfig(logger, nil, leader)
		require.NoError(t, err)
		require.NotNil(t, cfg)
		// Both need numSync=1. Shared standbys between outgoing and incoming sets:
		// outgoing has {leader,p1,p2}, incoming has {p1,p2}. Shared={p1,p2}.
		// sharedUsed=min(2,min(1,1))=1. outNeed=0, inNeed=0. Result: 1 standby.
		require.Equal(t, 1, cfg.NumSync)
	})

	t.Run("incoming cohort too small for its N → error from incoming GUC build", func(t *testing.T) {
		// inCohort has only 1 member; AtLeastNPolicy{N:3} needs numSync=2 standbys.
		// Neither intersection heuristic applies (different N, inCohort ⊆ outCohort but
		// not both-subset), so the fallback path tries to build incoming's GUC — which
		// fails because requiredNumSync(2) > len(inCohort)(1).
		outCohort := []*clustermetadatapb.ID{leader, id("p1", "c1"), id("p2", "c2")}
		inCohort := []*clustermetadatapb.ID{leader}
		p := TransitionPolicy{
			Outgoing:       AtLeastNPolicy{N: 2},
			Incoming:       AtLeastNPolicy{N: 3},
			OutgoingCohort: outCohort,
			IncomingCohort: inCohort,
		}
		_, err := p.BuildLeaderDurabilityPostgresConfig(logger, nil, leader)
		require.Error(t, err)
		require.Contains(t, err.Error(), "incoming GUC")
	})

	t.Run("MultiCell same type transition uses intersection heuristic", func(t *testing.T) {
		mcLeader := id("primary", "cell-a")
		outCohort := []*clustermetadatapb.ID{mcLeader, id("p1", "cell-b"), id("p2", "cell-c")}
		inCohort := []*clustermetadatapb.ID{mcLeader, id("p1", "cell-b"), id("p2", "cell-c"), id("p3", "cell-d")}
		p := TransitionPolicy{
			Outgoing:       MultiCellPolicy{N: 2},
			Incoming:       MultiCellPolicy{N: 2},
			OutgoingCohort: outCohort,
			IncomingCohort: inCohort,
		}
		// outCohort ⊆ inCohort → outgoing GUC (p1,p2 eligible, numSync=1)
		cfg, err := p.BuildLeaderDurabilityPostgresConfig(logger, nil, mcLeader)
		require.NoError(t, err)
		require.NotNil(t, cfg)
		require.Equal(t, 1, cfg.NumSync)
		require.ElementsMatch(t,
			[]string{"cell-b_p1", "cell-c_p2"},
			clusterIDStrings(cfg.SyncStandbyIDs),
		)
	})

	t.Run("incompatible commit levels → error", func(t *testing.T) {
		// N=1 uses SYNCHRONOUS_COMMIT_LOCAL; N=2 uses SYNCHRONOUS_COMMIT_ON.
		// With disjoint cohorts, neither subset heuristic applies and the
		// representative-sample fallback detects the mismatch.
		outCohort := []*clustermetadatapb.ID{leader, id("p1", "c1")}
		inCohort := []*clustermetadatapb.ID{leader, id("p2", "c2")}
		p := TransitionPolicy{
			Outgoing:       AtLeastNPolicy{N: 1}, // SYNCHRONOUS_COMMIT_LOCAL
			Incoming:       AtLeastNPolicy{N: 2}, // SYNCHRONOUS_COMMIT_ON
			OutgoingCohort: outCohort,
			IncomingCohort: inCohort,
		}
		_, err := p.BuildLeaderDurabilityPostgresConfig(logger, nil, leader)
		require.Error(t, err)
		require.Contains(t, err.Error(), "incompatible commit levels")
	})
}

func TestTransitionPolicy_Description(t *testing.T) {
	p := TransitionPolicy{
		Outgoing: AtLeastNPolicy{N: 2},
		Incoming: MultiCellPolicy{N: 2},
	}
	require.Equal(t, "Transition(AT_LEAST_N(N=2) → MULTI_CELL_AT_LEAST_N(N=2))", p.Description())
}
