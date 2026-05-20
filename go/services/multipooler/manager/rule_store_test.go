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

package manager

import (
	"errors"
	"io"
	"log/slog"
	"testing"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/services/multipooler/executor/mock"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBuildPoolerPosition(t *testing.T) {
	leaderApp := ptrStr("zone1_pooler-a")
	coordStr := "zone1_coord-1"
	cohort := []string{"zone1_pooler-a", "zone1_pooler-b"}
	dpName := "AT_LEAST_2"
	dpQuorum := "QUORUM_TYPE_AT_LEAST_N"
	var dpCount int64 = 2

	t.Run("decision only, no proposal", func(t *testing.T) {
		pos, err := buildPoolerPosition(
			3, 1, leaderApp, coordStr, cohort, dpName, dpQuorum, dpCount,
			nil, nil, nil, nil, nil, nil, nil, nil,
			"0/100",
		)
		require.NoError(t, err)
		require.NotNil(t, pos)
		assert.Equal(t, int64(3), pos.GetDecision().GetRuleNumber().GetCoordinatorTerm())
		assert.Equal(t, int64(1), pos.GetDecision().GetRuleNumber().GetLeaderSubterm())
		assert.Equal(t, "pooler-a", pos.GetDecision().GetLeaderId().GetName())
		assert.Equal(t, "0/100", pos.GetLsn())
		assert.Nil(t, pos.GetProposal(), "no proposal when proposalCoordTerm is nil")
	})

	t.Run("with proposal", func(t *testing.T) {
		var propTerm int64 = 5
		var propSubterm int64 = 0
		propLeader := ptrStr("zone1_pooler-b")
		propCoord := ptrStr("zone1_coord-2")
		propCohort := []string{"zone1_pooler-a", "zone1_pooler-b", "zone1_pooler-c"}
		propDPName := ptrStr("AT_LEAST_2")
		propDPQuorum := ptrStr("QUORUM_TYPE_AT_LEAST_N")
		var propDPCount int64 = 2

		pos, err := buildPoolerPosition(
			3, 1, leaderApp, coordStr, cohort, dpName, dpQuorum, dpCount,
			&propTerm, &propSubterm, propLeader, propCoord, propCohort, propDPName, propDPQuorum, &propDPCount,
			"0/200",
		)
		require.NoError(t, err)
		require.NotNil(t, pos)
		assert.Equal(t, int64(3), pos.GetDecision().GetRuleNumber().GetCoordinatorTerm())
		require.NotNil(t, pos.GetProposal(), "proposal must be set")
		assert.Equal(t, int64(5), pos.GetProposal().GetRuleNumber().GetCoordinatorTerm())
		assert.Equal(t, "pooler-b", pos.GetProposal().GetLeaderId().GetName())
		assert.Len(t, pos.GetProposal().GetCohortMembers(), 3)
	})

	t.Run("proposal with nil leader and coordinator", func(t *testing.T) {
		var propTerm int64 = 7
		propDP := ptrStr("AT_LEAST_2")
		propDPQ := ptrStr("QUORUM_TYPE_AT_LEAST_N")
		var propDPC int64 = 2
		pos, err := buildPoolerPosition(
			3, 1, leaderApp, coordStr, cohort, dpName, dpQuorum, dpCount,
			&propTerm, nil, nil, nil, nil, propDP, propDPQ, &propDPC,
			"0/1",
		)
		require.NoError(t, err)
		require.NotNil(t, pos.GetProposal())
		assert.Equal(t, int64(7), pos.GetProposal().GetRuleNumber().GetCoordinatorTerm())
		assert.Nil(t, pos.GetProposal().GetLeaderId(), "nil leaderIDStr yields no LeaderId")
	})
}

func ptrStr(s string) *string { return &s }

func TestCacheRuleObservation(t *testing.T) {
	newPos := func(coordTerm int64) *clustermetadatapb.PoolerPosition {
		return &clustermetadatapb.PoolerPosition{
			Decision: &clustermetadatapb.ShardRule{
				RuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: coordTerm},
			},
			Lsn: "0/1",
		}
	}

	t.Run("advances cache forward", func(t *testing.T) {
		rs := newRuleStore(slog.New(slog.NewTextHandler(io.Discard, nil)), nil)
		rs.cacheRuleObservation(newPos(3))
		rs.cacheRuleObservation(newPos(5))
		assert.Equal(t, int64(5), rs.cachedPosition().GetDecision().GetRuleNumber().GetCoordinatorTerm())
	})

	t.Run("ignores stale observation", func(t *testing.T) {
		rs := newRuleStore(slog.New(slog.NewTextHandler(io.Discard, nil)), nil)
		rs.cacheRuleObservation(newPos(5))
		rs.cacheRuleObservation(newPos(3)) // stale
		assert.Equal(t, int64(5), rs.cachedPosition().GetDecision().GetRuleNumber().GetCoordinatorTerm(),
			"stale observation must not overwrite a newer cached position")
	})
}

func TestQueryRuleHistory(t *testing.T) {
	t.Run("returns records ordered newest first", func(t *testing.T) {
		mockQueryService := mock.NewQueryService()
		rs := newRuleStore(slog.New(slog.NewTextHandler(io.Discard, nil)), mockQueryService)

		leaderAppName := "zone1_leader-1"
		coordID := "zone1_coordinator-1"
		walPos := "0/1234567"
		operation := "promotion"
		createdAt := "2026-03-24 09:00:17.000000+00"

		mockQueryService.AddQueryPatternOnce(
			"SELECT coordinator_term, leader_subterm, event_type",
			mock.MakeQueryResult(
				[]string{
					"coordinator_term", "leader_subterm", "event_type", "leader_id", "coordinator_id",
					"wal_position", "operation", "reason", "cohort_members", "accepted_members",
					"durability_policy_name", "durability_quorum_type", "durability_required_count",
					"created_at",
				},
				[][]any{
					{
						int64(2), int64(1), "promotion", leaderAppName, coordID, walPos, operation,
						"manual failover", "{zone1_pooler-2,zone1_pooler-3}", "{zone1_pooler-2}",
						nil, nil, nil, createdAt,
					},
					{
						int64(1), int64(0), "replication_config", leaderAppName, coordID, nil, nil,
						"initial bootstrap", "{zone1_pooler-1,zone1_pooler-2}", nil,
						nil, nil, nil, createdAt,
					},
				},
			),
		)

		records, err := rs.queryRuleHistory(t.Context(), 10)
		require.NoError(t, err)
		require.Len(t, records, 2)

		// First record: term 2, subterm 1 (newest)
		assert.Equal(t, int64(2), records[0].CoordinatorTerm)
		assert.Equal(t, int64(1), records[0].LeaderSubterm)
		assert.Equal(t, "promotion", records[0].EventType)
		require.NotNil(t, records[0].LeaderID)
		assert.Equal(t, leaderAppName, records[0].LeaderID.appName)
		require.NotNil(t, records[0].WALPosition)
		assert.Equal(t, walPos, *records[0].WALPosition)
		require.NotNil(t, records[0].Operation)
		assert.Equal(t, operation, *records[0].Operation)
		assert.Equal(t, "manual failover", records[0].Reason)
		require.Len(t, records[0].CohortMembers, 2)
		assert.Equal(t, "zone1_pooler-2", records[0].CohortMembers[0].appName)
		assert.Equal(t, "zone1_pooler-3", records[0].CohortMembers[1].appName)
		require.Len(t, records[0].AcceptedMembers, 1)
		assert.Equal(t, "zone1_pooler-2", records[0].AcceptedMembers[0].appName)
		assert.False(t, records[0].CreatedAt.IsZero())

		// Second record: term 1, subterm 0; nullable fields are nil/empty
		assert.Equal(t, int64(1), records[1].CoordinatorTerm)
		assert.Nil(t, records[1].WALPosition)
		assert.Nil(t, records[1].Operation)
		assert.Empty(t, records[1].AcceptedMembers)
		assert.NoError(t, mockQueryService.ExpectationsWereMet())
	})

	t.Run("returns empty slice when no records exist", func(t *testing.T) {
		mockQueryService := mock.NewQueryService()
		rs := newRuleStore(slog.New(slog.NewTextHandler(io.Discard, nil)), mockQueryService)

		mockQueryService.AddQueryPatternOnce(
			"SELECT coordinator_term, leader_subterm, event_type",
			mock.MakeQueryResult(
				[]string{
					"coordinator_term", "leader_subterm", "event_type", "leader_id", "coordinator_id",
					"wal_position", "operation", "reason", "cohort_members", "accepted_members",
					"durability_policy_name", "durability_quorum_type", "durability_required_count",
					"created_at",
				},
				[][]any{},
			),
		)

		records, err := rs.queryRuleHistory(t.Context(), 10)
		require.NoError(t, err)
		assert.Empty(t, records)
		assert.NoError(t, mockQueryService.ExpectationsWereMet())
	})

	t.Run("propagates query error", func(t *testing.T) {
		mockQueryService := mock.NewQueryService()
		rs := newRuleStore(slog.New(slog.NewTextHandler(io.Discard, nil)), mockQueryService)

		mockQueryService.AddQueryPatternOnceWithError(
			"SELECT coordinator_term, leader_subterm, event_type",
			errors.New("connection refused"),
		)

		_, err := rs.queryRuleHistory(t.Context(), 10)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "failed to query rule_history")
		assert.Contains(t, err.Error(), "connection refused")
		assert.NoError(t, mockQueryService.ExpectationsWereMet())
	})
}
