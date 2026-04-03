// Copyright 2025 Supabase, Inc.
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
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"

	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/common/queryservice"
	"github.com/multigres/multigres/go/common/topoclient/memorytopo"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/pb/query"
	"github.com/multigres/multigres/go/services/multipooler/executor"
	"github.com/multigres/multigres/go/services/multipooler/executor/mock"
	"github.com/multigres/multigres/go/services/multipooler/poolerserver"
	"github.com/multigres/multigres/go/services/multipooler/pubsub"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockPoolerController implements poolerserver.PoolerController for testing.
type mockPoolerController struct {
	queryService *mock.QueryService
}

func (m *mockPoolerController) Open(context.Context) error { return nil }
func (m *mockPoolerController) Close() error               { return nil }
func (m *mockPoolerController) IsHealthy() error           { return nil }
func (m *mockPoolerController) IsServing() bool            { return true }
func (m *mockPoolerController) OnStateChange(context.Context, clustermetadatapb.PoolerType, clustermetadatapb.PoolerServingStatus) error {
	return nil
}
func (m *mockPoolerController) StartRequest(*query.Target, bool) error { return nil }
func (m *mockPoolerController) AwaitStateChange(context.Context, clustermetadatapb.PoolerType, clustermetadatapb.PoolerServingStatus) {
}
func (m *mockPoolerController) Executor() (queryservice.QueryService, error) { return nil, nil }
func (m *mockPoolerController) InternalQueryService() executor.InternalQueryService {
	return m.queryService
}
func (m *mockPoolerController) RegisterGRPCServices()                {}
func (m *mockPoolerController) SetPubSubListener(_ *pubsub.Listener) {}
func (m *mockPoolerController) PubSubListener() *pubsub.Listener     { return nil }

var _ poolerserver.PoolerController = (*mockPoolerController)(nil)

// newTestManagerWithMock creates a test MultiPoolerManager with a mock query service
func newTestManagerWithMock(tableGroup, shard string) (*MultiPoolerManager, *mock.QueryService) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	mockQueryService := mock.NewQueryService()

	// Create a memorytopo store for tests that need topoClient (e.g., GetRemoteOperationTimeout)
	ctx := context.Background()
	topoStore := memorytopo.NewServer(ctx, "test-cell")

	multiPooler := &clustermetadatapb.MultiPooler{
		TableGroup: tableGroup,
		Shard:      shard,
	}

	svcID := &clustermetadatapb.ID{Cell: "test-cell", Name: "test-pooler"}
	svcPoolerID, err := newPoolerID(svcID)
	if err != nil {
		panic(err)
	}

	pm := &MultiPoolerManager{
		logger:          logger,
		qsc:             &mockPoolerController{queryService: mockQueryService},
		topoClient:      topoStore,
		config:          &Config{},
		multipooler:     multiPooler,
		serviceID:       svcID,
		servicePoolerID: svcPoolerID,
	}

	return pm, mockQueryService
}

func TestCreateSidecarSchema(t *testing.T) {
	tests := []struct {
		name          string
		tableGroup    string
		setupMock     func(m *mock.QueryService)
		expectError   bool
		errorContains string
	}{
		{
			name:       "successful schema creation for default tablegroup",
			tableGroup: constants.DefaultTableGroup,
			setupMock: func(m *mock.QueryService) {
				m.AddQueryPatternOnce("CREATE SCHEMA IF NOT EXISTS multigres", mock.MakeQueryResult(nil, nil))
				m.AddQueryPatternOnce("CREATE TABLE IF NOT EXISTS multigres.heartbeat", mock.MakeQueryResult(nil, nil))
				m.AddQueryPatternOnce("CREATE TABLE IF NOT EXISTS multigres.current_rule", mock.MakeQueryResult(nil, nil))
				m.AddQueryPatternOnce("INSERT INTO multigres.current_rule", mock.MakeQueryResult(nil, nil))
				m.AddQueryPatternOnce("CREATE TABLE IF NOT EXISTS multigres.rule_history", mock.MakeQueryResult(nil, nil))
				m.AddQueryPatternOnce("CREATE TABLE IF NOT EXISTS multigres.tablegroup", mock.MakeQueryResult(nil, nil))
				m.AddQueryPatternOnce("CREATE TABLE IF NOT EXISTS multigres.tablegroup_table", mock.MakeQueryResult(nil, nil))
				m.AddQueryPatternOnce("CREATE TABLE IF NOT EXISTS multigres.shard", mock.MakeQueryResult(nil, nil))
			},
			expectError: false,
		},
		{
			name:       "schema creation fails",
			tableGroup: constants.DefaultTableGroup,
			setupMock: func(m *mock.QueryService) {
				m.AddQueryPatternOnceWithError("CREATE SCHEMA IF NOT EXISTS multigres", errors.New("permission denied"))
			},
			expectError:   true,
			errorContains: "failed to create multigres schema",
		},
		{
			name:       "heartbeat table creation fails",
			tableGroup: constants.DefaultTableGroup,
			setupMock: func(m *mock.QueryService) {
				m.AddQueryPatternOnce("CREATE SCHEMA IF NOT EXISTS multigres", mock.MakeQueryResult(nil, nil))
				m.AddQueryPatternOnceWithError("CREATE TABLE IF NOT EXISTS multigres.heartbeat", errors.New("table creation failed"))
			},
			expectError:   true,
			errorContains: "failed to create heartbeat table",
		},
		{
			name:       "current_rule table creation fails",
			tableGroup: constants.DefaultTableGroup,
			setupMock: func(m *mock.QueryService) {
				m.AddQueryPatternOnce("CREATE SCHEMA IF NOT EXISTS multigres", mock.MakeQueryResult(nil, nil))
				m.AddQueryPatternOnce("CREATE TABLE IF NOT EXISTS multigres.heartbeat", mock.MakeQueryResult(nil, nil))
				m.AddQueryPatternOnceWithError("CREATE TABLE IF NOT EXISTS multigres.current_rule", errors.New("table creation failed"))
			},
			expectError:   true,
			errorContains: "failed to create current_rule table",
		},
		{
			name:       "current_rule initialization fails",
			tableGroup: constants.DefaultTableGroup,
			setupMock: func(m *mock.QueryService) {
				m.AddQueryPatternOnce("CREATE SCHEMA IF NOT EXISTS multigres", mock.MakeQueryResult(nil, nil))
				m.AddQueryPatternOnce("CREATE TABLE IF NOT EXISTS multigres.heartbeat", mock.MakeQueryResult(nil, nil))
				m.AddQueryPatternOnce("CREATE TABLE IF NOT EXISTS multigres.current_rule", mock.MakeQueryResult(nil, nil))
				m.AddQueryPatternOnceWithError("INSERT INTO multigres.current_rule", errors.New("insert failed"))
			},
			expectError:   true,
			errorContains: "failed to initialize current_rule",
		},
		{
			name:       "rule_history table creation fails",
			tableGroup: constants.DefaultTableGroup,
			setupMock: func(m *mock.QueryService) {
				m.AddQueryPatternOnce("CREATE SCHEMA IF NOT EXISTS multigres", mock.MakeQueryResult(nil, nil))
				m.AddQueryPatternOnce("CREATE TABLE IF NOT EXISTS multigres.heartbeat", mock.MakeQueryResult(nil, nil))
				m.AddQueryPatternOnce("CREATE TABLE IF NOT EXISTS multigres.current_rule", mock.MakeQueryResult(nil, nil))
				m.AddQueryPatternOnce("INSERT INTO multigres.current_rule", mock.MakeQueryResult(nil, nil))
				m.AddQueryPatternOnceWithError("CREATE TABLE IF NOT EXISTS multigres.rule_history", errors.New("table creation failed"))
			},
			expectError:   true,
			errorContains: "failed to create rule_history table",
		},
		{
			name:       "tablegroup table creation fails",
			tableGroup: constants.DefaultTableGroup,
			setupMock: func(m *mock.QueryService) {
				m.AddQueryPatternOnce("CREATE SCHEMA IF NOT EXISTS multigres", mock.MakeQueryResult(nil, nil))
				m.AddQueryPatternOnce("CREATE TABLE IF NOT EXISTS multigres.heartbeat", mock.MakeQueryResult(nil, nil))
				m.AddQueryPatternOnce("CREATE TABLE IF NOT EXISTS multigres.current_rule", mock.MakeQueryResult(nil, nil))
				m.AddQueryPatternOnce("INSERT INTO multigres.current_rule", mock.MakeQueryResult(nil, nil))
				m.AddQueryPatternOnce("CREATE TABLE IF NOT EXISTS multigres.rule_history", mock.MakeQueryResult(nil, nil))
				m.AddQueryPatternOnceWithError("CREATE TABLE IF NOT EXISTS multigres.tablegroup", errors.New("table creation failed"))
			},
			expectError:   true,
			errorContains: "failed to create tablegroup table",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pm, mockQueryService := newTestManagerWithMock(tt.tableGroup, constants.DefaultShard)

			tt.setupMock(mockQueryService)

			ctx := context.Background()
			err := pm.createSidecarSchema(ctx)

			if tt.expectError {
				assert.Error(t, err)
				if tt.errorContains != "" {
					assert.Contains(t, err.Error(), tt.errorContains)
				}
			} else {
				assert.NoError(t, err)
			}
			assert.NoError(t, mockQueryService.ExpectationsWereMet())
		})
	}
}

func TestInitializeMultischemaData(t *testing.T) {
	tests := []struct {
		name          string
		tableGroup    string
		shard         string
		setupMock     func(m *mock.QueryService)
		expectError   bool
		errorContains string
	}{
		{
			name:       "successful data initialization",
			tableGroup: constants.DefaultTableGroup,
			shard:      constants.DefaultShard,
			setupMock: func(m *mock.QueryService) {
				m.AddQueryPatternOnce("INSERT INTO multigres.tablegroup", mock.MakeQueryResult(nil, nil))
				m.AddQueryPatternOnce("SELECT oid FROM multigres.tablegroup", mock.MakeQueryResult([]string{"oid"}, [][]any{{int64(1)}}))
				m.AddQueryPatternOnce("INSERT INTO multigres.shard", mock.MakeQueryResult(nil, nil))
			},
			expectError: false,
		},
		{
			name:          "rejects non-default tablegroup",
			tableGroup:    "custom",
			shard:         constants.DefaultShard,
			setupMock:     func(m *mock.QueryService) {},
			expectError:   true,
			errorContains: "only default tablegroup is supported",
		},
		{
			name:          "rejects non-default shard",
			tableGroup:    constants.DefaultTableGroup,
			shard:         "shard-1",
			setupMock:     func(m *mock.QueryService) {},
			expectError:   true,
			errorContains: "only shard " + constants.DefaultShard + " is supported",
		},
		{
			name:       "tablegroup insert fails",
			tableGroup: constants.DefaultTableGroup,
			shard:      constants.DefaultShard,
			setupMock: func(m *mock.QueryService) {
				m.AddQueryPatternOnceWithError("INSERT INTO multigres.tablegroup", errors.New("insert failed"))
			},
			expectError:   true,
			errorContains: "failed to insert tablegroup",
		},
		{
			name:       "shard insert fails",
			tableGroup: constants.DefaultTableGroup,
			shard:      constants.DefaultShard,
			setupMock: func(m *mock.QueryService) {
				m.AddQueryPatternOnce("INSERT INTO multigres.tablegroup", mock.MakeQueryResult(nil, nil))
				m.AddQueryPatternOnce("SELECT oid FROM multigres.tablegroup", mock.MakeQueryResult([]string{"oid"}, [][]any{{int64(1)}}))
				m.AddQueryPatternOnceWithError("INSERT INTO multigres.shard", errors.New("insert failed"))
			},
			expectError:   true,
			errorContains: "failed to insert shard",
		},
		{
			name:       "idempotent insert (conflict)",
			tableGroup: constants.DefaultTableGroup,
			shard:      constants.DefaultShard,
			setupMock: func(m *mock.QueryService) {
				// ON CONFLICT DO NOTHING still succeeds
				m.AddQueryPatternOnce("INSERT INTO multigres.tablegroup", mock.MakeQueryResult(nil, nil))
				m.AddQueryPatternOnce("SELECT oid FROM multigres.tablegroup", mock.MakeQueryResult([]string{"oid"}, [][]any{{int64(1)}}))
				m.AddQueryPatternOnce("INSERT INTO multigres.shard", mock.MakeQueryResult(nil, nil))
			},
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pm, mockQueryService := newTestManagerWithMock(tt.tableGroup, tt.shard)

			tt.setupMock(mockQueryService)

			ctx := context.Background()
			err := pm.initializeMultischemaData(ctx)

			if tt.expectError {
				assert.Error(t, err)
				if tt.errorContains != "" {
					assert.Contains(t, err.Error(), tt.errorContains)
				}
			} else {
				assert.NoError(t, err)
			}
			assert.NoError(t, mockQueryService.ExpectationsWereMet())
		})
	}
}

func TestUpdateRule(t *testing.T) {
	returnCols := []string{
		"coordinator_term", "rule_subterm", "event_type", "leader_id", "coordinator_id",
		"wal_position", "operation", "reason", "cohort_members", "accepted_members",
		"durability_policy_name", "durability_quorum_type", "durability_required_count",
		"durability_async_fallback", "created_at",
	}

	tests := []struct {
		name          string
		buildUpdate   func(pm *MultiPoolerManager) *ruleUpdateBuilder
		setupMock     func(m *mock.QueryService)
		expectError   bool
		errorContains string
	}{
		{
			name: "successful update with all fields",
			buildUpdate: func(pm *MultiPoolerManager) *ruleUpdateBuilder {
				return newRuleUpdate(1, &clustermetadatapb.ID{Cell: "zone1", Name: "coordinator-1"}, "promotion", "Leadership changed due to manual promotion").
					withLeader(&clustermetadatapb.ID{Cell: "zone1", Name: "leader-1"}).
					withCohort([]*clustermetadatapb.ID{
						{Cell: "zone1", Name: "member-1"},
						{Cell: "zone1", Name: "member-2"},
						{Cell: "zone1", Name: "member-3"},
					}).
					withAcceptedMembers([]*clustermetadatapb.ID{
						{Cell: "zone1", Name: "member-1"},
						{Cell: "zone1", Name: "member-2"},
					}).
					withWALPosition("0/1234567").
					withOperation("promotion")
			},
			setupMock: func(m *mock.QueryService) {
				m.AddQueryPatternOnce("FROM multigres.current_rule", mock.MakeQueryResult(returnCols, [][]any{
					{
						int64(1), int64(0), "promotion", "zone1_leader-1", "zone1_coordinator-1",
						"0/1234567", "promotion", "Leadership changed due to manual promotion",
						"{zone1_member-1,zone1_member-2,zone1_member-3}", "{zone1_member-1,zone1_member-2}",
						nil, nil, nil, nil, "2026-03-24 09:00:17.000000+00",
					},
				}))
			},
			expectError: false,
		},
		{
			name: "partial update: cohort only, leader kept from current_rule",
			buildUpdate: func(pm *MultiPoolerManager) *ruleUpdateBuilder {
				return newRuleUpdate(2, &clustermetadatapb.ID{Cell: "zone1", Name: "coordinator-2"}, "replication_config", "standby added").
					withCohort([]*clustermetadatapb.ID{
						{Cell: "zone1", Name: "member-1"},
						{Cell: "zone1", Name: "member-2"},
					}).
					withOperation("add")
			},
			setupMock: func(m *mock.QueryService) {
				m.AddQueryPatternOnce("FROM multigres.current_rule", mock.MakeQueryResult(returnCols, [][]any{
					{
						int64(2), int64(0), "replication_config", "zone1_leader-1", "zone1_coordinator-2",
						nil, "add", "standby added",
						"{zone1_member-1,zone1_member-2}", nil,
						nil, nil, nil, nil, "2026-03-24 09:00:17.000000+00",
					},
				}))
			},
			expectError: false,
		},
		{
			name: "write fails with database error",
			buildUpdate: func(pm *MultiPoolerManager) *ruleUpdateBuilder {
				return newRuleUpdate(2, &clustermetadatapb.ID{Cell: "zone1", Name: "coordinator-2"}, "promotion", "Leadership changed due to failover").
					withLeader(&clustermetadatapb.ID{Cell: "zone1", Name: "leader-2"}).
					withCohort([]*clustermetadatapb.ID{{Cell: "zone1", Name: "member-1"}})
			},
			setupMock: func(m *mock.QueryService) {
				m.AddQueryPatternOnceWithError("FROM multigres.current_rule", errors.New("connection refused"))
			},
			expectError:   true,
			errorContains: "failed to write rule history record",
		},
		{
			name: "force mode skips write entirely",
			buildUpdate: func(pm *MultiPoolerManager) *ruleUpdateBuilder {
				return newRuleUpdate(2, &clustermetadatapb.ID{Cell: "zone1", Name: "coordinator-2"}, "replication_config", "Emergency replication GUC change").
					withCohort([]*clustermetadatapb.ID{{Cell: "zone1", Name: "member-1"}}).
					withForce()
			},
			setupMock:   func(m *mock.QueryService) {},
			expectError: false,
		},
		{
			name: "update with empty cohort",
			buildUpdate: func(pm *MultiPoolerManager) *ruleUpdateBuilder {
				return newRuleUpdate(3, &clustermetadatapb.ID{Cell: "zone1", Name: "coordinator-3"}, "promotion", "Initial cluster bootstrap").
					withLeader(&clustermetadatapb.ID{Cell: "zone1", Name: "leader-3"}).
					withCohort([]*clustermetadatapb.ID{}).
					withWALPosition("0/3456789").
					withOperation("bootstrap")
			},
			setupMock: func(m *mock.QueryService) {
				m.AddQueryPatternOnce("FROM multigres.current_rule", mock.MakeQueryResult(returnCols, [][]any{
					{
						int64(3), int64(0), "promotion", "zone1_leader-3", "zone1_coordinator-3",
						"0/3456789", "bootstrap", "Initial cluster bootstrap",
						"{}", "{}",
						nil, nil, nil, nil, "2026-03-24 09:00:17.000000+00",
					},
				}))
			},
			expectError: false,
		},
		{
			name: "partial update: leader only, cohort kept from current_rule",
			buildUpdate: func(pm *MultiPoolerManager) *ruleUpdateBuilder {
				// No withCohort call — cohort_members retains its existing value via COALESCE
				return newRuleUpdate(4, &clustermetadatapb.ID{Cell: "zone1", Name: "coordinator-4"}, "promotion", "failover").
					withLeader(&clustermetadatapb.ID{Cell: "zone1", Name: "new-leader"})
			},
			setupMock: func(m *mock.QueryService) {
				m.AddQueryPatternOnce("FROM multigres.current_rule", mock.MakeQueryResult(returnCols, [][]any{
					{
						int64(4), int64(0), "promotion", "zone1_new-leader", "zone1_coordinator-4",
						nil, nil, "failover",
						"{zone1_pooler-1,zone1_pooler-2}", nil,
						nil, nil, nil, nil, "2026-03-24 09:00:17.000000+00",
					},
				}))
			},
			expectError: false,
		},
		{
			name: "returns error for leader ID with underscore in cell",
			buildUpdate: func(pm *MultiPoolerManager) *ruleUpdateBuilder {
				return newRuleUpdate(5, &clustermetadatapb.ID{Cell: "zone1", Name: "coordinator-5"}, "promotion", "reason").
					withLeader(&clustermetadatapb.ID{Cell: "zone_1", Name: "leader"}) // underscore in cell is invalid
			},
			setupMock:     func(m *mock.QueryService) {},
			expectError:   true,
			errorContains: "invalid leader ID",
		},
		{
			name: "returns error for cohort member with underscore in name",
			buildUpdate: func(pm *MultiPoolerManager) *ruleUpdateBuilder {
				return newRuleUpdate(5, &clustermetadatapb.ID{Cell: "zone1", Name: "coordinator-5"}, "promotion", "reason").
					withCohort([]*clustermetadatapb.ID{
						{Cell: "zone1", Name: "pooler-1"},
						{Cell: "zone1", Name: "pooler_bad"}, // underscore in name is invalid
					})
			},
			setupMock:     func(m *mock.QueryService) {},
			expectError:   true,
			errorContains: "invalid cohort member ID",
		},
		{
			name: "returns error for accepted member with empty cell",
			buildUpdate: func(pm *MultiPoolerManager) *ruleUpdateBuilder {
				return newRuleUpdate(5, &clustermetadatapb.ID{Cell: "zone1", Name: "coordinator-5"}, "promotion", "reason").
					withAcceptedMembers([]*clustermetadatapb.ID{{Cell: "", Name: "pooler-1"}})
			},
			setupMock:     func(m *mock.QueryService) {},
			expectError:   true,
			errorContains: "invalid accepted member ID",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pm, mockQueryService := newTestManagerWithMock(constants.DefaultTableGroup, constants.DefaultShard)
			tt.setupMock(mockQueryService)

			_, err := pm.updateRule(t.Context(), tt.buildUpdate(pm))

			if tt.expectError {
				assert.Error(t, err)
				if tt.errorContains != "" {
					assert.Contains(t, err.Error(), tt.errorContains)
				}
			} else {
				assert.NoError(t, err)
			}
			assert.NoError(t, mockQueryService.ExpectationsWereMet())
		})
	}
}

func TestQueryRuleHistory(t *testing.T) {
	cols := []string{
		"coordinator_term", "rule_subterm", "event_type", "leader_id", "coordinator_id",
		"wal_position", "operation", "reason", "cohort_members", "accepted_members",
		"durability_policy_name", "durability_quorum_type", "durability_required_count",
		"durability_async_fallback", "created_at",
	}
	createdAt := "2026-03-24 09:00:17.000000+00"

	t.Run("returns records ordered newest first", func(t *testing.T) {
		pm, mockQueryService := newTestManagerWithMock(constants.DefaultTableGroup, constants.DefaultShard)

		leaderID := "zone1_leader-1"
		coordID := "zone1_coordinator-1"
		walPos := "0/1234567"

		mockQueryService.AddQueryPatternOnce(
			"SELECT coordinator_term",
			mock.MakeQueryResult(cols, [][]any{
				{
					int64(2), int64(1), "promotion", leaderID, coordID, walPos, "promotion",
					"manual failover", "{zone1_pooler-2,zone1_pooler-3}", "{zone1_pooler-2}",
					nil, nil, nil, nil, createdAt,
				},
				{
					int64(1), int64(0), "replication_config", leaderID, coordID, nil, "configure",
					"initial bootstrap", "{zone1_pooler-1,zone1_pooler-2}", nil,
					nil, nil, nil, nil, createdAt,
				},
			}),
		)

		records, err := pm.queryRuleHistory(t.Context(), 10)
		require.NoError(t, err)
		require.Len(t, records, 2)

		// First record: term 2, subterm 1 (newest)
		assert.Equal(t, int64(2), records[0].CoordinatorTerm)
		assert.Equal(t, int64(1), records[0].RuleSubterm)
		assert.Equal(t, "promotion", records[0].EventType)
		require.NotNil(t, records[0].LeaderID)
		assert.Equal(t, leaderID, records[0].LeaderID.appName)
		require.NotNil(t, records[0].WALPosition)
		assert.Equal(t, walPos, *records[0].WALPosition)
		assert.Equal(t, "manual failover", records[0].Reason)
		assert.Equal(t, []string{"zone1_pooler-2", "zone1_pooler-3"}, poolerIDsToAppNames(records[0].CohortMembers))
		assert.Equal(t, []string{"zone1_pooler-2"}, poolerIDsToAppNames(records[0].AcceptedMembers))

		// Second record: nullable fields are nil
		assert.Equal(t, int64(1), records[1].CoordinatorTerm)
		assert.Equal(t, int64(0), records[1].RuleSubterm)
		assert.Nil(t, records[1].WALPosition)
		assert.Empty(t, records[1].AcceptedMembers)
		assert.NoError(t, mockQueryService.ExpectationsWereMet())
	})

	t.Run("returns empty slice when no records exist", func(t *testing.T) {
		pm, mockQueryService := newTestManagerWithMock(constants.DefaultTableGroup, constants.DefaultShard)

		mockQueryService.AddQueryPatternOnce("SELECT coordinator_term", mock.MakeQueryResult(cols, [][]any{}))

		records, err := pm.queryRuleHistory(t.Context(), 10)
		require.NoError(t, err)
		assert.Empty(t, records)
		assert.NoError(t, mockQueryService.ExpectationsWereMet())
	})

	t.Run("propagates query error", func(t *testing.T) {
		pm, mockQueryService := newTestManagerWithMock(constants.DefaultTableGroup, constants.DefaultShard)

		mockQueryService.AddQueryPatternOnceWithError("SELECT coordinator_term", errors.New("connection refused"))

		_, err := pm.queryRuleHistory(t.Context(), 10)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "failed to query rule_history")
		assert.NoError(t, mockQueryService.ExpectationsWereMet())
	})

	t.Run("returns error when leader_id is malformed", func(t *testing.T) {
		pm, mockQueryService := newTestManagerWithMock(constants.DefaultTableGroup, constants.DefaultShard)

		mockQueryService.AddQueryPatternOnce("SELECT coordinator_term", mock.MakeQueryResult(cols, [][]any{
			{
				int64(1), int64(0), "promotion", "nounderscore", "zone1_coordinator-1", nil, nil,
				"reason", "{zone1_pooler-1}", nil,
				nil, nil, nil, nil, createdAt,
			},
		}))

		_, err := pm.queryRuleHistory(t.Context(), 10)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "failed to parse rule_history row")
	})

	t.Run("returns error when cohort_members contains malformed entry", func(t *testing.T) {
		pm, mockQueryService := newTestManagerWithMock(constants.DefaultTableGroup, constants.DefaultShard)

		mockQueryService.AddQueryPatternOnce("SELECT coordinator_term", mock.MakeQueryResult(cols, [][]any{
			{
				int64(1), int64(0), "promotion", nil, nil, nil, nil,
				"reason", "{valid_pooler,badentry}", nil,
				nil, nil, nil, nil, createdAt,
			},
		}))

		_, err := pm.queryRuleHistory(t.Context(), 10)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "failed to parse rule_history row")
	})
}

func TestParsePoolerIDStrings(t *testing.T) {
	t.Run("parses valid app name strings", func(t *testing.T) {
		result, err := parsePoolerIDStrings([]string{"zone1_pooler-1", "zone2_pooler-2"})
		require.NoError(t, err)
		require.Len(t, result, 2)
		assert.Equal(t, "zone1_pooler-1", result[0].appName)
		assert.Equal(t, "zone1", result[0].id.Cell)
		assert.Equal(t, "pooler-1", result[0].id.Name)
		assert.Equal(t, "zone2_pooler-2", result[1].appName)
	})

	t.Run("returns empty slice for empty input", func(t *testing.T) {
		result, err := parsePoolerIDStrings([]string{})
		require.NoError(t, err)
		assert.Empty(t, result)
	})

	t.Run("returns nil for nil input", func(t *testing.T) {
		result, err := parsePoolerIDStrings(nil)
		require.NoError(t, err)
		assert.Nil(t, result)
	})

	t.Run("returns error for string missing underscore separator", func(t *testing.T) {
		_, err := parsePoolerIDStrings([]string{"zone1_pooler-1", "nounderscore"})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "nounderscore")
	})

	t.Run("returns error for string with empty cell", func(t *testing.T) {
		_, err := parsePoolerIDStrings([]string{"_pooler-1"})
		require.Error(t, err)
	})

	t.Run("returns error for string with empty name", func(t *testing.T) {
		_, err := parsePoolerIDStrings([]string{"zone1_"})
		require.Error(t, err)
	})
}

func TestCurrentRuleRecord(t *testing.T) {
	// current_rule only stores operational state, not audit fields
	cols := []string{
		"coordinator_term", "rule_subterm", "leader_id", "coordinator_id", "cohort_members",
		"durability_policy_name", "durability_quorum_type", "durability_required_count",
		"durability_async_fallback", "created_at",
	}
	createdAt := "2026-03-24 09:00:17.000000+00"

	t.Run("returns current rule record", func(t *testing.T) {
		pm, mockQueryService := newTestManagerWithMock(constants.DefaultTableGroup, constants.DefaultShard)

		leaderID := "zone1_leader-2"
		coordID := "zone1_coordinator-1"

		mockQueryService.AddQueryPatternOnce(
			"SELECT coordinator_term",
			mock.MakeQueryResult(cols, [][]any{
				{
					int64(3), int64(0), leaderID, coordID,
					"{zone1_pooler-2,zone1_pooler-3}",
					nil, nil, nil, nil, createdAt,
				},
			}),
		)

		record, err := pm.currentRuleRecord(t.Context())
		require.NoError(t, err)
		require.NotNil(t, record)
		assert.Equal(t, int64(3), record.CoordinatorTerm)
		assert.Equal(t, int64(0), record.RuleSubterm)
		require.NotNil(t, record.LeaderID)
		assert.Equal(t, leaderID, record.LeaderID.appName)
		assert.Equal(t, []string{"zone1_pooler-2", "zone1_pooler-3"}, poolerIDsToAppNames(record.CohortMembers))
		assert.NoError(t, mockQueryService.ExpectationsWereMet())
	})

	t.Run("returns nil when table is empty", func(t *testing.T) {
		pm, mockQueryService := newTestManagerWithMock(constants.DefaultTableGroup, constants.DefaultShard)

		mockQueryService.AddQueryPatternOnce("SELECT coordinator_term", mock.MakeQueryResult(cols, [][]any{}))

		record, err := pm.currentRuleRecord(t.Context())
		require.NoError(t, err)
		assert.Nil(t, record)
		assert.NoError(t, mockQueryService.ExpectationsWereMet())
	})

	t.Run("propagates query error", func(t *testing.T) {
		pm, mockQueryService := newTestManagerWithMock(constants.DefaultTableGroup, constants.DefaultShard)

		mockQueryService.AddQueryPatternOnceWithError("SELECT coordinator_term", errors.New("connection refused"))

		_, err := pm.currentRuleRecord(t.Context())
		require.Error(t, err)
		assert.Contains(t, err.Error(), "failed to query current_rule")
		assert.NoError(t, mockQueryService.ExpectationsWereMet())
	})

	t.Run("returns error when leader_id is malformed", func(t *testing.T) {
		pm, mockQueryService := newTestManagerWithMock(constants.DefaultTableGroup, constants.DefaultShard)

		mockQueryService.AddQueryPatternOnce("SELECT coordinator_term", mock.MakeQueryResult(cols, [][]any{
			{int64(1), int64(0), "nounderscore", nil, "{zone1_pooler-1}", nil, nil, nil, nil, createdAt},
		}))

		_, err := pm.currentRuleRecord(t.Context())
		require.Error(t, err)
		assert.Contains(t, err.Error(), "failed to parse current_rule row")
	})

	t.Run("returns error when cohort_members contains malformed entry", func(t *testing.T) {
		pm, mockQueryService := newTestManagerWithMock(constants.DefaultTableGroup, constants.DefaultShard)

		mockQueryService.AddQueryPatternOnce("SELECT coordinator_term", mock.MakeQueryResult(cols, [][]any{
			{int64(1), int64(0), nil, nil, "{valid_pooler,badentry}", nil, nil, nil, nil, createdAt},
		}))

		_, err := pm.currentRuleRecord(t.Context())
		require.Error(t, err)
		assert.Contains(t, err.Error(), "failed to parse current_rule row")
	})
}

func TestInsertReplicationConfigHistory(t *testing.T) {
	tests := []struct {
		name          string
		termNumber    int64
		operation     string
		reason        string
		standbyIDs    []*clustermetadatapb.ID
		setupMock     func(m *mock.QueryService)
		expectError   bool
		errorContains string
	}{
		{
			name:       "successful insert with configure operation",
			termNumber: 1,
			operation:  "configure",
			reason:     "ConfigureSynchronousReplication called",
			standbyIDs: []*clustermetadatapb.ID{
				{Cell: "us-west", Name: "replica-1"},
				{Cell: "us-west", Name: "replica-2"},
			},
			setupMock: func(m *mock.QueryService) {
				returnCols := []string{
					"coordinator_term", "rule_subterm", "event_type", "leader_id", "coordinator_id",
					"wal_position", "operation", "reason", "cohort_members", "accepted_members",
					"durability_policy_name", "durability_quorum_type", "durability_required_count",
					"durability_async_fallback", "created_at",
				}
				m.AddQueryPatternOnce("FROM multigres.current_rule", mock.MakeQueryResult(returnCols, [][]any{
					{
						int64(1), int64(0), "replication_config", nil, nil,
						nil, "configure", "ConfigureSynchronousReplication called",
						"{us-west_replica-1,us-west_replica-2}", nil,
						nil, nil, nil, nil, "2026-03-24 09:00:17.000000+00",
					},
				}))
			},
			expectError: false,
		},
		{
			name:       "successful insert with add operation",
			termNumber: 2,
			operation:  "add",
			reason:     "UpdateSynchronousStandbyList: add",
			standbyIDs: []*clustermetadatapb.ID{
				{Cell: "us-west", Name: "replica-3"},
			},
			setupMock: func(m *mock.QueryService) {
				returnCols := []string{
					"coordinator_term", "rule_subterm", "event_type", "leader_id", "coordinator_id",
					"wal_position", "operation", "reason", "cohort_members", "accepted_members",
					"durability_policy_name", "durability_quorum_type", "durability_required_count",
					"durability_async_fallback", "created_at",
				}
				m.AddQueryPatternOnce("FROM multigres.current_rule", mock.MakeQueryResult(returnCols, [][]any{
					{
						int64(2), int64(0), "replication_config", nil, nil,
						nil, "add", "UpdateSynchronousStandbyList: add",
						"{us-west_replica-3}", nil,
						nil, nil, nil, nil, "2026-03-24 09:00:17.000000+00",
					},
				}))
			},
			expectError: false,
		},
		{
			name:       "write fails with database error",
			termNumber: 3,
			operation:  "remove",
			reason:     "UpdateSynchronousStandbyList: remove",
			standbyIDs: []*clustermetadatapb.ID{
				{Cell: "us-west", Name: "replica-1"},
			},
			setupMock: func(m *mock.QueryService) {
				m.AddQueryPatternOnceWithError("FROM multigres.current_rule", errors.New("timeout waiting for sync replication"))
			},
			expectError:   true,
			errorContains: "failed to write rule history record",
		},
		{
			name:       "insert with empty standby list",
			termNumber: 4,
			operation:  "configure",
			reason:     "ConfigureSynchronousReplication called",
			standbyIDs: []*clustermetadatapb.ID{},
			setupMock: func(m *mock.QueryService) {
				returnCols := []string{
					"coordinator_term", "rule_subterm", "event_type", "leader_id", "coordinator_id",
					"wal_position", "operation", "reason", "cohort_members", "accepted_members",
					"durability_policy_name", "durability_quorum_type", "durability_required_count",
					"durability_async_fallback", "created_at",
				}
				m.AddQueryPatternOnce("FROM multigres.current_rule", mock.MakeQueryResult(returnCols, [][]any{
					{
						int64(4), int64(0), "replication_config", nil, nil,
						nil, "configure", "ConfigureSynchronousReplication called",
						"{}", nil,
						nil, nil, nil, nil, "2026-03-24 09:00:17.000000+00",
					},
				}))
			},
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pm, mockQueryService := newTestManagerWithMock(constants.DefaultTableGroup, constants.DefaultShard)

			tt.setupMock(mockQueryService)

			ctx := context.Background()

			_, err := pm.updateRule(ctx, newRuleUpdate(tt.termNumber, pm.serviceID, "replication_config", tt.reason).
				withCohort(tt.standbyIDs).
				withOperation(tt.operation))

			if tt.expectError {
				assert.Error(t, err)
				if tt.errorContains != "" {
					assert.Contains(t, err.Error(), tt.errorContains)
				}
			} else {
				assert.NoError(t, err)
			}
			assert.NoError(t, mockQueryService.ExpectationsWereMet())
		})
	}
}
