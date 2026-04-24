// Copyright 2025 Supabase, Inc.
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

package manager

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/cmd/pgctld/testutil"
	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/common/servenv"
	"github.com/multigres/multigres/go/common/topoclient/memorytopo"
	"github.com/multigres/multigres/go/services/multipooler/executor/mock"
	"github.com/multigres/multigres/go/tools/viperutil"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	consensusdatapb "github.com/multigres/multigres/go/pb/consensusdata"
	pgctldpb "github.com/multigres/multigres/go/pb/pgctldservice"
)

// expectStandbyRevokeMocks sets up mock expectations for the standby WAL freeze path
// in Recruit: health check, role determination, pause replication, and replay stabilization.
func expectStandbyRevokeMocks(m *mock.QueryService, lsn string) {
	replStatusCols := []string{"replay_lsn", "receive_lsn", "is_paused", "pause_state", "last_xact_replay_ts", "primary_conninfo", "status", "last_msg_receive_time", "wal_receiver_status_interval", "wal_receiver_timeout"}
	replStatusRow := [][]any{{lsn, lsn, false, "not paused", nil, "", nil, nil, nil, nil}}

	// Replay state columns used by queryReplayState during stabilization polling
	replayStateCols := []string{"replay_lsn", "is_paused"}
	replayStateRow := [][]any{{lsn, false}}

	// freezeWAL: health check (SELECT 1)
	m.AddQueryPatternOnce("^SELECT 1$", mock.MakeQueryResult(nil, nil))
	// freezeWAL: determine role (standby)
	m.AddQueryPatternOnce("SELECT pg_is_in_recovery", mock.MakeQueryResult([]string{"pg_is_in_recovery"}, [][]any{{"t"}}))
	// pauseReplication: resetPrimaryConnInfo
	m.AddQueryPatternOnce("ALTER SYSTEM RESET primary_conninfo", mock.MakeQueryResult(nil, nil))
	m.AddQueryPatternOnce("SELECT pg_reload_conf", mock.MakeQueryResult(nil, nil))
	// waitForReceiverDisconnect
	m.AddQueryPatternOnce("SELECT COUNT.*pg_stat_wal_receiver", mock.MakeQueryResult([]string{"count"}, [][]any{{int64(0)}}))
	// queryReplicationStatus (from waitForReceiverDisconnect)
	m.AddQueryPatternOnce("pg_last_wal_replay_lsn", mock.MakeQueryResult(replStatusCols, replStatusRow))
	// waitForReplayStabilize: three consecutive polls with same replay_lsn = stable
	m.AddQueryPatternOnce("^SELECT pg_last_wal_replay_lsn", mock.MakeQueryResult(replayStateCols, replayStateRow))
	m.AddQueryPatternOnce("^SELECT pg_last_wal_replay_lsn", mock.MakeQueryResult(replayStateCols, replayStateRow))
	m.AddQueryPatternOnce("^SELECT pg_last_wal_replay_lsn", mock.MakeQueryResult(replayStateCols, replayStateRow))
	// Final queryReplicationStatus after stability confirmed
	m.AddQueryPatternOnce("pg_last_wal_replay_lsn", mock.MakeQueryResult(replStatusCols, replStatusRow))
}

func setupManagerWithMockDB(t *testing.T, mockQueryService *mock.QueryService, rules ruleStorer) (*MultiPoolerManager, string) {
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	ts, _ := memorytopo.NewServerAndFactory(ctx, "zone1")
	t.Cleanup(func() { ts.Close() })

	pgctldAddr, cleanupPgctld := testutil.StartMockPgctldServer(t, &testutil.MockPgCtldService{})
	t.Cleanup(cleanupPgctld)

	// Create the database in topology with backup location
	database := "testdb"
	addDatabaseToTopo(t, ts, database)

	serviceID := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      "zone1",
		Name:      "test-pooler",
	}
	multipooler := &clustermetadatapb.MultiPooler{
		Id:            serviceID,
		Database:      database,
		Hostname:      "localhost",
		PortMap:       map[string]int32{"grpc": 8080},
		Type:          clustermetadatapb.PoolerType_PRIMARY,
		ServingStatus: clustermetadatapb.PoolerServingStatus_SERVING,
		TableGroup:    constants.DefaultTableGroup,
		Shard:         constants.DefaultShard,
	}
	require.NoError(t, ts.CreateMultiPooler(ctx, multipooler))

	tmpDir := t.TempDir()
	multipooler.PoolerDir = tmpDir
	config := &Config{
		TopoClient: ts,
		PgctldAddr: pgctldAddr,
	}
	pm, err := NewMultiPoolerManager(logger, multipooler, config)
	require.NoError(t, err)
	t.Cleanup(func() { pm.Shutdown() })

	// Assign mock pooler controller and rule store BEFORE starting the manager
	// to avoid race conditions.
	pm.qsc = &mockPoolerController{queryService: mockQueryService}
	pm.rules = rules

	senv := servenv.NewServEnv(viperutil.NewRegistry())
	pm.Start(senv)

	require.Eventually(t, func() bool {
		return pm.GetState() == ManagerStateReady
	}, 5*time.Second, 100*time.Millisecond, "Manager should reach Ready state")

	// Create the pg_data directory to simulate initialized data directory
	pgDataDir := tmpDir + "/pg_data"
	err = os.MkdirAll(pgDataDir, 0o755)
	require.NoError(t, err)
	// Create PG_VERSION file to mark it as initialized
	err = os.WriteFile(pgDataDir+"/PG_VERSION", []byte("18\n"), 0o644)
	require.NoError(t, err)
	t.Setenv(constants.PgDataDirEnvVar, pgDataDir)

	// Initialize consensus state
	pm.mu.Lock()
	pm.consensusState = NewConsensusState(tmpDir, serviceID)
	pm.mu.Unlock()

	return pm, tmpDir
}

// ============================================================================
// Recruit Tests
// ============================================================================

func TestRecruit(t *testing.T) {
	coordinatorID := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      "zone1",
		Name:      "coordinator-A",
	}

	tests := []struct {
		name          string
		initialTerm   int64
		requestTerm   int64
		coordinator   *clustermetadatapb.ID
		setupMocks    func(*mock.QueryService)
		expectedError bool
		description   string
	}{
		{
			name:        "StaleTerm_PreCheckFails",
			initialTerm: 10,
			requestTerm: 5,
			coordinator: coordinatorID,
			setupMocks: func(m *mock.QueryService) {
				// Pre-check fails; no WAL freeze queries expected
			},
			expectedError: true,
			description:   "Proposed term < current term → pre-check fails, no WAL freeze",
		},
		{
			name:        "SameTermDifferentCoordinator_PreCheckFails",
			initialTerm: 5,
			requestTerm: 5,
			coordinator: &clustermetadatapb.ID{
				Component: clustermetadatapb.ID_MULTIPOOLER,
				Cell:      "zone1",
				Name:      "coordinator-B",
			},
			setupMocks: func(m *mock.QueryService) {
				// Initial term accepted coordinator-A; coordinator-B rejected at pre-check
			},
			expectedError: true,
			description:   "Same term but different coordinator already accepted → pre-check fails",
		},
		{
			name:        "PostgresDown_WALFreezeFailsNoAcceptance",
			initialTerm: 5,
			requestTerm: 10,
			coordinator: coordinatorID,
			setupMocks: func(m *mock.QueryService) {
				// health check (SELECT 1) fails → postgres is down → WAL freeze fails
				// No SELECT 1 expectation added → will fail
			},
			expectedError: true,
			description:   "Postgres is unhealthy, WAL freeze fails, term not accepted",
		},
		{
			name:        "StandbySuccessfulRecruit",
			initialTerm: 5,
			requestTerm: 10,
			coordinator: coordinatorID,
			setupMocks: func(m *mock.QueryService) {
				expectStandbyRevokeMocks(m, "0/5000000")
			},
			expectedError: false,
			description:   "Standby pauses replication and accepts revocation",
		},
		{
			name:        "HigherTerm_ReplacesExistingRevocation",
			initialTerm: 3,
			requestTerm: 7,
			coordinator: coordinatorID,
			setupMocks: func(m *mock.QueryService) {
				expectStandbyRevokeMocks(m, "0/7000000")
			},
			expectedError: false,
			description:   "Higher term replaces existing revocation",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()

			mockQueryService := mock.NewQueryService()
			tt.setupMocks(mockQueryService)

			pm, tmpDir := setupManagerWithMockDB(t, mockQueryService, &fakeRuleStore{})

			// For same-term-different-coordinator test, pre-set coordinator-A acceptance
			initialRevocation := &consensusdatapb.TermRevocation{
				RevokedBelowTerm: tt.initialTerm,
			}
			if tt.name == "SameTermDifferentCoordinator_PreCheckFails" {
				initialRevocation.AcceptedCoordinatorId = coordinatorID
			}
			err := pm.consensusState.setConsensusTerm(initialRevocation)
			require.NoError(t, err)
			_, err = pm.consensusState.Load()
			require.NoError(t, err)

			req := &consensusdatapb.RecruitRequest{
				TermRevocation: &consensusdatapb.TermRevocation{
					RevokedBelowTerm:      tt.requestTerm,
					AcceptedCoordinatorId: tt.coordinator,
				},
			}

			resp, err := pm.Recruit(ctx, req)

			if tt.expectedError {
				require.Error(t, err, tt.description)
				_ = resp // response may be non-nil even on error (carries status)
			} else {
				require.NoError(t, err, tt.description)
				require.NotNil(t, resp)
				// Verify the revocation was persisted
				persistedRevocation, loadErr := pm.consensusState.getConsensusTerm()
				require.NoError(t, loadErr)
				assert.Equal(t, tt.requestTerm, persistedRevocation.RevokedBelowTerm)
			}

			// Verify disk state for acceptance cases
			if !tt.expectedError {
				persistedRevocation, loadErr := pm.consensusState.getConsensusTerm()
				require.NoError(t, loadErr)
				assert.Equal(t, tt.coordinator.GetName(), persistedRevocation.GetAcceptedCoordinatorId().GetName())
			}

			assert.NoError(t, mockQueryService.ExpectationsWereMet())

			// Cleanup tmpDir permissions if needed
			_ = tmpDir
		})
	}
}

// ============================================================================
// AcceptRevocation Tests
// ============================================================================

// setActionLockHeld is a test helper that creates a context with action lock held
func setActionLockHeld(ctx context.Context) context.Context {
	lock := NewActionLock()
	newCtx, err := lock.Acquire(ctx, "test-operation")
	if err != nil {
		panic(err)
	}
	return newCtx
}

func TestAcceptRevocation(t *testing.T) {
	coordinatorA := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      "zone1",
		Name:      "coordinator-A",
	}
	coordinatorB := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      "zone1",
		Name:      "coordinator-B",
	}

	tests := []struct {
		name          string
		initial       *consensusdatapb.TermRevocation
		incoming      *consensusdatapb.TermRevocation
		expectError   bool
		expectedTerm  int64
		expectedCoord string
	}{
		{
			name:    "HigherTerm_Accepted",
			initial: &consensusdatapb.TermRevocation{RevokedBelowTerm: 5},
			incoming: &consensusdatapb.TermRevocation{
				RevokedBelowTerm:      10,
				AcceptedCoordinatorId: coordinatorA,
			},
			expectError:   false,
			expectedTerm:  10,
			expectedCoord: "coordinator-A",
		},
		{
			name: "SameTerm_SameCoordinator_SameTimestamp_Idempotent",
			initial: &consensusdatapb.TermRevocation{
				RevokedBelowTerm:      5,
				AcceptedCoordinatorId: coordinatorA,
				// CoordinatorInitiatedAt nil == same as incoming nil
			},
			incoming: &consensusdatapb.TermRevocation{
				RevokedBelowTerm:      5,
				AcceptedCoordinatorId: coordinatorA,
			},
			expectError:   false,
			expectedTerm:  5,
			expectedCoord: "coordinator-A",
		},
		{
			name: "SameTerm_DifferentCoordinator_Rejected",
			initial: &consensusdatapb.TermRevocation{
				RevokedBelowTerm:      5,
				AcceptedCoordinatorId: coordinatorA,
			},
			incoming: &consensusdatapb.TermRevocation{
				RevokedBelowTerm:      5,
				AcceptedCoordinatorId: coordinatorB,
			},
			expectError: true,
		},
		{
			name:    "StaleTerm_Rejected",
			initial: &consensusdatapb.TermRevocation{RevokedBelowTerm: 10},
			incoming: &consensusdatapb.TermRevocation{
				RevokedBelowTerm:      5,
				AcceptedCoordinatorId: coordinatorA,
			},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			poolerDir := t.TempDir()
			serviceID := &clustermetadatapb.ID{
				Component: clustermetadatapb.ID_MULTIPOOLER,
				Cell:      "zone1",
				Name:      "test-pooler",
			}

			pgDataDir := poolerDir + "/pg_data"
			require.NoError(t, os.MkdirAll(pgDataDir, 0o755))
			require.NoError(t, os.WriteFile(pgDataDir+"/PG_VERSION", []byte("18\n"), 0o644))
			t.Setenv(constants.PgDataDirEnvVar, pgDataDir)

			cs := NewConsensusState(poolerDir, serviceID)
			require.NoError(t, cs.setConsensusTerm(tt.initial))
			_, err := cs.Load()
			require.NoError(t, err)

			ctx := context.Background()
			ctx = setActionLockHeld(ctx)

			err = cs.AcceptRevocation(ctx, tt.incoming)

			if tt.expectError {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			rev, err := cs.GetRevocation(ctx)
			require.NoError(t, err)
			assert.Equal(t, tt.expectedTerm, rev.RevokedBelowTerm)
			assert.Equal(t, tt.expectedCoord, rev.GetAcceptedCoordinatorId().GetName())
		})
	}
}

// ============================================================================
// ConsensusStatus Tests
// ============================================================================

func TestConsensusStatus(t *testing.T) {
	tests := []struct {
		name                string
		initialRevocation   *consensusdatapb.TermRevocation
		termInMemory        bool
		nilQsc              bool
		setupMock           func(*mock.QueryService)
		expectedCurrentTerm int64
		expectedIsHealthy   bool
		expectedRole        consensusdatapb.PostgresRole
		expectedWALLsn      string
		description         string
	}{
		{
			name: "HealthyPrimary",
			initialRevocation: &consensusdatapb.TermRevocation{
				RevokedBelowTerm: 5,
				AcceptedCoordinatorId: &clustermetadatapb.ID{
					Cell: "zone1",
					Name: "leader-node",
				},
			},
			termInMemory: true,
			setupMock: func(m *mock.QueryService) {
				// Health check SELECT 1
				m.AddQueryPatternOnce("^SELECT 1$", mock.MakeQueryResult(nil, nil))
				// Single pg_is_in_recovery check determines both role and which WAL position to query
				m.AddQueryPatternOnce("SELECT pg_is_in_recovery", mock.MakeQueryResult([]string{"pg_is_in_recovery"}, [][]any{{"f"}}))
				m.AddQueryPatternOnce("SELECT pg_current_wal_lsn", mock.MakeQueryResult([]string{"pg_current_wal_lsn"}, [][]any{{"0/4000000"}}))
			},
			expectedCurrentTerm: 5,
			expectedIsHealthy:   true,
			expectedRole:        consensusdatapb.PostgresRole_POSTGRES_ROLE_PRIMARY,
			expectedWALLsn:      "0/4000000",
			description:         "Healthy primary should return correct status with WAL position",
		},
		{
			name: "HealthyStandby",
			initialRevocation: &consensusdatapb.TermRevocation{
				RevokedBelowTerm: 3,
			},
			termInMemory: true,
			setupMock: func(m *mock.QueryService) {
				// Health check SELECT 1
				m.AddQueryPatternOnce("^SELECT 1$", mock.MakeQueryResult(nil, nil))
				// Single pg_is_in_recovery check determines both role and which WAL position to query
				m.AddQueryPatternOnce("SELECT pg_is_in_recovery", mock.MakeQueryResult([]string{"pg_is_in_recovery"}, [][]any{{"t"}}))
				// queryReplicationStatus() expects full replication status query
				m.AddQueryPatternOnce("pg_last_wal_replay_lsn", mock.MakeQueryResult(
					[]string{
						"pg_last_wal_replay_lsn",
						"pg_last_wal_receive_lsn",
						"pg_is_wal_replay_paused",
						"pg_get_wal_replay_pause_state",
						"pg_last_xact_replay_timestamp",
						"current_setting",
						"wal_receiver_status",
						"last_msg_receive_time",
						"wal_receiver_status_interval",
						"wal_receiver_timeout",
					},
					[][]any{{"0/4FFFFFF", "0/5000000", "f", "not paused", nil, "", "streaming", nil, nil, nil}}))
			},
			expectedCurrentTerm: 3,
			expectedIsHealthy:   true,
			expectedRole:        consensusdatapb.PostgresRole_POSTGRES_ROLE_REPLICA,
			expectedWALLsn:      "0/5000000", // receive LSN
			description:         "Healthy standby should return correct status with receive/replay LSNs",
		},
		{
			name: "NoDatabaseConnection",
			initialRevocation: &consensusdatapb.TermRevocation{
				RevokedBelowTerm: 7,
			},
			termInMemory:        true,
			nilQsc:              true,
			setupMock:           func(m *mock.QueryService) {},
			expectedCurrentTerm: 7,
			expectedIsHealthy:   false,
			expectedRole:        consensusdatapb.PostgresRole_POSTGRES_ROLE_UNSPECIFIED, // no database, we can't check the postgres role
			description:         "Should handle missing database connection gracefully",
		},
		{
			name: "DatabaseQueryFailure",
			initialRevocation: &consensusdatapb.TermRevocation{
				RevokedBelowTerm: 4,
			},
			termInMemory: true,
			setupMock: func(m *mock.QueryService) {
				// Health check fails
				m.AddQueryPatternOnceWithError("^SELECT 1$", errors.New("connection refused"))
			},
			expectedCurrentTerm: 4,
			expectedIsHealthy:   false,
			expectedRole:        consensusdatapb.PostgresRole_POSTGRES_ROLE_UNSPECIFIED,
			description:         "Should handle database query failure gracefully",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()

			// Create mock and set ALL expectations BEFORE starting the manager
			mockQueryService := mock.NewQueryService()
			tt.setupMock(mockQueryService)

			pm, _ := setupManagerWithMockDB(t, mockQueryService, &fakeRuleStore{})

			// Initialize term on disk
			err := pm.consensusState.setConsensusTerm(tt.initialRevocation)
			require.NoError(t, err)

			// Load term into consensus state if term should be in memory
			if tt.termInMemory {
				loadedTerm, err := pm.consensusState.Load()
				require.NoError(t, err)
				assert.Equal(t, tt.expectedCurrentTerm, loadedTerm, "Loaded term should match expected current term")
			}

			// Handle nil qsc case
			if tt.nilQsc {
				pm.qsc = nil
			}

			req := &consensusdatapb.StatusRequest{
				ShardId: "test-shard",
			}

			resp, err := pm.ConsensusStatus(ctx, req)

			// Verify response
			require.NoError(t, err, tt.description)
			require.NotNil(t, resp)
			assert.Equal(t, "test-pooler", resp.PoolerId)
			assert.Equal(t, tt.expectedCurrentTerm, resp.GetConsensusStatus().GetTermRevocation().GetRevokedBelowTerm())
			assert.Equal(t, tt.expectedIsHealthy, resp.IsHealthy, tt.description)
			assert.True(t, resp.IsEligible)
			assert.Equal(t, "zone1", resp.Cell)
			assert.Equal(t, tt.expectedRole, resp.Role)

			// Verify WAL position if expected
			require.NotNil(t, resp.WalPosition)
			if tt.expectedWALLsn != "" {
				if tt.expectedRole == consensusdatapb.PostgresRole_POSTGRES_ROLE_PRIMARY {
					assert.Equal(t, tt.expectedWALLsn, resp.WalPosition.CurrentLsn)
				} else if tt.expectedRole == consensusdatapb.PostgresRole_POSTGRES_ROLE_REPLICA && tt.expectedIsHealthy {
					assert.Equal(t, tt.expectedWALLsn, resp.WalPosition.LastReceiveLsn)
				}
			}

			// Verify term was loaded if applicable
			if !tt.termInMemory && !tt.nilQsc {
				// Acquire action lock to inspect consensus state
				inspectCtx, err := pm.actionLock.Acquire(ctx, "inspect")
				require.NoError(t, err)
				currentTerm, err := pm.consensusState.GetRevokedBelowTerm(inspectCtx)
				require.NoError(t, err)
				assert.Equal(t, tt.expectedCurrentTerm, currentTerm, "Term should be loaded into memory")
				pm.actionLock.Release(inspectCtx)
			}
			assert.NoError(t, mockQueryService.ExpectationsWereMet())
		})
	}
}

// ============================================================================
// DemoteStalePrimary Tests
// ============================================================================

func TestDemoteStalePrimary_UpdatesConsensusTerm(t *testing.T) {
	tests := []struct {
		name                       string
		initialRevocation          *consensusdatapb.TermRevocation
		requestTerm                int64
		force                      bool
		setupPgRewindMock          func(*testutil.MockPgCtldService)
		setupQueryMock             func(*mock.QueryService)
		expectedFinalConsensusTerm int64
		expectedError              bool
		expectedErrorContains      string
		description                string
	}{
		{
			name: "SuccessfulDemotion_UpdatesTermFromLowerToHigher",
			initialRevocation: &consensusdatapb.TermRevocation{
				RevokedBelowTerm: 5,
			},
			requestTerm: 10,
			force:       false,
			setupPgRewindMock: func(m *testutil.MockPgCtldService) {
				// pg_rewind dry-run reports no divergence (servers already aligned)
				m.PgRewindResponse = &pgctldpb.PgRewindResponse{
					Message: "No divergence detected",
					Output:  "", // Empty output = no divergence
				}
			},
			setupQueryMock: func(m *mock.QueryService) {
				// waitForDatabaseConnection after restart - health check
				m.AddQueryPattern("^SELECT 1$", mock.MakeQueryResult(nil, nil))

				// resetSynchronousReplication queries
				m.AddQueryPattern("SELECT pg_is_in_recovery", mock.MakeQueryResult([]string{"pg_is_in_recovery"}, [][]any{{"t"}}))
				m.AddQueryPatternOnce("ALTER SYSTEM RESET synchronous_standby_names", mock.MakeQueryResult(nil, nil))
				m.AddQueryPatternOnce("SELECT pg_reload_conf", mock.MakeQueryResult(nil, nil)) // First pg_reload_conf call

				// setPrimaryConnInfoLocked queries
				m.AddQueryPatternOnce("ALTER SYSTEM SET primary_conninfo = 'host=correct-primary-host port=5433 user=postgres application_name=zone1_stale-primary'", mock.MakeQueryResult(nil, nil))
				m.AddQueryPatternOnce("SELECT pg_reload_conf", mock.MakeQueryResult(nil, nil)) // Second pg_reload_conf call
			},
			// updateTermIfNewer is a no-op: term stays at initial RevokedBelowTerm after demotion
			expectedFinalConsensusTerm: 5,
			expectedError:              false,
			description:                "Successful demotion should proceed; term advances via Recruit not DemoteStalePrimary",
		},
		{
			name: "OutdatedTerm_RejectedWithoutForce",
			initialRevocation: &consensusdatapb.TermRevocation{
				RevokedBelowTerm: 15,
			},
			requestTerm: 10,
			force:       false,
			setupPgRewindMock: func(m *testutil.MockPgCtldService) {
				// Should not reach pg_rewind since term validation fails
			},
			setupQueryMock: func(m *mock.QueryService) {
				// No queries should execute
			},
			expectedFinalConsensusTerm: 15, // Term should remain unchanged
			expectedError:              true,
			expectedErrorContains:      "consensus term too old",
			description:                "Should reject outdated term without force flag",
		},
		{
			name: "OutdatedTerm_AcceptedWithForce",
			initialRevocation: &consensusdatapb.TermRevocation{
				RevokedBelowTerm: 15,
			},
			requestTerm: 10,
			force:       true,
			setupPgRewindMock: func(m *testutil.MockPgCtldService) {
				m.PgRewindResponse = &pgctldpb.PgRewindResponse{
					Message: "No divergence",
					Output:  "",
				}
			},
			setupQueryMock: func(m *mock.QueryService) {
				// waitForDatabaseConnection after restart - health check
				m.AddQueryPattern("^SELECT 1$", mock.MakeQueryResult(nil, nil))

				// resetSynchronousReplication queries
				m.AddQueryPattern("SELECT pg_is_in_recovery", mock.MakeQueryResult([]string{"pg_is_in_recovery"}, [][]any{{"t"}}))
				m.AddQueryPatternOnce("ALTER SYSTEM RESET synchronous_standby_names", mock.MakeQueryResult(nil, nil))
				m.AddQueryPatternOnce("SELECT pg_reload_conf", mock.MakeQueryResult(nil, nil)) // First pg_reload_conf call

				// setPrimaryConnInfoLocked queries
				m.AddQueryPatternOnce("ALTER SYSTEM SET primary_conninfo = 'host=correct-primary-host port=5433 user=postgres application_name=zone1_stale-primary'", mock.MakeQueryResult(nil, nil))
				m.AddQueryPatternOnce("SELECT pg_reload_conf", mock.MakeQueryResult(nil, nil)) // Second pg_reload_conf call
			},
			// updateTermIfNewer is a no-op: term stays at 15 (unchanged)
			expectedFinalConsensusTerm: 15,
			expectedError:              false,
			description:                "With force=true, should accept outdated term; term unchanged since updateTermIfNewer is no-op",
		},
		{
			name: "SameTerm_Idempotent",
			initialRevocation: &consensusdatapb.TermRevocation{
				RevokedBelowTerm: 10,
			},
			requestTerm: 10,
			force:       false,
			setupPgRewindMock: func(m *testutil.MockPgCtldService) {
				m.PgRewindResponse = &pgctldpb.PgRewindResponse{
					Message: "No divergence",
					Output:  "",
				}
			},
			setupQueryMock: func(m *mock.QueryService) {
				// waitForDatabaseConnection after restart - health check
				m.AddQueryPattern("^SELECT 1$", mock.MakeQueryResult(nil, nil))

				// resetSynchronousReplication queries
				m.AddQueryPattern("SELECT pg_is_in_recovery", mock.MakeQueryResult([]string{"pg_is_in_recovery"}, [][]any{{"t"}}))
				m.AddQueryPatternOnce("ALTER SYSTEM RESET synchronous_standby_names", mock.MakeQueryResult(nil, nil))
				m.AddQueryPatternOnce("SELECT pg_reload_conf", mock.MakeQueryResult(nil, nil)) // First pg_reload_conf call

				// setPrimaryConnInfoLocked queries
				m.AddQueryPatternOnce("ALTER SYSTEM SET primary_conninfo = 'host=correct-primary-host port=5433 user=postgres application_name=zone1_stale-primary'", mock.MakeQueryResult(nil, nil))
				m.AddQueryPatternOnce("SELECT pg_reload_conf", mock.MakeQueryResult(nil, nil)) // Second pg_reload_conf call
			},
			expectedFinalConsensusTerm: 10,
			expectedError:              false,
			description:                "Idempotent: same term should succeed",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
			ts, _ := memorytopo.NewServerAndFactory(ctx, "zone1")
			t.Cleanup(func() { ts.Close() })

			// Start mock pgctld server
			mockPgctld := &testutil.MockPgCtldService{}
			tt.setupPgRewindMock(mockPgctld)
			pgctldAddr, cleanupPgctld := testutil.StartMockPgctldServer(t, mockPgctld)
			t.Cleanup(cleanupPgctld)

			// Create the database in topology
			database := "testdb"
			addDatabaseToTopo(t, ts, database)

			serviceID := &clustermetadatapb.ID{
				Component: clustermetadatapb.ID_MULTIPOOLER,
				Cell:      "zone1",
				Name:      "stale-primary",
			}
			multipooler := &clustermetadatapb.MultiPooler{
				Id:            serviceID,
				Database:      database,
				Hostname:      "localhost",
				PortMap:       map[string]int32{"grpc": 8080, "postgres": 5432},
				Type:          clustermetadatapb.PoolerType_PRIMARY, // Starting as PRIMARY
				ServingStatus: clustermetadatapb.PoolerServingStatus_SERVING,
				TableGroup:    constants.DefaultTableGroup,
				Shard:         constants.DefaultShard,
			}
			require.NoError(t, ts.CreateMultiPooler(ctx, multipooler))

			tmpDir := t.TempDir()
			multipooler.PoolerDir = tmpDir

			// Create pg_data directory
			pgDataDir := tmpDir + "/pg_data"
			err := os.MkdirAll(pgDataDir, 0o755)
			require.NoError(t, err)
			err = os.WriteFile(pgDataDir+"/PG_VERSION", []byte("18\n"), 0o644)
			require.NoError(t, err)
			t.Setenv(constants.PgDataDirEnvVar, pgDataDir)

			config := &Config{
				TopoClient: ts,
				PgctldAddr: pgctldAddr,
			}
			pm, err := NewMultiPoolerManager(logger, multipooler, config)
			require.NoError(t, err)
			t.Cleanup(func() { pm.Shutdown() })

			// Set up mock query service
			mockQueryService := mock.NewQueryService()

			tt.setupQueryMock(mockQueryService)
			pm.qsc = &mockPoolerController{queryService: mockQueryService}

			senv := servenv.NewServEnv(viperutil.NewRegistry())
			pm.Start(senv)
			require.Eventually(t, func() bool {
				return pm.GetState() == ManagerStateReady
			}, 5*time.Second, 100*time.Millisecond, "Manager should reach Ready state")

			// Initialize consensus state and set initial term
			pm.mu.Lock()
			pm.consensusState = NewConsensusState(tmpDir, serviceID)
			pm.mu.Unlock()

			err = pm.consensusState.setConsensusTerm(tt.initialRevocation)
			require.NoError(t, err)
			_, err = pm.consensusState.Load()
			require.NoError(t, err)

			// Create source pooler (the correct primary)
			sourcePooler := &clustermetadatapb.MultiPooler{
				Id: &clustermetadatapb.ID{
					Component: clustermetadatapb.ID_MULTIPOOLER,
					Cell:      "zone1",
					Name:      "correct-primary",
				},
				Hostname: "correct-primary-host",
				PortMap:  map[string]int32{"postgres": 5433},
			}

			// Call DemoteStalePrimary
			resp, err := pm.DemoteStalePrimary(ctx, sourcePooler, tt.requestTerm, tt.force)

			// Verify error expectation
			if tt.expectedError {
				require.Error(t, err, tt.description)
				if tt.expectedErrorContains != "" {
					assert.Contains(t, err.Error(), tt.expectedErrorContains, tt.description)
				}
			} else {
				require.NoError(t, err, tt.description)
				require.NotNil(t, resp)
				assert.True(t, resp.Success, tt.description)
			}

			// Verify consensus term was not modified (updateTermIfNewer is a no-op)
			persistedRevocation, err := pm.consensusState.getConsensusTerm()
			require.NoError(t, err)
			assert.Equal(t, tt.expectedFinalConsensusTerm, persistedRevocation.RevokedBelowTerm,
				"Consensus term should be %d but got %d", tt.expectedFinalConsensusTerm, persistedRevocation.RevokedBelowTerm)

			// Verify topology was updated to REPLICA (only on success).
			// The write is asynchronous so we poll until the publisher catches up.
			if !tt.expectedError {
				require.Eventually(t, func() bool {
					updatedPooler, err := ts.GetMultiPooler(ctx, serviceID)
					return err == nil && updatedPooler.Type == clustermetadatapb.PoolerType_REPLICA
				}, 500*time.Millisecond, 50*time.Millisecond, "Pooler type should be updated to REPLICA in topology")

				// Verify health streamer reports the new primary (source)
				healthState := pm.healthStreamer.getState()
				require.NotNil(t, healthState.PrimaryObservation,
					"health streamer should have primary observation pointing to new primary after DemoteStalePrimary")
				assert.Equal(t, sourcePooler.Id, healthState.PrimaryObservation.PrimaryID,
					"primary observation should point to the source (new primary)")
				assert.Equal(t, tt.requestTerm, healthState.PrimaryObservation.PrimaryTerm,
					"primary observation term should match the consensus term from the request")
			}

			assert.NoError(t, mockQueryService.ExpectationsWereMet())
		})
	}
}

func TestAvailabilityStatus(t *testing.T) {
	t.Run("buildAvailabilityStatus returns nil when no resignation is set", func(t *testing.T) {
		pm := &MultiPoolerManager{}
		assert.Nil(t, pm.buildAvailabilityStatus())
	})

	t.Run("resignedPrimaryAtTerm set makes buildAvailabilityStatus return the term", func(t *testing.T) {
		pm := &MultiPoolerManager{}
		pm.resignedPrimaryAtTerm = 7
		av := pm.buildAvailabilityStatus()
		require.NotNil(t, av)
		require.NotNil(t, av.LeadershipStatus)
		assert.Equal(t, int64(7), av.LeadershipStatus.PrimaryTerm)
		assert.Equal(t, clustermetadatapb.LeadershipSignal_LEADERSHIP_SIGNAL_REQUESTING_DEMOTION, av.LeadershipStatus.Signal)
	})

	t.Run("resignedPrimaryAtTerm cleared makes buildAvailabilityStatus return nil", func(t *testing.T) {
		pm := &MultiPoolerManager{}
		pm.resignedPrimaryAtTerm = 3
		pm.resignedPrimaryAtTerm = 0
		assert.Nil(t, pm.buildAvailabilityStatus())
	})
}
