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
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/backup"
	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/servenv"
	"github.com/multigres/multigres/go/common/topoclient"
	"github.com/multigres/multigres/go/common/topoclient/memorytopo"
	"github.com/multigres/multigres/go/services/multipooler/executor/mock"
	"github.com/multigres/multigres/go/test/utils"
	"github.com/multigres/multigres/go/tools/viperutil"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
)

func TestManagerState_InitialState(t *testing.T) {
	ctx := t.Context()
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	ts, _ := memorytopo.NewServerAndFactory(ctx, "zone1")
	defer ts.Close()

	serviceID := &clustermetadatapb.ID{
		Cell: "zone1",
		Name: "test-service",
	}

	multiPooler := topoclient.NewMultiPooler(serviceID.Name, serviceID.Cell, "localhost", constants.DefaultTableGroup)
	multiPooler.Shard = constants.DefaultShard
	multiPooler.PoolerDir = "/tmp/test"

	config := &Config{
		TopoClient: ts,
	}

	manager, err := NewMultiPoolerManager(logger, multiPooler, config)
	require.NoError(t, err)
	defer manager.Shutdown()

	// Manager should not be open until Start/Open is called
	assert.False(t, manager.IsOpen())
}

func TestManagerState_RetryUntilSuccess(t *testing.T) {
	ctx := t.Context()
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	ts, factory := memorytopo.NewServerAndFactory(ctx, "zone1")
	defer ts.Close()

	// Create temp directory for pooler-dir
	poolerDir := t.TempDir()

	// Create the database in topology with backup location
	database := "testdb"
	addDatabaseToTopo(t, ts, database)

	// Create the multipooler in topology
	serviceID := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      "zone1",
		Name:      "test-service",
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

	// Inject 2 one-time errors to simulate transient failures
	poolerPath := "/poolers/" + topoclient.MultiPoolerIDString(serviceID) + "/Pooler"
	factory.AddOneTimeOperationError(memorytopo.Get, poolerPath, assert.AnError)
	factory.AddOneTimeOperationError(memorytopo.Get, poolerPath, assert.AnError)

	multiPoolerObj := topoclient.NewMultiPooler(serviceID.Name, serviceID.Cell, "localhost", constants.DefaultTableGroup)
	multiPoolerObj.Shard = constants.DefaultShard
	multiPoolerObj.PoolerDir = poolerDir
	multiPoolerObj.Database = database

	config := &Config{
		TopoClient: ts,
	}

	manager, err := NewMultiPoolerManager(logger, multiPoolerObj, config)
	require.NoError(t, err)
	defer manager.Shutdown()

	// Start async topo loader and wait for it to succeed despite transient errors
	go manager.loadMultiPoolerFromTopo()

	require.Eventually(t, func() bool {
		manager.mu.Lock()
		defer manager.mu.Unlock()
		return manager.topoLoaded
	}, 5*time.Second, 100*time.Millisecond, "Topo should load after retries")
}

func TestManagerState_NilServiceID(t *testing.T) {
	ctx := t.Context()
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	ts, _ := memorytopo.NewServerAndFactory(ctx, "zone1")
	defer ts.Close()

	// Create MultiPooler with nil Id to test validation
	multiPooler := &clustermetadatapb.MultiPooler{
		Id:         nil, // Nil ID for testing
		TableGroup: constants.DefaultTableGroup,
		Shard:      constants.DefaultShard,
		PoolerDir:  "/tmp/test",
	}

	config := &Config{
		TopoClient: ts,
	}

	manager, err := NewMultiPoolerManager(logger, multiPooler, config)

	// Now that MultiPooler.Id is validated in constructor, we expect an error immediately
	require.Error(t, err)
	require.Nil(t, manager)
	assert.Contains(t, err.Error(), "MultiPooler.Id is required")
}

func TestValidateAndUpdateTerm(t *testing.T) {
	ctx := t.Context()
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	serviceID := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      "zone1",
		Name:      "test-service",
	}

	tests := []struct {
		name          string
		currentTerm   int64
		requestTerm   int64
		force         bool
		expectError   bool
		expectedCode  mtrpcpb.Code
		errorContains string
	}{
		{
			name:        "Equal term should accept",
			currentTerm: 5,
			requestTerm: 5,
			force:       false,
			expectError: false,
		},
		{
			name:        "Higher term should update and accept",
			currentTerm: 5,
			requestTerm: 10,
			force:       false,
			expectError: false,
		},
		{
			name:          "Lower term should reject",
			currentTerm:   10,
			requestTerm:   5,
			force:         false,
			expectError:   true,
			expectedCode:  mtrpcpb.Code_FAILED_PRECONDITION,
			errorContains: "consensus term too old",
		},
		{
			name:        "Force flag bypasses validation",
			currentTerm: 10,
			requestTerm: 5,
			force:       true,
			expectError: false,
		},
		{
			// Term 0 means uninitialized (consensus_term.json not yet written, e.g. on a fresh
			// standby after poolerDir was wiped by a restore). A positive request term is
			// accepted and initializes the local term.
			name:        "Zero cached term accepts higher request term (initializes standby)",
			currentTerm: 0,
			requestTerm: 5,
			force:       false,
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ts, _ := memorytopo.NewServerAndFactory(ctx, "zone1")
			defer ts.Close()

			// Create temp directory for pooler-dir
			poolerDir := t.TempDir()

			// Create a minimal data directory structure to satisfy IsDataDirInitialized check
			dataDir := filepath.Join(poolerDir, "pg_data")
			t.Setenv(constants.PgDataDirEnvVar, dataDir)
			require.NoError(t, os.MkdirAll(dataDir, 0o755))
			require.NoError(t, os.WriteFile(filepath.Join(dataDir, "PG_VERSION"), []byte("15\n"), 0o644))

			// Set initial consensus term on disk if currentTerm > 0
			if tt.currentTerm > 0 {
				initialTerm := &multipoolermanagerdatapb.ConsensusTerm{
					TermNumber: tt.currentTerm,
				}
				setupCS := NewConsensusState(poolerDir, nil)
				require.NoError(t, setupCS.setConsensusTerm(initialTerm))
			}

			// Create the database in topology with backup location
			database := "testdb"
			addDatabaseToTopo(t, ts, database)

			multipooler := &clustermetadatapb.MultiPooler{
				Id:            serviceID,
				Database:      database,
				Hostname:      "localhost",
				PortMap:       map[string]int32{"grpc": 8080},
				Type:          clustermetadatapb.PoolerType_PRIMARY,
				ServingStatus: clustermetadatapb.PoolerServingStatus_SERVING,
				TableGroup:    constants.DefaultTableGroup,
				Shard:         constants.DefaultShard,
				PoolerDir:     poolerDir,
			}
			require.NoError(t, ts.CreateMultiPooler(ctx, multipooler))

			config := &Config{
				TopoClient:       ts,
				ConsensusEnabled: true,
			}
			manager, err := NewMultiPoolerManager(logger, multipooler, config)
			require.NoError(t, err)
			defer manager.Shutdown()

			// Set up mock query service for isInRecovery check during startup
			mockQueryService := mock.NewQueryService()
			mockQueryService.AddQueryPattern("SELECT pg_is_in_recovery", mock.MakeQueryResult([]string{"pg_is_in_recovery"}, [][]any{{"f"}}))
			manager.qsc = &mockPoolerController{queryService: mockQueryService}

			// Start and wait for ready
			senv := servenv.NewServEnv(viperutil.NewRegistry())
			go manager.Start(senv)
			require.Eventually(t, manager.IsOpen,
				5*time.Second, 100*time.Millisecond, "Manager should be open")

			// Acquire action lock before calling validateAndUpdateTerm
			ctx, err := manager.actionLock.Acquire(ctx, "test")
			require.NoError(t, err)
			defer manager.actionLock.Release(ctx)

			// Call validateAndUpdateTerm
			err = manager.validateAndUpdateTerm(ctx, tt.requestTerm, tt.force)

			if tt.expectError {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.errorContains)

				if tt.expectedCode != 0 {
					code := mterrors.Code(err)
					assert.Equal(t, tt.expectedCode, code)
				}
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestGetBackupLocation(t *testing.T) {
	ctx := t.Context()
	tmpDir := t.TempDir()

	// Create test topology store
	ts, _ := memorytopo.NewServerAndFactory(ctx, "zone1")
	defer ts.Close()

	// Create test database with backup_location
	database := "testdb"
	addDatabaseToTopo(t, ts, database)

	// Create manager config
	serviceID := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      "zone1",
		Name:      "test-service",
	}
	multiPooler := &clustermetadatapb.MultiPooler{
		Id:         serviceID,
		Database:   database,
		TableGroup: constants.DefaultTableGroup,
		Shard:      constants.DefaultShard,
		PoolerDir:  filepath.Join(tmpDir, "pooler"),
	}
	config := &Config{
		TopoClient: ts,
		PgctldAddr: "",
	}

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	manager, err := NewMultiPoolerManager(logger, multiPooler, config)
	require.NoError(t, err)

	// Set backup config
	backupConfig, err := backup.NewConfig(
		utils.FilesystemBackupLocation("/var/backups/pgbackrest"),
	)
	require.NoError(t, err)
	manager.backupConfig = backupConfig

	// Test backup config
	assert.Equal(t, "filesystem", manager.backupConfig.Type())
	expectedShardBackupLocation := filepath.Join("/var/backups/pgbackrest", database, constants.DefaultTableGroup, constants.DefaultShard)
	shardPath, err := manager.backupConfig.FullPath(database, constants.DefaultTableGroup, constants.DefaultShard)
	require.NoError(t, err)
	assert.Equal(t, expectedShardBackupLocation, shardPath)
}

func TestGetBackupLocation_S3(t *testing.T) {
	ctx := t.Context()
	tmpDir := t.TempDir()

	// Create test topology store
	ts, _ := memorytopo.NewServerAndFactory(ctx, "zone1")
	defer ts.Close()

	// Create test database with S3 backup location
	database := "testdb"
	addDatabaseToTopo(t, ts, database)

	// Create manager config
	serviceID := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      "zone1",
		Name:      "test-service",
	}
	multiPooler := &clustermetadatapb.MultiPooler{
		Id:         serviceID,
		Database:   database,
		TableGroup: constants.DefaultTableGroup,
		Shard:      constants.DefaultShard,
		PoolerDir:  filepath.Join(tmpDir, "pooler"),
	}
	config := &Config{
		TopoClient: ts,
		PgctldAddr: "",
	}

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	manager, err := NewMultiPoolerManager(logger, multiPooler, config)
	require.NoError(t, err)

	// Set S3 backup config
	backupConfig, err := backup.NewConfig(
		utils.S3BackupLocation("my-backup-bucket", "us-west-2",
			utils.WithS3KeyPrefix("prod/backups/")),
	)
	require.NoError(t, err)
	manager.backupConfig = backupConfig

	// Test S3 backup config
	assert.Equal(t, "s3", manager.backupConfig.Type())

	// Verify full path includes S3 bucket, prefix, and path components
	expectedPath := "s3://my-backup-bucket/prod/backups/testdb/default/0-inf"
	shardPath, err := manager.backupConfig.FullPath(database, constants.DefaultTableGroup, constants.DefaultShard)
	require.NoError(t, err)
	assert.Equal(t, expectedPath, shardPath)

	// Verify PgBackRestConfig returns correct S3 settings
	pgbrConfig, err := manager.backupConfig.PgBackRestConfig("multigres")
	require.NoError(t, err)
	assert.Equal(t, "s3", pgbrConfig["repo1-type"])
	assert.Equal(t, "my-backup-bucket", pgbrConfig["repo1-s3-bucket"])
	assert.Equal(t, "us-west-2", pgbrConfig["repo1-s3-region"])
	assert.Equal(t, "auto", pgbrConfig["repo1-s3-key-type"])
	assert.Equal(t, "/prod/backups/multigres", pgbrConfig["repo1-path"])
}

func TestNewMultiPoolerManager_MVPValidation(t *testing.T) {
	ctx := t.Context()
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	ts, _ := memorytopo.NewServerAndFactory(ctx, "zone1")
	defer ts.Close()

	serviceID := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      "zone1",
		Name:      "test-service",
	}

	tests := []struct {
		name        string
		tableGroup  string
		shard       string
		wantErr     bool
		errContains string
	}{
		{
			name:       "valid default tablegroup and shard",
			tableGroup: constants.DefaultTableGroup,
			shard:      constants.DefaultShard,
			wantErr:    false,
		},
		{
			name:        "empty tablegroup fails",
			tableGroup:  "",
			shard:       constants.DefaultShard,
			wantErr:     true,
			errContains: "TableGroup is required",
		},
		{
			name:        "empty shard fails",
			tableGroup:  constants.DefaultTableGroup,
			shard:       "",
			wantErr:     true,
			errContains: "Shard is required",
		},
		{
			name:        "invalid tablegroup fails",
			tableGroup:  "custom",
			shard:       constants.DefaultShard,
			wantErr:     true,
			errContains: "only default tablegroup is supported",
		},
		{
			name:        "invalid shard fails",
			tableGroup:  constants.DefaultTableGroup,
			shard:       "0-100",
			wantErr:     true,
			errContains: "only shard " + constants.DefaultShard + " is supported",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			multiPooler := &clustermetadatapb.MultiPooler{
				Id:         serviceID,
				Database:   "testdb",
				Hostname:   "localhost",
				PortMap:    map[string]int32{"grpc": 8080},
				TableGroup: tt.tableGroup,
				Shard:      tt.shard,
				PoolerDir:  "/tmp/test",
			}

			config := &Config{
				TopoClient: ts,
			}

			manager, err := NewMultiPoolerManager(logger, multiPooler, config)
			if tt.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.errContains)
				assert.Nil(t, manager)
			} else {
				require.NoError(t, err)
				assert.NotNil(t, manager)
				manager.Shutdown()
			}
		})
	}
}
