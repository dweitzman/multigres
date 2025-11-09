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

package local

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	"google.golang.org/grpc"

	"github.com/multigres/multigres/go/clustermetadata/topo"
	"github.com/multigres/multigres/go/grpccommon"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	pb "github.com/multigres/multigres/go/pb/pgctldservice"
	"github.com/multigres/multigres/go/provisioner"
	"github.com/multigres/multigres/go/provisioner/local/ports"
)

// MultipoolerResource represents the multipooler service (including pgctld)
type MultipoolerResource struct {
	id           *clustermetadatapb.ID
	databaseID   *clustermetadatapb.ID
	cellName     string
	databaseName string
	config       *MultipoolerConfig
	pgctldConfig *PgctldConfig
}

// NewMultipoolerResource creates a new multipooler resource
func NewMultipoolerResource(cellName, databaseName, serviceID string, config *MultipoolerConfig, pgctldConfig *PgctldConfig) *MultipoolerResource {
	return &MultipoolerResource{
		id: &clustermetadatapb.ID{
			Component: clustermetadatapb.ID_MULTIPOOLER,
			Cell:      cellName,
			Name:      serviceID,
		},
		databaseID: &clustermetadatapb.ID{
			Component: clustermetadatapb.ID_DATABASE,
			Cell:      topo.GlobalCell,
			Name:      databaseName,
		},
		cellName:     cellName,
		databaseName: databaseName,
		config:       config,
		pgctldConfig: pgctldConfig,
	}
}

// ID returns the resource ID
func (r *MultipoolerResource) ID() *clustermetadatapb.ID {
	return r.id
}

// Dependencies returns the resources this resource depends on
func (r *MultipoolerResource) Dependencies() []*clustermetadatapb.ID {
	// Multipooler depends on:
	// - The database being registered in topology
	// - Cell topology (implicit, handled by engine)
	return []*clustermetadatapb.ID{r.databaseID}
}

// Provision provisions both pgctld and multipooler
func (r *MultipoolerResource) Provision(ctx context.Context, pctx ProvisionContext) (*provisioner.ProvisionResult, error) {
	// Check if multipooler is already running
	multipoolerState, err := pctx.ReadState(r.id)
	if err == nil && multipoolerState != nil && multipoolerState.PID > 0 {
		// Already running
		return &provisioner.ProvisionResult{
			ServiceName: "multipooler",
			FQDN:        multipoolerState.FQDN,
			Ports:       multipoolerState.Ports,
			Metadata: map[string]any{
				"service_id": multipoolerState.ID,
				"log_file":   multipoolerState.LogFile,
				"cell":       r.cellName,
				"database":   r.databaseName,
			},
		}, nil
	}

	// Step 1: Provision pgctld first
	pgctldResult, err := r.provisionPgctld(ctx, pctx)
	if err != nil {
		return nil, fmt.Errorf("failed to provision pgctld: %w", err)
	}

	// Step 2: Provision multipooler
	return r.provisionMultipoolerService(ctx, pctx, pgctldResult.Address)
}

// provisionPgctld provisions pgctld and PostgreSQL
func (r *MultipoolerResource) provisionPgctld(ctx context.Context, pctx ProvisionContext) (*pgctldProvisionResult, error) {
	serviceID := r.id.Name
	pgctldServiceID := fmt.Sprintf("pgctld-%s", serviceID)

	// Create pgctld resource ID
	pgctldID := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER, // Pgctld is part of multipooler
		Cell:      r.cellName,
		Name:      pgctldServiceID,
	}

	// Check if pgctld is already running
	pgctldState, err := pctx.ReadState(pgctldID)
	if err == nil && pgctldState != nil && pgctldState.PID > 0 {
		// Verify health
		grpcAddress := fmt.Sprintf("localhost:%d", pgctldState.Ports["grpc_port"])
		if err := checkPgctldHealth(grpcAddress); err != nil {
			return nil, fmt.Errorf("pgctld health check failed: %w", err)
		}

		return &pgctldProvisionResult{
			Address: grpcAddress,
			Port:    pgctldState.Ports["grpc_port"],
			LogFile: pgctldState.LogFile,
		}, nil
	}

	// Get configuration
	grpcPort := r.pgctldConfig.GrpcPort
	if grpcPort == 0 {
		grpcPort = ports.DefaultPgctldGRPC
	}

	pgPort := r.pgctldConfig.PgPort
	if pgPort == 0 {
		pgPort = ports.DefaultPostgresPort
	}

	poolerDir := r.config.PoolerDir
	if poolerDir == "" {
		poolerDir = pctx.DataPath("multipooler", serviceID)
	}

	pgDatabase := r.pgctldConfig.PgDatabase
	if pgDatabase == "" {
		pgDatabase = "postgres"
	}

	pgUser := r.pgctldConfig.PgUser
	if pgUser == "" {
		pgUser = "postgres"
	}

	timeout := r.pgctldConfig.Timeout
	if timeout == 0 {
		timeout = 30
	}

	logLevel := r.pgctldConfig.LogLevel
	if logLevel == "" {
		logLevel = "info"
	}

	// Find pgctld binary
	pgctldBinary, err := pctx.FindBinary("pgctld", r.pgctldConfig.Path)
	if err != nil {
		return nil, fmt.Errorf("pgctld binary not found: %w", err)
	}

	// Create log file
	pgctldLogFile, err := pctx.LogPath("pgctld", pgctldServiceID, r.databaseName)
	if err != nil {
		return nil, fmt.Errorf("failed to create pgctld log file: %w", err)
	}

	// Create pooler directory
	if err := os.MkdirAll(poolerDir, 0o755); err != nil {
		return nil, fmt.Errorf("failed to create pooler directory %s: %w", poolerDir, err)
	}

	// Create password file if configured
	if r.pgctldConfig.PgPwfile != "" {
		if err := os.WriteFile(r.pgctldConfig.PgPwfile, []byte("postgres"), 0o600); err != nil {
			return nil, fmt.Errorf("failed to create password file %s: %w", r.pgctldConfig.PgPwfile, err)
		}
	}

	// Initialize pgctld data directory
	initArgs := []string{
		"init",
		"--pooler-dir", poolerDir,
		"--pg-port", fmt.Sprintf("%d", pgPort),
		"--pg-database", pgDatabase,
		"--pg-user", pgUser,
		"--timeout", fmt.Sprintf("%d", timeout),
		"--log-level", logLevel,
	}

	if r.pgctldConfig.PgPwfile != "" {
		initArgs = append(initArgs, "--pg-pwfile", r.pgctldConfig.PgPwfile)
	}

	initCmd := exec.CommandContext(ctx, pgctldBinary, initArgs...)
	if err := initCmd.Run(); err != nil {
		return nil, fmt.Errorf("failed to initialize pgctld data directory: %w", err)
	}

	// Start pgctld server
	serverArgs := []string{
		"server",
		"--pooler-dir", poolerDir,
		"--grpc-port", fmt.Sprintf("%d", grpcPort),
		"--pg-port", fmt.Sprintf("%d", pgPort),
		"--pg-database", pgDatabase,
		"--pg-user", pgUser,
		"--timeout", fmt.Sprintf("%d", timeout),
		"--log-level", logLevel,
		"--log-output", pgctldLogFile,
	}

	if r.pgctldConfig.GRPCSocketFile != "" {
		serverArgs = append(serverArgs, "--grpc-socket-file", r.pgctldConfig.GRPCSocketFile)
		// Ensure socket directory exists
		socketDir := filepath.Dir(r.pgctldConfig.GRPCSocketFile)
		if err := os.MkdirAll(socketDir, 0o755); err != nil {
			return nil, fmt.Errorf("failed to create socket directory %s: %w", socketDir, err)
		}
	}

	pgctldCmd := exec.CommandContext(ctx, pgctldBinary, serverArgs...)
	if err := pgctldCmd.Start(); err != nil {
		return nil, fmt.Errorf("failed to start pgctld server: %w", err)
	}

	// Validate process is running
	if err := pctx.ValidateProcessRunning(pgctldCmd.Process.Pid); err != nil {
		return nil, fmt.Errorf("pgctld process validation failed: %w", err)
	}

	// Save pgctld state
	pgctldState = &LocalProvisionedService{
		ID:         pgctldServiceID,
		Service:    "pgctld",
		PID:        pgctldCmd.Process.Pid,
		BinaryPath: pgctldBinary,
		Ports:      map[string]int{"grpc_port": grpcPort},
		FQDN:       "localhost",
		LogFile:    pgctldLogFile,
		StartedAt:  time.Now(),
		DataDir:    poolerDir,
		Metadata: map[string]any{
			"cell":                   r.cellName,
			"database":               r.databaseName,
			"table_group":            r.config.TableGroup,
			"service_id":             serviceID,
			"multipooler_service_id": serviceID,
		},
	}

	if err := pctx.SaveState(pgctldState); err != nil {
		return nil, fmt.Errorf("failed to save pgctld state: %w", err)
	}

	// Wait for pgctld to be ready
	readyCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	if err := pctx.WaitForServiceReady(readyCtx, "pgctld", "localhost", pgctldState.Ports); err != nil {
		return nil, fmt.Errorf("pgctld readiness check failed: %w", err)
	}

	// Start PostgreSQL via pgctld
	grpcAddress := fmt.Sprintf("localhost:%d", grpcPort)
	if err := startPostgreSQLViaPgctld(grpcAddress); err != nil {
		return nil, fmt.Errorf("failed to start PostgreSQL: %w", err)
	}

	return &pgctldProvisionResult{
		Address: grpcAddress,
		Port:    grpcPort,
		LogFile: pgctldLogFile,
	}, nil
}

// provisionMultipoolerService provisions the multipooler service
func (r *MultipoolerResource) provisionMultipoolerService(ctx context.Context, pctx ProvisionContext, pgctldAddress string) (*provisioner.ProvisionResult, error) {
	serviceID := r.id.Name

	// Get configuration
	config := pctx.GetConfig()
	httpPort := r.config.HttpPort
	if httpPort == 0 {
		httpPort = ports.DefaultMultipoolerHTTP
	}

	grpcPort := r.config.GrpcPort
	if grpcPort == 0 {
		grpcPort = ports.DefaultMultipoolerGRPC
	}

	pgPort := r.config.PgPort
	if pgPort == 0 {
		pgPort = ports.DefaultPostgresPort
	}

	poolerDir := r.config.PoolerDir
	if poolerDir == "" {
		poolerDir = pctx.DataPath("multipooler", serviceID)
	}

	logLevel := r.config.LogLevel
	if logLevel == "" {
		logLevel = "info"
	}

	database := r.databaseName
	tableGroup := r.config.TableGroup
	if tableGroup == "" {
		tableGroup = "default"
	}

	// Get topology configuration
	etcdAddress := pctx.EtcdClientAddress()
	topoBackend := config.Topology.Backend
	if topoBackend == "" {
		topoBackend = "etcd2"
	}
	topoGlobalRoot := config.Topology.GlobalRootPath
	if topoGlobalRoot == "" {
		topoGlobalRoot = "/multigres/global"
	}

	// Find multipooler binary
	multipoolerBinary, err := pctx.FindBinary("multipooler", r.config.Path)
	if err != nil {
		return nil, fmt.Errorf("multipooler binary not found: %w", err)
	}

	// Create log file
	logFile, err := pctx.LogPath("multipooler", serviceID, database)
	if err != nil {
		return nil, fmt.Errorf("failed to create log file: %w", err)
	}

	// Build command arguments
	args := []string{
		"--http-port", fmt.Sprintf("%d", httpPort),
		"--grpc-port", fmt.Sprintf("%d", grpcPort),
		"--topo-global-server-addresses", etcdAddress,
		"--topo-global-root", topoGlobalRoot,
		"--topo-implementation", topoBackend,
		"--cell", r.cellName,
		"--database", database,
		"--table-group", tableGroup,
		"--service-id", serviceID,
		"--pgctld-addr", pgctldAddress,
		"--log-level", logLevel,
		"--log-output", logFile,
		"--pooler-dir", poolerDir,
		"--pg-port", fmt.Sprintf("%d", pgPort),
		"--hostname", "localhost",
	}

	if r.config.GRPCSocketFile != "" {
		args = append(args, "--grpc-socket-file", r.config.GRPCSocketFile)
		// Ensure socket directory exists
		socketDir := filepath.Dir(r.config.GRPCSocketFile)
		if err := os.MkdirAll(socketDir, 0o755); err != nil {
			return nil, fmt.Errorf("failed to create socket directory %s: %w", socketDir, err)
		}
	}

	args = append(args, "--service-map", "grpc-pooler")

	// Start multipooler process
	multipoolerCmd := exec.CommandContext(ctx, multipoolerBinary, args...)
	if err := multipoolerCmd.Start(); err != nil {
		return nil, fmt.Errorf("failed to start multipooler: %w", err)
	}

	// Validate process is running
	if err := pctx.ValidateProcessRunning(multipoolerCmd.Process.Pid); err != nil {
		return nil, fmt.Errorf("multipooler process validation failed: %w", err)
	}

	// Create state
	state := &LocalProvisionedService{
		ID:         serviceID,
		Service:    "multipooler",
		PID:        multipoolerCmd.Process.Pid,
		BinaryPath: multipoolerBinary,
		Ports:      map[string]int{"http_port": httpPort, "grpc_port": grpcPort},
		FQDN:       "localhost",
		LogFile:    logFile,
		StartedAt:  time.Now(),
		Metadata: map[string]any{
			"cell":        r.cellName,
			"database":    database,
			"table_group": tableGroup,
		},
	}

	// Save state
	if err := pctx.SaveState(state); err != nil {
		return nil, fmt.Errorf("failed to save state: %w", err)
	}

	// Wait for multipooler to be ready
	readyCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := pctx.WaitForServiceReady(readyCtx, "multipooler", "localhost", state.Ports); err != nil {
		return nil, fmt.Errorf("multipooler readiness check failed: %w", err)
	}

	return &provisioner.ProvisionResult{
		ServiceName: "multipooler",
		FQDN:        "localhost",
		Ports: map[string]int{
			"http_port": httpPort,
			"grpc_port": grpcPort,
		},
		Metadata: map[string]any{
			"service_id":  serviceID,
			"log_file":    logFile,
			"cell":        r.cellName,
			"database":    database,
			"table_group": tableGroup,
		},
	}, nil
}

// Deprovision stops both multipooler and pgctld
func (r *MultipoolerResource) Deprovision(ctx context.Context, pctx ProvisionContext) error {
	serviceID := r.id.Name

	// Stop multipooler first
	multipoolerState, err := pctx.ReadState(r.id)
	if err == nil && multipoolerState != nil && multipoolerState.PID > 0 {
		if err := pctx.StopProcess(multipoolerState.PID); err != nil {
			return fmt.Errorf("failed to stop multipooler process: %w", err)
		}
	}

	// Then stop pgctld (which will stop PostgreSQL first)
	pgctldServiceID := fmt.Sprintf("pgctld-%s", serviceID)
	pgctldID := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      r.cellName,
		Name:      pgctldServiceID,
	}

	pgctldState, err := pctx.ReadState(pgctldID)
	if err == nil && pgctldState != nil && pgctldState.PID > 0 {
		// Stop PostgreSQL via gRPC first
		grpcAddress := fmt.Sprintf("localhost:%d", pgctldState.Ports["grpc_port"])
		if err := stopPostgreSQLViaPgctld(grpcAddress); err != nil {
			// Log warning but continue
			fmt.Printf("Warning: failed to stop PostgreSQL gracefully: %v\n", err)
		}

		// Stop pgctld process
		if err := pctx.StopProcess(pgctldState.PID); err != nil {
			return fmt.Errorf("failed to stop pgctld process: %w", err)
		}
	}

	return nil
}

// pgctldProvisionResult contains the result of provisioning pgctld
type pgctldProvisionResult struct {
	Address string
	Port    int
	LogFile string
}

// Helper functions for pgctld gRPC operations

// startPostgreSQLViaPgctld starts PostgreSQL via pgctld gRPC
func startPostgreSQLViaPgctld(address string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conn, err := grpc.NewClient(address, grpccommon.LocalClientDialOptions()...)
	if err != nil {
		return fmt.Errorf("failed to connect to pgctld gRPC server: %w", err)
	}
	defer conn.Close()

	client := pb.NewPgCtldClient(conn)

	// Check if PostgreSQL is already running
	statusResp, err := client.Status(ctx, &pb.StatusRequest{})
	if err != nil {
		return fmt.Errorf("failed to get pgctld status: %w", err)
	}

	// If already running, we're good
	if statusResp.GetStatus() == pb.ServerStatus_RUNNING {
		return nil
	}

	// If not initialized, initialize first
	if statusResp.GetStatus() == pb.ServerStatus_NOT_INITIALIZED {
		_, err = client.InitDataDir(ctx, &pb.InitDataDirRequest{})
		if err != nil {
			return fmt.Errorf("failed to initialize PostgreSQL data directory: %w", err)
		}
	}

	// Start PostgreSQL
	_, err = client.Start(ctx, &pb.StartRequest{})
	if err != nil {
		return fmt.Errorf("failed to start PostgreSQL: %w", err)
	}

	// Verify it's running
	statusResp, err = client.Status(ctx, &pb.StatusRequest{})
	if err != nil {
		return fmt.Errorf("failed to verify PostgreSQL status: %w", err)
	}

	if statusResp.GetStatus() != pb.ServerStatus_RUNNING {
		return fmt.Errorf("PostgreSQL failed to start, status: %v", statusResp.GetStatus())
	}

	return nil
}

// stopPostgreSQLViaPgctld stops PostgreSQL via pgctld gRPC
func stopPostgreSQLViaPgctld(address string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conn, err := grpc.NewClient(address, grpccommon.LocalClientDialOptions()...)
	if err != nil {
		return fmt.Errorf("failed to connect to pgctld gRPC server: %w", err)
	}
	defer conn.Close()

	client := pb.NewPgCtldClient(conn)

	// Check if PostgreSQL is running
	statusResp, err := client.Status(ctx, &pb.StatusRequest{})
	if err != nil {
		return fmt.Errorf("failed to get pgctld status: %w", err)
	}

	// If not running, nothing to stop
	if statusResp.GetStatus() != pb.ServerStatus_RUNNING {
		return nil
	}

	// Stop PostgreSQL with fast mode
	_, err = client.Stop(ctx, &pb.StopRequest{Mode: "fast"})
	if err != nil {
		return fmt.Errorf("failed to stop PostgreSQL: %w", err)
	}

	return nil
}

// checkPgctldHealth checks if pgctld is healthy
func checkPgctldHealth(address string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, err := grpc.NewClient(address, grpccommon.LocalClientDialOptions()...)
	if err != nil {
		return fmt.Errorf("failed to connect to pgctld gRPC server: %w", err)
	}
	defer conn.Close()

	client := pb.NewPgCtldClient(conn)

	// Check status
	statusResp, err := client.Status(ctx, &pb.StatusRequest{})
	if err != nil {
		return fmt.Errorf("failed to get pgctld status: %w", err)
	}

	if statusResp.GetStatus() != pb.ServerStatus_RUNNING {
		return fmt.Errorf("PostgreSQL is not running, status: %v", statusResp.GetStatus())
	}

	return nil
}
