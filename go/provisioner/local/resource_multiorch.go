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
	"os/exec"
	"time"

	"github.com/multigres/multigres/go/clustermetadata/topo"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/provisioner"
	"github.com/multigres/multigres/go/provisioner/local/ports"
)

// MultiorchResource represents the multiorch service
type MultiorchResource struct {
	id           *clustermetadatapb.ID
	databaseID   *clustermetadatapb.ID
	cellName     string
	databaseName string
	config       *MultiorchConfig
}

// NewMultiorchResource creates a new multiorch resource
func NewMultiorchResource(cellName, databaseName, serviceID string, config *MultiorchConfig) *MultiorchResource {
	return &MultiorchResource{
		id: &clustermetadatapb.ID{
			Component: clustermetadatapb.ID_MULTIORCH,
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
	}
}

// ID returns the resource ID
func (r *MultiorchResource) ID() *clustermetadatapb.ID {
	return r.id
}

// Dependencies returns the resources this resource depends on
func (r *MultiorchResource) Dependencies() []*clustermetadatapb.ID {
	// Multiorch depends on:
	// - The database being registered in topology
	// - Cell topology (implicit, handled by engine)
	return []*clustermetadatapb.ID{r.databaseID}
}

// Provision provisions the multiorch service
func (r *MultiorchResource) Provision(ctx context.Context, pctx ProvisionContext) (*provisioner.ProvisionResult, error) {
	// Check if already running
	state, err := pctx.ReadState(r.id)
	if err == nil && state != nil && state.PID > 0 {
		// Already running
		return &provisioner.ProvisionResult{
			ServiceName: "multiorch",
			FQDN:        state.FQDN,
			Ports:       state.Ports,
			Metadata: map[string]any{
				"service_id": state.ID,
				"log_file":   state.LogFile,
				"cell":       r.cellName,
				"database":   r.databaseName,
			},
		}, nil
	}

	// Get configuration
	config := pctx.GetConfig()
	httpPort := r.config.HttpPort
	if httpPort == 0 {
		httpPort = ports.DefaultMultiorchHTTP
	}

	grpcPort := r.config.GrpcPort
	if grpcPort == 0 {
		grpcPort = ports.DefaultMultiorchGRPC
	}

	logLevel := r.config.LogLevel
	if logLevel == "" {
		logLevel = "info"
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

	// Find multiorch binary
	multiorchBinary, err := pctx.FindBinary("multiorch", r.config.Path)
	if err != nil {
		return nil, fmt.Errorf("multiorch binary not found: %w", err)
	}

	// Create log file
	logFile, err := pctx.LogPath("multiorch", r.id.Name, r.databaseName)
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
		"--log-level", logLevel,
		"--log-output", logFile,
		"--hostname", "localhost",
	}

	// Start multiorch process
	multiorchCmd := exec.CommandContext(ctx, multiorchBinary, args...)
	if err := multiorchCmd.Start(); err != nil {
		return nil, fmt.Errorf("failed to start multiorch: %w", err)
	}

	// Validate process is running
	if err := pctx.ValidateProcessRunning(multiorchCmd.Process.Pid); err != nil {
		return nil, fmt.Errorf("multiorch process validation failed: %w", err)
	}

	// Create state
	state = &LocalProvisionedService{
		ID:         r.id.Name,
		Service:    "multiorch",
		Database:   r.databaseName,
		PID:        multiorchCmd.Process.Pid,
		BinaryPath: multiorchBinary,
		Ports:      map[string]int{"http_port": httpPort, "grpc_port": grpcPort},
		FQDN:       "localhost",
		LogFile:    logFile,
		StartedAt:  time.Now(),
		Metadata: map[string]any{
			"cell":     r.cellName,
			"database": r.databaseName,
		},
	}

	// Save state
	if err := pctx.SaveState(state); err != nil {
		return nil, fmt.Errorf("failed to save state: %w", err)
	}

	// Wait for multiorch to be ready
	readyCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := pctx.WaitForServiceReady(readyCtx, "multiorch", "localhost", state.Ports); err != nil {
		return nil, fmt.Errorf("multiorch readiness check failed: %w", err)
	}

	return &provisioner.ProvisionResult{
		ServiceName: "multiorch",
		FQDN:        "localhost",
		Ports: map[string]int{
			"http_port": httpPort,
			"grpc_port": grpcPort,
		},
		Metadata: map[string]any{
			"service_id": r.id.Name,
			"log_file":   logFile,
			"cell":       r.cellName,
			"database":   r.databaseName,
		},
	}, nil
}

// Deprovision stops the multiorch service
func (r *MultiorchResource) Deprovision(ctx context.Context, pctx ProvisionContext) error {
	// Read state
	state, err := pctx.ReadState(r.id)
	if err != nil {
		// No state means nothing to deprovision
		return nil
	}

	// Stop the process
	if state.PID > 0 {
		if err := pctx.StopProcess(state.PID); err != nil {
			return fmt.Errorf("failed to stop multiorch process: %w", err)
		}
	}

	return nil
}
