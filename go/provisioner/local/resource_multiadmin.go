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

// MultiadminResource represents the multiadmin service
type MultiadminResource struct {
	id     *clustermetadatapb.ID
	config *MultiadminConfig
}

// NewMultiadminResource creates a new multiadmin resource
func NewMultiadminResource(serviceID string, config *MultiadminConfig) *MultiadminResource {
	return &MultiadminResource{
		id: &clustermetadatapb.ID{
			Component: clustermetadatapb.ID_MULTIADMIN,
			Cell:      topo.GlobalCell,
			Name:      serviceID,
		},
		config: config,
	}
}

// ID returns the resource ID
func (r *MultiadminResource) ID() *clustermetadatapb.ID {
	return r.id
}

// Dependencies returns the resources this resource depends on (global topology)
func (r *MultiadminResource) Dependencies() []*clustermetadatapb.ID {
	// Multiadmin depends on global topology (implicit, handled by engine)
	return []*clustermetadatapb.ID{}
}

// Provision provisions the multiadmin service
func (r *MultiadminResource) Provision(ctx context.Context, pctx ProvisionContext) (*provisioner.ProvisionResult, error) {
	// Check if already running
	state, err := pctx.ReadState(r.id)
	if err == nil && state != nil && state.PID > 0 {
		// Already running
		return &provisioner.ProvisionResult{
			ServiceName: "multiadmin",
			FQDN:        state.FQDN,
			Ports:       state.Ports,
			Metadata: map[string]any{
				"service_id": state.ID,
				"log_file":   state.LogFile,
			},
		}, nil
	}

	// Get configuration
	config := pctx.GetConfig()
	httpPort := r.config.HttpPort
	if httpPort == 0 {
		httpPort = ports.DefaultMultiadminHTTP
	}

	grpcPort := r.config.GrpcPort
	if grpcPort == 0 {
		grpcPort = ports.DefaultMultiadminGRPC
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

	// Find multiadmin binary
	multiadminBinary, err := pctx.FindBinary("multiadmin", r.config.Path)
	if err != nil {
		return nil, fmt.Errorf("multiadmin binary not found: %w", err)
	}

	// Create log file
	logFile, err := pctx.LogPath("multiadmin", r.id.Name, "")
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
		"--log-level", logLevel,
		"--log-output", logFile,
		"--service-map", "grpc-multiadmin",
		"--hostname", "localhost",
	}

	// Start multiadmin process
	multiadminCmd := exec.CommandContext(ctx, multiadminBinary, args...)
	if err := multiadminCmd.Start(); err != nil {
		return nil, fmt.Errorf("failed to start multiadmin: %w", err)
	}

	// Validate process is running
	if err := pctx.ValidateProcessRunning(multiadminCmd.Process.Pid); err != nil {
		return nil, fmt.Errorf("multiadmin process validation failed: %w", err)
	}

	// Create state
	state = &LocalProvisionedService{
		ID:         r.id.Name,
		Service:    "multiadmin",
		PID:        multiadminCmd.Process.Pid,
		BinaryPath: multiadminBinary,
		Ports:      map[string]int{"http_port": httpPort, "grpc_port": grpcPort},
		FQDN:       "localhost",
		LogFile:    logFile,
		StartedAt:  time.Now(),
		Metadata:   map[string]any{},
	}

	// Save state
	if err := pctx.SaveState(state); err != nil {
		return nil, fmt.Errorf("failed to save state: %w", err)
	}

	// Wait for multiadmin to be ready
	readyCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := pctx.WaitForServiceReady(readyCtx, "multiadmin", "localhost", state.Ports); err != nil {
		return nil, fmt.Errorf("multiadmin readiness check failed: %w", err)
	}

	return &provisioner.ProvisionResult{
		ServiceName: "multiadmin",
		FQDN:        "localhost",
		Ports: map[string]int{
			"http_port": httpPort,
			"grpc_port": grpcPort,
		},
		Metadata: map[string]any{
			"service_id": r.id.Name,
			"log_file":   logFile,
		},
	}, nil
}

// Deprovision stops the multiadmin service
func (r *MultiadminResource) Deprovision(ctx context.Context, pctx ProvisionContext) error {
	// Read state
	state, err := pctx.ReadState(r.id)
	if err != nil {
		// No state means nothing to deprovision
		return nil
	}

	// Stop the process
	if state.PID > 0 {
		if err := pctx.StopProcess(state.PID); err != nil {
			return fmt.Errorf("failed to stop multiadmin process: %w", err)
		}
	}

	return nil
}
