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
	"time"

	"github.com/multigres/multigres/go/clustermetadata/topo"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/provisioner"
)

// GlobalTopoResource represents the global topology server (etcd)
type GlobalTopoResource struct {
	id     *clustermetadatapb.ID
	config *EtcdConfig
}

// NewGlobalTopoResource creates a new global topology resource
func NewGlobalTopoResource(serviceID string, config *EtcdConfig) *GlobalTopoResource {
	return &GlobalTopoResource{
		id: &clustermetadatapb.ID{
			Component: clustermetadatapb.ID_GLOBAL_TOPO,
			Cell:      topo.GlobalCell,
			Name:      serviceID,
		},
		config: config,
	}
}

// ID returns the resource ID
func (r *GlobalTopoResource) ID() *clustermetadatapb.ID {
	return r.id
}

// Dependencies returns the resources this resource depends on (none for global topo)
func (r *GlobalTopoResource) Dependencies() []*clustermetadatapb.ID {
	return []*clustermetadatapb.ID{}
}

// Provision provisions the global topology server (etcd)
func (r *GlobalTopoResource) Provision(ctx context.Context, pctx ProvisionContext) (*provisioner.ProvisionResult, error) {
	// Check if already running
	state, err := pctx.ReadState(r.id)
	if err == nil && state != nil && state.PID > 0 {
		// Already running
		return &provisioner.ProvisionResult{
			ServiceName: "etcd",
			FQDN:        state.FQDN,
			Ports:       state.Ports,
			Metadata: map[string]any{
				"service_id": state.ID,
				"log_file":   state.LogFile,
			},
		}, nil
	}

	// Get port from config
	port := r.config.Port
	if port == 0 {
		port = 2379 // default
	}

	// Find etcd binary
	etcdBinary, err := pctx.FindBinary("etcd", "")
	if err != nil {
		return nil, fmt.Errorf("etcd binary not found: %w", err)
	}

	// Create data directory
	dataDir := r.config.DataDir
	if dataDir == "" {
		dataDir = pctx.DataPath("etcd", r.id.Name)
	}

	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return nil, fmt.Errorf("failed to create etcd data directory %s: %w", dataDir, err)
	}

	// Create log file
	logFile, err := pctx.LogPath("etcd", r.id.Name, "")
	if err != nil {
		return nil, fmt.Errorf("failed to create log file: %w", err)
	}

	peerPort := port + 1

	// Build etcd command
	args := []string{
		"--name", "default",
		"--data-dir", dataDir,
		"--listen-client-urls", fmt.Sprintf("http://0.0.0.0:%d", port),
		"--advertise-client-urls", fmt.Sprintf("http://localhost:%d", port),
		"--listen-peer-urls", fmt.Sprintf("http://0.0.0.0:%d", peerPort),
		"--initial-advertise-peer-urls", fmt.Sprintf("http://localhost:%d", peerPort),
		"--initial-cluster", fmt.Sprintf("default=http://localhost:%d", peerPort),
		"--initial-cluster-state", "new",
		"--log-outputs", logFile,
	}

	// Start etcd process
	etcdCmd := exec.CommandContext(ctx, etcdBinary, args...)
	if err := etcdCmd.Start(); err != nil {
		return nil, fmt.Errorf("failed to start etcd: %w", err)
	}

	// Validate process is running
	if err := pctx.ValidateProcessRunning(etcdCmd.Process.Pid); err != nil {
		return nil, fmt.Errorf("etcd process validation failed: %w", err)
	}

	// Create state
	state = &LocalProvisionedService{
		ID:         r.id.Name,
		Service:    "etcd",
		PID:        etcdCmd.Process.Pid,
		BinaryPath: etcdBinary,
		DataDir:    dataDir,
		Ports:      map[string]int{"tcp": port, "peer": peerPort, "etcd_port": port},
		FQDN:       "localhost",
		LogFile:    logFile,
		StartedAt:  time.Now(),
		Metadata:   map[string]any{},
	}

	// Save state
	if err := pctx.SaveState(state); err != nil {
		return nil, fmt.Errorf("failed to save state: %w", err)
	}

	// Wait for etcd to be ready using health check
	if err := pctx.WaitForServiceReady(ctx, "etcd", "localhost", state.Ports); err != nil {
		return nil, fmt.Errorf("etcd readiness check failed: %w", err)
	}

	return &provisioner.ProvisionResult{
		ServiceName: "etcd",
		FQDN:        "localhost",
		Ports: map[string]int{
			"tcp":  port,
			"peer": peerPort,
		},
		Metadata: map[string]any{
			"runtime":     "binary",
			"pid":         etcdCmd.Process.Pid,
			"binary-path": etcdBinary,
			"data-dir":    dataDir,
			"service-id":  r.id.Name,
			"log-file":    logFile,
		},
	}, nil
}

// Deprovision stops the global topology server (etcd)
func (r *GlobalTopoResource) Deprovision(ctx context.Context, pctx ProvisionContext) error {
	// Read state
	state, err := pctx.ReadState(r.id)
	if err != nil {
		// No state means nothing to deprovision
		return nil
	}

	// Stop the process
	if state.PID > 0 {
		if err := pctx.StopProcess(state.PID); err != nil {
			return fmt.Errorf("failed to stop etcd process: %w", err)
		}
	}

	return nil
}
