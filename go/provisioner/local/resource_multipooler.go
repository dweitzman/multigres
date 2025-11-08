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
	"syscall"
	"time"

	"github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/provisioner"
)

// MultipoolerResource represents a multipooler service in a cell.
// Note: Multipooler internally starts pgctld as part of its provisioning.
type MultipoolerResource struct {
	provisioner  *localProvisioner
	databaseName string
	cellName     string
}

// NewMultipoolerResource creates a new multipooler resource.
func NewMultipoolerResource(p *localProvisioner, databaseName, cellName string) (*MultipoolerResource, error) {
	return &MultipoolerResource{
		provisioner:  p,
		databaseName: databaseName,
		cellName:     cellName,
	}, nil
}

// ID returns the resource ID for multipooler.
func (m *MultipoolerResource) ID() *clustermetadata.ID {
	return &clustermetadata.ID{
		Component: clustermetadata.ID_MULTIPOOLER,
		Cell:      m.cellName,
		Name:      fmt.Sprintf("multipooler-%s-%s", m.databaseName, m.cellName),
	}
}

// DisplayName returns the display name for multipooler.
func (m *MultipoolerResource) DisplayName() string {
	return "Multipooler"
}

// Discover checks if multipooler is already running.
func (m *MultipoolerResource) Discover(ctx context.Context, provCtx *ProvisionContext) (ResourceStatus, error) {
	existingService, err := m.provisioner.findRunningDbService("multipooler", m.databaseName, m.cellName)
	if err != nil {
		return ResourceStatus{}, fmt.Errorf("failed to check for existing multipooler: %w", err)
	}

	if existingService != nil {
		return ResourceStatus{
			State: StateDiscovered,
			Metadata: map[string]any{
				"pid":   existingService.PID,
				"ports": existingService.Ports,
				"fqdn":  existingService.FQDN,
			},
			Message: fmt.Sprintf("multipooler already running (PID %d)", existingService.PID),
		}, nil
	}

	return ResourceStatus{
		State:   StateNotFound,
		Message: "multipooler not running",
	}, nil
}

// Provision starts the multipooler service (which also starts pgctld).
func (m *MultipoolerResource) Provision(ctx context.Context, provCtx *ProvisionContext) (ResourceStatus, error) {
	// Use provision context for topology connection info
	req := &provisioner.ProvisionRequest{
		Service:      "multipooler",
		DatabaseName: m.databaseName,
		Params: map[string]any{
			"etcd_address":     provCtx.EtcdAddress,
			"topo_backend":     provCtx.TopoBackend,
			"topo_global_root": provCtx.TopoGlobalRoot,
			"cell":             m.cellName,
		},
	}

	result, err := m.provisioner.provisionMultipooler(ctx, req)
	if err != nil {
		return ResourceStatus{}, err
	}

	return ResourceStatus{
		State: StateProvisioned,
		Metadata: map[string]any{
			"ports": result.Ports,
			"fqdn":  result.FQDN,
		},
		Message: "multipooler started successfully",
	}, nil
}

// Deprovision stops the multipooler service.
func (m *MultipoolerResource) Deprovision(ctx context.Context, status ResourceStatus) error {
	pid, ok := status.Metadata["pid"].(int)
	if !ok {
		return fmt.Errorf("PID not found in status metadata")
	}

	// Try to terminate gracefully first
	if err := syscall.Kill(pid, syscall.SIGTERM); err != nil {
		// Process might already be dead
		if err != syscall.ESRCH {
			return fmt.Errorf("failed to send SIGTERM to multipooler (PID %d): %w", pid, err)
		}
	}

	// Wait briefly for graceful shutdown
	time.Sleep(2 * time.Second)

	// Check if process is still running, force kill if necessary
	if err := m.provisioner.validateProcessRunning(pid); err == nil {
		// Process still running, force kill
		if err := syscall.Kill(pid, syscall.SIGKILL); err != nil && err != syscall.ESRCH {
			return fmt.Errorf("failed to force kill multipooler (PID %d): %w", pid, err)
		}
	}

	return nil
}

// Dependencies returns no explicit dependencies (etcd is implicit).
func (m *MultipoolerResource) Dependencies() []*clustermetadata.ID {
	return nil
}

// Children returns no children.
func (m *MultipoolerResource) Children() []Resource {
	return nil
}
