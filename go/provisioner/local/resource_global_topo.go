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
	"io"
	"os"
	"os/exec"
	"strings"
	"syscall"
	"time"

	"github.com/multigres/multigres/go/clustermetadata/topo"
	"github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/provisioner/local/ports"
	"github.com/multigres/multigres/go/tools/stringutil"
)

// GlobalTopoResource represents the global topology service (etcd).
type GlobalTopoResource struct {
	provisioner *localProvisioner
}

// NewGlobalTopoResource creates a new global topology resource.
func NewGlobalTopoResource(p *localProvisioner) (*GlobalTopoResource, error) {
	return &GlobalTopoResource{
		provisioner: p,
	}, nil
}

// ID returns the resource ID for global topology.
func (g *GlobalTopoResource) ID() *clustermetadata.ID {
	return &clustermetadata.ID{
		Component: clustermetadata.ID_GLOBAL_TOPO,
		Cell:      topo.GlobalCell,
		Name:      "etcd",
	}
}

// DisplayName returns the display name for global topology.
func (g *GlobalTopoResource) DisplayName() string {
	return "etcd (global topology)"
}

// Discover checks if etcd is already running.
func (g *GlobalTopoResource) Discover(ctx context.Context, provCtx *ProvisionContext) (ResourceStatus, error) {
	existingService, err := g.provisioner.findRunningEtcdService()
	if err != nil {
		return ResourceStatus{}, fmt.Errorf("failed to check for existing etcd service: %w", err)
	}

	if existingService != nil {
		// etcd is already running
		return ResourceStatus{
			State: StateDiscovered,
			Metadata: map[string]any{
				"pid":        existingService.PID,
				"address":    fmt.Sprintf("%s:%d", existingService.FQDN, existingService.Ports["tcp"]),
				"ports":      existingService.Ports,
				"service_id": existingService.ID,
				"log_file":   existingService.LogFile,
			},
			Message: fmt.Sprintf("etcd already running (PID %d)", existingService.PID),
		}, nil
	}

	return ResourceStatus{
		State:   StateNotFound,
		Message: "etcd not running",
	}, nil
}

// Provision starts the etcd service.
func (g *GlobalTopoResource) Provision(ctx context.Context, provCtx *ProvisionContext) (ResourceStatus, error) {
	etcdConfig := g.provisioner.getServiceConfig("etcd")

	// Get port from config or use default
	port := ports.DefaultEtcdPort
	if p, ok := etcdConfig["port"].(int); ok && p > 0 {
		port = p
	}

	// Find etcd binary
	etcdBinary, err := g.provisioner.findBinary("etcd", etcdConfig)
	if err != nil {
		return ResourceStatus{}, fmt.Errorf("etcd binary not found: %w", err)
	}

	// Check etcd version if specified
	expectedVersion, ok := etcdConfig["version"].(string)
	if ok && expectedVersion != "" {
		if err := g.provisioner.checkEtcdVersion(etcdBinary, expectedVersion); err != nil {
			return ResourceStatus{}, fmt.Errorf("etcd version check failed: %w", err)
		}
	}

	// Get data directory
	dir, ok := etcdConfig["data-dir"].(string)
	if !ok {
		return ResourceStatus{}, fmt.Errorf("etcd data directory not found in config")
	}
	dataDir := dir

	// Create data directory
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return ResourceStatus{}, fmt.Errorf("failed to create etcd data directory %s: %w", dataDir, err)
	}

	// Generate unique ID for this service instance
	serviceID := stringutil.RandomString(8)

	// Create log file
	logFile, err := g.provisioner.createLogFile("etcd", serviceID, "")
	if err != nil {
		return ResourceStatus{}, fmt.Errorf("failed to create log file: %w", err)
	}

	// Calculate peer port
	peerPort := port + 1

	// Build etcd command arguments
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
		return ResourceStatus{}, fmt.Errorf("failed to start etcd: %w", err)
	}

	// Validate process is running
	if err := g.provisioner.validateProcessRunning(etcdCmd.Process.Pid); err != nil {
		return ResourceStatus{}, fmt.Errorf("etcd process validation failed: %w", err)
	}

	// Wait for etcd to be ready
	servicePorts := map[string]int{"etcd_port": port}
	if err := g.provisioner.waitForServiceReady("etcd", "localhost", servicePorts, 10*time.Second); err != nil {
		logs := g.provisioner.readServiceLogs(logFile, 20)
		return ResourceStatus{}, fmt.Errorf("etcd readiness check failed: %w\n\nLast 20 lines from etcd logs:\n%s", err, logs)
	}

	// Create and save provision state
	service := &LocalProvisionedService{
		ID:         serviceID,
		Service:    "etcd",
		PID:        etcdCmd.Process.Pid,
		BinaryPath: etcdBinary,
		DataDir:    dataDir,
		Ports:      map[string]int{"tcp": port},
		FQDN:       "localhost",
		LogFile:    logFile,
		StartedAt:  time.Now(),
	}

	if err := g.provisioner.saveServiceState(service, ""); err != nil {
		// Log warning but don't fail
		fmt.Printf("Warning: failed to save etcd state: %v\n", err)
	}

	return ResourceStatus{
		State: StateProvisioned,
		Metadata: map[string]any{
			"pid":        etcdCmd.Process.Pid,
			"address":    fmt.Sprintf("localhost:%d", port),
			"ports":      map[string]int{"tcp": port},
			"service_id": serviceID,
			"log_file":   logFile,
			"binary":     etcdBinary,
		},
		Message: "etcd started successfully",
	}, nil
}

// Deprovision stops the etcd service.
func (g *GlobalTopoResource) Deprovision(ctx context.Context, status ResourceStatus) error {
	pid, ok := status.Metadata["pid"].(int)
	if !ok {
		return fmt.Errorf("PID not found in status metadata")
	}

	// Try to terminate gracefully first
	if err := syscall.Kill(pid, syscall.SIGTERM); err != nil {
		// Process might already be dead
		if err != syscall.ESRCH {
			return fmt.Errorf("failed to send SIGTERM to etcd (PID %d): %w", pid, err)
		}
	}

	// Wait briefly for graceful shutdown
	time.Sleep(2 * time.Second)

	// Check if process is still running, force kill if necessary
	if err := g.provisioner.validateProcessRunning(pid); err == nil {
		// Process still running, force kill
		if err := syscall.Kill(pid, syscall.SIGKILL); err != nil && err != syscall.ESRCH {
			return fmt.Errorf("failed to force kill etcd (PID %d): %w", pid, err)
		}
	}

	// Remove state file if service_id is available
	if serviceID, ok := status.Metadata["service_id"].(string); ok {
		if err := g.provisioner.removeServiceState(serviceID, "etcd", ""); err != nil {
			// Log warning but don't fail
			fmt.Printf("Warning: failed to remove etcd state file: %v\n", err)
		}
	}

	return nil
}

// Dependencies returns no dependencies for global topology.
func (g *GlobalTopoResource) Dependencies() []*clustermetadata.ID {
	return nil
}

// Children returns no children for global topology.
func (g *GlobalTopoResource) Children() []Resource {
	return nil
}

// PrintStatus implements custom status printing for global topology.
func (g *GlobalTopoResource) PrintStatus(ctx context.Context, w io.Writer, node *ResourceNode, indent int) error {
	indentStr := strings.Repeat("  ", indent)

	// Print section header
	fmt.Fprintf(w, "%s=== Provisioning etcd ===\n", indentStr)

	// Wait for this resource to complete
	select {
	case <-node.completed:
	case <-ctx.Done():
		return ctx.Err()
	}

	// Get final status
	status := node.GetStatus()

	switch status.State {
	case StateDiscovered:
		if pid, ok := status.Metadata["pid"].(int); ok {
			fmt.Fprintf(w, "%setcd is already running (PID %d) ✓\n", indentStr, pid)
		} else {
			fmt.Fprintf(w, "%setcd is already running ✓\n", indentStr)
		}

	case StateProvisioned:
		if addr, ok := status.Metadata["address"].(string); ok {
			fmt.Fprintf(w, "%s🌐 etcd available at: %s\n", indentStr, addr)
		}

	case StateFailed:
		fmt.Fprintf(w, "%s✗ etcd provisioning failed", indentStr)
		if status.Error != nil {
			fmt.Fprintf(w, ": %v", status.Error)
		}
		fmt.Fprintln(w)
	}

	fmt.Fprintln(w, "")
	return nil
}
