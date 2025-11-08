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
	"os/exec"
	"strings"
	"syscall"
	"time"

	"github.com/multigres/multigres/go/clustermetadata/topo"
	"github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/provisioner/local/ports"
	"github.com/multigres/multigres/go/tools/stringutil"
)

// MultiadminResource represents the global multiadmin service.
type MultiadminResource struct {
	provisioner *localProvisioner
}

// NewMultiadminResource creates a new multiadmin resource.
func NewMultiadminResource(p *localProvisioner) (*MultiadminResource, error) {
	return &MultiadminResource{
		provisioner: p,
	}, nil
}

// ID returns the resource ID for multiadmin.
func (m *MultiadminResource) ID() *clustermetadata.ID {
	return &clustermetadata.ID{
		Component: clustermetadata.ID_MULTIADMIN,
		Cell:      topo.GlobalCell,
		Name:      "multiadmin",
	}
}

// DisplayName returns the display name for multiadmin.
func (m *MultiadminResource) DisplayName() string {
	return "multiadmin"
}

// Discover checks if multiadmin is already running.
func (m *MultiadminResource) Discover(ctx context.Context, provCtx *ProvisionContext) (ResourceStatus, error) {
	existingService, err := m.provisioner.findRunningService("multiadmin")
	if err != nil {
		return ResourceStatus{}, fmt.Errorf("failed to check for existing multiadmin service: %w", err)
	}

	if existingService != nil {
		// multiadmin is already running
		return ResourceStatus{
			State: StateDiscovered,
			Metadata: map[string]any{
				"pid":        existingService.PID,
				"ports":      existingService.Ports,
				"service_id": existingService.ID,
				"log_file":   existingService.LogFile,
			},
			Message: fmt.Sprintf("multiadmin already running (PID %d)", existingService.PID),
		}, nil
	}

	return ResourceStatus{
		State:   StateNotFound,
		Message: "multiadmin not running",
	}, nil
}

// Provision starts the multiadmin service.
func (m *MultiadminResource) Provision(ctx context.Context, provCtx *ProvisionContext) (ResourceStatus, error) {
	multiadminConfig := m.provisioner.getServiceConfig("multiadmin")

	// Get HTTP port from config
	httpPort := ports.DefaultMultiadminHTTP
	if p, ok := multiadminConfig["http_port"].(int); ok && p > 0 {
		httpPort = p
	}

	// Get gRPC port from config
	grpcPort := ports.DefaultMultiadminGRPC
	if p, ok := multiadminConfig["grpc_port"].(int); ok && p > 0 {
		grpcPort = p
	}

	// Get etcd address from running etcd service
	etcdService, err := m.provisioner.findRunningEtcdService()
	if err != nil {
		return ResourceStatus{}, fmt.Errorf("failed to find running etcd service: %w", err)
	}
	if etcdService == nil {
		return ResourceStatus{}, fmt.Errorf("etcd service is not running (required dependency)")
	}
	etcdPort := etcdService.Ports["tcp"]
	etcdAddress := fmt.Sprintf("localhost:%d", etcdPort)

	// Get topology config
	topoConfig, err := m.provisioner.getTopologyConfig()
	if err != nil {
		return ResourceStatus{}, fmt.Errorf("failed to get topology config: %w", err)
	}

	// Get log level
	logLevel := "info"
	if level, ok := multiadminConfig["log_level"].(string); ok {
		logLevel = level
	}

	// Find multiadmin binary
	multiadminBinary, err := m.provisioner.findBinary("multiadmin", multiadminConfig)
	if err != nil {
		return ResourceStatus{}, fmt.Errorf("multiadmin binary not found: %w", err)
	}

	// Generate unique ID for this service instance
	serviceID := stringutil.RandomString(8)

	// Create log file
	logFile, err := m.provisioner.createLogFile("multiadmin", serviceID, "")
	if err != nil {
		return ResourceStatus{}, fmt.Errorf("failed to create log file: %w", err)
	}

	// Build command arguments
	args := []string{
		"--http-port", fmt.Sprintf("%d", httpPort),
		"--grpc-port", fmt.Sprintf("%d", grpcPort),
		"--topo-global-server-addresses", etcdAddress,
		"--topo-global-root", topoConfig.GlobalRootPath,
		"--topo-implementation", topoConfig.Backend,
		"--log-level", logLevel,
		"--log-output", logFile,
		"--service-map", "grpc-multiadmin",
		"--hostname", "localhost",
	}

	// Start multiadmin process
	multiadminCmd := exec.CommandContext(ctx, multiadminBinary, args...)
	if err := multiadminCmd.Start(); err != nil {
		return ResourceStatus{}, fmt.Errorf("failed to start multiadmin: %w", err)
	}

	// Validate process is running
	if err := m.provisioner.validateProcessRunning(multiadminCmd.Process.Pid); err != nil {
		return ResourceStatus{}, fmt.Errorf("multiadmin process validation failed: %w", err)
	}

	// Create and save provision state
	service := &LocalProvisionedService{
		ID:         serviceID,
		Service:    "multiadmin",
		PID:        multiadminCmd.Process.Pid,
		BinaryPath: multiadminBinary,
		Ports:      map[string]int{"http_port": httpPort, "grpc_port": grpcPort},
		FQDN:       "localhost",
		LogFile:    logFile,
		StartedAt:  time.Now(),
	}

	if err := m.provisioner.saveServiceState(service, ""); err != nil {
		// Log warning but don't fail
		fmt.Printf("Warning: failed to save multiadmin state: %v\n", err)
	}

	// Wait for multiadmin to be ready
	servicePorts := map[string]int{"http_port": httpPort, "grpc_port": grpcPort}
	if err := m.provisioner.waitForServiceReady("multiadmin", "localhost", servicePorts, 10*time.Second); err != nil {
		logs := m.provisioner.readServiceLogs(logFile, 20)
		return ResourceStatus{}, fmt.Errorf("multiadmin readiness check failed: %w\n\nLast 20 lines from multiadmin logs:\n%s", err, logs)
	}

	return ResourceStatus{
		State: StateProvisioned,
		Metadata: map[string]any{
			"pid":        multiadminCmd.Process.Pid,
			"ports":      map[string]int{"http_port": httpPort, "grpc_port": grpcPort},
			"service_id": serviceID,
			"log_file":   logFile,
			"binary":     multiadminBinary,
		},
		Message: "multiadmin started successfully",
	}, nil
}

// Deprovision stops the multiadmin service.
func (m *MultiadminResource) Deprovision(ctx context.Context, status ResourceStatus) error {
	pid, ok := status.Metadata["pid"].(int)
	if !ok {
		return fmt.Errorf("PID not found in status metadata")
	}

	// Try to terminate gracefully first
	if err := syscall.Kill(pid, syscall.SIGTERM); err != nil {
		// Process might already be dead
		if err != syscall.ESRCH {
			return fmt.Errorf("failed to send SIGTERM to multiadmin (PID %d): %w", pid, err)
		}
	}

	// Wait briefly for graceful shutdown
	time.Sleep(2 * time.Second)

	// Check if process is still running, force kill if necessary
	if err := m.provisioner.validateProcessRunning(pid); err == nil {
		// Process still running, force kill
		if err := syscall.Kill(pid, syscall.SIGKILL); err != nil && err != syscall.ESRCH {
			return fmt.Errorf("failed to force kill multiadmin (PID %d): %w", pid, err)
		}
	}

	// Remove state file if service_id is available
	if serviceID, ok := status.Metadata["service_id"].(string); ok {
		if err := m.provisioner.removeServiceState(serviceID, "multiadmin", ""); err != nil {
			// Log warning but don't fail
			fmt.Printf("Warning: failed to remove multiadmin state file: %v\n", err)
		}
	}

	return nil
}

// Dependencies returns the global topology (etcd) as a dependency.
func (m *MultiadminResource) Dependencies() []*clustermetadata.ID {
	return []*clustermetadata.ID{
		{
			Component: clustermetadata.ID_GLOBAL_TOPO,
			Cell:      topo.GlobalCell,
			Name:      "etcd",
		},
	}
}

// Children returns no children for multiadmin.
func (m *MultiadminResource) Children() []Resource {
	return nil
}

// PrintStatus implements custom status printing for multiadmin.
func (m *MultiadminResource) PrintStatus(ctx context.Context, w io.Writer, node *ResourceNode, indent int) error {
	indentStr := strings.Repeat("  ", indent)

	// Print section header
	fmt.Fprintf(w, "%s=== Provisioning multiadmin ===\n", indentStr)

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
			fmt.Fprintf(w, "%smultiadmin is already running (PID %d) ✓\n", indentStr, pid)
		} else {
			fmt.Fprintf(w, "%smultiadmin is already running ✓\n", indentStr)
		}

	case StateProvisioned:
		if ports, ok := status.Metadata["ports"].(map[string]int); ok {
			fmt.Fprintf(w, "%s🌐 multiadmin available (HTTP:%d, gRPC:%d)\n",
				indentStr, ports["http_port"], ports["grpc_port"])
		}

	case StateFailed:
		fmt.Fprintf(w, "%s✗ multiadmin provisioning failed", indentStr)
		if status.Error != nil {
			fmt.Fprintf(w, ": %v", status.Error)
		}
		fmt.Fprintln(w)
	}

	fmt.Fprintln(w, "")
	return nil
}
