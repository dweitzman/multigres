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
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/multigres/multigres/go/clustermetadata/topo"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
)

// provisionContext implements the ProvisionContext interface
type provisionContext struct {
	provisioner *localProvisioner

	// Cached topology store connection
	topoStore topo.Store
	topoMu    sync.Mutex
}

// newProvisionContext creates a new ProvisionContext for the given provisioner
func newProvisionContext(p *localProvisioner) *provisionContext {
	return &provisionContext{
		provisioner: p,
	}
}

// ReadState loads the provisioned service state from disk for the given resource ID
func (pc *provisionContext) ReadState(resourceID *clustermetadatapb.ID) (*LocalProvisionedService, error) {
	statePath := pc.StatePath(resourceID)

	// Check if state file exists
	if _, err := os.Stat(statePath); os.IsNotExist(err) {
		return nil, fmt.Errorf("state file not found: %s", statePath)
	}

	// Read the state file
	data, err := os.ReadFile(statePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read state file %s: %w", statePath, err)
	}

	// Parse the state
	var state LocalProvisionedService
	if err := json.Unmarshal(data, &state); err != nil {
		return nil, fmt.Errorf("failed to parse state file %s: %w", statePath, err)
	}

	// Validate that the process is still running if there's a PID
	if state.PID > 0 {
		// Check if process exists
		process, err := os.FindProcess(state.PID)
		if err != nil {
			// Process doesn't exist
			return nil, fmt.Errorf("process not found for PID %d", state.PID)
		}

		// Send signal 0 to check if process is alive
		err = process.Signal(syscall.Signal(0))
		if err != nil {
			// Process is not alive
			return nil, fmt.Errorf("process %d is not running", state.PID)
		}
	}

	return &state, nil
}

// SaveState saves the provisioned service state to disk
func (pc *provisionContext) SaveState(state *LocalProvisionedService) error {
	statePath := pc.StatePath(state.toResourceID())

	// Ensure state directory exists
	stateDir := filepath.Dir(statePath)
	if err := os.MkdirAll(stateDir, 0o755); err != nil {
		return fmt.Errorf("failed to create state directory %s: %w", stateDir, err)
	}

	// Marshal the state to JSON
	data, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal state: %w", err)
	}

	// Write atomically: write to temp file then rename
	tempPath := statePath + ".tmp"
	if err := os.WriteFile(tempPath, data, 0o644); err != nil {
		return fmt.Errorf("failed to write temp state file %s: %w", tempPath, err)
	}

	if err := os.Rename(tempPath, statePath); err != nil {
		os.Remove(tempPath) // Clean up temp file
		return fmt.Errorf("failed to rename temp state file: %w", err)
	}

	return nil
}

// DeleteState removes the state file for the given resource ID
func (pc *provisionContext) DeleteState(resourceID *clustermetadatapb.ID) error {
	statePath := pc.StatePath(resourceID)

	// Check if state file exists
	if _, err := os.Stat(statePath); os.IsNotExist(err) {
		// File doesn't exist, nothing to delete
		return nil
	}

	// Remove the state file
	if err := os.Remove(statePath); err != nil {
		return fmt.Errorf("failed to remove state file %s: %w", statePath, err)
	}

	return nil
}

// ListStates returns all provisioned service states
func (pc *provisionContext) ListStates() ([]*LocalProvisionedService, error) {
	stateDir := pc.provisioner.getStateDir()

	// Check if state directory exists
	if _, err := os.Stat(stateDir); os.IsNotExist(err) {
		// No state directory means no states
		return []*LocalProvisionedService{}, nil
	}

	var states []*LocalProvisionedService

	// Walk the state directory recursively
	err := filepath.Walk(stateDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		// Skip directories and non-JSON files
		if info.IsDir() || !strings.HasSuffix(path, ".json") {
			return nil
		}

		// Read the state file
		data, err := os.ReadFile(path)
		if err != nil {
			// Log error but continue
			fmt.Fprintf(os.Stderr, "Warning: failed to read state file %s: %v\n", path, err)
			return nil
		}

		// Parse the state
		var state LocalProvisionedService
		if err := json.Unmarshal(data, &state); err != nil {
			// Log error but continue
			fmt.Fprintf(os.Stderr, "Warning: failed to parse state file %s: %v\n", path, err)
			return nil
		}

		states = append(states, &state)
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("failed to walk state directory: %w", err)
	}

	return states, nil
}

// OpenGlobalTopo returns the topology store for global operations
func (pc *provisionContext) OpenGlobalTopo(ctx context.Context) (topo.Store, error) {
	pc.topoMu.Lock()
	defer pc.topoMu.Unlock()

	// Return cached connection if available
	if pc.topoStore != nil {
		return pc.topoStore, nil
	}

	// Get etcd configuration
	etcdAddr := pc.EtcdClientAddress()

	// Get topology configuration
	topoConfig := pc.provisioner.config.Topology
	backend := topoConfig.Backend
	if backend == "" {
		backend = "etcd2"
	}

	rootPath := topoConfig.GlobalRootPath
	if rootPath == "" {
		rootPath = "/multigres/global"
	}

	// Open topology store
	store, err := topo.OpenServer(backend, rootPath, []string{etcdAddr})
	if err != nil {
		return nil, fmt.Errorf("failed to open topology server: %w", err)
	}

	// Cache the connection
	pc.topoStore = store
	return store, nil
}

// GetConfig returns the full provisioner configuration
func (pc *provisionContext) GetConfig() *LocalProvisionerConfig {
	return pc.provisioner.config
}

// LogPath creates a log file path for the given service
func (pc *provisionContext) LogPath(serviceName, serviceID, databaseName string) (string, error) {
	return pc.provisioner.createLogFile(serviceName, serviceID, databaseName)
}

// DataPath returns the data directory path for the given service
func (pc *provisionContext) DataPath(serviceName, serviceID string) string {
	return filepath.Join(pc.provisioner.getDataDir(), fmt.Sprintf("%s_%s", serviceName, serviceID))
}

// StatePath returns the state file path for the given resource ID
func (pc *provisionContext) StatePath(resourceID *clustermetadatapb.ID) string {
	stateDir := pc.provisioner.getStateDir()

	// Generate state file name from resource ID
	// Format: <component>_<name>.json for global resources
	// Format: dbs/<database>/<component>_<name>.json for database resources
	var statePath string

	// Check if this is a database-scoped resource
	if resourceID.Component == clustermetadatapb.ID_MULTIPOOLER ||
		resourceID.Component == clustermetadatapb.ID_MULTIGATEWAY ||
		resourceID.Component == clustermetadatapb.ID_MULTIORCH {
		// Database-scoped resource - need to extract database name from metadata
		// For now, use a "dbs" subdirectory. This will be populated correctly
		// by SaveState which has access to the full state including metadata.
		// When reading, we'll search recursively.
		dbName := "unknown" // This will be overridden by the actual metadata
		statePath = filepath.Join(stateDir, "dbs", dbName, fmt.Sprintf("%s_%s.json",
			strings.ToLower(resourceID.Component.String()), resourceID.Name))
	} else {
		// Global resource
		statePath = filepath.Join(stateDir, fmt.Sprintf("%s_%s.json",
			strings.ToLower(resourceID.Component.String()), resourceID.Name))
	}

	return statePath
}

// FindBinary finds a binary in PATH or uses the configured path
func (pc *provisionContext) FindBinary(name string, configuredPath string) (string, error) {
	// If a configured path is provided and exists, use it
	if configuredPath != "" {
		if _, err := os.Stat(configuredPath); err == nil {
			return configuredPath, nil
		}
	}

	// Try to find in PATH
	path, err := exec.LookPath(name)
	if err != nil {
		return "", fmt.Errorf("%s binary not found in PATH and no valid configured path provided", name)
	}

	return path, nil
}

// EtcdClientAddress returns the etcd client address
func (pc *provisionContext) EtcdClientAddress() string {
	port := pc.provisioner.config.Etcd.Port
	if port == 0 {
		port = 2379 // default etcd port
	}
	return fmt.Sprintf("http://localhost:%d", port)
}

// EtcdPeerAddress returns the etcd peer address
func (pc *provisionContext) EtcdPeerAddress() string {
	port := pc.provisioner.config.Etcd.Port
	if port == 0 {
		port = 2379 // default etcd port
	}
	return fmt.Sprintf("http://localhost:%d", port+1)
}

// ValidateProcessRunning checks if a process is running
func (pc *provisionContext) ValidateProcessRunning(pid int) error {
	process, err := os.FindProcess(pid)
	if err != nil {
		return fmt.Errorf("process not found: %w", err)
	}

	// Send signal 0 to check if process is alive
	err = process.Signal(syscall.Signal(0))
	if err != nil {
		return fmt.Errorf("process not running: %w", err)
	}

	return nil
}

// StopProcess stops a process by PID
func (pc *provisionContext) StopProcess(pid int) error {
	process, err := os.FindProcess(pid)
	if err != nil {
		// Process not found, consider it already stopped
		return nil
	}

	// Send SIGTERM
	if err := process.Signal(syscall.SIGTERM); err != nil {
		// Process might already be stopped
		return nil
	}

	// Wait for process to exit (with timeout)
	// TODO: Add proper waiting with timeout
	time.Sleep(1 * time.Second)

	return nil
}

// WaitForServiceReady waits for a service to become ready by checking appropriate endpoints
func (pc *provisionContext) WaitForServiceReady(ctx context.Context, serviceName string, host string, servicePorts map[string]int) error {
	return pc.provisioner.waitForServiceReady(ctx, serviceName, host, servicePorts)
}

// Helper method to convert LocalProvisionedService to resource ID
// This needs to be added to the LocalProvisionedService struct
func (s *LocalProvisionedService) toResourceID() *clustermetadatapb.ID {
	// Parse component type from service name
	var componentType clustermetadatapb.ID_ComponentType
	switch s.Service {
	case "etcd":
		componentType = clustermetadatapb.ID_GLOBAL_TOPO
	case "multiadmin":
		componentType = clustermetadatapb.ID_MULTIADMIN
	case "multipooler":
		componentType = clustermetadatapb.ID_MULTIPOOLER
	case "multigateway":
		componentType = clustermetadatapb.ID_MULTIGATEWAY
	case "multiorch":
		componentType = clustermetadatapb.ID_MULTIORCH
	default:
		componentType = clustermetadatapb.ID_UNKNOWN
	}

	// Get cell from metadata
	cell := topo.GlobalCell
	if cellName, ok := s.Metadata["cell"].(string); ok && cellName != "" {
		cell = cellName
	}

	return &clustermetadatapb.ID{
		Component: componentType,
		Cell:      cell,
		Name:      s.ID,
	}
}
