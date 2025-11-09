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
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"time"

	"github.com/multigres/multigres/go/clustermetadata/topo"
	"github.com/multigres/multigres/go/provisioner"
	"github.com/multigres/multigres/go/tools/pathutil"
	"github.com/multigres/multigres/go/tools/retry"
	"github.com/multigres/multigres/go/tools/stringutil"

	"gopkg.in/yaml.v3"
)

// localProvisioner implements the Provisioner interface for local binary-based provisioning
type localProvisioner struct {
	config  *LocalProvisionerConfig
	dataDir string // Base data directory for this provisioner instance
}

// Compile-time check to ensure localProvisioner implements Provisioner
var _ provisioner.Provisioner = (*localProvisioner)(nil)

const (
	// StateDir is the directory name where provision state files are stored
	StateDir = "state"
)

// Name returns the name of this provisioner
func (p *localProvisioner) Name() string {
	return "local"
}

// findBinary finds a binary by name, checking PATH first, then the executable directory,
// and then the optional configured path
func (p *localProvisioner) findBinary(name string, serviceConfig map[string]any) (string, error) {
	// First try to find in PATH
	if binaryPath, err := exec.LookPath(name); err == nil {
		return binaryPath, nil
	}

	// Then try configured path if provided
	if pathConfig, ok := serviceConfig["path"].(string); ok && pathConfig != "" {
		// Check if it's an absolute path or relative path
		var fullPath string
		if filepath.IsAbs(pathConfig) {
			fullPath = pathConfig
		} else {
			// Make it relative to current directory
			fullPath = filepath.Join(".", pathConfig)
		}

		// Check if the binary exists and is executable
		if info, err := os.Stat(fullPath); err == nil && !info.IsDir() {
			return fullPath, nil
		}
	}

	return "", fmt.Errorf("binary '%s' not found in PATH or configured path", name)
}

// readServiceLogs reads the last few lines from a service's log file for debugging
func (p *localProvisioner) readServiceLogs(logFile string, lines int) string {
	if logFile == "" {
		return "No log file available"
	}

	// Check if log file exists
	if _, err := os.Stat(logFile); os.IsNotExist(err) {
		return fmt.Sprintf("Log file not found: %s", logFile)
	}

	// Read the file
	data, err := os.ReadFile(logFile)
	if err != nil {
		return fmt.Sprintf("Failed to read log file %s: %v", logFile, err)
	}

	// Get the last N lines
	logLines := strings.Split(strings.TrimSpace(string(data)), "\n")
	if len(logLines) == 0 {
		return "Log file is empty"
	}

	// Return last 'lines' lines or all lines if fewer exist
	start := max(len(logLines)-lines, 0)

	result := strings.Join(logLines[start:], "\n")
	if result == "" {
		return "Log file is empty"
	}

	return result
}

// getRootWorkingDir returns the root working directory from config
func (p *localProvisioner) getRootWorkingDir() string {
	if p.config == nil {
		return "."
	}

	return p.config.RootWorkingDir
}

// GeneratePoolerDir generates a pooler directory path for a given base directory and service ID
func GeneratePoolerDir(baseDir, serviceID string) string {
	return filepath.Join(baseDir, "data", fmt.Sprintf("pooler_%s", serviceID))
}

// PgctldProvisionResult contains the result of provisioning pgctld
type PgctldProvisionResult struct {
	Address string
	Port    int
	LogFile string
}

// loadServiceState loads a specific service state from disk
func (p *localProvisioner) loadServiceState(req *provisioner.DeprovisionRequest) (*LocalProvisionedService, error) {
	stateDir := p.getStateDir()
	var targetDir string

	if req.DatabaseName != "" {
		// For database services: state/dbs/dbname
		targetDir = filepath.Join(stateDir, "dbs", req.DatabaseName)
	} else {
		// For non-database services (like etcd): state/
		targetDir = stateDir
	}

	fileName := fmt.Sprintf("%s_%s.json", req.Service, req.ServiceID)
	filePath := filepath.Join(targetDir, fileName)

	// Check if state file exists
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		return nil, nil // Service not found
	}

	data, err := os.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read state file %s: %w", filePath, err)
	}

	var service LocalProvisionedService
	if err := json.Unmarshal(data, &service); err != nil {
		return nil, fmt.Errorf("failed to parse state file %s: %w", filePath, err)
	}

	// Sanity check: ensure this method is called for the expected service type
	if req.Service != service.Service {
		return nil, fmt.Errorf("deprovision%s called for wrong service type: %s", service.Service, req.Service)
	}

	return &service, nil
}

// stopService stops a specific service based on its type using the internal methods
func (p *localProvisioner) stopService(ctx context.Context, req *provisioner.DeprovisionRequest) error {
	switch req.Service {
	case "etcd":
		fallthrough
	case "multigateway":
		fallthrough
	case "multipooler":
		fallthrough
	case "multiorch":
		fallthrough
	case "multiadmin":
		return p.deprovisionService(ctx, req)
	case "pgctld":
		// pgctld requires special handling to stop PostgreSQL first
		service, err := p.loadServiceState(req)
		if err != nil {
			return err
		}
		if service == nil {
			return fmt.Errorf("pgctld service not found")
		}
		return p.deprovisionPgctld(ctx, service)
	default:
		return fmt.Errorf("unknown service type: %s", req.Service)
	}
}

// deprovisionService(ctx stops a multiorch service instance
func (p *localProvisioner) deprovisionService(ctx context.Context, req *provisioner.DeprovisionRequest) error {
	// Load the specific service state
	service, err := p.loadServiceState(req)
	if err != nil {
		return err
	}

	if service == nil {
		return fmt.Errorf("service not found")
	}

	// Stop the process if it's running
	if service.PID > 0 {
		if err := p.stopProcessByPID(service.PID); err != nil {
			return fmt.Errorf("failed to stop process: %w", err)
		}
	}

	// Clean up log file if it exists
	if service.LogFile != "" {
		if err := p.cleanupLogFile(service.LogFile); err != nil {
			fmt.Printf("Warning: failed to clean up log file %s: %v\n", service.LogFile, err)
		}
	}

	// Remove state file
	if err := p.removeServiceState(req.ServiceID, req.Service, req.DatabaseName); err != nil {
		fmt.Printf("Warning: failed to remove etcd state file: %v\n", err)
	}

	// Clean up data directory if requested
	if req.Clean && service.DataDir != "" {
		fmt.Printf("Cleaning service data directory: %s\n", service.DataDir)
		if err := os.RemoveAll(service.DataDir); err != nil {
			return fmt.Errorf("failed to remove etcd data directory: %w", err)
		}
	}

	return nil
}

// stopProcessByPID stops a process by its PID
func (p *localProvisioner) stopProcessByPID(pid int) error {
	// Check if process exists
	process, err := os.FindProcess(pid)
	if err != nil {
		// Process not found, assume already cleaned up
		fmt.Printf("Process %d not found, assuming already stopped\n", pid)
		return nil
	}

	// Send SIGTERM to gracefully stop the process
	if err := process.Signal(syscall.SIGTERM); err != nil {
		// Process might already be dead, check errno
		errMsg := err.Error()
		if strings.Contains(errMsg, "no such process") || strings.Contains(errMsg, "process already finished") {
			fmt.Printf("Process %d already stopped\n", pid)
			return nil
		}

		// If SIGTERM fails for other reasons, try SIGKILL
		if err := process.Kill(); err != nil {
			// If kill also fails and it's because process doesn't exist, that's ok
			errMsg := err.Error()
			if strings.Contains(errMsg, "no such process") || strings.Contains(errMsg, "process already finished") {
				fmt.Printf("Process %d already stopped\n", pid)
				return nil
			}
			return fmt.Errorf("failed to kill process %d: %w", pid, err)
		}
	}

	// Wait for the process to actually exit
	p.waitForProcessExit(process, 2*time.Second)

	return nil
}

// waitForProcessExit waits for a process to exit by polling with Signal(0)
func (p *localProvisioner) waitForProcessExit(process *os.Process, timeout time.Duration) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	r := retry.New(10*time.Millisecond, 1*time.Second)
	for _, err := range r.Attempts(ctx) {
		if err != nil {
			// Timeout reached
			fmt.Printf("Process %d still running after SIGTERM\n", process.Pid)
			return
		}

		// Send null signal to test if process exists
		err := process.Signal(syscall.Signal(0))
		if err != nil {
			fmt.Printf("Process %d stopped successfully\n", process.Pid)
			// Process has exited or doesn't exist
			return
		}
	}
}

// Bootstrap sets up etcd and creates the default database
// This method now uses the new resource-based architecture (BootstrapV2)
func (p *localProvisioner) Bootstrap(ctx context.Context) ([]*provisioner.ProvisionResult, error) {
	// Call the new resource-based bootstrap with failFast=false for backward compatibility
	return p.BootstrapV2(ctx, false)
}

// discoverDatabasesFromState discovers all databases that have running services by examining state files
func (p *localProvisioner) discoverDatabasesFromState() ([]string, error) {
	stateDir := p.getStateDir()
	dbsDir := filepath.Join(stateDir, "dbs")

	// Check if dbs directory exists
	if _, err := os.Stat(dbsDir); os.IsNotExist(err) {
		return nil, nil // No databases directory, no databases to deprovision
	}

	// Read all database directories
	entries, err := os.ReadDir(dbsDir)
	if err != nil {
		return nil, fmt.Errorf("failed to read databases directory %s: %w", dbsDir, err)
	}

	var databases []string
	for _, entry := range entries {
		if entry.IsDir() {
			databases = append(databases, entry.Name())
		}
	}

	return databases, nil
}

// buildGlobalResources creates Resource objects for global services (etcd, multiadmin)
func (p *localProvisioner) buildGlobalResources() ([]Resource, error) {
	var resources []Resource

	// Add etcd resource
	etcdResource := NewGlobalTopoResource("etcd", &p.config.Etcd)
	resources = append(resources, etcdResource)

	// Add multiadmin resource
	multiadminResource := NewMultiadminResource("multiadmin", &p.config.Multiadmin)
	resources = append(resources, multiadminResource)

	return resources, nil
}

// Teardown shuts down all services (reverse of Bootstrap)
func (p *localProvisioner) Teardown(ctx context.Context, clean bool) error {
	fmt.Println("=== Tearing down Multigres cluster ===")

	// Get etcd address for database deprovisioning
	etcdPort := p.config.Etcd.Port
	etcdAddress := fmt.Sprintf("localhost:%d", etcdPort)

	// 1. Discover and deprovision all databases from state files
	databases, err := p.discoverDatabasesFromState()
	if err != nil {
		fmt.Printf("Warning: failed to discover databases from state: %v\n", err)
	} else if len(databases) > 0 {
		fmt.Printf("Found %d database(s) to deprovision\n", len(databases))
		for _, dbName := range databases {
			fmt.Printf("Deprovisioning database: %s\n", dbName)
			if err := p.DeprovisionDatabase(ctx, dbName, etcdAddress); err != nil {
				fmt.Printf("Warning: failed to deprovision database %s: %v\n", dbName, err)
			}
		}
	}

	// 2. Deprovision global services (multiadmin, etcd) using the provisioner engine
	fmt.Println("=== Deprovisioning global services ===")
	globalResources, err := p.buildGlobalResources()
	if err != nil {
		fmt.Printf("Warning: failed to build global resources: %v\n", err)
	} else {
		pctx := newProvisionContext(p)
		engine := NewProvisionerEngine(pctx, false) // Don't fail fast during teardown

		if err := engine.DeprovisionResources(ctx, globalResources); err != nil {
			fmt.Printf("Warning: failed to deprovision global services: %v\n", err)
		}
	}

	// 3. Clean up logs, state, and data directories if requested
	if clean {
		logsDir := p.getLogsDir()
		if err := p.cleanupLogsDirectory(logsDir); err != nil {
			fmt.Printf("Warning: failed to clean up logs directory: %v\n", err)
		}

		stateDir := p.getStateDir()
		if err := p.cleanupStateDirectory(stateDir); err != nil {
			fmt.Printf("Warning: failed to clean up state directory: %v\n", err)
		}

		dataDir := p.getDataDir()
		if err := p.cleanupDataDirectory(dataDir); err != nil {
			fmt.Printf("Warning: failed to clean up data directory: %v\n", err)
		}

		socketsDir := filepath.Join(p.config.RootWorkingDir, "sockets")
		if err := p.cleanupSocketsDirectory(socketsDir); err != nil {
			fmt.Printf("Warning: failed to clean up sockets directory: %v\n", err)
		}
	}

	fmt.Println("Teardown completed successfully")
	return nil
}

// cleanupLogsDirectory removes the entire logs directory and all its contents
func (p *localProvisioner) cleanupLogsDirectory(logsDir string) error {
	// Check if logs directory exists
	if _, err := os.Stat(logsDir); os.IsNotExist(err) {
		return nil // Directory doesn't exist, nothing to clean up
	}

	// Remove the entire logs directory
	if err := os.RemoveAll(logsDir); err != nil {
		return fmt.Errorf("failed to remove logs directory %s: %w", logsDir, err)
	}

	fmt.Printf("Cleaned up logs directory: %s\n", logsDir)
	return nil
}

// cleanupStateDirectory removes the entire state directory and all its contents
func (p *localProvisioner) cleanupStateDirectory(stateDir string) error {
	// Check if state directory exists
	if _, err := os.Stat(stateDir); os.IsNotExist(err) {
		return nil // Directory doesn't exist, nothing to clean up
	}

	// Remove the entire state directory
	if err := os.RemoveAll(stateDir); err != nil {
		return fmt.Errorf("failed to remove state directory %s: %w", stateDir, err)
	}

	fmt.Printf("Cleaned up state directory: %s\n", stateDir)
	return nil
}

// cleanupDataDirectory removes the entire data directory and all its contents
func (p *localProvisioner) cleanupDataDirectory(dataDir string) error {
	// Check if data directory exists
	if _, err := os.Stat(dataDir); os.IsNotExist(err) {
		return nil // Directory doesn't exist, nothing to clean up
	}

	// Remove the entire data directory
	if err := os.RemoveAll(dataDir); err != nil {
		return fmt.Errorf("failed to remove data directory %s: %w", dataDir, err)
	}

	fmt.Printf("Cleaned up data directory: %s\n", dataDir)
	return nil
}

// cleanupSocketsDirectory removes the entire sockets directory and all its contents
func (p *localProvisioner) cleanupSocketsDirectory(socketsDir string) error {
	if _, err := os.Stat(socketsDir); os.IsNotExist(err) {
		return nil // Directory doesn't exist, nothing to clean up
	}

	if err := os.RemoveAll(socketsDir); err != nil {
		return fmt.Errorf("failed to remove sockets directory %s: %w", socketsDir, err)
	}

	fmt.Printf("Cleaned up sockets directory: %s\n", socketsDir)
	return nil
}

// getGRPCSocketFile extracts and prepares the gRPC socket file path from a service config.
// It returns the absolute path to the socket file and ensures the socket directory exists.
// Returns empty string if no socket file is configured.
func getGRPCSocketFile(serviceConfig map[string]any) (string, error) {
	sf, ok := serviceConfig["grpc_socket_file"].(string)
	if !ok || sf == "" {
		return "", nil // No socket file configured
	}

	// Convert to absolute path since the working directory may change
	socketFile, err := filepath.Abs(sf)
	if err != nil {
		return "", fmt.Errorf("failed to resolve socket file path: %w", err)
	}

	// Ensure socket directory exists
	socketDir := filepath.Dir(socketFile)
	if err := os.MkdirAll(socketDir, 0o755); err != nil {
		return "", fmt.Errorf("failed to create socket directory: %w", err)
	}

	return socketFile, nil
}

// getDefaultDatabaseName returns the default database name from config
func (p *localProvisioner) getDefaultDatabaseName() (string, error) {
	if p.config == nil {
		return "", fmt.Errorf("provisioner config not set")
	}

	if p.config.DefaultDbName == "" {
		return "", fmt.Errorf("default-dbname not specified in configuration")
	}

	return p.config.DefaultDbName, nil
}

// ProvisionDatabase provisions a complete database stack using the new resource-based architecture
func (p *localProvisioner) ProvisionDatabase(ctx context.Context, databaseName string, etcdAddress string) ([]*provisioner.ProvisionResult, error) {
	// Build database resources
	dbResources, err := p.buildDatabaseResources(databaseName)
	if err != nil {
		return nil, fmt.Errorf("failed to build database resources: %w", err)
	}

	// Create provisioning context and engine
	pctx := newProvisionContext(p)
	engine := NewProvisionerEngine(pctx, false)

	// Provision all database resources
	results, err := engine.ProvisionResources(ctx, dbResources)
	if err != nil {
		return nil, fmt.Errorf("failed to provision database %s: %w", databaseName, err)
	}

	return results, nil
}

// DeprovisionDatabase deprovisions all services for a database using the provisioner engine
// It discovers services from state files rather than configuration
func (p *localProvisioner) DeprovisionDatabase(ctx context.Context, databaseName string, etcdAddress string) error {
	fmt.Printf("=== Deprovisioning database: %s ===\n", databaseName)

	// Build database resources from actual running state, not configuration
	dbResources, err := p.buildDatabaseResourcesFromState(databaseName)
	if err != nil {
		return fmt.Errorf("failed to build database resources for %s: %w", databaseName, err)
	}

	if len(dbResources) == 0 {
		fmt.Printf("No services found for database %s\n", databaseName)
		return nil
	}

	// Use the provisioner engine to deprovision all database resources in reverse order
	pctx := newProvisionContext(p)
	engine := NewProvisionerEngine(pctx, false) // Don't fail fast during teardown

	if err := engine.DeprovisionResources(ctx, dbResources); err != nil {
		return fmt.Errorf("failed to deprovision database %s: %w", databaseName, err)
	}

	fmt.Printf("Database %s deprovisioned successfully\n", databaseName)
	return nil
}

// getAllCells returns all configured cells
func (p *localProvisioner) getAllCells() ([]CellConfig, error) {
	if p.config == nil {
		return nil, fmt.Errorf("provisioner config not set")
	}

	if len(p.config.Topology.Cells) == 0 {
		return nil, fmt.Errorf("no cells configured")
	}

	return p.config.Topology.Cells, nil
}

// getCellNames returns the names of all configured cells
func (p *localProvisioner) getCellNames() ([]string, error) {
	cells, err := p.getAllCells()
	if err != nil {
		return nil, err
	}

	var names []string
	for _, cell := range cells {
		names = append(names, cell.Name)
	}
	return names, nil
}

// getCellIndex returns the index of a cell in the list of cell names (for port calculation)
func (p *localProvisioner) getCellIndex(cellName string) (int, error) {
	cells, err := p.getAllCells()
	if err != nil {
		return -1, err
	}

	// Find the cell by name and return its index
	for i, cell := range cells {
		if cell.Name == cellName {
			return i, nil
		}
	}

	return -1, fmt.Errorf("cell %s not found", cellName)
}

// getCellByName returns the cell configuration for a specific cell name
func (p *localProvisioner) getCellByName(cellName string) (*CellConfig, error) {
	if p.config == nil {
		return nil, fmt.Errorf("provisioner config not set")
	}

	if len(p.config.Topology.Cells) == 0 {
		return nil, fmt.Errorf("no cells configured")
	}

	// Find the specific cell by name
	for _, cell := range p.config.Topology.Cells {
		if cell.Name == cellName {
			return &cell, nil
		}
	}

	return nil, fmt.Errorf("cell %s not found in configuration", cellName)
}

// ValidateConfig validates the local provisioner configuration
func (p *localProvisioner) ValidateConfig(config map[string]any) error {
	// Convert to typed configuration for validation
	typedConfig := &LocalProvisionerConfig{}
	yamlData, err := yaml.Marshal(config)
	if err != nil {
		return fmt.Errorf("failed to marshal config: %w", err)
	}
	if err := yaml.Unmarshal(yamlData, typedConfig); err != nil {
		return fmt.Errorf("failed to unmarshal config: %w", err)
	}

	// Validate topology backend
	availableBackends := topo.GetAvailableImplementations()
	validBackend := slices.Contains(availableBackends, typedConfig.Topology.Backend)
	if !validBackend {
		return fmt.Errorf("invalid topo backend: %s (available: %v)", typedConfig.Topology.Backend, availableBackends)
	}

	// Validate required topology fields
	if typedConfig.Topology.GlobalRootPath == "" {
		return fmt.Errorf("topology global-root-path is required")
	}
	if len(typedConfig.Topology.Cells) == 0 {
		return fmt.Errorf("topology must have at least one cell configured")
	}
	// Validate each cell
	for i, cell := range typedConfig.Topology.Cells {
		if cell.Name == "" {
			return fmt.Errorf("cell at index %d name is required", i)
		}
		if cell.RootPath == "" {
			return fmt.Errorf("cell %s root-path is required", cell.Name)
		}
	}

	// Validate Unix socket path length limits
	if err := p.validateUnixSocketPathLength(typedConfig); err != nil {
		return err
	}

	return nil
}

// UnixPathMax returns the maximum Unix socket path length for the current platform.
func UnixPathMax() int {
	var addr syscall.RawSockaddrUnix
	return len(addr.Path)
}

// validateUnixSocketPathLength validates that Unix socket paths won't exceed system limits
func (p *localProvisioner) validateUnixSocketPathLength(config *LocalProvisionerConfig) error {
	maxSocketPathLength := UnixPathMax()

	// Convert root working dir to absolute path for accurate length calculation
	absRootWorkingDir, err := filepath.Abs(config.RootWorkingDir)
	if err != nil {
		return fmt.Errorf("failed to convert root working dir to absolute path: %w", err)
	}

	// Calculate the maximum possible path length for Unix sockets
	// Path structure: <rootWorkingDir>/data/pooler_<serviceID>/pg_sockets/.s.PGSQL.5432
	// We use a worst-case service ID length (8 chars) to be safe
	maxServiceIDLength := 8
	worstCasePoolerSocketPath := []string{
		"data",
		fmt.Sprintf("pooler_%s", strings.Repeat("x", maxServiceIDLength)),
		"pg_sockets",
		".s.PGSQL.5432",
	}
	worstCaseCurrentSocketPath := filepath.Join(append([]string{absRootWorkingDir}, worstCasePoolerSocketPath...)...)

	worstCaseProposedSocketPath := filepath.Join(append([]string{"/tmp/mt"}, worstCasePoolerSocketPath...)...)

	if len(worstCaseCurrentSocketPath) > maxSocketPathLength {
		return fmt.Errorf("unix socket path would exceed system limit (%d bytes): %s\n\n"+
			"To fix this issue:\n"+
			"1. Initialize multigres from a directory with a shorter path\n"+
			"2. Provide config-path to multigres (--config-path target_dir) that has a shorter length\n\n"+
			"Example:\n"+
			"  Current: multigres cluster init --config-path %s\n"+
			"  Better:  multigres cluster init --config-path /tmp/mt/\n\n"+
			"This will generate socket paths like:\n"+
			"  %s (%d bytes)\n\n"+
			"Current path length: %d bytes (limit: %d bytes)",
			maxSocketPathLength, worstCaseCurrentSocketPath, config.RootWorkingDir, worstCaseProposedSocketPath, len(worstCaseProposedSocketPath), len(worstCaseCurrentSocketPath), maxSocketPathLength)
	}

	return nil
}

// validateBinaryPaths validates that all configured binary paths exist and are executable
func (p *localProvisioner) validateBinaryPaths(config *LocalProvisionerConfig) error {
	var errors []string

	// Validate global service binaries
	if config.Multiadmin.Path != "" {
		if err := p.validateBinaryExists(config.Multiadmin.Path, "multiadmin"); err != nil {
			errors = append(errors, err.Error())
		}
	}

	// Validate cell service binaries
	for cellName, cellConfig := range config.Cells {
		// Validate multigateway
		if cellConfig.Multigateway.Path != "" {
			if err := p.validateBinaryExists(cellConfig.Multigateway.Path, fmt.Sprintf("multigateway (cell %s)", cellName)); err != nil {
				errors = append(errors, err.Error())
			}
		}

		// Validate multipooler
		if cellConfig.Multipooler.Path != "" {
			if err := p.validateBinaryExists(cellConfig.Multipooler.Path, fmt.Sprintf("multipooler (cell %s)", cellName)); err != nil {
				errors = append(errors, err.Error())
			}
		}

		// Validate multiorch
		if cellConfig.Multiorch.Path != "" {
			if err := p.validateBinaryExists(cellConfig.Multiorch.Path, fmt.Sprintf("multiorch (cell %s)", cellName)); err != nil {
				errors = append(errors, err.Error())
			}
		}

		// Validate pgctld
		if cellConfig.Pgctld.Path != "" {
			if err := p.validateBinaryExists(cellConfig.Pgctld.Path, fmt.Sprintf("pgctld (cell %s)", cellName)); err != nil {
				errors = append(errors, err.Error())
			}
		}
	}

	if len(errors) > 0 {
		return fmt.Errorf("binary validation failed:\n%s", strings.Join(errors, "\n"))
	}

	return nil
}

// validateBinaryExists checks if a binary path exists and is executable
func (p *localProvisioner) validateBinaryExists(binaryPath, serviceName string) error {
	// Convert to absolute path if it's relative
	var fullPath string
	if filepath.IsAbs(binaryPath) {
		fullPath = binaryPath
	} else {
		// Make it relative to current directory
		fullPath = filepath.Join(".", binaryPath)
	}

	// Check if the binary exists and is executable
	info, err := os.Stat(fullPath)
	if err != nil {
		return fmt.Errorf("  %s binary not found at %s: %w", serviceName, binaryPath, err)
	}

	if info.IsDir() {
		return fmt.Errorf("  %s path is a directory, not a binary: %s", serviceName, binaryPath)
	}

	// Check if it's executable (on Unix systems)
	if info.Mode()&0o111 == 0 {
		return fmt.Errorf("  %s binary is not executable: %s", serviceName, binaryPath)
	}

	return nil
}

// validateSystemBinaries validates that required system binaries are available in PATH
func (p *localProvisioner) validateSystemBinaries() error {
	requiredBinaries := []string{
		"etcd",
		"pg_ctl",
		"postgres",
		"pg_isready",
	}

	var missingBinaries []string

	for _, binary := range requiredBinaries {
		if _, err := exec.LookPath(binary); err != nil {
			missingBinaries = append(missingBinaries, binary)
		}
	}

	if len(missingBinaries) > 0 {
		return fmt.Errorf("required system binaries not found in PATH: %s\n\n"+
			"Please ensure PostgreSQL and etcd are installed and available in your PATH.\n"+
			"For PostgreSQL: Install PostgreSQL client tools (pg_ctl, postgres, pg_isready)\n"+
			"For etcd: Install etcd client binary",
			strings.Join(missingBinaries, ", "))
	}

	return nil
}

// NewLocalProvisioner creates a new local provisioner instance
func NewLocalProvisioner() (provisioner.Provisioner, error) {
	p := &localProvisioner{
		config: &LocalProvisionerConfig{},
	}

	return p, nil
}

// buildBootstrapResources creates the resource list for bootstrap (etcd, cells, multiadmin)
func (p *localProvisioner) buildBootstrapResources() ([]Resource, error) {
	var resources []Resource

	// 1. Global topology (etcd)
	etcdResource := NewGlobalTopoResource("etcd", &p.config.Etcd)
	resources = append(resources, etcdResource)

	// 2. Cell topology resources (one per cell)
	for _, cellConfig := range p.config.Topology.Cells {
		cellResource := NewCellTopoResource(cellConfig.Name, &cellConfig)
		resources = append(resources, cellResource)
	}

	// 3. Multiadmin (global admin service)
	multiadminResource := NewMultiadminResource("multiadmin", &p.config.Multiadmin)
	resources = append(resources, multiadminResource)

	return resources, nil
}

// buildDatabaseResourcesFromState creates the resource list for a database by reading state files
// This is used for deprovisioning to ensure we clean up all services that are actually running,
// regardless of what's in the configuration
func (p *localProvisioner) buildDatabaseResourcesFromState(databaseName string) ([]Resource, error) {
	var resources []Resource

	// Load all services from state for this database
	services, err := p.loadDbProvisionedServices(databaseName)
	if err != nil {
		return nil, fmt.Errorf("failed to load services from state: %w", err)
	}

	// Create resource objects for each service found in state
	for _, service := range services {
		// Extract metadata
		cell, _ := service.Metadata["cell"].(string)
		if cell == "" {
			fmt.Printf("Warning: service %s has no cell metadata, skipping\n", service.ID)
			continue
		}

		// Get cell config - for deprovisioning we need minimal config
		cellServices, ok := p.config.Cells[cell]
		if !ok {
			// If cell doesn't exist in config, create minimal config for deprovisioning
			// This can happen if services were created with a different config
			cellServices = CellServicesConfig{}
		}

		// Create the appropriate resource based on service type
		switch service.Service {
		case "multigateway":
			resource := NewMultigatewayResource(cell, databaseName, service.ID, &cellServices.Multigateway)
			resources = append(resources, resource)

		case "multipooler":
			resource := NewMultipoolerResource(cell, databaseName, service.ID, &cellServices.Multipooler, &cellServices.Pgctld)
			resources = append(resources, resource)

		case "multiorch":
			resource := NewMultiorchResource(cell, databaseName, service.ID, &cellServices.Multiorch)
			resources = append(resources, resource)

		case "pgctld":
			// Pgctld is handled by multipooler resource, skip standalone pgctld entries
			continue

		default:
			fmt.Printf("Warning: unknown service type %s for service %s, skipping\n", service.Service, service.ID)
		}
	}

	// Add database resource for topology cleanup
	// Note: For deprovisioning, we don't need full database config
	if len(resources) > 0 {
		databaseConfig := &DatabaseConfig{
			Name: databaseName,
		}
		databaseResource := NewDatabaseResource(databaseConfig)
		resources = append(resources, databaseResource)
	}

	return resources, nil
}

// buildDatabaseResources creates the resource list for a database (database + services in all cells)
func (p *localProvisioner) buildDatabaseResources(databaseName string) ([]Resource, error) {
	var resources []Resource

	// Get cell names
	cellNames, err := p.getCellNames()
	if err != nil {
		return nil, fmt.Errorf("failed to get cell names: %w", err)
	}

	// 1. Database resource (registers database in topology)
	databaseConfig := &DatabaseConfig{
		Name:             databaseName,
		BackupLocation:   "",
		DurabilityPolicy: "none",
		Cells:            cellNames,
	}
	databaseResource := NewDatabaseResource(databaseConfig)
	resources = append(resources, databaseResource)

	// 2. Cell services (multigateway, multipooler, multiorch per cell)
	for _, cellName := range cellNames {
		cellServices, ok := p.config.Cells[cellName]
		if !ok {
			return nil, fmt.Errorf("no configuration found for cell %s", cellName)
		}

		// Generate service IDs
		multigatewayID := fmt.Sprintf("multigateway-%s-%s", cellName, stringutil.RandomString(8))
		multipoolerID := cellServices.Multipooler.ServiceID
		if multipoolerID == "" {
			multipoolerID = stringutil.RandomString(8)
		}
		multiorchID := fmt.Sprintf("multiorch-%s-%s", cellName, stringutil.RandomString(8))

		// Multigateway
		multigatewayResource := NewMultigatewayResource(cellName, databaseName, multigatewayID, &cellServices.Multigateway)
		resources = append(resources, multigatewayResource)

		// Multipooler (includes pgctld)
		multipoolerResource := NewMultipoolerResource(cellName, databaseName, multipoolerID, &cellServices.Multipooler, &cellServices.Pgctld)
		resources = append(resources, multipoolerResource)

		// Multiorch
		multiorchResource := NewMultiorchResource(cellName, databaseName, multiorchID, &cellServices.Multiorch)
		resources = append(resources, multiorchResource)
	}

	return resources, nil
}

// buildAllBootstrapResources creates ALL resources for bootstrap (infrastructure + default database)
func (p *localProvisioner) buildAllBootstrapResources() ([]Resource, error) {
	var resources []Resource

	// 1. Global topology (etcd)
	etcdResource := NewGlobalTopoResource("etcd", &p.config.Etcd)
	resources = append(resources, etcdResource)

	// 2. Cell topology resources (one per cell)
	for _, cellConfig := range p.config.Topology.Cells {
		cellResource := NewCellTopoResource(cellConfig.Name, &cellConfig)
		resources = append(resources, cellResource)
	}

	// 3. Multiadmin (global admin service)
	multiadminResource := NewMultiadminResource("multiadmin", &p.config.Multiadmin)
	resources = append(resources, multiadminResource)

	// 4. Default database + all its services
	defaultDBName, err := p.getDefaultDatabaseName()
	if err != nil {
		return nil, fmt.Errorf("failed to get default database name: %w", err)
	}

	dbResources, err := p.buildDatabaseResources(defaultDBName)
	if err != nil {
		return nil, fmt.Errorf("failed to build database resources: %w", err)
	}
	resources = append(resources, dbResources...)

	return resources, nil
}

// BootstrapV2 bootstraps the entire cluster using the new resource-based architecture
// Single call provisions everything: etcd → cells → multiadmin → database → all cell services
// The engine handles dependency ordering and parallel execution automatically
func (p *localProvisioner) BootstrapV2(ctx context.Context, failFast bool) ([]*provisioner.ProvisionResult, error) {
	// Validate binary paths before starting
	if err := p.validateBinaryPaths(p.config); err != nil {
		return nil, err
	}

	// Validate required system binaries
	if err := p.validateSystemBinaries(); err != nil {
		return nil, err
	}

	fmt.Println("=== Bootstrapping Multigres cluster ===")
	fmt.Println("")

	// Build ALL resources (everything needed for a working cluster)
	resources, err := p.buildAllBootstrapResources()
	if err != nil {
		return nil, fmt.Errorf("failed to build resources: %w", err)
	}

	// Create provisioning context and engine
	pctx := newProvisionContext(p)
	engine := NewProvisionerEngine(pctx, failFast)

	// Single call provisions everything - engine handles dependency ordering and parallelism
	results, err := engine.ProvisionResources(ctx, resources)
	if err != nil {
		return nil, fmt.Errorf("bootstrap failed: %w", err)
	}

	return results, nil
}

// CleanupOrphans detects and removes orphaned services
func (p *localProvisioner) CleanupOrphans(ctx context.Context, dryRun bool) error {
	fmt.Println("=== Detecting orphaned services ===")

	// Get all running services from state files
	pctx := newProvisionContext(p)
	allStates, err := pctx.ListStates()
	if err != nil {
		return fmt.Errorf("failed to list states: %w", err)
	}

	// Build all expected resources (what should be running)
	allExpectedResources, err := p.buildAllBootstrapResources()
	if err != nil {
		return fmt.Errorf("failed to build expected resources: %w", err)
	}

	// Detect orphans
	result := DetectOrphans(allStates, allExpectedResources)

	if result.TotalOrphans == 0 {
		fmt.Println("✓ No orphaned services found")
		return nil
	}

	fmt.Printf("Found %d orphaned service(s):\n", result.TotalOrphans)
	for _, orphan := range result.OrphanResources {
		fmt.Printf("  - %s (ID: %s, PID: %d)\n", orphan.Service, orphan.ID, orphan.PID)
	}

	if dryRun {
		fmt.Println("\nDry run mode - no services will be stopped")
		return nil
	}

	// Stop orphaned processes and delete state
	fmt.Println("\nCleaning up orphaned services...")
	for _, orphan := range result.OrphanResources {
		resourceID := orphan.toResourceID()

		fmt.Printf("  - Stopping %s (PID: %d)\n", orphan.Service, orphan.PID)

		if orphan.PID > 0 {
			if err := pctx.StopProcess(orphan.PID); err != nil {
				fmt.Printf("    Warning: failed to stop process %d: %v\n", orphan.PID, err)
			}
		}

		// Delete state file
		if err := pctx.DeleteState(resourceID); err != nil {
			fmt.Printf("    Warning: failed to delete state for %s: %v\n", orphan.ID, err)
		}
	}

	fmt.Printf("\n✓ Cleaned up %d orphaned service(s)\n", result.TotalOrphans)
	return nil
}

func getExecutablePath() (string, error) {
	executablePath, err := os.Executable()
	if err != nil {
		executablePath, err = os.Getwd()
	}
	return filepath.Dir(executablePath), err
}

func init() {
	// Register the local provisioner
	provisioner.RegisterProvisioner("local", NewLocalProvisioner)

	// Add the executable directory to the PATH. We're expecting
	// to find the other executables in the same directory.
	if binDir, err := getExecutablePath(); err == nil {
		pathutil.PrependPath(binDir)
	} else {
		slog.Error(fmt.Sprintf("Local Provisioner failed to get executable path: %v", err))
		os.Exit(1)
	}
}
