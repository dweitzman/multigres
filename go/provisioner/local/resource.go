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
	"sync"
	"syscall"
	"time"

	"github.com/multigres/multigres/go/clustermetadata/topo"
	"github.com/multigres/multigres/go/pb/clustermetadata"
)

// ProvisionContext provides common services and information needed during provisioning.
type ProvisionContext struct {
	// EtcdAddress is the address of the running etcd service (e.g., "localhost:2379")
	EtcdAddress string

	// TopoBackend is the topology backend type (e.g., "etcd2")
	TopoBackend string

	// TopoGlobalRoot is the root path in the topology for global data
	TopoGlobalRoot string

	// CellName is the name of the cell this resource belongs to (empty for global resources)
	CellName string
}

// OpenGlobalTopology opens a connection to the global topology server.
// The caller is responsible for closing the returned topology store.
func (pc *ProvisionContext) OpenGlobalTopology(ctx context.Context) (topo.Store, error) {
	if pc.EtcdAddress == "" {
		return nil, fmt.Errorf("etcd address not available in provision context")
	}
	if pc.TopoBackend == "" {
		return nil, fmt.Errorf("topology backend not configured")
	}
	if pc.TopoGlobalRoot == "" {
		return nil, fmt.Errorf("topology global root not configured")
	}

	ts, err := topo.OpenServer(pc.TopoBackend, pc.TopoGlobalRoot, []string{pc.EtcdAddress})
	if err != nil {
		return nil, fmt.Errorf("failed to connect to global topology server: %w", err)
	}

	return ts, nil
}

// OpenCellTopology opens a connection to the cell's topology server.
// This requires CellName to be set in the context.
// The caller is responsible for closing the returned topology store.
func (pc *ProvisionContext) OpenCellTopology(ctx context.Context) (topo.Store, error) {
	if pc.CellName == "" {
		return nil, fmt.Errorf("cell name not set in provision context")
	}

	// For now, cell topology uses the same backend as global topology
	// In the future, this could be extended to support different backends per cell
	return pc.OpenGlobalTopology(ctx)
}

// ProcessProvisionResult contains the result of starting a process.
type ProcessProvisionResult struct {
	PID      int
	Ports    map[string]int
	FQDN     string
	Metadata map[string]any
}

// ProcessResource provides a generic implementation for resources that manage a single process.
// It handles discovery via state files, provisioning via a command starter, and deprovisioning.
type ProcessResource struct {
	id           *clustermetadata.ID
	displayName  string
	serviceName  string
	provisioner  *localProvisioner
	databaseName string // empty for global services

	// startCommand is called to start the process and return the result
	startCommand func(ctx context.Context, provCtx *ProvisionContext) (*ProcessProvisionResult, error)
}

// NewProcessResource creates a new process resource.
func NewProcessResource(
	id *clustermetadata.ID,
	displayName string,
	serviceName string,
	provisioner *localProvisioner,
	databaseName string,
	startCommand func(ctx context.Context, provCtx *ProvisionContext) (*ProcessProvisionResult, error),
) *ProcessResource {
	return &ProcessResource{
		id:           id,
		displayName:  displayName,
		serviceName:  serviceName,
		provisioner:  provisioner,
		databaseName: databaseName,
		startCommand: startCommand,
	}
}

// ID returns the resource ID.
func (p *ProcessResource) ID() *clustermetadata.ID {
	return p.id
}

// DisplayName returns the display name.
func (p *ProcessResource) DisplayName() string {
	return p.displayName
}

// Discover checks if the process is already running by loading state.
func (p *ProcessResource) Discover(ctx context.Context, provCtx *ProvisionContext) (ResourceStatus, error) {
	var existingService *LocalProvisionedService
	var err error

	if p.databaseName == "" {
		existingService, err = p.provisioner.findRunningService(p.serviceName)
	} else {
		existingService, err = p.provisioner.findRunningDbService(p.serviceName, p.databaseName, provCtx.CellName)
	}

	if err != nil {
		return ResourceStatus{}, fmt.Errorf("failed to check for existing %s: %w", p.serviceName, err)
	}

	if existingService != nil {
		return ResourceStatus{
			State: StateDiscovered,
			Metadata: map[string]any{
				"pid":   existingService.PID,
				"ports": existingService.Ports,
				"fqdn":  existingService.FQDN,
			},
			Message: fmt.Sprintf("%s already running (PID %d)", p.serviceName, existingService.PID),
		}, nil
	}

	return ResourceStatus{
		State:   StateNotFound,
		Message: fmt.Sprintf("%s not running", p.serviceName),
	}, nil
}

// Provision starts the process using the provided start function.
func (p *ProcessResource) Provision(ctx context.Context, provCtx *ProvisionContext) (ResourceStatus, error) {
	result, err := p.startCommand(ctx, provCtx)
	if err != nil {
		return ResourceStatus{}, err
	}

	return ResourceStatus{
		State: StateProvisioned,
		Metadata: map[string]any{
			"pid":   result.PID,
			"ports": result.Ports,
			"fqdn":  result.FQDN,
		},
		Message: fmt.Sprintf("%s started successfully", p.serviceName),
	}, nil
}

// Deprovision terminates the process.
func (p *ProcessResource) Deprovision(ctx context.Context, status ResourceStatus) error {
	pid, ok := status.Metadata["pid"].(int)
	if !ok {
		return fmt.Errorf("PID not found in status metadata for %s", p.serviceName)
	}

	// Try to terminate gracefully first
	if err := syscall.Kill(pid, syscall.SIGTERM); err != nil {
		if err != syscall.ESRCH {
			return fmt.Errorf("failed to send SIGTERM to %s (PID %d): %w", p.serviceName, pid, err)
		}
		return nil
	}

	// Wait briefly for graceful shutdown
	time.Sleep(2 * time.Second)

	// Check if process is still running
	if err := syscall.Kill(pid, 0); err == nil {
		// Process still running, force kill
		if err := syscall.Kill(pid, syscall.SIGKILL); err != nil && err != syscall.ESRCH {
			return fmt.Errorf("failed to force kill %s (PID %d): %w", p.serviceName, pid, err)
		}
	}

	return nil
}

// Dependencies returns no explicit dependencies by default.
func (p *ProcessResource) Dependencies() []*clustermetadata.ID {
	return nil
}

// Children returns no children by default.
func (p *ProcessResource) Children() []Resource {
	return nil
}

// Resource represents a provisionable component with lifecycle management.
// Resources form a hierarchy and can have explicit dependencies on other resources.
type Resource interface {
	// ID returns the unique identifier for this resource using the clustermetadata.ID proto type.
	ID() *clustermetadata.ID

	// DisplayName returns a human-readable name for this resource, used in status output.
	DisplayName() string

	// Discover checks if the resource already exists and returns its current status.
	// This should be idempotent and relatively fast. If the resource exists and is
	// functioning correctly, return StateDiscovered. If it doesn't exist, return StateNotFound.
	Discover(ctx context.Context, provCtx *ProvisionContext) (ResourceStatus, error)

	// Provision creates the resource. This is only called if Discover returned StateNotFound.
	// The implementation can assume that all dependencies have already been satisfied.
	// Returns the final status after provisioning completes.
	// The ProvisionContext provides access to topology services and common configuration.
	Provision(ctx context.Context, provCtx *ProvisionContext) (ResourceStatus, error)

	// Deprovision removes the resource. It receives the current status (from either Discover
	// or Provision) which may contain metadata needed for cleanup (e.g., PIDs, file paths).
	Deprovision(ctx context.Context, status ResourceStatus) error

	// Dependencies returns the IDs of resources that must complete successfully before
	// this resource can be provisioned. Dependencies must be on resources that appear
	// earlier in a traversal of the resource hierarchy.
	Dependencies() []*clustermetadata.ID

	// Children returns sub-resources that belong to this resource. This forms the
	// hierarchy used for display and management. Children are provisioned after their
	// parent completes.
	Children() []Resource
}

// ResourceStatus represents the state of a resource at a point in time.
type ResourceStatus struct {
	// State is the current lifecycle state of the resource
	State ResourceState

	// Metadata contains resource-specific information such as ports, addresses, PIDs, etc.
	// The exact contents depend on the resource type.
	Metadata map[string]any

	// Message is an optional human-readable message providing additional context
	Message string

	// Error is set if the State is StateFailed
	Error error

	// Timestamp is when this status was recorded
	Timestamp time.Time
}

// ResourceState represents the lifecycle state of a resource.
type ResourceState int

const (
	// StateUnknown indicates the resource state is unknown or uninitialized
	StateUnknown ResourceState = iota

	// StateNotFound indicates the resource doesn't exist yet and needs provisioning
	StateNotFound

	// StateDiscovered indicates the resource already exists and was found during discovery
	StateDiscovered

	// StateProvisioning indicates the resource is currently being created
	StateProvisioning

	// StateProvisioned indicates the resource was successfully created during this run
	StateProvisioned

	// StateFailed indicates provisioning failed
	StateFailed

	// StateDeprovisioning indicates the resource is currently being deleted
	StateDeprovisioning

	// StateDeprovisioned indicates the resource was successfully deleted
	StateDeprovisioned
)

// String returns a string representation of the resource state.
func (s ResourceState) String() string {
	switch s {
	case StateUnknown:
		return "Unknown"
	case StateNotFound:
		return "NotFound"
	case StateDiscovered:
		return "Discovered"
	case StateProvisioning:
		return "Provisioning"
	case StateProvisioned:
		return "Provisioned"
	case StateFailed:
		return "Failed"
	case StateDeprovisioning:
		return "Deprovisioning"
	case StateDeprovisioned:
		return "Deprovisioned"
	default:
		return "Unknown"
	}
}

// ResourceNode wraps a Resource with runtime execution state.
// This is used internally by the orchestrator to track provisioning progress.
type ResourceNode struct {
	// Resource is the underlying resource being managed
	Resource Resource

	// Status is the current status of this resource
	Status ResourceStatus

	// Children are the child resource nodes in the hierarchy
	Children []*ResourceNode

	// mu protects concurrent access to Status
	mu sync.RWMutex

	// started is closed when this resource begins execution
	started chan struct{}

	// completed is closed when this resource finishes execution (success or failure)
	completed chan struct{}
}

// NewResourceNode creates a new ResourceNode wrapping the given resource.
func NewResourceNode(resource Resource) *ResourceNode {
	return &ResourceNode{
		Resource:  resource,
		Status:    ResourceStatus{State: StateUnknown, Timestamp: time.Now()},
		started:   make(chan struct{}),
		completed: make(chan struct{}),
	}
}

// GetStatus safely retrieves the current status.
func (n *ResourceNode) GetStatus() ResourceStatus {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.Status
}

// SetStatus safely updates the current status.
func (n *ResourceNode) SetStatus(status ResourceStatus) {
	n.mu.Lock()
	defer n.mu.Unlock()
	status.Timestamp = time.Now()
	n.Status = status
}

// ErrorStrategy defines how the orchestrator handles errors during provisioning.
type ErrorStrategy int

const (
	// ErrorStrategyFailFast stops all provisioning immediately on the first error.
	// This is the default and safest strategy.
	ErrorStrategyFailFast ErrorStrategy = iota

	// ErrorStrategyContinueIndependent stops resources that depend on failed resources,
	// but continues provisioning resources that have no dependency relationship with
	// the failed resource.
	ErrorStrategyContinueIndependent

	// ErrorStrategyBestEffort continues provisioning all resources that don't have
	// a direct dependency on failed resources, attempting to provision as much as possible.
	ErrorStrategyBestEffort
)

// String returns a string representation of the error strategy.
func (e ErrorStrategy) String() string {
	switch e {
	case ErrorStrategyFailFast:
		return "FailFast"
	case ErrorStrategyContinueIndependent:
		return "ContinueIndependent"
	case ErrorStrategyBestEffort:
		return "BestEffort"
	default:
		return "Unknown"
	}
}
