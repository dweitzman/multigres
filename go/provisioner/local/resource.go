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

	"github.com/multigres/multigres/go/clustermetadata/topo"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/provisioner"
)

// Resource represents a provisionable resource in the Multigres cluster.
// Each resource type (etcd, cell, multipooler, etc.) implements this interface.
type Resource interface {
	// Provision creates or reuses the resource. If the resource already exists
	// and is running, it should return the existing state. Otherwise, it should
	// start the resource and return its state.
	Provision(ctx context.Context, pctx ProvisionContext) (*provisioner.ProvisionResult, error)

	// Deprovision stops and cleans up the resource. It should gracefully
	// shutdown any running processes and clean up associated state.
	Deprovision(ctx context.Context, pctx ProvisionContext) error

	// Dependencies returns the resource IDs that must be provisioned before
	// this resource can be provisioned. Returns an empty slice if the resource
	// has no explicit dependencies (implicit dependencies like global topology
	// are handled by the provisioner engine).
	Dependencies() []*clustermetadatapb.ID

	// ID returns this resource's unique identifier
	ID() *clustermetadatapb.ID
}

// ProvisionPhase represents the logical phase of provisioning for status display.
// The phase is derived from the resource's ID (ComponentType and cell).
type ProvisionPhase int

const (
	// PhaseTopologySetup includes global topology (etcd) and cell topology setup
	PhaseTopologySetup ProvisionPhase = iota
	// PhaseGlobalServices includes global services like multiadmin and database registration
	PhaseGlobalServices
	// PhaseCellServices includes cell-scoped services like multipooler, multigateway, multiorch
	PhaseCellServices
)

// String returns the human-readable name of the phase
func (p ProvisionPhase) String() string {
	switch p {
	case PhaseTopologySetup:
		return "Topology Setup"
	case PhaseGlobalServices:
		return "Global Services"
	case PhaseCellServices:
		return "Cell Services"
	default:
		return "Unknown"
	}
}

// GetPhase determines the provisioning phase from a resource ID
func GetPhase(id *clustermetadatapb.ID) ProvisionPhase {
	// Topology setup: GLOBAL_TOPO and CELL_TOPO component types
	if id.Component == clustermetadatapb.ID_GLOBAL_TOPO || id.Component == clustermetadatapb.ID_CELL_TOPO {
		return PhaseTopologySetup
	}

	// Global services: MULTIADMIN and DATABASE component types
	if id.Component == clustermetadatapb.ID_MULTIADMIN || id.Component == clustermetadatapb.ID_DATABASE {
		return PhaseGlobalServices
	}

	// Cell services: MULTIPOOLER, MULTIGATEWAY, MULTIORCH
	return PhaseCellServices
}

// ProvisionContext provides shared functionality to all resource provisioners.
// It encapsulates common operations like state management, topology access,
// configuration access, and path generation.
type ProvisionContext interface {
	// State management
	ReadState(resourceID *clustermetadatapb.ID) (*LocalProvisionedService, error)
	SaveState(state *LocalProvisionedService) error
	DeleteState(resourceID *clustermetadatapb.ID) error
	ListStates() ([]*LocalProvisionedService, error)

	// Topology access - returns the topology store for global operations
	OpenGlobalTopo(ctx context.Context) (topo.Store, error)

	// Configuration
	GetConfig() *LocalProvisionerConfig

	// Path helpers
	LogPath(serviceName, serviceID, databaseName string) (string, error)
	DataPath(serviceName, serviceID string) string
	StatePath(resourceID *clustermetadatapb.ID) string

	// Binary resolution
	FindBinary(name string, configuredPath string) (string, error)

	// Etcd connection info
	EtcdClientAddress() string
	EtcdPeerAddress() string

	// Process management helpers
	ValidateProcessRunning(pid int) error
	StopProcess(pid int) error

	// Health check helper
	WaitForServiceReady(ctx context.Context, serviceName string, host string, servicePorts map[string]int) error
}

// ResourceStatus represents the current status of a resource
type ResourceStatus int

const (
	// StatusPending means the resource has not started provisioning yet
	StatusPending ResourceStatus = iota
	// StatusProvisioning means the resource is currently being provisioned
	StatusProvisioning
	// StatusReady means the resource has been successfully provisioned
	StatusReady
	// StatusFailed means the resource failed to provision
	StatusFailed
	// StatusDeprovisioning means the resource is being deprovisioned
	StatusDeprovisioning
)

// String returns the human-readable name of the status
func (s ResourceStatus) String() string {
	switch s {
	case StatusPending:
		return "Pending"
	case StatusProvisioning:
		return "Provisioning"
	case StatusReady:
		return "Ready"
	case StatusFailed:
		return "Failed"
	case StatusDeprovisioning:
		return "Deprovisioning"
	default:
		return "Unknown"
	}
}
