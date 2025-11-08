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

	"github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/provisioner"
)

// MultigatewayResource represents a multigateway service in a cell.
type MultigatewayResource struct {
	provisioner  *localProvisioner
	databaseName string
	cellName     string
}

// NewMultigatewayResource creates a new multigateway resource.
func NewMultigatewayResource(p *localProvisioner, databaseName, cellName string) (*MultigatewayResource, error) {
	return &MultigatewayResource{
		provisioner:  p,
		databaseName: databaseName,
		cellName:     cellName,
	}, nil
}

// ID returns the resource ID for multigateway.
func (m *MultigatewayResource) ID() *clustermetadata.ID {
	return &clustermetadata.ID{
		Component: clustermetadata.ID_MULTIGATEWAY,
		Cell:      m.cellName,
		Name:      fmt.Sprintf("multigateway-%s-%s", m.databaseName, m.cellName),
	}
}

// DisplayName returns the display name for multigateway.
func (m *MultigatewayResource) DisplayName() string {
	return "Multigateway"
}

// Discover checks if multigateway is already running.
func (m *MultigatewayResource) Discover(ctx context.Context, provCtx *ProvisionContext) (ResourceStatus, error) {
	existingService, err := m.provisioner.findRunningDbService("multigateway", m.databaseName, m.cellName)
	if err != nil {
		return ResourceStatus{}, fmt.Errorf("failed to check for existing multigateway: %w", err)
	}

	if existingService != nil {
		return ResourceStatus{
			State: StateDiscovered,
			Metadata: map[string]any{
				"pid":   existingService.PID,
				"ports": existingService.Ports,
				"fqdn":  existingService.FQDN,
			},
			Message: fmt.Sprintf("multigateway already running (PID %d)", existingService.PID),
		}, nil
	}

	return ResourceStatus{
		State:   StateNotFound,
		Message: "multigateway not running",
	}, nil
}

// Provision starts the multigateway service using the existing provisioner method.
func (m *MultigatewayResource) Provision(ctx context.Context, provCtx *ProvisionContext) (ResourceStatus, error) {
	// Use provision context for topology connection info
	req := &provisioner.ProvisionRequest{
		Service:      "multigateway",
		DatabaseName: m.databaseName,
		Params: map[string]any{
			"etcd_address":     provCtx.EtcdAddress,
			"topo_backend":     provCtx.TopoBackend,
			"topo_global_root": provCtx.TopoGlobalRoot,
			"cell":             m.cellName,
		},
	}

	result, err := m.provisioner.provisionMultigateway(ctx, req)
	if err != nil {
		return ResourceStatus{}, err
	}

	return ResourceStatus{
		State: StateProvisioned,
		Metadata: map[string]any{
			"ports": result.Ports,
			"fqdn":  result.FQDN,
		},
		Message: "multigateway started successfully",
	}, nil
}

// Deprovision stops the multigateway service.
func (m *MultigatewayResource) Deprovision(ctx context.Context, status ResourceStatus) error {
	// Deprovisioning handled by existing cleanup logic in Teardown
	return nil
}

// Dependencies returns no explicit dependencies (etcd is implicit).
func (m *MultigatewayResource) Dependencies() []*clustermetadata.ID {
	return nil
}

// Children returns no children.
func (m *MultigatewayResource) Children() []Resource {
	return nil
}
