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
	"errors"
	"fmt"

	"github.com/multigres/multigres/go/clustermetadata/topo"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/provisioner"
)

// CellTopoResource represents a cell topology resource
type CellTopoResource struct {
	id         *clustermetadatapb.ID
	cellName   string
	cellConfig *CellConfig
}

// NewCellTopoResource creates a new cell topology resource
func NewCellTopoResource(cellName string, cellConfig *CellConfig) *CellTopoResource {
	return &CellTopoResource{
		id: &clustermetadatapb.ID{
			Component: clustermetadatapb.ID_CELL_TOPO,
			Cell:      cellName,
			Name:      cellName, // Use cell name as the resource name
		},
		cellName:   cellName,
		cellConfig: cellConfig,
	}
}

// ID returns the resource ID
func (r *CellTopoResource) ID() *clustermetadatapb.ID {
	return r.id
}

// Dependencies returns the resources this resource depends on (global topology)
func (r *CellTopoResource) Dependencies() []*clustermetadatapb.ID {
	// Cell topology depends on global topology (implicit, handled by engine)
	return []*clustermetadatapb.ID{}
}

// Provision creates the cell in the topology server
func (r *CellTopoResource) Provision(ctx context.Context, pctx ProvisionContext) (*provisioner.ProvisionResult, error) {
	// Open global topology
	ts, err := pctx.OpenGlobalTopo(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to global topology: %w", err)
	}
	defer ts.Close()

	// Check if cell already exists
	_, err = ts.GetCell(ctx, r.cellName)
	if err == nil {
		// Cell already exists, nothing to do
		return &provisioner.ProvisionResult{
			ServiceName: "cell",
			FQDN:        "",
			Ports:       map[string]int{},
			Metadata: map[string]any{
				"cell": r.cellName,
			},
		}, nil
	}

	// Create the cell if it doesn't exist
	if errors.Is(err, &topo.TopoError{Code: topo.NoNode}) {
		etcdAddress := pctx.EtcdClientAddress()

		cellConfig := &clustermetadatapb.Cell{
			Name:            r.cellName,
			ServerAddresses: []string{etcdAddress},
			Root:            r.cellConfig.RootPath,
		}

		if err := ts.CreateCell(ctx, r.cellName, cellConfig); err != nil {
			return nil, fmt.Errorf("failed to create cell '%s': %w", r.cellName, err)
		}

		return &provisioner.ProvisionResult{
			ServiceName: "cell",
			FQDN:        "",
			Ports:       map[string]int{},
			Metadata: map[string]any{
				"cell": r.cellName,
			},
		}, nil
	}

	// Some other error occurred
	return nil, fmt.Errorf("failed to check cell '%s': %w", r.cellName, err)
}

// Deprovision removes the cell from the topology (currently a no-op as we don't delete cells)
func (r *CellTopoResource) Deprovision(ctx context.Context, pctx ProvisionContext) error {
	// We don't delete cells from topology during deprovision
	// This is a no-op
	return nil
}
