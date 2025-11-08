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
	"strings"

	"github.com/multigres/multigres/go/pb/clustermetadata"
)

// CellResource represents a grouping of services within a cell for a specific database.
type CellResource struct {
	provisioner  *localProvisioner
	databaseName string
	cellName     string
	children     []Resource
}

// NewCellResource creates a new cell resource with all cell services.
func NewCellResource(p *localProvisioner, databaseName, cellName string) (*CellResource, error) {
	c := &CellResource{
		provisioner:  p,
		databaseName: databaseName,
		cellName:     cellName,
	}

	// Create multigateway resource
	multigateway, err := NewMultigatewayResource(p, databaseName, cellName)
	if err != nil {
		return nil, fmt.Errorf("failed to create multigateway resource: %w", err)
	}
	c.children = append(c.children, multigateway)

	// Create multipooler resource (starts pgctld internally)
	multipooler, err := NewMultipoolerResource(p, databaseName, cellName)
	if err != nil {
		return nil, fmt.Errorf("failed to create multipooler resource: %w", err)
	}
	c.children = append(c.children, multipooler)

	// Create multiorch resource
	multiorch, err := NewMultiorchResource(p, databaseName, cellName)
	if err != nil {
		return nil, fmt.Errorf("failed to create multiorch resource: %w", err)
	}
	c.children = append(c.children, multiorch)

	return c, nil
}

// ID returns the resource ID for the cell.
func (c *CellResource) ID() *clustermetadata.ID {
	return &clustermetadata.ID{
		Component: clustermetadata.ID_CELL_TOPO,
		Cell:      c.cellName,
		Name:      fmt.Sprintf("%s-%s-cell", c.databaseName, c.cellName),
	}
}

// DisplayName returns the display name for the cell.
func (c *CellResource) DisplayName() string {
	return fmt.Sprintf("Cell: %s", c.cellName)
}

// Discover checks if cell services are provisioned (delegated to children).
func (c *CellResource) Discover(ctx context.Context, provCtx *ProvisionContext) (ResourceStatus, error) {
	// Cell is just a grouping, discovery is handled by children
	return ResourceStatus{
		State:   StateNotFound,
		Message: "cell is a container resource",
	}, nil
}

// Provision sets up the cell (actual provisioning done by children).
func (c *CellResource) Provision(ctx context.Context, provCtx *ProvisionContext) (ResourceStatus, error) {
	// Cell provisioning is handled by children
	return ResourceStatus{
		State:   StateProvisioned,
		Message: fmt.Sprintf("cell %s initialized", c.cellName),
	}, nil
}

// Deprovision removes cell services (handled by children).
func (c *CellResource) Deprovision(ctx context.Context, status ResourceStatus) error {
	// Children handle their own cleanup
	return nil
}

// Dependencies returns the database as a dependency.
func (c *CellResource) Dependencies() []*clustermetadata.ID {
	// Depends on the parent database being registered
	return nil // Dependencies handled at database level
}

// Children returns all cell services.
func (c *CellResource) Children() []Resource {
	return c.children
}

// PrintStatus implements custom status printing for the cell.
func (c *CellResource) PrintStatus(ctx context.Context, w io.Writer, node *ResourceNode, indent int) error {
	indentStr := strings.Repeat("  ", indent)

	// Print cell header
	fmt.Fprintf(w, "%s=== Provisioning services in cell: %s ===\n", indentStr, c.cellName)

	// Print children
	for _, child := range node.Children {
		if err := PrintResourceStatus(ctx, w, child, indent); err != nil {
			return err
		}
	}

	// Wait for this resource to complete
	select {
	case <-node.completed:
	case <-ctx.Done():
		return ctx.Err()
	}

	// Print completion
	fmt.Fprintf(w, "%s✓ Cell %s provisioned successfully\n\n", indentStr, c.cellName)

	return nil
}
