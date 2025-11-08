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
	"io"
	"strings"

	"github.com/multigres/multigres/go/clustermetadata/topo"
	"github.com/multigres/multigres/go/pb/clustermetadata"
)

// DatabaseResource represents a database and its cell-specific services.
type DatabaseResource struct {
	provisioner  *localProvisioner
	databaseName string
	children     []Resource
}

// NewDatabaseResource creates a new database resource.
func NewDatabaseResource(p *localProvisioner, databaseName string) (*DatabaseResource, error) {
	d := &DatabaseResource{
		provisioner:  p,
		databaseName: databaseName,
	}

	// Get all cells
	cellNames, err := p.getCellNames()
	if err != nil {
		return nil, fmt.Errorf("failed to get cell names: %w", err)
	}

	// Create a CellResource for each cell
	for _, cellName := range cellNames {
		cellResource, err := NewCellResource(p, databaseName, cellName)
		if err != nil {
			return nil, fmt.Errorf("failed to create cell resource for %s: %w", cellName, err)
		}
		d.children = append(d.children, cellResource)
	}

	return d, nil
}

// ID returns the resource ID for the database.
func (d *DatabaseResource) ID() *clustermetadata.ID {
	return &clustermetadata.ID{
		Component: clustermetadata.ID_DATABASE,
		Cell:      topo.GlobalCell,
		Name:      d.databaseName,
	}
}

// DisplayName returns the display name for the database.
func (d *DatabaseResource) DisplayName() string {
	return fmt.Sprintf("Database: %s", d.databaseName)
}

// Discover checks if the database is registered in topology.
func (d *DatabaseResource) Discover(ctx context.Context, provCtx *ProvisionContext) (ResourceStatus, error) {
	// Open topology server using provision context
	ts, err := provCtx.OpenGlobalTopology(ctx)
	if err != nil {
		return ResourceStatus{}, err
	}
	defer ts.Close()

	// Check if database exists
	_, err = ts.GetDatabase(ctx, d.databaseName)
	if err == nil {
		// Database already registered
		return ResourceStatus{
			State:   StateDiscovered,
			Message: fmt.Sprintf("database %s already registered", d.databaseName),
		}, nil
	}

	if errors.Is(err, &topo.TopoError{Code: topo.NoNode}) {
		// Database doesn't exist
		return ResourceStatus{
			State:   StateNotFound,
			Message: "database not registered",
		}, nil
	}

	// Some other error
	return ResourceStatus{}, fmt.Errorf("failed to check database: %w", err)
}

// Provision registers the database in topology.
func (d *DatabaseResource) Provision(ctx context.Context, provCtx *ProvisionContext) (ResourceStatus, error) {
	// Open topology server using provision context
	ts, err := provCtx.OpenGlobalTopology(ctx)
	if err != nil {
		return ResourceStatus{}, err
	}
	defer ts.Close()

	// Get all cell names
	cellNames, err := d.provisioner.getCellNames()
	if err != nil {
		return ResourceStatus{}, fmt.Errorf("failed to get cell names: %w", err)
	}

	// Create database config
	databaseConfig := &clustermetadata.Database{
		Name:             d.databaseName,
		BackupLocation:   "",
		DurabilityPolicy: "none",
		Cells:            cellNames,
	}

	// Register database
	if err := ts.CreateDatabase(ctx, d.databaseName, databaseConfig); err != nil {
		return ResourceStatus{}, fmt.Errorf("failed to create database in topology: %w", err)
	}

	return ResourceStatus{
		State:   StateProvisioned,
		Message: fmt.Sprintf("database %s registered with cells: %v", d.databaseName, cellNames),
		Metadata: map[string]any{
			"cells": cellNames,
		},
	}, nil
}

// Deprovision removes the database (children handle service cleanup).
func (d *DatabaseResource) Deprovision(ctx context.Context, status ResourceStatus) error {
	// The database resource itself doesn't need cleanup beyond children
	return nil
}

// Dependencies returns etcd as a dependency.
func (d *DatabaseResource) Dependencies() []*clustermetadata.ID {
	return []*clustermetadata.ID{
		{
			Component: clustermetadata.ID_GLOBAL_TOPO,
			Cell:      topo.GlobalCell,
			Name:      "etcd",
		},
	}
}

// Children returns cell resources.
func (d *DatabaseResource) Children() []Resource {
	return d.children
}

// PrintStatus implements custom status printing for the database.
func (d *DatabaseResource) PrintStatus(ctx context.Context, w io.Writer, node *ResourceNode, indent int) error {
	indentStr := strings.Repeat("  ", indent)

	// Print database header
	fmt.Fprintf(w, "%s=== Provisioning database: %s ===\n", indentStr, d.databaseName)
	fmt.Fprintln(w, "")

	// Print registration section
	fmt.Fprintf(w, "%s=== Registering database in topology ===\n", indentStr)

	// Wait for this resource to complete
	select {
	case <-node.completed:
	case <-ctx.Done():
		return ctx.Err()
	}

	// Get status
	status := node.GetStatus()

	switch status.State {
	case StateDiscovered:
		fmt.Fprintf(w, "%s⚙️  Database \"%s\" detected — reusing existing database ✓\n", indentStr, d.databaseName)
	case StateProvisioned:
		if cells, ok := status.Metadata["cells"].([]string); ok {
			fmt.Fprintf(w, "%s⚙️  Creating database \"%s\" with cells: [%s]...\n",
				indentStr, d.databaseName, strings.Join(cells, ", "))
			fmt.Fprintf(w, "%s⚙️  Database \"%s\" registered successfully ✓\n", indentStr, d.databaseName)
		}
	case StateFailed:
		fmt.Fprintf(w, "%s✗ Database registration failed: %v\n", indentStr, status.Error)
		return nil // Don't continue to children if we failed
	}

	fmt.Fprintln(w, "")

	// Print children
	for _, child := range node.Children {
		if err := PrintResourceStatus(ctx, w, child, indent); err != nil {
			return err
		}
	}

	// Print completion message
	fmt.Fprintln(w, "")
	fmt.Fprintf(w, "%sDatabase %s provisioned successfully\n", indentStr, d.databaseName)

	return nil
}
