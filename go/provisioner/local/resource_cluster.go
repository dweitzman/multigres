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

	"github.com/multigres/multigres/go/clustermetadata/topo"
	"github.com/multigres/multigres/go/pb/clustermetadata"
)

// ClusterResource is the root resource representing the entire Multigres cluster.
// It contains global services (etcd, multiadmin) and databases as children.
type ClusterResource struct {
	provisioner *localProvisioner
	children    []Resource
}

// NewClusterResource creates a new cluster resource with global services and databases.
func NewClusterResource(p *localProvisioner, databases []string) (*ClusterResource, error) {
	c := &ClusterResource{
		provisioner: p,
	}

	// Add global topology (etcd) as first child
	globalTopo, err := NewGlobalTopoResource(p)
	if err != nil {
		return nil, fmt.Errorf("failed to create global topo resource: %w", err)
	}
	c.children = append(c.children, globalTopo)

	// Add multiadmin as second child (depends on etcd)
	multiadmin, err := NewMultiadminResource(p)
	if err != nil {
		return nil, fmt.Errorf("failed to create multiadmin resource: %w", err)
	}
	c.children = append(c.children, multiadmin)

	// Add each database as a child
	for _, dbName := range databases {
		db, err := NewDatabaseResource(p, dbName)
		if err != nil {
			return nil, fmt.Errorf("failed to create database resource for %s: %w", dbName, err)
		}
		c.children = append(c.children, db)
	}

	return c, nil
}

// ID returns the resource ID for the cluster.
func (c *ClusterResource) ID() *clustermetadata.ID {
	return &clustermetadata.ID{
		Component: clustermetadata.ID_CLUSTER,
		Cell:      topo.GlobalCell,
		Name:      "cluster",
	}
}

// DisplayName returns the display name for the cluster.
func (c *ClusterResource) DisplayName() string {
	return "Multigres Cluster"
}

// Discover checks if the cluster resources are already running.
// For the cluster itself, this is mainly a container, so we just return NotFound.
func (c *ClusterResource) Discover(ctx context.Context, provCtx *ProvisionContext) (ResourceStatus, error) {
	// The cluster resource itself doesn't represent a running service,
	// it's just a container for other resources
	return ResourceStatus{
		State:   StateNotFound,
		Message: "cluster is a container resource",
	}, nil
}

// Provision validates binaries and initializes directories.
func (c *ClusterResource) Provision(ctx context.Context, provCtx *ProvisionContext) (ResourceStatus, error) {
	// Validate binary paths
	if err := c.provisioner.validateBinaryPaths(); err != nil {
		return ResourceStatus{}, fmt.Errorf("binary validation failed: %w", err)
	}

	// Validate system binaries (PostgreSQL)
	if err := c.provisioner.validateSystemBinaries(); err != nil {
		return ResourceStatus{}, fmt.Errorf("system binary validation failed: %w", err)
	}

	// Initialize pgctld directories for all cells
	if err := c.provisioner.initializePgctldDirectories(); err != nil {
		return ResourceStatus{}, fmt.Errorf("failed to initialize pgctld directories: %w", err)
	}

	return ResourceStatus{
		State:   StateProvisioned,
		Message: "cluster initialized",
	}, nil
}

// Deprovision stops all cluster resources.
func (c *ClusterResource) Deprovision(ctx context.Context, status ResourceStatus) error {
	// The cluster resource itself doesn't need specific cleanup
	// Children will handle their own deprovisioning
	return nil
}

// Dependencies returns no dependencies (cluster is the root).
func (c *ClusterResource) Dependencies() []*clustermetadata.ID {
	return nil
}

// Children returns all child resources.
func (c *ClusterResource) Children() []Resource {
	return c.children
}

// PrintStatus implements custom status printing for the cluster.
func (c *ClusterResource) PrintStatus(ctx context.Context, w io.Writer, node *ResourceNode, indent int) error {
	indentStr := strings.Repeat("  ", indent)

	// Print cluster header
	fmt.Fprintf(w, "%s=== Bootstrapping Multigres Cluster ===\n", indentStr)
	fmt.Fprintln(w, "")

	// Print children recursively
	for _, child := range node.Children {
		if err := PrintResourceStatus(ctx, w, child, indent); err != nil {
			return err
		}
	}

	// Wait for this node to complete
	select {
	case <-node.completed:
	case <-ctx.Done():
		return ctx.Err()
	}

	// Print completion message
	status := node.GetStatus()
	if status.State == StateProvisioned || status.State == StateDiscovered {
		fmt.Fprintln(w, "")
		fmt.Fprintf(w, "%s=== Cluster Bootstrap Complete ===\n", indentStr)
	} else if status.State == StateFailed {
		fmt.Fprintln(w, "")
		fmt.Fprintf(w, "%s=== Cluster Bootstrap Failed ===\n", indentStr)
		if status.Error != nil {
			fmt.Fprintf(w, "%sError: %v\n", indentStr, status.Error)
		}
	}

	return nil
}
