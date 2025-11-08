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

// DatabaseConfig holds configuration for a database
type DatabaseConfig struct {
	Name             string
	BackupLocation   string
	DurabilityPolicy string
	Cells            []string
}

// DatabaseResource represents a database resource
type DatabaseResource struct {
	id     *clustermetadatapb.ID
	config *DatabaseConfig
}

// NewDatabaseResource creates a new database resource
func NewDatabaseResource(config *DatabaseConfig) *DatabaseResource {
	return &DatabaseResource{
		id: &clustermetadatapb.ID{
			Component: clustermetadatapb.ID_DATABASE,
			Cell:      topo.GlobalCell,
			Name:      config.Name,
		},
		config: config,
	}
}

// ID returns the resource ID
func (r *DatabaseResource) ID() *clustermetadatapb.ID {
	return r.id
}

// Dependencies returns the resources this resource depends on (global topology)
func (r *DatabaseResource) Dependencies() []*clustermetadatapb.ID {
	// Database depends on global topology (implicit, handled by engine)
	return []*clustermetadatapb.ID{}
}

// Provision registers the database in the topology server
func (r *DatabaseResource) Provision(ctx context.Context, pctx ProvisionContext) (*provisioner.ProvisionResult, error) {
	// Open global topology
	ts, err := pctx.OpenGlobalTopo(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to global topology: %w", err)
	}
	defer ts.Close()

	// Check if database already exists
	existingDB, err := ts.GetDatabase(ctx, r.config.Name)
	if err == nil {
		// Database already exists, return existing configuration
		return &provisioner.ProvisionResult{
			ServiceName: "database",
			FQDN:        "",
			Ports:       map[string]int{},
			Metadata: map[string]any{
				"database":          r.config.Name,
				"backup_location":   existingDB.BackupLocation,
				"durability_policy": existingDB.DurabilityPolicy,
				"cells":             existingDB.Cells,
			},
		}, nil
	}

	// Create the database if it doesn't exist
	if errors.Is(err, &topo.TopoError{Code: topo.NoNode}) {
		databaseConfig := &clustermetadatapb.Database{
			Name:             r.config.Name,
			BackupLocation:   r.config.BackupLocation,
			DurabilityPolicy: r.config.DurabilityPolicy,
			Cells:            r.config.Cells,
		}

		if err := ts.CreateDatabase(ctx, r.config.Name, databaseConfig); err != nil {
			return nil, fmt.Errorf("failed to create database '%s': %w", r.config.Name, err)
		}

		return &provisioner.ProvisionResult{
			ServiceName: "database",
			FQDN:        "",
			Ports:       map[string]int{},
			Metadata: map[string]any{
				"database":          r.config.Name,
				"backup_location":   r.config.BackupLocation,
				"durability_policy": r.config.DurabilityPolicy,
				"cells":             r.config.Cells,
			},
		}, nil
	}

	// Some other error occurred
	return nil, fmt.Errorf("failed to check database '%s': %w", r.config.Name, err)
}

// Deprovision removes the database from the topology (currently a no-op as we don't delete databases)
func (r *DatabaseResource) Deprovision(ctx context.Context, pctx ProvisionContext) error {
	// We don't delete databases from topology during deprovision
	// This is a no-op
	return nil
}
