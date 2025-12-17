// Copyright 2025 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package poolergateway handles selection and communication with multipooler instances.
// It is responsible for:
// - Discovering available poolers via PoolerDiscovery
// - Selecting healthy poolers for a given tablegroup
// - Providing QueryService instances for query execution
//
// gRPC connections are managed by PoolerDiscovery; this package creates
// lightweight QueryService wrappers on demand. In the future it will also
// set up health monitoring.
//
// This is analogous to Vitess's TabletGateway component.
package poolergateway

import (
	"context"
	"log/slog"

	"github.com/multigres/multigres/go/common/queryservice"
	"github.com/multigres/multigres/go/common/topoclient"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/pb/query"

	"google.golang.org/grpc"
)

// PoolerDiscovery is the interface for discovering multipooler instances.
// This abstracts the PoolerDiscovery implementation for easier testing.
type PoolerDiscovery interface {
	// GetPoolers returns all discovered poolers
	GetPoolers() []*clustermetadatapb.MultiPooler

	// GetPooler returns a pooler matching the target specification and its gRPC connection.
	// Target specifies the tablegroup, shard, and pooler type to route to.
	// Returns an error if no matching pooler is found.
	GetPooler(target *query.Target) (*clustermetadatapb.MultiPooler, *grpc.ClientConn, error)
}

// A Gateway is the query processing module for each shard,
// which is used by ScatterConn.
type Gateway interface {
	// the query service that this Gateway wraps around
	queryservice.QueryService

	// QueryServiceByID returns a QueryService
	QueryServiceByID(ctx context.Context, id *clustermetadatapb.ID, target *query.Target) (queryservice.QueryService, error)
}

// PoolerGateway selects poolers and creates QueryService instances for query execution.
// gRPC connections are managed by PoolerDiscovery.
type PoolerGateway struct {
	// discovery is used to find available poolers and their connections
	discovery PoolerDiscovery

	// logger for debugging
	logger *slog.Logger
}

// NewPoolerGateway creates a new PoolerGateway.
func NewPoolerGateway(
	discovery PoolerDiscovery,
	logger *slog.Logger,
) *PoolerGateway {
	return &PoolerGateway{
		discovery: discovery,
		logger:    logger,
	}
}

// QueryServiceByID implements Gateway.
func (pg *PoolerGateway) QueryServiceByID(ctx context.Context, id *clustermetadatapb.ID, target *query.Target) (queryservice.QueryService, error) {
	// TODO: IMPLEMENT queryservicebyid
	return pg, nil
}

// StreamExecute implements queryservice.QueryService.
// It routes the query to the appropriate multipooler instance based on the target.
//
// This method:
// 1. Uses discovery to find a pooler matching the target specification
// 2. Establishes gRPC connection if needed
// 3. Delegates to the pooler's QueryService for execution
//
// The target specifies:
// - TableGroup: Required
// - PoolerType: PRIMARY (writes), REPLICA (reads), etc. Defaults to PRIMARY if not set.
// - Shard: Optional, empty matches any shard
func (pg *PoolerGateway) StreamExecute(
	ctx context.Context,
	target *query.Target,
	sql string,
	options *query.ExecuteOptions,
	callback func(context.Context, *query.QueryResult) error,
) error {
	// Get a pooler matching the target
	queryService, err := pg.getQueryServiceForTarget(ctx, target)
	if err != nil {
		return err
	}

	// Delegate to the pooler's QueryService
	return queryService.StreamExecute(ctx, target, sql, options, callback)
}

func (pg *PoolerGateway) getQueryServiceForTarget(ctx context.Context, target *query.Target) (queryservice.QueryService, error) {
	pooler, conn, err := pg.discovery.GetPooler(target)
	if err != nil {
		return nil, err
	}

	poolerID := topoclient.MultiPoolerIDString(pooler.Id)

	pg.logger.DebugContext(ctx, "selected pooler for target",
		"tablegroup", target.TableGroup,
		"shard", target.Shard,
		"pooler_type", target.PoolerType.String(),
		"pooler_id", poolerID,
		"actual_pooler_type", pooler.Type.String())

	// Create QueryService on demand - it's lightweight, just wraps the connection
	return newGRPCQueryService(conn, poolerID, pg.logger), nil
}

// ExecuteQuery implements queryservice.QueryService.
// It routes the query to the appropriate multipooler instance based on the target.
// This should be used sparingly only when we know the result set is small,
// otherwise StreamExecute should be used.
func (pg *PoolerGateway) ExecuteQuery(ctx context.Context, target *query.Target, sql string, options *query.ExecuteOptions) (*query.QueryResult, error) {
	// Get a pooler matching the target
	queryService, err := pg.getQueryServiceForTarget(ctx, target)
	if err != nil {
		return nil, err
	}

	// Delegate to the pooler's QueryService
	return queryService.ExecuteQuery(ctx, target, sql, options)
}

// PortalStreamExecute implements queryservice.QueryService.
// It executes a portal and returns reservation information.
func (pg *PoolerGateway) PortalStreamExecute(
	ctx context.Context,
	target *query.Target,
	preparedStatement *query.PreparedStatement,
	portal *query.Portal,
	options *query.ExecuteOptions,
	callback func(context.Context, *query.QueryResult) error,
) (queryservice.ReservedState, error) {
	// Get a pooler matching the target
	queryService, err := pg.getQueryServiceForTarget(ctx, target)
	if err != nil {
		return queryservice.ReservedState{}, err
	}

	// Delegate to the pooler's QueryService
	return queryService.PortalStreamExecute(ctx, target, preparedStatement, portal, options, callback)
}

// Describe implements queryservice.QueryService.
// It returns metadata about a prepared statement or portal.
func (pg *PoolerGateway) Describe(
	ctx context.Context,
	target *query.Target,
	preparedStatement *query.PreparedStatement,
	portal *query.Portal,
	options *query.ExecuteOptions,
) (*query.StatementDescription, error) {
	// Get a pooler matching the target
	queryService, err := pg.getQueryServiceForTarget(ctx, target)
	if err != nil {
		return nil, err
	}

	// Delegate to the pooler's QueryService
	return queryService.Describe(ctx, target, preparedStatement, portal, options)
}

// Close implements queryservice.QueryService.
// Connections are managed by PoolerDiscovery, so this is a no-op.
func (pg *PoolerGateway) Close(ctx context.Context) error {
	pg.logger.InfoContext(ctx, "PoolerGateway.Close called")
	return nil
}

// Ensure PoolerGateway implements Gateway
var _ Gateway = (*PoolerGateway)(nil)

// Stats returns statistics about the gateway.
func (pg *PoolerGateway) Stats() map[string]any {
	return map[string]any{
		"poolers_discovered": len(pg.discovery.GetPoolers()),
	}
}
