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

package multigateway

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/multigres/multigres/go/common/topoclient"
	"github.com/multigres/multigres/go/pb/query"
	"github.com/multigres/multigres/go/tools/retry"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"
)

// PoolerWithConn bundles pooler info with its gRPC connection.
type PoolerWithConn struct {
	Pooler  *topoclient.MultiPoolerInfo
	Conn    *grpc.ClientConn
	ConnErr error // Error from createConnection if connection failed
}

// PoolerDiscovery is a discovery service that watches for multipoolers
// in the topology using topology watches and maintains a list of available poolers.
// It also manages gRPC connections to poolers.
type PoolerDiscovery struct {
	// Configuration
	topoStore topoclient.Store
	cell      string
	logger    *slog.Logger

	// Control
	ctx        context.Context
	cancelFunc context.CancelFunc
	wg         sync.WaitGroup

	// State - all poolers we have connections to
	mu          sync.Mutex
	poolers     map[string]*PoolerWithConn // pooler ID -> pooler + connection
	lastRefresh time.Time
}

// NewPoolerDiscovery creates a new pooler discovery service.
func NewPoolerDiscovery(ctx context.Context, topoStore topoclient.Store, cell string, logger *slog.Logger) *PoolerDiscovery {
	discoveryCtx, cancel := context.WithCancel(ctx)

	return &PoolerDiscovery{
		topoStore:  topoStore,
		cell:       cell,
		logger:     logger,
		ctx:        discoveryCtx,
		cancelFunc: cancel,
		poolers:    make(map[string]*PoolerWithConn),
	}
}

// Start begins the discovery process using topology watch.
func (pd *PoolerDiscovery) Start() {
	pd.wg.Go(func() {
		pd.logger.Info("Starting pooler discovery with topology watch", "cell", pd.cell)

		r := retry.New(100*time.Millisecond, 30*time.Second)
		for attempt, err := range r.Attempts(pd.ctx) {
			if err != nil {
				// Context cancelled
				pd.logger.Info("Pooler discovery shutting down")
				return
			}

			if attempt > 0 {
				pd.logger.Info("Restarting pooler discovery with topology watch", "cell", pd.cell)
			}

			// Establish watch and process changes
			func() {
				// Get connection for the cell
				conn, err := pd.topoStore.ConnForCell(pd.ctx, pd.cell)
				if err != nil {
					pd.logger.Error("Failed to get connection for cell", "cell", pd.cell, "error", err)
					return
				}

				// Start watching the poolers directory
				poolersPath := "poolers" // This matches the PoolersPath constant from store.go
				initial, changes, err := conn.WatchRecursive(pd.ctx, poolersPath)
				if err != nil {
					pd.logger.Error("Failed to start recursive watch on poolers", "path", poolersPath, "error", err)
					return
				}

				// Process initial values
				pd.processInitialPoolers(initial)

				// Reset backoff after watch has been stable for 30s
				resetTimer := time.AfterFunc(30*time.Second, func() {
					r.Reset()
				})
				defer resetTimer.Stop()

				// Process changes as they come in
				for {
					select {
					case <-pd.ctx.Done():
						return
					case watchData, ok := <-changes:
						if !ok {
							pd.logger.Info("Watch channel closed, will reconnect")
							return
						}

						if watchData.Err != nil {
							pd.logger.Error("Watch error received", "error", watchData.Err)
							// Continue watching despite the error
							continue
						}

						pd.processPoolerChange(watchData)
					}
				}
			}()
		}
	})
}

// Stop stops the discovery service and closes all connections.
func (pd *PoolerDiscovery) Stop() {
	pd.cancelFunc()
	pd.wg.Wait()

	// Close all connections
	pd.mu.Lock()
	defer pd.mu.Unlock()
	for _, pwc := range pd.poolers {
		if pwc.Conn != nil {
			pwc.Conn.Close()
		}
	}
}

// processInitialPoolers processes the initial set of poolers from the watch
func (pd *PoolerDiscovery) processInitialPoolers(initial []*topoclient.WatchDataRecursive) {
	pd.mu.Lock()
	defer pd.mu.Unlock()

	// Close existing connections before clearing
	for _, pwc := range pd.poolers {
		if pwc.Conn != nil {
			pwc.Conn.Close()
		}
	}

	// Clear existing poolers
	pd.poolers = make(map[string]*PoolerWithConn)

	// Process initial pooler data
	for _, watchData := range initial {
		if watchData.Err != nil {
			pd.logger.Warn("Error in initial watch data", "path", watchData.Path, "error", watchData.Err)
			continue
		}

		// Parse the pooler from watch data
		pooler, err := pd.parsePoolerFromWatchData(watchData)
		if err != nil {
			pd.logger.Warn("Failed to parse pooler from initial data", "path", watchData.Path, "error", err)
			continue
		}

		if pooler != nil {
			poolerID := topoclient.MultiPoolerIDString(pooler.Id)

			// Create gRPC connection to the pooler
			conn, connErr := pd.createConnection(pooler)
			if connErr != nil {
				pd.logger.Warn("Failed to create connection to pooler",
					"id", poolerID,
					"addr", pooler.Addr(),
					"error", connErr)
			}

			pd.poolers[poolerID] = &PoolerWithConn{
				Pooler:  pooler,
				Conn:    conn,
				ConnErr: connErr,
			}
			pd.logger.Info("Initial pooler discovered",
				"id", poolerID,
				"hostname", pooler.Hostname,
				"addr", pooler.Addr(),
				"database", pooler.Database,
				"shard", pooler.Shard,
				"type", pooler.Type.String(),
				"connected", conn != nil)
		}
	}

	pd.lastRefresh = time.Now()
	pd.logger.Info("Initial pooler discovery completed",
		"cell", pd.cell,
		"pooler_count", len(pd.poolers))
}

// processPoolerChange processes a single pooler change from the watch
func (pd *PoolerDiscovery) processPoolerChange(watchData *topoclient.WatchDataRecursive) {
	pd.mu.Lock()
	defer pd.mu.Unlock()

	// Parse the pooler from watch data
	pooler, err := pd.parsePoolerFromWatchData(watchData)
	if err != nil {
		pd.logger.Warn("Failed to parse pooler from change data", "path", watchData.Path, "error", err)
		return
	}

	if pooler == nil {
		// This might be a deletion or non-pooler file
		pd.logger.Debug("Skipping non-pooler file or deletion", "path", watchData.Path)
		return
	}

	// Add or update the pooler
	poolerID := topoclient.MultiPoolerIDString(pooler.Id)

	// Check if this is a new pooler or an update
	existing, existed := pd.poolers[poolerID]

	if existed {
		// Update: reuse existing connection if address hasn't changed
		if existing.Pooler.Addr() == pooler.Addr() {
			// Address unchanged, reuse connection and error
			pd.poolers[poolerID] = &PoolerWithConn{
				Pooler:  pooler,
				Conn:    existing.Conn,
				ConnErr: existing.ConnErr,
			}
		} else {
			// Address changed, close old connection and create new one
			if existing.Conn != nil {
				existing.Conn.Close()
			}
			conn, connErr := pd.createConnection(pooler)
			if connErr != nil {
				pd.logger.Warn("Failed to create connection to updated pooler",
					"id", poolerID,
					"addr", pooler.Addr(),
					"error", connErr)
			}
			pd.poolers[poolerID] = &PoolerWithConn{
				Pooler:  pooler,
				Conn:    conn,
				ConnErr: connErr,
			}
		}
		pd.logger.Info("Pooler updated",
			"id", poolerID,
			"hostname", pooler.Hostname,
			"addr", pooler.Addr(),
			"tableGroup", pooler.TableGroup,
			"database", pooler.Database,
			"shard", pooler.Shard,
			"type", pooler.Type.String())
	} else {
		// New pooler: create connection
		conn, connErr := pd.createConnection(pooler)
		if connErr != nil {
			pd.logger.Warn("Failed to create connection to new pooler",
				"id", poolerID,
				"addr", pooler.Addr(),
				"error", connErr)
		}
		pd.poolers[poolerID] = &PoolerWithConn{
			Pooler:  pooler,
			Conn:    conn,
			ConnErr: connErr,
		}
		pd.logger.Info("New pooler discovered",
			"id", poolerID,
			"hostname", pooler.Hostname,
			"addr", pooler.Addr(),
			"tableGroup", pooler.TableGroup,
			"database", pooler.Database,
			"shard", pooler.Shard,
			"type", pooler.Type.String(),
			"connected", conn != nil)
	}

	pd.lastRefresh = time.Now()
}

// GetPoolers returns a list of all discovered poolers.
func (pd *PoolerDiscovery) GetPoolers() []*clustermetadatapb.MultiPooler {
	pd.mu.Lock()
	defer pd.mu.Unlock()

	poolers := make([]*clustermetadatapb.MultiPooler, 0, len(pd.poolers))
	for _, pwc := range pd.poolers {
		poolers = append(poolers, proto.Clone(pwc.Pooler.MultiPooler).(*clustermetadatapb.MultiPooler))
	}
	return poolers
}

// GetPooler returns a pooler matching the target specification and its gRPC connection.
// Target specifies the tablegroup, shard, and pooler type to route to.
// Returns an error if no matching pooler is found or if the pooler has no connection.
//
// Filtering logic:
// - TableGroup: Required, must match exactly
// - PoolerType: If not specified (UNKNOWN), defaults to PRIMARY
// - Shard: If empty, matches any shard; otherwise must match exactly
func (pd *PoolerDiscovery) GetPooler(target *query.Target) (*clustermetadatapb.MultiPooler, *grpc.ClientConn, error) {
	pd.mu.Lock()
	defer pd.mu.Unlock()

	// Default to PRIMARY if not specified
	targetType := target.PoolerType
	if targetType == clustermetadatapb.PoolerType_UNKNOWN {
		targetType = clustermetadatapb.PoolerType_PRIMARY
	}

	// Debug: Log all discovered poolers
	pd.logger.Debug("GetPooler called - listing all discovered poolers",
		"target_tablegroup", target.TableGroup,
		"target_shard", target.Shard,
		"target_pooler_type", targetType.String(),
		"total_poolers", len(pd.poolers))
	for id, pwc := range pd.poolers {
		pd.logger.Debug("discovered pooler",
			"index", id,
			"pooler_id", topoclient.MultiPoolerIDString(pwc.Pooler.Id),
			"tablegroup", pwc.Pooler.TableGroup,
			"shard", pwc.Pooler.Shard,
			"type", pwc.Pooler.Type.String())
	}

	// Find matching pooler
	for _, pwc := range pd.poolers {
		pooler := pwc.Pooler
		// TableGroup must match
		if pooler.TableGroup != target.TableGroup {
			continue
		}

		// PoolerType must match
		if pooler.Type != targetType {
			continue
		}

		// Shard must match if specified
		if target.Shard != "" && pooler.Shard != target.Shard {
			continue
		}

		// Found a match!
		pd.logger.Debug("selected pooler for target",
			"pooler_id", topoclient.MultiPoolerIDString(pooler.Id),
			"pooler_type", pooler.Type.String(),
			"tablegroup", pooler.TableGroup,
			"shard", pooler.Shard)

		if pwc.Conn == nil {
			return nil, nil, fmt.Errorf("no connection available for pooler %s at %s: %w",
				topoclient.MultiPoolerIDString(pooler.Id), pooler.Addr(), pwc.ConnErr)
		}
		return proto.Clone(pooler.MultiPooler).(*clustermetadatapb.MultiPooler), pwc.Conn, nil
	}

	pd.logger.Warn("no matching pooler found",
		"tablegroup", target.TableGroup,
		"shard", target.Shard,
		"pooler_type", targetType.String())
	return nil, nil, fmt.Errorf("no pooler found for target: tablegroup=%s, shard=%s, type=%s",
		target.TableGroup, target.Shard, targetType.String())
}

// LastRefresh returns the timestamp of the last successful refresh.
func (pd *PoolerDiscovery) LastRefresh() time.Time {
	pd.mu.Lock()
	defer pd.mu.Unlock()
	return pd.lastRefresh
}

// PoolerCount returns the current number of discovered poolers.
func (pd *PoolerDiscovery) PoolerCount() int {
	pd.mu.Lock()
	defer pd.mu.Unlock()
	return len(pd.poolers)
}

// parsePoolerFromWatchData parses a MultiPooler from watch data
func (pd *PoolerDiscovery) parsePoolerFromWatchData(watchData *topoclient.WatchDataRecursive) (*topoclient.MultiPoolerInfo, error) {
	// Only process files that end with "Pooler" (the actual pooler data files)
	if !strings.HasSuffix(watchData.Path, "/Pooler") {
		return nil, nil // Not a pooler file, skip
	}

	// If Contents is nil, this might be a deletion
	if watchData.Contents == nil {
		return nil, nil
	}

	// Parse the protobuf data
	pooler := &clustermetadatapb.MultiPooler{}
	if err := proto.Unmarshal(watchData.Contents, pooler); err != nil {
		return nil, err
	}

	return &topoclient.MultiPoolerInfo{
		MultiPooler: pooler,
	}, nil
}

// createConnection creates a gRPC connection to a pooler.
func (pd *PoolerDiscovery) createConnection(pooler *topoclient.MultiPoolerInfo) (*grpc.ClientConn, error) {
	addr := pooler.Addr()
	conn, err := grpc.NewClient(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create gRPC client for %s: %w", addr, err)
	}
	return conn, nil
}
