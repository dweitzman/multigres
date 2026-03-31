// Copyright 2026 Supabase, Inc.
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

package analysis

import (
	"time"

	commontypes "github.com/multigres/multigres/go/common/types"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multiorchdatapb "github.com/multigres/multigres/go/pb/multiorchdata"
)

// ShardAnalysis is the input to all analyzers — the full state of a shard at a point in time.
// Analyzers receive one ShardAnalysis per shard and compute whatever cross-pooler aggregation
// they need to detect problems.
type ShardAnalysis struct {
	ShardKey   commontypes.ShardKey
	Poolers    []*PoolerState
	AnalyzedAt time.Time
}

// FindPrimary returns the PoolerState with IsPrimary=true and the highest PrimaryTerm.
// Returns nil if no primary can be found in the shard.
func (s *ShardAnalysis) FindPrimary() *PoolerState {
	var primary *PoolerState
	var highestPrimaryTerm int64

	for _, ps := range s.Poolers {
		if !ps.IsPrimary {
			continue
		}
		if primary == nil || ps.PrimaryTerm > highestPrimaryTerm {
			primary = ps
			highestPrimaryTerm = ps.PrimaryTerm
		}
	}

	return primary
}

// AllReplicasConnectedToPrimary returns true only if every replica in the shard is
// actively receiving WAL from the given primary. Used to distinguish "primary pooler
// crashed but Postgres still running" from "primary Postgres is dead": if replicas are
// still connected, failover is not warranted — the operator should restart the pooler.
// Returns false if there are no replicas or any replica is not connected.
func (s *ShardAnalysis) AllReplicasConnectedToPrimary(primary *PoolerState) bool {
	if primary.Health == nil || primary.Health.MultiPooler == nil {
		return false
	}
	primaryHost := primary.Health.MultiPooler.Hostname
	primaryPort := primary.Health.MultiPooler.PortMap["postgres"]

	replicaCount := 0
	connectedCount := 0
	for _, ps := range s.Poolers {
		if ps.IsPrimary {
			continue
		}
		replicaCount++
		if isReplicaConnectedToPrimary(ps, primaryHost, primaryPort) {
			connectedCount++
		}
	}
	return replicaCount > 0 && connectedCount == replicaCount
}

// isReplicaConnectedToPrimary checks if a replica is receiving WAL from the given primary.
// Returns false if the replica is unreachable, has no replication status, or its
// primary_conninfo points elsewhere.
//
// TODO: Check heartbeat timestamp to verify writes are actively flowing through replication.
// The multigres.heartbeat table is updated periodically on the primary, so checking if the
// replica's heartbeat timestamp is recent would prove the replication connection is active.
// Currently we check that LastReceiveLsn is non-empty, but this doesn't prove active connectivity.
func isReplicaConnectedToPrimary(ps *PoolerState, primaryHost string, primaryPort int32) bool {
	if !ps.LastCheckValid {
		return false
	}
	if ps.Health == nil || ps.Health.ReplicationStatus == nil {
		return false
	}
	connInfo := ps.Health.ReplicationStatus.PrimaryConnInfo
	if connInfo == nil || connInfo.Host == "" {
		return false
	}
	if connInfo.Host != primaryHost || connInfo.Port != primaryPort {
		return false
	}
	return ps.Health.ReplicationStatus.LastReceiveLsn != ""
}

// PoolerState is the analyzed state of a single pooler within a ShardAnalysis.
// Light computed fields are pre-populated here to avoid duplicating type-resolution
// logic across every analyzer. Cross-pooler aggregation (replica counts, primary
// connectivity) is intentionally left to analyzers.
type PoolerState struct {
	// Raw health state from the pooler store.
	Health *multiorchdatapb.PoolerHealthState

	// ID of this pooler.
	ID *clustermetadatapb.ID

	// Type resolved from the health check first, falling back to topology type.
	// Health check type is authoritative since nodes report their actual running state.
	Type clustermetadatapb.PoolerType

	// IsPrimary is true when Type == PoolerType_PRIMARY.
	IsPrimary bool

	// LastCheckValid is true when the pooler is reachable and returning a valid health response.
	LastCheckValid bool

	// IsStale is true when the pooler's health data is not up to date (!IsUpToDate).
	IsStale bool

	// IsInitialized indicates the pooler is fully initialized and ready to join the cohort.
	IsInitialized bool

	// HasDataDirectory indicates the pooler has a PostgreSQL data directory (PG_VERSION exists).
	HasDataDirectory bool

	// PrimaryTerm is the consensus term when this pooler was promoted to primary.
	// Zero for poolers that have never been primary.
	PrimaryTerm int64

	// ConsensusTerm is this node's current consensus term from its health check.
	ConsensusTerm int64
}
