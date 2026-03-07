// Copyright 2026 Supabase, Inc.
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

package consensus

// Indicator is implemented by all incoming events in the consensus protocol.
// CoordNode and PoolerNode both receive Indicator values in their Step() calls.
type Indicator interface {
	consensusIndicator()
}

// --- Indicators for PoolerNode ---

// WritePolicyIndicator is delivered to the primary PoolerNode when a CoordNode
// requests a DurabilityRules change. The primary validates the CAS (Rules.Seq
// must equal its current PolicySeq + 1); if valid, it emits a
// PolicyRecordApplyRequest to the local postgres driver.
type WritePolicyIndicator struct {
	FromCoord NodeID
	Rules     DurabilityRules // Rules.Seq is the CAS key (must be currentSeq+1)
}

func (WritePolicyIndicator) consensusIndicator() {}

// PolicyRecordAppliedIndicator is delivered to a PoolerNode when the local
// postgres changes needed for a DurabilityRules update have been completed:
//
//   - Primary path: the local driver updated synchronous_standby_names and
//     committed the SQL transaction that writes the record. The transaction
//     generates a WAL entry that propagates to replicas.
//
//   - Replica path: the WAL watcher detected the rules record in the replica's
//     WAL stream and the replica's local postgres state is now up to date.
//
// In both cases, the PoolerNode responds by persisting the updated rules and
// emitting a status broadcast.
type PolicyRecordAppliedIndicator struct {
	Rules DurabilityRules
}

func (PolicyRecordAppliedIndicator) consensusIndicator() {}

// TerminateIndicator is delivered to a PoolerNode to signal graceful shutdown.
// The pooler records PostgresStopped and emits a final status update.
// In production this corresponds to a SIGTERM sent to the multipooler process.
type TerminateIndicator struct{}

func (TerminateIndicator) consensusIndicator() {}

// --- Indicators for CoordNode ---

// WritePolicyResponseIndicator is delivered to a CoordNode when the target
// primary has completed handling a WritePolicyIndicator. Accepted=true means
// the record was committed and is propagating via WAL. When false, CurrentSeq
// carries the primary's actual current policy seq so the coord can compute the
// correct next seq on retry without a separate round-trip.
type WritePolicyResponseIndicator struct {
	FromPooler NodeID
	Accepted   bool
	CurrentSeq int64 // set when Accepted=false
}

func (WritePolicyResponseIndicator) consensusIndicator() {}

// PoolerStatusIndicator is delivered to CoordNode when a pooler broadcasts its
// status. It carries the pooler's full committed state (including the current
// DurabilityRules), postgres operational status, and static node properties.
type PoolerStatusIndicator struct {
	PoolerID       NodeID
	State          PoolerPersistentState
	PostgresStatus PostgresStatus
	Properties     NodeProperties
}

func (PoolerStatusIndicator) consensusIndicator() {}

// PoolerDiscoveredIndicator is delivered to CoordNode when a new pooler
// registers in etcd (or is otherwise discovered by the provisioner).
type PoolerDiscoveredIndicator struct {
	PoolerID NodeID
}

func (PoolerDiscoveredIndicator) consensusIndicator() {}

// PoolerRemovedIndicator is delivered to CoordNode when a pooler deregisters
// from etcd.
type PoolerRemovedIndicator struct {
	PoolerID NodeID
}

func (PoolerRemovedIndicator) consensusIndicator() {}
