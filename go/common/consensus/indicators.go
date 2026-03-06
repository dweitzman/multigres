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
// requests a DurabilityPolicyRecord change. The primary validates the CAS
// (Record.PreviousID must equal its current Policy.ID); if valid, it emits a
// PolicyRecordApplyRequest to the local postgres driver.
type WritePolicyIndicator struct {
	FromCoord NodeID
	Record    DurabilityPolicyRecord // Record.PreviousID is the CAS key
}

func (WritePolicyIndicator) consensusIndicator() {}

// PolicyRecordAppliedIndicator is delivered to a PoolerNode when the local
// postgres changes needed for a DurabilityPolicyRecord have been completed:
//
//   - Primary path: the local driver updated synchronous_standby_names and
//     committed the SQL transaction that writes the record. The transaction
//     generates a WAL entry that propagates to replicas.
//
//   - Replica path: the WAL watcher detected the policy record in the replica's
//     WAL stream and the replica's local postgres state is now up to date.
//
// In both cases, the PoolerNode responds by persisting the updated policy and
// emitting a status broadcast. PolicyID identifies which record was applied so
// the PoolerNode can discard stale indicators if the committed state advanced.
type PolicyRecordAppliedIndicator struct {
	PolicyID PolicyID
	Record   DurabilityPolicyRecord
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
// the record was committed and is propagating via WAL. When false, CurrentID
// carries the primary's actual current policy version so the coord can correct
// its PreviousID on retry without a separate round-trip.
type WritePolicyResponseIndicator struct {
	FromPooler NodeID
	Accepted   bool
	CurrentID  PolicyID // set when Accepted=false
}

func (WritePolicyResponseIndicator) consensusIndicator() {}

// PoolerStatusIndicator is delivered to CoordNode when a pooler broadcasts its
// status. It carries the pooler's full committed state (including the current
// DurabilityPolicyRecord) and postgres operational status.
type PoolerStatusIndicator struct {
	PoolerID       NodeID
	State          PoolerPersistentState
	PostgresStatus PostgresStatus
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
