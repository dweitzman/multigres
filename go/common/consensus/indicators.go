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
// OrchNode and PoolerNode both receive Indicator values in their Step() calls.
type Indicator interface {
	consensusIndicator()
}

// --- Indicators for OrchNode (from local etcd watcher — reliable, ordered) ---

// PoolerDiscoveredIndicator is delivered to OrchNode when a new pooler registers in etcd.
type PoolerDiscoveredIndicator struct {
	PoolerID NodeID
}

func (PoolerDiscoveredIndicator) consensusIndicator() {}

// PoolerRemovedIndicator is delivered to OrchNode when a pooler deregisters from etcd.
type PoolerRemovedIndicator struct {
	PoolerID NodeID
}

func (PoolerRemovedIndicator) consensusIndicator() {}

// PoolerStatusIndicator is delivered to OrchNode from the periodic health-check agent.
// It carries the pooler's full last-committed state so a new orch can reconstruct the
// cluster view without requiring a separate quorum read round.
type PoolerStatusIndicator struct {
	PoolerID  NodeID
	StatusSeq int64                 // sequence number; orch discards updates older than the last seen seq
	State     PoolerPersistentState // full committed state at time of health check
}

func (PoolerStatusIndicator) consensusIndicator() {}

// PoolerResponseIndicator is delivered to OrchNode when a pooler responds to a broadcast.
type PoolerResponseIndicator struct {
	FromPooler   NodeID
	Accepted     bool
	KnownTerm    int64  // if rejected: the term the pooler is currently on (orch can escalate)
	KnownCoordID NodeID // if rejected at the same term: which coordinator won that term
}

func (PoolerResponseIndicator) consensusIndicator() {}

// --- Indicators for PoolerNode (from orch over the network — unreliable) ---

// OrchStateIndicator is delivered to PoolerNode when orch broadcasts a ConsensusState.
// ExpectedPrimaryTerm is a CAS check: if > 0 the pooler rejects the state unless its
// current PrimaryTerm matches, protecting against an orch acting on stale cluster info.
type OrchStateIndicator struct {
	FromOrch            NodeID
	State               ConsensusState
	ExpectedPrimaryTerm int64
}

func (OrchStateIndicator) consensusIndicator() {}
