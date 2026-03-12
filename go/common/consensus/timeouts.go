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

// Timing constants (all units: ticks).
//
// One "tick" corresponds to one call to CoordNode.Step or PoolerNode.Step.
// In simulation tests each tick is a logical step; in production the tick
// rate is driven by whatever polling loop drives the nodes.
//
// Relationship invariant:
//
//	PoolerHeartbeatIntervalTicks < HealthTimeoutTicks
//
// If the coordinator's health timeout fires before the pooler has had a chance
// to send its periodic heartbeat, the coordinator will incorrectly declare
// the pooler unreachable and trigger unnecessary failovers.
const (
	// PoolerHeartbeatIntervalTicks is the number of ticks between periodic
	// status broadcasts from a PoolerNode. The node emits a
	// PoolerStatusUpdateRequest at least this often even when no state has
	// changed, so coordinators can re-learn cluster state after a crash.
	PoolerHeartbeatIntervalTicks int64 = 100

	// HealthTimeoutTicks is the number of consecutive ticks without a
	// PoolerStatusIndicator before a CoordNode considers a pooler unreachable.
	// Must be strictly greater than PoolerHeartbeatIntervalTicks.
	HealthTimeoutTicks int64 = 300

	// WriteTimeoutTicks is the number of ticks the coordinator waits for a
	// WritePolicyResponse before abandoning the in-flight write and retrying.
	WriteTimeoutTicks int64 = 15

	// PhaseRetryTicks is the number of ticks to wait for responses in
	// post-recruit phases before re-sending to unacked nodes. Handles the
	// case where a recruited node crashes mid-phase and needs to be retried
	// after it restarts.
	PhaseRetryTicks int64 = 15
)
