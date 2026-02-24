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

package dstsim

// Node is a pure state machine for distributed protocols
// I = Indicator type (protocol-specific, what the node observes)
// R = Request type (protocol-specific, what the node wants to do)
// ID = Node identifier type (must be comparable for use as map key)
//
// Production code implements this interface. The event loop:
// - Accumulates indicators that arrived during a time slice
// - Calls Step() once per time slice with the tick number and all indicators
// - Processes returned requests (outgoing RPCs, timers, etc.)
// - Maintains no mutable state in the event loop itself
//
// For testing, the Simulator provides deterministic execution and chaos injection.
type Node[I any, R any, ID comparable] interface {
	// Step processes all indicators that arrived this tick and returns requests (no side effects)
	// The tick parameter represents the current logical time
	// The indicators parameter contains all messages/events that arrived during this tick
	Step(tick int64, indicators []I) []R

	// ID returns the unique identifier for this node
	ID() ID
}
