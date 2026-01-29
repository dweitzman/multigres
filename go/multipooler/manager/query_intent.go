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

package manager

// QueryIntent indicates whether a query modifies state and requires the action lock.
// This is explicitly declared by the caller when executing internal queries.
type QueryIntent int

const (
	// QueryIntentReadOnly indicates a read-only query that doesn't modify state.
	// Examples: SELECT, SHOW, queries that read from multigres schema tables.
	// These queries don't require the action lock.
	QueryIntentReadOnly QueryIntent = iota

	// QueryIntentStateChange indicates a query that modifies PostgreSQL or cluster state.
	// Examples: INSERT, UPDATE, DELETE, CREATE, ALTER, queries that write to multigres schema.
	// These queries require the caller to hold the action lock (enforced by assertion).
	QueryIntentStateChange
)

// String returns the string representation of QueryIntent.
func (qi QueryIntent) String() string {
	switch qi {
	case QueryIntentReadOnly:
		return "ReadOnly"
	case QueryIntentStateChange:
		return "StateChange"
	default:
		return "Unknown"
	}
}
