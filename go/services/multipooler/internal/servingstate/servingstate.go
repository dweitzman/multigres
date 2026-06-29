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

// Package servingstate defines the effective serving state that the multipooler
// manager's StateManager delivers to components via OnStateChange.
//
// It lives in its own leaf package because the components that react to state
// changes (heartbeat, pubsub, query server) satisfy the manager's StateAware
// interface structurally and cannot import the manager package back.
package servingstate

import (
	"google.golang.org/protobuf/proto"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
)

// State is the effective serving state components react to.
type State struct {
	// Routing is the self-reported routing/HA role plus the rule that qualifies
	// it. role == PRIMARY is the writable signal (see Writable): the conjunction
	// of postgres being out of recovery, our highest non-revoked *committed* rule
	// naming us (never true on a not-yet-committed rule — the pg_promote()→commit
	// window), AND being the highest-known leader (so a superseded stale primary
	// resigns the moment it learns a newer leader, before it can rewind). The
	// qualifying rule is the committed rule when PRIMARY (write authority) and the
	// highest-known rule when REPLICA (advisory). Published verbatim to the gateway.
	Routing *clustermetadatapb.RoutingState

	// ServingStatus is the serving intent (SERVING / DISABLED / DRAINING).
	ServingStatus clustermetadatapb.PoolerServingStatus
}

// Writable reports write-safety: it is true iff the routing role is PRIMARY. It
// is a derived accessor (the role already encodes it), used to gate write
// traffic — heartbeats, LISTEN/NOTIFY, query admission, and the gateway's writes.
func (s State) Writable() bool {
	return s.Routing.GetRole() == clustermetadatapb.RoutingRole_ROUTING_ROLE_PRIMARY
}

// Equal reports whether two States are equivalent. It exists because State
// carries a proto RoutingState pointer and so cannot be compared with ==; the
// StateManager uses it to dedup redundant fan-outs.
func (s State) Equal(o State) bool {
	return s.ServingStatus == o.ServingStatus &&
		proto.Equal(s.Routing, o.Routing)
}
