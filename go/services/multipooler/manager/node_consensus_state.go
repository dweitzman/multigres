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

package manager

import (
	"context"
	"fmt"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
)

// nodeConsensusState is the in-memory source of truth for building a
// ConsensusStatus proto without any database I/O. The health streamer reads
// directly from this via buildConsensusStatus, so health pushes are zero-I/O.
//
// Update paths:
//   - rules.updateRule / rules.observePosition: any DB operation that writes or
//     reads the current rule updates the position cache in ruleStore and fires
//     onChange if the rule number changed.
//   - term changes: wired via termRevocation.SetOnTermChange → healthStreamer.Broadcast.
//
// Rule changes fire onChange immediately; LSN-only updates are silent — the
// periodic heartbeat delivers them.
type nodeConsensusState struct {
	// revokedUntil holds the coordinator promise (term + accepted coordinator).
	// Mutations are owned by termRevocation and fire healthStreamer.Broadcast
	// directly via SetOnTermChange.
	revokedUntil *termRevocation

	// rules owns the position cache (rule + WAL LSN) and all DB operations that
	// read or write the current rule.
	rules *ruleStore

	// highestKnownRule is not yet populated; it will be added once the pooler
	// receives forward rule knowledge from the coordinator.
}

// newNodeConsensusState creates a nodeConsensusState.
func newNodeConsensusState(revokedUntil *termRevocation, rules *ruleStore) *nodeConsensusState {
	return &nodeConsensusState{
		revokedUntil: revokedUntil,
		rules:        rules,
	}
}

// buildConsensusStatus assembles a ConsensusStatus proto from the cached state.
// Zero I/O — reads only from memory. Returns nil if nothing has been loaded yet.
func (ncs *nodeConsensusState) buildConsensusStatus() *clustermetadatapb.ConsensusStatus {
	term, _ := ncs.revokedUntil.GetInconsistentTerm()
	pos := ncs.rules.cachedPosition()

	if term == nil && pos == nil {
		return nil
	}

	status := &clustermetadatapb.ConsensusStatus{}

	if term != nil {
		status.Promise = &clustermetadatapb.HighestCoordinatorPromise{
			TermNumber:            term.TermNumber,
			AcceptedCoordinatorId: term.AcceptedTermFromCoordinatorId,
		}
	}

	if pos != nil {
		ruleNumber := &clustermetadatapb.RuleNumber{
			CoordinatorTerm: pos.rule.CoordinatorTerm,
			RuleSubterm:     pos.rule.RuleSubterm,
		}
		var primaryID *clustermetadatapb.ID
		if pos.rule.LeaderID != nil {
			primaryID = pos.rule.LeaderID.id
		}
		cohortIDs := make([]*clustermetadatapb.ID, 0, len(pos.rule.CohortMembers))
		for _, m := range pos.rule.CohortMembers {
			cohortIDs = append(cohortIDs, m.id)
		}
		status.CurrentPosition = &clustermetadatapb.NodePosition{
			Rule: &clustermetadatapb.ShardRule{
				RuleNumber:    ruleNumber,
				PrimaryId:     primaryID,
				CohortMembers: cohortIDs,
			},
			Lsn: pos.lsn,
		}
	}

	return status
}

// claimCurrentAuthority attempts to claim authority for the given term and
// coordinator. It durably records the promise if the request satisfies consensus
// rules (term ≥ current, or same term with same coordinator).
//
// Returns the current ConsensusStatus and nil on success, or the current
// ConsensusStatus and a non-nil error if the claim was rejected or could not
// be persisted. The ConsensusStatus is always built from the in-memory cache
// (zero I/O), so callers should call refreshNodePosition first if an accurate
// LSN is required.
//
// TODO: Accept coordinator_initiated_at (from BeginTermRequest once the
// coordinator starts sending it) to detect coordinator restarts — a coordinator
// that crashes and forgets its prior session should not be able to re-claim the
// same (term, ID) pair as an idempotent retry.
//
// TODO: Distinguish rejection (stale term, conflicting coordinator) from
// infrastructure failure (disk write error) so the caller can propagate
// infrastructure errors rather than silently treating them as rejections.
func (ncs *nodeConsensusState) claimCurrentAuthority(ctx context.Context, term int64, coordinatorID *clustermetadatapb.ID) (*clustermetadatapb.ConsensusStatus, error) {
	err := ncs.revokedUntil.UpdateTermAndAcceptCandidate(ctx, term, coordinatorID)
	return ncs.buildConsensusStatus(), err
}

// refreshNodePosition queries postgres for the current rule and WAL position,
// updates the cache, and returns the resulting ConsensusStatus. Call this
// before building responses for RPCs that need an accurate LSN (BeginTerm,
// Status, Promote, EmergencyDemote).
//
// Caller must hold the action lock.
func (pm *MultiPoolerManager) refreshNodePosition(ctx context.Context) (*clustermetadatapb.ConsensusStatus, error) {
	if _, err := pm.rules.observePosition(ctx); err != nil {
		return nil, fmt.Errorf("failed to observe node position: %w", err)
	}
	return pm.nodeConsensus.buildConsensusStatus(), nil
}
