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

package manager

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"google.golang.org/protobuf/proto"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	consensusdatapb "github.com/multigres/multigres/go/pb/consensusdata"
)

// ConsensusState manages the in-memory and on-disk consensus state for this node.
// It provides thread-safe access to consensus state and ensures that memory is only
// updated after successful disk writes (pessimistic approach).
//
// Persisted state (revocation) is written to disk before updating memory.
// In-progress state (inProgressProposal, highestKnownDecision) is memory-only.
type ConsensusState struct {
	poolerDir string
	serviceID *clustermetadatapb.ID

	mu         sync.Mutex
	revocation *consensusdatapb.TermRevocation // cached revocation from disk

	// Memory-only fields — not persisted.
	inProgressProposal   *consensusdatapb.CoordinatorProposal
	highestKnownDecision *consensusdatapb.ShardRule
}

// NewConsensusState creates a new ConsensusState manager.
// It does not load state from disk - call Load() to initialize.
func NewConsensusState(poolerDir string, serviceID *clustermetadatapb.ID) *ConsensusState {
	return &ConsensusState{
		poolerDir:  poolerDir,
		serviceID:  serviceID,
		revocation: nil,
	}
}

// Load loads consensus state from disk into memory.
// If the file doesn't exist, initializes with default values (revoked_below_term 0, no accepted coordinator).
// This method is idempotent - subsequent calls will reload from disk.
func (cs *ConsensusState) Load() (int64, error) {
	revocation, err := cs.getConsensusTerm()
	if err != nil {
		return 0, fmt.Errorf("failed to load consensus term: %w", err)
	}

	cs.mu.Lock()
	cs.revocation = revocation
	cs.mu.Unlock()

	return revocation.RevokedBelowTerm, nil
}

// GetRevokedBelowTerm returns the current revoked_below_term.
// Returns 0 if state has not been loaded.
func (cs *ConsensusState) GetRevokedBelowTerm(ctx context.Context) (int64, error) {
	if err := AssertActionLockHeld(ctx); err != nil {
		return 0, err
	}
	return cs.GetInconsistentRevokedBelowTerm()
}

// GetInconsistentRevokedBelowTerm returns the current revoked_below_term for monitoring.
// It doesn't require the action lock to be held, so the value returned may
// be outdated by the time it's used. Use GetRevokedBelowTerm() as part of
// any action workflow to protect against race conditions.
// Returns 0 if state has not been loaded.
func (cs *ConsensusState) GetInconsistentRevokedBelowTerm() (int64, error) {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	if cs.revocation == nil {
		return 0, nil
	}
	return cs.revocation.GetRevokedBelowTerm(), nil
}

// GetInconsistentRevocation returns a copy of the current term revocation for monitoring.
// It doesn't require the action lock to be held, so the value returned may
// be outdated by the time it's used. Use GetRevocation() as part of any action
// workflow to protect against race conditions.
// Returns nil if state has not been loaded.
func (cs *ConsensusState) GetInconsistentRevocation() (*consensusdatapb.TermRevocation, error) {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	if cs.revocation == nil {
		return nil, nil
	}

	return cloneRevocation(cs.revocation), nil
}

// GetAcceptedLeader returns the coordinator ID this pooler accepted the term from.
// Returns empty string if no coordinator was accepted.
func (cs *ConsensusState) GetAcceptedLeader(ctx context.Context) (string, error) {
	if err := AssertActionLockHeld(ctx); err != nil {
		return "", err
	}
	cs.mu.Lock()
	defer cs.mu.Unlock()

	if cs.revocation == nil || cs.revocation.AcceptedCoordinatorId == nil {
		return "", nil
	}
	return cs.revocation.AcceptedCoordinatorId.GetName(), nil
}

// GetRevocation returns a copy of the current term revocation.
// Returns nil if state has not been loaded.
func (cs *ConsensusState) GetRevocation(ctx context.Context) (*consensusdatapb.TermRevocation, error) {
	if err := AssertActionLockHeld(ctx); err != nil {
		return nil, err
	}
	cs.mu.Lock()
	defer cs.mu.Unlock()

	if cs.revocation == nil {
		return nil, nil
	}

	return cloneRevocation(cs.revocation), nil
}

// CanAcceptRevocation performs the same validation as AcceptRevocation but does
// not write to disk. Used as a pre-check in Recruit before halting writes.
// Requires the action lock.
func (cs *ConsensusState) CanAcceptRevocation(ctx context.Context, revocation *consensusdatapb.TermRevocation) error {
	if err := AssertActionLockHeld(ctx); err != nil {
		return err
	}
	cs.mu.Lock()
	defer cs.mu.Unlock()

	if revocation == nil {
		return errors.New("revocation cannot be nil")
	}

	currentTerm := int64(0)
	if cs.revocation != nil {
		currentTerm = cs.revocation.GetRevokedBelowTerm()
	}

	newTerm := revocation.GetRevokedBelowTerm()

	if newTerm < currentTerm {
		return fmt.Errorf("cannot accept stale revocation: current=%d, new=%d", currentTerm, newTerm)
	}

	if newTerm == currentTerm && cs.revocation != nil {
		sameCoordinator := cs.revocation.AcceptedCoordinatorId != nil && revocation.AcceptedCoordinatorId != nil &&
			proto.Equal(cs.revocation.AcceptedCoordinatorId, revocation.AcceptedCoordinatorId)
		if sameCoordinator {
			sameTimestamp := proto.Equal(cs.revocation.CoordinatorInitiatedAt, revocation.CoordinatorInitiatedAt)
			if !sameTimestamp {
				return fmt.Errorf("same coordinator %s at term %d but different timestamp",
					cs.revocation.AcceptedCoordinatorId.GetName(), currentTerm)
			}
			return nil // would be idempotent success
		}
		if cs.revocation.AcceptedCoordinatorId != nil {
			return fmt.Errorf("already accepted term %d from coordinator %s",
				currentTerm, cs.revocation.AcceptedCoordinatorId.GetName())
		}
	}

	return nil
}

// AcceptRevocation accepts a TermRevocation from the coordinator (called from Recruit).
//
// Acceptance rules:
//   - revocation.RevokedBelowTerm > current.RevokedBelowTerm → accept and persist.
//   - revocation.RevokedBelowTerm == current.RevokedBelowTerm and same coordinator → idempotent success.
//   - revocation.RevokedBelowTerm == current.RevokedBelowTerm but different coordinator → reject.
//   - revocation.RevokedBelowTerm < current.RevokedBelowTerm → reject (stale).
func (cs *ConsensusState) AcceptRevocation(ctx context.Context, revocation *consensusdatapb.TermRevocation) error {
	if err := AssertActionLockHeld(ctx); err != nil {
		return err
	}
	cs.mu.Lock()
	defer cs.mu.Unlock()

	if revocation == nil {
		return errors.New("revocation cannot be nil")
	}

	currentTerm := int64(0)
	if cs.revocation != nil {
		currentTerm = cs.revocation.GetRevokedBelowTerm()
	}

	newTerm := revocation.GetRevokedBelowTerm()

	if newTerm < currentTerm {
		return fmt.Errorf("cannot accept stale revocation: current=%d, new=%d", currentTerm, newTerm)
	}

	if newTerm == currentTerm && cs.revocation != nil {
		// Same term: idempotent only if coordinator AND timestamp both match.
		// A different timestamp at the same term from the same coordinator indicates
		// a distinct recruitment attempt, which must be rejected for safety.
		sameCoordinator := cs.revocation.AcceptedCoordinatorId != nil && revocation.AcceptedCoordinatorId != nil &&
			proto.Equal(cs.revocation.AcceptedCoordinatorId, revocation.AcceptedCoordinatorId)
		if sameCoordinator {
			sameTimestamp := proto.Equal(cs.revocation.CoordinatorInitiatedAt, revocation.CoordinatorInitiatedAt)
			if sameTimestamp {
				return nil // idempotent success
			}
			return fmt.Errorf("same coordinator %s at term %d but different timestamp: stored=%v new=%v",
				cs.revocation.AcceptedCoordinatorId.GetName(), currentTerm,
				cs.revocation.CoordinatorInitiatedAt.AsTime(),
				revocation.CoordinatorInitiatedAt.AsTime())
		}
		if cs.revocation.AcceptedCoordinatorId != nil {
			return fmt.Errorf("already accepted term %d from coordinator %s",
				currentTerm, cs.revocation.AcceptedCoordinatorId.GetName())
		}
	}

	return cs.saveAndUpdateLocked(cloneRevocation(revocation))
}

// saveAndUpdateLocked saves the revocation to disk and updates memory.
// MUST be called with cs.mu held.
// This is the key method that ensures memory never diverges from disk.
// If the save fails, memory remains unchanged and the error is returned.
func (cs *ConsensusState) saveAndUpdateLocked(newRevocation *consensusdatapb.TermRevocation) error {
	// Save to disk (lock still held)
	if err := cs.setConsensusTerm(newRevocation); err != nil {
		// Save failed - don't update memory, propagate error
		return fmt.Errorf("failed to save consensus term: %w", err)
	}

	// Save succeeded - NOW update memory
	cs.revocation = cloneRevocation(newRevocation)
	return nil
}

// SetInProgressProposal records the coordinator proposal currently being processed.
// This is memory-only and not persisted to disk.
func (cs *ConsensusState) SetInProgressProposal(proposal *consensusdatapb.CoordinatorProposal) {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	cs.inProgressProposal = proposal
}

// GetInProgressProposal returns the coordinator proposal currently being processed.
// Returns nil if no proposal is in progress.
func (cs *ConsensusState) GetInProgressProposal() *consensusdatapb.CoordinatorProposal {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	return cs.inProgressProposal
}

// SetHighestKnownDecision records the most recently committed ShardRule known to this node.
// This is memory-only and not persisted to disk.
func (cs *ConsensusState) SetHighestKnownDecision(rule *consensusdatapb.ShardRule) {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	cs.highestKnownDecision = rule
}

// GetHighestKnownDecision returns the most recently committed ShardRule known to this node.
// Returns nil if no decision has been received yet.
func (cs *ConsensusState) GetHighestKnownDecision() *consensusdatapb.ShardRule {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	return cs.highestKnownDecision
}

// cloneRevocation creates a deep copy of a TermRevocation.
func cloneRevocation(revocation *consensusdatapb.TermRevocation) *consensusdatapb.TermRevocation {
	if revocation == nil {
		return nil
	}
	return proto.Clone(revocation).(*consensusdatapb.TermRevocation)
}
