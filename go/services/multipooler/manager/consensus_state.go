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
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/multigres/multigres/go/common/mterrors"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
)

// termRevocation manages the in-memory and on-disk consensus state for this node.
// It provides thread-safe access to consensus state and ensures that memory is only
// updated after successful disk writes (pessimistic approach).
type termRevocation struct {
	poolerDir string
	serviceID *clustermetadatapb.ID

	mu   sync.Mutex
	term *multipoolermanagerdatapb.ConsensusTerm // cached term from disk

	// onTermChange is called (outside mu) after any successful term mutation.
	onTermChange func()
}

// newTermRevocation creates a new termRevocation manager.
// It does not load state from disk - call Load() to initialize.
func newTermRevocation(poolerDir string, serviceID *clustermetadatapb.ID) *termRevocation {
	return &termRevocation{
		poolerDir: poolerDir,
		serviceID: serviceID,
		term:      nil,
	}
}

// SetOnTermChange registers a callback invoked after any successful term
// mutation. The callback is called outside mu with no locks held.
func (cs *termRevocation) SetOnTermChange(fn func()) {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	cs.onTermChange = fn
}

// Load loads consensus state from disk into memory.
// If the file doesn't exist, initializes with default values (term 0, no accepted coordinator).
// This method is idempotent - subsequent calls will reload from disk.
func (cs *termRevocation) Load() (int64, error) {
	term, err := cs.getConsensusTerm()
	if err != nil {
		return 0, fmt.Errorf("failed to load consensus term: %w", err)
	}

	cs.mu.Lock()
	cs.term = term
	cs.mu.Unlock()

	return term.TermNumber, nil
}

// GetCurrentTermNumber returns the current term.
// Returns 0 if state has not been loaded.
func (cs *termRevocation) GetCurrentTermNumber(ctx context.Context) (int64, error) {
	if err := AssertActionLockHeld(ctx); err != nil {
		return 0, err
	}
	return cs.GetInconsistentCurrentTermNumber()
}

// GetInconsistentCurrentTermNumber returns the current term for monitoring.
// It doesn't require the action lock to be held, so the value returned may
// be outdated by the time it's used. Use GetCurrentTermNumber() as part of
// any action workflow to protect against race conditions.
// Returns 0 if state has not been loaded.
func (cs *termRevocation) GetInconsistentCurrentTermNumber() (int64, error) {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	if cs.term == nil {
		return 0, nil
	}
	return cs.term.GetTermNumber(), nil
}

// GetInconsistentTerm returns a copy of the current consensus term for monitoring.
// It doesn't require the action lock to be held, so the value returned may
// be outdated by the time it's used. Use GetTerm() as part of any action
// workflow to protect against race conditions.
// Returns nil if state has not been loaded.
func (cs *termRevocation) GetInconsistentTerm() (*multipoolermanagerdatapb.ConsensusTerm, error) {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	if cs.term == nil {
		return nil, nil
	}

	// Return a copy to prevent external modifications
	return cloneTerm(cs.term), nil
}

// GetTerm returns a copy of the current consensus term.
// Returns nil if state has not been loaded.
func (cs *termRevocation) GetTerm(ctx context.Context) (*multipoolermanagerdatapb.ConsensusTerm, error) {
	if err := AssertActionLockHeld(ctx); err != nil {
		return nil, err
	}
	cs.mu.Lock()
	defer cs.mu.Unlock()

	if cs.term == nil {
		return nil, nil
	}

	// Return a copy to prevent external modifications
	return cloneTerm(cs.term), nil
}

// AcceptCandidateAndSave atomically records acceptance of the term from a coordinator.
// This is called when a node accepts the term during BeginTerm.
// Returns error if already accepted from a different coordinator in this term.
// Idempotent: succeeds if already accepted from the same coordinator.
// Deprecated: prefer node position and/or highest known rule
func (cs *termRevocation) AcceptCandidateAndSave(ctx context.Context, candidateID *clustermetadatapb.ID) error {
	if err := AssertActionLockHeld(ctx); err != nil {
		return err
	}
	cs.mu.Lock()
	defer cs.mu.Unlock()

	if cs.term == nil {
		return errors.New("consensus term not initialized")
	}

	if candidateID == nil {
		return errors.New("candidate ID cannot be nil")
	}

	// If already accepted from this coordinator, idempotent success
	if cs.term.AcceptedTermFromCoordinatorId != nil && proto.Equal(cs.term.AcceptedTermFromCoordinatorId, candidateID) {
		return nil
	}

	// Check if already accepted from someone else in this term
	if cs.term.AcceptedTermFromCoordinatorId != nil {
		return fmt.Errorf("already accepted term from %s in term %d",
			cs.term.AcceptedTermFromCoordinatorId.GetName(), cs.term.TermNumber)
	}

	// Prepare acceptance
	newTerm := cloneTerm(cs.term)

	// Update acceptance - use proto.Clone to ensure deep copy
	newTerm.AcceptedTermFromCoordinatorId = proto.Clone(candidateID).(*clustermetadatapb.ID)

	// Update last acceptance time
	now := time.Now()
	newTerm.LastAcceptanceTime = timestamppb.New(now)

	// Save and update under lock
	return cs.saveAndUpdateLocked(newTerm)
}

// UpdateTermAndAcceptCandidate atomically updates the term and accepts a candidate in one file write.
// This is used by BeginTerm to avoid two separate file writes.
// If newTerm > currentTerm, updates term and resets acceptance, then sets the candidate.
// If newTerm == currentTerm, just accepts the candidate (idempotent for same coordinator).
// Returns error if newTerm < currentTerm.
func (cs *termRevocation) UpdateTermAndAcceptCandidate(ctx context.Context, newTerm int64, candidateID *clustermetadatapb.ID) error {
	if err := AssertActionLockHeld(ctx); err != nil {
		return err
	}
	cs.mu.Lock()
	defer cs.mu.Unlock()

	if candidateID == nil {
		return errors.New("candidate ID cannot be nil")
	}

	currentTerm := int64(0)
	currentPrimaryTerm := int64(0)
	if cs.term != nil {
		currentTerm = cs.term.GetTermNumber()
		currentPrimaryTerm = cs.term.GetPrimaryTerm()
	}

	if newTerm < currentTerm {
		return fmt.Errorf("cannot update to older term: current=%d, new=%d", currentTerm, newTerm)
	}

	var newTermProto *multipoolermanagerdatapb.ConsensusTerm

	if newTerm > currentTerm {
		// Higher term: create new term with the candidate already set
		newTermProto = &multipoolermanagerdatapb.ConsensusTerm{
			TermNumber:                    newTerm,
			AcceptedTermFromCoordinatorId: proto.Clone(candidateID).(*clustermetadatapb.ID),
			LastAcceptanceTime:            timestamppb.New(time.Now()),
			LeaderId:                      nil,
			PrimaryTerm:                   currentPrimaryTerm,
		}
	} else {
		// Same term: just update acceptance (idempotent check first)
		if cs.term == nil {
			return errors.New("consensus term not initialized")
		}

		// If already accepted from this coordinator, idempotent success
		if cs.term.AcceptedTermFromCoordinatorId != nil && proto.Equal(cs.term.AcceptedTermFromCoordinatorId, candidateID) {
			return nil
		}

		// Check if already accepted from someone else in this term
		if cs.term.AcceptedTermFromCoordinatorId != nil {
			return fmt.Errorf("already accepted term from %s in term %d",
				cs.term.AcceptedTermFromCoordinatorId.GetName(), cs.term.TermNumber)
		}

		// Prepare acceptance
		newTermProto = cloneTerm(cs.term)
		newTermProto.AcceptedTermFromCoordinatorId = proto.Clone(candidateID).(*clustermetadatapb.ID)
		newTermProto.LastAcceptanceTime = timestamppb.New(time.Now())
	}

	// Single file write
	return cs.saveAndUpdateLocked(newTermProto)
}

// UpdateTermAndSave atomically updates the term number, resetting accepted coordinator.
// This is called when discovering a newer term from another node.
// Returns error if newTerm < currentTerm.
// Idempotent: succeeds without changes if newTerm == currentTerm.
// Deprecated: prefer node position and/or highest known rule
func (cs *termRevocation) UpdateTermAndSave(ctx context.Context, newTerm int64) error {
	if err := AssertActionLockHeld(ctx); err != nil {
		return err
	}
	cs.mu.Lock()
	defer cs.mu.Unlock()

	currentTerm := int64(0)
	currentPrimaryTerm := int64(0)
	if cs.term != nil {
		currentTerm = cs.term.GetTermNumber()
		currentPrimaryTerm = cs.term.GetPrimaryTerm()
	}

	if newTerm < currentTerm {
		return fmt.Errorf("cannot update to older term: current=%d, new=%d", currentTerm, newTerm)
	}

	// If same term, nothing to do (idempotent success)
	if newTerm == currentTerm {
		return nil
	}

	// Only if newTerm > currentTerm: create new term with reset acceptance
	term := &multipoolermanagerdatapb.ConsensusTerm{
		TermNumber:                    newTerm,
		AcceptedTermFromCoordinatorId: nil,
		LastAcceptanceTime:            nil,
		LeaderId:                      nil,
		PrimaryTerm:                   currentPrimaryTerm,
	}

	// Save and update under lock
	return cs.saveAndUpdateLocked(term)
}

// SetPrimaryTerm updates the primary term in the consensus record.
// This is called during propagation when a multipooler is promoted to primary.
// The force parameter bypasses invariant validation for manual intervention (e.g., split-brain recovery).
// Deprecated: prefer node position and/or highest known rule
func (cs *termRevocation) SetPrimaryTerm(ctx context.Context, primaryTerm int64, force bool) error {
	if err := AssertActionLockHeld(ctx); err != nil {
		return err
	}

	// Validation: negative values are never allowed
	if primaryTerm < 0 {
		return mterrors.New(mtrpcpb.Code_INVALID_ARGUMENT,
			fmt.Sprintf("primary_term cannot be negative: %d", primaryTerm))
	}

	cs.mu.Lock()
	defer cs.mu.Unlock()

	if cs.term == nil {
		return errors.New("consensus term not initialized")
	}

	// Invariant validation (unless force=true for manual intervention)
	if !force {
		// INVARIANT: When setting primary_term to a non-zero value, it must match the current consensus term.
		// This ensures we record the exact term in which this pooler was promoted to primary.
		// After promotion, term_number may increase (due to new elections) but primary_term remains fixed.
		// Exception: clearing primary_term to 0 (during demotion or restore) is always allowed.
		currentTermNumber := cs.term.GetTermNumber()
		isSettingNonZeroTerm := primaryTerm != 0
		termMismatch := primaryTerm != currentTermNumber

		if isSettingNonZeroTerm && termMismatch {
			return mterrors.New(mtrpcpb.Code_FAILED_PRECONDITION,
				fmt.Sprintf("primary_term must match current term when setting: primary_term=%d, current_term=%d",
					primaryTerm, currentTermNumber))
		}
	}

	newTerm := cloneTerm(cs.term)
	newTerm.PrimaryTerm = primaryTerm

	return cs.saveAndUpdateLocked(newTerm)
}

// saveAndUpdateLocked saves the term to disk and updates memory.
// MUST be called with cs.mu held.
// This is the key method that ensures memory never diverges from disk.
// If the save fails, memory remains unchanged and the error is returned.
func (cs *termRevocation) saveAndUpdateLocked(newTerm *multipoolermanagerdatapb.ConsensusTerm) error {
	// Save to disk (lock still held)
	if err := cs.setConsensusTerm(newTerm); err != nil {
		// Save failed - don't update memory, propagate error
		return fmt.Errorf("failed to save consensus term: %w", err)
	}

	// Save succeeded - NOW update memory
	cs.term = cloneTerm(newTerm)
	return nil
}

// cloneTerm creates a deep copy of a ConsensusTerm
func cloneTerm(term *multipoolermanagerdatapb.ConsensusTerm) *multipoolermanagerdatapb.ConsensusTerm {
	if term == nil {
		return nil
	}
	return proto.Clone(term).(*multipoolermanagerdatapb.ConsensusTerm)
}
