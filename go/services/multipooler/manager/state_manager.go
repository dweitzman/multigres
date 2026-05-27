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
	"log/slog"
	"sync"

	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/proto"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
)

// StateAware is implemented by components that need to react to serving state changes.
// Each component decides internally what action to take based on the target state.
// For example, a heartbeat component starts writing when (PRIMARY, SERVING) and
// stops otherwise, while a query service component adjusts which queries it accepts.
type StateAware interface {
	// OnStateChange is called when the serving state changes.
	// The component should transition to match the target state.
	OnStateChange(ctx context.Context, poolerType clustermetadatapb.PoolerType, servingStatus clustermetadatapb.PoolerServingStatus) error
}

// StateManager coordinates serving state transitions across components.
//
// The current state lives in the poolerRecord (Type and ServingStatus),
// which is the source of truth for topology.
//
// On SetState, the manager fans out OnStateChange to all registered components
// in parallel, waits for completion, then updates the record via Mutate. The
// Mutate also schedules an asynchronous publish so callers cannot forget to
// reflect the new state to etcd.
//
// SetState also keeps the record's LastObservedLeader field in sync with
// consensus state via the leaderObsFor callback. When transitioning to
// PRIMARY, the callback returns the LeaderObservation derived from the
// pooler's latest recorded consensus rule. When transitioning away from
// PRIMARY, LastObservedLeader is cleared. The Type ↔ LastObservedLeader
// invariant is enforced by poolerRecord.Mutate.
type StateManager struct {
	mu     sync.Mutex
	logger *slog.Logger

	// Current state lives in record.Type() / .ServingStatus(). The StateManager
	// is the exclusive caller of record.Mutate for Type/ServingStatus.
	record *poolerRecord

	// leaderObsFor returns the LeaderObservation to publish into the record
	// when this pooler is the leader, or nil if it isn't. Typically reads
	// the latest rule from ConsensusState.replicationPrimary and filters to
	// "names this pooler." Must not return non-nil for a leader that isn't
	// this pooler — Mutate will reject that.
	leaderObsFor func() *clustermetadatapb.LeaderObservation

	// Registered components that react to state changes.
	components []StateAware
}

// NewStateManager creates a new StateManager. leaderObsFor must return the
// LeaderObservation to associate with this pooler when it transitions to
// PRIMARY, or nil if the pooler is not currently the consensus leader.
func NewStateManager(
	logger *slog.Logger,
	record *poolerRecord,
	leaderObsFor func() *clustermetadatapb.LeaderObservation,
	components ...StateAware,
) *StateManager {
	return &StateManager{
		logger:       logger,
		record:       record,
		leaderObsFor: leaderObsFor,
		components:   components,
	}
}

// Register adds a component to be notified on state changes.
// This is used for components created after the manager (e.g., ReplTracker).
// Must not be called concurrently with SetState.
func (ssm *StateManager) Register(component StateAware) {
	ssm.mu.Lock()
	defer ssm.mu.Unlock()
	ssm.components = append(ssm.components, component)
}

// RegisterAndSync adds a component and immediately syncs it to the current state.
// This is used for components created after the initial state transition (e.g.,
// ReplTracker is created after Open() has already transitioned to SERVING).
// The component is initialized from the record, which is the source of truth.
func (ssm *StateManager) RegisterAndSync(ctx context.Context, component StateAware) error {
	ssm.mu.Lock()
	defer ssm.mu.Unlock()
	ssm.components = append(ssm.components, component)
	return component.OnStateChange(ctx, ssm.record.Type(), ssm.record.ServingStatus())
}

// SetState transitions all components to the given state in parallel and
// keeps the record's LastObservedLeader in sync via leaderObsFor. The
// record is updated only after all components converge.
//
// LastObservedLeader is derived from leaderObsFor when transitioning to
// PRIMARY, and cleared otherwise. The Type ↔ LastObservedLeader invariant
// is validated by record.Mutate; if leaderObsFor returns nil for a PRIMARY
// transition (i.e. consensusState does not name this pooler), Mutate will
// reject the transition with an error. That's intentional: a pooler that
// hasn't yet recorded itself as the consensus leader cannot become Type=PRIMARY.
//
// Returns an error if any component fails to transition or the invariant
// is violated. No-op if Type, ServingStatus, and the derived
// LastObservedLeader are all unchanged.
func (ssm *StateManager) SetState(ctx context.Context, poolerType clustermetadatapb.PoolerType, servingStatus clustermetadatapb.PoolerServingStatus) error {
	ssm.mu.Lock()
	defer ssm.mu.Unlock()

	currentType := ssm.record.Type()
	currentStatus := ssm.record.ServingStatus()
	currentObs := ssm.record.LastObservedLeader()

	var desiredObs *clustermetadatapb.LeaderObservation
	if poolerType == clustermetadatapb.PoolerType_PRIMARY {
		desiredObs = ssm.leaderObsFor()
	}

	if currentType == poolerType && currentStatus == servingStatus && proto.Equal(currentObs, desiredObs) {
		ssm.logger.InfoContext(ctx, "Serving state unchanged, skipping",
			"type", poolerType, "status", servingStatus)
		return nil
	}

	ssm.logger.InfoContext(ctx, "Setting serving state",
		"target_type", poolerType, "target_status", servingStatus,
		"current_type", currentType, "current_status", currentStatus)

	g, ctx := errgroup.WithContext(ctx)
	for _, c := range ssm.components {
		g.Go(func() error {
			return c.OnStateChange(ctx, poolerType, servingStatus)
		})
	}
	if err := g.Wait(); err != nil {
		return err
	}

	// All components converged — update the record and schedule a publish.
	// Requires the caller to hold the action lock; Mutate will return an
	// error otherwise. Mutate also validates the Type ↔ LastObservedLeader
	// invariant.
	if err := ssm.record.Mutate(ctx, func(s *MutablePoolerRecordState) {
		s.Type = poolerType
		s.ServingStatus = servingStatus
		s.LastObservedLeader = desiredObs
	}); err != nil {
		return err
	}

	ssm.logger.InfoContext(ctx, "Serving state converged",
		"type", poolerType, "status", servingStatus)

	return nil
}
