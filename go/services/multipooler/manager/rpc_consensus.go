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
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/multigres/multigres/go/common/eventlog"
	"github.com/multigres/multigres/go/common/mterrors"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	consensusdatapb "github.com/multigres/multigres/go/pb/consensusdata"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
	"github.com/multigres/multigres/go/services/multipooler/executor"
)

// BeginTerm handles coordinator requests during leader appointments.
// It consists of two phases:
//
// 1. Term Acceptance: Accept the new term based on consensus rules
//   - Term must be >= current term
//   - Cannot accept different coordinator for same term
//   - Atomically update term and accept candidate
//
// 2. Action Execution: Execute the specified action after term acceptance
//   - NO_ACTION: Do nothing
//   - REVOKE: Demote primary or pause standby replication to revoke old term
func (pm *MultiPoolerManager) BeginTerm(ctx context.Context, req *consensusdatapb.BeginTermRequest) (_ *consensusdatapb.BeginTermResponse, retErr error) {
	// Acquire the action lock to ensure only one consensus operation runs at a time
	// This prevents split-brain acceptance and ensures term updates are serialized
	var err error
	ctx, err = pm.actionLock.Acquire(ctx, "BeginTerm")
	if err != nil {
		return nil, err
	}
	defer pm.actionLock.Release(ctx)

	// Log the action type for observability
	pm.logger.InfoContext(ctx, "BeginTerm received",
		"term", req.Term,
		"candidate_id", req.CandidateId.GetName(),
		"action", req.Action.String(),
		"shard_id", req.ShardId)

	// Validate action
	switch req.Action {
	case consensusdatapb.BeginTermAction_BEGIN_TERM_ACTION_REVOKE:
		// Valid action
	case consensusdatapb.BeginTermAction_BEGIN_TERM_ACTION_NO_ACTION:
		// Valid action
	case consensusdatapb.BeginTermAction_BEGIN_TERM_ACTION_UNSPECIFIED:
		return nil, mterrors.New(mtrpcpb.Code_INVALID_ARGUMENT,
			"action must be specified (cannot be UNSPECIFIED)")
	default:
		return nil, mterrors.Errorf(mtrpcpb.Code_INVALID_ARGUMENT,
			"unknown BeginTerm action type: %v", req.Action)
	}

	// ========================================================================
	// Term Acceptance (Consensus Rules)
	// ========================================================================

	pm.mu.Lock()
	cs := pm.consensusState
	pm.mu.Unlock()

	if cs == nil {
		return nil, errors.New("consensus state not initialized")
	}

	// Get current term for response
	currentTerm, err := cs.GetCurrentTermNumber(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get current term: %w", err)
	}

	// Atomically update term and accept candidate
	// This handles all consensus rules: term validation, duplicate check, etc.
	err = cs.UpdateTermAndAcceptCandidate(ctx, req.Term, req.CandidateId)
	if err != nil {
		// Term not accepted - return rejection with current consensus status so
		// the coordinator gains up-to-date node state even from rejections.
		pm.logger.InfoContext(ctx, "Term not accepted",
			"request_term", req.Term,
			"current_term", currentTerm,
			"error", err)
		consensusStatus, statusErr := pm.getConsensusStatus(ctx)
		if statusErr != nil {
			pm.logger.WarnContext(ctx, "Failed to build consensus status for rejection response", "error", statusErr)
		}
		return &consensusdatapb.BeginTermResponse{
			Term:            currentTerm,
			Accepted:        false,
			PoolerId:        pm.serviceID.GetName(),
			ConsensusStatus: consensusStatus,
		}, nil
	}

	pm.logger.InfoContext(ctx, "Term accepted",
		"term", req.Term,
		"coordinator", req.CandidateId.GetName())

	// Determine revoked role before executing any action (needed for event)
	revokedRole := ""
	if req.Action == consensusdatapb.BeginTermAction_BEGIN_TERM_ACTION_REVOKE {
		if primary, err := pm.isPrimary(ctx); err == nil {
			if primary {
				revokedRole = "primary"
			} else {
				revokedRole = "standby"
			}
		}
	}

	termEvent := eventlog.TermBegin{
		NewTerm:      req.Term,
		PreviousTerm: currentTerm,
		RevokedRole:  revokedRole,
	}
	eventlog.Emit(ctx, pm.logger, eventlog.Started, termEvent)
	defer func() {
		if retErr == nil {
			eventlog.Emit(ctx, pm.logger, eventlog.Success, termEvent)
		} else {
			eventlog.Emit(ctx, pm.logger, eventlog.Failed, termEvent, "error", retErr)
		}
	}()

	response := &consensusdatapb.BeginTermResponse{
		Term:     req.Term,
		Accepted: true,
		PoolerId: pm.serviceID.GetName(),
	}

	// ========================================================================
	// Action Execution
	// ========================================================================

	switch req.Action {
	case consensusdatapb.BeginTermAction_BEGIN_TERM_ACTION_NO_ACTION:
		consensusStatus, statusErr := pm.getConsensusStatus(ctx)
		if statusErr != nil {
			pm.logger.WarnContext(ctx, "Failed to build consensus status", "error", statusErr)
		}
		response.ConsensusStatus = consensusStatus
		return response, nil

	case consensusdatapb.BeginTermAction_BEGIN_TERM_ACTION_REVOKE:
		if err := pm.executeRevoke(ctx, req.Term, response); err != nil {
			// Term was already accepted and persisted above, so we must return
			// the response with accepted=true AND the error. This tells the coordinator:
			// 1. The term was accepted (response.Accepted = true)
			// 2. The revoke action failed (error != nil)
			pm.logger.ErrorContext(ctx, "Term accepted but revoke action failed",
				"term", req.Term,
				"error", err)
			return response, mterrors.Wrap(err, "term accepted but revoke action failed")
		}
		return response, nil

	default:
		// Should never reach here due to validation above
		return response, nil
	}
}

// executeRevoke executes the REVOKE action by demoting primary or pausing standby replication.
// This is called after the term has been accepted.
func (pm *MultiPoolerManager) executeRevoke(ctx context.Context, term int64, response *consensusdatapb.BeginTermResponse) error {
	// CRITICAL: Must be able to reach Postgres to execute revoke
	if _, err := pm.query(ctx, "SELECT 1"); err != nil {
		return mterrors.Wrap(err, "postgres unhealthy, cannot execute revoke")
	}

	isPrimary, err := pm.isPrimary(ctx)
	if err != nil {
		return mterrors.Wrap(err, "failed to determine role for revoke")
	}

	response.WalPosition = &consensusdatapb.WALPosition{
		Timestamp: timestamppb.Now(),
	}

	if isPrimary {
		// Revoke primary: demote
		// TODO: Implement graceful (non-emergency) demote for planned failovers.
		// This emergency demote path will remain for BeginTerm REVOKE actions.
		pm.logger.InfoContext(ctx, "Revoking primary", "term", term)
		drainTimeout := 5 * time.Second
		demoteResp, err := pm.emergencyDemoteLocked(ctx, term, drainTimeout)
		if err != nil {
			return mterrors.Wrap(err, "failed to demote primary during revoke")
		}
		response.WalPosition.CurrentLsn = demoteResp.LsnPosition
		pm.logger.InfoContext(ctx, "Primary demoted", "lsn", demoteResp.LsnPosition, "term", term)
	} else {
		// Revoke standby: stop receiver and wait for replay to catch up
		pm.logger.InfoContext(ctx, "Revoking standby", "term", term)

		// Stop WAL receiver and wait for it to fully disconnect
		_, err := pm.pauseReplication(
			ctx,
			multipoolermanagerdatapb.ReplicationPauseMode_REPLICATION_PAUSE_MODE_RECEIVER_ONLY,
			true /* wait */)
		if err != nil {
			return mterrors.Wrap(err, "failed to pause replication during revoke")
		}

		// Wait for replay to finish processing all WAL that is on disk
		status, err := pm.waitForReplayStabilize(ctx)
		if err != nil {
			return mterrors.Wrap(err, "failed waiting for replay to stabilize during revoke")
		}

		response.WalPosition.LastReceiveLsn = status.LastReceiveLsn
		response.WalPosition.LastReplayLsn = status.LastReplayLsn
		pm.logger.InfoContext(ctx, "Standby revoke complete",
			"term", term,
			"last_receive_lsn", status.LastReceiveLsn,
			"last_replay_lsn", status.LastReplayLsn)
	}

	// Always capture timeline ID after WAL positions are frozen.
	// Retained for observability only; does not affect candidate selection.
	timelineID, err := pm.getTimelineID(ctx)
	if err != nil {
		pm.logger.WarnContext(ctx, "Failed to get timeline ID during revoke; observability data will be incomplete",
			"term", term, "error", err)
	} else {
		response.WalPosition.TimelineId = timelineID
		pm.logger.InfoContext(ctx, "Captured timeline ID for observability",
			"term", term, "timeline_id", timelineID)
	}

	// Capture consensus status after WAL positions are frozen (post-revoke snapshot).
	// Also extract leadership_term and cohort_members for WAL position candidate
	// selection: a node that has seen a higher coordinator term has applied more of
	// the agreed WAL history, so this is the primary criterion (LSN is a tiebreaker).
	consensusStatus, statusErr := pm.getConsensusStatus(ctx)
	if statusErr != nil {
		pm.logger.WarnContext(ctx, "Failed to build consensus status during revoke; candidate selection may be suboptimal", "error", statusErr)
	} else if pos := consensusStatus.GetCurrentPosition(); pos != nil {
		ruleNumber := pos.GetRule().GetRuleNumber()
		response.WalPosition.LeadershipTerm = ruleNumber.GetCoordinatorTerm()
		cohortIDs := pos.GetRule().GetCohortMembers()
		appNames := make([]string, 0, len(cohortIDs))
		var firstIDErr error
		for _, id := range cohortIDs {
			pid, err := newPoolerID(id)
			if err != nil && firstIDErr == nil {
				firstIDErr = err
			}
			appNames = append(appNames, pid.appName)
		}
		if firstIDErr != nil {
			pm.logger.WarnContext(ctx, "Some cohort member IDs were invalid during revoke", "error", firstIDErr)
		}
		response.WalPosition.CohortMembers = appNames
		pm.logger.InfoContext(ctx, "Captured coordinator term for candidate selection",
			"term", term, "coordinator_term", ruleNumber.GetCoordinatorTerm())
	}
	response.ConsensusStatus = consensusStatus

	return nil
}

// getInconsistentConsensusStatus builds a ConsensusStatus snapshot of this node
// without holding the action lock. Like GetInconsistentTerm, it may observe a
// partially-updated state if a concurrent operation is in progress, so it is
// suitable for observability (StatusResponse) but not for decisions that require
// a consistent view.
//
// Returns an error if postgres is unreachable, since a partial status (promise
// without current_position) could mislead callers about this node's rule position.
func (pm *MultiPoolerManager) getInconsistentConsensusStatus(ctx context.Context) (*clustermetadatapb.ConsensusStatus, error) {
	pm.mu.Lock()
	cs := pm.consensusState
	pm.mu.Unlock()

	var term *multipoolermanagerdatapb.ConsensusTerm
	if cs != nil {
		term, _ = cs.GetInconsistentTerm()
	}
	return pm.buildConsensusStatus(ctx, term, nil)
}

// getConsensusStatus builds a ConsensusStatus snapshot while holding the action lock.
// Callers must hold the action lock (i.e. ctx must have been acquired via actionLock.Acquire).
// Use this inside consensus operations (BeginTerm, executeRevoke) where a consistent
// term read is both safe and appropriate.
func (pm *MultiPoolerManager) getConsensusStatus(ctx context.Context) (*clustermetadatapb.ConsensusStatus, error) {
	pm.mu.Lock()
	cs := pm.consensusState
	pm.mu.Unlock()

	var term *multipoolermanagerdatapb.ConsensusTerm
	if cs != nil {
		var err error
		term, err = cs.GetTerm(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to read consensus term: %w", err)
		}
	}
	return pm.buildConsensusStatus(ctx, term, nil)
}

// buildConsensusStatus constructs a ConsensusStatus from a pre-fetched consensus term
// and the current postgres state (rule + WAL position from a single current_rule query).
//
// If prefetchedPos is non-nil it is used directly, avoiding a redundant DB round-trip
// when the caller already has the current position (e.g. from updateRule's return value).
// If nil, the position is fetched from postgres via currentRuleRecord.
//
// Returns an error if postgres is unreachable, since a partial status (promise
// without current_position) could mislead callers about this node's rule position.
//
// The highest_known_rule field is not yet populated; it will be added once
// the pooler tracks forward rule knowledge from the coordinator.
func (pm *MultiPoolerManager) buildConsensusStatus(ctx context.Context, term *multipoolermanagerdatapb.ConsensusTerm, prefetchedPos *clustermetadatapb.NodePosition) (*clustermetadatapb.ConsensusStatus, error) {
	status := &clustermetadatapb.ConsensusStatus{}

	if term != nil {
		status.TermRevocation = &clustermetadatapb.TermRevocation{
			RevokedBelowTerm:      term.TermNumber,
			AcceptedCoordinatorId: term.AcceptedTermFromCoordinatorId,
			// TODO: populate CoordinatorInitiatedAt once BeginTermRequest carries
			// the coordinator's initiation timestamp and ConsensusTerm persists it.
		}
	}

	var pos *clustermetadatapb.NodePosition
	if prefetchedPos != nil {
		pos = prefetchedPos
	} else {
		var err error
		pos, err = pm.currentRuleRecord(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to read current rule: %w", err)
		}
	}
	if pos != nil {
		status.CurrentPosition = pos
	}

	if av := pm.buildAvailabilityStatus(); av != nil {
		status.AvailabilityStatus = av
	}
	return status, nil
}

// buildAvailabilityStatus returns the current AvailabilityStatus for this node,
// or nil if no fitness signals are set.
func (pm *MultiPoolerManager) buildAvailabilityStatus() *clustermetadatapb.AvailabilityStatus {
	pm.mu.Lock()
	resignedTerm := pm.resignedPrimaryAtTerm
	pm.mu.Unlock()
	if resignedTerm == 0 {
		return nil
	}
	return &clustermetadatapb.AvailabilityStatus{
		ResignedPrimaryAtTerm: resignedTerm,
	}
}

// setResignedPrimaryAtTerm records that this node voluntarily resigned as primary
// at the given consensus term. The signal is included in subsequent StatusResponses
// so the coordinator can trigger an immediate election.
func (pm *MultiPoolerManager) setResignedPrimaryAtTerm(term int64) {
	pm.mu.Lock()
	pm.resignedPrimaryAtTerm = term
	pm.mu.Unlock()
}

// clearResignedPrimaryAtTerm clears the voluntary resignation signal, called when
// this node is elected as primary again.
func (pm *MultiPoolerManager) clearResignedPrimaryAtTerm() {
	pm.mu.Lock()
	pm.resignedPrimaryAtTerm = 0
	pm.mu.Unlock()
}

// ConsensusStatus returns the current status of this node for consensus
func (pm *MultiPoolerManager) ConsensusStatus(ctx context.Context, req *consensusdatapb.StatusRequest) (*consensusdatapb.StatusResponse, error) {
	// Get consensus state
	pm.mu.Lock()
	cs := pm.consensusState
	pm.mu.Unlock()

	if cs == nil {
		return nil, errors.New("consensus state not initialized")
	}

	term, err := cs.GetInconsistentTerm()
	if err != nil {
		return nil, fmt.Errorf("failed to get consensus term: %w", err)
	}

	localCurrentTerm := int64(0)
	if term != nil {
		localCurrentTerm = term.GetTermNumber()
	}
	localPrimaryTerm := int64(0)
	if term != nil {
		localPrimaryTerm = term.GetPrimaryTerm()
	}

	// Check if database is healthy by attempting a simple query
	_, healthErr := pm.query(ctx, "SELECT 1")
	isHealthy := healthErr == nil

	// Get WAL position and determine role (primary/replica)
	walPosition := &consensusdatapb.WALPosition{
		Timestamp: timestamppb.New(time.Now()),
	}
	role := "unknown"

	if isHealthy {
		// Check role and get appropriate WAL position
		isPrimary, err := pm.isPrimary(ctx)
		if err == nil {
			if isPrimary {
				// On primary: get current write position
				role = "primary"
				currentLsn, err := pm.getPrimaryLSN(ctx)
				if err == nil {
					walPosition.CurrentLsn = currentLsn
				}
			} else {
				role = "replica"
				// On standby: get receive and replay positions
				status, err := pm.queryReplicationStatus(ctx)
				if err == nil {
					walPosition.LastReceiveLsn = status.LastReceiveLsn
					walPosition.LastReplayLsn = status.LastReplayLsn
				}
			}
		}
	}

	// Get timeline information for divergence detection
	var timelineInfo *consensusdatapb.TimelineInfo
	if isHealthy {
		timelineID, err := pm.getTimelineID(ctx)
		if err == nil {
			timelineInfo = &consensusdatapb.TimelineInfo{
				TimelineId: timelineID,
				// TODO: Populate history for primaries
			}
		}
	}

	consensusStatus, statusErr := pm.getInconsistentConsensusStatus(ctx)
	if statusErr != nil {
		pm.logger.WarnContext(ctx, "Failed to build consensus status", "error", statusErr)
	}

	return &consensusdatapb.StatusResponse{
		PoolerId:           pm.serviceID.GetName(),
		CurrentTerm:        localCurrentTerm,
		WalPosition:        walPosition,
		IsHealthy:          isHealthy,
		IsEligible:         true, // TODO: implement eligibility logic based on policy
		Cell:               pm.serviceID.GetCell(),
		Role:               role,
		TimelineInfo:       timelineInfo,
		PrimaryTerm:        localPrimaryTerm,
		ConsensusStatus:    consensusStatus,
		AvailabilityStatus: pm.buildAvailabilityStatus(),
	}, nil
}

// GetLeadershipView returns leadership information from the heartbeat table
func (pm *MultiPoolerManager) GetLeadershipView(ctx context.Context, req *consensusdatapb.LeadershipViewRequest) (*consensusdatapb.LeadershipViewResponse, error) {
	if pm.replTracker == nil {
		return nil, errors.New("replication tracker not initialized")
	}

	// Use the heartbeat reader to get leadership view
	reader := pm.replTracker.HeartbeatReader()
	view, err := reader.GetLeadershipView()
	if err != nil {
		return nil, fmt.Errorf("failed to get leadership view: %w", err)
	}

	return &consensusdatapb.LeadershipViewResponse{
		LeaderId:         view.LeaderID,
		LastHeartbeat:    timestamppb.New(view.LastHeartbeat),
		ReplicationLagNs: view.ReplicationLag.Nanoseconds(),
	}, nil
}

// CanReachPrimary checks if this node can reach the specified primary
// by querying the pg_stat_wal_receiver view to check the WAL receiver status
// and verifying it's connected to the expected primary host/port
func (pm *MultiPoolerManager) CanReachPrimary(ctx context.Context, req *consensusdatapb.CanReachPrimaryRequest) (*consensusdatapb.CanReachPrimaryResponse, error) {
	// Query pg_stat_wal_receiver to check if we can reach the primary
	result, err := pm.query(ctx, "SELECT status, conninfo FROM pg_stat_wal_receiver")
	if err != nil {
		//nolint:nilerr // Error is communicated via response struct, not error return
		return &consensusdatapb.CanReachPrimaryResponse{
			Reachable:    false,
			ErrorMessage: "database connection not available",
		}, nil
	}
	var status, conninfo string
	err = executor.ScanSingleRow(result, &status, &conninfo)
	if err != nil {
		// No rows returned means we're not receiving WAL (likely not a replica or not connected)
		//nolint:nilerr // Error is communicated via response struct, not error return
		return &consensusdatapb.CanReachPrimaryResponse{
			Reachable:    false,
			ErrorMessage: "no active WAL receiver",
		}, nil
	}

	// If status is "stopping", the connection is not healthy
	if status == "stopping" {
		return &consensusdatapb.CanReachPrimaryResponse{
			Reachable:    false,
			ErrorMessage: "WAL receiver is stopping",
		}, nil
	}

	// Parse conninfo to extract host and port
	parsedConnInfo, err := parseAndRedactPrimaryConnInfo(conninfo)
	if err != nil {
		return &consensusdatapb.CanReachPrimaryResponse{
			Reachable:    false,
			ErrorMessage: fmt.Sprintf("failed to parse conninfo: %v", err),
		}, nil
	}

	// Compare with requested primary host and port
	if parsedConnInfo.Host != req.PrimaryHost {
		return &consensusdatapb.CanReachPrimaryResponse{
			Reachable:    false,
			ErrorMessage: fmt.Sprintf("WAL receiver connected to different host: expected %s, got %s", req.PrimaryHost, parsedConnInfo.Host),
		}, nil
	}

	if parsedConnInfo.Port != req.PrimaryPort {
		return &consensusdatapb.CanReachPrimaryResponse{
			Reachable:    false,
			ErrorMessage: fmt.Sprintf("WAL receiver connected to different port: expected %d, got %d", req.PrimaryPort, parsedConnInfo.Port),
		}, nil
	}

	// WAL receiver is active and connected to the expected primary
	return &consensusdatapb.CanReachPrimaryResponse{
		Reachable:    true,
		ErrorMessage: "",
	}, nil
}
