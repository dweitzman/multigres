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
	"fmt"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/multigres/multigres/go/common/mterrors"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	consensusdatapb "github.com/multigres/multigres/go/pb/consensusdata"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
)

// Recruit handles Phase 1 of the Paxos-inspired consensus protocol.
// The coordinator sends a TermRevocation; this pooler accepts it if the
// revoked_below_term is >= the current value, then freezes its WAL position
// (demotes primary or stops standby replication) so the coordinator can safely
// choose the best candidate for promotion.
func (pm *MultiPoolerManager) Recruit(ctx context.Context, req *consensusdatapb.RecruitRequest) (_ *consensusdatapb.RecruitResponse, retErr error) {
	var err error
	ctx, err = pm.actionLock.Acquire(ctx, "Recruit")
	if err != nil {
		return nil, err
	}
	defer pm.actionLock.Release(ctx)

	if req.TermRevocation == nil {
		return nil, mterrors.New(mtrpcpb.Code_INVALID_ARGUMENT, "term_revocation must be specified")
	}

	proposedTerm := req.TermRevocation.GetRevokedBelowTerm()

	pm.logger.InfoContext(ctx, "Recruit received",
		"revoked_below_term", proposedTerm,
		"coordinator_id", req.TermRevocation.GetAcceptedCoordinatorId().GetName())

	// Pre-check: verify the revocation could be accepted before we halt writes.
	// This avoids disrupting replication for a stale or conflicting request.
	if err := pm.consensusState.CanAcceptRevocation(ctx, req.TermRevocation); err != nil {
		pm.logger.InfoContext(ctx, "Revocation pre-check failed, rejecting without WAL freeze", "error", err)
		cs, _ := pm.getConsensusStatus(ctx)
		return &consensusdatapb.RecruitResponse{ConsensusStatus: cs}, mterrors.Wrap(err, "revocation pre-check failed")
	}

	// Freeze WAL first: stop writes before committing to the revocation on disk.
	// For primaries: emergency demote (rejects all writes).
	// For standbys: pause receiver and wait for any in-flight WAL to replay.
	// This ordering ensures we never record a revocation while still participating
	// in the current epoch's writes.
	//
	// freezeWAL returns the position observed immediately after freezing; we reuse
	// it for the post-freeze staleness check and the response, avoiding a second
	// postgres query.
	frozenPos, err := pm.freezeWAL(ctx, proposedTerm)
	if err != nil {
		pm.logger.ErrorContext(ctx, "WAL freeze failed, not persisting revocation",
			"revoked_below_term", proposedTerm,
			"error", err)
		cs, _ := pm.getCachedConsensusStatus()
		return &consensusdatapb.RecruitResponse{ConsensusStatus: cs},
			mterrors.Wrap(err, "WAL freeze failed")
	}

	// Re-check after WAL freeze: applying in-flight WAL may have revealed a
	// higher committed coordinator_term, making this proposed term stale.
	if frozenPos != nil && frozenPos.Rule != nil {
		committedTerm := frozenPos.Rule.GetRuleNumber().GetCoordinatorTerm()
		if committedTerm >= proposedTerm {
			pm.logger.InfoContext(ctx, "Post-freeze re-check: found higher committed term in WAL, rejecting revocation",
				"committed_term", committedTerm,
				"proposed_term", proposedTerm)
			revocation, _ := pm.consensusState.GetInconsistentRevocation()
			cs := buildConsensusStatus(pm.serviceID, revocation, frozenPos)
			return &consensusdatapb.RecruitResponse{ConsensusStatus: cs},
				mterrors.Errorf(mtrpcpb.Code_FAILED_PRECONDITION,
					"committed term %d in WAL >= proposed term %d; proposed term is stale",
					committedTerm, proposedTerm)
		}
	}

	// WAL is frozen and no higher committed term found — now safe to persist.
	if err := pm.consensusState.AcceptRevocation(ctx, req.TermRevocation); err != nil {
		pm.logger.InfoContext(ctx, "Revocation not accepted after WAL freeze", "error", err)
		revocation, _ := pm.consensusState.GetInconsistentRevocation()
		cs := buildConsensusStatus(pm.serviceID, revocation, frozenPos)
		return &consensusdatapb.RecruitResponse{ConsensusStatus: cs}, mterrors.Wrap(err, "revocation rejected")
	}

	pm.logger.InfoContext(ctx, "Revocation accepted and persisted",
		"revoked_below_term", proposedTerm)

	// Return the persisted revocation with the frozen position. The revocation is
	// freshly read from disk; the position is the one captured at freeze time
	// (WAL cannot advance further since we froze it).
	revocation, err := pm.consensusState.GetRevocation(ctx)
	if err != nil {
		pm.logger.WarnContext(ctx, "Failed to read revocation after recruit", "error", err)
	}
	cs := buildConsensusStatus(pm.serviceID, revocation, frozenPos)
	return &consensusdatapb.RecruitResponse{ConsensusStatus: cs}, nil
}

// freezeWAL stops WAL movement on this node so the coordinator can determine
// which pooler has the highest WAL position.
// For primaries: emergency demote (revokes write authority).
// For standbys: pause receiver and wait for replay to stabilize.
// Returns the position observed immediately after freezing so callers can
// avoid a redundant second postgres query.
func (pm *MultiPoolerManager) freezeWAL(ctx context.Context, term int64) (*consensusdatapb.PoolerPosition, error) {
	if _, err := pm.query(ctx, "SELECT 1"); err != nil {
		return nil, mterrors.Wrap(err, "postgres unhealthy, cannot freeze WAL")
	}

	isPrimary, err := pm.isPrimary(ctx)
	if err != nil {
		return nil, mterrors.Wrap(err, "failed to determine role for WAL freeze")
	}

	if isPrimary {
		pm.logger.InfoContext(ctx, "Freezing WAL: demoting primary", "term", term)
		drainTimeout := 5 * time.Second
		if _, err := pm.emergencyDemoteLocked(ctx, term, drainTimeout); err != nil {
			return nil, mterrors.Wrap(err, "failed to demote primary during WAL freeze")
		}
		pm.logger.InfoContext(ctx, "Primary demoted for WAL freeze", "term", term)
	} else {
		pm.logger.InfoContext(ctx, "Freezing WAL: stopping standby replication", "term", term)
		_, err := pm.pauseReplication(
			ctx,
			multipoolermanagerdatapb.ReplicationPauseMode_REPLICATION_PAUSE_MODE_RECEIVER_ONLY,
			true /* wait */)
		if err != nil {
			return nil, mterrors.Wrap(err, "failed to pause replication during WAL freeze")
		}
		if _, err := pm.waitForReplayStabilize(ctx); err != nil {
			return nil, mterrors.Wrap(err, "failed waiting for replay to stabilize during WAL freeze")
		}
		pm.logger.InfoContext(ctx, "Standby WAL frozen", "term", term)
	}

	pos, err := pm.rules.observePosition(ctx)
	if err != nil {
		pm.logger.WarnContext(ctx, "Failed to observe position after WAL freeze; status may lack current_position",
			"term", term, "error", err)
		return nil, nil
	}
	return pos, nil
}

// buildConsensusStatus constructs a ConsensusStatus from a pre-resolved revocation and position.
// Both arguments may be nil; in that case the corresponding fields in the returned status
// are left unset. Never performs I/O.
func buildConsensusStatus(id *clustermetadatapb.ID, revocation *consensusdatapb.TermRevocation, pos *consensusdatapb.PoolerPosition) *consensusdatapb.ConsensusStatus {
	status := &consensusdatapb.ConsensusStatus{Id: id}
	if revocation != nil {
		status.TermRevocation = revocation
	}
	if pos != nil {
		status.CurrentPosition = pos
	}
	return status
}

// getConsensusStatus builds a ConsensusStatus snapshot while holding the action lock.
// Uses a consistent disk read for the revocation and a fresh postgres query for the
// current position.
func (pm *MultiPoolerManager) getConsensusStatus(ctx context.Context) (*consensusdatapb.ConsensusStatus, error) {
	revocation, err := pm.consensusState.GetRevocation(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to read consensus revocation: %w", err)
	}

	pos, err := pm.rules.observePosition(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to read current rule position: %w", err)
	}
	return buildConsensusStatus(pm.serviceID, revocation, pos), nil
}

// getCachedConsensusStatus builds a ConsensusStatus using the in-memory revocation cache and
// the ruleStore's cached position. Never queries postgres or disk.
//
// The action lock must be held by the caller, which prevents concurrent revocation updates.
// Returns nil if no position has been cached yet.
func (pm *MultiPoolerManager) getCachedConsensusStatus() (*consensusdatapb.ConsensusStatus, error) {
	revocation, err := pm.consensusState.GetInconsistentRevocation()
	if err != nil {
		return nil, err
	}

	pos := pm.rules.cachedPosition()
	if pos == nil {
		return nil, nil
	}
	return buildConsensusStatus(pm.serviceID, revocation, pos), nil
}

// getInconsistentConsensusStatus builds a ConsensusStatus without holding the action lock.
// Suitable for observability (StatusResponse, health monitors) but not for decisions
// that require a consistent view.
//
// Returns (nil, err) if postgres is unreachable; callers should log and continue.
func (pm *MultiPoolerManager) getInconsistentConsensusStatus(ctx context.Context) (*consensusdatapb.ConsensusStatus, error) {
	revocation, err := pm.consensusState.GetInconsistentRevocation()
	if err != nil {
		return nil, err
	}

	pos, err := pm.rules.observePosition(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to read current rule position: %w", err)
	}
	return buildConsensusStatus(pm.serviceID, revocation, pos), nil
}

// buildAvailabilityStatus returns the current AvailabilityStatus for this node.
// Leaders always publish a LeadershipStatus. Returns nil if no signals are set
// and no leadership context exists.
func (pm *MultiPoolerManager) buildAvailabilityStatus() *clustermetadatapb.AvailabilityStatus {
	pm.mu.Lock()
	resignedTerm := pm.resignedPrimaryAtTerm
	pm.mu.Unlock()

	if resignedTerm == 0 {
		return nil
	}

	return &clustermetadatapb.AvailabilityStatus{
		LeadershipStatus: &clustermetadatapb.LeadershipStatus{
			PrimaryTerm: resignedTerm,
			Signal:      clustermetadatapb.LeadershipSignal_LEADERSHIP_SIGNAL_REQUESTING_DEMOTION,
		},
	}
}

// setResignedPrimaryAtTerm records that this node is requesting demotion as primary
// for the given term. The signal is included in subsequent StatusResponses so the
// coordinator can trigger an immediate election.
// Requires the action lock (ctx must be an action-lock context).
func (pm *MultiPoolerManager) setResignedPrimaryAtTerm(ctx context.Context, term int64) error {
	if err := AssertActionLockHeld(ctx); err != nil {
		return err
	}
	pm.mu.Lock()
	pm.resignedPrimaryAtTerm = term
	pm.mu.Unlock()
	return nil
}

// clearResignedPrimaryAtTerm clears the leadership demotion request. Called by
// coordinator-driven promotion (Propose) when this node is explicitly re-appointed
// as primary at a new term.
// Requires the action lock (ctx must be an action-lock context).
func (pm *MultiPoolerManager) clearResignedPrimaryAtTerm(ctx context.Context) error {
	if err := AssertActionLockHeld(ctx); err != nil {
		return err
	}
	pm.mu.Lock()
	pm.resignedPrimaryAtTerm = 0
	pm.mu.Unlock()
	return nil
}

// Propose handles Phase 2 of the Paxos-inspired consensus protocol.
// The coordinator sends a CoordinatorProposal to all poolers. Each pooler
// self-identifies by comparing proposal_leader_id to its own ID:
//   - If proposal_leader_id == self: promote to primary and configure sync replication.
//   - Otherwise: configure primary_conninfo to the proposal leader and resume streaming.
func (pm *MultiPoolerManager) Propose(ctx context.Context, req *consensusdatapb.ProposeRequest) (_ *consensusdatapb.ProposeResponse, retErr error) {
	proposal := req.GetProposal()
	if proposal == nil {
		return nil, mterrors.New(mtrpcpb.Code_INVALID_ARGUMENT, "proposal must be specified")
	}

	ctx, err := pm.actionLock.Acquire(ctx, "Propose")
	if err != nil {
		return nil, err
	}
	defer pm.actionLock.Release(ctx)

	// Validate that the proposal's term revocation matches what we accepted in Recruit.
	if err := pm.validateProposalTerm(ctx, proposal.GetTermRevocation()); err != nil {
		return nil, mterrors.Wrap(err, "proposal term mismatch")
	}

	pm.consensusState.SetInProgressProposal(proposal)

	amLeader := proposal.GetProposalLeader().GetId().GetName() == pm.serviceID.GetName()

	if amLeader {
		pm.logger.InfoContext(ctx, "Propose: self is proposal leader, promoting to primary",
			"term", proposal.GetTermRevocation().GetRevokedBelowTerm())

		syncCfg := proposedRuleToSyncConfig(proposal.GetProposedRule())
		result, err := pm.promote(ctx, proposal, syncCfg, false)
		if err != nil {
			cs, _ := pm.getCachedConsensusStatus()
			return &consensusdatapb.ProposeResponse{ConsensusStatus: cs}, mterrors.Wrap(err, "promote failed")
		}
		pm.logger.InfoContext(ctx, "Propose: promotion complete", "final_lsn", result.finalLSN)
	} else {
		pm.logger.InfoContext(ctx, "Propose: self is replica, configuring replication to proposal leader",
			"leader", proposal.GetProposalLeader().GetId().GetName())

		if err := pm.configureReplicationToLeader(ctx, proposal); err != nil {
			cs, _ := pm.getCachedConsensusStatus()
			return &consensusdatapb.ProposeResponse{ConsensusStatus: cs}, mterrors.Wrap(err, "configure replication to leader failed")
		}
	}

	cs, err := pm.getCachedConsensusStatus()
	if err != nil {
		pm.logger.WarnContext(ctx, "Failed to build cached consensus status after propose", "error", err)
	}
	return &consensusdatapb.ProposeResponse{ConsensusStatus: cs}, nil
}

// validateProposalTerm checks that the proposal's term revocation matches the
// revocation this pooler accepted during Recruit. Requires action lock.
func (pm *MultiPoolerManager) validateProposalTerm(ctx context.Context, proposalRevocation *consensusdatapb.TermRevocation) error {
	currentRevocation, err := pm.consensusState.GetRevocation(ctx)
	if err != nil {
		return fmt.Errorf("failed to read current revocation: %w", err)
	}

	if currentRevocation == nil {
		return mterrors.New(mtrpcpb.Code_FAILED_PRECONDITION, "no revocation accepted yet; run Recruit first")
	}

	proposalTerm := proposalRevocation.GetRevokedBelowTerm()
	currentTerm := currentRevocation.GetRevokedBelowTerm()

	if proposalTerm != currentTerm {
		return mterrors.Errorf(mtrpcpb.Code_FAILED_PRECONDITION,
			"proposal term %d does not match accepted term %d", proposalTerm, currentTerm)
	}

	return nil
}

// proposedRuleToSyncConfig converts a ShardRule's cohort members into a
// ConfigureSynchronousReplicationRequest suitable for passing to promote().
// Returns nil if the proposed rule has no cohort members (async replication).
func proposedRuleToSyncConfig(rule *consensusdatapb.ShardRule) *multipoolermanagerdatapb.ConfigureSynchronousReplicationRequest {
	if rule == nil || len(rule.GetCohortMembers()) == 0 {
		return nil
	}
	return &multipoolermanagerdatapb.ConfigureSynchronousReplicationRequest{
		SynchronousCommit: multipoolermanagerdatapb.SynchronousCommitLevel_SYNCHRONOUS_COMMIT_REMOTE_WRITE,
		SynchronousMethod: multipoolermanagerdatapb.SynchronousMethod_SYNCHRONOUS_METHOD_ANY,
		NumSync:           1,
		StandbyIds:        rule.GetCohortMembers(),
		ReloadConfig:      true,
	}
}

// configureReplicationToLeader sets primary_conninfo on this replica to point at the
// proposal leader. Called from Propose() when this node is not the proposal leader.
func (pm *MultiPoolerManager) configureReplicationToLeader(ctx context.Context, proposal *consensusdatapb.CoordinatorProposal) error {
	leader := proposal.GetProposalLeader()
	host := leader.GetHost()
	port := leader.GetPostgresPort()
	leaderID := leader.GetId()

	if host == "" || port == 0 {
		return mterrors.Errorf(mtrpcpb.Code_INVALID_ARGUMENT,
			"proposal_leader host and postgres_port must be set for replica path")
	}

	pm.mu.Lock()
	pm.primaryPoolerID = leaderID
	pm.primaryHost = host
	pm.primaryPort = port
	pm.mu.Unlock()

	return pm.setPrimaryConnInfoLocked(ctx, host, port, true /* stop */, true /* start */)
}

// Inform handles notification of a committed shard rule decision.
// This is the final phase of the Paxos-inspired protocol. No authority check
// is required — the coordinator broadcasts the committed rule to all poolers.
func (pm *MultiPoolerManager) Inform(ctx context.Context, req *consensusdatapb.InformRequest) (*consensusdatapb.InformResponse, error) {
	rule := req.GetRule()
	if rule == nil {
		return nil, mterrors.New(mtrpcpb.Code_INVALID_ARGUMENT, "rule must be specified")
	}

	pm.consensusState.SetHighestKnownDecision(rule)
	pm.consensusState.SetInProgressProposal(nil)

	pm.logger.InfoContext(ctx, "Inform received: rule committed",
		"coordinator_term", rule.GetRuleNumber().GetCoordinatorTerm(),
		"leader_subterm", rule.GetRuleNumber().GetLeaderSubterm())

	cs, err := pm.getInconsistentConsensusStatus(ctx)
	if err != nil {
		pm.logger.WarnContext(ctx, "Failed to build consensus status for InformResponse", "error", err)
	}
	return &consensusdatapb.InformResponse{ConsensusStatus: cs}, nil
}

// ConsensusStatus returns the current status of this node for consensus
func (pm *MultiPoolerManager) ConsensusStatus(ctx context.Context, req *consensusdatapb.StatusRequest) (*consensusdatapb.StatusResponse, error) {
	revocation, err := pm.consensusState.GetInconsistentRevocation()
	if err != nil {
		return nil, fmt.Errorf("failed to get consensus revocation: %w", err)
	}

	localRevokedBelowTerm := int64(0)
	if revocation != nil {
		localRevokedBelowTerm = revocation.GetRevokedBelowTerm()
	}

	// Check if database is healthy by attempting a simple query
	_, healthErr := pm.query(ctx, "SELECT 1")
	isHealthy := healthErr == nil

	// Get WAL position and determine role (primary/replica)
	walPosition := &consensusdatapb.WALPosition{
		Timestamp: timestamppb.New(time.Now()),
	}
	role := consensusdatapb.PostgresRole_POSTGRES_ROLE_UNSPECIFIED

	if isHealthy {
		isPrimary, err := pm.isPrimary(ctx)
		if err == nil {
			if isPrimary {
				role = consensusdatapb.PostgresRole_POSTGRES_ROLE_PRIMARY
				currentLsn, err := pm.getPrimaryLSN(ctx)
				if err == nil {
					walPosition.CurrentLsn = currentLsn
				}
			} else {
				role = consensusdatapb.PostgresRole_POSTGRES_ROLE_REPLICA
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
			}
		}
	}

	consensusStatus, statusErr := pm.getInconsistentConsensusStatus(ctx)
	if statusErr != nil {
		pm.logger.WarnContext(ctx, "Failed to build consensus status for StatusResponse", "error", statusErr)
	}

	// localRevokedBelowTerm is embedded in ConsensusStatus.TermRevocation; included
	// in the log for observability but not as a top-level field on StatusResponse.
	pm.logger.DebugContext(ctx, "ConsensusStatus built",
		"revoked_below_term", localRevokedBelowTerm)

	return &consensusdatapb.StatusResponse{
		PoolerId:           pm.serviceID.GetName(),
		WalPosition:        walPosition,
		IsHealthy:          isHealthy,
		IsEligible:         true, // TODO: implement eligibility logic based on policy
		Cell:               pm.serviceID.GetCell(),
		Role:               role,
		TimelineInfo:       timelineInfo,
		ConsensusStatus:    consensusStatus,
		AvailabilityStatus: pm.buildAvailabilityStatus(),
	}, nil
}
