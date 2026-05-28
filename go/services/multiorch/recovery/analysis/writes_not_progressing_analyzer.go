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

package analysis

import (
	"errors"
	"fmt"
	"time"

	"github.com/multigres/multigres/go/services/multiorch/recovery/types"
)

// writesNotProgressingThreshold is how long the cohort may show
// "replicas connected but no LSN advance" before we flag it as a
// primary that's stopped making forward progress. Heartbeats commit
// every few seconds on a healthy primary, so 60s is well above the
// healthy cadence — anything past this is genuinely stuck.
const writesNotProgressingThreshold = 60 * time.Second

// WritesNotProgressingAnalyzer detects a primary that's alive enough to
// keep replication TCP connections alive but isn't actually writing
// WAL. The signal:
//
//   - At least one replica has its WAL receiver currently in
//     "streaming" state (per WalReceiverNotStreamingSince == zero on
//     PoolerAnalysis).
//   - Across ALL such streaming replicas, the most recent observed
//     LSN advance (LastLsnAdvance) is older than
//     writesNotProgressingThreshold.
//
// When both hold, every standby is configured for, and believes itself
// to be, streaming — but none has received new WAL bytes in too long.
// Heartbeats commit on a healthy primary every few seconds and bump
// every replica's last_receive_lsn, so the absence of progress
// across the entire cohort is strong evidence the primary itself has
// stopped writing.
//
// Remediation: AppointLeader. A new recruit at a higher term will
// either succeed (the runaway primary loses its term) or fail because
// the cohort genuinely can't reach a leader — either way the cluster
// stops sitting on a wedged primary.
//
// Distinct from LeaderUnreachable (reachability-based; pooler can't be
// reached). A reachable-but-wedged primary won't trip LeaderIsDead
// but will trip this.
//
// Future enhancement: suppress when the cluster is intentionally
// quiesced (e.g. user marked the shard read-only). No such signal
// exists today; revisit when one does.
type WritesNotProgressingAnalyzer struct {
	factory *RecoveryActionFactory
}

func (a *WritesNotProgressingAnalyzer) Name() types.CheckName {
	return "WritesNotProgressing"
}

func (a *WritesNotProgressingAnalyzer) ProblemCode() types.ProblemCode {
	return types.ProblemWritesNotProgressing
}

func (a *WritesNotProgressingAnalyzer) RecoveryAction() types.RecoveryAction {
	return a.factory.NewAppointLeaderAction()
}

func (a *WritesNotProgressingAnalyzer) Analyze(sa *ShardAnalysis) ([]types.Problem, error) {
	if a.factory == nil {
		return nil, errors.New("recovery action factory not initialized")
	}

	cutoff := time.Now().Add(-writesNotProgressingThreshold)
	var streamingReplicas int
	var oldestAdvance time.Time
	for _, pa := range sa.Analyses {
		if pa.IsLeader {
			continue
		}
		if !pa.IsInitialized {
			continue
		}
		// "Believes it's streaming" — the pooler's own state machine
		// shows the WAL receiver currently in streaming mode (i.e. not
		// stuck per the pooler's own tracking).
		if !pa.WalReceiverNotStreamingSince.IsZero() {
			continue
		}
		// Discard observations where we've never seen the replica
		// advance — could be a freshly-discovered pooler before its
		// first advance landed; not evidence either way.
		if pa.LastLsnAdvance.IsZero() {
			continue
		}
		streamingReplicas++
		if oldestAdvance.IsZero() || pa.LastLsnAdvance.Before(oldestAdvance) {
			oldestAdvance = pa.LastLsnAdvance
		}
		// If any replica has advanced recently, primary is writing;
		// short-circuit.
		if pa.LastLsnAdvance.After(cutoff) {
			return nil, nil
		}
	}
	if streamingReplicas == 0 {
		// No witnesses — can't conclude anything about primary writes
		// from replicas alone.
		return nil, nil
	}

	// All streaming replicas have stale LastLsnAdvance.
	leaderName := "<unknown>"
	if sa.LeaderObservation.GetLeaderId() != nil {
		leaderName = sa.LeaderObservation.GetLeaderId().GetName()
	}
	return []types.Problem{{
		Code:      types.ProblemWritesNotProgressing,
		CheckName: a.Name(),
		ShardKey:  sa.ShardKey,
		Description: fmt.Sprintf(
			"%d replica(s) believe they're streaming from leader %s but none has observed an LSN advance since %s (%s ago) — leader has stopped writing WAL",
			streamingReplicas,
			leaderName,
			oldestAdvance.Format(time.RFC3339),
			time.Since(oldestAdvance).Truncate(time.Second),
		),
		Priority:       types.PriorityEmergency,
		Scope:          types.ScopeShard,
		DetectedAt:     time.Now(),
		RecoveryAction: a.factory.NewAppointLeaderAction(),
	}}, nil
}
