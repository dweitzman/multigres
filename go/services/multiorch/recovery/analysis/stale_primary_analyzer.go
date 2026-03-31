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

package analysis

import (
	"errors"
	"fmt"
	"time"

	"github.com/multigres/multigres/go/services/multiorch/recovery/types"
)

// StalePrimaryAnalyzer detects when multiple poolers in a shard claim to be primary.
// This happens when a demoted primary restarts without being properly reset. The analyzer
// identifies the stale primary (lower PrimaryTerm) and triggers demotion.
//
// Note: This is NOT true split-brain. True split-brain means both primaries can accept
// writes. In this scenario, the new primary cannot accept writes because it cannot
// recruit standbys while the stale primary exists.
type StalePrimaryAnalyzer struct {
	factory *RecoveryActionFactory
}

func (a *StalePrimaryAnalyzer) Name() types.CheckName {
	return "StalePrimary"
}

func (a *StalePrimaryAnalyzer) ProblemCode() types.ProblemCode {
	return types.ProblemStalePrimary
}

func (a *StalePrimaryAnalyzer) RecoveryAction() types.RecoveryAction {
	return a.factory.NewDemoteStalePrimaryAction()
}

func (a *StalePrimaryAnalyzer) Analyze(shard *ShardAnalysis) ([]*types.Problem, error) {
	if a.factory == nil {
		return nil, errors.New("recovery action factory not initialized")
	}

	// Collect all reachable, initialized primaries with a valid PrimaryTerm.
	var primaries []*PoolerState
	for _, ps := range shard.Poolers {
		if !ps.IsPrimary || !ps.IsInitialized || !ps.LastCheckValid {
			continue
		}
		// Invariant: initialized PRIMARY poolers must have PrimaryTerm>0.
		// PrimaryTerm is set during promotion and only cleared during demotion.
		if ps.PrimaryTerm == 0 {
			continue
		}
		primaries = append(primaries, ps)
	}

	if len(primaries) < 2 {
		return nil, nil // no conflict
	}

	// Find the most advanced primary (highest PrimaryTerm).
	mostAdvanced := findHighestTermPrimary(primaries)
	if mostAdvanced == nil {
		// Tie in PrimaryTerm — consensus bug, requires manual intervention.
		// Skip automatic demotion to avoid making the situation worse.
		return nil, nil
	}

	// Return one problem per stale primary. Any primary with a lower PrimaryTerm than
	// the most advanced is stale and should be demoted.
	var problems []*types.Problem
	for _, ps := range primaries {
		if ps.ID.Cell == mostAdvanced.ID.Cell && ps.ID.Name == mostAdvanced.ID.Name {
			continue // this is the most advanced primary, skip
		}
		problems = append(problems, &types.Problem{
			Code:      types.ProblemStalePrimary,
			CheckName: "StalePrimary",
			PoolerID:  ps.ID,
			ShardKey:  shard.ShardKey,
			Description: fmt.Sprintf("Stale primary detected: %s (primary_term=%d) is stale, most advanced is %s (primary_term=%d)",
				ps.ID.Name, ps.PrimaryTerm,
				mostAdvanced.ID.Name, mostAdvanced.PrimaryTerm),
			Priority:       types.PriorityEmergency,
			Scope:          types.ScopeShard,
			DetectedAt:     time.Now(),
			RecoveryAction: a.factory.NewDemoteStalePrimaryAction(),
		})
	}
	return problems, nil
}

// findHighestTermPrimary returns the primary with the highest PrimaryTerm.
// Returns nil if there is a tie (same PrimaryTerm on two or more primaries), which
// indicates a consensus bug — PrimaryTerm should be unique per promotion.
//
// Invariant: In a properly initialized shard, PrimaryTerm is always >0 for PRIMARY poolers.
// This function is defensive and returns nil if all primaries have PrimaryTerm=0.
func findHighestTermPrimary(primaries []*PoolerState) *PoolerState {
	var mostAdvanced *PoolerState
	var maxTerm int64
	tieDetected := false

	for _, ps := range primaries {
		switch {
		case ps.PrimaryTerm > maxTerm:
			maxTerm = ps.PrimaryTerm
			mostAdvanced = ps
			tieDetected = false
		case ps.PrimaryTerm == maxTerm && ps.PrimaryTerm > 0:
			tieDetected = true
		}
	}

	if maxTerm == 0 || tieDetected {
		return nil
	}
	return mostAdvanced
}
