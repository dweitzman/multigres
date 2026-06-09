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

package consensus

import (
	"fmt"
	"strings"

	"github.com/multigres/multigres/go/common/mterrors"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
)

// FormatStandbyList formats a list of pooler IDs as a comma-separated list of quoted application names.
func FormatStandbyList(ids []ReplicaID) string {
	quoted := make([]string, len(ids))
	for i, id := range ids {
		quoted[i] = fmt.Sprintf(`"%s"`, id.appName)
	}
	return strings.Join(quoted, ", ")
}

// BuildSynchronousStandbyNamesValue constructs the synchronous_standby_names value string
// This produces values like: FIRST 1 ("standby-1", "standby-2") or ANY 1 ("standby-1", "standby-2")
func BuildSynchronousStandbyNamesValue(method multipoolermanagerdatapb.SynchronousMethod, numSync int32, names []ReplicaID) (string, error) {
	if len(names) == 0 {
		return "", nil
	}

	var methodStr string
	switch method {
	case multipoolermanagerdatapb.SynchronousMethod_SYNCHRONOUS_METHOD_FIRST:
		methodStr = "FIRST"
	case multipoolermanagerdatapb.SynchronousMethod_SYNCHRONOUS_METHOD_ANY:
		methodStr = "ANY"
	default:
		return "", mterrors.New(mtrpcpb.Code_INVALID_ARGUMENT,
			fmt.Sprintf("invalid synchronous method: %s, must be FIRST or ANY", method.String()))
	}

	return fmt.Sprintf("%s %d (%s)", methodStr, numSync, FormatStandbyList(names)), nil
}

// ----------------------------------------------------------------------------
// Validation Helpers
// ----------------------------------------------------------------------------
// ValidateStandbyIDs validates that the list is non-empty and converts each ID to its ReplicaID.
func ValidateStandbyIDs(standbyIDs []*clustermetadatapb.ID) ([]ReplicaID, error) {
	if len(standbyIDs) == 0 {
		return nil, mterrors.New(mtrpcpb.Code_INVALID_ARGUMENT, "standby_ids cannot be empty")
	}
	pids, err := ToReplicaIDs(standbyIDs)
	if err != nil {
		return pids, mterrors.Wrap(err, "invalid standby_ids")
	}
	return pids, nil
}

// ValidateSyncReplicationParams validates the parameters for setting synchronous_standby_names.
func ValidateSyncReplicationParams(numSync int32, standbyIDs []*clustermetadatapb.ID) ([]ReplicaID, error) {
	// Validate numSync is non-negative
	if numSync < 0 {
		return nil, mterrors.New(mtrpcpb.Code_INVALID_ARGUMENT,
			fmt.Sprintf("num_sync must be non-negative, got: %d", numSync))
	}

	// If standbyIDs are provided, validate them
	if len(standbyIDs) > 0 {
		// Validate that numSync doesn't exceed the number of standbys (PostgreSQL requirement)
		// Note: numSync=0 is allowed and will be defaulted to 1 in setSynchronousStandbyNames
		if numSync > int32(len(standbyIDs)) {
			return nil, mterrors.New(mtrpcpb.Code_INVALID_ARGUMENT,
				fmt.Sprintf("num_sync (%d) cannot exceed number of standby_ids (%d)", numSync, len(standbyIDs)))
		}

		// Validate each standby ID
		names, err := ValidateStandbyIDs(standbyIDs)
		if err != nil {
			return nil, err
		}
		return names, nil
	}

	return nil, nil
}

// ApplyAddOperation adds new standbys to the standby list (idempotent)
func ApplyAddOperation(currentStandbys, newStandbys []ReplicaID) []ReplicaID {
	updatedStandbys := append([]ReplicaID{}, currentStandbys...)
	existingMap := make(map[string]bool, len(currentStandbys))
	for _, standby := range currentStandbys {
		existingMap[standby.appName] = true
	}
	for _, newStandby := range newStandbys {
		if !existingMap[newStandby.appName] {
			updatedStandbys = append(updatedStandbys, newStandby)
		}
	}
	return updatedStandbys
}

// ApplyRemoveOperation removes standby names from the standby list (idempotent)
func ApplyRemoveOperation(currentStandbys, standbysToRemove []ReplicaID) []ReplicaID {
	removeMap := make(map[string]bool, len(standbysToRemove))
	for _, standby := range standbysToRemove {
		removeMap[standby.appName] = true
	}
	var updatedStandbys []ReplicaID
	for _, standby := range currentStandbys {
		if !removeMap[standby.appName] {
			updatedStandbys = append(updatedStandbys, standby)
		}
	}
	return updatedStandbys
}
