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
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/multigres/multigres/go/common/mterrors"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	multipoolermanagerdata "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
	"github.com/multigres/multigres/go/services/multipooler/internal/manager/consensus"
)

// Regex for parsing synchronous_standby_names format: METHOD NUM (member1, member2, ...)
// Case-insensitive to accept both "FIRST"/"ANY" and "first"/"any"
var syncStandbyNamesRegex = regexp.MustCompile(`(?i)^(FIRST|ANY)\s+(\d+)\s*\(([^)]*)\)$`)

// SyncStandbyConfig represents a parsed synchronous_standby_names configuration
type SyncStandbyConfig struct {
	Method     multipoolermanagerdata.SynchronousMethod // FIRST or ANY
	NumSync    int32                                    // Number of synchronous standbys
	StandbyIDs []*clustermetadatapb.ID                  // List of standby IDs
}

// parseSynchronousStandbyNames parses a PostgreSQL synchronous_standby_names string
// Examples:
//   - "FIRST 2 ("cell_replica1", "cell_replica2", "cell_replica3")"
//   - "ANY 1 ("cell_replica1", "cell_replica2")"
//   - "*" (wildcard - all connected standbys)
//   - "" (empty - no synchronous replication)
func parseSynchronousStandbyNames(value string) (*SyncStandbyConfig, error) {
	value = strings.TrimSpace(value)

	// Handle empty case
	if value == "" {
		return nil, mterrors.New(mtrpcpb.Code_FAILED_PRECONDITION, "synchronous replication not configured")
	}

	// Handle wildcard case - not supported in Multigres context
	if value == "*" || strings.Contains(value, "(*)") {
		return nil, mterrors.New(mtrpcpb.Code_INVALID_ARGUMENT,
			"wildcard (*) is not supported in Multigres - standby list must be explicit")
	}

	// Parse format: METHOD NUM (member1, member2, ...)
	// Note: this regex assumes standby_names are being controlled by multigres
	// and will have the format we expect (i.e cell_name). We are not validating
	// for this format here.
	matches := syncStandbyNamesRegex.FindStringSubmatch(value)
	if matches == nil {
		return nil, mterrors.New(mtrpcpb.Code_INVALID_ARGUMENT,
			fmt.Sprintf("invalid synchronous_standby_names format: %q", value))
	}

	methodStr := strings.ToUpper(matches[1]) // Normalize to uppercase
	numSync, err := strconv.ParseInt(matches[2], 10, 32)
	if err != nil {
		return nil, mterrors.New(mtrpcpb.Code_INVALID_ARGUMENT,
			fmt.Sprintf("invalid num_sync value in synchronous_standby_names: %q", matches[2]))
	}

	// Convert string method to enum
	var method multipoolermanagerdata.SynchronousMethod
	switch methodStr {
	case "FIRST":
		method = multipoolermanagerdata.SynchronousMethod_SYNCHRONOUS_METHOD_FIRST
	case "ANY":
		method = multipoolermanagerdata.SynchronousMethod_SYNCHRONOUS_METHOD_ANY
	default:
		return nil, mterrors.New(mtrpcpb.Code_INVALID_ARGUMENT,
			fmt.Sprintf("unsupported synchronous method: %q", methodStr))
	}

	// Parse member list
	membersStr := strings.TrimSpace(matches[3])
	if membersStr == "" {
		return nil, mterrors.New(mtrpcpb.Code_INVALID_ARGUMENT,
			"empty member list in synchronous_standby_names")
	}

	// Split by comma and clean up each member
	membersParts := strings.Split(membersStr, ",")
	standbyIDs := make([]*clustermetadatapb.ID, 0, len(membersParts))
	for _, part := range membersParts {
		part = strings.TrimSpace(part)
		// Remove surrounding quotes if present
		part = strings.Trim(part, `"`)
		if part != "" {
			// Parse application name back to ID
			id, err := consensus.ParseApplicationName(part)
			if err != nil {
				return nil, mterrors.Wrap(err, fmt.Sprintf("failed to parse application name %q", part))
			}
			standbyIDs = append(standbyIDs, id)
		}
	}

	if len(standbyIDs) == 0 {
		return nil, mterrors.New(mtrpcpb.Code_INVALID_ARGUMENT,
			"no valid members found in synchronous_standby_names")
	}

	return &SyncStandbyConfig{
		Method:     method,
		NumSync:    int32(numSync),
		StandbyIDs: standbyIDs,
	}, nil
}
