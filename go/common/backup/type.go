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

package backup

import (
	multiadminpb "github.com/multigres/multigres/go/pb/multiadmin"
)

// PgBackRestType converts a BackupType enum to the string format used by pgBackRest.
// Returns "full" for unknown types as a safe default.
func PgBackRestType(bt multiadminpb.BackupType) string {
	switch bt {
	case multiadminpb.BackupType_BACKUP_TYPE_FULL:
		return "full"
	case multiadminpb.BackupType_BACKUP_TYPE_DIFFERENTIAL:
		return "differential"
	case multiadminpb.BackupType_BACKUP_TYPE_INCREMENTAL:
		return "incremental"
	default:
		return "full"
	}
}

// ParsePgBackRestType converts a pgBackRest backup type string to the BackupType enum.
// Accepts both full names and common abbreviations (e.g., "diff" for "differential").
func ParsePgBackRestType(typeStr string) multiadminpb.BackupType {
	switch typeStr {
	case "full":
		return multiadminpb.BackupType_BACKUP_TYPE_FULL
	case "differential", "diff":
		return multiadminpb.BackupType_BACKUP_TYPE_DIFFERENTIAL
	case "incremental", "incr":
		return multiadminpb.BackupType_BACKUP_TYPE_INCREMENTAL
	default:
		return multiadminpb.BackupType_BACKUP_TYPE_UNKNOWN
	}
}

// TypeToString converts a BackupType enum to a human-readable lowercase string.
// Useful for CLI output and logging.
func TypeToString(bt multiadminpb.BackupType) string {
	switch bt {
	case multiadminpb.BackupType_BACKUP_TYPE_FULL:
		return "full"
	case multiadminpb.BackupType_BACKUP_TYPE_DIFFERENTIAL:
		return "differential"
	case multiadminpb.BackupType_BACKUP_TYPE_INCREMENTAL:
		return "incremental"
	default:
		return "unknown"
	}
}
