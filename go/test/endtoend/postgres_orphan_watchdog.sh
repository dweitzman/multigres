#!/bin/bash
# Copyright 2025 Supabase, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# postgres_orphan_watchdog.sh
# Monitors postmaster.pid and kills postgres if the parent pgctld dies or testdata dir is deleted.
# This script continuously tracks the postgres PID from postmaster.pid, handling restarts.
#
# Usage: postgres_orphan_watchdog.sh <datadir>
# Example: postgres_orphan_watchdog.sh /path/to/pg_data
#
# Environment variables:
#   MULTIGRES_TESTDATA_DIR - If set and directory is deleted, trigger cleanup
#   MULTIGRES_TEST_PARENT_PID - If set, monitor this PID instead of parent

set -euo pipefail

if [ $# -ne 1 ]; then
  echo "Usage: $0 <datadir>" >&2
  exit 1
fi

DATADIR="$1"
POSTMASTER_PID_FILE="$DATADIR/postmaster.pid"

# Get orphan detection environment variables
TESTDATA_DIR="${MULTIGRES_TESTDATA_DIR:-}"
TEST_PARENT_PID="${MULTIGRES_TEST_PARENT_PID:-}"

# Determine which PID to monitor (prefer TEST_PARENT_PID, fallback to parent)
if [ -n "$TEST_PARENT_PID" ]; then
  MONITOR_PID="$TEST_PARENT_PID"
else
  MONITOR_PID="$PPID"
fi

# Track the most recent postgres PID we've observed
POSTGRES_PID=""

# Function to read postgres PID from postmaster.pid
read_postgres_pid() {
  if [ -f "$POSTMASTER_PID_FILE" ]; then
    # First line of postmaster.pid contains the PID
    head -n 1 "$POSTMASTER_PID_FILE" 2>/dev/null || echo ""
  else
    echo ""
  fi
}

# Function to kill postgres gracefully then forcefully
kill_postgres() {
  local pid="$1"
  if [ -z "$pid" ]; then
    return
  fi

  # Check if process is still running
  if ! kill -0 "$pid" 2>/dev/null; then
    return
  fi

  # Try SIGTERM first (graceful shutdown)
  kill -TERM "$pid" 2>/dev/null || true

  # Wait up to 5 seconds for graceful shutdown
  for i in $(seq 1 5); do
    if ! kill -0 "$pid" 2>/dev/null; then
      return
    fi
    sleep 1
  done

  # Force kill with SIGKILL
  kill -KILL "$pid" 2>/dev/null || true
}

# Monitor loop
while kill -0 "$MONITOR_PID" 2>/dev/null; do
  # Update our tracked postgres PID
  current_pid=$(read_postgres_pid)
  if [ -n "$current_pid" ]; then
    POSTGRES_PID="$current_pid"
  fi

  # Check if testdata directory was deleted
  if [ -n "$TESTDATA_DIR" ] && [ ! -d "$TESTDATA_DIR" ]; then
    # Directory deleted, kill the last known postgres PID
    kill_postgres "$POSTGRES_PID"
    exit 0
  fi

  sleep 1
done

# Monitored process died, kill the last known postgres PID
kill_postgres "$POSTGRES_PID"
