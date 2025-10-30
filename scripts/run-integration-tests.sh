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

set -e

# Find PostgreSQL binaries dynamically (same as GitHub Actions workflow)
# shellcheck disable=SC2012
PGBIN="/usr/lib/postgresql/$(ls /usr/lib/postgresql/ | head -1)/bin"
export PATH="$PGBIN:$PATH"

# Setup PostgreSQL (needs to run as root)
/workspace/scripts/setup-postgres.sh

# Set defaults if not provided
GO_TEST_PACKAGES=${GO_TEST_PACKAGES:-./...}
GO_TEST_FLAGS=${GO_TEST_FLAGS:-}

# Run integration tests as postgres user (to avoid initdb root error)
echo "Running integration tests..."
echo "Packages: $GO_TEST_PACKAGES"
echo "Flags: $GO_TEST_FLAGS"
# Use 'su' without '-' to preserve environment, and explicitly set needed paths
# Use postgres user's actual home directory instead of /tmp
# shellcheck disable=SC2086
su postgres -c "cd /workspace && PATH=/usr/local/go/bin:$PGBIN:\$PATH go test -json -v $GO_TEST_FLAGS $GO_TEST_PACKAGES" 2>&1 | go tool tparse -follow
TEST_EXIT=${PIPESTATUS[0]}

# Stop PostgreSQL
echo "Stopping PostgreSQL..."
su - postgres -c "PATH=$PATH pg_ctl -D /etc/postgresql stop" || true

exit "$TEST_EXIT"
