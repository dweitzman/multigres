#!/usr/bin/env zsh
# Copyright 2026 Supabase, Inc.
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

# bootstrap-stress-test.sh: Repeatedly create and destroy a kind k8s cluster to verify that
# multigres bootstrap reliably converges to 1 primary + 2 connected, healthy replicas.
#
# Usage: ./bootstrap-stress-test.sh [iterations] [convergence_timeout_seconds] [stagger_delay_seconds]
#   iterations             - number of create/destroy cycles (default: 10)
#   convergence_timeout_s  - seconds to wait for cluster to converge per iteration (default: 300)
#   stagger_delay_s        - seconds to wait after pod-0 is up before scaling to 3 replicas.
#                            Lets the orch bootstrap with just 1 pooler before the replicas
#                            arrive. (default: 60)
#
# If the cluster fails to converge, the script stops WITHOUT destroying the cluster
# so that you can debug the stuck state.

set -euo pipefail

SCRIPT_DIR="${0:a:h}"
REPO_ROOT="${SCRIPT_DIR:h:h}"

ITERATIONS="${1:-10}"
CONVERGENCE_TIMEOUT="${2:-300}"
STAGGER_DELAY="${3:-60}"
ADMIN_API="http://localhost:18000/api/v1"
POLL_INTERVAL=5

log() {
  echo "[$(date '+%H:%M:%S')] $*"
}

# Check that required tools are available
for tool in kind kubectl curl jq; do
  if ! command -v "$tool" &>/dev/null; then
    echo "Error: $tool is required but not found in PATH"
    exit 1
  fi
done

# check_convergence: poll all three poolers until the cluster converges to
# 1 primary (has_backup=true, postgres_role=primary) and 2 replicas
# (postgres_role=standby, wal_receiver_status=streaming).
# Returns 0 on success, 1 on timeout.
check_convergence() {
  local deadline=$((SECONDS + CONVERGENCE_TIMEOUT))

  while ((SECONDS < deadline)); do
    local remaining=$((deadline - SECONDS))

    # Query the poolers list; multiadmin may not be ready yet
    local poolers_json
    poolers_json=$(curl -sf --max-time 5 "$ADMIN_API/poolers" 2>/dev/null) || {
      log "  [${remaining}s left] multiadmin not reachable yet, retrying..."
      sleep $POLL_INTERVAL
      continue
    }

    # Count registered poolers
    local n_poolers
    n_poolers=$(echo "$poolers_json" | jq '.poolers | length')

    if ((n_poolers != 3)); then
      log "  [${remaining}s left] $n_poolers/3 poolers registered in topology"
      sleep $POLL_INTERVAL
      continue
    fi

    # Check each pooler's status using tab-separated cell/name pairs
    local n_primary=0
    local n_streaming=0
    local status_ok=true

    while IFS=$'\t' read -r cell name; do
      local st
      st=$(curl -sf --max-time 5 "$ADMIN_API/poolers/$cell/$name/status" 2>/dev/null) || {
        log "  [${remaining}s left] could not reach $cell/$name, retrying..."
        status_ok=false
        break
      }

      local pooler_type postgres_running postgres_role has_backup wal_receiver
      pooler_type=$(echo "$st" | jq -r '.status.pooler_type                           // "unknown"')
      postgres_running=$(echo "$st" | jq -r '.status.postgres_running                      // false')
      postgres_role=$(echo "$st" | jq -r '.status.postgres_role                         // "unknown"')
      has_backup=$(echo "$st" | jq -r '.status.has_backup                            // false')
      wal_receiver=$(echo "$st" | jq -r '.status.replication_status.wal_receiver_status // ""')

      if [[ "$pooler_type" == "PRIMARY" ]] &&
        [[ "$postgres_running" == "true" ]] &&
        [[ "$postgres_role" == "primary" ]] &&
        [[ "$has_backup" == "true" ]]; then
        ((n_primary++))
      elif [[ "$pooler_type" == "REPLICA" ]] &&
        [[ "$postgres_running" == "true" ]] &&
        [[ "$postgres_role" == "standby" ]] &&
        [[ "$wal_receiver" == "streaming" ]]; then
        ((n_streaming++))
      else
        log "  [${remaining}s left] $cell/$name: type=$pooler_type role=$postgres_role running=$postgres_running backup=$has_backup wal=$wal_receiver"
      fi
    done < <(echo "$poolers_json" | jq -r '.poolers[] | "\(.id.cell)\t\(.id.name)"')

    if [[ "$status_ok" == "false" ]]; then
      sleep $POLL_INTERVAL
      continue
    fi

    if ((n_primary == 1)) && ((n_streaming == 2)); then
      return 0
    fi

    log "  [${remaining}s left] converging: primary=$n_primary streaming_replicas=$n_streaming"
    sleep $POLL_INTERVAL
  done

  return 1
}

print_debug_hints() {
  log ""
  log "=== Debug hints ==="
  log "Multiorch logs:"
  log "  kubectl logs -n default -l app=multiorch --tail=100"
  log ""
  log "Multipooler logs:"
  log "  kubectl logs -n default multipooler-zone1-0 --tail=100"
  log "  kubectl logs -n default multipooler-zone1-1 --tail=100"
  log "  kubectl logs -n default multipooler-zone1-2 --tail=100"
  log ""
  log "Pooler status via REST API:"
  log "  curl -s http://localhost:18000/api/v1/poolers | jq ."
  log "  for p in zone1-0 zone1-1 zone1-2; do"
  log "    echo \"--- \$p ---\""
  log "    curl -s http://localhost:18000/api/v1/poolers/zone1/\$p/status | jq ."
  log "  done"
  log ""
  log "Pod status:"
  log "  kubectl get pods -n default"
  log ""
  log "To destroy the cluster when done:"
  log "  cd $SCRIPT_DIR && ./teardown.sh"
}

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

log "Bootstrap stress test: $ITERATIONS iterations, ${CONVERGENCE_TIMEOUT}s convergence timeout, ${STAGGER_DELAY}s stagger delay"
log "Repo root: $REPO_ROOT"
log ""

# Build Docker images once before the loop
log "Building Docker images..."
cd "$REPO_ROOT"
make images
log "Docker images built successfully"
log ""

integer succeeded=0
integer failed=0

for i in $(seq 1 "$ITERATIONS"); do
  log "========================================="
  log "Iteration $i / $ITERATIONS"
  log "========================================="

  cd "$SCRIPT_DIR"

  log "Launching infrastructure..."
  ./launch-infra.sh

  log "Launching multigres cluster (1 pod initially)..."
  ./launch-multigres-cluster.sh

  if ((STAGGER_DELAY > 0)); then
    log "Pod-0 ready. Waiting ${STAGGER_DELAY}s before scaling to 3 replicas..."
    sleep $STAGGER_DELAY
    log "Scaling multipooler StatefulSet to 3 replicas..."
    kubectl scale statefulset/multipooler-zone1 --replicas=3
  else
    kubectl scale statefulset/multipooler-zone1 --replicas=3
  fi

  log "Waiting for cluster to converge (timeout: ${CONVERGENCE_TIMEOUT}s)..."
  if check_convergence; then
    log "SUCCESS: Cluster converged — 1 primary + 2 streaming replicas"
    ((++succeeded))
    log "Tearing down cluster..."
    ./teardown.sh
    log ""
  else
    log "FAILURE: Cluster did not converge within ${CONVERGENCE_TIMEOUT}s"
    ((++failed))
    print_debug_hints
    log ""
    log "=== Stopping stress test. Cluster preserved for debugging. ==="
    break
  fi
done

log ""
log "========================================="
log "Stress test complete"
log "  Iterations run : $((succeeded + failed)) / $ITERATIONS"
log "  Succeeded       : $succeeded"
log "  Failed          : $failed"
log "========================================="

if ((failed > 0)); then
  exit 1
fi
