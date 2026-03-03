#!/usr/bin/env bash
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

set -euo pipefail

# Kubernetes configuration
KUBECTL_CONTEXT="kind-multidemo"
NAMESPACE="default"
K8S_DIR="demo/k8s"

# API endpoints
MULTIORCH_API="http://localhost:17000"
MULTIADMIN_API="http://localhost:18000"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Helper functions
error() {
  echo -e "${RED}Error: $1${NC}" >&2
  exit 1
}

success() {
  echo -e "${GREEN}$1${NC}"
}

info() {
  echo -e "${BLUE}$1${NC}"
}

warn() {
  echo -e "${YELLOW}$1${NC}"
}

# Check if kubectl context exists
check_context() {
  if ! kubectl config get-contexts "$KUBECTL_CONTEXT" &>/dev/null; then
    error "kubectl context '$KUBECTL_CONTEXT' not found. Is the kind cluster running?"
  fi
}

# Main command router
main() {
  local command="${1:-help}"
  shift || true

  case "$command" in
  cluster)
    cluster_command "$@"
    ;;
  logs)
    logs_command "$@"
    ;;
  pods)
    pods_command "$@"
    ;;
  status)
    status_command "$@"
    ;;
  exec)
    exec_command "$@"
    ;;
  restart-pod)
    restart_pod_command "$@"
    ;;
  primary)
    primary_command "$@"
    ;;
  replicas)
    replicas_command "$@"
    ;;
  failover-test)
    failover_test_command "$@"
    ;;
  psql)
    psql_command "$@"
    ;;
  help | *)
    show_help
    ;;
  esac
}

cluster_command() {
  local subcommand="${1:-status}"
  shift || true

  case "$subcommand" in
  start)
    info "Starting kind cluster infrastructure..."
    cd "$K8S_DIR" && ./launch-infra.sh
    success "Infrastructure started"
    info "Starting multigres cluster..."
    ./launch-multigres-cluster.sh
    success "Multigres cluster started"
    ;;
  start-infra)
    info "Starting kind cluster infrastructure..."
    cd "$K8S_DIR" && ./launch-infra.sh
    success "Infrastructure started"
    ;;
  start-multigres)
    info "Starting multigres cluster..."
    cd "$K8S_DIR" && ./launch-multigres-cluster.sh
    success "Multigres cluster started"
    ;;
  stop)
    info "Stopping kind cluster..."
    cd "$K8S_DIR" && ./teardown.sh
    success "Cluster stopped"
    ;;
  status)
    check_context
    kubectl --context "$KUBECTL_CONTEXT" get pods -o wide
    ;;
  restart-forwards)
    info "Restarting port-forwards..."
    pkill -f "kubectl.*port-forward" || true
    cd "$K8S_DIR"
    ./port-forward-infra.sh &
    ./port-forward-multigres-cluster.sh &
    success "Port-forwards restarted"
    ;;
  *)
    error "Unknown cluster subcommand: $subcommand"
    ;;
  esac
}

logs_command() {
  check_context
  local component="${1:-}"
  local pod_name="${2:-}"
  local flags=("${@:3}")

  if [[ -z "$component" ]]; then
    error "Usage: logs <component> [pod-name] [-f]"
  fi

  case "$component" in
  multiorch)
    kubectl --context "$KUBECTL_CONTEXT" logs deployment/multiorch -c multiorch "${flags[@]}"
    ;;
  multigateway)
    kubectl --context "$KUBECTL_CONTEXT" logs deployment/multigateway -c multigateway "${flags[@]}"
    ;;
  multipooler)
    if [[ -z "$pod_name" ]]; then
      kubectl --context "$KUBECTL_CONTEXT" logs -l app=multipooler -c multipooler "${flags[@]}"
    else
      kubectl --context "$KUBECTL_CONTEXT" logs "pod/$pod_name" -c multipooler "${flags[@]}"
    fi
    ;;
  pgctld)
    if [[ -z "$pod_name" ]]; then
      error "Pod name required for pgctld logs"
    fi
    kubectl --context "$KUBECTL_CONTEXT" logs "pod/$pod_name" -c pgctld "${flags[@]}"
    ;;
  postgres)
    if [[ -z "$pod_name" ]]; then
      error "Pod name required for postgres logs"
    fi
    kubectl --context "$KUBECTL_CONTEXT" exec "pod/$pod_name" -c pgctld -- \
      tail "${flags[@]}" /data/pg_data/postgresql.log
    ;;
  *)
    error "Unknown component: $component. Use: multiorch, multigateway, multipooler, pgctld, postgres"
    ;;
  esac
}

pods_command() {
  check_context
  local filter="${1:-all}"

  case "$filter" in
  all)
    kubectl --context "$KUBECTL_CONTEXT" get pods -o wide
    ;;
  multipooler)
    kubectl --context "$KUBECTL_CONTEXT" get pods -l app=multipooler -o wide
    ;;
  multiorch)
    kubectl --context "$KUBECTL_CONTEXT" get pods -l app=multiorch -o wide
    ;;
  multigateway)
    kubectl --context "$KUBECTL_CONTEXT" get pods -l app=multigateway -o wide
    ;;
  restarts)
    kubectl --context "$KUBECTL_CONTEXT" get pods \
      -o jsonpath='{range .items[*]}{.metadata.name}{"\t"}{.status.containerStatuses[*].restartCount}{"\n"}{end}'
    ;;
  *)
    error "Unknown filter: $filter. Use: all, multipooler, multiorch, multigateway, restarts"
    ;;
  esac
}

status_command() {
  local target="${1:-all}"

  case "$target" in
  all)
    info "Fetching cluster status..."
    curl -s "$MULTIORCH_API/api/v1/shards/postgres/default/0-inf" | jq '.'
    ;;
  primary)
    info "Finding primary..."
    curl -s "$MULTIORCH_API/api/v1/shards/postgres/default/0-inf" |
      jq '.pooler_healths[] | select(.pooler_type=="PRIMARY")'
    ;;
  replicas)
    info "Finding replicas..."
    curl -s "$MULTIORCH_API/api/v1/shards/postgres/default/0-inf" |
      jq '.pooler_healths[] | select(.pooler_type=="REPLICA")'
    ;;
  poolers)
    info "Fetching all poolers..."
    curl -s "$MULTIADMIN_API/api/v1/poolers" | jq '.'
    ;;
  *)
    error "Unknown status target: $target. Use: all, primary, replicas, poolers"
    ;;
  esac
}

exec_command() {
  check_context
  local pod_name="${1:-}"
  local container="${2:-}"
  shift 2 || error "Usage: exec <pod-name> <container> [command...]"

  kubectl --context "$KUBECTL_CONTEXT" exec -it "pod/$pod_name" -c "$container" -- "$@"
}

restart_pod_command() {
  check_context
  local pod_name="${1:-}"

  if [[ -z "$pod_name" ]]; then
    error "Usage: restart-pod <pod-name>"
  fi

  warn "Deleting pod: $pod_name (will be restarted by controller)"
  kubectl --context "$KUBECTL_CONTEXT" delete pod "$pod_name"
  success "Pod deleted"
}

primary_command() {
  info "Finding current primary..."
  local service_id
  service_id=$(curl -s "$MULTIORCH_API/api/v1/shards/postgres/default/0-inf" |
    jq -r '.pooler_healths[] | select(.pooler_type=="PRIMARY") | .service_id')

  if [[ -z "$service_id" ]]; then
    error "No primary found!"
  fi

  success "Primary service ID: $service_id"

  # Get full status
  curl -s "$MULTIORCH_API/api/v1/shards/postgres/default/0-inf" |
    jq ".pooler_healths[] | select(.service_id==\"$service_id\")"
}

replicas_command() {
  info "Finding all replicas..."
  curl -s "$MULTIORCH_API/api/v1/shards/postgres/default/0-inf" |
    jq '.pooler_healths[] | select(.pooler_type=="REPLICA")'
}

failover_test_command() {
  local flags=("$@")

  info "Running failover test..."
  cd "$K8S_DIR" && go run failover-test.go "${flags[@]}"
}

psql_command() {
  check_context
  local target="${1:-multigateway}"
  shift || true

  case "$target" in
  multigateway)
    info "Connecting to multigateway..."
    kubectl --context "$KUBECTL_CONTEXT" exec -it deployment/multigateway -c multigateway -- \
      psql -h localhost -p 15432 -U postgres -d postgres "$@"
    ;;
  *)
    # Assume it's a pod name for multipooler
    info "Connecting to multipooler pod: $target"
    kubectl --context "$KUBECTL_CONTEXT" exec -it "pod/$target" -c pgctld -- \
      psql -h /data/pg_sockets -p 5432 -U postgres -d postgres "$@"
    ;;
  esac
}

show_help() {
  cat <<EOF
mt-local-k8s - Local Kind Cluster Manager for Multigres

Usage: /mt-local-k8s <command> [options]

Cluster Management:
  cluster start           Start infrastructure and multigres cluster
  cluster start-infra     Start only infrastructure (etcd, observability)
  cluster start-multigres Start only multigres components
  cluster stop            Stop and delete cluster
  cluster status          Show all pods
  cluster restart-forwards Restart port-forwards

Pod Operations:
  pods [filter]           List pods (all|multipooler|multiorch|multigateway|restarts)
  restart-pod <name>      Delete pod (will be restarted)

Logs:
  logs multiorch [-f]                  Multiorch logs
  logs multigateway [-f]               Multigateway logs
  logs multipooler [pod-name] [-f]     Multipooler logs
  logs pgctld <pod-name> [-f]          pgctld logs
  logs postgres <pod-name> [-f]        PostgreSQL logs

Status & Monitoring:
  status [target]         Get status (all|primary|replicas|poolers)
  primary                 Find current primary
  replicas                Find all replicas

Failover Testing:
  failover-test [flags]   Run automated failover test
                          (e.g., --yes --debug)

PostgreSQL Access:
  psql [target]           Connect with psql
                          target: multigateway (default) or pod-name

Low-Level:
  exec <pod> <container> <cmd>  Execute command in pod

Examples:
  /mt-local-k8s cluster start
  /mt-local-k8s logs multiorch -f
  /mt-local-k8s restart-pod multipooler-zone1-0
  /mt-local-k8s status primary
  /mt-local-k8s failover-test --yes
  /mt-local-k8s psql multigateway

Environment:
  KUBECTL_CONTEXT: $KUBECTL_CONTEXT
  NAMESPACE: $NAMESPACE
  MULTIORCH_API: $MULTIORCH_API
  MULTIADMIN_API: $MULTIADMIN_API
EOF
}

# Run main
main "$@"
