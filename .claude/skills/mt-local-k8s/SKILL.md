---
name: "Local Kind Cluster Manager"
description: "Manage local kind cluster for multigres - start/stop cluster, view logs, restart pods, monitor multiorch, test failover scenarios"
---

# Local Kind Cluster Manager

Manage the local kind cluster for testing multigres in Kubernetes. Provides commands for cluster lifecycle, monitoring, pod operations, and failover testing.

## When to Use This Skill

Invoke this skill when the user asks to:

- Start/stop/check the kind cluster
- View logs from pods (multiorch, multipooler, pgctld, multigateway)
- Restart or delete pods (simulating failures)
- Check multiorch status or pooler health
- Run failover tests
- Connect to PostgreSQL through multigateway or specific poolers
- Monitor replication status
- Troubleshoot cluster issues

## Configuration

### Kubernetes Context

- **Context**: `kind-multidemo`
- **Namespace**: `default`
- **Cluster name**: `multidemo`

### Port Forwards

After starting the cluster, port-forwards should be established:

- **MultiAdmin API**: `localhost:18000` → `service/multiadmin:18000`
- **MultiAdmin gRPC**: `localhost:18070` → `service/multiadmin:18070`
- **Multiorch API**: `localhost:17000` → `deployment/multiorch:17000`
- **Grafana**: `localhost:3000` → `service/observability:3000`

Port-forward scripts are automatically started by launch scripts.

## Cluster-Wide Operations

### Start Cluster

**Launch infrastructure** (etcd, observability, cert-manager, multiadmin):

```bash
cd demo/k8s/ && ./launch-infra.sh
```

**Launch multigres cluster** (multipooler, multiorch, multigateway):

```bash
cd demo/k8s/ && ./launch-multigres-cluster.sh
```

**Full startup** (from scratch):

```bash
cd demo/k8s/ && ./launch-infra.sh && ./launch-multigres-cluster.sh
```

### Stop Cluster

**Teardown** (delete cluster and data):

```bash
cd demo/k8s/ && ./teardown.sh
```

### Build and Reload Images

**When to use each operation:**

1. **Apply configuration changes** - When you modify k8s YAML files (env vars, args, volumes, etc.):

   ```bash
   kubectl --context kind-multidemo apply -f demo/k8s/k8s-multipooler-statefulset.yaml
   # Or for other components:
   kubectl --context kind-multidemo apply -f demo/k8s/k8s-multiorch.yaml
   ```

2. **Rebuild and reload images** - When you modify Go code:

   ```bash
   make images
   kind load docker-image multigres/multigres multigres/pgctld-postgres multigres/multiadmin-web --name=multidemo
   kubectl --context kind-multidemo rollout restart statefulset/multipooler-zone1
   kubectl --context kind-multidemo rollout restart deployment/multiorch
   ```

3. **Restart pods** - When you need to pick up new images after loading them:

   ```bash
   kubectl --context kind-multidemo rollout restart statefulset/multipooler-zone1
   ```

4. **Delete and recreate pods** - When rollout restart doesn't work (stuck pods):

   ```bash
   kubectl --context kind-multidemo delete pod multipooler-zone1-0
   ```

5. **Teardown and recreate cluster** - When you have incompatible changes (protobuf schema changes, etcd data corruption):
   ```bash
   cd demo/k8s/ && ./teardown.sh && ./launch-infra.sh && ./launch-multigres-cluster.sh
   ```

**Build all Docker images**:

```bash
make images
```

This builds:

- `multigres/multigres:latest` (multipooler, multiorch, multigateway, multiadmin, multigres CLI)
- `multigres/pgctld-postgres:latest` (pgctld with PostgreSQL)
- `multigres/multiadmin-web:latest` (multiadmin web UI)

**Load images into kind cluster**:

```bash
kind load docker-image multigres/multigres multigres/pgctld-postgres multigres/multiadmin-web --name=multidemo
```

**Restart pods to use new images**:

```bash
# Restart multipooler StatefulSet
kubectl --context kind-multidemo rollout restart statefulset/multipooler-zone1

# Restart multiorch
kubectl --context kind-multidemo rollout restart deployment/multiorch

# Restart multigateway
kubectl --context kind-multidemo rollout restart deployment/multigateway

# Restart multiadmin
kubectl --context kind-multidemo rollout restart deployment/multiadmin
```

**Full rebuild and reload workflow**:

```bash
# 1. Build binaries
make build

# 2. Build Docker images
make images

# 3. Load into kind
kind load docker-image multigres/multigres multigres/pgctld-postgres multigres/multiadmin-web --name=multidemo

# 4. Restart pods
kubectl --context kind-multidemo rollout restart statefulset/multipooler-zone1
kubectl --context kind-multidemo rollout restart deployment/multiorch
kubectl --context kind-multidemo rollout restart deployment/multigateway
```

### Check Cluster Status

**Get all pods**:

```bash
kubectl --context kind-multidemo get pods -o wide
```

**Check StatefulSet status**:

```bash
kubectl --context kind-multidemo get statefulset multipooler-zone1
```

**Check Deployment status**:

```bash
kubectl --context kind-multidemo get deployments
```

**Check pod restart counts**:

```bash
kubectl --context kind-multidemo get pods -o jsonpath='{range .items[*]}{.metadata.name}{"\t"}{.status.containerStatuses[*].restartCount}{"\n"}{end}'
```

## Pod Operations

### List Pods

**All pods**:

```bash
kubectl --context kind-multidemo get pods
```

**Multipooler pods only**:

```bash
kubectl --context kind-multidemo get pods -l app=multipooler
```

**Multiorch pods**:

```bash
kubectl --context kind-multidemo get pods -l app=multiorch
```

### Restart/Delete Pods

**Delete a specific pod** (simulates crash, will be restarted by StatefulSet/Deployment):

```bash
kubectl --context kind-multidemo delete pod <pod-name>
```

Examples:

```bash
# Delete a multipooler pod
kubectl --context kind-multidemo delete pod multipooler-zone1-0

# Delete multiorch pod
kubectl --context kind-multidemo delete pod -l app=multiorch

# Delete multigateway pod
kubectl --context kind-multidemo delete pod -l app=multigateway
```

### Pod Information

**Describe pod** (events, status, conditions):

```bash
kubectl --context kind-multidemo describe pod <pod-name>
```

**Get pod events**:

```bash
kubectl --context kind-multidemo get events --field-selector involvedObject.name=<pod-name>
```

**Check pod readiness**:

```bash
kubectl --context kind-multidemo get pod <pod-name> -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}'
```

## Logs

### View Logs

**Multipooler logs** (main container):

```bash
kubectl --context kind-multidemo logs pod/<pod-name> -c multipooler
```

**pgctld logs** (sidecar container):

```bash
kubectl --context kind-multidemo logs pod/<pod-name> -c pgctld
```

**Multiorch logs**:

```bash
kubectl --context kind-multidemo logs deployment/multiorch -c multiorch
```

**Multigateway logs**:

```bash
kubectl --context kind-multidemo logs deployment/multigateway -c multigateway
```

**Follow logs** (live tail):

```bash
kubectl --context kind-multidemo logs -f <resource>/<name> -c <container>
```

**Previous container logs** (after restart):

```bash
kubectl --context kind-multidemo logs <pod-name> -c <container> --previous
```

**All pods with label**:

```bash
kubectl --context kind-multidemo logs -f -l app=multipooler --all-containers --tail=50
```

### PostgreSQL Logs

**Access PostgreSQL logs directly**:

```bash
kubectl --context kind-multidemo exec pod/<pod-name> -c pgctld -- \
  tail -f /data/pg_data/postgresql.log
```

## Multiorch Status & Monitoring

### Multiorch API

**Get shard status**:

```bash
curl -s http://localhost:17000/api/v1/shards/postgres/default/0-inf | jq .
```

**Get pooler healths**:

```bash
curl -s http://localhost:17000/api/v1/shards/postgres/default/0-inf | jq '.pooler_healths'
```

**Find current primary**:

```bash
curl -s http://localhost:17000/api/v1/shards/postgres/default/0-inf | \
  jq '.pooler_healths[] | select(.pooler_type=="PRIMARY")'
```

**Get all replicas**:

```bash
curl -s http://localhost:17000/api/v1/shards/postgres/default/0-inf | \
  jq '.pooler_healths[] | select(.pooler_type=="REPLICA")'
```

**Check specific pooler health**:

```bash
curl -s http://localhost:17000/api/v1/shards/postgres/default/0-inf | \
  jq '.pooler_healths[] | select(.service_id=="<service-id>")'
```

### MultiAdmin API

**Get all poolers**:

```bash
curl -s http://localhost:18000/api/v1/poolers | jq .
```

**Get specific pooler status**:

```bash
curl -s http://localhost:18000/api/v1/poolers/<cell>/<service-id>/status | jq .
```

## Execute Commands in Pods

### General kubectl exec

```bash
kubectl --context kind-multidemo exec -it pod/<pod-name> -c <container> -- <command>
```

### PostgreSQL Commands

**Connect to PostgreSQL via Unix socket**:

```bash
kubectl --context kind-multidemo exec -it pod/<pod-name> -c pgctld -- \
  psql -h /data/pg_sockets -p 5432 -U postgres -d postgres
```

**Run SQL query**:

```bash
kubectl --context kind-multidemo exec pod/<pod-name> -c pgctld -- \
  psql -h /data/pg_sockets -p 5432 -U postgres -d postgres \
  -c "SELECT pg_is_in_recovery();"
```

**Check replication status**:

```bash
kubectl --context kind-multidemo exec pod/<pod-name> -c pgctld -- \
  psql -h /data/pg_sockets -p 5432 -U postgres -d postgres \
  -c "SELECT * FROM pg_stat_replication;"
```

### pgctld Commands

**Check pgctld status**:

```bash
kubectl --context kind-multidemo exec pod/<pod-name> -c pgctld -- \
  pgctld status --pooler-dir /data
```

**Stop PostgreSQL**:

```bash
kubectl --context kind-multidemo exec pod/<pod-name> -c pgctld -- \
  pgctld stop --pooler-dir /data
```

**Start PostgreSQL**:

```bash
kubectl --context kind-multidemo exec pod/<pod-name> -c pgctld -- \
  pgctld start --pooler-dir /data
```

## Failover Testing

### Automated Failover Test

**Run continuous failover test** (automatic mode):

```bash
cd demo/k8s/ && go run failover-test.go --yes
```

**Run with debug logging**:

```bash
cd demo/k8s/ && go run failover-test.go --yes --debug
```

**Interactive mode**:

```bash
cd demo/k8s/ && go run failover-test.go
```

What it does:

1. Finds current primary
2. Kills primary PostgreSQL (via pgctld stop)
3. Waits for new primary election
4. Verifies old primary rejoins as standby
5. Checks replication health
6. Repeats

### Manual Failover Test

**1. Find current primary**:

```bash
curl -s http://localhost:17000/api/v1/shards/postgres/default/0-inf | \
  jq -r '.pooler_healths[] | select(.pooler_type=="PRIMARY") | .service_id'
```

**2. Kill primary pod**:

```bash
kubectl --context kind-multidemo delete pod multipooler-zone1-<N>
```

**3. Monitor multiorch logs for failover**:

```bash
kubectl --context kind-multidemo logs -f deployment/multiorch | \
  grep -i "failover\|elect\|primary\|consensus"
```

**4. Check for new primary**:

```bash
curl -s http://localhost:17000/api/v1/shards/postgres/default/0-inf | \
  jq '.pooler_healths[] | select(.pooler_type=="PRIMARY")'
```

**5. Verify old primary rejoined as standby**:

```bash
curl -s http://localhost:17000/api/v1/shards/postgres/default/0-inf | jq '.pooler_healths'
```

## Observability

### Grafana

**Access Grafana** (if port-forward active):

```
http://localhost:3000
```

Default credentials: admin/admin

### Prometheus

**Query Prometheus** (via kubectl port-forward):

```bash
kubectl --context kind-multidemo port-forward svc/prometheus 9090:9090
```

Then access: `http://localhost:9090`

### Check Metrics

**Multipooler metrics**:

```bash
kubectl --context kind-multidemo exec pod/<pod-name> -c multipooler -- \
  curl -s http://localhost:8080/metrics
```

**Multiorch metrics**:

```bash
kubectl --context kind-multidemo exec deployment/multiorch -c multiorch -- \
  curl -s http://localhost:17100/metrics
```

## Troubleshooting

### Pod Won't Start

**Check pod events**:

```bash
kubectl --context kind-multidemo describe pod <pod-name> | grep -A 20 Events
```

**Check container status**:

```bash
kubectl --context kind-multidemo get pod <pod-name> -o jsonpath='{.status.containerStatuses[*]}'
```

**Check image pull**:

```bash
kubectl --context kind-multidemo describe pod <pod-name> | grep -i image
```

### Port Forward Issues

**Kill existing port-forwards**:

```bash
pkill -f "kubectl.*port-forward"
```

**Restart port-forwards**:

```bash
cd demo/k8s/ && ./port-forward-infra.sh
cd demo/k8s/ && ./port-forward-multigres-cluster.sh
```

**Manual port-forward setup**:

```bash
# Multiorch API (port 17000)
kubectl --context kind-multidemo port-forward deployment/multiorch 17000:17000 &

# MultiAdmin API (port 18000)
kubectl --context kind-multidemo port-forward service/multiadmin 18000:18000 &

# MultiAdmin Web UI (port 18080)
kubectl --context kind-multidemo port-forward service/multiadmin-web 18080:18100 &

# Grafana (port 3000)
kubectl --context kind-multidemo port-forward service/observability 3000:3000 &
```

**Access Web UIs**:

After port-forwards are active:

- **MultiAdmin Web UI**: http://localhost:18080 (main admin interface)
- **Multiorch Status**: http://localhost:17000 (API and status page)
- **Grafana Dashboards**: http://localhost:3000 (metrics and monitoring)
  - Default credentials: admin/admin

**Check if port-forwards are active**:

```bash
# List all active port-forwards
ps aux | grep "kubectl.*port-forward" | grep -v grep

# Test if ports are accessible
curl -s http://localhost:17000 | head -5
curl -s http://localhost:18000/api/v1/poolers | jq . | head -10
```

### etcd Issues

**Check etcd logs**:

```bash
kubectl --context kind-multidemo logs statefulset/etcd
```

**Verify etcd is running**:

```bash
kubectl --context kind-multidemo get pod -l app=etcd
```

### Cluster Metadata Issues

**Check createclustermetadata job**:

```bash
kubectl --context kind-multidemo get job createclustermetadata
kubectl --context kind-multidemo logs job/createclustermetadata
```

**Re-run cluster metadata job**:

```bash
kubectl --context kind-multidemo delete job createclustermetadata
kubectl --context kind-multidemo apply -f demo/k8s/k8s-createclustermetadata-job.yaml
```

## Common Workflows

### Test Pod Restart Recovery

1. **Start cluster**:

   ```bash
   cd demo/k8s/ && ./launch-infra.sh && ./launch-multigres-cluster.sh
   ```

2. **Find primary**:

   ```bash
   curl -s http://localhost:17000/api/v1/shards/postgres/default/0-inf | \
     jq -r '.pooler_healths[] | select(.pooler_type=="PRIMARY")'
   ```

3. **Delete primary pod**:

   ```bash
   kubectl --context kind-multidemo delete pod <primary-pod-name>
   ```

4. **Monitor logs**:

   ```bash
   kubectl --context kind-multidemo logs -f deployment/multiorch | \
     grep -i "failover\|elect\|primary"
   ```

5. **Verify new primary elected**:
   ```bash
   curl -s http://localhost:17000/api/v1/shards/postgres/default/0-inf | jq '.pooler_healths'
   ```

### Debug Replication Issues

1. **Check all pooler types**:

   ```bash
   curl -s http://localhost:17000/api/v1/shards/postgres/default/0-inf | \
     jq '.pooler_healths[] | {service_id, pooler_type, postgres_running}'
   ```

2. **Check replication on primary**:

   ```bash
   kubectl --context kind-multidemo exec pod/<primary-pod> -c pgctld -- \
     psql -h /data/pg_sockets -p 5432 -U postgres -d postgres \
     -c "SELECT * FROM pg_stat_replication;"
   ```

3. **Check replication on replica**:

   ```bash
   kubectl --context kind-multidemo exec pod/<replica-pod> -c pgctld -- \
     psql -h /data/pg_sockets -p 5432 -U postgres -d postgres \
     -c "SELECT * FROM pg_stat_wal_receiver;"
   ```

4. **Check multiorch analysis**:
   ```bash
   kubectl --context kind-multidemo logs deployment/multiorch | \
     grep -i "replication\|analysis\|problem"
   ```

## Implementation Notes for Claude

### Parsing kubectl output

When showing results to users:

- Use `jq` for JSON formatting when available
- Summarize pod status (Running, Pending, CrashLoopBackOff)
- Highlight important events (restarts, failures, elections)
- Show timestamps for time-sensitive operations

### Monitoring failover

When monitoring failover scenarios:

1. Start with high-level status (multiorch API)
2. Drill down to logs if issues detected
3. Verify both old and new primary states
4. Check replication lag and LSN progression
5. Report timing (detection → election → rejoin)

### Error handling

If commands fail:

- Check if port-forwards are active
- Verify cluster is running (`kubectl get pods`)
- Check for recent pod restarts
- Suggest checking logs for specific errors

### Expected timings

Document expected behavior for users:

- Pod restart detection: ~5s (health check interval)
- New primary election: ~15-20s
- Old primary rejoin as standby: ~30s
- Replication lag convergence: <1s after stable

## Tips

- **Always check port-forwards first** - Many commands depend on them
- **Use `jq` for JSON output** - Makes API responses readable
- **Monitor multiorch logs during changes** - Shows decision-making
- **Check all three sources**: multiorch API, pod logs, PostgreSQL queries
- **Pod names are dynamic** - Use labels or list pods first
- **StatefulSet pods are numbered** - `multipooler-zone1-0`, `multipooler-zone1-1`, etc.
- **Use `--previous` flag** - To see logs before pod restart
- **Grafana dashboards show trends** - Better than point-in-time queries
- **failover-test.go is comprehensive** - Use it for systematic testing
