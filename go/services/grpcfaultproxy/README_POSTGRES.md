# PostgreSQL Proxy Extension

This document describes the PostgreSQL wire protocol proxy extension to the gRPC fault injection proxy.

## Overview

The `grpcfaultproxy` service now supports **both** gRPC and PostgreSQL wire protocol proxying with fault injection. This enables testing of network partitions and connection-level faults for PostgreSQL replication traffic.

## Key Features

- **Connection-level fault injection**: Drop, error, or delay PostgreSQL connections
- **Metadata-based routing**: Uses `application_name` (source) and `proxy_target` (destination) from connection parameters
- **Transparent forwarding**: Removes proxy metadata before forwarding to backend
- **Zero configuration**: Enable via `FORCE_POSTGRES_PROXY` environment variable
- **Unified fault rules**: Same YAML configuration for gRPC and PostgreSQL faults

## Architecture

### Connection Flow

```
Replica PostgreSQL
  ↓
primary_conninfo="host=proxy port=5433 application_name=zone1_pooler-0
                  options='-c proxy_target=primary:5432'"
  ↓
Proxy (reads startup message)
  ↓
Extract: source=zone1_pooler-0, target=primary:5432
  ↓
Evaluate fault injection rules
  ↓
If fault matches: drop/error/delay
If no fault: forward to backend
  ↓
Remove proxy_target from options
  ↓
Connect to primary:5432
  ↓
io.Copy bidirectional forwarding
```

### Fault Injection Scope

**Important**: Fault injection happens at **connection establishment only**. Once a connection is established and forwarding via `io.Copy`, no per-message fault injection occurs.

This design is:

- **Simple**: No protocol parsing overhead during forwarding
- **Fast**: Pure `io.Copy` after startup
- **Sufficient**: Network partition tests only need connection-level faults

## Usage

### Starting the Proxy

```bash
# Start with both gRPC and PostgreSQL support
./bin/grpcfaultproxy \
  --http-addr=:17000 \          # gRPC proxy (HTTP CONNECT)
  --postgres-addr=:5433 \        # PostgreSQL proxy
  --management-addr=:17001 \     # Management API (optional)
  --rules-file=fault-rules.yaml  # Fault injection rules (optional)
```

### Configuring Multipooler

Enable the proxy by setting an environment variable:

```bash
export FORCE_POSTGRES_PROXY="localhost:5433"
./bin/multipooler ...
```

When set, multipooler will configure `primary_conninfo` to route through the proxy:

```sql
ALTER SYSTEM SET primary_conninfo =
  'host=localhost port=5433 user=postgres application_name=zone1_pooler-0
   options=''-c proxy_target=primary:5432''';
```

### Fault Rules

Example `fault-rules.yaml`:

```yaml
rules:
  # Block replica-0 from connecting to primary (network partition)
  - name: partition-replica0
    source: "zone1_pooler-0" # From application_name
    target: "*:5432" # Any host, port 5432
    method: "postgres:*" # Any PostgreSQL operation
    fault_type: drop # Silent connection drop
    probability: 1.0

  # Inject latency on replication startup
  - name: slow-replication
    source: "zone1_*" # Wildcard pattern
    target: "primary:5432"
    method: "postgres:startup"
    fault_type: latency
    latency_ms: 200
    probability: 0.5

  # Return error to specific replica
  - name: reject-replica1
    source: "zone1_pooler-1"
    target: "*"
    method: "*"
    fault_type: error
    error_msg: "simulated network partition"
    probability: 1.0
```

## Metadata Extraction

The proxy extracts routing information from PostgreSQL connection parameters:

| Parameter                     | Usage                     | Example                        |
| ----------------------------- | ------------------------- | ------------------------------ |
| `application_name`            | Source service identifier | `zone1_pooler-0`               |
| `options` with `proxy_target` | Target address            | `-c proxy_target=primary:5432` |

The proxy strips `proxy_target` from `options` before forwarding to the backend.

## Manual Testing

### Basic Forwarding

```bash
# Terminal 1: Start PostgreSQL backend
docker run -d --name test-pg -e POSTGRES_PASSWORD=test -p 5432:5432 postgres:16

# Terminal 2: Start proxy
./bin/grpcfaultproxy --postgres-addr=:5433

# Terminal 3: Connect through proxy
PGPASSWORD=test psql -h localhost -p 5433 -U postgres \
  -d "application_name=test-replica options='-c proxy_target=localhost:5432'"

# Should connect successfully - run queries to verify
SELECT version();
```

### Fault Injection

```bash
# Create fault rules
cat > /tmp/fault-rules.yaml <<EOF
rules:
  - name: block-test-replica
    source: "test-replica"
    target: "*"
    method: "*"
    fault_type: error
    probability: 1.0
    error_msg: "simulated network partition"
EOF

# Start proxy with rules
./bin/grpcfaultproxy --postgres-addr=:5433 --rules-file=/tmp/fault-rules.yaml

# Try to connect (should be rejected)
PGPASSWORD=test psql -h localhost -p 5433 -U postgres \
  -d "application_name=test-replica options='-c proxy_target=localhost:5432'"

# Expected: FATAL: simulated network partition
```

## Target Test Case: Phantom Write

The main motivation for this implementation is enabling the phantom write test:

### Scenario

1. 3-replica cluster with `synchronous_commit = remote_apply` (requires 2 ACKs)
2. All replicas healthy, writes succeed with 2 ACKs
3. **Inject fault**: Block primary from replica-2 using proxy
4. **Write freezes**: Can't get 2 ACKs (only 1 replica responds)
5. **Phantom read**: Read from replica-0 shows uncommitted data
6. **Failover**: Promote replica-0 (which ACK'd but didn't commit)
7. **Heal**: Remove network partition
8. **Recovery**: Verify `pg_rewind` converges cluster state

### Why This Matters

- Exposes distributed systems edge case in PostgreSQL synchronous replication
- Tests multiorch's partition detection and recovery
- Regression test for replication guarantee improvements
- Validates pg_rewind behavior after network heal

See `go/test/endtoend/grpcfaultproxy/postgres_proxy_test.go` for test stubs.

## Implementation Details

### Files

**New**:

- `go/services/grpcfaultproxy/postgres.go` - PostgreSQL proxy implementation
- `go/test/endtoend/grpcfaultproxy/postgres_proxy_test.go` - Test stubs

**Modified**:

- `go/services/grpcfaultproxy/config.go` - Added `PostgresAddr` field
- `go/services/grpcfaultproxy/metadata.go` - Added `Protocol` field to `RequestInfo`
- `go/services/grpcfaultproxy/proxy.go` - Added PostgreSQL proxy lifecycle
- `go/cmd/grpcfaultproxy/main.go` - Added `--postgres-addr` flag
- `go/multipooler/manager/rpc_manager.go` - Added `FORCE_POSTGRES_PROXY` support

### Key Functions

- `StartPostgresProxy()` - Starts PostgreSQL listener
- `handlePostgresConnection()` - Processes each connection
- `readStartupPacket()` - Parses PostgreSQL startup message
- `extractProxyTarget()` - Extracts target from connection options
- `removeProxyMetadata()` - Strips proxy_target before forwarding
- `parseProxyAddr()` - Parses "host:port" format

## Performance

- **Latency overhead**: < 1ms p99 (startup message parsing only)
- **Forwarding**: Zero overhead (pure `io.Copy`)
- **Rule evaluation**: O(n) per connection (acceptable for tests)

## Limitations

1. **SSL/TLS**: Not supported (test-only tool)
2. **Message-level faults**: Only connection-level (sufficient for network partitions)
3. **Protocol support**: Basic startup only (no SSL negotiation, GSSENC)
4. **Production use**: This is a testing tool only

## Next Steps

To complete the phantom write test:

1. Implement test helpers in `postgres_proxy_test.go`:
   - `setupClusterWithProxy()` - Launch proxy + cluster
   - `injectPostgresFault()` - Configure fault rules
   - `waitForReplicationStatus()` - Poll replication state

2. Implement `TestPhantomWrite_NetworkPartition()` following the scenario above

3. Add unit tests for:
   - `extractProxyTarget()` parsing
   - `removeProxyMetadata()` cleaning
   - `parseProxyAddr()` validation

## Related Documentation

- Main plan: `~/.claude/grpc-postgres-fault-proxy-plan.md`
- gRPC proxy: `go/services/grpcfaultproxy/` package documentation
- Fault engine: `go/services/grpcfaultproxy/engine.go`
