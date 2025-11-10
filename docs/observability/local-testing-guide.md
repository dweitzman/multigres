# OpenTelemetry Local Testing Guide

This guide shows you how to test OpenTelemetry instrumentation locally using Jaeger for traces and the Prometheus metrics endpoint.

## Quick Start: Jaeger with Docker (Recommended)

The simplest way to test distributed tracing locally is with Jaeger all-in-one using Docker:

### 1. Start Jaeger

```bash
docker run --rm --name jaeger \
  -p 16686:16686 \
  -p 4318:4318 \
  -p 4317:4317 \
  jaegertracing/jaeger:2.3.0
```

This starts:

- **Jaeger UI**: http://localhost:16686
- **OTLP HTTP endpoint**: http://localhost:4318
- **OTLP gRPC endpoint**: http://localhost:4317

Leave this running and use Ctrl-C to stop when finished.

### 2. Start Multigres with OTel Enabled

In a new terminal:

```bash
# Set OpenTelemetry environment variables
export OTEL_EXPORTER_OTLP_ENDPOINT="http://localhost:4318"
export OTEL_TRACES_EXPORTER="otlp"
export OTEL_METRICS_EXPORTER="otlp"
export OTEL_TRACES_SAMPLER="always_on"
export OTEL_SERVICE_NAME="multigres-local"

# Start cluster
./bin/multigres cluster start
```

> **Note**: Telemetry is always initialized in Multigres. When `OTEL_TRACES_EXPORTER` or `OTEL_METRICS_EXPORTER` are not set (or set to "none"), noop exporters are used with minimal overhead.

### 3. View Traces

1. Open http://localhost:16686 in your browser
2. Select "multigres-local" from the Service dropdown
3. Click "Find Traces"
4. You should see a trace for "cluster_startup"
5. Click on the trace to explore:
   - The provisioner's Bootstrap span with operation name `cluster_startup`
   - Child spans for each service startup:
     - etcd
     - multiadmin
     - multigateway
     - multipooler (per database)
     - pgctld (per database)
     - multiorch (per database)
   - Health check spans (`wait_for_service_ready`) within each service startup
   - gRPC Status calls to pgctld during health checks
   - HTTP health check requests to `/live` endpoints

**Key observations:**

- All spans should be part of the same trace ID
- Parent-child relationships should be correctly established
- The `STARTUP_TRACEPARENT` environment variable propagates trace context to subprocesses

### 4. View Metrics

Check the Prometheus metrics endpoint on any service:

```bash
# Multiadmin metrics
curl http://localhost:8080/metrics

# Multigateway metrics
curl http://multigateway-zone1.localhost:8080/metrics

# Multipooler metrics
curl http://multipooler-zone1.localhost:8080/metrics
```

You should see metrics like:

- `rpc_server_duration_milliseconds` - gRPC request durations with exemplars
- `http_server_duration_milliseconds` - HTTP request durations with exemplars
- `target_info` - Service identification metadata

**Exemplars** link individual metric samples to their corresponding distributed traces. They appear in the OpenMetrics format:

```bash
# Request metrics with OpenMetrics format to see exemplars
curl -H "Accept: application/openmetrics-text" http://localhost:15100/metrics | grep "# {trace_id"
```

Example output:

```
http_server_request_duration_seconds_bucket{...,le="0.005"} 11 # {trace_id="0d7f9f991279712d5248fc8c6d522208",span_id="05121bb1dd5635c3"} 0.000702375 1.762809925577199e+09
```

The exemplar (after `#`) shows:

- `trace_id`: The distributed trace ID you can look up in Jaeger
- `span_id`: The specific span that recorded this measurement
- Value: The actual measurement value (duration in seconds)
- Timestamp: When this sample was recorded

### 5. Verify Trace Context in Logs

Check that trace IDs are automatically injected into logs:

```bash
# View logs from any service with jq for pretty printing
tail -f logs/multipooler_*.log | jq '{time, level, msg, trace_id, span_id}'
```

Expected output:

```json
{
  "time": "2025-01-07T10:30:45Z",
  "level": "INFO",
  "msg": "Processing request",
  "trace_id": "4bf92f3577b34da6a3ce929d0e0e4736",
  "span_id": "00f067aa0ba902b7"
}
```

## Testing Scenarios

### Scenario 1: Startup Trace End-to-End

**Goal**: Verify that the entire cluster startup is captured in a single distributed trace with proper parent-child relationships

**Steps**:

1. Start Jaeger all-in-one
2. Start cluster with `OTEL_TRACES_EXPORTER=otlp` and `OTEL_TRACES_SAMPLER=always_on` environment variables
3. Open Jaeger UI and search for traces
4. Find the trace with operation `cluster_startup`

**Success Criteria**:

- ✅ Single trace contains all service startups
- ✅ Root span is `cluster_startup` from the provisioner
- ✅ Each service (etcd, multiadmin, multigateway, multipooler, pgctld, multiorch) has child spans
- ✅ Health check spans (`wait_for_service_ready`) are children of their respective service startup spans
- ✅ gRPC health check calls show up as spans with proper parent relationships
- ✅ All spans have the same trace ID
- ✅ Span durations are reasonable (startup typically takes 2-5 seconds total)

**What to look for in the trace**:

```
cluster_startup (provisioner)
├── start_etcd
│   └── wait_for_service_ready (etcd)
├── start_multiadmin
│   └── wait_for_service_ready (multiadmin)
└── start_database_services (db1)
    ├── start_multigateway
    │   └── wait_for_service_ready (multigateway)
    ├── start_multipooler
    │   └── wait_for_service_ready (multipooler)
    ├── start_pgctld
    │   └── wait_for_service_ready (pgctld)
    │       └── PgCtld/Status (gRPC call)
    └── start_multiorch
        └── wait_for_service_ready (multiorch)
```

### Scenario 2: gRPC Request Tracing

**Goal**: Verify gRPC calls are automatically traced with OTel interceptors

**Steps**:

1. Start cluster with OTel enabled
2. Make a gRPC call (e.g., via multigateway to multipooler, or multipooler to pgctld)
3. Search Jaeger for the gRPC method name

**Success Criteria**:

- ✅ Trace shows client and server spans
- ✅ Span attributes include:
  - `rpc.method` - The gRPC method name
  - `rpc.service` - The gRPC service name
  - `rpc.system` - "grpc"
  - `net.peer.name` or `net.peer.ip` - Target address
- ✅ Errors (if any) are recorded in spans with proper status codes
- ✅ Trace context propagates correctly across service boundaries

### Scenario 3: HTTP Request Tracing

**Goal**: Verify HTTP endpoints are automatically traced

**Steps**:

1. Start cluster with OTel enabled
2. Access an HTTP endpoint: `curl http://localhost:8080/live`
3. Search Jaeger for HTTP operations

**Success Criteria**:

- ✅ Trace shows HTTP server span
- ✅ Span attributes include:
  - `http.method` - GET, POST, etc.
  - `http.route` or `http.target` - Request path
  - `http.status_code` - Response status
  - `http.scheme` - http or https
- ✅ Response times are recorded accurately

### Scenario 4: Trace Context in Logs

**Goal**: Verify trace IDs appear in all log entries during traced operations

**Steps**:

1. Start cluster with OTel enabled
2. Trigger a traced operation (e.g., startup, gRPC call, HTTP request)
3. Check logs for `trace_id` and `span_id` fields
4. Copy a trace_id from logs
5. Search for that trace_id in Jaeger

**Success Criteria**:

- ✅ All log entries during a trace have `trace_id` field populated
- ✅ Log entries have `span_id` matching their current span
- ✅ Trace IDs in logs match trace IDs in Jaeger
- ✅ Can correlate logs to traces by trace ID
- ✅ Multiple log entries from the same operation share the same trace_id

### Scenario 5: Metrics with Exemplars

**Goal**: Verify metrics include exemplars linking to traces, enabling correlation from aggregated metrics to individual traces

**Steps**:

1. Start Jaeger (if not already running):

   ```bash
   docker run -d --name jaeger -p 16686:16686 -p 4318:4318 jaegertracing/jaeger:2.3.0
   ```

2. Start cluster with tracing enabled:

   ```bash
   OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4318 \
   OTEL_TRACES_EXPORTER=otlp \
   OTEL_METRICS_EXPORTER=none \
   ./bin/multigres cluster start
   ```

3. Generate traffic to create metrics and traces:

   ```bash
   for i in {1..10}; do curl -s http://localhost:15100/ > /dev/null; done
   ```

4. Request metrics in OpenMetrics format to see exemplars:

   ```bash
   curl -H "Accept: application/openmetrics-text" http://localhost:15100/metrics | grep '# {trace_id'
   ```

5. Extract a trace ID from an exemplar and look it up in Jaeger:
   ```bash
   TRACE_ID=$(curl -s -H "Accept: application/openmetrics-text" http://localhost:15100/metrics | grep -m 1 '# {trace_id' | sed -E 's/.*trace_id="([^"]+)".*/\1/')
   echo "View trace: http://localhost:16686/trace/$TRACE_ID"
   ```

**Success Criteria**:

- ✅ Histogram metrics have exemplar comments in OpenMetrics format
- ✅ Exemplar format includes `trace_id` and `span_id`
- ✅ Exemplar trace IDs can be found in Jaeger
- ✅ Exemplar duration values match the corresponding span duration in the trace
- ✅ Can navigate from high-latency metric exemplar to specific trace in Jaeger for root cause analysis

Example exemplar in metrics output:

```
http_server_request_duration_seconds_bucket{http_request_method="GET",http_response_status_code="200",...,le="0.005"} 11 # {trace_id="0d7f9f991279712d5248fc8c6d522208",span_id="05121bb1dd5635c3"} 0.000702375 1.762809925577199e+09
```

**How Exemplars Work**:

Exemplars provide a powerful observability workflow:

1. **Monitor aggregated metrics** - Notice anomalies in histogram buckets (e.g., high latency)
2. **Find example traces** - Use the exemplar's trace_id to jump to a real request that contributed to that metric
3. **Analyze root cause** - Examine the full distributed trace to understand what caused the issue

**Note**: Exemplars only appear when using OpenMetrics format (`Accept: application/openmetrics-text`). The standard Prometheus text format does not support exemplars.

## Configuration Options

### Environment Variables (Standard OTel)

| Variable                      | Description                                                  | Example                                                            | Default                        |
| ----------------------------- | ------------------------------------------------------------ | ------------------------------------------------------------------ | ------------------------------ |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | OTLP collector endpoint                                      | `http://localhost:4318`                                            | -                              |
| `OTEL_EXPORTER_OTLP_PROTOCOL` | OTLP protocol                                                | `http/protobuf`, `grpc`                                            | `http/protobuf`                |
| `OTEL_TRACES_EXPORTER`        | Traces exporter type                                         | `otlp`, `console`, `none`                                          | `otlp`                         |
| `OTEL_METRICS_EXPORTER`       | Metrics exporter type                                        | `otlp`, `console`, `none`                                          | `otlp`                         |
| `OTEL_TRACES_SAMPLER`         | Sampling strategy                                            | `always_on`, `always_off`, `traceidratio`, `parentbased_always_on` | `parentbased_always_on`        |
| `OTEL_TRACES_SAMPLER_ARG`     | Sampler ratio (for traceidratio)                             | `0.01` (1%)                                                        | `1.0` (100%)                   |
| `COMMAND_OTEL_TRACES_SAMPLER` | CLI command-specific sampler (not inherited by subprocesses) | `always_on`                                                        | -                              |
| `OTEL_SERVICE_NAME`           | Service name in traces                                       | `multipooler`                                                      | Auto-detected from binary name |
| `OTEL_RESOURCE_ATTRIBUTES`    | Additional resource attributes                               | `deployment.environment=local,version=1.0.0`                       | None                           |

### Sampling Strategies

- `always_on`: Sample 100% of traces (high overhead, use for testing only)
- `always_off`: Sample 0% of traces (disables tracing)
- `traceidratio`: Sample based on ratio set in `OTEL_TRACES_SAMPLER_ARG` (e.g., 0.01 = 1%)
- `parentbased_always_on`: Always sample if parent was sampled, otherwise always sample (default)
- `parentbased_traceidratio`: Sample based on ratio for root spans, always sample children

### CLI-Specific Sampling

The `COMMAND_OTEL_TRACES_SAMPLER` environment variable allows you to set different sampling behavior for CLI commands without affecting subprocesses they start:

- If `OTEL_TRACES_SAMPLER` is not set and `COMMAND_OTEL_TRACES_SAMPLER` is set, the CLI command will use `COMMAND_OTEL_TRACES_SAMPLER` for its sampling strategy
- Subprocesses started by the CLI command will not inherit `COMMAND_OTEL_TRACES_SAMPLER` behavior (they use standard `OTEL_TRACES_SAMPLER`)
- This is useful for debugging CLI operations with `always_on` sampling while keeping services at lower sampling rates

Example:

```bash
# CLI uses always_on sampling, but services use parentbased_always_on (default)
export COMMAND_OTEL_TRACES_SAMPLER=always_on
./bin/multigres cluster start

# Both CLI and services use always_on sampling
export OTEL_TRACES_SAMPLER=always_on
./bin/multigres cluster start
```

### Endpoints

- **Prometheus metrics**: Always available at `/metrics` endpoint (minimal overhead when not scraped)
- **pprof profiling**: Available at `/debug/pprof/*` when `--pprof-http` flag is set

## Troubleshooting

### No Traces Appear in Jaeger

**Symptoms**: Jaeger UI shows no traces after starting cluster

**Check**:

1. Jaeger is running: `curl http://localhost:16686`
2. OTLP endpoint is reachable: `curl http://localhost:4318/v1/traces` (should return 405 Method Not Allowed, which is expected)
3. `OTEL_TRACES_EXPORTER` is set to `otlp` (or unset, defaults to `otlp`)
4. `OTEL_EXPORTER_OTLP_ENDPOINT` is set correctly (e.g., `http://localhost:4318`)
5. `OTEL_TRACES_SAMPLER` is not set to `always_off`
6. Services are actually starting (check `./bin/multigres cluster status`)

**Debug**:

```bash
# Check OTel configuration in service logs
grep -i "opentelemetry\|otel\|tracing" logs/*.log

# Verify OTLP endpoint from inside the process
curl -X POST http://localhost:4318/v1/traces \
  -H "Content-Type: application/json" \
  -d '{"resourceSpans":[]}'
```

### Trace IDs Missing from Logs

**Symptoms**: Log entries don't have `trace_id` or `span_id` fields

**Check**:

1. Telemetry is initialized (check for "OpenTelemetry initialized" in startup logs)
2. Operations are actually being traced (verify in Jaeger)
3. Log format is JSON (`--log-format json`)

**Debug**:

```bash
# Check if slog handler was wrapped with OTel bridge
grep "logging initialized" logs/*.log

# Verify log format
head -1 logs/multipooler_*.log | jq '.'
```

### Metrics Endpoint Returns 503 or 404

**Symptoms**: `curl http://localhost:8080/metrics` returns 503 or 404

**Check**:

1. Telemetry is initialized (check for "OpenTelemetry initialized" in startup logs)
2. Service has HTTP port configured and is listening (`--http-port` for services)
3. Check that you're accessing the correct port (multiadmin: 8080, multigateway/multipooler: varies by zone)

**Debug**:

```bash
# Check if service is listening on HTTP port
curl -v http://localhost:8080/live

# Check if metrics endpoint exists
curl -v http://localhost:8080/metrics 2>&1 | head -20
```

### Startup Trace Not Showing Child Spans

**Symptoms**: Jaeger shows `cluster_startup` trace but no child spans for individual services

**Check**:

1. `STARTUP_TRACEPARENT` environment variable is being set in subprocesses
2. Services are parsing the traceparent correctly
3. All services have OTel enabled

**Debug**:

```bash
# Check if STARTUP_TRACEPARENT was passed to subprocesses
ps auxe | grep multipool | tr ' ' '\n' | grep STARTUP_TRACEPARENT

# Check service logs for trace context parsing
grep "Parsed startup trace parent\|trace_id\|span_id" logs/*.log | head -20
```

### Health Check gRPC Calls Not Appearing in Trace

**Symptoms**: `wait_for_service_ready` spans appear but gRPC Status calls to pgctld don't show up as child spans

**Check**:

1. Context is being propagated through health check functions
2. gRPC interceptors are registered on both client and server
3. pgctld service has OTel enabled

**Debug**:
Look for the `PgCtld/Status` operation in Jaeger. It should appear as a child span of `wait_for_service_ready`.

## Performance Impact

### With `always_on` Sampling (100%)

**Use case**: Local development and testing

- **CPU overhead**: ~10-20%
- **Memory overhead**: ~50-100 MB per service
- **Network**: ~10-50 KB/s per service for trace export
- **Disk**: Minimal (traces exported, not stored locally)

### With `traceidratio=0.01` Sampling (1%)

**Use case**: Production

- **CPU overhead**: ~1-2%
- **Memory overhead**: ~10-20 MB per service
- **Network**: ~1-5 KB/s per service
- **Disk**: Minimal

**Recommendation**:

- **Local/Dev**: Use `always_on` for complete visibility
- **Staging**: Use `traceidratio=0.1` (10%) for good coverage
- **Production**: Use `traceidratio=0.01` (1%) or `parentbased_traceidratio=0.01`

## Next Steps

Once you've verified local testing works:

1. **Test with real workload**: Run queries through multigateway and verify end-to-end tracing
2. **Experiment with sampling**: Try different sampling rates to find the right balance
3. **Add custom spans**: Instrument critical code paths with manual spans
4. **Add custom metrics**: Track application-specific metrics beyond the automatic instrumentation
5. **Production setup**: Configure OTLP exporter to send to your production collector (e.g., Grafana Cloud, Datadog, New Relic)

## Alternative: Jaeger without Docker

If you don't have Docker installed, you can download the Jaeger binary directly:

### Download Jaeger

Visit the [Jaeger releases page](https://github.com/jaegertracing/jaeger/releases) or use these direct links:

```bash
# macOS (Apple Silicon)
curl -LO https://github.com/jaegertracing/jaeger/releases/download/v2.3.0/jaeger-2.3.0-darwin-arm64.tar.gz
tar -xzf jaeger-2.3.0-darwin-arm64.tar.gz
cd jaeger-2.3.0-darwin-arm64

# macOS (Intel)
curl -LO https://github.com/jaegertracing/jaeger/releases/download/v2.3.0/jaeger-2.3.0-darwin-amd64.tar.gz
tar -xzf jaeger-2.3.0-darwin-amd64.tar.gz
cd jaeger-2.3.0-darwin-amd64

# Linux
curl -LO https://github.com/jaegertracing/jaeger/releases/download/v2.3.0/jaeger-2.3.0-linux-amd64.tar.gz
tar -xzf jaeger-2.3.0-linux-amd64.tar.gz
cd jaeger-2.3.0-linux-amd64
```

### Start Jaeger

```bash
./jaeger-all-in-one
```

This starts the same endpoints as the Docker version. Use Ctrl-C to stop when finished.

## References

- [OpenTelemetry Go Documentation](https://opentelemetry.io/docs/languages/go/)
- [Jaeger Documentation](https://www.jaegertracing.io/docs/)
- [Prometheus Exemplars](https://prometheus.io/docs/prometheus/latest/feature_flags/#exemplars-storage)
- [W3C Trace Context Specification](https://www.w3.org/TR/trace-context/)
- [OpenTelemetry Implementation Plan](./otel-implementation-plan.md)
