# Visual Exemplar Demonstration with Grafana, Prometheus, and Jaeger

This guide demonstrates how to visualize metrics with exemplars in Grafana and use them to jump directly to the corresponding traces in Jaeger.

## Prerequisites

- Docker and Docker Compose installed
- Multigres cluster running with OpenTelemetry enabled

## Step 1: Start the Observability Stack

```bash
# From the repository root
docker compose -f docker-compose-observability.yml up -d
```

This starts:

- **Jaeger** (UI: http://localhost:16686) - Distributed tracing backend
- **Prometheus** (UI: http://localhost:9090) - Metrics storage (receives via OTLP)
- **Grafana** (UI: http://localhost:3000) - Visualization dashboard

## Step 2: Start Multigres with Telemetry Enabled

```bash
# Use the helper script to start the cluster with telemetry enabled
./observability/multigres-with-telemetry.sh cluster start
```

The helper script automatically configures:

- Traces exported to Jaeger (http://localhost:4318)
- Metrics exported to Prometheus via OTLP (http://localhost:9090)
- 5-second metric export interval for responsive dashboards
- Cumulative temporality (required by Prometheus)

For manual configuration or customization, see the script for the environment variables used.

## Step 3: Generate Traffic with Errors

```bash
# Generate some successful requests and errors
for i in {1..20}; do
  curl -s http://localhost:15100/ > /dev/null
  curl -s http://localhost:15100/does-not-exist > /dev/null
done
```

This creates:

- 20 successful HTTP 200 responses
- 20 HTTP 404 errors

## Step 4: Open Grafana and Explore Exemplars

### Access Grafana

1. Open http://localhost:3000 in your browser
2. The dashboard "Multigres HTTP Metrics with Exemplars" should load automatically
3. No login required (anonymous access enabled for demo)

### Verify Data is Showing

Before exploring exemplars, confirm the dashboard has data:

1. **All 4 panels should show data** - if panels are empty, wait 30 seconds and refresh
2. **Look for colored lines** in the time series charts
3. If still empty, verify metrics in Prometheus: http://localhost:9090/graph → query `http_server_request_duration_seconds_count`

### Understanding the Dashboard

The dashboard has 4 panels, all with exemplars enabled:

#### Panel 1: HTTP Response Codes Over Time

- Shows HTTP request rate grouped by status code (200, 404, etc.)
- Useful for spotting error spikes
- **Exemplars enabled** - click data points to see traces

#### Panel 2: gRPC Response Codes Over Time

- Shows gRPC request rate grouped by status code (0 = OK)
- Useful for monitoring inter-service communication
- **Exemplars enabled** - click data points to see traces

#### Panel 3: HTTP Latency (with Exemplars)

- **This is the key panel for exemplar demonstration!**
- Shows p95 and p50 latency percentiles
- **Data points are clickable** - they contain exemplar links to traces

**How to use:**

1. Look at the chart - you'll see lines showing latency
2. **Hover over a data point** - a tooltip appears showing the metric value
3. **Click on a data point** - a modal appears with "Jaeger" link
4. **Click the Jaeger link** - Opens the exact trace in Jaeger that contributed to that metric!

#### Panel 4: gRPC Latency

- Shows p95 and p50 latency for gRPC calls
- Useful for monitoring internal service latencies
- **Exemplars enabled** - click data points to see traces

### Visual Exemplar Flow

```
1. Notice high latency in Panel 3 (HTTP Latency p95 spike)
   ↓
2. Click on the data point at that time
   ↓
3. Modal appears with exemplar trace_id
   ↓
4. Click "View Trace in Jaeger"
   ↓
5. Jaeger opens showing the EXACT request that caused that latency
   ↓
6. Examine the trace spans to understand what was slow
```

## Step 5: Verify Exemplars in Prometheus

You can also see exemplars directly in the Prometheus UI:

1. Open http://localhost:9090/graph
2. Enter a histogram query like: `http_server_request_duration_seconds_bucket`
3. Click "Execute"
4. Toggle to "Graph" view
5. Enable "Show exemplars" checkbox

Exemplars appear as diamond markers on the graph, each linked to a specific trace.

## Step 6: Correlation Demo - From Error Metric to Trace

### Find 404 Errors in Metrics

1. In Grafana, look at **Panel 1: HTTP Response Codes Over Time**
2. You should see separate lines for status codes 200 and 404
3. Find a data point on the 404 line
4. Click on the data point to see the exemplar modal
5. Click "View Trace in Jaeger"
6. Jaeger opens showing:
   - `http.response.status_code = 404`
   - `url.path = /does-not-exist`
   - `http.request.method = GET`
   - Full timing breakdown of the request

### This demonstrates:

- **Aggregated view** (Panel 1) shows you have 404 errors over time
- **Exemplar** (click data point) lets you jump to a specific error instance
- **Trace** (Jaeger) shows you exactly what happened in that request

## How Exemplars Work

```
┌─────────────────────┐
│  Metrics (Counters, │
│  Histograms)        │ ← Aggregated data (rates, percentiles)
│                     │
│  + Exemplars        │ ← Sample individual traces
└─────────┬───────────┘
          │
          ├─── trace_id: "abc123..."
          └─── span_id: "def456..."
                    │
                    ↓
          ┌─────────────────┐
          │  Jaeger Trace   │
          │  (Full details  │
          │   of request)   │
          └─────────────────┘
```

**Without Exemplars:**

- See p95 latency = 50ms → "Something is slow" → Manual detective work

**With Exemplars:**

- See p95 latency = 50ms → Click data point → Opens trace showing exactly which endpoint/query caused it

## Architecture

```
┌──────────────┐
│  Multigres   │  Exports metrics and traces via OTLP
│  Services    │  with embedded exemplars (trace_id, span_id)
└──────┬───────┘
       │
       │ OTLP (metrics + traces)
       ↓
┌──────────────┐     ┌──────────────┐
│  Prometheus  │     │    Jaeger    │
│  (metrics)   │     │   (traces)   │
└──────┬───────┘     └──────┬───────┘
       │                    │
       └────────┬───────────┘
                ↓
         ┌──────────────┐
         │   Grafana    │  Queries Prometheus for metrics
         │              │  Displays exemplars as clickable links
         │              │  Links to Jaeger traces
         └──────────────┘
```

## Configuration Details

### Prometheus

- Receives metrics via OTLP push (`--web.enable-otlp-receiver`)
- Stores exemplars in memory (`--enable-feature=exemplar-storage`)
- Native histograms enabled (`--enable-feature=native-histograms`)
- No scrape configuration needed - services push metrics directly

### Native Histograms

The setup uses Prometheus native histograms (exponential bucket histograms) which provide:

- More efficient storage than classic bucket-based histograms
- Better resolution without pre-defining bucket boundaries
- Ability to compute any quantile accurately

The helper script configures the OTel SDK to export exponential histograms via:
`OTEL_EXPORTER_OTLP_METRICS_DEFAULT_HISTOGRAM_AGGREGATION=base2_exponential_bucket_histogram`

### Grafana Datasources

- **Prometheus**: Points to http://prometheus:9090
- **Jaeger**: Points to http://jaeger:16686
- **Exemplar Configuration**: Links `trace_id` from Prometheus to Jaeger traces

### Grafana Dashboard

- Pre-configured to show exemplars
- `exemplar: true` enabled on histogram queries
- Instant queries (`range: false`) show individual exemplar points

## Cleanup

```bash
# Stop Multigres cluster (with telemetry to capture shutdown spans)
./observability/multigres-with-telemetry.sh cluster stop

# Stop observability stack
docker compose -f docker-compose-observability.yml down
```

## Troubleshooting

### No Exemplars Appearing in Grafana

**Check 1: Verify metrics are reaching Prometheus**

1. Open http://localhost:9090/graph
2. Query for `http_server_request_duration_seconds_bucket`
3. If no results, check that `OTEL_METRICS_EXPORTER=otlp` is set

**Check 2: Verify traces are reaching Jaeger**

1. Open http://localhost:16686
2. Search for service "multigateway"
3. Should see traces

**Check 3: Panel configuration**

- Edit panel in Grafana
- Check "Exemplar" toggle is ON
- Query must be histogram_quantile() or rate() on histogram metric

### Port Conflicts

If ports 3000, 9090, 16686, or 4318 are already in use:

```bash
# Check what's using the ports
lsof -i :3000
lsof -i :9090
lsof -i :16686
lsof -i :4318

# Stop conflicting services or edit docker-compose-observability.yml to use different ports
```

## Next Steps

1. **Custom Metrics**: Add application-specific metrics with exemplars
2. **Alerts**: Configure Prometheus alerts based on error rates
3. **SLOs**: Use exemplars to debug SLO violations
4. **Production**: Use managed observability services (Grafana Cloud, Datadog, etc.) with exemplar support
