# Visual Exemplar Demonstration with Grafana, Prometheus, and Jaeger

This guide demonstrates how to visualize metrics with exemplars in Grafana and use them to jump directly to the corresponding traces in Jaeger.

## Prerequisites

- Docker and Docker Compose installed
- Multigres cluster running with OpenTelemetry enabled

## Step 1: Start the Observability Stack

```bash
# From the repository root
docker-compose -f docker-compose-observability.yml up -d
```

This starts:

- **Jaeger** (UI: http://localhost:16686) - Distributed tracing backend
- **Prometheus** (UI: http://localhost:9090) - Metrics scraper and storage
- **Grafana** (UI: http://localhost:3000) - Visualization dashboard

## Step 2: Start Multigres with Tracing Enabled

```bash
# Start the cluster with OpenTelemetry tracing
OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4318 \
OTEL_TRACES_EXPORTER=otlp \
OTEL_METRICS_EXPORTER=none \
OTEL_TRACES_SAMPLER=always_on \
./bin/multigres cluster start
```

The provisioner automatically generates `multigres_local/observability/prometheus.yml` with all service endpoints.

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

### Understanding the Dashboard

The dashboard has 4 panels demonstrating different aspects of exemplars:

#### Panel 1: HTTP Request Rate

- Shows requests per second over time
- No exemplars (counter metric)

#### Panel 2: HTTP Request Duration (with Exemplars)

- **This is the key panel for exemplar demonstration!**
- Shows p95 and p50 latency percentiles
- **Data points are clickable** - they contain exemplar links to traces

**How to use:**

1. Look at the chart - you'll see lines showing latency
2. **Hover over a data point** - a tooltip appears showing the metric value
3. **Click on a data point** - a modal appears with "Jaeger" link
4. **Click the Jaeger link** - Opens the exact trace in Jaeger that contributed to that metric!

#### Panel 3: HTTP Status Codes

- Bar gauge showing distribution of 200 vs 404 responses
- Useful for identifying error rates

#### Panel 4: Individual Request Durations

- Shows each individual request as a point
- Every point is clickable and links to its trace
- Great for investigating specific slow requests

### Visual Exemplar Flow

```
1. Notice high latency in Panel 2 (p95 duration spike)
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

You can also see exemplars directly in Prometheus:

```bash
# Request metrics in OpenMetrics format (required for exemplars)
curl -H "Accept: application/openmetrics-text" http://localhost:15100/metrics \
  | grep '# {trace_id' | head -5
```

Example output:

```
http_server_request_duration_seconds_bucket{...,le="0.005"} 45 # {trace_id="abc123...",span_id="def456..."} 0.002341 1762810234.567
```

The `# {trace_id="...",span_id="..."}` comment is the exemplar - it links this metric bucket to a specific trace!

## Step 6: Correlation Demo - From Error Metric to Trace

### Find 404 Errors in Metrics

1. In Grafana, look at **Panel 3: HTTP Status Codes**
2. You should see both "200" and "404" bars
3. Go to **Panel 4: Individual Request Durations**
4. Click on a red/orange data point (these are errors)
5. Click "View Trace in Jaeger"
6. Jaeger opens showing:
   - `http.status_code = 404`
   - `http.target = /does-not-exist`
   - `http.method = GET`
   - Full timing breakdown of the request

### This demonstrates:

- **Aggregated view** (Panel 3) shows you have 404 errors
- **Exemplar** (Panel 4) lets you jump to a specific error instance
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
│  Multigres   │  Exports metrics (OpenMetrics format)
│  Services    │  with embedded exemplars (trace_id, span_id)
└──────┬───────┘
       │
       │ /metrics (OpenMetrics)
       ↓
┌──────────────┐
│  Prometheus  │  Scrapes metrics every 5s
│              │  Stores exemplars in memory
└──────┬───────┘
       │
       ↓
┌──────────────┐
│   Grafana    │  Queries Prometheus for metrics
│              │  Displays exemplars as clickable links
│              │  Links to Jaeger traces
└──────┬───────┘
       │
       ↓
┌──────────────┐
│   Jaeger     │  Stores distributed traces
│              │  Displays trace details
└──────────────┘
```

## Configuration Details

### Prometheus (`prometheus.yml`)

- **Auto-generated** during `multigres cluster start`
- Located at `multigres_local/observability/prometheus.yml`
- Scrapes all Multigres services every 5s
- **Critical**: Uses `format: ['openmetrics']` to get exemplars
- Enables `--enable-feature=exemplar-storage` flag

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
# Stop observability stack
docker-compose -f docker-compose-observability.yml down

# Stop Multigres cluster
./bin/multigres cluster stop
```

## Troubleshooting

### No Exemplars Appearing in Grafana

**Check 1: Verify OpenMetrics format**

```bash
curl -H "Accept: application/openmetrics-text" http://localhost:15100/metrics | grep -c '# {trace_id'
```

Should return > 0

**Check 2: Check Prometheus is scraping**

```bash
# Open http://localhost:9090/targets
# All targets should be "UP" with green status
```

**Check 3: Verify traces are reaching Jaeger**

```bash
# Open http://localhost:16686
# Search for service "multigateway"
# Should see traces
```

**Check 4: Panel configuration**

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
