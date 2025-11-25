# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Multigres is a Vitess-style distributed database proxy for PostgreSQL. It provides horizontal scaling, connection pooling, and cluster orchestration for PostgreSQL deployments.

## Getting Started

### Build Commands

```bash
make tools      # Install build dependencies (protoc, goyacc, etc.)
make build      # Build Go binaries to bin/
make proto      # Generate protobuf files
make parser     # Generate PostgreSQL parser from grammar
make build-all  # Proto + parser + binaries
make test       # Run all tests
make test-short # Run short tests only (skips PostgreSQL integration tests)
make test-race  # Run tests with race detector
make clean      # Remove build artifacts
```

### Running Tests

```bash
go test -v -run TestName ./go/path/to/package/...
go test -v ./go/multipooler/...  # Run all tests in a package
```

### Local Development Cluster

```bash
./bin/multigres cluster init    # Initialize local cluster
./bin/multigres cluster start   # Start all services
./bin/multigres cluster stop    # Stop all services
./bin/multigres cluster status  # Check service status
```

Configuration: `multigres_local/multigres.yaml`

### Development Workflow

- Run `make proto` after modifying `.proto` files
- Run `make build` before running integration tests

## Architecture

### Core Services (go/cmd/)

- **multigateway** - PostgreSQL proxy accepting client connections, routes queries
- **multipooler** - Connection pooling service, communicates with pgctld
- **pgctld** - PostgreSQL interface daemon, connects directly to PostgreSQL instances
- **multiorch** - Cluster orchestration for consensus and failover
- **multiadmin** - Administrative service for cluster management
- **multigres** - CLI tool for cluster management

### Key Packages

- **go/pgprotocol/** - PostgreSQL wire protocol implementation (server/client, buffer pooling)
- **go/parser/** - PostgreSQL SQL parser (partial AST implementation, sufficient for query analysis)
- **go/clustermetadata/** - Cluster topology and metadata management
- **go/servenv/** - Service environment (config, logging, HTTP/gRPC setup)
- **go/viperutil/** - Configuration utilities using Viper
- **go/pb/** - Generated protobuf code

### Data Flow

1. Client → **multigateway** (accepts PostgreSQL connections)
2. **multigateway** → **multipooler** (query routing and pooling)
3. **multipooler** → **pgctld** (database interface)
4. **pgctld** → PostgreSQL (actual database)
5. **multiorch** handles failover and consensus across cells

### Topology

The system uses etcd for service discovery and topology storage. The topology is organized by cells (zones), with each cell having its own set of services.

### Directory Structure and Dependencies

```
./go/cmd/...      # Commands - can depend on anything
./go/services/... # Service code - cannot depend on cmd/ or other services
./go/common/...   # Shared code - cannot depend on cmd/ or services/
./go/tools/...    # Generic utilities - cannot depend on any repo code outside tools/
```

- **go/tools/**: Generic helpers (timers, retry, etc.) that aren't multigres-specific
- **go/common/**: Shared multigres code (error codes, gRPC clients, protocol code)

### Generated Files

Files with `// Code generated` comments should not be edited directly. Regenerate with `make proto` (protobufs) or `make parser` (SQL parser/AST). When debugging, trace to source files instead of analyzing generated code.

## Engineering Principles

This is mission-critical infrastructure. Prioritize reliability, security, and maintainability.

### Error Handling

- Use `mterrors.New()`, `mterrors.Errorf()`, `mterrors.Wrap()` instead of `fmt.Errorf()` to preserve stack traces
- Errors carry canonical codes (`mtrpcpb.Code_*`) - check with `mterrors.Code(err)`
- Always check error returns; never discard with `_`

### Performance

- The query path (multigateway → multipooler → pgctld) is latency-sensitive
- Use `sync.Pool` for frequently allocated objects in hot paths (see `go/pgprotocol/server/`)
- Profile before optimizing; measure after

### Security

- Validate all external input at system boundaries
- Never log credentials, tokens, or sensitive query data
- **Supply chain security**:
  - Pin GitHub Actions to exact commit SHAs, not version tags
  - Verify SHA256 checksums for downloaded tools (see `tools/tool_checksums.sh`)
  - Use minimal permissions in CI workflows

### Service Lifecycle

- Register initialization with `OnInit()`, startup with `OnRun()`
- Graceful shutdown: `OnTerm()` (async) → `OnTermSync()` (with timeout) → `OnClose()`
- Lameduck period allows in-flight requests to drain

### Process Termination

**Always attempt graceful shutdown before killing processes:**

1. Send `SIGTERM` (or `os.Interrupt`) first
2. Wait with a timeout for graceful exit
3. Only send `SIGKILL` if the process doesn't respond

Ungraceful termination causes real problems:

- PostgreSQL may leak IPC shared memory slots
- Services may fail to upload OpenTelemetry metrics
- Temporary files and locks may not be cleaned up

**Avoid `os.Exit()`** - it bypasses deferred cleanup and makes code harder to test. Prefer:

- Returning errors from `main()` and letting the Cobra command handle exit codes
- Using the service lifecycle hooks for shutdown logic
- Reserving `os.Exit()` for truly unrecoverable initialization failures

## Code Patterns

### Formatting and Linting

- Go 1.25+
- Formatting: gofumpt + goimports (local prefix: `github.com/multigres`)
- Linting: golangci-lint with custom ruleguard rules in `go/tools/ruleguard/rules.go`
- Use `math/rand/v2` not `math/rand`
- Protobufs follow [Google Cloud API Design Guide](https://cloud.google.com/apis/design/)

### Context Usage

Avoid `context.Background()`. Most contexts should inherit from:

- The top-level Cobra command context (in services)
- `t.Context()` (in tests, except `Cleanup()` where context is cancelled)

Propagate existing context to preserve cancellation and telemetry (tracing spans). When unsure, prefer `context.TODO()` over `context.Background()`.

### Logging

Use `*Context()` log methods (e.g., `InfoContext()` over `Info()`) to propagate telemetry data like trace IDs.

### SQL Queries

Define SQL queries in separate helper files rather than inline. This keeps queries visible and modifiable in one place without reading through service logic.

### Retry Logic

Use `go/tools/retry.Retry` for exponential backoff instead of custom solutions.

### Telemetry

Follow OpenTelemetry naming conventions for metrics, attributes, and span names. Separate metric definitions from instrumented code. Telemetry data is a form of API—aim for clean, useful, stable output.

## Testing

### Coverage Requirements

High test coverage required across:

- **Query path**: Protocol handling, connection pooling, routing
- **Management plane**: Multipooler manager operations, state transitions
- **Orchestration**: Failover, promotion/demotion, consensus

Fast, reliable failover is essential—test failure scenarios thoroughly. Test error paths, not just happy paths.

### Test Patterns

- Use `bufconn.Listener` for fast gRPC unit tests, real TCP for integration tests
- Mock services: embed `pb.UnimplementedXxxServer`, track calls for assertions
- Prefer `require.Eventually()` over `time.Sleep()` to minimize unnecessary waiting

### End-to-End Tests

- Located in `go/test/endtoend/`
- Require PostgreSQL binaries: `initdb`, `postgres`, `pg_ctl`, `pg_isready`
- Use `-short` flag to skip tests requiring real PostgreSQL

### Subprocess Management

End-to-end tests spawn subprocesses (postgres, etcd, services) that must be cleaned up reliably:

- **`run_in_test.sh`**: Wraps long-running processes; monitors parent PID and terminates child if parent dies
- **`run_command_if_parent_dies.sh`**: Runs cleanup commands (like `pg_ctl stop`) when test process dies
- **`terminateProcess()`**: Helper in `bootstrap_test.go` - SIGTERM first, wait, then SIGKILL only if needed

When writing tests that spawn processes:

- Always register cleanup in `t.Cleanup()`
- Use graceful termination (see Process Termination section)
- Set `MULTIGRES_TESTDATA_DIR` and `MULTIGRES_TEST_PARENT_PID` for orphan detection
