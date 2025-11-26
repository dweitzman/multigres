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
./bin/multigres cluster init --config-path multigres_local   # Initialize local cluster
./bin/multigres cluster start --config-path multigres_local  # Start all services
./bin/multigres cluster stop --config-path multigres_local   # Stop all services
./bin/multigres cluster status --config-path multigres_local # Check service status
```

Configuration: `multigres_local/multigres.yaml`

### Development Workflow

- Run `make proto` after modifying `.proto` files
- Run `make build` before running integration tests

## Architecture

Client connections flow through multigateway → multipooler → pgctld → PostgreSQL. The multiorch service handles failover and consensus. See the architecture reference doc for details on services, packages, and data flow.

## Engineering Principles

This is mission-critical infrastructure. Prioritize reliability, security, and maintainability.

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

### SQL Queries

Define SQL queries in separate helper files rather than inline. This keeps queries visible and modifiable in one place without reading through service logic.

### Retry Logic

Use `go/tools/retry.Retry` for exponential backoff instead of custom solutions.

## Reference Documentation

@.claude/docs/architecture.md
@.claude/docs/errors.md
@.claude/docs/telemetry.md
@.claude/docs/testing.md

## Personal Preferences

@~/.claude/multigres.md
