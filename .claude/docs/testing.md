# Testing

## Coverage Requirements

High test coverage required across:

- **Query path**: Protocol handling, connection pooling, routing
- **Management plane**: Multipooler manager operations, state transitions
- **Orchestration**: Failover, promotion/demotion, consensus

Fast, reliable failover is essential—test failure scenarios thoroughly. Test error paths, not just happy paths.

## Test Patterns

- Use `bufconn.Listener` for fast gRPC unit tests, real TCP for integration tests
- Mock services: embed `pb.UnimplementedXxxServer`, track calls for assertions
- Prefer `require.Eventually()` over `time.Sleep()` to minimize unnecessary waiting
- `t.Context()` over `context.Background()`, except `Cleanup()` where context is already cancelled

## End-to-End Tests

- Located in `go/test/endtoend/`
- Require PostgreSQL binaries: `initdb`, `postgres`, `pg_ctl`, `pg_isready`
- Use `-short` flag to skip tests requiring real PostgreSQL

## Subprocess Management

End-to-end tests spawn subprocesses (postgres, etcd, services) that must be cleaned up reliably:

- **`run_in_test.sh`**: Wraps long-running processes; monitors parent PID and terminates child if parent dies
- **`run_command_if_parent_dies.sh`**: Runs cleanup commands (like `pg_ctl stop`) when test process dies
- **`terminateProcess()`**: Helper in `bootstrap_test.go` - SIGTERM first, wait, then SIGKILL only if needed

When writing tests that spawn processes:

- Always register cleanup in `t.Cleanup()`
- Use graceful termination (see Process Termination section in main CLAUDE.md)
- Set `MULTIGRES_TESTDATA_DIR` and `MULTIGRES_TEST_PARENT_PID` for orphan detection
