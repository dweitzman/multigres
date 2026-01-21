# Session State: SO_REUSEPORT Implementation for Endtoend Tests

**Date**: 2026-01-19
**Status**: Planning complete, ready to start implementation
**Plan File**: `/Users/weitzman/.claude/plans/streamed-noodling-ritchie.md`

## Summary

Implementing SO_REUSEPORT support to eliminate port conflicts in parallel endtoend tests. The TOCTOU race between `GetFreePort()` and subprocess binding will be eliminated by keeping test listeners open with SO_REUSEPORT until subprocesses successfully bind.

## Key Decisions Made

1. **Scope**:
   - ✅ multigres Go services (multigateway, multipooler, pgctld gRPC, multiorch, multiadmin)
   - ✅ etcd (has `--socket-reuse-port` flag)
   - ❌ PostgreSQL (no native SO_REUSEPORT support, though it does use TCP for replication)

2. **Environment Variable**: `MULTIGRES_TEST_REUSEPORT=1` (follows `MULTIGRES_TEST_PARENT_PID` pattern)

3. **Platform Support**: Unix-like systems (Linux, macOS, BSD) via build tags, Windows fallback to standard `net.Listen`

4. **etcd Flags**: Only `--socket-reuse-port` (not `--socket-reuse-address`)

## Implementation Checklist

### Core Infrastructure (Step 1-2)

- [ ] Create `go/tools/netutil/reuseport_unix.go`
  - `IsTestReusePortEnabled()` - checks env var
  - `ListenWithReusePort()` - creates listener with SO_REUSEPORT
- [ ] Create `go/tools/netutil/reuseport_windows.go`
  - Windows fallback (no SO_REUSEPORT)
- [ ] Create `go/tools/netutil/reuseport_test.go`
  - Unit tests for env var detection
  - Unit tests for listener creation
  - Platform-specific behavior tests
- [ ] Update `go/test/utils/portallocator.go`
  - Add `listenerRegistry sync.Map` for tracking open listeners
  - Modify `GetFreePort()` to keep listener open if SO_REUSEPORT enabled
  - Add `CloseListenerForPort(port int)` to close registered listeners

### Service Binding Updates (Step 3)

- [ ] Update `go/common/servenv/run.go` (line 48)
  - HTTP server: conditional `netutil.ListenWithReusePort()`
- [ ] Update `go/common/servenv/grpc_server.go` (line 411)
  - gRPC server: conditional `netutil.ListenWithReusePort()`
- [ ] Update `go/common/pgprotocol/server/listener.go` (line 80)
  - PostgreSQL protocol: conditional `netutil.ListenWithReusePort()`

### Test Infrastructure (Step 4-5)

- [ ] Update `go/test/endtoend/shardsetup/testmain.go` (after line 143)
  - Set `os.Setenv("MULTIGRES_TEST_REUSEPORT", "1")`
  - Add cleanup `os.Unsetenv("MULTIGRES_TEST_REUSEPORT")` before line 166
- [ ] Update `go/test/endtoend/shardsetup/process.go` (line 310)
  - Call `utils.CloseListenerForPort(p.GrpcPort)` after successful connection test
  - Repeat for other ports (HTTP, PG) wherever we verify subprocess bound successfully

### etcd Support (Step 6)

- [ ] Update `go/provisioner/local/local.go` (lines 205-215)
  - Add `--socket-reuse-port` to etcd args when `netutil.IsTestReusePortEnabled()`

### Testing & Verification

- [ ] Run `go test -v ./go/tools/netutil/...` (unit tests)
- [ ] Run `go test -v ./go/test/endtoend/shardsetup/...` with `MULTIGRES_TEST_REUSEPORT=1`
- [ ] Run `go test -parallel=10 ./go/test/endtoend/...` to test parallel execution
- [ ] Verify backward compatibility (tests pass without env var)
- [ ] Check for "address already in use" errors → should be zero

## Critical Files

| File                                      | Type   | Line(s)  | Purpose                     |
| ----------------------------------------- | ------ | -------- | --------------------------- |
| `go/tools/netutil/reuseport_unix.go`      | NEW    | -        | SO_REUSEPORT helpers (Unix) |
| `go/tools/netutil/reuseport_windows.go`   | NEW    | -        | Windows fallback            |
| `go/tools/netutil/reuseport_test.go`      | NEW    | -        | Unit tests                  |
| `go/test/utils/portallocator.go`          | MODIFY | -        | Listener registry           |
| `go/common/servenv/run.go`                | MODIFY | 48       | HTTP binding                |
| `go/common/servenv/grpc_server.go`        | MODIFY | 411      | gRPC binding                |
| `go/common/pgprotocol/server/listener.go` | MODIFY | 80       | PG protocol binding         |
| `go/test/endtoend/shardsetup/testmain.go` | MODIFY | 143, 166 | Env var setup               |
| `go/test/endtoend/shardsetup/process.go`  | MODIFY | 310      | Close listeners             |
| `go/provisioner/local/local.go`           | MODIFY | 205-215  | etcd flag                   |

## Key Implementation Details

### Port Allocator Pattern

```go
// Global listener registry for SO_REUSEPORT mode
var listenerRegistry sync.Map

func GetFreePort(t *testing.T) int {
    if netutil.IsTestReusePortEnabled() {
        lis, _ := netutil.ListenWithReusePort("tcp", "localhost:0")
        port := lis.Addr().(*net.TCPAddr).Port
        listenerRegistry.Store(port, lis)
        t.Cleanup(func() { CloseListenerForPort(port) })
        return port
    }
    // Existing logic for non-SO_REUSEPORT mode
    ...
}

func CloseListenerForPort(port int) {
    if val, ok := listenerRegistry.LoadAndDelete(port); ok {
        val.(net.Listener).Close()
    }
}
```

### Service Binding Pattern

```go
var listener net.Listener
var err error
if netutil.IsTestReusePortEnabled() {
    listener, err = netutil.ListenWithReusePort("tcp", address)
} else {
    listener, err = net.Listen("tcp", address)
}
```

### Listener Lifecycle

1. Test: `GetFreePort()` → creates listener with SO_REUSEPORT, keeps it open
2. Test: passes port to subprocess
3. Subprocess: binds to port with SO_REUSEPORT (succeeds because both have flag)
4. Test: waits for subprocess ready (TCP connection test at line 306-310)
5. Test: calls `CloseListenerForPort()` to close test's listener
6. Subprocess: now sole owner of port

## Research Notes

### PostgreSQL Connections

- **Tests use Unix sockets**: Connection string `host=socketDir port=5432` uses Unix socket in directory
- **PostgreSQL binds TCP**: `ListenAddresses = "localhost"` in config (line 72 of postgresconfig_gen.go)
- **TCP needed for replication**: `primary_conninfo` uses TCP for streaming replication between primary/replicas
- **No SO_REUSEPORT support**: PostgreSQL server lacks native support (PgBouncer has it)

### etcd Support

- **Confirmed via `etcd --help`**: `--socket-reuse-port` flag exists
- **Documentation**: https://etcd.io/docs/v3.6/op-guide/configuration/
- **Bug history**: Fixed in v3.5.10 (previously had YAML config bugs)

## Next Steps to Resume

1. Start with Step 1: Create `go/tools/netutil/reuseport_unix.go`
2. Continue through checklist in order
3. Run unit tests after completing each step
4. Run full endtoend test suite at the end

## Success Criteria

- ✅ Zero "address already in use" errors in CI
- ✅ Tests pass with and without `MULTIGRES_TEST_REUSEPORT`
- ✅ Platform compatibility (Linux, macOS, Windows)
- ✅ No performance regression
- ✅ <300 lines of new code total

## References

- Plan file: `/Users/weitzman/.claude/plans/streamed-noodling-ritchie.md`
- CLAUDE.md patterns: Test environment variables follow `MULTIGRES_TEST_*` pattern
- Architecture: Services use etcd for discovery, topology stored in cells
- Port allocator: `/Users/weitzman/src/multigres-2/go/test/utils/portallocator.go`
