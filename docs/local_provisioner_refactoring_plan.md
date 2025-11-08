# Local Provisioner Refactoring: Implementation Plan

## Executive Summary

This document outlines the comprehensive refactoring plan for the `go/provisioner/local` package. The refactoring will transform the current monolithic, sequential provisioning system into a resource-based architecture with parallel execution, explicit dependency management, and orphan detection.

## Goals

1. **Simplicity**: Smaller, focused resource provisioners instead of monolithic code
2. **Extensibility**: Easy to add new resource types
3. **Performance**: Parallel provisioning where dependencies allow
4. **Reliability**: Automatic orphan detection and cleanup

## Current State Analysis

### Existing Implementation
- **~1,983 lines** in `local.go` with 7+ service-specific provision methods
- **Sequential execution**: Services start one at a time
- **Implicit dependencies**: No explicit dependency graph
- **No orphan detection**: Stale state files can accumulate
- **Mixed concerns**: Provisioning, topology setup, state management all interleaved

### Pain Points
1. Hard to extend with new service types
2. Slow startup due to sequential provisioning
3. No reconciliation between desired state (config) and actual state (running processes)
4. Difficult to test and reason about
5. Inconsistent error handling and status display

## New Architecture

### Core Abstractions

#### 1. Resource Interface
```go
type Resource interface {
    // Provision creates or reuses the resource
    Provision(ctx context.Context, pctx ProvisionContext) (*ProvisionResult, error)

    // Deprovision stops and cleans up the resource
    Deprovision(ctx context.Context, pctx ProvisionContext) error

    // Dependencies returns resource IDs that must be provisioned first
    Dependencies() []clustermetadatapb.ID

    // ID returns this resource's unique identifier
    ID() clustermetadatapb.ID

    // Phase returns the display phase for status monitoring
    Phase() ProvisionPhase
}
```

#### 2. ProvisionContext
Provides shared functionality to all resource provisioners:
- **State management**: `ReadState()`, `SaveState()`, `DeleteState()`, `ListStates()`
- **Topology access**: `OpenGlobalTopo()`, `OpenCellTopo(cellName)`
- **Configuration**: `GetConfig()` returns the full `LocalProvisionerConfig`
- **Path helpers**: `LogPath()`, `DataPath()`, `StatePath()`
- **Binary resolution**: `FindBinary(name)` checks PATH then config
- **Etcd info**: `EtcdClientAddress()`, `EtcdPeerAddress()`

#### 3. Resource Provisioner (formerly "Orchestrator")
Coordinates provisioning across all resources:
- **Dependency graph construction**: Builds DAG from resource dependencies
- **Parallel execution**: Provisions independent resources concurrently
- **Error handling**: Configurable (stop immediately vs continue on error)
- **Orphan detection**: Compares state files to requested config
- **Progress tracking**: Reports status to monitor

#### 4. Status Monitor
Displays provisioning progress with consistent ordering:
- **Phase-based display**: Groups resources by logical phases
- **Real-time updates**: Shows progress as it happens
- **Deterministic ordering**: Same order every time

### Resource Types

| Resource Type | Scope | Dependencies | State Tracked |
|--------------|-------|--------------|---------------|
| **GlobalTopologyResource** | Global | None | Etcd process |
| **CellTopologyResource** | Cell | Global topology | Cell existence in topo |
| **MultiadminResource** | Global | Global topology | Multiadmin process |
| **DatabaseResource** | Database | Global topology | Database registration |
| **MultipoolerResource** | Cell | Cell topology, Database | Multipooler + pgctld processes (single resource) |
| **MultigatewayResource** | Cell | Cell topology, Database | Multigateway process |
| **MultiOrchResource** | Cell | Cell topology, Database | Multiorch process |

**Note**: Multipooler and pgctld are provisioned and deprovisioned together as a single resource. During provisioning, the multipooler resource starts both processes and tracks both PIDs. During deprovisioning, it handles the case where either process may be missing.

### Dependency Rules

**Explicit Dependencies:**
- Each resource declares dependencies via `Dependencies()` method
- Returns `[]clustermetadatapb.ID` of resources that must be provisioned first

**Implicit Dependencies (enforced by provisioner engine):**
- Global topology is a dependency for all resources (except itself)
- Cell topology is a dependency for all cell-scoped resources (except cell topology itself)

### Example Dependency Graph

```
GlobalTopology (etcd)
  ├─> Multiadmin
  ├─> Database
  ├─> CellTopology (cell1)
  │     ├─> Multipooler (cell1, db1) [includes pgctld]
  │     ├─> Multigateway (cell1, db1)
  │     └─> Multiorch (cell1, db1)
  └─> CellTopology (cell2)
        ├─> Multipooler (cell2, db1) [includes pgctld]
        ├─> Multigateway (cell2, db1)
        └─> Multiorch (cell2, db1)
```

## File Structure

```
go/provisioner/local/
├── resource.go              [NEW] Resource interface + common types
├── context.go               [NEW] ProvisionContext implementation
├── provisioner_engine.go    [NEW] Dependency graph + parallel execution
├── status.go                [NEW] Phase-based status display
├── resource_etcd.go         [NEW] Etcd/global topology provisioner
├── resource_cell.go         [NEW] Cell topology provisioner
├── resource_multiadmin.go   [NEW] Multiadmin provisioner
├── resource_database.go     [NEW] Database registration provisioner
├── resource_multipooler.go  [NEW] Multipooler + pgctld provisioner (single resource)
├── resource_multigateway.go [NEW] Multigateway provisioner
├── resource_multiorch.go    [NEW] Multiorch provisioner
├── local.go                 [REFACTOR] Simplified main provisioner
├── state.go                 [ENHANCE] Add orphan detection
├── config.go                [MINOR] Minimal changes if needed
├── healthcheck.go           [KEEP] Minor updates for new patterns
├── pgctld.go                [KEEP] Helper functions (initdb, gRPC calls)
└── ports/ports.go           [KEEP] As-is
```

## Implementation Plan

### Phase 1: Core Infrastructure
**Goal**: Create the foundational types and orchestration engine

1. **Create `resource.go`**
   - Define `Resource` interface
   - Define `ProvisionPhase` enum (TopologySetup, GlobalServices, CellServices)
   - Define `ProvisionResult` and related types
   - Create base `ResourceInfo` struct for common fields

2. **Implement `context.go`**
   - Implement `ProvisionContext` interface
   - State file operations (read/write/delete/list)
   - Topology server connection pooling
   - Config access methods
   - Path generation helpers
   - Binary resolution with PATH fallback

3. **Implement `provisioner_engine.go`**
   - Dependency graph builder (topological sort)
   - Parallel execution engine with worker pools
   - Error handling (configurable: stop-immediately vs continue)
   - Orphan detection logic (compare state vs config)
   - Integration with status monitor

4. **Implement `status.go`**
   - Phase-based status display
   - Progress tracking (pending → in-progress → completed/failed)
   - Consistent ordering within phases
   - Real-time updates without jumpy output

### Phase 2: Resource Implementations
**Goal**: Implement each resource type

5. **Implement `resource_etcd.go`**
   - Start etcd process if not already running
   - Validate etcd health via HTTP endpoint
   - Track PID in state file
   - Handle port conflicts

6. **Implement `resource_cell.go`**
   - Connect to global topology server
   - Create cell if it doesn't exist
   - No process to track (stateless)
   - Handle cell already exists gracefully

7. **Implement `resource_multiadmin.go`**
   - Start multiadmin process
   - Pass etcd connection info via CLI flags
   - Track PID in state file
   - Health check via HTTP /live endpoint

8. **Implement `resource_database.go`**
   - Connect to global topology server
   - Register database in topology
   - No process to track (stateless)
   - Handle database already exists gracefully

9. **Implement `resource_multipooler.go`** (includes pgctld)
   - Check state file for existing multipooler + pgctld PIDs
   - Initialize pgctld data directory if needed
   - Start pgctld process (if not running)
   - Start PostgreSQL via pgctld gRPC
   - Start multipooler process (if not running)
   - Track both PIDs in single state file
   - **Complex deprovision logic:**
     - Stop multipooler process if PID exists
     - Stop PostgreSQL + pgctld via `pg_ctl stop`
     - Handle case where either process is missing
     - Only mark as deprovisioned when both are stopped
   - Health checks for both components

10. **Implement `resource_multigateway.go`**
    - Start multigateway process
    - Pass topology and database info via CLI flags
    - Track PID in state file
    - Health check via HTTP /live endpoint

11. **Implement `resource_multiorch.go`**
    - Start multiorch process
    - Pass topology and database info via CLI flags
    - Track PID in state file
    - Health check via HTTP /live endpoint

### Phase 3: Integration
**Goal**: Integrate new architecture with existing code

12. **Enhance `state.go`**
    - Add `ListAllStates()` to find all state files
    - Add `OrphanDetection()` to compare states vs config
    - Keep existing `LocalProvisionedService` struct
    - Extend state struct to track multiple PIDs if needed (for multipooler + pgctld)
    - Add helper for state file path generation

13. **Refactor `local.go`**
    - Keep `Provisioner` interface but simplify implementation
    - `Bootstrap()`: Build resource list, call provisioner engine
    - `ProvisionDatabase()`: Build database resource list, call provisioner engine
    - `DeprovisionDatabase()`: Find resources for database, deprovision
    - `Teardown()`: Find all resources, deprovision
    - Remove all individual `provisionXXX()` methods

14. **Update `healthcheck.go`**
    - Extract health check logic into reusable functions
    - Add health check registry for extensibility
    - Keep existing validation logic

15. **Update `proto/clustermetadata.proto`** (if needed)
    - Add ComponentType values: ETCD, MULTIADMIN, CELL, DATABASE
    - Note: PGCTLD is NOT a separate component type (part of MULTIPOOLER)
    - Regenerate protobuf code

### Phase 4: Testing & Validation
**Goal**: Ensure the refactored code works correctly

16. **Test Bootstrap flow**
    - Verify parallel provisioning
    - Check status display ordering
    - Validate all services start correctly

17. **Test ProvisionDatabase/DeprovisionDatabase**
    - Test multi-cell database provisioning
    - Verify parallel cell provisioning
    - Test deprovision cleanup

18. **Test orphan detection**
    - Start cluster with 2 cells
    - Update config to 1 cell
    - Restart cluster, verify orphan cell services are cleaned up

19. **Test multipooler/pgctld edge cases**
    - Kill multipooler but leave pgctld running, then deprovision
    - Kill pgctld but leave multipooler running, then deprovision
    - Verify both are properly cleaned up

20. **Test error handling**
    - Simulate failures at various points
    - Test both error handling modes
    - Verify partial provisioning cleanup

21. **Test performance**
    - Measure startup time vs old implementation
    - Verify parallelism is working
    - Profile if needed

## Status Display Design

### Phases
1. **Topology Setup**
   - Global topology (etcd)
   - Cell topologies (one per cell)

2. **Global Services**
   - Multiadmin
   - Database registration

3. **Cell Services** (repeated per cell)
   - Multipooler (includes pgctld)
   - Multigateway
   - Multiorch

### Output Format
```
Topology Setup
  ✓ Global topology (etcd-abc12345) [2.1s]
  ✓ Cell zone1 topology [0.3s]
  ✓ Cell zone2 topology [0.3s]

Global Services
  ✓ Multiadmin (multiadmin-xyz78901) [1.5s]
  ✓ Database 'testdb' [0.2s]

Cell Services: zone1
  ✓ Multipooler (multipooler-def34567) [3.2s]
  ✓ Multigateway (multigateway-ghi45678) [1.1s]
  ✓ Multiorch (multiorch-jkl56789) [0.9s]

Cell Services: zone2
  ✓ Multipooler (multipooler-mno67890) [3.1s]
  ✓ Multigateway (multigateway-pqr78901) [1.0s]
  ✓ Multiorch (multiorch-stu89012) [0.9s]

Cluster started successfully in 8.4s
```

## Error Handling

### Configuration Option
Add to `LocalProvisionerConfig`:
```yaml
failFast: true  # Stop immediately on first error (default)
# OR
failFast: false # Continue provisioning other resources on error
```

### Behavior

**Fail Fast (default):**
- First error stops all pending provisions
- Provisioner returns immediately with error
- Already-running resources remain running (no automatic rollback)
- User can manually run Teardown if desired

**Continue on Error:**
- Errors logged but provisioning continues for independent resources
- At the end, return combined error with all failures
- Successfully provisioned resources remain running
- Useful for debugging or partial cluster startup

## Orphan Detection

### Algorithm
1. List all state files in state directory
2. Build set of resource IDs from current config
3. Identify state files not in config (orphans)
4. Deprovision orphaned resources before provisioning new ones
5. Log orphan cleanup actions

### Example Scenario
```
Config previously:
  - cell1: multipooler, multigateway, multiorch
  - cell2: multipooler, multigateway, multiorch

Config now:
  - cell1: multipooler, multigateway, multiorch

Orphan detection finds:
  - multipooler-cell2-xxx (orphan) [will clean up both multipooler + pgctld]
  - multigateway-cell2-xxx (orphan)
  - multiorch-cell2-xxx (orphan)

Action: Deprovision all cell2 resources before starting cluster
```

## State Management

### State File Structure
For most services (unchanged):
```json
{
  "id": "multigateway-abc12345",
  "service": "multigateway",
  "pid": 12345,
  "binary_path": "/usr/local/bin/multigateway",
  "data_dir": "",
  "log_file": "/tmp/multigres/logs/dbs/testdb/multigateway/abc12345.log",
  "ports": {
    "grpc": 15200,
    "http": 15201
  },
  "fqdn": "multigateway-abc12345.localhost",
  "started_at": "2025-11-08T10:30:45Z",
  "metadata": {
    "cell": "zone1",
    "database": "testdb"
  }
}
```

For multipooler (tracks multiple PIDs):
```json
{
  "id": "multipooler-abc12345",
  "service": "multipooler",
  "pid": 12345,                  // multipooler process
  "binary_path": "/usr/local/bin/multipooler",
  "data_dir": "/tmp/multigres/data/pooler_multipooler-abc12345",
  "log_file": "/tmp/multigres/logs/dbs/testdb/multipooler/abc12345.log",
  "ports": {
    "grpc": 15100,
    "http": 15101,
    "pg": 5432
  },
  "fqdn": "multipooler-abc12345.localhost",
  "started_at": "2025-11-08T10:30:45Z",
  "metadata": {
    "cell": "zone1",
    "database": "testdb",
    "pgctld_pid": "12344"         // pgctld process tracked here
  }
}
```

### State Operations
- **ReadState**: Load from disk, validate PID(s) still exist
- **SaveState**: Atomically write to disk (temp file + rename)
- **DeleteState**: Remove file after successful deprovision
- **ListStates**: Find all state files for orphan detection

## Configuration Changes

### Minimal Changes Expected
The existing `LocalProvisionerConfig` structure should work with minimal changes:
- Add `FailFast bool` field (default true)
- Keep existing service configs, cell configs, etc.
- Resource implementations read from config as needed

### Example Config (unchanged)
```yaml
rootWorkingDir: /tmp/multigres
failFast: true
etcd:
  id: etcd-abc12345
  clientPort: 2379
  peerPort: 2380
multiadmin:
  id: multiadmin-xyz78901
  port: 15000
cells:
  - name: zone1
    multigateway:
      id: multigateway-ghi45678
    multipooler:
      id: multipooler-def34567
    multiorch:
      id: multiorch-jkl56789
```

## Changes Outside Local Provisioner

### proto/clustermetadata.proto
**Current:**
```protobuf
enum ComponentType {
    UNKNOWN = 0;
    MULTIPOOLER = 1;
    MULTIGATEWAY = 2;
    MULTIORCH = 3;
}
```

**Proposed Addition** (evaluate during implementation):
```protobuf
enum ComponentType {
    UNKNOWN = 0;
    MULTIPOOLER = 1;      // Includes pgctld (not separate)
    MULTIGATEWAY = 2;
    MULTIORCH = 3;
    ETCD = 4;
    MULTIADMIN = 5;
    CELL = 6;             // For cell topology resources
    DATABASE = 7;         // For database registration resources
}
```

**Note**: PGCTLD is NOT a separate ComponentType - it's always provisioned/deprovisioned as part of MULTIPOOLER.

## Benefits Summary

### Simplicity
- Each resource type in its own ~200-line file
- Clear separation of concerns
- Easy to understand resource lifecycle

### Extensibility
- Add new resource type: create one new file implementing Resource interface
- Declare dependencies explicitly
- No changes to provisioner engine logic

### Performance
- Parallel provisioning where dependencies allow
- Example: All cell services can start simultaneously after cell topology is ready
- Expected 2-3x faster cluster startup for multi-cell clusters

### Reliability
- Automatic orphan cleanup
- Consistent error handling
- Better state management (handles complex cases like multipooler + pgctld)
- Easier to debug and maintain

## Migration Path

This is a complete rewrite of the package internals, but:
- External `Provisioner` interface can remain compatible (or change as needed)
- Config format stays mostly the same
- State file format largely unchanged (minor extension for multipooler)
- Binary requirements unchanged

Therefore, migration should be:
1. Implement all new code
2. Switch main provisioner to use new engine
3. Test thoroughly
4. Ship as a single change (no gradual migration needed)

## Open Questions

These can be resolved during implementation:
1. Should ProvisionContext provide logging methods?
2. What's the best way to pass config to resources (full config vs subset)?
3. Should we add metrics/telemetry hooks to Resource interface?
4. Do we need transaction/rollback support for failed provisions?
5. Should status monitor support interactive/non-interactive modes?
6. How should we track pgctld's data directory path in state (for `pg_ctl stop`)?

## Success Criteria

The refactoring is successful when:
1. All existing functionality works (Bootstrap, ProvisionDatabase, etc.)
2. Cluster startup is measurably faster (2-3x for multi-cell)
3. Orphan detection works correctly
4. Multipooler + pgctld deprovisioning handles edge cases (either process missing)
5. Adding a new resource type takes <1 hour
6. Code is easier to understand and maintain
7. Status display is clear and consistent
