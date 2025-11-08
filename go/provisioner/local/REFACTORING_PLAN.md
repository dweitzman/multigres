# Local Provisioner Refactoring: Resource-Based Architecture

## Overview

This document describes the refactoring of the local provisioner from an imperative, sequential codebase to a declarative, resource-based system that separates lifecycle management from status rendering and enables maximum parallelism.

## Goals

1. **Separation of Concerns**: Decouple resource lifecycle (create/discover/delete) from presentation (status output)
2. **Testability**: Each resource can be tested independently without worrying about fmt.Printf
3. **Parallelism**: Natural support for concurrent provisioning with explicit dependencies
4. **Reusability**: Resource implementations can be composed and reused
5. **Observability**: Centralized status tracking makes it easier to add progress bars, structured logging, or UIs
6. **Declarative**: The dependency graph is explicit rather than implicit in call order

## Architecture Changes

### 1. Core Interfaces

**File**: `go/provisioner/local/resource.go`

Core abstractions:
- **Resource interface**: Defines lifecycle methods (Discover, Provision, Deprovision), dependencies, and hierarchy
- **Status types**: Resource states (NotFound, Discovered, Provisioning, Provisioned, Failed, etc.)
- **ResourceNode**: Runtime wrapper with execution tracking and status
- **Orchestrator**: Manages parallel execution with dependency resolution

```go
type Resource interface {
    ID() *clustermetadata.ID
    DisplayName() string
    Discover(ctx context.Context) (Status, error)
    Provision(ctx context.Context) (Status, error)
    Deprovision(ctx context.Context, status Status) error
    Dependencies() []*clustermetadata.ID
    Children() []Resource
}

type Status struct {
    State     ResourceState
    Metadata  map[string]any
    Message   string
    Error     error
    Timestamp time.Time
}

type ResourceState int
const (
    StateUnknown ResourceState = iota
    StateNotFound
    StateDiscovered
    StateProvisioning
    StateProvisioned
    StateFailed
    StateDeprovisioning
    StateDeprovisioned
)
```

### 2. Resource ID System

**Proto changes**: `proto/clustermetadata.proto`

Extend the `ComponentType` enum inside the `ID` message to include:
- `GLOBAL_TOPO` - Global topology service (etcd)
- `CELL_TOPO` - Cell topology registration
- `MULTIADMIN` - Global multiadmin service
- `PGCTLD` - PostgreSQL controller
- `DATABASE` - Database-level resources
- `CLUSTER` - Cluster-level resources

**ID helpers**: `go/provisioner/local/resource_id.go`
- Functions for creating proto IDs from components
- String representation for logging/display
- Comparison and equality functions

### 3. Resource Hierarchy

The provisioning hierarchy matches the current system:

```
Cluster (root)
├── GlobalTopoResource (etcd)
├── MultiadminResource (depends on GlobalTopo)
└── DatabaseResource (per database)
    ├── CellTopoResource (topology registration)
    └── CellResource (per cell)
        ├── MultigatewayResource
        ├── PgctldResource
        ├── MultipoolerResource (depends on Pgctld)
        └── MultiorchResource
```

### 4. Resource Implementations

All resource implementations are in `go/provisioner/local/resource_*.go`:

- `resource_cluster.go`: ClusterResource (root)
- `resource_global_topo.go`: GlobalTopoResource (etcd provisioning)
- `resource_multiadmin.go`: MultiadminResource
- `resource_database.go`: DatabaseResource
- `resource_cell_topo.go`: CellTopoResource (topology registration)
- `resource_cell.go`: CellResource (groups cell services)
- `resource_multigateway.go`: MultigatewayResource
- `resource_pgctld.go`: PgctldResource
- `resource_multipooler.go`: MultipoolerResource
- `resource_multiorch.go`: MultiorchResource

Each resource:
- Implements the Resource interface
- Encapsulates discovery logic (checking if resource exists)
- Encapsulates provisioning logic (creating the resource)
- Encapsulates deprovisioning logic (removing the resource)
- Declares explicit dependencies via proto IDs
- Optionally implements StatusRenderer for custom output

### 5. Orchestration & Execution

**File**: `go/provisioner/local/resource_orchestrator.go`

The orchestrator:
- Builds a resource graph from the root resource
- Validates the graph (no cycles, all dependencies exist)
- Executes resources in parallel via goroutines
- Each resource waits for its dependencies before executing
- Implements error handling strategies
- Tracks execution state for progress reporting

**Error Handling Strategies**:
```go
type ErrorStrategy int
const (
    ErrorStrategyFailFast ErrorStrategy = iota        // Stop all on first error (default)
    ErrorStrategyContinueIndependent                  // Stop dependents, continue independent
    ErrorStrategyBestEffort                           // Continue all non-dependent resources
)
```

### 6. Status & Progress Rendering

**File**: `go/provisioner/local/resource_status.go`

Features:
- **Semi-live progress**: Print resource status as each completes
- **Pre-order traversal**: Print in logical hierarchy order
- **StatusRenderer interface**: Optional custom rendering per resource
- **Progress printer goroutine**: Watches completion events and prints incrementally

The rendering system is designed to be future-ready for in-place terminal updates (rich UIs) without requiring changes to resource implementations.

### 7. Deprovisioning

**File**: `go/provisioner/local/resource_deprovision.go`

Deprovisioning:
- Traverses hierarchy in reverse order (children before parents)
- Uses post-order traversal to ensure proper cleanup sequencing
- Each resource's Deprovision method receives its current status
- Can optionally kill processes, clean up state files, etc.

## Implementation Strategy

### Phase 1: Core Infrastructure
1. Update proto: Add new ComponentType values
2. Regenerate proto code
3. Implement core interfaces (resource.go)
4. Implement ID helpers (resource_id.go)
5. Implement orchestrator (resource_orchestrator.go)
6. Implement status/rendering (resource_status.go)
7. Implement deprovisioning (resource_deprovision.go)

### Phase 2: Global Resources
1. Implement ClusterResource (resource_cluster.go)
2. Implement GlobalTopoResource (resource_global_topo.go)
3. Implement MultiadminResource (resource_multiadmin.go)
4. Update Bootstrap() in local.go to use resource system

### Phase 3: Database/Cell Resources
1. Implement DatabaseResource (resource_database.go)
2. Implement CellTopoResource (resource_cell_topo.go)
3. Implement CellResource (resource_cell.go)
4. Implement service resources:
   - MultigatewayResource (resource_multigateway.go)
   - PgctldResource (resource_pgctld.go)
   - MultipoolerResource (resource_multipooler.go)
   - MultiorchResource (resource_multiorch.go)
5. Update ProvisionDatabase() in local.go to use resource system

### Phase 4: Cleanup
1. Test the new system end-to-end
2. Remove old imperative provisioning methods
3. Clean up unused code
4. Update documentation

## Integration with Existing Code

### Reused Components
- **State management** (`state.go`): Keep existing JSON-based state persistence
- **Health checks** (`healthcheck.go`): Reuse existing readiness checks
- **Configuration** (`config.go`): Keep existing config types
- **pgctld helpers** (`pgctld.go`): Reuse existing pgctld initialization logic
- **Binary finding**: Reuse existing path resolution
- **Port allocation**: Reuse existing port management

### Updated Components
- **local.go**:
  - `Bootstrap()`: Build and execute ClusterResource with global services
  - `ProvisionDatabase()`: Build and execute DatabaseResource with cell services
  - `Teardown()`: Use reverse-order deprovisioning

## Testing Strategy

### Unit Tests
- Test each resource type independently with mocked dependencies
- Test orchestrator with synthetic resource graphs
- Test all error handling strategies
- Test status rendering with different resource states

### Integration Tests
- Test full Bootstrap() flow with new system
- Test full ProvisionDatabase() flow with new system
- Test deprovisioning in correct reverse order
- Compare behavior with old system (if kept temporarily)

### Test Coverage Goals
- Core interfaces: 100%
- Orchestrator: 90%+
- Individual resources: 80%+

## File Structure

```
go/provisioner/local/
├── local.go                    # Main provisioner (updated)
├── config.go                   # Existing config (unchanged)
├── state.go                    # Existing state management (unchanged)
├── healthcheck.go              # Existing healthchecks (unchanged)
├── pgctld.go                   # Existing pgctld helpers (reused)
├── REFACTORING_PLAN.md         # This document
├── resource.go                 # NEW: Core interfaces and types
├── resource_id.go              # NEW: ID helpers
├── resource_orchestrator.go    # NEW: Parallel execution engine
├── resource_status.go          # NEW: Status types and rendering
├── resource_deprovision.go     # NEW: Reverse-order deprovisioning
├── resource_cluster.go         # NEW: ClusterResource
├── resource_global_topo.go     # NEW: GlobalTopoResource
├── resource_multiadmin.go      # NEW: MultiadminResource
├── resource_database.go        # NEW: DatabaseResource
├── resource_cell_topo.go       # NEW: CellTopoResource
├── resource_cell.go            # NEW: CellResource
├── resource_multigateway.go    # NEW: MultigatewayResource
├── resource_pgctld.go          # NEW: PgctldResource
├── resource_multipooler.go     # NEW: MultipoolerResource
└── resource_multiorch.go       # NEW: MultiorchResource
```

## Non-Goals (Out of Scope)

- Persisting the resource graph itself (only individual resource state persists)
- Backwards compatibility with old provisioning approach
- Changes to state file format (keep existing JSON state)
- Changes to CLI interface
- Support for distributed provisioning (remains local-only)

## Benefits

1. **Maintainability**: Clear separation between "what" (resource definitions) and "how" (orchestration)
2. **Testability**: Each resource can be unit tested in isolation
3. **Performance**: Maximum parallelism with explicit dependency management
4. **Extensibility**: Easy to add new resource types
5. **Observability**: Centralized status tracking enables rich progress reporting
6. **Code Quality**: Removes ~1000+ lines of repetitive imperative code

## Migration Path

Since backwards compatibility is not a concern, we can:
1. Implement the new system alongside the old
2. Switch Bootstrap() and ProvisionDatabase() to use new system
3. Delete old imperative provisioning code once validated
4. No need to support both systems long-term
