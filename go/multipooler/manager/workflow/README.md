# MultiPooler Workflow Package

A generic phase-based orchestration system for declarative workflow execution.

## Overview

This package provides a reusable framework for organizing complex multi-step operations into phases. Components register themselves as executors for specific phases, and the workflow orchestrator ensures correct execution order, failure handling, and observability.

## Key Concepts

### Workflow

A sequence of phases executed in order. Each phase may have multiple executors.

### Phase

A distinct milestone in the workflow (e.g., "Validate", "StopWrites", "Drain"). Phases are executed sequentially.

### Executor

A component that performs work during specific phases. Executors declare:

- Which phases they participate in
- Whether failures are fatal or non-fatal
- The actual work to perform

### State

Data accumulated throughout the workflow, passed to each executor and updated as work progresses.

## Benefits

### Compared to Imperative Code

| Aspect              | Imperative (before)                       | Phase-Based (now)                      |
| ------------------- | ----------------------------------------- | -------------------------------------- |
| **Ordering**        | Implicit (code order)                     | Explicit (phase sequence)              |
| **Coupling**        | Tight (manager calls components directly) | Loose (components register themselves) |
| **Extensibility**   | Hard (modify core logic)                  | Easy (add new executor)                |
| **Testability**     | Integration tests only                    | Unit test executors + integration      |
| **Reordering bugs** | Easy to introduce                         | Hard (phases have fixed order)         |

### Design Goals

1. **Declarative**: Express _what_ needs to happen, not _how_
2. **Component-oriented**: Each component handles its own concerns
3. **Reusable**: Generic workflow can be used for Demote, Promote, etc.
4. **Observable**: Clear phase transitions in logs
5. **Testable**: Test executors independently

## Usage Example: Demote Workflow

### 1. Define Phases

```go
type DemotePhase int

const (
    DemotePhaseValidate DemotePhase = iota
    DemotePhaseStopWrites
    DemotePhaseDrain
    DemotePhaseCapture
    DemotePhaseRestart
    DemotePhaseCleanup
)

func (p DemotePhase) String() string {
    switch p {
    case DemotePhaseValidate:
        return "Validate"
    // ...
    }
}
```

### 2. Define Input, State, and Result

```go
type DemoteInput struct {
    ConsensusTerm int64
    DrainTimeout  time.Duration
    Force         bool
}

type DemoteState struct {
    IsServingReadOnly   bool
    IsReplicaInTopology bool
    FinalLSN            string
    // ... accumulated data
}

type DemoteResult struct {
    WasAlreadyDemoted     bool
    ConsensusTerm         int64
    LsnPosition           string
    ConnectionsTerminated int32
}
```

### 3. Create Executors

```go
type ValidationExecutor struct {
    manager ManagerDependencies
    logger  Logger
}

func (e *ValidationExecutor) Name() string {
    return "ValidationExecutor"
}

func (e *ValidationExecutor) Phases() []DemotePhase {
    return []DemotePhase{DemotePhaseValidate}
}

func (e *ValidationExecutor) Execute(
    ctx context.Context,
    phaseCtx *PhaseContext[DemotePhase, DemoteInput, DemoteState],
) error {
    // Validate preconditions
    if err := e.manager.ValidateAndUpdateTerm(ctx, phaseCtx.Input.ConsensusTerm, phaseCtx.Input.Force); err != nil {
        return err
    }

    // Update state for subsequent executors
    phaseCtx.State.IsServingReadOnly = // ... check current state

    return nil
}

func (e *ValidationExecutor) CanFail(phase DemotePhase) bool {
    return false // Validation failures are always fatal
}
```

### 4. Create and Execute Workflow

```go
func DemoteWithWorkflow(ctx context.Context, input *DemoteInput) (*DemoteResult, error) {
    // Create workflow
    wf := NewDemoteWorkflow(logger)

    // Register executors
    wf.Register(NewValidationExecutor(manager, logger))
    wf.Register(NewTopologyExecutor(manager, topoClient, logger))
    wf.Register(NewReplTrackerExecutor(replTracker, logger))
    wf.Register(NewDrainExecutor(manager, logger))
    wf.Register(NewRestartExecutor(manager, pgctldClient, logger))
    wf.Register(NewCleanupExecutor(manager, logger))

    // Execute
    result, err := wf.Execute(ctx, input, &DemoteState{}, BuildDemoteResult, &ExecuteOptions{
        EarlyExitChecker: CheckDemoteEarlyExit,
    })

    return result, err
}
```

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                     Workflow Orchestrator                    │
│  - Executes phases in order                                  │
│  - Calls executors registered for each phase                 │
│  - Handles failures (fatal vs non-fatal)                     │
│  - Accumulates state                                         │
│  - Provides observability (logs, metrics)                    │
└─────────────────────────────────────────────────────────────┘
                              │
                              │ executes
                              ▼
        ┌─────────────────────────────────────────┐
        │          Phase Sequence                  │
        │                                          │
        │  ┌──────────┐  ┌──────────┐  ┌────────┐│
        │  │Validate  │─►│StopWrites│─►│ Drain  ││
        │  └──────────┘  └──────────┘  └────────┘│
        │       │             │              │    │
        │       ▼             ▼              ▼    │
        │  ┌────────────────────────────────────┐│
        │  │    Registered Executors            ││
        │  │  - ValidationExecutor              ││
        │  │  - TopologyExecutor                ││
        │  │  - ReplTrackerExecutor             ││
        │  │  - DrainExecutor                   ││
        │  │  - RestartExecutor                 ││
        │  │  - CleanupExecutor                 ││
        │  └────────────────────────────────────┘│
        └─────────────────────────────────────────┘
```

## Key Features

### 1. Component Registration

Executors register for the phases they care about:

```go
type TopologyExecutor struct {
    // ... dependencies
}

func (e *TopologyExecutor) Phases() []DemotePhase {
    // This executor participates in two phases
    return []DemotePhase{DemotePhaseStopWrites, DemotePhaseCleanup}
}

func (e *TopologyExecutor) Execute(ctx context.Context, phaseCtx *PhaseContext[...]) error {
    switch phaseCtx.Phase {
    case DemotePhaseStopWrites:
        return e.updateToServingReadOnly(ctx, phaseCtx)
    case DemotePhaseCleanup:
        return e.updateToReplica(ctx, phaseCtx)
    }
    return nil
}
```

### 2. Failure Handling

Executors declare whether failures are fatal:

```go
func (e *CleanupExecutor) CanFail(phase DemotePhase) bool {
    return true // Cleanup failures are non-fatal, workflow continues
}

func (e *ValidationExecutor) CanFail(phase DemotePhase) bool {
    return false // Validation failures are fatal, workflow stops
}
```

### 3. State Accumulation

Executors read and update shared state:

```go
func (e *DrainExecutor) Execute(ctx context.Context, phaseCtx *PhaseContext[...]) error {
    // Do drain work
    connectionsTerminated := terminateConnections()

    // Update state for subsequent executors
    phaseCtx.State.ConnectionsTerminated = connectionsTerminated

    return nil
}
```

### 4. Early Exit

Workflows can exit early based on state:

```go
func CheckDemoteEarlyExit(phase DemotePhase, phaseCtx *PhaseContext[...]) (bool, *DemoteResult, error) {
    if phase == DemotePhaseValidate && phaseCtx.State.WasAlreadyDemoted {
        // Already demoted, skip remaining phases
        return true, &DemoteResult{WasAlreadyDemoted: true, ...}, nil
    }
    return false, nil, nil
}
```

### 5. Multiple Executors Per Phase

Multiple executors can run in the same phase (in registration order):

```go
// Both run during DemotePhaseStopWrites
wf.Register(NewTopologyExecutor(...))    // Updates topology
wf.Register(NewReplTrackerExecutor(...)) // Stops heartbeat
```

## Testing

### Unit Tests for Executors

Test executors independently with mocks:

```go
func TestValidationExecutor_Success(t *testing.T) {
    mockManager := &MockManager{...}
    executor := NewValidationExecutor(mockManager, logger)

    phaseCtx := &PhaseContext[DemotePhase, DemoteInput, DemoteState]{
        Phase: DemotePhaseValidate,
        Input: &DemoteInput{ConsensusTerm: 42},
        State: &DemoteState{},
    }

    err := executor.Execute(context.Background(), phaseCtx)
    assert.NoError(t, err)
    assert.True(t, mockManager.ValidateAndUpdateTermCalled)
}
```

### Integration Tests for Workflows

Test complete workflow with real or test double dependencies:

```go
func TestDemoteWorkflow_EndToEnd(t *testing.T) {
    wf := NewDemoteWorkflow(logger)
    wf.Register(/* ... all executors ... */)

    result, err := wf.Execute(ctx, input, initialState, buildResult, opts)

    assert.NoError(t, err)
    assert.Equal(t, expectedLSN, result.LsnPosition)
}
```

## Extending to Other Workflows

The workflow system is generic. To create a new workflow (e.g., Promote):

1. Define phases: `PromotePhase` enum
2. Define types: `PromoteInput`, `PromoteState`, `PromoteResult`
3. Create executors: Implement `Executor[PromotePhase, PromoteInput, PromoteState]`
4. Wire it up: Create `NewPromoteWorkflow()` factory

Example:

```go
type PromotePhase int

const (
    PromotePhaseValidate PromotePhase = iota
    PromotePhasePromoteDB
    PromotePhaseUpdateTopology
    PromotePhaseConfigureReplication
)

func NewPromoteWorkflow(logger Logger) *Workflow[PromotePhase, PromoteInput, PromoteState, PromoteResult] {
    phases := []PromotePhase{
        PromotePhaseValidate,
        PromotePhasePromoteDB,
        PromotePhaseUpdateTopology,
        PromotePhaseConfigureReplication,
    }
    return NewWorkflow[PromotePhase, PromoteInput, PromoteState, PromoteResult](phases, logger)
}
```

## Files in This Package

- **`workflow.go`**: Generic workflow orchestrator (reusable for any workflow)
- **`demote.go`**: Demote-specific types (phases, input, state, result)
- **`executors.go`**: Demote executors (ValidationExecutor, TopologyExecutor, etc.)
- **`workflow_test.go`**: Tests for generic workflow system
- **`DESIGN.md`**: Detailed design document with alternatives and rationale
- **`README.md`**: This file

## Migration Path

The phase-based workflow is implemented alongside the existing imperative `Demote()` method:

1. **Current**: `Demote()` in `rpc_manager.go` (existing implementation, unchanged)
2. **New**: `DemoteWithWorkflow()` in `rpc_manager_workflow.go` (phase-based implementation)

Benefits:

- Compare behavior side-by-side
- Gradual rollout with feature flag
- Rollback if issues found
- Remove old implementation once validated

## Future Enhancements

### Short Term

- Apply to `Promote()` operation
- Add metrics for phase duration and failure rates
- Persist phase progress to topology (for resume after crash)

### Medium Term

- Combine with Kubernetes-style reconciliation loop
- Add configurable retry logic for transient failures
- Implement Saga pattern for automatic rollback

### Long Term

- Dependency graph within phases (fine-grained ordering)
- State machine formalization with type safety library
- Distributed coordination for multiple manager instances

## References

- [DESIGN.md](./DESIGN.md) - Detailed design document
- [Current Demote implementation](../rpc_manager.go#L677-L799)
- Kubernetes controller pattern: https://kubernetes.io/docs/concepts/extend-kubernetes/operator/
- Vitess VTOrc: https://vitess.io/docs/reference/vtorc/
