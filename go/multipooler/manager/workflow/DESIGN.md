# Phase-Based Demote() Workflow Design

## Overview

This document describes the prototype implementation of a phase-based orchestration system for MultiPooler's Demote() operation. The goal is to make state transitions more declarative, reduce the risk of reordering bugs, and improve component decoupling.

## Motivation

The current Demote() implementation in `rpc_manager.go` has several challenges:

1. **Imperative sequencing**: 10+ steps executed in a specific order with implicit dependencies
2. **Tight coupling**: Manager directly calls methods on multiple components (replTracker, topoClient, pgctldClient)
3. **Reordering risks**: Easy to accidentally move code and break ordering constraints
4. **Hard to extend**: Adding new behavior requires modifying the core Demote() logic
5. **Unclear failure semantics**: Some failures are fatal, others are logged but ignored

## Design Principles

### 1. Phase-Based Organization

The workflow is divided into distinct phases:

```
Validate → StopWrites → Drain → Capture → Restart → Cleanup → Done
```

Each phase represents a coarse-grained milestone in the demotion process.

### 2. Component Registration

Components register themselves as executors for the phases they care about:

```go
workflow.Register(&TopologyExecutor{topoClient})
workflow.Register(&ReplTrackerExecutor{replTracker})
workflow.Register(&DrainExecutor{db})
// etc.
```

### 3. Declarative Failure Handling

Executors declare whether they can fail non-fatally:

```go
func (e *CheckpointExecutor) CanFail(phase DemotePhase) bool {
    return true // Checkpoint failure doesn't block demotion
}
```

### 4. State Accumulation

A `PhaseContext` carries state through the workflow:

```go
type PhaseContext struct {
    Phase     DemotePhase
    Input     *DemoteInput
    State     *DemoteState // Accumulated from previous phases
    Errors    []error      // Non-fatal errors
}
```

## Architecture

### Core Types

#### DemotePhase

```go
type DemotePhase int

const (
    DemotePhaseInit DemotePhase = iota
    DemotePhaseValidate
    DemotePhaseStopWrites
    DemotePhaseDrain
    DemotePhaseCapture
    DemotePhaseRestart
    DemotePhaseCleanup
    DemotePhaseDone
)
```

#### PhaseExecutor Interface

```go
type PhaseExecutor interface {
    Name() string
    Phases() []DemotePhase
    Execute(ctx context.Context, phaseCtx *PhaseContext) error
    CanFail(phase DemotePhase) bool
}
```

#### DemoteWorkflow

```go
type DemoteWorkflow struct {
    executors []PhaseExecutor
    phases    []DemotePhase
}

func (dw *DemoteWorkflow) Execute(ctx context.Context, input *DemoteInput) (*DemoteResult, error)
```

### Phase Mapping

| Phase          | Purpose                                   | Executors                             | Can Fail?                  |
| -------------- | ----------------------------------------- | ------------------------------------- | -------------------------- |
| **Validate**   | Check preconditions                       | ValidationExecutor                    | No                         |
| **StopWrites** | Transition to read-only                   | TopologyExecutor, ReplTrackerExecutor | No                         |
| **Drain**      | Wait for writes to complete               | DrainExecutor (checkpoint + monitor)  | Checkpoint: Yes, Drain: No |
| **Capture**    | Record final LSN                          | RestartExecutor                       | No                         |
| **Restart**    | Restart PostgreSQL as standby             | RestartExecutor                       | No                         |
| **Cleanup**    | Reset replication config, update topology | CleanupExecutor, TopologyExecutor     | Yes                        |

### Executors

#### ValidationExecutor

- **Phases**: Validate
- **Responsibilities**:
  - Validate and update consensus term
  - Connect to database
  - Check PRIMARY guardrails
  - Check current demotion state (idempotency)
- **Failure**: Fatal (blocks entire workflow)

#### TopologyExecutor

- **Phases**: StopWrites, Cleanup
- **Responsibilities**:
  - StopWrites: Update `ServingStatus` to `SERVING_RDONLY`
  - Cleanup: Update `Type` to `REPLICA`
- **Failure**:
  - StopWrites: Fatal
  - Cleanup: Non-fatal (log warning)

#### ReplTrackerExecutor

- **Phases**: StopWrites
- **Responsibilities**:
  - Call `replTracker.MakeNonPrimary()` to stop heartbeat writer
- **Failure**: Never fails (always succeeds)

#### DrainExecutor

- **Phases**: Drain
- **Responsibilities**:
  - Run `CHECKPOINT` in background goroutine
  - Monitor for active write connections (poll every 100ms)
  - Early exit if 2 consecutive polls show no writes
  - Terminate remaining write connections after drain timeout
- **Failure**:
  - Checkpoint: Non-fatal
  - Connection monitoring: Fatal
- **Parallelism**: Checkpoint and monitoring run concurrently

#### RestartExecutor

- **Phases**: Capture, Restart
- **Responsibilities**:
  - Capture: Get final LSN from primary
  - Restart: Call pgctld to restart with `AsStandby=true`, reconnect, verify `pg_is_in_recovery()`
- **Failure**: Fatal (leaves system in inconsistent state)

#### CleanupExecutor

- **Phases**: Cleanup
- **Responsibilities**:
  - Clear `synchronous_standby_names`
  - Clear `synchronous_commit` config
- **Failure**: Non-fatal (log warning)

## Implementation Details

### Workflow Execution

```go
func (dw *DemoteWorkflow) Execute(ctx context.Context, input *DemoteInput) (*DemoteResult, error) {
    phaseCtx := &PhaseContext{
        Input: input,
        State: &DemoteState{},
    }

    for _, phase := range dw.phases {
        phaseCtx.Phase = phase

        // Execute all registered executors for this phase
        for _, executor := range dw.executors {
            if !slices.Contains(executor.Phases(), phase) {
                continue
            }

            if err := executor.Execute(ctx, phaseCtx); err != nil {
                if executor.CanFail(phase) {
                    // Accumulate non-fatal error
                    phaseCtx.Errors = append(phaseCtx.Errors, err)
                } else {
                    // Fatal error stops workflow
                    return nil, fmt.Errorf("phase %v failed: %w", phase, err)
                }
            }
        }
    }

    return &DemoteResult{...}, nil
}
```

### Locking Strategy

- Workflow holds `actionSema` for entire execution (same as current implementation)
- Executors assume lock is held and don't acquire additional locks
- No changes to existing locking semantics

### Idempotency

- ValidationExecutor checks current state and may short-circuit workflow
- Individual executors check their own state and skip if already complete
- Same idempotency guarantees as current implementation

### Observability

- Log entry/exit for each phase
- Log each executor's execution
- Metrics for phase duration
- Metrics for executor success/failure

## Migration Strategy

### Phase 1: Prototype (Current)

- Implement workflow system alongside existing code
- Create new method `DemoteWithWorkflow()`
- Keep existing `Demote()` unchanged
- Run tests against both implementations

### Phase 2: Validation

- Add feature flag to switch between implementations
- Test in development environment
- Compare behavior and performance
- Gather feedback from team

### Phase 3: Rollout

- Enable for percentage of traffic
- Monitor for errors and regressions
- Gradually increase rollout

### Phase 4: Cleanup

- Remove old implementation once stable
- Make phase-based approach the default

## Testing Strategy

### Unit Tests

- Test each executor independently with mocked dependencies
- Test failure scenarios for each executor
- Test `CanFail()` semantics

### Integration Tests

- Test full workflow with real dependencies (or integration test doubles)
- Test phase ordering
- Test idempotency (run workflow twice)
- Test failure at each phase
- Test non-fatal failure accumulation

### Comparison Tests

- Run same test scenarios against both old and new implementations
- Assert same final state
- Assert same error behavior

## Benefits

### For Developers

- **Clearer structure**: Phases make workflow easier to understand
- **Safer refactoring**: Can't accidentally reorder phases
- **Easier testing**: Test executors in isolation
- **Better extensibility**: Add new executor without modifying core logic

### For Operations

- **Better observability**: Clear phase transitions in logs
- **Easier debugging**: Know exactly which phase failed
- **More predictable**: Consistent behavior across different failure scenarios

### For Correctness

- **Explicit dependencies**: Phase ordering enforces constraints
- **Declarative failure handling**: Clear semantics for fatal vs non-fatal
- **Component decoupling**: Components don't call each other directly

## Future Enhancements

### Short Term

1. **Apply to Promote()**: Use same phase-based approach for promotion
2. **Add metrics**: Instrument phase duration and failure rates
3. **State persistence**: Optionally persist phase progress to topology

### Medium Term

1. **Async reconciliation**: Combine with Kubernetes-style reconciliation loop
2. **Retry logic**: Add configurable retry for transient failures
3. **Compensation**: Automatic rollback on certain failure types

### Long Term

1. **Dependency graph within phases**: Fine-grained ordering within coarse phases
2. **State machine formalization**: Use library like `qmuntal/stateless` for type safety
3. **Distributed coordination**: Support multiple MultiPooler instances coordinating

## Comparison to Alternatives

### vs. Current Procedural Approach

| Aspect               | Procedural                       | Phase-Based                 |
| -------------------- | -------------------------------- | --------------------------- |
| **Ordering**         | Implicit (code order)            | Explicit (phase order)      |
| **Coupling**         | Tight (manager calls components) | Loose (components register) |
| **Extensibility**    | Hard (modify Demote)             | Easy (add executor)         |
| **Testability**      | Integration tests only           | Unit + integration          |
| **Failure handling** | Ad-hoc (if/else)                 | Declarative (CanFail)       |

### vs. Pure State Machine

| Aspect             | State Machine       | Phase-Based         |
| ------------------ | ------------------- | ------------------- |
| **Granularity**    | Fine (10-15 states) | Coarse (5-7 phases) |
| **Complexity**     | Higher              | Lower               |
| **Branching**      | Natural             | Awkward             |
| **Learning curve** | Steeper             | Gentler             |

### vs. Dependency Graph (DAG)

| Aspect           | DAG               | Phase-Based          |
| ---------------- | ----------------- | -------------------- |
| **Dependencies** | Explicit per-task | Implicit per-phase   |
| **Parallelism**  | Automatic         | Manual within phase  |
| **Ordering**     | Topological sort  | Sequential phases    |
| **Familiarity**  | Build systems     | Test lifecycle hooks |

## Open Questions

1. **Should we persist phase progress?** If workflow crashes mid-execution, should it resume from last phase?

2. **How to handle rollback?** If restart fails, should we automatically undo previous phases (Saga pattern)?

3. **Should executors be stateful?** Or should all state live in PhaseContext?

4. **How to handle executor dependencies?** If TopologyExecutor must run before ReplTrackerExecutor within same phase?

5. **Should we support sub-phases?** For very complex phases, might we need "Early Drain" vs "Late Drain"?

## References

- [Original TODO comment](../manager.go#L85-L88) calling for async state management
- [Current Demote implementation](../rpc_manager.go#L677-L799)
- [Research on declarative patterns](./RESEARCH.md) (if created)
- [Kubernetes controller pattern](https://kubernetes.io/docs/concepts/extend-kubernetes/operator/)
- [Vitess VTOrc recovery](https://vitess.io/docs/reference/vtorc/)

## Authors

- Initial design: @weitzman (user)
- Prototype implementation: Claude Code
- Date: 2025-11-11
