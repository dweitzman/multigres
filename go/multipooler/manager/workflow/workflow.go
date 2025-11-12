// Copyright 2025 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package workflow provides a generic phase-based orchestration system.
//
// This package implements a declarative approach to complex workflows where operations
// are organized into phases and components register themselves as executors for the
// phases they care about.
//
// The workflow system is generic and can be used for any multi-step operation like
// demotion, promotion, configuration changes, etc.
package workflow

import (
	"context"
	"fmt"
)

// Phase represents a distinct phase in a workflow.
// Implementations should use an int-based enum type.
type Phase interface {
	~int
	String() string
}

// PhaseContext carries context through a workflow execution.
// The TState type accumulates state as executors complete their work.
type PhaseContext[TPhase Phase, TInput any, TState any] struct {
	// Current phase being executed
	Phase TPhase

	// Input parameters (read-only for executors)
	Input *TInput

	// Accumulated state (executors may read and update)
	State *TState

	// Non-fatal errors accumulated during execution
	Errors []error
}

// Executor is implemented by components that participate in a workflow.
//
// Components register themselves for specific phases and the workflow
// orchestrator calls Execute() when those phases are reached.
type Executor[TPhase Phase, TInput any, TState any] interface {
	// Name returns a unique identifier for this executor (for logging).
	Name() string

	// Phases returns the list of phases this executor participates in.
	// The executor's Execute() method will be called for each of these phases.
	Phases() []TPhase

	// Execute performs the executor's work for the given phase.
	// It may read from phaseCtx.State (set by previous executors) and
	// update phaseCtx.State for subsequent executors.
	// Whether errors are fatal depends on CanFail().
	Execute(ctx context.Context, phaseCtx *PhaseContext[TPhase, TInput, TState]) error

	// CanFail determines whether Execute() errors are fatal for the given phase.
	// If true, errors are logged but don't stop the workflow.
	// If false, errors immediately terminate the workflow.
	CanFail(phase TPhase) bool
}

// Logger interface for workflow logging.
type Logger interface {
	InfoContext(ctx context.Context, msg string, keysAndValues ...any)
	WarnContext(ctx context.Context, msg string, keysAndValues ...any)
	ErrorContext(ctx context.Context, msg string, keysAndValues ...any)
}

// Workflow orchestrates execution through a sequence of phases.
//
// Type parameters:
//   - TPhase: The phase enum type (must implement Phase interface)
//   - TInput: Input parameters for the workflow
//   - TState: State accumulated throughout the workflow
//   - TResult: Result returned on successful completion
//
// Example usage:
//
//	workflow := NewWorkflow[DemotePhase, DemoteInput, DemoteState, DemoteResult](
//	    []DemotePhase{PhaseValidate, PhaseStopWrites, PhaseDrain, ...},
//	    logger,
//	)
//	workflow.Register(&ValidationExecutor{...})
//	workflow.Register(&TopologyExecutor{...})
//
//	result, err := workflow.Execute(ctx, input, initialState, buildResult)
type Workflow[TPhase Phase, TInput any, TState any, TResult any] struct {
	phases    []TPhase
	executors []Executor[TPhase, TInput, TState]
	logger    Logger
}

// NewWorkflow creates a new workflow orchestrator.
//
// phases defines the sequence of phases to execute.
// logger is used for observability.
func NewWorkflow[TPhase Phase, TInput any, TState any, TResult any](
	phases []TPhase,
	logger Logger,
) *Workflow[TPhase, TInput, TState, TResult] {
	return &Workflow[TPhase, TInput, TState, TResult]{
		phases:    phases,
		executors: []Executor[TPhase, TInput, TState]{},
		logger:    logger,
	}
}

// Register adds an executor to the workflow.
// Executors are called in registration order within each phase.
func (w *Workflow[TPhase, TInput, TState, TResult]) Register(
	executor Executor[TPhase, TInput, TState],
) {
	w.executors = append(w.executors, executor)
}

// ResultBuilder is a function that constructs the final result from accumulated state.
// This is called after all phases complete successfully.
type ResultBuilder[TInput any, TState any, TResult any] func(
	input *TInput,
	state *TState,
	errors []error,
) (*TResult, error)

// EarlyExitChecker is an optional function that can short-circuit the workflow.
// It's called after each phase. If it returns true and a result, the workflow
// terminates early with that result.
//
// Example: Check if demotion is already complete after validation phase.
type EarlyExitChecker[TPhase Phase, TInput any, TState any, TResult any] func(
	phase TPhase,
	phaseCtx *PhaseContext[TPhase, TInput, TState],
) (shouldExit bool, result *TResult, err error)

// ExecuteOptions contains optional configuration for workflow execution.
type ExecuteOptions[TPhase Phase, TInput any, TState any, TResult any] struct {
	// EarlyExitChecker is called after each phase to check for early exit.
	EarlyExitChecker EarlyExitChecker[TPhase, TInput, TState, TResult]
}

// Execute runs the workflow from start to finish.
//
// The workflow proceeds through each phase in order, calling all registered
// executors for each phase. If an executor returns an error and CanFail()
// returns false, the workflow terminates immediately. Otherwise, the error
// is accumulated and the workflow continues.
//
// Parameters:
//   - ctx: Context for cancellation and deadlines
//   - input: Input parameters (read-only)
//   - initialState: Initial state (will be updated by executors)
//   - buildResult: Function to construct result from final state
//   - opts: Optional configuration (can be nil)
//
// Returns the result on success, or an error if a fatal failure occurred.
func (w *Workflow[TPhase, TInput, TState, TResult]) Execute(
	ctx context.Context,
	input *TInput,
	initialState *TState,
	buildResult ResultBuilder[TInput, TState, TResult],
	opts *ExecuteOptions[TPhase, TInput, TState, TResult],
) (*TResult, error) {
	phaseCtx := &PhaseContext[TPhase, TInput, TState]{
		Input:  input,
		State:  initialState,
		Errors: []error{},
	}

	w.logger.InfoContext(ctx, "Workflow starting")

	for _, phase := range w.phases {
		phaseCtx.Phase = phase

		w.logger.InfoContext(ctx, "Entering phase", "phase", phase.String())

		// Execute all executors for this phase
		for _, executor := range w.executors {
			if !containsPhase(executor.Phases(), phase) {
				continue
			}

			w.logger.InfoContext(ctx, "Executing",
				"executor", executor.Name(),
				"phase", phase.String())

			if err := executor.Execute(ctx, phaseCtx); err != nil {
				if executor.CanFail(phase) {
					// Non-fatal error - log and continue
					phaseCtx.Errors = append(phaseCtx.Errors, err)
					w.logger.WarnContext(ctx, "Executor failed (non-fatal)",
						"executor", executor.Name(),
						"phase", phase.String(),
						"error", err)
				} else {
					// Fatal error - terminate workflow
					w.logger.ErrorContext(ctx, "Executor failed (fatal)",
						"executor", executor.Name(),
						"phase", phase.String(),
						"error", err)
					return nil, fmt.Errorf("phase %s failed at %s: %w",
						phase.String(), executor.Name(), err)
				}
			}
		}

		w.logger.InfoContext(ctx, "Completed phase", "phase", phase.String())

		// Check for early exit
		if opts != nil && opts.EarlyExitChecker != nil {
			if shouldExit, result, err := opts.EarlyExitChecker(phase, phaseCtx); shouldExit {
				if err != nil {
					return nil, err
				}
				w.logger.InfoContext(ctx, "Workflow exiting early after phase", "phase", phase.String())
				return result, nil
			}
		}
	}

	w.logger.InfoContext(ctx, "Workflow completed successfully",
		"non_fatal_errors", len(phaseCtx.Errors))

	// Build final result
	return buildResult(input, phaseCtx.State, phaseCtx.Errors)
}

// containsPhase checks if a phase is in the list.
func containsPhase[TPhase Phase](phases []TPhase, phase TPhase) bool {
	for _, p := range phases {
		if p == phase {
			return true
		}
	}
	return false
}
