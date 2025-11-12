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

package workflow

import (
	"time"

	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
)

// DemotePhase represents a distinct phase in the demotion workflow.
type DemotePhase int

const (
	// DemotePhaseValidate validates preconditions (term, connection, guardrails)
	DemotePhaseValidate DemotePhase = iota

	// DemotePhaseStopWrites transitions to read-only mode (topology, heartbeat)
	DemotePhaseStopWrites

	// DemotePhaseDrain waits for writes to complete (checkpoint, connection monitoring)
	DemotePhaseDrain

	// DemotePhaseCapture captures final state (LSN)
	DemotePhaseCapture

	// DemotePhaseRestart restarts PostgreSQL as standby
	DemotePhaseRestart

	// DemotePhaseCleanup performs cleanup operations (reset sync repl, update topology)
	DemotePhaseCleanup
)

// String returns the name of the phase for logging.
func (p DemotePhase) String() string {
	switch p {
	case DemotePhaseValidate:
		return "Validate"
	case DemotePhaseStopWrites:
		return "StopWrites"
	case DemotePhaseDrain:
		return "Drain"
	case DemotePhaseCapture:
		return "Capture"
	case DemotePhaseRestart:
		return "Restart"
	case DemotePhaseCleanup:
		return "Cleanup"
	default:
		return "Unknown"
	}
}

// DemoteInput contains input parameters for the demotion workflow.
type DemoteInput struct {
	ConsensusTerm int64
	DrainTimeout  time.Duration
	Force         bool
}

// DemoteState tracks accumulated state throughout the workflow.
// This is populated by executors as they complete their work.
type DemoteState struct {
	// From validation phase
	IsServingReadOnly   bool
	IsReplicaInTopology bool
	IsReadOnly          bool
	WasAlreadyDemoted   bool

	// From capture phase
	FinalLSN string

	// From drain phase
	ConnectionsTerminated int32
}

// DemoteResult is the output of a successful demotion workflow.
type DemoteResult struct {
	WasAlreadyDemoted     bool
	ConsensusTerm         int64
	LsnPosition           string
	ConnectionsTerminated int32
	NonFatalErrors        []error
}

// ToProto converts DemoteResult to protobuf response.
func (r *DemoteResult) ToProto() *multipoolermanagerdatapb.DemoteResponse {
	return &multipoolermanagerdatapb.DemoteResponse{
		WasAlreadyDemoted:     r.WasAlreadyDemoted,
		ConsensusTerm:         r.ConsensusTerm,
		LsnPosition:           r.LsnPosition,
		ConnectionsTerminated: r.ConnectionsTerminated,
	}
}

// NewDemoteWorkflow creates a workflow configured for demotion.
func NewDemoteWorkflow(logger Logger) *Workflow[DemotePhase, DemoteInput, DemoteState, DemoteResult] {
	phases := []DemotePhase{
		DemotePhaseValidate,
		DemotePhaseStopWrites,
		DemotePhaseDrain,
		DemotePhaseCapture,
		DemotePhaseRestart,
		DemotePhaseCleanup,
	}

	return NewWorkflow[DemotePhase, DemoteInput, DemoteState, DemoteResult](phases, logger)
}

// BuildDemoteResult constructs a DemoteResult from the final state.
// This is passed to Workflow.Execute as the ResultBuilder.
func BuildDemoteResult(
	input *DemoteInput,
	state *DemoteState,
	errors []error,
) (*DemoteResult, error) {
	return &DemoteResult{
		WasAlreadyDemoted:     state.WasAlreadyDemoted,
		ConsensusTerm:         input.ConsensusTerm,
		LsnPosition:           state.FinalLSN,
		ConnectionsTerminated: state.ConnectionsTerminated,
		NonFatalErrors:        errors,
	}, nil
}

// CheckDemoteEarlyExit checks if the workflow can exit early after validation.
// If the system is already demoted, we can skip all remaining phases.
func CheckDemoteEarlyExit(
	phase DemotePhase,
	phaseCtx *PhaseContext[DemotePhase, DemoteInput, DemoteState],
) (bool, *DemoteResult, error) {
	// Only check after validation phase
	if phase != DemotePhaseValidate {
		return false, nil, nil
	}

	// If already demoted, exit early
	if phaseCtx.State.WasAlreadyDemoted {
		return true, &DemoteResult{
			WasAlreadyDemoted:     true,
			ConsensusTerm:         phaseCtx.Input.ConsensusTerm,
			LsnPosition:           phaseCtx.State.FinalLSN,
			ConnectionsTerminated: 0,
			NonFatalErrors:        phaseCtx.Errors,
		}, nil
	}

	return false, nil, nil
}

// DemoteExecutor is a type alias for executors in the demotion workflow.
type DemoteExecutor = Executor[DemotePhase, DemoteInput, DemoteState]
