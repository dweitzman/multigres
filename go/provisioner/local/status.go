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

package local

import (
	"fmt"
	"sort"
	"sync"
	"time"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
)

// ResourceState tracks the state of a resource during provisioning
type ResourceState struct {
	Resource  Resource
	Status    ResourceStatus
	StartTime time.Time
	EndTime   time.Time
	Error     error
}

// StatusMonitor tracks and displays the status of resource provisioning
type StatusMonitor struct {
	states map[string]*ResourceState // resourceKey -> ResourceState
	mu     sync.RWMutex
}

// NewStatusMonitor creates a new status monitor
func NewStatusMonitor() *StatusMonitor {
	return &StatusMonitor{
		states: make(map[string]*ResourceState),
	}
}

// Start initializes the status monitor with the resources to track
func (sm *StatusMonitor) Start(resources []Resource) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	for _, r := range resources {
		key := resourceKey(r.ID())
		sm.states[key] = &ResourceState{
			Resource:  r,
			Status:    StatusPending,
			StartTime: time.Time{},
		}
	}

	// Display initial status
	sm.displayStatus()
}

// UpdateStatus updates the status of a resource
func (sm *StatusMonitor) UpdateStatus(id *clustermetadatapb.ID, status ResourceStatus) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	key := resourceKey(id)
	if state, exists := sm.states[key]; exists {
		state.Status = status

		if status == StatusProvisioning || status == StatusDeprovisioning {
			state.StartTime = time.Now()
		}

		if status == StatusReady || status == StatusFailed {
			state.EndTime = time.Now()
		}

		// Display updated status
		sm.displayStatus()
	}
}

// Stop finalizes the status display
func (sm *StatusMonitor) Stop() {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	sm.displayStatus()
}

// displayStatus displays the current status of all resources
func (sm *StatusMonitor) displayStatus() {
	// Group resources by phase
	phases := make(map[ProvisionPhase][]*ResourceState)

	for _, state := range sm.states {
		phase := GetPhase(state.Resource.ID())
		phases[phase] = append(phases[phase], state)
	}

	// Sort phases
	phaseOrder := []ProvisionPhase{PhaseTopologySetup, PhaseGlobalServices, PhaseCellServices}

	// Display each phase
	for _, phase := range phaseOrder {
		states := phases[phase]
		if len(states) == 0 {
			continue
		}

		// Sort resources within phase for consistent ordering
		sort.Slice(states, func(i, j int) bool {
			// Sort by cell first, then by component, then by name
			iID := states[i].Resource.ID()
			jID := states[j].Resource.ID()

			if iID.Cell != jID.Cell {
				return iID.Cell < jID.Cell
			}
			if iID.Component != jID.Component {
				return iID.Component < jID.Component
			}
			return iID.Name < jID.Name
		})

		// Display phase header
		fmt.Printf("\n%s\n", phase.String())

		// Display each resource status
		for _, state := range states {
			sm.displayResourceStatus(state)
		}
	}
}

// displayResourceStatus displays the status of a single resource
func (sm *StatusMonitor) displayResourceStatus(state *ResourceState) {
	id := state.Resource.ID()

	// Format the resource name
	var name string
	switch id.Component {
	case clustermetadatapb.ID_GLOBAL_TOPO:
		name = fmt.Sprintf("Global topology (%s)", id.Name)
	case clustermetadatapb.ID_CELL_TOPO:
		name = fmt.Sprintf("Cell %s topology", id.Cell)
	case clustermetadatapb.ID_DATABASE:
		name = fmt.Sprintf("Database '%s'", id.Name)
	default:
		name = fmt.Sprintf("%s (%s)", id.Component.String(), id.Name)
	}

	// Format status icon
	var icon string
	switch state.Status {
	case StatusPending:
		icon = "⏳"
	case StatusProvisioning:
		icon = "▶️"
	case StatusReady:
		icon = "✓"
	case StatusFailed:
		icon = "✗"
	case StatusDeprovisioning:
		icon = "⏹️"
	default:
		icon = "?"
	}

	// Format duration
	var duration string
	if !state.StartTime.IsZero() && !state.EndTime.IsZero() {
		d := state.EndTime.Sub(state.StartTime)
		duration = fmt.Sprintf(" [%.1fs]", d.Seconds())
	} else if !state.StartTime.IsZero() {
		d := time.Since(state.StartTime)
		duration = fmt.Sprintf(" [%.1fs...]", d.Seconds())
	}

	// Display resource status
	fmt.Printf("  %s %s%s\n", icon, name, duration)

	// Display error if failed
	if state.Status == StatusFailed && state.Error != nil {
		fmt.Printf("    Error: %v\n", state.Error)
	}
}
