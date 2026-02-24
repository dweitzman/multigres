// Copyright 2026 Supabase, Inc.
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

package ticks

import "time"

// TickProvider provides access to the current logical tick.
// This interface allows components to get the current tick without
// depending on the specific implementation (e.g., recovery Engine).
type TickProvider interface {
	// CurrentTick returns the current logical tick.
	CurrentTick() Tick
}

// RealTimeTickProvider calculates ticks from the current wall-clock time.
// This is the default implementation used in production and tests.
type RealTimeTickProvider struct{}

// NewRealTimeTickProvider creates a new real-time tick provider.
func NewRealTimeTickProvider() *RealTimeTickProvider {
	return &RealTimeTickProvider{}
}

// CurrentTick returns the current tick calculated from wall-clock time.
func (r *RealTimeTickProvider) CurrentTick() Tick {
	now := time.Now()
	return ToTick(int64(DurationToTicks(time.Duration(now.UnixNano()))))
}
