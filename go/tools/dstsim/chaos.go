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

package dstsim

import "math/rand/v2"

// ChaosParams encapsulates the parameters for chaos applied to a communication
// channel: drop probability, delivery delay, duplicate probability, and whether
// out-of-order delivery is permitted. It is a shared primitive used by delivery
// managers (e.g. ChaosDeliveryManager) and by intra-node queues (e.g. the WAL
// receive buffer in SimPooler).
//
// Eligibility for each chaos mode is controlled by the field values: set
// DropRate=0 to prevent drops, DupRate=0 to prevent duplicates, MaxDelay<=1
// to prevent extra delay, Reorder=false (the default) to enforce FIFO delivery
// order in queues.
//
// The zero value represents a reliable, non-chaotic channel: no drops, no
// duplicates, no reordering, 1-tick delivery delay.
type ChaosParams struct {
	MinDelay int64      // Minimum delivery delay in ticks; 0 defaults to 1. Set MinDelay=MaxDelay for a fixed delay.
	MaxDelay int64      // Maximum delivery delay in ticks; 0 or <=MinDelay means always MinDelay (or 1)
	DropRate float64    // Probability of dropping the item (0.0–1.0); 0 = never drop
	DupRate  float64    // Probability of duplicating the item (0.0–1.0); 0 = never dup
	Reorder  bool       // If true, items may arrive out of push order (different delays)
	Rng      *rand.Rand // Nil disables random chaos; MinDelay still applies when Rng is nil
}

// Decide makes a chaos decision for a single item. The active chaos modes are
// determined by the field values: DropRate>0 can drop, MaxDelay>MinDelay adds
// random jitter, DupRate>0 can duplicate.
//
// Returns:
//   - deliver: false if the item should be discarded
//   - delay: number of ticks until delivery; always >= 1 when deliver=true
//   - duplicate: true if the item should also be delivered a second time
//
// When Rng is nil, drops and duplicates are disabled and a fixed delay of
// max(1, MinDelay) is returned. With Rng non-nil, delay is chosen uniformly
// in [max(1, MinDelay), max(max(1,MinDelay), MaxDelay)].
func (c *ChaosParams) Decide() (deliver bool, delay int64, duplicate bool) {
	minDelay := max(c.MinDelay, 1)
	if c.Rng == nil {
		return true, minDelay, false
	}
	if c.DropRate > 0 && c.Rng.Float64() < c.DropRate {
		return false, 0, false
	}
	delay = minDelay
	if c.MaxDelay > minDelay {
		delay = minDelay + c.Rng.Int64N(c.MaxDelay-minDelay+1)
	}
	duplicate = c.DupRate > 0 && c.Rng.Float64() < c.DupRate
	return true, delay, duplicate
}
