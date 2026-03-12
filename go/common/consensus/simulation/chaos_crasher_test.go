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

package simulation

import (
	"fmt"
	"math/rand/v2"

	"github.com/multigres/multigres/go/common/consensus"
	"github.com/multigres/multigres/go/tools/dstsim/sortedmaps"
)

// chaosCrasher is a simulator node that periodically crashes and restarts its
// target nodes. avgTicksBetweenCrashes controls the expected number of ticks
// between successive crashes: on each tick a running target is crashed with
// probability 1/avgTicksBetweenCrashes. Crash counts are tracked per-target
// for post-run assertions.
type chaosCrasher struct {
	id                     consensus.NodeID
	sim                    *simType
	rng                    *rand.Rand
	targets                func() []consensus.NodeID
	avgTicksBetweenCrashes int64 // expected ticks between successive crashes
	downTicks              int64 // number of ticks to keep a crashed node down
	downUntil              map[consensus.NodeID]int64
	crashes                map[consensus.NodeID]int
}

func newChaosCrasher(
	id consensus.NodeID,
	sim *simType,
	rng *rand.Rand,
	targets func() []consensus.NodeID,
	avgTicksBetweenCrashes int64,
	downTicks int64,
) *chaosCrasher {
	return &chaosCrasher{
		id:                     id,
		sim:                    sim,
		rng:                    rng,
		targets:                targets,
		avgTicksBetweenCrashes: avgTicksBetweenCrashes,
		downTicks:              downTicks,
		downUntil:              make(map[consensus.NodeID]int64),
		crashes:                make(map[consensus.NodeID]int),
	}
}

func (c *chaosCrasher) ID() consensus.NodeID { return c.id }

func (c *chaosCrasher) Step(tick int64, _ []consensus.Indicator) []consensus.Request {
	// Restart any previously-crashed node whose downtime has elapsed, even if
	// it is no longer in the current targets() list. This is necessary when
	// targets() is dynamic (e.g. "crash the current quorum primary"): once the
	// quorum shifts away from a node, it would otherwise stay stopped forever.
	for target, downUntil := range sortedmaps.All(c.downUntil) {
		if downUntil > 0 && tick >= downUntil {
			c.sim.RestartNode(target)
			c.downUntil[target] = 0
		}
	}

	crashProb := 1.0 / float64(c.avgTicksBetweenCrashes)
	for _, target := range c.targets() {
		if c.downUntil[target] == 0 && c.rng.Float64() < crashProb {
			c.crashes[target]++
			c.downUntil[target] = tick + c.downTicks
			c.sim.StopNode(target)
		}
	}
	return nil
}

func (c *chaosCrasher) totalCrashes() int {
	total := 0
	for _, n := range sortedmaps.Values(c.crashes) {
		total += n
	}
	return total
}

// minCrashes returns a Condition that becomes true once the total crash count
// across all targets reaches n.
func (c *chaosCrasher) minCrashes(n int) *minCrashesCondition {
	return &minCrashesCondition{crasher: c, n: n}
}

// minCrashesCondition is a Condition that becomes true once the total crash
// count across all crasher targets reaches the threshold.
type minCrashesCondition struct {
	crasher *chaosCrasher
	n       int
}

func (c *minCrashesCondition) Name() string         { return fmt.Sprintf("min_crashes_%d", c.n) }
func (c *minCrashesCondition) Eval(_ *simType) bool { return c.crasher.totalCrashes() >= c.n }
func (c *minCrashesCondition) Describe(_ *simType) string {
	return fmt.Sprintf("crashes=%d, want>=%d", c.crasher.totalCrashes(), c.n)
}
