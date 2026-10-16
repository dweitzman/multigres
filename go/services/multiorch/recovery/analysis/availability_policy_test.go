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

package analysis

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/multigres/multigres/go/services/multiorch/config"
)

func TestNewAvailabilityPolicy_ReadsTheFailoverGraceFromConfig(t *testing.T) {
	cfg := config.NewTestConfig(
		config.WithLeaderFailoverGracePeriodBase(3*time.Second),
		config.WithLeaderFailoverGracePeriodMaxJitter(2*time.Second),
	)
	assert.Equal(t, 5*time.Second, NewAvailabilityPolicy(cfg).FailoverGracePeriod, "base plus the maximum jitter")
}

func TestNewAvailabilityPolicy_NilConfigLeavesTheConfigValuesUnset(t *testing.T) {
	policy := NewAvailabilityPolicy(nil)
	assert.Zero(t, policy.FailoverGracePeriod)
	assert.Zero(t, policy.LeaderPostgresResponseThreshold)
	assert.NotZero(t, policy.ObservationFreshness, "the built-in thresholds still apply")
}

func TestDefaultAvailabilityPolicy_MatchesTheConfigDefaults(t *testing.T) {
	policy := DefaultAvailabilityPolicy()
	assert.Equal(t, 12*time.Second, policy.FailoverGracePeriod, "default base 4s plus default max jitter 8s")
	assert.Equal(t, 30*time.Second, policy.LeaderPostgresResponseThreshold)
}
