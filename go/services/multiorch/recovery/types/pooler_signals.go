// Copyright 2026 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package types

import (
	"github.com/multigres/multigres/go/common/ha"
	multiorchdatapb "github.com/multigres/multigres/go/pb/multiorchdata"
)

// HAReport is the part of a pooler's health state that HA decisions read; pass
// it to the go/common/ha interpretation functions.
func HAReport(p *multiorchdatapb.PoolerHealthState) ha.PoolerReport {
	return ha.PoolerReport{
		Consensus:      p.GetConsensusStatus(),
		Availability:   p.GetAvailabilityStatus(),
		LeadershipMode: p.GetMultipooler().GetLeadershipMode(),
	}
}
