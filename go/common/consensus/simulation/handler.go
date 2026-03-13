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

	"github.com/multigres/multigres/go/common/consensus"
)

// Handler implements dstsim.RequestHandler by routing consensus Requests to
// the appropriate Indicators for their target nodes.
//
// Status broadcasts (PoolerStatusUpdateRequest) are delivered to all
// coordinator nodes. The handler discovers coordinator IDs by inspecting the
// simulator's registered nodes at routing time.
//
// The handler models gRPC-style request/response pairs for LeaderWritePolicyRequest:
// when routing a LeaderWritePolicyRequest to a LeaderWritePolicyIndicator, it auto-generates
// a correlation ID and records the originating node. When the pooler emits a
// LeaderWritePolicyResponseRequest with that correlation ID, the handler routes the
// response back to the origin and cleans up the entry.
type Handler struct {
	// coordIDs lists the node IDs that should receive PoolerStatusIndicator
	// broadcasts. Set this to all coordinator IDs before running the simulation.
	coordIDs []consensus.NodeID

	// correlationReturnTo maps a correlation ID to the node that should receive
	// the corresponding LeaderWritePolicyResponseRequest. Entries are removed once
	// the response is delivered.
	correlationReturnTo map[string]consensus.NodeID

	// nextCorrSeq is a monotone counter for generating unique correlation IDs.
	nextCorrSeq int
}

// NewHandler creates a Handler that broadcasts pooler status to the given
// coordinator IDs.
func NewHandler(coordIDs ...consensus.NodeID) *Handler {
	return &Handler{
		coordIDs:            coordIDs,
		correlationReturnTo: make(map[string]consensus.NodeID),
	}
}

// AddCoordID adds a coordinator ID to the broadcast list.
func (h *Handler) AddCoordID(id consensus.NodeID) {
	h.coordIDs = append(h.coordIDs, id)
}

// ProcessRequests converts each Request into Indicators and returns a map of
// target node ID → indicators to deliver. Implements
// dstsim.RequestHandler[consensus.Indicator, consensus.Request, consensus.NodeID].
func (h *Handler) ProcessRequests(
	_ *simType,
	fromNode consensus.NodeID,
	requests []consensus.Request,
) map[consensus.NodeID][]consensus.Indicator {
	result := make(map[consensus.NodeID][]consensus.Indicator)

	for _, req := range requests {
		switch r := req.(type) {
		case consensus.RecruitRequest:
			h.nextCorrSeq++
			corrID := fmt.Sprintf("%s/%d", fromNode, h.nextCorrSeq)
			h.correlationReturnTo[corrID] = fromNode
			result[r.TargetPooler] = append(result[r.TargetPooler], consensus.RecruitIndicator{
				CorrelationID: corrID,
				CoordID:       fromNode,
				AtTermSeq:     r.AtTermSeq,
				ProposedSeq:   r.ProposedSeq,
			})

		case consensus.RecruitResponseRequest:
			dest := h.correlationReturnTo[r.CorrelationID]
			delete(h.correlationReturnTo, r.CorrelationID)
			if dest != "" {
				result[dest] = append(result[dest], consensus.RecruitResponseIndicator{
					CorrelationID: r.CorrelationID,
					FromPooler:    fromNode,
					Accepted:      r.Accepted,
					Position:      r.Position,
					Commitment:    r.Commitment,
				})
			}

		case consensus.ProposeRequest:
			h.nextCorrSeq++
			corrID := fmt.Sprintf("%s/%d", fromNode, h.nextCorrSeq)
			h.correlationReturnTo[corrID] = fromNode
			result[r.TargetPooler] = append(result[r.TargetPooler], consensus.ProposeIndicator{
				CorrelationID: corrID,
				Term:          r.Term,
				BaseLSN:       r.BaseLSN,
				ApplyNow:      r.ApplyNow,
			})

		case consensus.ProposeAckedRequest:
			dest := h.correlationReturnTo[r.CorrelationID]
			delete(h.correlationReturnTo, r.CorrelationID)
			if dest != "" {
				result[dest] = append(result[dest], consensus.ProposeAckedIndicator{
					CorrelationID: r.CorrelationID,
					FromPooler:    fromNode,
					Accepted:      r.Accepted,
				})
			}

		case consensus.LeaderWritePolicyRequest:
			h.nextCorrSeq++
			corrID := fmt.Sprintf("%s/%d", fromNode, h.nextCorrSeq)
			h.correlationReturnTo[corrID] = fromNode
			result[r.TargetPooler] = append(result[r.TargetPooler], consensus.LeaderWritePolicyIndicator{
				CorrelationID: corrID,
				FromSeq:       r.FromSeq,
				Term:          r.Term,
			})

		case consensus.LeaderWritePolicyResponseRequest:
			dest := h.correlationReturnTo[r.CorrelationID]
			delete(h.correlationReturnTo, r.CorrelationID)
			if dest != "" {
				result[dest] = append(result[dest], consensus.LeaderWritePolicyResponseIndicator{
					CorrelationID: r.CorrelationID,
					FromPooler:    fromNode,
					Accepted:      r.Accepted,
					CurrentSeq:    r.CurrentSeq,
				})
			}

		case consensus.PoolerStatusUpdateRequest:
			for _, coordID := range h.coordIDs {
				result[coordID] = append(result[coordID], consensus.PoolerStatusIndicator{
					PoolerID:       fromNode,
					State:          r.State,
					PostgresStatus: r.PostgresStatus,
					Properties:     r.Properties,
				})
			}

		case consensus.PropagatePositionRequest:
			h.nextCorrSeq++
			corrID := fmt.Sprintf("%s/%d", fromNode, h.nextCorrSeq)
			h.correlationReturnTo[corrID] = fromNode
			result[r.TargetPooler] = append(result[r.TargetPooler], consensus.PropagatePositionIndicator{
				CorrelationID:  corrID,
				SourceNode:     r.SourceNode,
				TargetPosition: r.TargetPosition,
			})

		case consensus.PropagatePositionAckedRequest:
			dest := h.correlationReturnTo[r.CorrelationID]
			delete(h.correlationReturnTo, r.CorrelationID)
			if dest != "" {
				result[dest] = append(result[dest], consensus.PropagatePositionAckedIndicator{
					CorrelationID: r.CorrelationID,
					FromPooler:    fromNode,
					Accepted:      r.Accepted,
				})
			}

		case consensus.ResumeRequest:
			result[r.TargetPooler] = append(result[r.TargetPooler], consensus.ResumeIndicator{
				FromCoord: fromNode,
				Term:      r.Term,
			})

		case consensus.TerminateRequest:
			result[r.Target] = append(result[r.Target], consensus.TerminateIndicator{})

		case consensus.PoolerMembershipRequest:
			for _, coordID := range h.coordIDs {
				if r.TargetCoord != "" && r.TargetCoord != coordID {
					continue
				}
				for _, id := range r.Discovered {
					result[coordID] = append(result[coordID], consensus.PoolerDiscoveredIndicator{PoolerID: id})
				}
				for _, id := range r.Removed {
					result[coordID] = append(result[coordID], consensus.PoolerRemovedIndicator{PoolerID: id})
				}
			}
		}
	}
	return result
}
