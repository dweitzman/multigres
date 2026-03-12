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

package postgres

import (
	"context"
	"time"

	"github.com/multigres/multigres/go/common/consensus"
)

// ── CoordDriver ──────────────────────────────────────────────────────────────

// CoordDriver runs a CoordNode state machine. The coordinator owns all outbound
// gRPC connections to poolers; a pooler never dials the coordinator.
//
// The coordinator interacts with poolers via three outbound gRPC calls:
//
//   - WritePolicy (unary): sends a Term to the primary and waits for
//     accept/reject. On accept it also calls PushRules on all known replicas so
//     they learn about the change without polling.
//
//   - Recruit (unary): recruits a node into a coordinator-led term change
//     (Stage 3).
//
//   - WatchStatus (server-streaming): subscribes to a pooler's status stream.
//     Each received snapshot becomes a PoolerStatusIndicator for the CoordNode.
//
// Discovery of new poolers is driven by etcd: an etcd watcher goroutine calls
// OnPoolerDiscovered/OnPoolerRemoved as poolers register and deregister.
// Each discovery event causes the coordinator to open a WatchStatus stream to
// the new pooler, feeding its status into the incoming indicator channel.
//
// All state in CoordNode is ephemeral: on restart the coordinator re-learns
// the cluster by processing the initial PoolerStatusIndicator bursts from each
// pooler's WatchStatus stream.
type CoordDriver struct {
	node     *consensus.CoordNode
	tick     int64
	incoming chan consensus.Indicator
	// poolerClients map[consensus.NodeID]pb.PoolerServiceClient
	// (one outbound gRPC connection per discovered pooler)
}

// NewCoordDriver creates a CoordDriver for the given node.
func NewCoordDriver(node *consensus.CoordNode) *CoordDriver {
	return &CoordDriver{
		node:     node,
		incoming: make(chan consensus.Indicator, 64),
	}
}

// Incoming returns the channel onto which external events should be pushed.
// The etcd watch goroutine writes PoolerMembershipRequest here;
// WatchStatus stream goroutines write PoolerStatusIndicator here.
func (d *CoordDriver) Incoming() chan<- consensus.Indicator {
	return d.incoming
}

// Run starts the coordinator tick loop. Returns when ctx is cancelled.
func (d *CoordDriver) Run(ctx context.Context) error {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	var pending []consensus.Indicator

	for {
		select {
		case ind := <-d.incoming:
			pending = append(pending, ind)

		case <-ticker.C:
			d.tick++
			requests := d.node.Step(d.tick, pending)
			pending = pending[:0]

			for _, req := range requests {
				switch r := req.(type) {
				case consensus.WritePolicyRequest:
					// Send the rules write to the target primary, then broadcast
					// to replicas on success so they don't have to poll.
					knownPoolerIDs := d.node.KnownPoolerIDs()
					go d.sendWritePolicy(ctx, r, knownPoolerIDs)
				}
			}

		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// sendWritePolicy calls WritePolicy on the target primary pooler and feeds the
// response back as a WritePolicyResponseIndicator. On success it also calls
// PushRules on all known replicas so they learn about the new rules without
// having to poll the consensus table.
func (d *CoordDriver) sendWritePolicy(ctx context.Context, req consensus.WritePolicyRequest, knownPoolerIDs []consensus.NodeID) {
	// Call WritePolicy on the primary.
	//
	//   resp, err := d.poolerClients[req.TargetPooler].WritePolicy(ctx, &pb.WritePolicyRequest{
	//       CorrelationId: generateCorrelationID(),
	//       Term:          termToProto(req.Term),
	//   })
	//   if err != nil {
	//       return // primary unreachable; CoordNode will time out and retry
	//   }
	//   d.incoming <- consensus.WritePolicyResponseIndicator{
	//       CorrelationID: resp.CorrelationId,
	//       FromPooler:    req.TargetPooler,
	//       Accepted:      resp.Accepted,
	//       CurrentSeq:    resp.CurrentSeq,
	//   }
	//   if !resp.Accepted {
	//       return // coordinator will reconcile on next advance
	//   }
	//
	// Push the committed rules to all known replicas so they update their
	// PoolerNode without polling. Failures are non-fatal: the replica will
	// catch up when the coordinator retries or when its next status snapshot
	// arrives.
	//
	//   for _, id := range knownPoolerIDs {
	//       if id == req.TargetPooler {
	//           continue // primary already committed the rules
	//       }
	//       go func(poolerID consensus.NodeID) {
	//           _, _ = d.poolerClients[poolerID].PushRules(ctx, &pb.PushRulesRequest{
	//               Term: termToProto(req.Term),
	//           })
	//       }(id)
	//   }

	_ = req
	_ = knownPoolerIDs
	_ = ctx
}

// OnPoolerDiscovered should be called (e.g. from an etcd watch callback) when
// a new pooler registers. It notifies the CoordNode and begins streaming status
// updates from the pooler. Run in a goroutine; exits when ctx is cancelled.
func (d *CoordDriver) OnPoolerDiscovered(ctx context.Context, poolerID consensus.NodeID) {
	d.incoming <- consensus.PoolerDiscoveredIndicator{PoolerID: poolerID}

	// Open a WatchStatus stream to the pooler. Each snapshot becomes a
	// PoolerStatusIndicator for the CoordNode. The stream is kept alive
	// until ctx is cancelled or the pooler deregisters.
	//
	//   stream, err := d.poolerClients[poolerID].WatchStatus(ctx, &pb.WatchStatusRequest{})
	//   if err != nil { return }
	//   for {
	//       snap, err := stream.Recv()
	//       if err != nil { return }
	//       d.incoming <- consensus.PoolerStatusIndicator{
	//           PoolerID:       poolerID,
	//           State:          stateFromProto(snap.State),
	//           PostgresStatus: postgresStatusFromProto(snap.PostgresStatus),
	//           Properties:     propertiesFromProto(snap.Properties),
	//       }
	//   }

	_ = ctx
	_ = poolerID
}

// OnPoolerRemoved should be called when a pooler deregisters from etcd.
// It notifies the CoordNode that the pooler is gone. The WatchStatus goroutine
// will exit naturally when the stream closes.
func (d *CoordDriver) OnPoolerRemoved(poolerID consensus.NodeID) {
	d.incoming <- consensus.PoolerRemovedIndicator{PoolerID: poolerID}
}
