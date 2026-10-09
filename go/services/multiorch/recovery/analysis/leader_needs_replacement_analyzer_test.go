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
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/multigres/multigres/go/common/rpcclient"
	"github.com/multigres/multigres/go/common/topoclient"
	"github.com/multigres/multigres/go/common/topoclient/memorytopo"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multiorchdatapb "github.com/multigres/multigres/go/pb/multiorchdata"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
	"github.com/multigres/multigres/go/services/multiorch/config"
	"github.com/multigres/multigres/go/services/multiorch/consensus"
	"github.com/multigres/multigres/go/services/multiorch/recovery/types"
	"github.com/multigres/multigres/go/services/multiorch/store"
)

func TestLeaderNeedsReplacementAnalyzer_Analyze(t *testing.T) {
	// Set up factory for tests
	ctx := context.Background()
	ts, _ := memorytopo.NewServerAndFactory(ctx, "cell1")
	defer ts.Close()
	rpcClient := &rpcclient.FakeClient{}
	poolerStore := store.NewTestCache(t)
	coordID := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIORCH,
		Cell:      "cell1",
		Name:      "test-coord",
	}
	coord := consensus.NewCoordinator(coordID, ts, rpcClient, slog.Default())
	cfg := config.NewTestConfig()
	factory := NewRecoveryActionFactory(cfg, poolerStore, rpcClient, ts, coord, slog.Default())

	analyzer := &LeaderNeedsReplacementAnalyzer{factory: factory}

	leaderID := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "leader-1"}
	follower1ID := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "follower-1"}
	follower2ID := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "follower-2"}
	observerID := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "observer-1"}
	priorLeaderID := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "prior-leader-1"}
	shardKey := &clustermetadatapb.ShardKey{Database: "db", TableGroup: "tg", Shard: "0"}

	atLeastN := func(n int32) *clustermetadatapb.DurabilityPolicy {
		return &clustermetadatapb.DurabilityPolicy{
			QuorumType:    clustermetadatapb.QuorumType_QUORUM_TYPE_AT_LEAST_N,
			RequiredCount: n,
		}
	}

	// freshFollower builds an initialized, freshly-observed follower rider. It has
	// no replication status, so it counts as reachable/initialized (for recruitment
	// feasibility) but is NOT streaming from the leader (does not vouch) until
	// connectReplica gives it a live stream. It carries a minimal cached
	// position (recruitable requires one) but no rule, so it doesn't
	// independently vouch for the leader either.
	freshFollower := func(id *clustermetadatapb.ID, now time.Time) *store.Pooler {
		return newRider(&multiorchdatapb.PoolerHealthState{
			Multipooler: &clustermetadatapb.Multipooler{Id: id, ShardKey: shardKey},
			LastSeen:    timestamppb.New(now),
			Status:      &multipoolermanagerdatapb.Status{IsInitialized: true},
			ConsensusStatus: &clustermetadatapb.ConsensusStatus{
				Id:              id,
				CurrentPosition: &clustermetadatapb.PoolerPosition{Lsn: "0/1"},
			},
		})
	}

	// deadLeaderShardAnalysis builds a ShardAnalysis with a dead leader and two
	// initialized followers — the base case for leader-replacement detection. The
	// leader rider starts not-valid (no live observation); use setLeaderLive to mark
	// it observed-live in subtests that need a reachable leader.
	//
	// The rule's cohort is {leader, follower1, follower2} (3 members) and the
	// durability policy is AtLeast(2). A recruitment quorum needs a strict majority
	// (2 of 3) reachable, with the unreachable remainder unable to satisfy the
	// policy. So with both followers reachable a failover is FEASIBLE; subtests that
	// need infeasibility (ShardStuck / ShardAtRisk) drop a follower from Analyses.
	//
	// The rule's CreationTime is set an hour in the past so the promotion grace
	// window never accidentally suppresses detection; promotion subtests reset it.
	deadLeaderShardAnalysis := func(overrides ...func(*ShardAnalysis)) *ShardAnalysis {
		now := time.Now()
		decision := &clustermetadatapb.ShardRule{
			LeaderId:         leaderID,
			RuleNumber:       &clustermetadatapb.RuleNumber{CoordinatorTerm: 1},
			CohortMembers:    []*clustermetadatapb.ID{leaderID, follower1ID, follower2ID},
			CreationTime:     timestamppb.New(now.Add(-time.Hour)),
			DurabilityPolicy: atLeastN(2),
		}
		sa := &ShardAnalysis{
			ShardKey:        shardKey,
			HighestPosition: &clustermetadatapb.RulePosition{Decision: decision},
			Now:             now,
			Policy:          DefaultAvailabilityPolicy(),
			// The leader's own ConsensusStatus mirrors HighestPosition's decision
			// by default — a genuinely-promoted leader confirms its own rule
			// (commonconsensus.IsActiveLeader). setLeaderRevoked/setLeaderStaleTerm
			// override this for subtests exercising the rule-support axis.
			Leader: store.NewPooler(&multiorchdatapb.PoolerHealthState{
				Multipooler: &clustermetadatapb.Multipooler{
					Id:       leaderID,
					ShardKey: shardKey,
					Hostname: "leader-host",
					PortMap:  map[string]int32{"postgres": 5432},
				},
				Status: &multipoolermanagerdatapb.Status{},
				ConsensusStatus: &clustermetadatapb.ConsensusStatus{
					Id:              leaderID,
					CurrentPosition: &clustermetadatapb.PoolerPosition{Position: &clustermetadatapb.RulePosition{Decision: decision}},
				},
			}, nil),
			Analyses: []*store.Pooler{
				freshFollower(follower1ID, now),
				freshFollower(follower2ID, now),
			},
		}
		for _, o := range overrides {
			o(sa)
		}
		return sa
	}

	// setLeaderLive marks the leader rider as observed-live (recent snapshot) or
	// not, replacing the old pre-baked LeaderPoolerReachable verdict. A live leader
	// is also marked initialized so it can participate in a recruitment quorum.
	setLeaderLive := func(sa *ShardAnalysis, live bool) {
		sa.Leader.Mutate(func(h *multiorchdatapb.PoolerHealthState) {
			if live {
				h.LastSeen = timestamppb.New(sa.Now)
				h.Status.IsInitialized = true
			} else {
				h.LastSeen = nil
			}
		})
	}

	// dropFollower removes a follower from Analyses, making it unreachable for
	// recruitment. Used to shrink the reachable set below a majority.
	dropFollower := func(sa *ShardAnalysis, id *clustermetadatapb.ID) {
		kept := sa.Analyses[:0:0]
		for _, pa := range sa.Analyses {
			if poolerID(pa).Name != id.Name {
				kept = append(kept, pa)
			}
		}
		sa.Analyses = kept
	}

	// setRuleCreatedNow marks the leadership rule as freshly created, so the
	// promotion grace window is in effect.
	setRuleCreatedNow := func(sa *ShardAnalysis) {
		sa.HighestPosition.Decision.CreationTime = timestamppb.New(sa.Now)
	}

	// setRuleUndecided models a genuine leadership transition still in
	// flight: the proposal names leaderID as the new candidate (a bumped
	// RuleNumber, fresh CreationTime), while the decision -- the last
	// locally-confirmed rule -- still names a distinct prior leader. A
	// promotion to leaderID that's genuinely undecided can't already be
	// reflected in the decision; if it were, it would BE decided.
	// commonconsensus.IsRuleDecided(sa.HighestPosition) is false throughout,
	// simulating a leader whose promotion hasn't yet demonstrated a
	// quorum-acked commit (the finalize/decide commit itself is
	// quorum-gated, so it can't complete while a required standby is still
	// catching up).
	setRuleUndecided := func(sa *ShardAnalysis) {
		decision := sa.HighestPosition.Decision
		proposal := proto.Clone(decision).(*clustermetadatapb.ShardRule) // still names leaderID here
		proposal.RuleNumber = &clustermetadatapb.RuleNumber{CoordinatorTerm: 2}
		proposal.CreationTime = timestamppb.New(sa.Now)
		sa.HighestPosition.Proposal = proposal

		decision.LeaderId = priorLeaderID
		decision.RuleNumber = &clustermetadatapb.RuleNumber{CoordinatorTerm: 1}
	}

	// setLeaderAcceptedPromotion records the in-flight proposal on the leader's
	// own report, as Promote does (via RecordTermPrimary) before its
	// quorum-gated rule write. Use after setRuleUndecided.
	setLeaderAcceptedPromotion := func(sa *ShardAnalysis) {
		sa.Leader.Mutate(func(h *multiorchdatapb.PoolerHealthState) {
			h.ConsensusStatus.ReplicationPrimary = &clustermetadatapb.ReplicationPrimary{
				Position: proto.Clone(sa.HighestPosition).(*clustermetadatapb.RulePosition),
			}
		})
	}

	// setLeaderPGRunning / setLeaderLastReady / setLeaderPromoting drive the
	// leader's postgres state on its rider, replacing the removed shard-level
	// verdict fields (now derived inside LeaderNeedsReplacementAnalyzer).
	setLeaderPGRunning := func(sa *ShardAnalysis, running bool) {
		sa.Leader.Mutate(func(h *multiorchdatapb.PoolerHealthState) { h.Status.PostgresRunning = running })
	}
	setLeaderPGReady := func(sa *ShardAnalysis, ready bool) {
		sa.Leader.Mutate(func(h *multiorchdatapb.PoolerHealthState) { h.Status.PostgresReady = ready })
	}
	setLeaderLastReady := func(sa *ShardAnalysis, at time.Time) {
		sa.Leader.Mutate(func(h *multiorchdatapb.PoolerHealthState) { h.LastPostgresReadyTime = timestamppb.New(at) })
	}
	setLeaderPromoting := func(sa *ShardAnalysis) {
		sa.Leader.Mutate(func(h *multiorchdatapb.PoolerHealthState) {
			h.Status.PostgresStatus = multipoolermanagerdatapb.PostgresStatus_POSTGRES_STATUS_PROMOTING
		})
	}

	// setLeaderPGStandby marks the leader's postgres as genuinely in recovery (a
	// STANDBY: pg_is_in_recovery() = true), i.e. a rule-designated leader whose
	// postgres never left recovery and cannot accept writes.
	setLeaderPGStandby := func(sa *ShardAnalysis) {
		sa.Leader.Mutate(func(h *multiorchdatapb.PoolerHealthState) {
			h.Status.PostgresStatus = multipoolermanagerdatapb.PostgresStatus_POSTGRES_STATUS_STANDBY
		})
	}

	// setLeaderResigned marks the leader as voluntarily wanting replacement via its
	// AvailabilityStatus (cohort-eligibility INELIGIBLE), which LeaderNeedsReplacement
	// treats as a resignation.
	setLeaderResigned := func(sa *ShardAnalysis) {
		sa.Leader.Mutate(func(h *multiorchdatapb.PoolerHealthState) {
			h.AvailabilityStatus = &clustermetadatapb.AvailabilityStatus{
				CohortEligibilityStatus: &clustermetadatapb.CohortEligibilityStatus{
					Signal: clustermetadatapb.CohortEligibilitySignal_COHORT_ELIGIBILITY_SIGNAL_INELIGIBLE,
				},
			}
		})
	}

	// connectReplica gives every follower in Analyses a fresh rider that is actively
	// streaming from the leader, so each one vouches for the leader (and
	// allFollowersStreaming sees a live connection). Replaces the old pre-baked
	// ReplicasConnectedToLeader verdict.
	connectReplica := func(sa *ShardAnalysis) {
		for i, pa := range sa.Analyses {
			id := poolerID(pa)
			sa.Analyses[i] = store.NewPooler(&multiorchdatapb.PoolerHealthState{
				Multipooler: &clustermetadatapb.Multipooler{Id: id, ShardKey: shardKey},
				LastSeen:    timestamppb.New(sa.Now),
				Status: &multipoolermanagerdatapb.Status{
					IsInitialized: true,
					ReplicationStatus: &multipoolermanagerdatapb.StandbyReplicationStatus{
						PrimaryConnInfo:    &multipoolermanagerdatapb.PrimaryConnInfo{Host: "leader-host", Port: 5432},
						LastReceiveLsn:     "0/1",
						WalReceiverStatus:  "streaming",
						LastMsgReceiveTime: timestamppb.New(sa.Now),
					},
				},
				ConsensusStatus: &clustermetadatapb.ConsensusStatus{
					Id:              id,
					CurrentPosition: &clustermetadatapb.PoolerPosition{Lsn: "0/1"},
				},
			}, nil)
		}
	}

	// setQuorumCommitTs stamps a follower's replication status with a
	// quorum_commit_ts, as if its heartbeat reader had observed one.
	setQuorumCommitTs := func(sa *ShardAnalysis, id *clustermetadatapb.ID, at time.Time) {
		for _, pa := range sa.Analyses {
			if poolerID(pa).Name != id.Name {
				continue
			}
			pa.Mutate(func(h *multiorchdatapb.PoolerHealthState) {
				if h.Status.ReplicationStatus == nil {
					h.Status.ReplicationStatus = &multipoolermanagerdatapb.StandbyReplicationStatus{}
				}
				h.Status.ReplicationStatus.QuorumCommitTs = timestamppb.New(at)
			})
		}
	}

	// setQuorumCommitFresh stamps a just-committed watermark on follower1's
	// report: the proof of progress a healthy shard always carries.
	setQuorumCommitFresh := func(sa *ShardAnalysis) {
		setQuorumCommitTs(sa, follower1ID, sa.Now)
	}

	// setLeaderQuorumCommitTs stamps the leader's own PrimaryStatus with a
	// quorum_commit_ts, as if its heartbeat writer had observed one directly
	// (the first-hand signal, vs. setQuorumCommitTs's cohort-observed one).
	setLeaderQuorumCommitTs := func(sa *ShardAnalysis, at time.Time) {
		sa.Leader.Mutate(func(h *multiorchdatapb.PoolerHealthState) {
			if h.Status.PrimaryStatus == nil {
				h.Status.PrimaryStatus = &multipoolermanagerdatapb.PrimaryStatus{}
			}
			h.Status.PrimaryStatus.QuorumCommitTs = timestamppb.New(at)
		})
	}

	// setLastReceiveLsnAdvance stamps a follower's replication status with a
	// last_receive_lsn_advance_time, as if its heartbeat reader had observed
	// raw WAL streaming progress (distinct from setQuorumCommitTs's
	// quorum-proven signal).
	setLastReceiveLsnAdvance := func(sa *ShardAnalysis, id *clustermetadatapb.ID, at time.Time) {
		for _, pa := range sa.Analyses {
			if poolerID(pa).Name != id.Name {
				continue
			}
			pa.Mutate(func(h *multiorchdatapb.PoolerHealthState) {
				if h.Status.ReplicationStatus == nil {
					h.Status.ReplicationStatus = &multipoolermanagerdatapb.StandbyReplicationStatus{}
				}
				h.Status.ReplicationStatus.LastReceiveLsnAdvanceTime = timestamppb.New(at)
			})
		}
	}

	// setPrimaryConnInfo stamps a follower's replication status with the
	// host:port it is configured to stream from -- may or may not be the
	// candidate leader's.
	setPrimaryConnInfo := func(sa *ShardAnalysis, id *clustermetadatapb.ID, host string, port int32) {
		for _, pa := range sa.Analyses {
			if poolerID(pa).Name != id.Name {
				continue
			}
			pa.Mutate(func(h *multiorchdatapb.PoolerHealthState) {
				if h.Status.ReplicationStatus == nil {
					h.Status.ReplicationStatus = &multipoolermanagerdatapb.StandbyReplicationStatus{}
				}
				h.Status.ReplicationStatus.PrimaryConnInfo = &multipoolermanagerdatapb.PrimaryConnInfo{Host: host, Port: port}
			})
		}
	}

	// cutOffCandidate builds a fresh, initialized follower configured to follow
	// ruleLeader (primary_conninfo connHost:connPort), with a rule of the given
	// creation time, not streaming — a parametrizable base for exercising each
	// classifyFollowerToLeader guard. cutOffFollower is the fully-cut-off case.
	cutOffCandidate := func(id, ruleLeader *clustermetadatapb.ID, ruleCreated time.Time, connHost string, connPort int32, now time.Time) *store.Pooler {
		return newRider(&multiorchdatapb.PoolerHealthState{
			Multipooler: &clustermetadatapb.Multipooler{Id: id, ShardKey: shardKey},
			LastSeen:    timestamppb.New(now),
			ConsensusStatus: &clustermetadatapb.ConsensusStatus{
				CurrentPosition: &clustermetadatapb.PoolerPosition{
					Position: &clustermetadatapb.RulePosition{Decision: &clustermetadatapb.ShardRule{
						LeaderId:      ruleLeader,
						CohortMembers: []*clustermetadatapb.ID{leaderID, follower1ID, follower2ID},
						CreationTime:  timestamppb.New(ruleCreated),
					}},
				},
			},
			Status: &multipoolermanagerdatapb.Status{
				IsInitialized: true,
				ReplicationStatus: &multipoolermanagerdatapb.StandbyReplicationStatus{
					PrimaryConnInfo:   &multipoolermanagerdatapb.PrimaryConnInfo{Host: connHost, Port: connPort},
					WalReceiverStatus: "", // not streaming
				},
			},
		})
	}

	// cutOffFollower is the fully-cut-off case: knows the leader (rule an hour old,
	// past the connect grace), pointed at it, not streaming → in the revocation set.
	cutOffFollower := func(id *clustermetadatapb.ID, now time.Time) *store.Pooler {
		return cutOffCandidate(id, leaderID, now.Add(-time.Hour), "leader-host", 5432, now)
	}

	// cutOffAllFollowers replaces the base fixture's bare followers with ones that
	// positively testify they are cut off from the leader, so the revocation set is
	// durability-sufficient and the leader is convicted.
	cutOffAllFollowers := func(sa *ShardAnalysis) {
		sa.Analyses = []*store.Pooler{cutOffFollower(follower1ID, sa.Now), cutOffFollower(follower2ID, sa.Now)}
	}

	t.Run("progress is impossible: durable consensus facts act at once", func(t *testing.T) {
		t.Run("resigned leader takes precedence over liveness", func(t *testing.T) {
			// A resigned leader emits ProblemLeaderResigned regardless of liveness —
			// even a dead leader is reported as resigned (voluntary), and immediately,
			// without the follower-streaming suppression the dead path applies.
			sa := deadLeaderShardAnalysis(func(sa *ShardAnalysis) {
				setLeaderResigned(sa)
			})

			problems, err := analyzer.Analyze(sa)
			require.NoError(t, err)
			require.Len(t, problems, 1)
			require.Equal(t, types.ProblemLeaderResigned, problems[0].Code)
			require.Equal(t, analyzer.Name(), problems[0].CheckName)
			require.Equal(t, types.ScopeShard, problems[0].Scope)
			require.Equal(t, leaderID, problems[0].PoolerID)
		})

		t.Run("resigned leader reported even when otherwise live and connected", func(t *testing.T) {
			sa := deadLeaderShardAnalysis(func(sa *ShardAnalysis) {
				setLeaderLive(sa, true)
				setLeaderPGReady(sa, true)
				connectReplica(sa)
				setLeaderResigned(sa)
			})

			problems, err := analyzer.Analyze(sa)
			require.NoError(t, err)
			require.Len(t, problems, 1)
			require.Equal(t, types.ProblemLeaderResigned, problems[0].Code)
		})

		t.Run("shutdown tombstone drives failover when the leader is gone from cache", func(t *testing.T) {
			// The leader has fully shut down: evicted from the live cache (sa.Leader nil)
			// but recorded as a SHUTDOWN tombstone. With the ephemeral resignation broadcast
			// lost, the durable tombstone still drives the failover — leaderID comes from the
			// shard rule, so we act with no cached leader.
			sa := deadLeaderShardAnalysis(func(sa *ShardAnalysis) {
				sa.Leader = nil
				sa.TombstoneIDs = map[topoclient.ComponentID]struct{}{
					topoclient.ComponentIDString(leaderID): {},
				}
			})

			problems, err := analyzer.Analyze(sa)
			require.NoError(t, err)
			require.Len(t, problems, 1)
			require.Equal(t, types.ProblemLeaderResigned, problems[0].Code)
			require.Equal(t, leaderID, problems[0].PoolerID)
		})

		// Regression: term comparison alone is not enough. Here the leader's own
		// report already matches the shard's highest known term and names itself
		// — but it has ALSO accepted a newer revocation with no successor decided
		// or gossiped anywhere yet (a recruit that revoked the old leader, then
		// stalled before establishing a new one). commonconsensus.IsActiveLeader's
		// revocation check catches this; a bare term comparison would not, since
		// no cohort member's report shows a higher term to compare against.
		t.Run("detects a leader that self-revoked with no successor decided yet", func(t *testing.T) {
			sa := deadLeaderShardAnalysis(func(sa *ShardAnalysis) {
				setLeaderLive(sa, true)
				setLeaderPGReady(sa, true)
				sa.Leader.Mutate(func(h *multiorchdatapb.PoolerHealthState) {
					h.ConsensusStatus.TermRevocation = &clustermetadatapb.TermRevocation{
						RevokedBelowTerm: 2,
						OutgoingRule:     &clustermetadatapb.RuleNumber{CoordinatorTerm: 1},
					}
				})
			})

			problems, err := analyzer.Analyze(sa)
			require.NoError(t, err)
			require.Len(t, problems, 1, "a self-revoked leader must be convicted even with no higher term known anywhere else")
			require.Equal(t, types.ProblemLeaderNotSelfConfirmed, problems[0].Code)
		})

		// Regression: a staging incident where a recruit round reached quorum and
		// decided a new leader via the OTHER cohort members' SetPrimary, but the
		// Promote RPC to the designated leader itself was lost — so it kept
		// running as an ordinary, healthy-looking standby forever, and orch never
		// noticed no one was actually primary.
		t.Run("detects a leader that was recorded but never actually promoted", func(t *testing.T) {
			sa := deadLeaderShardAnalysis(func(sa *ShardAnalysis) {
				setLeaderLive(sa, true)
				setLeaderPGReady(sa, true)
				// The leader's own report never caught up: it still names the OLD
				// leader (follower1) at the OLD term, even though HighestPosition
				// (fed by the other cohort members' SetPrimary) has moved on to
				// leaderID at term 2.
				sa.HighestPosition.Decision.RuleNumber = &clustermetadatapb.RuleNumber{CoordinatorTerm: 2}
				sa.Leader.Mutate(func(h *multiorchdatapb.PoolerHealthState) {
					h.ConsensusStatus.CurrentPosition.Position.Decision = &clustermetadatapb.ShardRule{
						LeaderId:   follower1ID,
						RuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: 1},
					}
				})
			})

			problems, err := analyzer.Analyze(sa)
			require.NoError(t, err)
			require.Len(t, problems, 1, "a live, postgres-ready leader that never confirmed its own promotion must still be convicted")
			require.Equal(t, types.ProblemLeaderNotSelfConfirmed, problems[0].Code)
		})

		// Regression: leader participation is a mandatory precondition, not one
		// vote among many. Writes only ever flow through the leader, so even a
		// durability-sufficient set of genuinely vouching followers cannot make up
		// for a leader that never confirmed its own promotion.
		t.Run("convicts a never-promoted leader even when followers are genuinely vouching for it", func(t *testing.T) {
			sa := deadLeaderShardAnalysis(func(sa *ShardAnalysis) {
				setLeaderLive(sa, true)
				setLeaderPGReady(sa, true)
				connectReplica(sa) // both followers genuinely stream from the leader
				sa.HighestPosition.Decision.RuleNumber = &clustermetadatapb.RuleNumber{CoordinatorTerm: 2}
				sa.Leader.Mutate(func(h *multiorchdatapb.PoolerHealthState) {
					h.ConsensusStatus.CurrentPosition.Position.Decision = &clustermetadatapb.ShardRule{
						LeaderId:   follower1ID,
						RuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: 1},
					}
				})
			})

			problems, err := analyzer.Analyze(sa)
			require.NoError(t, err)
			require.Len(t, problems, 1, "a leader that never confirmed its own promotion must be convicted even if followers vouch for it")
			require.Equal(t, types.ProblemLeaderNotSelfConfirmed, problems[0].Code)
		})

		t.Run("does not convict a leader whose report trails a just-created rule", func(t *testing.T) {
			// A leader whose own report hasn't caught up to a just-created rule is
			// not convicted as NotSelfConfirmed: its report may trail its followers'.
			sa := deadLeaderShardAnalysis(func(sa *ShardAnalysis) {
				setLeaderLive(sa, true)
				setLeaderPGReady(sa, true)
				setQuorumCommitFresh(sa)
				setRuleCreatedNow(sa)
				sa.Leader.Mutate(func(h *multiorchdatapb.PoolerHealthState) {
					h.ConsensusStatus.CurrentPosition.Position.Decision = &clustermetadatapb.ShardRule{
						LeaderId: follower1ID,
					}
				})
			})

			problems, err := analyzer.Analyze(sa)
			require.NoError(t, err)
			require.Empty(t, problems, "a leader within the connect grace must not be convicted yet")
		})

		t.Run("rule support is judged before the in-recovery guard", func(t *testing.T) {
			// A standby leader that never confirmed its own promotion fails the
			// rule-support axis first, so it is LeaderNotSelfConfirmed, not LeaderUnhealthy.
			sa := deadLeaderShardAnalysis(func(sa *ShardAnalysis) {
				setLeaderLive(sa, true)
				setLeaderPGReady(sa, true)
				setLeaderPGStandby(sa)
				sa.HighestPosition.Decision.RuleNumber = &clustermetadatapb.RuleNumber{CoordinatorTerm: 2}
				sa.Leader.Mutate(func(h *multiorchdatapb.PoolerHealthState) {
					h.ConsensusStatus.CurrentPosition.Position.Decision = &clustermetadatapb.ShardRule{
						LeaderId:   follower1ID,
						RuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: 1},
					}
				})
			})

			problems, err := analyzer.Analyze(sa)
			require.NoError(t, err)
			require.Len(t, problems, 1)
			require.Equal(t, types.ProblemLeaderNotSelfConfirmed, problems[0].Code)
		})

		t.Run("counts a self-revoked follower as cut off even though it still appears to stream", func(t *testing.T) {
			// Revocation must win over streaming evidence: a follower that reports
			// itself self-revoked but still looks like it's streaming (recruit
			// presumably failed to stop its WAL receiver) must not vouch for the
			// leader. Both followers this way → durability-sufficient revocation.
			selfRevokedButStreaming := func(id *clustermetadatapb.ID, now time.Time) *store.Pooler {
				return newRider(&multiorchdatapb.PoolerHealthState{
					Multipooler: &clustermetadatapb.Multipooler{Id: id, ShardKey: shardKey},
					LastSeen:    timestamppb.New(now),
					ConsensusStatus: &clustermetadatapb.ConsensusStatus{
						Id: id,
						CurrentPosition: &clustermetadatapb.PoolerPosition{
							Position: &clustermetadatapb.RulePosition{Decision: &clustermetadatapb.ShardRule{
								LeaderId:   leaderID,
								RuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: 1},
							}},
						},
						TermRevocation: &clustermetadatapb.TermRevocation{
							RevokedBelowTerm: 2,
							OutgoingRule:     &clustermetadatapb.RuleNumber{CoordinatorTerm: 1},
						},
					},
					Status: &multipoolermanagerdatapb.Status{
						IsInitialized: true,
						ReplicationStatus: &multipoolermanagerdatapb.StandbyReplicationStatus{
							PrimaryConnInfo:    &multipoolermanagerdatapb.PrimaryConnInfo{Host: "leader-host", Port: 5432},
							LastReceiveLsn:     "0/1",
							WalReceiverStatus:  "streaming",
							LastMsgReceiveTime: timestamppb.New(now),
						},
					},
				})
			}
			sa := deadLeaderShardAnalysis(func(sa *ShardAnalysis) {
				sa.Analyses = []*store.Pooler{selfRevokedButStreaming(follower1ID, sa.Now), selfRevokedButStreaming(follower2ID, sa.Now)}
			})

			problems, err := analyzer.Analyze(sa)
			require.NoError(t, err)
			require.Len(t, problems, 1, "a self-revoked follower must not vouch for the leader just because it appears to stream")
			require.Equal(t, types.ProblemLeaderLacksCohortSupport, problems[0].Code)
		})
	})

	t.Run("blockers: states that would stop writes, and how long they wait", func(t *testing.T) {
		t.Run("fails over a live, pg_isready leader whose postgres is a standby (in recovery)", func(t *testing.T) {
			// The consensus rule names this pooler leader, but its postgres is a STANDBY
			// (pg_is_in_recovery = true) — it answers pg_isready fine, yet cannot accept
			// writes. It must NOT be judged a healthy serving primary (which would emit only
			// an alert-only ShardAtRisk); it must route to failover so a real primary is
			// promoted. Covers the healthy fast-path (postgres_ready = true).
			sa := deadLeaderShardAnalysis(func(sa *ShardAnalysis) {
				setLeaderLive(sa, true)
				setLeaderPGReady(sa, true) // a hot standby passes pg_isready
				setLeaderPGStandby(sa)     // but postgres is in recovery
			})

			problems, err := analyzer.Analyze(sa)
			require.NoError(t, err)
			require.Len(t, problems, 1)
			require.Equal(t, types.ProblemLeaderUnhealthy, problems[0].Code,
				"a leader whose postgres is in recovery is not a healthy primary")
			require.Equal(t, types.ScopeShard, problems[0].Scope)
			require.Equal(t, leaderID, problems[0].PoolerID)
		})

		t.Run("fails over an in-recovery leader even when its postgres recently reported ready", func(t *testing.T) {
			// A standby keeps postgres_ready true continuously, so its LastPostgresReadyTime
			// stays fresh; the in-recovery blocker must still convict it at once.
			sa := deadLeaderShardAnalysis(func(sa *ShardAnalysis) {
				setLeaderLive(sa, true)
				setLeaderPGRunning(sa, true) // process alive
				setLeaderPGReady(sa, false)
				setLeaderLastReady(sa, time.Now().Add(-5*time.Second)) // recently ready
				setLeaderPGStandby(sa)                                 // but postgres is in recovery
			})

			problems, err := analyzer.Analyze(sa)
			require.NoError(t, err)
			require.Len(t, problems, 1)
			require.Equal(t, types.ProblemLeaderUnhealthy, problems[0].Code,
				"a recent ready report must not mask a standby leader")
			require.Equal(t, leaderID, problems[0].PoolerID)
		})

		t.Run("triggers failover when leader pooler up but postgres down", func(t *testing.T) {
			sa := deadLeaderShardAnalysis(func(sa *ShardAnalysis) {
				setLeaderLive(sa, true) // Pooler is up
				// LeaderReachable remains false (postgres down)
			})

			problems, err := analyzer.Analyze(sa)
			require.NoError(t, err)
			require.Len(t, problems, 1)
			require.Equal(t, types.ProblemLeaderUnhealthy, problems[0].Code)
			require.Equal(t, leaderID, problems[0].PoolerID)
		})

		t.Run("triggers failover when pooler up but postgres process dead (SIGKILL), replicas still connected", func(t *testing.T) {
			sa := deadLeaderShardAnalysis(func(sa *ShardAnalysis) {
				setLeaderLive(sa, true)       // Pooler is up and reachable
				setLeaderPGReady(sa, false)   // Postgres not accepting connections
				setLeaderPGRunning(sa, false) // Process is dead (SIGKILL)
				connectReplica(sa)            // Replicas still appear connected (TCP keepalive not yet fired)
				setLeaderLastReady(sa, time.Now().Add(-5*time.Second))
			})

			problems, err := analyzer.Analyze(sa)
			require.NoError(t, err)
			require.Len(t, problems, 1, "should trigger failover when postgres process is dead even if replicas still appear connected")
			require.Equal(t, types.ProblemLeaderUnhealthy, problems[0].Code)
		})

		t.Run("triggers failover when pooler reachable but postgres process alive and unresponsive beyond threshold", func(t *testing.T) {
			// The pooler is reachable and reports the postgres process is alive but not
			// accepting connections (pg_isready failing). Because we can reach the pooler,
			// we trust its direct signal: suppress only while postgres responded recently,
			// then allow failover once the response threshold lapses so a wedged postgres
			// cannot block failover forever.
			sa := deadLeaderShardAnalysis(func(sa *ShardAnalysis) {
				setLeaderLive(sa, true)
				setLeaderPGRunning(sa, true)                            // process exists
				setLeaderPGReady(sa, false)                             // but wedged (pg_isready fails)
				connectReplica(sa)                                      // replicas still appear connected
				setLeaderLastReady(sa, time.Now().Add(-60*time.Second)) // beyond 30s default threshold
			})

			problems, err := analyzer.Analyze(sa)
			require.NoError(t, err)
			require.Len(t, problems, 1, "should fail over when a reachable postgres has been unresponsive past the threshold")
			require.Equal(t, types.ProblemLeaderUnhealthy, problems[0].Code)
		})

		t.Run("ignores when pooler up, postgres starting (replicas connected, recent timestamp)", func(t *testing.T) {
			sa := deadLeaderShardAnalysis(func(sa *ShardAnalysis) {
				setLeaderLive(sa, true)      // Pooler is up
				setLeaderPGReady(sa, false)  // Postgres not yet accepting connections
				setLeaderPGRunning(sa, true) // But process exists (starting up or SIGSTOP'd)
				connectReplica(sa)           // Replicas still connected via streaming replication
				setQuorumCommitFresh(sa)
				setLeaderLastReady(sa, time.Now().Add(-5*time.Second))
			})

			problems, err := analyzer.Analyze(sa)
			require.NoError(t, err)
			require.Empty(t, problems, "should not trigger failover when postgres is starting but replicas are connected and timestamp is recent")
		})

		t.Run("a blocker waits while commits stalled for less than its patience", func(t *testing.T) {
			// Postgres is running but not ready (starting or wedged) and the last commit
			// was 5s ago: within PostgresUnreadyPatience, so it may still recover.
			sa := deadLeaderShardAnalysis(func(sa *ShardAnalysis) {
				setLeaderLive(sa, true)
				setLeaderPGRunning(sa, true)
				setLeaderPGReady(sa, false)
				setQuorumCommitTs(sa, follower1ID, sa.Now.Add(-5*time.Second))
			})

			problems, err := analyzer.Analyze(sa)
			require.NoError(t, err)
			require.Empty(t, problems)
		})

		t.Run("a blocker convicts sooner than the full stale window once its patience is exceeded", func(t *testing.T) {
			// Commits stalled 15s: past PostgresUnreadyPatience (10s) but still inside
			// QuorumCommitStaleAfter (20s), so only the blocker convicts.
			sa := deadLeaderShardAnalysis(func(sa *ShardAnalysis) {
				setLeaderLive(sa, true)
				setLeaderPGRunning(sa, true)
				setLeaderPGReady(sa, false)
				setQuorumCommitTs(sa, follower1ID, sa.Now.Add(-15*time.Second))
			})

			problems, err := analyzer.Analyze(sa)
			require.NoError(t, err)
			require.Len(t, problems, 1)
			require.Equal(t, types.ProblemLeaderUnhealthy, problems[0].Code)
		})

		t.Run("without a blocker the full stale window applies", func(t *testing.T) {
			sa := deadLeaderShardAnalysis(func(sa *ShardAnalysis) {
				setLeaderLive(sa, true)
				setLeaderPGReady(sa, true)
				setQuorumCommitTs(sa, follower1ID, sa.Now.Add(-15*time.Second))
			})

			problems, err := analyzer.Analyze(sa)
			require.NoError(t, err)
			require.Empty(t, problems, "15s is inside QuorumCommitStaleAfter, and nothing blocks writes")
		})

		t.Run("disconnected followers wait longer than an unready postgres", func(t *testing.T) {
			// Postgres can be slow to connect; convicting while replication is still
			// being set up would interrupt it. 12s is past PostgresUnreadyPatience (10s)
			// but inside FollowerDisconnectPatience (15s).
			sa := deadLeaderShardAnalysis(cutOffAllFollowers, func(sa *ShardAnalysis) {
				setLeaderLive(sa, true)
				setLeaderPGReady(sa, true)
				setQuorumCommitTs(sa, follower1ID, sa.Now.Add(-12*time.Second))
			})

			problems, err := analyzer.Analyze(sa)
			require.NoError(t, err)
			require.Empty(t, problems)
		})

		t.Run("disconnected followers convict once commits stall past FollowerDisconnectPatience", func(t *testing.T) {
			sa := deadLeaderShardAnalysis(cutOffAllFollowers, func(sa *ShardAnalysis) {
				setLeaderLive(sa, true)
				setLeaderPGReady(sa, true)
				setQuorumCommitTs(sa, follower1ID, sa.Now.Add(-16*time.Second))
			})

			problems, err := analyzer.Analyze(sa)
			require.NoError(t, err)
			require.Len(t, problems, 1)
			require.Equal(t, types.ProblemLeaderUnreachableByCohort, problems[0].Code)
		})

		t.Run("a fresh commit overrides a blocker", func(t *testing.T) {
			// Every follower looks disconnected, yet a quorum acknowledged a write
			// moments ago: the blocker was wrong, or already recovered.
			sa := deadLeaderShardAnalysis(cutOffAllFollowers, func(sa *ShardAnalysis) {
				setLeaderLive(sa, true)
				setLeaderPGReady(sa, true)
				setQuorumCommitFresh(sa)
			})

			problems, err := analyzer.Analyze(sa)
			require.NoError(t, err)
			require.Empty(t, problems)
		})

		t.Run("a dead postgres convicts at once, even with a fresh commit", func(t *testing.T) {
			// The watermark may be up to QuorumCommitStaleAfter old, so it cannot be
			// allowed to delay a failover the leader's own report already justifies.
			sa := deadLeaderShardAnalysis(func(sa *ShardAnalysis) {
				setLeaderLive(sa, true)
				setLeaderPGRunning(sa, false)
				setLeaderPGReady(sa, false)
				setQuorumCommitFresh(sa)
			})

			problems, err := analyzer.Analyze(sa)
			require.NoError(t, err)
			require.Len(t, problems, 1)
			require.Equal(t, types.ProblemLeaderUnhealthy, problems[0].Code)
		})

		t.Run("in-recovery guard takes precedence over a stale quorum-commit watermark", func(t *testing.T) {
			sa := deadLeaderShardAnalysis(func(sa *ShardAnalysis) {
				setLeaderLive(sa, true)
				setLeaderPGReady(sa, true)
				setLeaderPGStandby(sa)
				setQuorumCommitTs(sa, follower1ID, sa.Now.Add(-sa.Policy.QuorumCommitStaleAfter-time.Second))
			})

			problems, err := analyzer.Analyze(sa)
			require.NoError(t, err)
			require.Len(t, problems, 1)
			require.Equal(t, types.ProblemLeaderUnhealthy, problems[0].Code,
				"the leader-fitness guard fires before the quorum-commit backstop")
		})

		t.Run("detects dead leader (cohort confirms it is cut off)", func(t *testing.T) {
			sa := deadLeaderShardAnalysis(cutOffAllFollowers)

			problems, err := analyzer.Analyze(sa)
			require.NoError(t, err)
			require.Len(t, problems, 1)
			problem := problems[0]
			require.Equal(t, types.ProblemLeaderUnreachableByCohort, problem.Code)
			require.Equal(t, types.ScopeShard, problem.Scope)
			require.Equal(t, types.PriorityEmergency, problem.Priority)
			require.Equal(t, leaderID, problem.PoolerID)
			require.NotNil(t, problem.RecoveryAction)
		})

		t.Run("triggers failover when both pooler and replicas disconnected", func(t *testing.T) {
			sa := deadLeaderShardAnalysis(func(sa *ShardAnalysis) {
				setLeaderLive(sa, false)
			}, cutOffAllFollowers)

			problems, err := analyzer.Analyze(sa)
			require.NoError(t, err)
			require.Len(t, problems, 1)
			require.Equal(t, types.ProblemLeaderUnreachableByCohort, problems[0].Code)
			require.Equal(t, leaderID, problems[0].PoolerID)
		})

		t.Run("still confirms leader is dead via a replica with a momentary connectivity blip", func(t *testing.T) {
			// StreamConnected false (e.g. a stream reconnect) must not hide an
			// otherwise fresh, initialized, conclusively-cut-off replica from
			// recruitableCohort/classifyFollowerToLeader — cutoff evidence is judged on
			// observation freshness (HealthWithin), not on whether the health stream
			// happens to be connected at this instant.
			sa := deadLeaderShardAnalysis(cutOffAllFollowers, func(sa *ShardAnalysis) {
				sa.Analyses[0].Mutate(func(h *multiorchdatapb.PoolerHealthState) { h.StreamConnected = false })
			})

			problems, err := analyzer.Analyze(sa)
			require.NoError(t, err)
			require.Len(t, problems, 1)
			require.Equal(t, types.ProblemLeaderUnreachableByCohort, problems[0].Code)
		})

		t.Run("counts a follower that learned the leader via its replication primary as cut off", func(t *testing.T) {
			// The follower's own WAL position doesn't name the leader, but its
			// ReplicationPrimary does — its highest-known rule still names the leader, so a
			// configured-but-not-streaming follower is cut off. Both followers this way →
			// durability-sufficient revocation → failover.
			viaReplPrimary := func(id *clustermetadatapb.ID, now time.Time) *store.Pooler {
				return newRider(&multiorchdatapb.PoolerHealthState{
					Multipooler: &clustermetadatapb.Multipooler{Id: id, ShardKey: shardKey},
					LastSeen:    timestamppb.New(now),
					ConsensusStatus: &clustermetadatapb.ConsensusStatus{
						Id:              id,
						CurrentPosition: &clustermetadatapb.PoolerPosition{Lsn: "0/1"},
						ReplicationPrimary: &clustermetadatapb.ReplicationPrimary{
							Position: &clustermetadatapb.RulePosition{Decision: &clustermetadatapb.ShardRule{
								RuleNumber:    &clustermetadatapb.RuleNumber{CoordinatorTerm: 1},
								LeaderId:      leaderID,
								CohortMembers: []*clustermetadatapb.ID{leaderID, follower1ID, follower2ID},
								CreationTime:  timestamppb.New(now.Add(-time.Hour)),
							}},
						},
					},
					Status: &multipoolermanagerdatapb.Status{
						IsInitialized: true,
						ReplicationStatus: &multipoolermanagerdatapb.StandbyReplicationStatus{
							PrimaryConnInfo:   &multipoolermanagerdatapb.PrimaryConnInfo{Host: "leader-host", Port: 5432},
							WalReceiverStatus: "", // not streaming
						},
					},
				})
			}
			sa := deadLeaderShardAnalysis(func(sa *ShardAnalysis) {
				sa.Analyses = []*store.Pooler{viaReplPrimary(follower1ID, sa.Now), viaReplPrimary(follower2ID, sa.Now)}
			})

			problems, err := analyzer.Analyze(sa)
			require.NoError(t, err)
			require.Len(t, problems, 1)
			require.Equal(t, types.ProblemLeaderUnreachableByCohort, problems[0].Code)
		})

		t.Run("does not count a follower still within the connect grace as cut off", func(t *testing.T) {
			// follower1 knows the leader but its rule was created just now (within the
			// connect grace) — not enough time to connect, so it does NOT testify. Only
			// follower2 is cut off → sub-quorum revocation set → the leader is kept.
			sa := deadLeaderShardAnalysis(func(sa *ShardAnalysis) {
				sa.Analyses = []*store.Pooler{
					cutOffCandidate(follower1ID, leaderID, sa.Now, "leader-host", 5432, sa.Now),
					cutOffFollower(follower2ID, sa.Now),
				}
				setQuorumCommitTs(sa, follower2ID, sa.Now)
			})

			problems, err := analyzer.Analyze(sa)
			require.NoError(t, err)
			require.Empty(t, problems, "a follower within the connect grace must not count toward revocation")
		})

		t.Run("does not count a follower whose rule names a different leader as cut off", func(t *testing.T) {
			// follower1's own rule names follower2, not the leader — it is uninformed of
			// the current term, so it does not testify against this leader.
			sa := deadLeaderShardAnalysis(func(sa *ShardAnalysis) {
				sa.Analyses = []*store.Pooler{
					cutOffCandidate(follower1ID, follower2ID, sa.Now.Add(-time.Hour), "leader-host", 5432, sa.Now),
					cutOffFollower(follower2ID, sa.Now),
				}
				setQuorumCommitTs(sa, follower2ID, sa.Now)
			})

			problems, err := analyzer.Analyze(sa)
			require.NoError(t, err)
			require.Empty(t, problems, "a follower uninformed of this leader must not count toward revocation")
		})

		t.Run("does not count a follower configured for a different primary as cut off", func(t *testing.T) {
			// follower1 is not pointed at the leader (primary_conninfo → other-host), so it
			// is not refusing THIS leader.
			sa := deadLeaderShardAnalysis(func(sa *ShardAnalysis) {
				sa.Analyses = []*store.Pooler{
					cutOffCandidate(follower1ID, leaderID, sa.Now.Add(-time.Hour), "other-host", 5432, sa.Now),
					cutOffFollower(follower2ID, sa.Now),
				}
				setQuorumCommitTs(sa, follower2ID, sa.Now)
			})

			problems, err := analyzer.Analyze(sa)
			require.NoError(t, err)
			require.Empty(t, problems, "a follower pointed at a different primary must not count toward revocation")
		})
	})

	t.Run("proof of progress: the quorum-commit watermark", func(t *testing.T) {
		t.Run("reports LeaderProgressUnproven for a healthy-looking leader with no watermark at all", func(t *testing.T) {
			// Liveness alone doesn't prove writes commit; no watermark is not proof.
			// The rule is an hour old, so the failover isn't deferred.
			sa := deadLeaderShardAnalysis(func(sa *ShardAnalysis) {
				setLeaderLive(sa, true)
				setLeaderPGReady(sa, true)
			})

			problems, err := analyzer.Analyze(sa)
			require.NoError(t, err)
			require.Len(t, problems, 1)
			require.Equal(t, types.ProblemLeaderProgressUnproven, problems[0].Code)
			require.False(t, problems[0].NotBefore.After(sa.Now), "an hour-old rule has had time to commit a watermark")
		})

		t.Run("ignores healthy leader with fresh quorum-commit watermark", func(t *testing.T) {
			sa := deadLeaderShardAnalysis(func(sa *ShardAnalysis) {
				setLeaderLive(sa, true)
				setLeaderPGReady(sa, true)
				setQuorumCommitTs(sa, follower1ID, sa.Now)
			})

			problems, err := analyzer.Analyze(sa)
			require.NoError(t, err)
			require.Empty(t, problems)
		})

		t.Run("LeaderProgressUnproven when quorum-commit watermark goes stale", func(t *testing.T) {
			sa := deadLeaderShardAnalysis(func(sa *ShardAnalysis) {
				setLeaderLive(sa, true)
				setLeaderPGReady(sa, true)
				setQuorumCommitTs(sa, follower1ID, sa.Now.Add(-sa.Policy.QuorumCommitStaleAfter-time.Second))
			})

			problems, err := analyzer.Analyze(sa)
			require.NoError(t, err)
			require.Len(t, problems, 1)
			require.Equal(t, types.ProblemLeaderProgressUnproven, problems[0].Code)
			require.Equal(t, leaderID, problems[0].PoolerID)
		})

		t.Run("a fresh non-cohort observer proves quorum is not stuck despite a stale cohort member", func(t *testing.T) {
			// quorum_commit_ts is a single leader-authored fact, not an independent
			// per-member value, so a witness outside the durability-required cohort
			// is still valid proof — this must look at ALL known shard members, not
			// just the cohort.
			sa := deadLeaderShardAnalysis(func(sa *ShardAnalysis) {
				setLeaderLive(sa, true)
				setLeaderPGReady(sa, true)
				setQuorumCommitTs(sa, follower1ID, sa.Now.Add(-sa.Policy.QuorumCommitStaleAfter-time.Second))
				sa.Analyses = append(sa.Analyses, freshFollower(observerID, sa.Now))
				setQuorumCommitTs(sa, observerID, sa.Now)
			})

			problems, err := analyzer.Analyze(sa)
			require.NoError(t, err)
			require.Empty(t, problems)
		})

		t.Run("prefers the leader's own first-hand quorum-commit report when reachable", func(t *testing.T) {
			sa := deadLeaderShardAnalysis(func(sa *ShardAnalysis) {
				setLeaderLive(sa, true)
				setLeaderPGReady(sa, true)
				setLeaderQuorumCommitTs(sa, sa.Now)                                                           // fresh, first-hand
				setQuorumCommitTs(sa, follower1ID, sa.Now.Add(-sa.Policy.QuorumCommitStaleAfter-time.Second)) // stale, cohort-observed
			})

			problems, err := analyzer.Analyze(sa)
			require.NoError(t, err)
			require.Empty(t, problems, "leader's own fresh first-hand report takes precedence over a stale cohort-observed one")
		})

		t.Run("LeaderProgressUnproven fires despite LSN still advancing once the rule is decided", func(t *testing.T) {
			// A DECIDED rule is itself proof a quorum-acked commit already
			// succeeded under this leadership (the finalize commit is quorum-gated
			// like any other write), so the backlog-draining excuse no longer
			// applies -- unlike the undecided case above, this must convict.
			sa := deadLeaderShardAnalysis(func(sa *ShardAnalysis) {
				setLeaderLive(sa, true)
				setLeaderPGReady(sa, true)
				setQuorumCommitTs(sa, follower1ID, sa.Now.Add(-sa.Policy.QuorumCommitStaleAfter-time.Second))
				setLastReceiveLsnAdvance(sa, follower1ID, sa.Now)
			})

			problems, err := analyzer.Analyze(sa)
			require.NoError(t, err)
			require.Len(t, problems, 1)
			require.Equal(t, types.ProblemLeaderProgressUnproven, problems[0].Code)
		})

		t.Run("LeaderProgressUnproven via cohort corroboration when quorum-commit watermark goes stale", func(t *testing.T) {
			sa := deadLeaderShardAnalysis(func(sa *ShardAnalysis) {
				setLeaderLive(sa, false)
				connectReplica(sa)
				setLeaderLastReady(sa, time.Now().Add(-5*time.Second))
				setQuorumCommitTs(sa, follower1ID, sa.Now.Add(-sa.Policy.QuorumCommitStaleAfter-time.Second))
			})

			problems, err := analyzer.Analyze(sa)
			require.NoError(t, err)
			require.Len(t, problems, 1)
			require.Equal(t, types.ProblemLeaderProgressUnproven, problems[0].Code)
		})

		t.Run("keeps the leader on cold start when followers relay a fresh watermark", func(t *testing.T) {
			// A freshly (re)started orch has fresh follower reports but hasn't observed
			// the leader or any streaming yet. Their relayed watermark is proof of
			// progress on its own, so there is nothing to act on.
			sa := deadLeaderShardAnalysis(setQuorumCommitFresh)

			problems, err := analyzer.Analyze(sa)
			require.NoError(t, err)
			require.Empty(t, problems, "cold start must not fail over a leader with a fresh watermark")
		})

		t.Run("keeps an unreachable leader when a follower relays a fresh watermark", func(t *testing.T) {
			// Leader pooler unreachable, only ONE of the two followers streaming, the
			// other's report silent. The streaming follower relays a fresh watermark,
			// which proves writes commit; one silent follower can't revoke the rule.
			sa := deadLeaderShardAnalysis(func(sa *ShardAnalysis) {
				sa.Analyses[0] = store.NewPooler(&multiorchdatapb.PoolerHealthState{
					Multipooler: &clustermetadatapb.Multipooler{Id: follower1ID, ShardKey: shardKey},
					LastSeen:    timestamppb.New(sa.Now),
					Status: &multipoolermanagerdatapb.Status{
						IsInitialized: true,
						ReplicationStatus: &multipoolermanagerdatapb.StandbyReplicationStatus{
							PrimaryConnInfo:    &multipoolermanagerdatapb.PrimaryConnInfo{Host: "leader-host", Port: 5432},
							LastReceiveLsn:     "0/1",
							WalReceiverStatus:  "streaming",
							LastMsgReceiveTime: timestamppb.New(sa.Now),
						},
					},
					ConsensusStatus: &clustermetadatapb.ConsensusStatus{
						Id:              follower1ID,
						CurrentPosition: &clustermetadatapb.PoolerPosition{Lsn: "0/1"},
					},
				}, nil)
				setQuorumCommitFresh(sa)
			})

			problems, err := analyzer.Analyze(sa)
			require.NoError(t, err)
			require.Empty(t, problems)
		})

		t.Run("ignores when leader pooler down but all replicas still connected to postgres", func(t *testing.T) {
			// The followers relay a fresh watermark, so writes provably commit even
			// though orch can't observe the leader's pooler directly.
			sa := deadLeaderShardAnalysis(func(sa *ShardAnalysis) {
				setLeaderLive(sa, false)
				connectReplica(sa)
				setQuorumCommitFresh(sa)
				setLeaderLastReady(sa, time.Now().Add(-5*time.Second)) // Responded recently
			})

			problems, err := analyzer.Analyze(sa)
			require.NoError(t, err)
			require.Empty(t, problems, "should not trigger failover when pooler is down but replicas are connected")
		})

		t.Run("ignores when leader pooler down but replicas connected (postgres still running, recent timestamp)", func(t *testing.T) {
			sa := deadLeaderShardAnalysis(func(sa *ShardAnalysis) {
				setLeaderLive(sa, false)                               // Pooler is down
				setLeaderPGReady(sa, false)                            // Unknown since pooler is down
				connectReplica(sa)                                     // But replicas are still connected to postgres
				setQuorumCommitFresh(sa)                               // and relay a fresh watermark
				setLeaderLastReady(sa, time.Now().Add(-5*time.Second)) // Responded recently (within 30s default threshold)
			})

			problems, err := analyzer.Analyze(sa)
			require.NoError(t, err)
			require.Empty(t, problems, "should not trigger failover when pooler is down but replicas are connected and postgres responded recently")
		})

		t.Run("suppresses failover when pooler unreachable but replicas connected, even with an expired postgres timestamp", func(t *testing.T) {
			// When the leader pooler is unreachable we cannot observe its postgres
			// directly, so the leader's own LastPostgresReadyTime is irrelevant. The
			// followers' relayed watermark is the proof that writes commit.
			sa := deadLeaderShardAnalysis(func(sa *ShardAnalysis) {
				setLeaderLive(sa, false)
				setLeaderPGReady(sa, false)
				connectReplica(sa)
				setQuorumCommitFresh(sa)
				setLeaderLastReady(sa, time.Now().Add(-60*time.Second)) // Older than 30s default threshold
			})

			problems, err := analyzer.Analyze(sa)
			require.NoError(t, err)
			require.Empty(t, problems, "a relayed fresh watermark proves progress; do not fail over")
		})

		t.Run("suppresses failover when pooler unreachable but replicas connected, even with a zero postgres timestamp", func(t *testing.T) {
			// Regression: the leader's pooler died before multiorch ever recorded a
			// PostgresReady snapshot (zero timestamp), yet replicas are still streaming
			// and relaying a fresh watermark, so failover must be suppressed.
			sa := deadLeaderShardAnalysis(func(sa *ShardAnalysis) {
				setLeaderLive(sa, false)
				setLeaderPGReady(sa, false)
				connectReplica(sa)
				setQuorumCommitFresh(sa)
			})

			problems, err := analyzer.Analyze(sa)
			require.NoError(t, err)
			require.Empty(t, problems, "a dead pooler cannot report a postgres timestamp; trust the relayed watermark")
		})
	})

	t.Run("promotion in flight: reported, with the failover deferred", func(t *testing.T) {
		t.Run("defers failover of a promotion whose followers are still receiving its WAL",
			func(t *testing.T) {
				sa := deadLeaderShardAnalysis(func(sa *ShardAnalysis) {
					setLeaderLive(sa, true)
					setLeaderPGReady(sa, true)
					setRuleUndecided(sa)
					setLeaderAcceptedPromotion(sa)
					setQuorumCommitTs(sa, follower1ID, sa.Now.Add(-sa.Policy.QuorumCommitStaleAfter-time.Second))
					setPrimaryConnInfo(sa, follower1ID, "leader-host", 5432)
					setLastReceiveLsnAdvance(sa, follower1ID, sa.Now)
				})

				problems, err := analyzer.Analyze(sa)
				require.NoError(t, err)
				require.Len(t, problems, 1, "writes are unavailable until the promotion commits, so the problem is reported")
				require.Equal(t, types.ProblemLeaderPromotionIncomplete, problems[0].Code)
				require.WithinDuration(t, sa.Now.Add(sa.Policy.MaxPromotionTime), problems[0].NotBefore, 0,
					"a quorum receiving fresh WAL from the candidate means it is likely still catching up, so failover waits")
			})

		// Regression: a promotion still draining a WAL backlog well past the connect
		// grace (so the candidate's report can't be "trailing") must be reported but
		// not failed over while its followers keep receiving WAL from it.
		t.Run("defers failover of a slow promotion whose followers are still receiving WAL", func(t *testing.T) {
			sa := deadLeaderShardAnalysis(func(sa *ShardAnalysis) {
				setRuleUndecided(sa)
				sa.HighestPosition.Proposal.CreationTime = timestamppb.New(sa.Now.Add(-time.Minute))
				setLeaderAcceptedPromotion(sa)
				setLeaderLive(sa, true)
				setLeaderPGReady(sa, true)
				connectReplica(sa)
				setLastReceiveLsnAdvance(sa, follower1ID, sa.Now)
				setLastReceiveLsnAdvance(sa, follower2ID, sa.Now)
			})

			problems, err := analyzer.Analyze(sa)
			require.NoError(t, err)
			require.Len(t, problems, 1)
			require.Equal(t, types.ProblemLeaderPromotionIncomplete, problems[0].Code)
			require.True(t, problems[0].NotBefore.After(sa.Now), "a propagating promotion must not be failed over yet")
		})

		t.Run("defers failover while the candidate is mid pg_promote()", func(t *testing.T) {
			sa := deadLeaderShardAnalysis(func(sa *ShardAnalysis) {
				setRuleUndecided(sa)
				setLeaderAcceptedPromotion(sa)
				setLeaderLive(sa, true)
				setLeaderPGRunning(sa, true)
				setLeaderPGReady(sa, false) // not yet accepting connections
				setLeaderPromoting(sa)
			})

			problems, err := analyzer.Analyze(sa)
			require.NoError(t, err)
			require.Len(t, problems, 1, "writes are unavailable until the promotion commits")
			require.Equal(t, types.ProblemLeaderPromotionIncomplete, problems[0].Code)
			require.WithinDuration(t, sa.Now.Add(sa.Policy.MaxPromotionTime), problems[0].NotBefore, 0)
		})

		t.Run("stops deferring a promotion that has outlasted MaxPromotionTime", func(t *testing.T) {
			sa := deadLeaderShardAnalysis(func(sa *ShardAnalysis) {
				setRuleUndecided(sa)
				sa.HighestPosition.Proposal.CreationTime = timestamppb.New(sa.Now.Add(-sa.Policy.MaxPromotionTime - time.Second))
				setLeaderAcceptedPromotion(sa)
				setLeaderLive(sa, true)
				setLeaderPGRunning(sa, true)
				setLeaderPGReady(sa, false)
				setLeaderPromoting(sa)
			})

			problems, err := analyzer.Analyze(sa)
			require.NoError(t, err)
			require.Len(t, problems, 1)
			require.Equal(t, types.ProblemLeaderPromotionIncomplete, problems[0].Code)
			require.False(t, problems[0].NotBefore.After(sa.Now), "a promotion past MaxPromotionTime must not be deferred")
		})

		t.Run("does not defer a promotion whose followers receive no WAL and which is not mid pg_promote()", func(t *testing.T) {
			// Accepted but showing no progress: deferred only for QuorumCommitStaleAfter
			// after the proposal, like any new rule — not up to MaxPromotionTime.
			sa := deadLeaderShardAnalysis(func(sa *ShardAnalysis) {
				setRuleUndecided(sa)
				sa.HighestPosition.Proposal.CreationTime = timestamppb.New(sa.Now.Add(-time.Minute))
				setLeaderAcceptedPromotion(sa)
				setLeaderLive(sa, true)
				setLeaderPGReady(sa, true)
			})

			problems, err := analyzer.Analyze(sa)
			require.NoError(t, err)
			require.Len(t, problems, 1)
			require.Equal(t, types.ProblemLeaderPromotionIncomplete, problems[0].Code)
			require.False(t, problems[0].NotBefore.After(sa.Now))
		})

		t.Run("does not let a cascading standby's WAL advance excuse an undecided promotion", func(t *testing.T) {
			// follower1 streams directly from the candidate leader (correctly
			// configured) but has gone quiet -- no LastReceiveLsnAdvanceTime of
			// its own. follower2 streams from follower1 (cascading), not from
			// the leader, and IS fresh. A cascading standby's ack never reaches
			// the leader's synchronous-commit quorum, so follower2's fresh WAL
			// receipt cannot stand in for follower1's -- the only report that
			// would actually speak to the candidate leader's health.
			sa := deadLeaderShardAnalysis(func(sa *ShardAnalysis) {
				setLeaderLive(sa, true)
				setLeaderPGReady(sa, true)
				setRuleUndecided(sa)
				setLeaderAcceptedPromotion(sa)
				setQuorumCommitTs(sa, follower1ID, sa.Now.Add(-sa.Policy.QuorumCommitStaleAfter-time.Second))
				setPrimaryConnInfo(sa, follower1ID, "leader-host", 5432)
				setPrimaryConnInfo(sa, follower2ID, "follower1-host", 5433)
				setLastReceiveLsnAdvance(sa, follower2ID, sa.Now)
			})

			problems, err := analyzer.Analyze(sa)
			require.NoError(t, err)
			require.Len(t, problems, 1)
			require.Equal(t, types.ProblemLeaderPromotionIncomplete, problems[0].Code)
			require.WithinDuration(t, sa.Now.Add(sa.Policy.QuorumCommitStaleAfter), problems[0].NotBefore, 0,
				"a cascading standby's WAL advance must not extend the deferral to MaxPromotionTime")
		})

		t.Run("does not defer failover when postgres crashes during promotion", func(t *testing.T) {
			sa := deadLeaderShardAnalysis(func(sa *ShardAnalysis) {
				setRuleUndecided(sa)
				setLeaderAcceptedPromotion(sa)
				setLeaderLive(sa, true)       // multipooler survived
				setLeaderPGRunning(sa, false) // postgres process died during promotion
				setLeaderPGReady(sa, false)
				setLeaderPromoting(sa) // flag still set until the promote call gives up
			})

			problems, err := analyzer.Analyze(sa)
			require.NoError(t, err)
			require.Len(t, problems, 1, "should detect dead leader when postgres crashes during promotion")
			require.Equal(t, types.ProblemLeaderUnhealthy, problems[0].Code)
		})

		t.Run("exempts a candidate still in recovery before its pg_promote() from the in-recovery check", func(t *testing.T) {
			sa := deadLeaderShardAnalysis(func(sa *ShardAnalysis) {
				setRuleUndecided(sa)
				setLeaderAcceptedPromotion(sa)
				setLeaderLive(sa, true)
				setLeaderPGReady(sa, true)
				setLeaderPGStandby(sa) // Promote landed; pg_promote() hasn't run yet
			})

			problems, err := analyzer.Analyze(sa)
			require.NoError(t, err)
			require.Len(t, problems, 1)
			require.Equal(t, types.ProblemLeaderPromotionIncomplete, problems[0].Code)
		})

		t.Run("does not count followers disconnected by a promotion's timeline switch as cut off", func(t *testing.T) {
			// pg_promote() switches timeline, briefly disconnecting cascading followers
			// all at once; during a promotion only revocation counts against the leader.
			sa := deadLeaderShardAnalysis(cutOffAllFollowers, func(sa *ShardAnalysis) {
				setRuleUndecided(sa)
				setLeaderAcceptedPromotion(sa)
				setLeaderLive(sa, true)
				setLeaderPGRunning(sa, true)
				setLeaderPGReady(sa, true)
				setLeaderPromoting(sa)
			})

			problems, err := analyzer.Analyze(sa)
			require.NoError(t, err)
			require.Len(t, problems, 1)
			require.Equal(t, types.ProblemLeaderPromotionIncomplete, problems[0].Code)
		})

		t.Run("a PROMOTING flag without an in-flight promotion does not excuse an unready postgres", func(t *testing.T) {
			// The rule is decided, so no promotion is in flight: the flag is stale.
			sa := deadLeaderShardAnalysis(func(sa *ShardAnalysis) {
				setLeaderLive(sa, true)
				setLeaderPGRunning(sa, true)
				setLeaderPGReady(sa, false)
				setLeaderPromoting(sa)
			})

			problems, err := analyzer.Analyze(sa)
			require.NoError(t, err)
			require.Len(t, problems, 1)
			require.Equal(t, types.ProblemLeaderUnhealthy, problems[0].Code)
		})

		t.Run("a cohort-change proposal on an established leader is not a promotion", func(t *testing.T) {
			// An undecided proposal that keeps the same leader doesn't relax anything:
			// a stale watermark is LeaderProgressUnproven, not a deferred promotion.
			sa := deadLeaderShardAnalysis(func(sa *ShardAnalysis) {
				setLeaderLive(sa, true)
				setLeaderPGReady(sa, true)
				proposal := proto.Clone(sa.HighestPosition.Decision).(*clustermetadatapb.ShardRule)
				proposal.RuleNumber = &clustermetadatapb.RuleNumber{CoordinatorTerm: 1, LeaderSubterm: 1}
				proposal.CreationTime = timestamppb.New(sa.Now.Add(-time.Hour))
				sa.HighestPosition.Proposal = proposal
				setLeaderAcceptedPromotion(sa)
				setQuorumCommitTs(sa, follower1ID, sa.Now.Add(-sa.Policy.QuorumCommitStaleAfter-time.Second))
				setLastReceiveLsnAdvance(sa, follower1ID, sa.Now)
			})

			problems, err := analyzer.Analyze(sa)
			require.NoError(t, err)
			require.Len(t, problems, 1)
			require.Equal(t, types.ProblemLeaderProgressUnproven, problems[0].Code)
			require.False(t, problems[0].NotBefore.After(sa.Now))
		})

		t.Run("does not suppress failover when multipooler unreachable during promotion", func(t *testing.T) {
			sa := deadLeaderShardAnalysis(func(sa *ShardAnalysis) {
				setLeaderLive(sa, false) // stream disconnected (stale flag)
				setLeaderPGRunning(sa, true)
				setLeaderPGReady(sa, false)
				setLeaderPromoting(sa) // stale flag from last snapshot
			}, cutOffAllFollowers)

			problems, err := analyzer.Analyze(sa)
			require.NoError(t, err)
			require.Len(t, problems, 1, "should detect dead leader when multipooler is unreachable even if promotion flag is set")
			require.Equal(t, types.ProblemLeaderUnreachableByCohort, problems[0].Code)
		})
	})

	t.Run("whether a failover can proceed, and what to alert", func(t *testing.T) {
		t.Run("reports ShardStuck when a must-replace leader cannot reach a recruitment quorum", func(t *testing.T) {
			// LeaderUnhealthy (a convict cause): leader reachable but postgres wedged past
			// the response threshold. With both followers gone, only the leader is
			// reachable — below a majority — so the failover is infeasible: ShardStuck.
			// Exercises emitFailover's infeasible branch (distinct from emitInconclusive).
			sa := deadLeaderShardAnalysis(func(sa *ShardAnalysis) {
				setLeaderLive(sa, true)
				setLeaderPGRunning(sa, true)
				setLeaderPGReady(sa, false)
				setLeaderLastReady(sa, time.Now().Add(-60*time.Second))
				dropFollower(sa, follower1ID)
				dropFollower(sa, follower2ID)
			})

			problems, err := analyzer.Analyze(sa)
			require.NoError(t, err)
			require.Len(t, problems, 1)
			require.Equal(t, types.ProblemShardStuck, problems[0].Code)
		})

		t.Run("reports ShardStuck when the leader is dead but only one follower is reachable", func(t *testing.T) {
			// One reachable follower is not a strict majority of the 3-member cohort, so
			// recruitment is infeasible: ShardStuck rather than an actionable failover.
			sa := deadLeaderShardAnalysis(func(sa *ShardAnalysis) {
				dropFollower(sa, follower2ID)
			})

			problems, err := analyzer.Analyze(sa)
			require.NoError(t, err)
			require.Len(t, problems, 1)
			require.Equal(t, types.ProblemShardStuck, problems[0].Code)
			require.Equal(t, types.ScopeShard, problems[0].Scope)
		})

		t.Run("reports ShardStuck when a cohort member's observation is stale", func(t *testing.T) {
			// follower2's snapshot is older than the recruitment freshness bound.
			// recruitableCohort keys on observation freshness, so follower2 must NOT count
			// toward the recruitment quorum — leaving only follower1 reachable, which is
			// below the majority of 3, so the failover is infeasible. (Were staleness not
			// checked, follower2 would count and this would be an actionable
			// LeaderNotSelfConfirmed instead.)
			sa := deadLeaderShardAnalysis(func(sa *ShardAnalysis) {
				sa.Analyses[1].Mutate(func(h *multiorchdatapb.PoolerHealthState) {
					h.LastSeen = timestamppb.New(sa.Now.Add(-time.Hour))
				})
			})

			problems, err := analyzer.Analyze(sa)
			require.NoError(t, err)
			require.Len(t, problems, 1)
			require.Equal(t, types.ProblemShardStuck, problems[0].Code)
		})

		t.Run("reports ShardStuck when a cohort member is cohort-ineligible", func(t *testing.T) {
			// follower2 is fresh and initialized but has self-reported INELIGIBLE
			// (e.g. graceful shutdown). Recruit would refuse it server-side, so
			// recruitableCohort must not count it — leaving only follower1, below the
			// majority of 3, so the failover is infeasible.
			sa := deadLeaderShardAnalysis(func(sa *ShardAnalysis) {
				sa.Analyses[1].Mutate(func(h *multiorchdatapb.PoolerHealthState) {
					h.AvailabilityStatus = &clustermetadatapb.AvailabilityStatus{
						CohortEligibilityStatus: &clustermetadatapb.CohortEligibilityStatus{
							Signal: clustermetadatapb.CohortEligibilitySignal_COHORT_ELIGIBILITY_SIGNAL_INELIGIBLE,
						},
					}
				})
			})

			problems, err := analyzer.Analyze(sa)
			require.NoError(t, err)
			require.Len(t, problems, 1)
			require.Equal(t, types.ProblemShardStuck, problems[0].Code)
		})

		t.Run("reports ShardStuck when a cohort member has a recruit-position floor outstanding", func(t *testing.T) {
			// follower2 is fresh and initialized but hasn't caught back up from a
			// pg_rewind yet (RecruitBlockedUntil set). Recruit would refuse it
			// server-side, so recruitableCohort must not count it — leaving only
			// follower1, below the majority of 3, so the failover is infeasible.
			sa := deadLeaderShardAnalysis(func(sa *ShardAnalysis) {
				sa.Analyses[1].Mutate(func(h *multiorchdatapb.PoolerHealthState) {
					h.ConsensusStatus = &clustermetadatapb.ConsensusStatus{
						RecruitBlockedUntil: &clustermetadatapb.LsnPosition{Lsn: "0/2000000"},
					}
				})
			})

			problems, err := analyzer.Analyze(sa)
			require.NoError(t, err)
			require.Len(t, problems, 1)
			require.Equal(t, types.ProblemShardStuck, problems[0].Code)
		})

		t.Run("reports ShardStuck, not NoHealthyCohortMembers, when the only fresh member is ineligible", func(t *testing.T) {
			// follower1 is dropped entirely (no observation) and follower2 is fresh but
			// INELIGIBLE. orch is NOT blind — follower2's report is a real, trustworthy
			// observation (freshInitializedCohort counts it) — it just can't win a Recruit round
			// (recruitableCohort excludes it). That distinction must produce ShardStuck
			// (a confident "can't proceed" verdict), not NoHealthyCohortMembers (which
			// would wrongly claim orch has no usable signal at all).
			sa := deadLeaderShardAnalysis(func(sa *ShardAnalysis) {
				dropFollower(sa, follower1ID)
				sa.Analyses[0].Mutate(func(h *multiorchdatapb.PoolerHealthState) {
					h.AvailabilityStatus = &clustermetadatapb.AvailabilityStatus{
						CohortEligibilityStatus: &clustermetadatapb.CohortEligibilityStatus{
							Signal: clustermetadatapb.CohortEligibilitySignal_COHORT_ELIGIBILITY_SIGNAL_INELIGIBLE,
						},
					}
				})
			})

			problems, err := analyzer.Analyze(sa)
			require.NoError(t, err)
			require.Len(t, problems, 1)
			require.Equal(t, types.ProblemShardStuck, problems[0].Code)
		})

		t.Run("reports NoHealthyCohortMembers when no pooler has usable health", func(t *testing.T) {
			// The leader is not observed live and no cohort member has a fresh
			// observation — orch is blind, so it cannot tell whether the leader really
			// failed. Rather than convict on stale evidence (ShardStuck), surface the
			// blind spot.
			sa := deadLeaderShardAnalysis(func(sa *ShardAnalysis) {
				sa.Analyses = nil // no reachable cohort member, leader not fresh
			})

			problems, err := analyzer.Analyze(sa)
			require.NoError(t, err)
			require.Len(t, problems, 1)
			require.Equal(t, types.ProblemNoHealthyCohortMembers, problems[0].Code)
			require.Equal(t, types.ScopeShard, problems[0].Scope)
			require.Equal(t, types.PriorityEmergency, problems[0].Priority)
		})

		t.Run("does not convict ShardStuck when only a sub-quorum streams from an unreachable leader", func(t *testing.T) {
			// Leader unobserved and only follower1 is reachable, streaming from the
			// leader and relaying a fresh watermark: writes provably commit, so this is
			// NOT a failover and NOT ShardStuck. Because losing the leader now could not
			// be recovered (only one member reachable), we warn ShardAtRisk instead.
			sa := deadLeaderShardAnalysis(func(sa *ShardAnalysis) {
				sa.Analyses[0] = store.NewPooler(&multiorchdatapb.PoolerHealthState{
					Multipooler: &clustermetadatapb.Multipooler{Id: follower1ID, ShardKey: shardKey},
					LastSeen:    timestamppb.New(sa.Now),
					Status: &multipoolermanagerdatapb.Status{
						IsInitialized: true,
						ReplicationStatus: &multipoolermanagerdatapb.StandbyReplicationStatus{
							PrimaryConnInfo:    &multipoolermanagerdatapb.PrimaryConnInfo{Host: "leader-host", Port: 5432},
							LastReceiveLsn:     "0/1",
							WalReceiverStatus:  "streaming",
							LastMsgReceiveTime: timestamppb.New(sa.Now),
						},
					},
				}, nil)
				dropFollower(sa, follower2ID)
				setQuorumCommitFresh(sa)
			})

			problems, err := analyzer.Analyze(sa)
			require.NoError(t, err)
			require.Len(t, problems, 1)
			require.Equal(t, types.ProblemShardAtRisk, problems[0].Code,
				"a fresh watermark proves progress; warn fragility, do not convict ShardStuck")
		})

		t.Run("reports ShardAtRisk when the leader is healthy but could not be recovered if lost", func(t *testing.T) {
			// Leader is healthy, so no failover is needed. But excluding the leader,
			// only one follower is reachable — below a majority — so losing the leader
			// now would strand the shard. Warn (ShardAtRisk), non-blocking.
			sa := deadLeaderShardAnalysis(func(sa *ShardAnalysis) {
				setLeaderLive(sa, true)
				setLeaderPGReady(sa, true)
				setQuorumCommitFresh(sa)
				dropFollower(sa, follower2ID)
			})

			problems, err := analyzer.Analyze(sa)
			require.NoError(t, err)
			require.Len(t, problems, 1)
			require.Equal(t, types.ProblemShardAtRisk, problems[0].Code)
			require.Equal(t, types.ScopePooler, problems[0].Scope)
			require.Equal(t, types.PriorityNormal, problems[0].Priority)
			require.Equal(t, leaderID, problems[0].PoolerID)
		})

		t.Run("does not warn ShardAtRisk for a cohort at its policy floor (2 of 2)", func(t *testing.T) {
			// A healthy 2-member cohort under AtLeast(2) inherently cannot recover from a
			// leader loss — that is the operator's chosen posture, not a degradation — so
			// no ShardAtRisk fires (it would otherwise fire forever for any minimum-size
			// cohort, e.g. the spurious-failover-recovery e2e scenario).
			sa := deadLeaderShardAnalysis(func(sa *ShardAnalysis) {
				sa.HighestPosition.Decision.CohortMembers = []*clustermetadatapb.ID{leaderID, follower1ID}
				dropFollower(sa, follower2ID)
				setLeaderLive(sa, true)
				setLeaderPGReady(sa, true)
				setQuorumCommitFresh(sa)
			})

			problems, err := analyzer.Analyze(sa)
			require.NoError(t, err)
			require.Empty(t, problems, "a cohort at its policy floor is not 'at risk' — that would fire permanently")
		})
	})

	t.Run("rules with no leader or no cohort", func(t *testing.T) {
		t.Run("recruits a leader when the rule names none but has a cohort", func(t *testing.T) {
			// A rule with cohort members but no designated leader needs one recruited.
			// Both followers are reachable, so recruitment is feasible and actionable.
			sa := deadLeaderShardAnalysis(func(sa *ShardAnalysis) {
				sa.HighestPosition.Decision.LeaderId = nil
			})

			problems, err := analyzer.Analyze(sa)
			require.NoError(t, err)
			require.Len(t, problems, 1)
			require.Equal(t, types.ProblemLeaderUnspecified, problems[0].Code)
			require.Equal(t, types.ScopeShard, problems[0].Scope)
		})

		t.Run("ignores a rule with neither leader nor cohort (unbootstrapped)", func(t *testing.T) {
			// An empty cohort with no leader is the initial, unbootstrapped rule —
			// ShardNeedsInitialization owns that, so this analyzer does nothing.
			sa := deadLeaderShardAnalysis(func(sa *ShardAnalysis) {
				sa.HighestPosition.Decision.LeaderId = nil
				sa.HighestPosition.Decision.CohortMembers = nil
			})

			problems, err := analyzer.Analyze(sa)
			require.NoError(t, err)
			require.Empty(t, problems)
		})

		t.Run("ignores when no leader exists in topology (future analysis)", func(t *testing.T) {
			sa := deadLeaderShardAnalysis(func(sa *ShardAnalysis) {
				sa.HighestPosition = nil
			})

			problems, err := analyzer.Analyze(sa)
			require.NoError(t, err)
			require.Empty(t, problems)
		})
	})

	t.Run("identity", func(t *testing.T) {
		t.Run("analyzer name is correct", func(t *testing.T) {
			require.Equal(t, types.CheckName("LeaderNeedsReplacement"), analyzer.Name())
		})
	})
}
