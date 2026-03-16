// Scenario: phantom transaction recovery via coordinator failover.
//
// Setup: 4-node cluster (policy≥3), node-1 as primary.
// node-1 crashes while replicating T42. T42 reached node-2 (which
// persisted it) but was never delivered to node-3 or node-4. T42 never
// reached write quorum, so on crash recovery PostgreSQL truncates it from
// node-1's WAL; node-1 restarts with its log ending at T41.
//
// coord-1 starts recruiting but crashes before reaching quorum.
// coord-2 successfully recruits node-1 (recovered, highest WAL = T41),
// node-3 (highest WAL = T41), and node-4 (highest WAL = T41) — quorum of
// 3 without ever visiting node-2. T42 is absent from all three recruited
// nodes. Term=4 is established with the full cohort unchanged.
//
// node-2's T42 is now a phantom: it was never committed and the new
// primary doesn't have it. coord-2 detects the divergence when node-2
// reports its status, then issues a Resume call. pg_rewind removes T42
// from node-2; node-2 rejoins the cohort on term=4.

window.EXAMPLES = window.EXAMPLES || {}
window.EXAMPLES["Phantom transaction recovery"] = {
  _sourceFile: "examples/phantom_transaction_recovery.js",
  term_details: {
    "R(2)": "term=2: leader=node-1, cohort=[node-1,node-2,node-3,node-4], policy≥3",
    "R(4)": "term=4: leader=node-3, cohort=[node-1,node-2,node-3,node-4], policy≥3"
  },
  log_entry_types: {
    "R(2)": "rule",
    "R(4)": "rule"
  },
  ticks: [
    {
      tick: 1,
      nodes: [
        {
          id: "coord-1", type: "coordinator",
          quorum_term_seq: 2, quorum_primary: "node-1",
          known_poolers: [
            {id: "node-1", role: "member", term_seq: 2},
            {id: "node-2", role: "member", term_seq: 2},
            {id: "node-3", role: "member", term_seq: 2},
            {id: "node-4", role: "member", term_seq: 2}
          ]
        },
        {
          id: "coord-2", type: "coordinator",
          quorum_term_seq: 2, quorum_primary: "node-1",
          known_poolers: [
            {id: "node-1", role: "member", term_seq: 2},
            {id: "node-2", role: "member", term_seq: 2},
            {id: "node-3", role: "member", term_seq: 2},
            {id: "node-4", role: "member", term_seq: 2}
          ]
        },
        {
          id: "node-1", type: "pooler", role: "member", term_seq: 2,
          log: ["R(2)", "T41"], committed_through: "T41",
          note: "crashing…"
        },
        {
          id: "node-2", type: "pooler", role: "member", term_seq: 2,
          log: ["R(2)", "T41", "T42"], committed_through: "T41"
        },
        {
          id: "node-3", type: "pooler", role: "member", term_seq: 2,
          log: ["R(2)", "T41"], committed_through: "T41"
        },
        {
          id: "node-4", type: "pooler", role: "member", term_seq: 2,
          log: ["R(2)", "T41"], committed_through: "T41"
        }
      ],
      messages: [
        {from: "node-1", to: ["node-2", "node-3", "node-4"], label: "WAL(T42)", dropped: ["node-3", "node-4"]}
      ]
    },
    {
      tick: 2,
      nodes: [
        {
          id: "coord-1", type: "coordinator",
          quorum_term_seq: 2, quorum_primary: "node-1",
          known_poolers: [
            {id: "node-1", role: "member", term_seq: 2},
            {id: "node-2", role: "member", term_seq: 2},
            {id: "node-3", role: "member", term_seq: 2},
            {id: "node-4", role: "member", term_seq: 2}
          ]
        },
        {
          id: "coord-2", type: "coordinator",
          quorum_term_seq: 2, quorum_primary: "node-1",
          known_poolers: [
            {id: "node-1", role: "member", term_seq: 2},
            {id: "node-2", role: "member", term_seq: 2},
            {id: "node-3", role: "member", term_seq: 2},
            {id: "node-4", role: "member", term_seq: 2}
          ]
        },
        {
          id: "node-1", type: "pooler", role: "member", term_seq: 2,
          log: ["R(2)", "T41"], committed_through: "T41",
          note: "crashed (T42 truncated on recovery)"
        },
        {
          id: "node-2", type: "pooler", role: "member", term_seq: 2,
          log: ["R(2)", "T41", "T42"], committed_through: "T41"
        },
        {
          id: "node-3", type: "pooler", role: "member", term_seq: 2,
          log: ["R(2)", "T41"], committed_through: "T41"
        },
        {
          id: "node-4", type: "pooler", role: "member", term_seq: 2,
          log: ["R(2)", "T41"], committed_through: "T41"
        }
      ],
      messages: [
        {from: "coord-1", to: "node-3", label: "Recruit(at=2, seq=3)"},
        {from: "coord-1", to: "node-4", label: "Recruit(at=2, seq=3)"}
      ]
    },
    {
      tick: 3,
      nodes: [
        {
          id: "coord-1", type: "coordinator",
          note: "crashed",
          quorum_term_seq: 2, quorum_primary: "node-1",
          known_poolers: []
        },
        {
          id: "coord-2", type: "coordinator",
          quorum_term_seq: 2, quorum_primary: "node-1",
          known_poolers: [
            {id: "node-1", role: "member", term_seq: 2},
            {id: "node-2", role: "member", term_seq: 2},
            {id: "node-3", role: "member", term_seq: 2},
            {id: "node-4", role: "member", term_seq: 2}
          ]
        },
        {
          id: "node-1", type: "pooler", role: "member", term_seq: 2,
          log: ["R(2)", "T41"], committed_through: "T41",
          note: "recovering…"
        },
        {
          id: "node-2", type: "pooler", role: "member", term_seq: 2,
          log: ["R(2)", "T41", "T42"], committed_through: "T41"
        },
        {
          id: "node-3", type: "pooler", role: "member", term_seq: 2,
          log: ["R(2)", "T41"], committed_through: "T41",
          commitment: {coord: "coord-1", at_seq: 2, proposed_seq: 3}
        },
        {
          id: "node-4", type: "pooler", role: "member", term_seq: 2,
          log: ["R(2)", "T41"], committed_through: "T41",
          commitment: {coord: "coord-1", at_seq: 2, proposed_seq: 3}
        }
      ],
      messages: [
        {from: "node-3", to: "coord-1", label: "RevocationAck(T41)", dropped: true},
        {from: "node-4", to: "coord-1", label: "RevocationAck(T41)", dropped: true},
        {from: "coord-2", to: "node-1", label: "Recruit(at=2, seq=4)"},
        {from: "coord-2", to: "node-3", label: "Recruit(at=2, seq=4)"},
        {from: "coord-2", to: "node-4", label: "Recruit(at=2, seq=4)"}
      ]
    },
    {
      tick: 4,
      nodes: [
        {
          id: "coord-1", type: "coordinator",
          note: "crashed",
          quorum_term_seq: 2, quorum_primary: "node-1",
          known_poolers: []
        },
        {
          id: "coord-2", type: "coordinator",
          quorum_term_seq: 2, quorum_primary: "node-1",
          known_poolers: [
            {id: "node-1", role: "member", term_seq: 2},
            {id: "node-2", role: "member", term_seq: 2},
            {id: "node-3", role: "member", term_seq: 2},
            {id: "node-4", role: "member", term_seq: 2}
          ]
        },
        {
          id: "node-1", type: "pooler", role: "member", term_seq: 2,
          log: ["R(2)", "T41"], committed_through: "T41",
          commitment: {coord: "coord-2", at_seq: 2, proposed_seq: 4}
        },
        {
          id: "node-2", type: "pooler", role: "member", term_seq: 2,
          log: ["R(2)", "T41", "T42"], committed_through: "T41"
        },
        {
          id: "node-3", type: "pooler", role: "member", term_seq: 2,
          log: ["R(2)", "T41"], committed_through: "T41",
          commitment: {coord: "coord-2", at_seq: 2, proposed_seq: 4}
        },
        {
          id: "node-4", type: "pooler", role: "member", term_seq: 2,
          log: ["R(2)", "T41"], committed_through: "T41",
          commitment: {coord: "coord-2", at_seq: 2, proposed_seq: 4}
        }
      ],
      messages: [
        {from: "node-1", to: "coord-2", label: "RevocationAck(T41)"},
        {from: "node-3", to: "coord-2", label: "RevocationAck(T41)"},
        {from: "node-4", to: "coord-2", label: "RevocationAck(T41)"}
      ]
    },
    {
      tick: 5,
      nodes: [
        {
          id: "coord-1", type: "coordinator",
          note: "crashed",
          quorum_term_seq: 2, quorum_primary: "node-1",
          known_poolers: []
        },
        {
          id: "coord-2", type: "coordinator",
          quorum_term_seq: 2, quorum_primary: "node-1",
          known_poolers: [
            {id: "node-1", role: "member", term_seq: 2},
            {id: "node-2", role: "member", term_seq: 2},
            {id: "node-3", role: "member", term_seq: 2},
            {id: "node-4", role: "member", term_seq: 2}
          ]
        },
        {
          id: "node-1", type: "pooler", role: "member", term_seq: 2,
          log: ["R(2)", "T41"], committed_through: "T41",
          commitment: {coord: "coord-2", at_seq: 2, proposed_seq: 4}
        },
        {
          id: "node-2", type: "pooler", role: "member", term_seq: 2,
          log: ["R(2)", "T41", "T42"], committed_through: "T41"
        },
        {
          id: "node-3", type: "pooler", role: "member", term_seq: 2,
          log: ["R(2)", "T41"], committed_through: "T41",
          commitment: {coord: "coord-2", at_seq: 2, proposed_seq: 4}
        },
        {
          id: "node-4", type: "pooler", role: "member", term_seq: 2,
          log: ["R(2)", "T41"], committed_through: "T41",
          commitment: {coord: "coord-2", at_seq: 2, proposed_seq: 4}
        }
      ],
      messages: [
        {from: "coord-2", to: "node-1", label: "Propagate(seq=4, primary=node-3)"},
        {from: "coord-2", to: "node-3", label: "Propagate(seq=4, primary=node-3)"},
        {from: "coord-2", to: "node-4", label: "Propagate(seq=4, primary=node-3)"}
      ]
    },
    {
      tick: 6,
      nodes: [
        {
          id: "coord-1", type: "coordinator",
          note: "crashed",
          quorum_term_seq: 2, quorum_primary: "node-1",
          known_poolers: []
        },
        {
          id: "coord-2", type: "coordinator",
          quorum_term_seq: 2, quorum_primary: "node-1",
          known_poolers: [
            {id: "node-1", role: "member", term_seq: 2},
            {id: "node-2", role: "member", term_seq: 2},
            {id: "node-3", role: "member", term_seq: 2},
            {id: "node-4", role: "member", term_seq: 2}
          ]
        },
        {
          id: "node-1", type: "pooler", role: "member", term_seq: 2,
          log: ["R(2)", "T41"], committed_through: "T41",
          commitment: {coord: "coord-2", at_seq: 2, proposed_seq: 4}
        },
        {
          id: "node-2", type: "pooler", role: "member", term_seq: 2,
          log: ["R(2)", "T41", "T42"], committed_through: "T41"
        },
        {
          id: "node-3", type: "pooler", role: "member", term_seq: 2,
          log: ["R(2)", "T41"], committed_through: "T41",
          commitment: {coord: "coord-2", at_seq: 2, proposed_seq: 4}
        },
        {
          id: "node-4", type: "pooler", role: "member", term_seq: 2,
          log: ["R(2)", "T41"], committed_through: "T41",
          commitment: {coord: "coord-2", at_seq: 2, proposed_seq: 4}
        }
      ],
      messages: [
        {from: "node-1", to: "coord-2", label: "PropagateAck(seq=4)"},
        {from: "node-3", to: "coord-2", label: "PropagateAck(seq=4)"},
        {from: "node-4", to: "coord-2", label: "PropagateAck(seq=4)"}
      ]
    },
    {
      tick: 7,
      nodes: [
        {
          id: "coord-1", type: "coordinator",
          note: "crashed",
          quorum_term_seq: 2, quorum_primary: "node-1",
          known_poolers: []
        },
        {
          id: "coord-2", type: "coordinator",
          quorum_term_seq: 2, quorum_primary: "node-1",
          known_poolers: [
            {id: "node-1", role: "member", term_seq: 2},
            {id: "node-2", role: "member", term_seq: 2},
            {id: "node-3", role: "member", term_seq: 2},
            {id: "node-4", role: "member", term_seq: 2}
          ]
        },
        {
          id: "node-1", type: "pooler", role: "member", term_seq: 2,
          log: ["R(2)", "T41"], committed_through: "T41",
          commitment: {coord: "coord-2", at_seq: 2, proposed_seq: 4}
        },
        {
          id: "node-2", type: "pooler", role: "member", term_seq: 2,
          log: ["R(2)", "T41", "T42"], committed_through: "T41"
        },
        {
          id: "node-3", type: "pooler", role: "member", term_seq: 4,
          log: ["R(2)", "T41", "R(4)"], committed_through: "T41",
          commitment: {coord: "coord-2", at_seq: 2, proposed_seq: 4}
        },
        {
          id: "node-4", type: "pooler", role: "member", term_seq: 2,
          log: ["R(2)", "T41"], committed_through: "T41",
          commitment: {coord: "coord-2", at_seq: 2, proposed_seq: 4}
        }
      ],
      messages: [
        {from: "coord-2", to: "node-3", label: "Propose(term=4)"},
        {from: "node-3", to: ["node-1", "node-4"], label: "WAL(R(4))"}
      ]
    },
    {
      tick: 8,
      nodes: [
        {
          id: "coord-1", type: "coordinator",
          note: "crashed",
          quorum_term_seq: 2, quorum_primary: "node-1",
          known_poolers: []
        },
        {
          id: "coord-2", type: "coordinator",
          quorum_term_seq: 4, quorum_primary: "node-3",
          known_poolers: [
            {id: "node-1", role: "member", term_seq: 4},
            {id: "node-2", role: "member", term_seq: 2},
            {id: "node-3", role: "member", term_seq: 4},
            {id: "node-4", role: "member", term_seq: 4}
          ]
        },
        {
          id: "node-1", type: "pooler", role: "member", term_seq: 4,
          log: ["R(2)", "T41", "R(4)"], committed_through: "R(4)"
        },
        {
          id: "node-2", type: "pooler", role: "member", term_seq: 2,
          log: ["R(2)", "T41", "T42"], committed_through: "T41",
          diverged: ["T42"]
        },
        {
          id: "node-3", type: "pooler", role: "member", term_seq: 4,
          log: ["R(2)", "T41", "R(4)"], committed_through: "R(4)"
        },
        {
          id: "node-4", type: "pooler", role: "member", term_seq: 4,
          log: ["R(2)", "T41", "R(4)"], committed_through: "R(4)"
        }
      ],
      messages: [
        {from: "node-1", to: "node-3", label: "Ack(R(4))", sent_tick: 7},
        {from: "node-4", to: "node-3", label: "Ack(R(4))", sent_tick: 7},
        {from: "node-2", to: "coord-2", label: "Status(member, term=2, high=T42)"}
      ]
    },
    {
      tick: 9,
      nodes: [
        {
          id: "coord-1", type: "coordinator",
          note: "crashed",
          quorum_term_seq: 2, quorum_primary: "node-1",
          known_poolers: []
        },
        {
          id: "coord-2", type: "coordinator",
          quorum_term_seq: 4, quorum_primary: "node-3",
          known_poolers: [
            {id: "node-1", role: "member", term_seq: 4},
            {id: "node-2", role: "member", term_seq: 2},
            {id: "node-3", role: "member", term_seq: 4},
            {id: "node-4", role: "member", term_seq: 4}
          ]
        },
        {
          id: "node-1", type: "pooler", role: "member", term_seq: 4,
          log: ["R(2)", "T41", "R(4)"], committed_through: "R(4)"
        },
        {
          id: "node-2", type: "pooler", role: "member", term_seq: 2,
          log: ["R(2)", "T41", "T42"], committed_through: "T41",
          diverged: ["T42"]
        },
        {
          id: "node-3", type: "pooler", role: "member", term_seq: 4,
          log: ["R(2)", "T41", "R(4)"], committed_through: "R(4)"
        },
        {
          id: "node-4", type: "pooler", role: "member", term_seq: 4,
          log: ["R(2)", "T41", "R(4)"], committed_through: "R(4)"
        }
      ],
      messages: [
        {from: "coord-2", to: "node-2", label: "Resume(term=4, pg_rewind→node-3)"}
      ]
    },
    {
      tick: 10,
      nodes: [
        {
          id: "coord-1", type: "coordinator",
          note: "crashed",
          quorum_term_seq: 2, quorum_primary: "node-1",
          known_poolers: []
        },
        {
          id: "coord-2", type: "coordinator",
          quorum_term_seq: 4, quorum_primary: "node-3",
          known_poolers: [
            {id: "node-1", role: "member", term_seq: 4},
            {id: "node-2", role: "member", term_seq: 4},
            {id: "node-3", role: "member", term_seq: 4},
            {id: "node-4", role: "member", term_seq: 4}
          ]
        },
        {
          id: "node-1", type: "pooler", role: "member", term_seq: 4,
          log: ["R(2)", "T41", "R(4)"], committed_through: "R(4)"
        },
        {
          id: "node-2", type: "pooler", role: "member", term_seq: 4,
          log: ["R(2)", "T41", "R(4)"], committed_through: "R(4)"
        },
        {
          id: "node-3", type: "pooler", role: "member", term_seq: 4,
          log: ["R(2)", "T41", "R(4)"], committed_through: "R(4)"
        },
        {
          id: "node-4", type: "pooler", role: "member", term_seq: 4,
          log: ["R(2)", "T41", "R(4)"], committed_through: "R(4)"
        }
      ],
      messages: [
        {from: "node-2", to: "coord-2", label: "Status(member, term=4)"}
      ]
    }
  ]
}
