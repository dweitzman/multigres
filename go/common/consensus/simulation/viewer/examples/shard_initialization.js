window.EXAMPLES = window.EXAMPLES || {}
window.EXAMPLES["Shard initialization"] = {
  _sourceFile: "examples/shard_initialization.js",
  term_details: {
    "R(1)": "term=1: leader=node-1, cohort=[node-1]",
    "R(2)": "term=2: leader=node-1, cohort=[node-1, node-2, node-3]"
  },
  log_entry_types: {
    "R(1)": "rule",
    "R(2)": "rule"
  },
  ticks: [
    {
      tick: 1,
      nodes: [
        {
          id: "coord-1",
          type: "coordinator",
          quorum_term_seq: 0,
          quorum_primary: null,
          known_poolers: []
        },
        {id: "node-1", type: "pooler", role: "unprovisioned", term_seq: 0, log: []},
        {id: "node-2", type: "pooler", role: "unprovisioned", term_seq: 0, log: []},
        {id: "node-3", type: "pooler", role: "unprovisioned", term_seq: 0, log: []}
      ],
      messages: [
        {from: "node-1", to: "coord-1", label: "Status(unprovisioned)"},
        {from: "node-2", to: "coord-1", label: "Status(unprovisioned)"},
        {from: "node-3", to: "coord-1", label: "Status(unprovisioned)"}
      ]
    },
    {
      tick: 2,
      nodes: [
        {
          id: "coord-1",
          type: "coordinator",
          quorum_term_seq: 0,
          quorum_primary: null,
          known_poolers: [
            {id: "node-1", role: "unprovisioned"},
            {id: "node-2", role: "unprovisioned"},
            {id: "node-3", role: "unprovisioned"}
          ]
        },
        {id: "node-1", type: "pooler", role: "unprovisioned", term_seq: 0, log: []},
        {id: "node-2", type: "pooler", role: "unprovisioned", term_seq: 0, log: []},
        {id: "node-3", type: "pooler", role: "unprovisioned", term_seq: 0, log: []}
      ],
      messages: [
        {from: "coord-1", to: "node-1", label: "InitializeCohort(LSN=0)"}
      ]
    },
    {
      tick: 3,
      nodes: [
        {
          id: "coord-1",
          type: "coordinator",
          quorum_term_seq: 0,
          quorum_primary: null,
          known_poolers: [
            {id: "node-1", role: "unprovisioned"},
            {id: "node-2", role: "unprovisioned"},
            {id: "node-3", role: "unprovisioned"}
          ]
        },
        {
          id: "node-1", type: "pooler", role: "unprovisioned", term_seq: 0, log: [],
          note: "running initdb…"
        },
        {id: "node-2", type: "pooler", role: "unprovisioned", term_seq: 0, log: []},
        {id: "node-3", type: "pooler", role: "unprovisioned", term_seq: 0, log: []}
      ],
      messages: []
    },
    {
      tick: 4,
      nodes: [
        {
          id: "coord-1",
          type: "coordinator",
          quorum_term_seq: 0,
          quorum_primary: null,
          known_poolers: [
            {id: "node-1", role: "unprovisioned"},
            {id: "node-2", role: "unprovisioned"},
            {id: "node-3", role: "unprovisioned"}
          ]
        },
        {
          id: "node-1", type: "pooler", role: "member", term_seq: 1,
          log: ["R(1)"], committed_through: "R(1)"
        },
        {id: "node-2", type: "pooler", role: "unprovisioned", term_seq: 0, log: []},
        {id: "node-3", type: "pooler", role: "unprovisioned", term_seq: 0, log: []}
      ],
      messages: [
        {from: "node-1", to: "coord-1", label: "Status(member, term=1)"}
      ]
    },
    {
      tick: 5,
      nodes: [
        {
          id: "coord-1",
          type: "coordinator",
          quorum_term_seq: 1,
          quorum_primary: "node-1",
          known_poolers: [
            {id: "node-1", role: "member", term_seq: 1},
            {id: "node-2", role: "unprovisioned"},
            {id: "node-3", role: "unprovisioned"}
          ]
        },
        {
          id: "node-1", type: "pooler", role: "member", term_seq: 1,
          log: ["R(1)"], committed_through: "R(1)"
        },
        {id: "node-2", type: "pooler", role: "observer", term_seq: 0, log: []},
        {id: "node-3", type: "pooler", role: "observer", term_seq: 0, log: []}
      ],
      messages: [
        {from: "node-2", to: "coord-1", label: "Status(observer)"},
        {from: "node-3", to: "coord-1", label: "Status(observer)"}
      ]
    },
    {
      tick: 6,
      nodes: [
        {
          id: "coord-1",
          type: "coordinator",
          quorum_term_seq: 1,
          quorum_primary: "node-1",
          known_poolers: [
            {id: "node-1", role: "member", term_seq: 1},
            {id: "node-2", role: "observer"},
            {id: "node-3", role: "observer"}
          ]
        },
        {
          id: "node-1", type: "pooler", role: "member", term_seq: 1,
          log: ["R(1)"], committed_through: "R(1)"
        },
        {id: "node-2", type: "pooler", role: "observer", term_seq: 0, log: []},
        {id: "node-3", type: "pooler", role: "observer", term_seq: 0, log: []}
      ],
      messages: [
        {from: "coord-1", to: "node-1", label: "ExpandCohort(+node-2, +node-3)"}
      ]
    },
    {
      tick: 7,
      nodes: [
        {
          id: "coord-1",
          type: "coordinator",
          quorum_term_seq: 1,
          quorum_primary: "node-1",
          known_poolers: [
            {id: "node-1", role: "member", term_seq: 1},
            {id: "node-2", role: "observer"},
            {id: "node-3", role: "observer"}
          ]
        },
        {
          id: "node-1", type: "pooler", role: "member", term_seq: 1,
          log: ["R(1)", "R(2)"], committed_through: "R(1)"
        },
        {id: "node-2", type: "pooler", role: "observer", term_seq: 0, log: []},
        {id: "node-3", type: "pooler", role: "observer", term_seq: 0, log: []}
      ],
      messages: [
        {from: "node-1", to: ["node-2", "node-3"], label: "WAL(R(2))"}
      ]
    },
    {
      tick: 8,
      nodes: [
        {
          id: "coord-1",
          type: "coordinator",
          quorum_term_seq: 1,
          quorum_primary: "node-1",
          known_poolers: [
            {id: "node-1", role: "member", term_seq: 1},
            {id: "node-2", role: "observer"},
            {id: "node-3", role: "observer"}
          ]
        },
        {
          id: "node-1", type: "pooler", role: "member", term_seq: 1,
          log: ["R(1)", "R(2)"], committed_through: "R(1)"
        },
        {id: "node-2", type: "pooler", role: "observer", term_seq: 0, log: ["R(2)"]},
        {id: "node-3", type: "pooler", role: "observer", term_seq: 0, log: ["R(2)"]}
      ],
      messages: [
        {from: "node-2", to: "node-1", label: "Ack(R(2))", sent_tick: 7},
        {from: "node-3", to: "node-1", label: "Ack(R(2))", sent_tick: 7}
      ]
    },
    {
      tick: 9,
      nodes: [
        {
          id: "coord-1",
          type: "coordinator",
          quorum_term_seq: 2,
          quorum_primary: "node-1",
          known_poolers: [
            {id: "node-1", role: "member", term_seq: 2},
            {id: "node-2", role: "member", term_seq: 2},
            {id: "node-3", role: "member", term_seq: 2}
          ]
        },
        {
          id: "node-1", type: "pooler", role: "member", term_seq: 2,
          log: ["R(1)", "R(2)"], committed_through: "R(2)"
        },
        {
          id: "node-2", type: "pooler", role: "member", term_seq: 2,
          log: ["R(2)"], committed_through: "R(2)"
        },
        {
          id: "node-3", type: "pooler", role: "member", term_seq: 2,
          log: ["R(2)"], committed_through: "R(2)"
        }
      ],
      messages: []
    }
  ]
}
