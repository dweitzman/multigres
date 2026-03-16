# Consensus Trace Viewer

A lightweight browser-based tool for visualising the consensus algorithm
tick by tick: node state, WAL log contents, and message traffic.

The viewer is plain HTML + SVG + JavaScript with no build step and no
framework dependencies. It should stay that way.

---

## Priorities

**Primary goal — hand-written traces for documentation and explanation.**
The most immediate need is a way to craft small, readable examples that show
interesting scenarios: a primary failing over, a replica catching up, a split
brain resolving, a timeline diverging. The transaction log is the centrepiece
of these examples — seeing which entries have reached quorum, which are
uncommitted, and where nodes have diverging histories is what makes these
diagrams useful. The author controls the log size, so the visualisation stays
readable.

**Secondary goal — exporting from a DST run.**
A real simulation trace has thousands of WAL entries and hundreds of ticks; it
cannot be visualised directly. If we add DST export in the future, it would
need a filtering and summary layer. This is deferred.

---

## Current state

`viewer.html` + `viewer.js` are a working proof-of-concept. They can render
nodes, step through ticks, and draw message arrows. The example trace is
hardcoded in `viewer.js` and uses a Raft-centric schema that doesn't match the
consensus package's concepts.

---

## Plan

### Step 1 — Define the JSON trace schema

The schema is designed to be written by hand. Every field is optional so a
minimal example can skip what it doesn't need.

#### Trace root

```json
{
  "ticks": [ ... ],
  "term_details": {
    "Rule(1)": "term=1: leader=node-1, cohort=[node-1]",
    "Rule(2)": "term=2: leader=node-1, cohort=[node-1, node-2, node-3]"
  }
}
```

`term_details` is optional. When present, hovering over a log entry cell in
the SVG shows the value as a tooltip. Use it to annotate rule-change entries
with the term rules they encode.

#### Tick

```json
{
  "ticks": [
    {
      "tick": 1,
      "nodes": [ ... ],
      "messages": [ ... ]
    }
  ]
}
```

#### Node

```json
{
  "id": "node-1",
  "type": "pooler",
  "role": "member",
  "term_seq": 3,
  "log": ["T1", "T2", "T3"],
  "committed_through": "T2",
  "diverged": ["T3"],
  "shutdown_intent": false,
  "note": "running initdb…"
}
```

```json
{
  "id": "coord-1",
  "type": "coordinator",
  "quorum_term_seq": 3,
  "quorum_primary": "node-1",
  "known_poolers": [
    { "id": "node-1", "role": "member", "term_seq": 3 },
    { "id": "node-2", "role": "observer" }
  ]
}
```

**Node fields:**

| Field               | Applies to  | Description                                                                                                                                                                       |
| ------------------- | ----------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `id`                | all         | node ID                                                                                                                                                                           |
| `type`              | all         | `"pooler"` or `"coordinator"`                                                                                                                                                     |
| `role`              | pooler      | `"unprovisioned"` (no backup loaded), `"observer"` (backup loaded, not in cohort), or `"member"` (in cohort); being primary is not a role — it is a consequence of the term rules |
| `term_seq`          | pooler      | highest term seq this node has applied (0 = none)                                                                                                                                 |
| `log`               | pooler      | ordered list of log entry labels, e.g. `["T1","T2","T3"]`                                                                                                                         |
| `committed_through` | pooler      | label of the last committed entry; entries up to and including this label render green, entries after render white (uncommitted)                                                  |
| `diverged`          | pooler      | labels of entries on this node that conflict with the current primary — rendered red to show a competing timeline                                                                 |
| `commitment`        | pooler      | active recruit commitment: `{"coord": "coord-1", "at_seq": 2, "proposed_seq": 4}` — the coordinator holding revocation authority and the term-seq range it may write              |
| `shutdown_intent`   | pooler      | `true` if SIGTERM has been received                                                                                                                                               |
| `note`              | pooler      | optional short annotation for transient internal state, e.g. `"running initdb…"`                                                                                                  |
| `quorum_term_seq`   | coordinator | highest confirmed quorum term seq                                                                                                                                                 |
| `quorum_primary`    | coordinator | primary of the highest quorum term                                                                                                                                                |
| `known_poolers`     | coordinator | what this coordinator knows about each pooler: `[{id, role, term_seq?}]`                                                                                                          |

**Log entry labels** are arbitrary short strings. Use `"T1"`, `"T2"`, … for
user transactions; `"Rule(4)"` or similar for shadow WAL rule-change entries.

#### Messages

```json
{
  "from": "node-1",
  "to": ["node-2", "node-3"],
  "label": "WAL(T3)",
  "sent_tick": 1,
  "dropped": ["node-3"]
}
```

**Message fields:**

| Field       | Description                                                                                                                                               |
| ----------- | --------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `from`      | source node ID                                                                                                                                            |
| `to`        | destination(s): a single ID, an array of IDs, or a type shorthand: `"poolers"` (all poolers in this tick), `"coordinators"`, or `"*"` (all nodes)         |
| `label`     | short human-readable description, e.g. `"RecruitRequest(atSeq=3)"`                                                                                        |
| `sent_tick` | optional; the tick this message was sent, if earlier than the tick it appears under. Lets the viewer annotate the arrow with the delivery delay           |
| `dropped`   | marks the message as not delivered: `true` (all recipients) or an array of node IDs (specific recipients). Dropped arrows render as dashed and greyed out |

**Where to place a message:** put it under the tick where it is delivered
(received). To show a message sent earlier and arriving later, set `sent_tick`
to the originating tick. The viewer draws the arrow annotated with how many
ticks it was in flight.

**Broadcast shorthand:** when a coordinator recruits all poolers or a pooler
broadcasts status to all coordinators, write one message with `"to": "poolers"`
or `"to": "coordinators"` instead of repeating it for each recipient.

### Step 2 — Update the viewer ✓

`viewer.js` now handles the full schema:

- `<select>` dropdown populated from `window.EXAMPLES`
- Pooler log colour coding: green (committed), white (uncommitted), red (diverged)
- `term_details` hover tooltips on log entry cells
- Coordinator compact panel with `quorum_term_seq`, `quorum_primary`, and `known_poolers`
- Pooler node fill colour by role: grey (unprovisioned), blue (observer), neutral (member)
- `note` field rendered as italic annotation on pooler nodes
- Broadcast `to` shorthands (`"poolers"`, `"coordinators"`, `"*"`) expanded before drawing
- Dropped messages rendered as dashed grey arrows
- `sent_tick` delay annotated on arrow labels
- Arrowhead defined once per render, not once per arrow

**Still to do:** `<input type="file">` picker for loading arbitrary local JSON files.

### Step 3 — Write example traces

Examples live in `examples/` as `.js` files, each registering on
`window.EXAMPLES`. Current examples:

- **`shard_initialization.js`** ✓ — three unprovisioned poolers → coordinator
  issues `InitializeCohort` → single-node cohort forms → observers load backup
  → cohort expands to all three members via a rule-change quorum write.

Scenarios still to add:

- **Normal replication**: primary streaming WAL to two replicas, entries
  reaching quorum one by one.
- **Primary failover**: primary stops, coordinator recruits a replica, new
  primary resumes with shadow WAL migration.
- **Graceful shutdown**: `shutdown_intent` triggers coordinator-led handoff
  before postgres stops.
- **Split brain**: two nodes with conflicting uncommitted entries after a
  partition, one is rewound on recovery.
- **Partial quorum**: a WAL entry reaches one replica but not the other before
  the primary crashes; shows what quorum does and does not protect.

### A note on CORS and local files

Opening `viewer.html` directly via `file://` blocks `fetch()` calls to local
files due to browser CORS policy. The simplest fix is to serve the directory:

```bash
./serve.sh        # uses npx serve, opens http://localhost:3000/viewer.html
```

This lets the viewer load `.json` trace files via normal `fetch()` calls.

For pre-bundled examples that should work without running a server, include
them as `.js` files in the HTML so they are available immediately. Each
example file registers itself on a global:

```js
// examples/primary_failover.js
window.EXAMPLES = window.EXAMPLES || {};
window.EXAMPLES["Primary failover"] = { "ticks": [ ... ] };
```

The viewer populates a `<select>` dropdown from `window.EXAMPLES`. A separate
`<input type="file">` picker (using `FileReader`, which has no CORS
restriction) lets the user open any local `.json` file as well. Together these
cover the two main use cases: browsing bundled examples and opening a file you
just wrote.

---

## Minimal handwritten example

```json
{
  "ticks": [
    {
      "tick": 1,
      "nodes": [
        {
          "id": "node-1",
          "type": "pooler",
          "role": "primary",
          "term_seq": 2,
          "log": ["T1", "T2", "T3"],
          "committed_through": "T2"
        },
        {
          "id": "node-2",
          "type": "pooler",
          "role": "replica",
          "term_seq": 2,
          "log": ["T1", "T2"],
          "committed_through": "T2"
        },
        {
          "id": "coord-1",
          "type": "coordinator",
          "quorum_term_seq": 2,
          "quorum_primary": "node-1"
        }
      ],
      "messages": [{ "from": "node-1", "to": "node-2", "label": "WAL(T3)" }]
    },
    {
      "tick": 2,
      "nodes": [
        {
          "id": "node-1",
          "type": "pooler",
          "role": "primary",
          "term_seq": 2,
          "log": ["T1", "T2", "T3"],
          "committed_through": "T3",
          "shutdown_intent": true
        },
        {
          "id": "node-2",
          "type": "pooler",
          "role": "replica",
          "term_seq": 2,
          "log": ["T1", "T2", "T3"],
          "committed_through": "T2"
        },
        {
          "id": "coord-1",
          "type": "coordinator",
          "quorum_term_seq": 2,
          "quorum_primary": "node-1"
        }
      ],
      "messages": [
        {
          "from": "node-2",
          "to": "node-1",
          "label": "Ack(T3)",
          "sent_tick": 1
        },
        {
          "from": "node-1",
          "to": "coordinators",
          "label": "Status(shutdown_intent=true)"
        },
        {
          "from": "coord-1",
          "to": "node-2",
          "label": "RecruitRequest(atSeq=2, proposedSeq=3)"
        }
      ]
    }
  ]
}
```

---

## DST export (future)

When we add export from simulation tests:

- Trigger via env var: `DSTSIM_TRACE_JSON=out.json go test -run TestX ./...`
- Filter to ticks where something notable happened (term change, message drop,
  node restart) plus surrounding context.
- Truncate the log to the last N entries per node so the viewer stays readable.
- Map Go indicator/request types to message labels using the same short strings
  already printed in text traces.

---

## Out of scope

- No message animation across ticks (static arrows per tick are fine)
- No zoom or pan
- No framework, build step, or bundler
- No automatic node layout changes between ticks (nodes stay in fixed positions)
- Manual layout control in the JSON (may add later if the automatic layout
  produces confusing results for specific examples)
