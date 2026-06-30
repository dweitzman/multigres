# Architecture

## Core Services (go/cmd/)

- **multigateway** - PostgreSQL proxy accepting client connections, routes queries
- **multipooler** - Connection pooling service, communicates with postgres
- **pgctld** - PostgreSQL manager, starts, restarts, and stops PostgreSQL instances
- **multiorch** - Cluster orchestration for consensus and failover
- **multiadmin** - Administrative service for cluster management
- **multigres** - CLI tool for cluster management

## Data Flow

1. Client → **multigateway** (accepts PostgreSQL connections)
2. **multigateway** → **multipooler** (query routing and pooling)
3. **multipooler** → **postgres** (database interface)
4. **pgctld** → handles starting and stopping PostgreSQL (actual database)
5. **multiorch** handles failover and consensus across cells

## Topology

The system uses etcd for service discovery and topology storage. The topology is organized by cells (zones), with each cell having its own set of services.

## Directory Structure and Dependencies

```text
./go/cmd/...      # Commands - can depend on anything
./go/services/... # Service code - cannot depend on cmd/ or other services
./go/common/...   # Shared code - cannot depend on cmd/ or services/
./go/tools/...    # Generic utilities - cannot depend on any repo code outside tools/
```

- **go/tools/**: Generic helpers (timers, retry, etc.) that aren't multigres-specific
- **go/common/**: Shared multigres code (error codes, gRPC clients, protocol code, etc.)

## Shared utilities

Before defining new constants, helpers, or type wrappers for PG/SQL concepts, check whether they already exist in `go/common/`. Common spots worth grepping first:

- **`go/common/parser/ast/oids.go`** — PostgreSQL type OIDs (`BOOLOID`, `TEXTOID`, `VARCHAROID`, `INT4OID`, etc.) and the `Oid` type alias. Don't redefine `uint32 = 25` for TEXT — use `ast.TEXTOID`.
- **`go/common/parser/ast/`** — AST node types, clone helpers (`CloneRefOfSelectStmt`, etc.), tree walker (`Rewrite`), constructors (`NewA_Const`, `NewString`, `NewBoolean`, `NewParamRef`).
- **`go/common/sqltypes/params.go`** — wire-format Bind param packing (`ParamsToProto` / `ParamsFromProto`).
- **`go/common/preparedstatement/`** — `PortalInfo`, `PreparedStatementInfo`, `Consolidator`, bind value decoders (`DecodeBindAsText`, `DecodeBindAsBool`).
- **`go/common/mterrors/`** — PostgreSQL SQLSTATE codes and `PgDiagnostic` builders (`NewFeatureNotSupported`, `NewPgError`, etc.). Don't construct error strings ad-hoc.
- **`go/common/protoutil/`** — proto message constructors (`NewPreparedStatement`, `NewPortal`).
- **`go/common/pgprotocol/`** — wire protocol (`server/`, `client/`, `scram/`, `protocol/` codes).

When in doubt, `grep -r '<concept>' go/common/` before adding a new definition. Reuse keeps wire-protocol constants and PG semantics consistent across the codebase.

## Terminology: roles and the state they derive from

"Primary" is overloaded in distributed databases. Multigres de-conflates it into
three independent notions, each derived from its own source of truth:

| Notion                                            | Source of truth                                                    | How to derive                                                                                                                                                                 | Use it for                                                               |
| ------------------------------------------------- | ------------------------------------------------------------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------ |
| **consensus role** — leader / follower / observer | `ConsensusStatus` (the pooler's view of the highest rule it knows) | `commonconsensus.SelfConsensusRole(cs)`: **leader** = that rule names self; **follower** = self is in that rule's cohort; **observer** = not a cohort member (or no rule yet) | orchestration, failover, HA, and tests asserting who the cluster elected |
| **postgres role** — primary / standby             | PostgreSQL recovery mode                                           | `pg_is_in_recovery()` (e.g. `isInRecovery`)                                                                                                                                   | directly observing or configuring postgres replication                   |
| **routing role** — PRIMARY / REPLICA              | `RoutingState` (the writability self-report)                       | `StateManager.RoutingRole()`; persisted in `MultiPooler.routing_state`, streamed to the gateway                                                                               | the gateway's "is there a writable leader, and which one" decision       |

Key points:

- **`ConsensusStatus` + `ConsensusRole`** answer _"what does consensus say this node is?"_ Prefer `SelfConsensusRole` so follower and observer stay distinct (treating a non-cohort observer as a follower is a bug). `NamesSelfAsLeader` is a leader-only shim slated for deprecation — don't reach for it in new code.
- **`RoutingState` + `RoutingRole`** answer _"is this the writable leader to route to?"_ — the conjunction of postgres-out-of-recovery + committed + non-revoked + highest-known leader. Gateway and backup code use this, not the consensus role.
- The legacy stored `PoolerType` label is **deprecated** and being removed; derive the consensus role or routing role above instead, and don't add new references to it.

Quick check: consensus/HA/election → consensus role (`SelfConsensusRole`); postgres replication mechanics → primary/standby (`isInRecovery`); gateway routing/writability → routing role (`RoutingRole`).

## Generated Files

Files with `// Code generated` comments should not be edited directly. Regenerate with `make proto` (protobufs) or `make parser` (SQL parser/AST). When debugging, trace to source files instead of analyzing generated code.
