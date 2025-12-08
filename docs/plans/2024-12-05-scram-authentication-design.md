# SCRAM Authentication and Connection Pooling Design

## Quality Standards

This is **critical enterprise infrastructure**. The implementation must be:

- **Correct**: Authentication must never allow unauthorized access. Edge cases must be handled. The SCRAM protocol implementation must match RFC 5802 exactly.
- **Secure**: No privilege escalation paths. Defense in depth. All security assumptions documented and tested.
- **Scalable**: Connection pooling must support thousands of concurrent users without per-user connection overhead.
- **Performant**: Authentication should not add measurable latency to connection establishment. Hash caching, efficient crypto operations.
- **Thoroughly tested**: Unit tests for all crypto operations with known test vectors. Integration tests for the full auth flow. Security-focused tests that attempt bypass. Fuzz testing for protocol parsing.

Every code path must be tested. Security-critical code must have additional review.

## Overview

Implement PostgreSQL SCRAM-SHA-256 authentication in multigres with secure connection pooling that allows a shared pool to serve multiple authenticated users without privilege escalation.

## Problem Statement

multigres currently uses "trust" authentication - any client can connect as any user without a password. To be production-ready, we need:

1. **Real authentication**: Verify client identity using PostgreSQL's SCRAM-SHA-256 protocol
2. **Connection pooling compatibility**: Share backend connections across authenticated users without leaking privileges
3. **Performance**: Avoid PostgreSQL round-trips during authentication by verifying passwords locally

## Design Decisions and Alternatives Considered

### Decision 1: Where to Perform SCRAM Authentication

**Options considered:**

| Option                                | Description                     | Pros                   | Cons                                           |
| ------------------------------------- | ------------------------------- | ---------------------- | ---------------------------------------------- |
| **A. Passthrough to PostgreSQL**      | Proxy SCRAM messages to backend | Simple, always in sync | Defeats pooling - need new connection per auth |
| **B. multigateway with local hashes** | Fetch hashes, verify locally    | Fast, pool-friendly    | Need hash sync mechanism                       |
| **C. multipooler performs auth**      | Auth happens at pooler level    | Single auth point      | Requires PostgreSQL protocol at multipooler    |

**Chosen: Option B** - multigateway performs SCRAM authentication using password hashes fetched from multipooler.

**Rationale:**

- Matches pgbouncer's approach (`auth_query` fetches hashes for local verification)
- Avoids PostgreSQL connection overhead during authentication
- multigateway already speaks PostgreSQL wire protocol
- Clean separation: multigateway handles client protocol, multipooler handles pooling

**How pgbouncer does it:**
pgbouncer uses `auth_query` to fetch password hashes:

```ini
auth_type = scram-sha-256
auth_query = SELECT usename, passwd FROM pg_shadow WHERE usename=$1
```

It then performs SCRAM verification locally. We'll do the same, but fetch via gRPC from multipooler rather than direct PostgreSQL query.

---

### Decision 2: Connection Pool Isolation Strategy

**The core problem:** How do we let multiple users share backend connections without privilege escalation?

**Options considered:**

| Option                                   | Description              | Pros                   | Cons                                       |
| ---------------------------------------- | ------------------------ | ---------------------- | ------------------------------------------ |
| **A. Separate pools per user**           | Each user gets own pool  | Simple, secure         | N users × M connections = many connections |
| **B. SET ROLE sandboxing**               | Use SET ROLE to restrict | Fewer connections      | RESET ROLE escapes to pool connector       |
| **C. SET SESSION AUTHORIZATION**         | Change session user      | Full isolation         | Requires superuser; escape via RESET       |
| **D. SET SESSION AUTH + proxy blocking** | C with SQL filtering     | Full isolation, secure | Complexity in SQL filtering                |

**Chosen: Option D** - SET SESSION AUTHORIZATION with proxy-level SQL filtering.

**Rationale:**

The key insight from our analysis: **PostgreSQL cannot provide a one-way sandbox**. After `SET SESSION AUTHORIZATION 'alice'`, the connection can `RESET SESSION AUTHORIZATION` back to the superuser because PostgreSQL checks the _authenticated user's_ privileges, not the _current user's_.

Therefore, the proxy must be the sandbox enforcement layer:

- Block `SET SESSION AUTHORIZATION` entirely (superuser-only command anyway)
- Rewrite `RESET SESSION AUTHORIZATION` to `SET SESSION AUTHORIZATION '<authenticated_user>'`
- The user can never access the pool connector's superuser privileges

**Why not SET ROLE?**
`SET ROLE` has a different problem:

```sql
-- Connect as pool_admin, SET ROLE to alice
SET ROLE 'alice';
-- RESET ROLE goes back to pool_admin, not alice!
RESET ROLE;  -- Now pool_admin again - escaped!
```

Also, after `SET ROLE alice`, the `session_user` is still `pool_admin`, so `SET ROLE` to other roles checks against pool_admin's memberships, not alice's.

**SET SESSION AUTHORIZATION is better:**

```sql
SET SESSION AUTHORIZATION 'alice';
-- Now session_user = alice
-- SET ROLE restricted to alice's memberships
-- RESET ROLE returns to alice (the session user)
```

The only escape is `RESET SESSION AUTHORIZATION`, which we block at the proxy.

**How pgbouncer handles this:**
pgbouncer does NOT do role assumption. It either:

1. Opens separate connections per user, or
2. Uses transaction pooling where a single database user is configured

pgbouncer cannot safely share connections across users with role assumption because it doesn't parse SQL (can't block RESET SESSION AUTHORIZATION).

**multigres advantage:** We have a full SQL parser, so we CAN safely implement shared pools with role assumption.

#### Prior Art: How Other PostgreSQL Proxies Handle This

**Important finding (December 2024):** No major PostgreSQL proxy uses shared pools with user switching. All use per-user connection pools:

| Proxy                                                                           | Architecture                    | Source                                                                                                                    |
| ------------------------------------------------------------------------------- | ------------------------------- | ------------------------------------------------------------------------------------------------------------------------- |
| **[PgBouncer](http://www.pgbouncer.org/config.html)**                           | One pool per user+database pair | "PgBouncer tries to log into the destination database with client username, meaning that there will be one pool per user" |
| **[Supavisor](https://supabase.com/blog/supavisor-postgres-connection-pooler)** | Per-tenant pools                | Each tenant gets their own pool of database connections                                                                   |
| **[PgCat](https://github.com/postgresml/pgcat)**                                | Per user+database pools         | "Each database/user combination will be a separate connection pool"                                                       |
| **[Odyssey](https://github.com/yandex/odyssey)**                                | Per user+database pools         | "Allows to define connection pools as a pair of Database and User"                                                        |

**Why shared pools with SET SESSION AUTHORIZATION are problematic:**

Per [Stack Overflow security discussion](https://stackoverflow.com/questions/77272972/enforcing-row-level-security-in-a-shared-postgres-pool):

> "The problem is that this tenant user can himself do a 'SET SESSION AUTHORIZATION some_other_tenant' and then Row security doesn't matter."

> "The problem is a fundamental one: you handle authentication in the application, not in the database."

**Security vectors that are difficult to mitigate:**

1. **Direct SQL escape**: User sends `RESET SESSION AUTHORIZATION` or `SET SESSION AUTHORIZATION 'admin'`
   - Mitigation: SQL parsing/rewriting (adds latency, complexity)

2. **Stored procedure escape**: `SECURITY DEFINER` function contains escape command
   - Mitigation: Requires PostgreSQL extension hook (complex deployment)

3. **Multi-statement injection**: `RESET SESSION AUTHORIZATION; DROP TABLE users`
   - Mitigation: Parse all statements, filter each one

4. **Dynamic SQL in functions**: `EXECUTE 'RESET SESSION AUTHORIZATION'`
   - Mitigation: Cannot be caught by proxy-level filtering

#### Tradeoffs: Per-User Pools vs Shared Pool with User Swapping

| Factor                | Per-User Pools (Option A)        | Shared Pool + SET SESSION AUTH (Option D)                            |
| --------------------- | -------------------------------- | -------------------------------------------------------------------- |
| **Security**          | ✅ Complete isolation by design  | ⚠️ Requires SQL filtering + extension for SECURITY DEFINER functions |
| **Connection count**  | ❌ N users × M connections       | ✅ Single shared pool                                                |
| **Complexity**        | ✅ Simple, no SQL parsing needed | ❌ Requires SQL parser integration                                   |
| **Latency**           | ✅ No per-query overhead         | ⚠️ SET/RESET overhead per query + SQL parsing                        |
| **Industry adoption** | ✅ Used by all major proxies     | ❌ Not used in production by any major proxy                         |
| **Stored procedures** | ✅ User's permissions apply      | ⚠️ Pool user's permissions unless extension installed                |

#### Pool Sizing Configuration in Major Proxies

Since all major proxies use per-user pools, understanding how they handle pool sizing is critical:

##### PgBouncer Pool Sizing ([config docs](http://www.pgbouncer.org/config.html))

**Global settings:**
| Parameter | Default | Description |
|-----------|---------|-------------|
| `default_pool_size` | 20 | Server connections per user/database pair |
| `min_pool_size` | 0 | Minimum connections to maintain (when clients connected) |
| `reserve_pool_size` | 0 | Additional connections for demand spikes |
| `reserve_pool_timeout` | 5s | Seconds before using reserve pool |
| `max_client_conn` | 100 | Maximum client connections allowed |
| `max_user_connections` | 0 | Max server connections per user (all databases) |
| `max_db_connections` | 0 | Max server connections per database (all users) |

**Per-database overrides:** `pool_size`, `min_pool_size`, `reserve_pool_size`, `max_db_connections`

**Per-user overrides:** `pool_size`, `reserve_pool_size`, `max_user_connections`

**Sizing formula:** With `default_pool_size=20` and `max_client_conn=100`, 5 distinct user/database pairs can each max out their pools before hitting limits.

**Note:** There's a [longstanding feature request](https://github.com/pgbouncer/pgbouncer/issues/166) for finer-grained per-user pool size configuration. Current workarounds include running multiple PgBouncer instances.

##### Supavisor Pool Sizing ([source](https://github.com/supabase/supavisor))

Supavisor organizes pools hierarchically: **Tenant** → **User** → **Pool**.

- **Tenant** = A Supabase project (maps to a database server: `db_host`, `db_port`, `db_database`)
- **User** = A PostgreSQL database user within that tenant
- **Pool identity** = `{tenant, user, mode, db_name}` - each combination gets its own pool

**Per-tenant settings** (defaults for users in this tenant):
| Parameter | Default | Description |
|-----------|---------|-------------|
| `default_pool_size` | 15 | Database connections per user pool |
| `default_max_clients` | 1000 | Client connections limit per user |

**Per-user settings** (overrides tenant defaults):
| Parameter | Description |
|-----------|-------------|
| `pool_size` | Override tenant's `default_pool_size` for this user |
| `max_clients` | Override tenant's `default_max_clients` for this user |
| `mode_type` | `:transaction` or `:session` pooling |
| `pool_checkout_timeout` | Max wait time for a connection |

**Key behavior:**

- Pools are created dynamically on first client connection
- Each pool connects to PostgreSQL **as the actual user** (not a superuser)
- Session mode limits connections to `pool_size` (one client per connection)
- Transaction mode allows `max_clients` concurrent clients sharing `pool_size` connections

##### PgCat Pool Sizing ([CONFIG.md](https://github.com/postgresml/pgcat/blob/main/CONFIG.md))

**Per-user settings:**
| Parameter | Default | Description |
|-----------|---------|-------------|
| `pool_size` | 9 | Max server connections for this user |
| `min_pool_size` | 0 | Minimum idle connections to retain |
| `connect_timeout` | 1000ms | How long client waits before aborting |
| `idle_timeout` | 30000ms | Duration idle connections remain available |
| `statement_timeout` | 0 | Max query duration (0 = disabled) |

**Total connections:** Sum of `pool_size` across all users must fit within PostgreSQL `max_connections`.

**Sizing guidance:** Set `pool_size x users` to fit within Postgres `max_connections` with headroom. Common practice: 25-50% of total connections.

##### Odyssey Pool Sizing ([GitHub](https://github.com/yandex/odyssey))

**Per user+database pair:**
| Parameter | Description |
|-----------|-------------|
| `pool_size` | Max connections in pool (0 = dynamic) |
| `client_max` | Max concurrent client connections |
| `pool_timeout` | Timeout for pool operations |
| `pool_ttl` | Connection lifetime in pool |
| `pool_client_idle_timeout` | Idle client timeout |
| `pool_idle_in_transaction_timeout` | Idle-in-transaction timeout |
| `server_lifetime` | Backend connection max age |

**Dynamic behavior:** `pool_size 0` enables dynamic pool sizing.

**Sizing formula:** `default_pool_size = (RAM / 20MB) / databases`, max 100-200 per server.

#### Decision: Use Per-User Connection Pools

**Chosen approach:** Per-user connection pools (matching industry practice).

**Rationale:**

1. All major PostgreSQL proxies use per-user pools
2. Security model is simple and proven in production
3. No risk of privilege escalation via SQL injection or stored procedures
4. Lower complexity - no SQL parsing required in the connection pool path

**Future option:** Keep the shared pool + SET SESSION AUTHORIZATION approach available for potential future use if/when we develop a secure implementation (likely requiring a PostgreSQL extension to block session auth commands inside stored procedures).

**Hybrid approach:**

- Per-user pools for authenticated client connections (secure, simple)
- Shared superuser pool for internal/trusted operations only (multiorch, admin tasks)

#### Pool Sizing Recommendations for Multigres

Based on prior art, multigres should support:

**Global settings:**

- `default_pool_size` - Per user+database pool size (default: 10-20)
- `max_user_connections` - Hard limit per user across all databases
- `max_db_connections` - Hard limit per database across all users
- `max_client_conn` - Total client connection limit

**Per-user/database overrides:**

- `pool_size` - Override default for specific users/databases
- `min_pool_size` - Keep warm connections for frequently-used pools
- `reserve_pool_size` - Burst capacity for demand spikes

**Dynamic sizing (future):**

- Start with 0 connections, grow on demand
- Shrink idle pools after timeout
- Auto-size based on PostgreSQL `max_connections`

---

### Decision 3: Stored Procedure Escape Vector

**The problem:** A stored procedure could contain `RESET SESSION AUTHORIZATION` that the proxy never sees.

**Analysis:**
If a `SECURITY DEFINER` function owned by a superuser contains `RESET SESSION AUTHORIZATION`, it will succeed because PostgreSQL checks the _authenticated user_ (pool superuser), not the current user.

**Options considered:**

| Option                          | Description                    | Pros                   | Cons                       |
| ------------------------------- | ------------------------------ | ---------------------- | -------------------------- |
| **A. Accept and document**      | Known limitation               | Simple                 | Security gap               |
| **B. Audit all functions**      | Scan for dangerous commands    | Reduces risk           | Not foolproof              |
| **C. PostgreSQL extension**     | Hook ExecutorStart             | Bulletproof            | Requires extension install |
| **D. Restrict CREATE FUNCTION** | Limit who can create functions | Reduces attack surface | Limits functionality       |

**Chosen: Option A for now, with Option C as future enhancement**

**Rationale:**

- Most deployments don't have user-created SECURITY DEFINER functions
- Attack requires: (1) ability to create function, (2) function owner is superuser, (3) function contains escape command
- Proxy-level blocking handles the common case (direct SQL)
- PostgreSQL extension can be added later for high-security deployments

**Future PostgreSQL extension design:**

```c
// In extension initialization
ExecutorStart_hook = multigres_executor_start;

static void multigres_executor_start(QueryDesc *queryDesc, int eflags) {
    // Check if sandbox is enabled via GUC
    if (multigres_sandbox_enabled) {
        // Inspect planned statement for SET/RESET SESSION AUTHORIZATION
        // Block if found
    }
    // Call previous hook or standard function
}
```

---

### Decision 4: Password Hash Source

**Options considered:**

| Option                            | Description                        | Pros                   | Cons                                |
| --------------------------------- | ---------------------------------- | ---------------------- | ----------------------------------- |
| **A. Query pg_authid directly**   | multigateway queries PostgreSQL    | Single source of truth | Requires DB connection from gateway |
| **B. Fetch via multipooler gRPC** | multipooler provides hash endpoint | Clean separation       | Extra hop                           |
| **C. External credential store**  | Vault, etcd, config file           | Flexible               | Sync complexity, divergence risk    |

**Chosen: Option B** - multipooler provides gRPC endpoint for credential fetching.

**Rationale:**

- multipooler already connects to PostgreSQL as superuser
- multigateway doesn't need direct PostgreSQL access
- Credential caching can happen at multigateway
- Clean architecture: multipooler is source of truth for all PostgreSQL state

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                           Client                                    │
│                    (psql, application, etc.)                        │
└─────────────────────────────────────────────────────────────────────┘
                                │
                                │ PostgreSQL wire protocol
                                │ SCRAM-SHA-256 authentication
                                ▼
┌─────────────────────────────────────────────────────────────────────┐
│                        multigateway                                 │
│  • Accepts PostgreSQL connections                                   │
│  • Performs SCRAM authentication using hashes from multipooler      │
│  • Routes queries to appropriate shards                             │
│  • Trusted to assert authenticated identity to multipooler          │
│  • Caches password hashes with TTL                                  │
└─────────────────────────────────────────────────────────────────────┘
                                │
                                │ gRPC (authenticated session info)
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────────┐
│                        multipooler                                  │
│  • Source of truth for credentials (fetches from PostgreSQL)        │
│  • Maintains shared connection pool (superuser connections)         │
│  • Sandboxes queries via SET SESSION AUTHORIZATION                  │
│  • Enforces session auth restrictions at SQL level                  │
│  • Rewrites RESET SESSION AUTHORIZATION                             │
└─────────────────────────────────────────────────────────────────────┘
                                │
                                │ PostgreSQL wire protocol
                                │ (superuser connection)
                                ▼
┌─────────────────────────────────────────────────────────────────────┐
│                         PostgreSQL                                  │
└─────────────────────────────────────────────────────────────────────┘
```

## Component Responsibilities

### multigateway

- Accepts client connections via PostgreSQL wire protocol
- Performs SCRAM-SHA-256 authentication:
  - Fetches password hashes from multipooler via gRPC
  - Executes SCRAM handshake locally (no PostgreSQL round-trip during auth)
  - Caches hashes with TTL for performance
- Passes authenticated identity (user, database) to multipooler with each request
- Routes queries across multiple shards/table groups

### multipooler

- Source of truth for authentication credentials
- Provides gRPC endpoint to fetch SCRAM password hashes
- Maintains shared connection pool using superuser PostgreSQL connections
- Sandboxes each request:
  - On request start: `SET SESSION AUTHORIZATION '<authenticated_user>'`
  - On request end: `RESET SESSION AUTHORIZATION`
- Enforces SQL-level restrictions:
  - Block `SET SESSION AUTHORIZATION` (superuser-only command)
  - Rewrite `RESET SESSION AUTHORIZATION` → `SET SESSION AUTHORIZATION '<auth_user>'`

---

## Part 1: SCRAM-SHA-256 Protocol Implementation

### SCRAM Protocol Overview

SCRAM (Salted Challenge Response Authentication Mechanism) is defined in RFC 5802. PostgreSQL uses SCRAM-SHA-256.

**Why SCRAM over MD5?**

- MD5 is cryptographically broken
- SCRAM uses PBKDF2 with configurable iterations (default 4096)
- SCRAM provides mutual authentication (server proves it knows password too)
- SCRAM protects against replay attacks via nonces

### Protocol Flow

```
Client                          Server (multigateway)
   │                                   │
   │──── StartupMessage ──────────────►│  (user, database)
   │                                   │
   │◄──── AuthenticationSASL ──────────│  (mechanisms: SCRAM-SHA-256)
   │                                   │
   │───── SASLInitialResponse ────────►│  (client-first-message)
   │      n,,n=user,r=client-nonce     │
   │                                   │
   │◄──── AuthenticationSASLContinue ──│  (server-first-message)
   │      r=client+server-nonce,       │
   │      s=salt,i=iterations          │
   │                                   │
   │───── SASLResponse ───────────────►│  (client-final-message)
   │      c=channel-binding,           │
   │      r=combined-nonce,            │
   │      p=client-proof               │
   │                                   │
   │◄──── AuthenticationSASLFinal ─────│  (server-final-message)
   │      v=server-signature           │
   │                                   │
   │◄──── AuthenticationOk ────────────│
   │                                   │
   │◄──── ParameterStatus (multiple) ──│
   │◄──── BackendKeyData ──────────────│
   │◄──── ReadyForQuery ───────────────│
```

### SCRAM Cryptographic Operations

```
SaltedPassword  := Hi(Normalize(password), salt, i)
                   where Hi = PBKDF2-HMAC-SHA256

ClientKey       := HMAC(SaltedPassword, "Client Key")
StoredKey       := H(ClientKey)  // SHA-256
ServerKey       := HMAC(SaltedPassword, "Server Key")

AuthMessage     := client-first-message-bare + "," +
                   server-first-message + "," +
                   client-final-message-without-proof

ClientSignature := HMAC(StoredKey, AuthMessage)
ClientProof     := ClientKey XOR ClientSignature
ServerSignature := HMAC(ServerKey, AuthMessage)
```

**Verification:**

1. Server has StoredKey and ServerKey (from password hash)
2. Client sends ClientProof
3. Server computes: `ClientKey = ClientProof XOR HMAC(StoredKey, AuthMessage)`
4. Server verifies: `H(ClientKey) == StoredKey`
5. Server sends ServerSignature for mutual auth

### PostgreSQL Password Hash Format

PostgreSQL stores SCRAM hashes in `pg_authid.rolpassword`:

```
SCRAM-SHA-256$<iterations>:<salt-base64>$<StoredKey-base64>:<ServerKey-base64>
```

Example:

```
SCRAM-SHA-256$4096:abc123base64salt$StoredKeyBase64:ServerKeyBase64
```

### Implementation Location

New package: `go/pgprotocol/auth/`

**Files:**

- `scram.go` - SCRAM message parsing and generation
- `scram_crypto.go` - Cryptographic operations (PBKDF2, HMAC, etc.)
- `password.go` - PostgreSQL password hash parsing
- `authenticator.go` - Authentication state machine

### Test-Driven Development Approach

**Test 1: Password hash parsing**

```go
func TestParseScramHash(t *testing.T) {
    hash := "SCRAM-SHA-256$4096:c2FsdA==$storedkey:serverkey"
    parsed, err := ParseScramHash(hash)
    require.NoError(t, err)
    assert.Equal(t, 4096, parsed.Iterations)
    assert.Equal(t, []byte("salt"), parsed.Salt)
    // ...
}
```

**Test 2: Client-first-message parsing**

```go
func TestParseClientFirstMessage(t *testing.T) {
    msg := "n,,n=user,r=fyko+d2lbbFgONRv9qkxdawL"
    parsed, err := ParseClientFirstMessage(msg)
    require.NoError(t, err)
    assert.Equal(t, "user", parsed.Username)
    assert.Equal(t, "fyko+d2lbbFgONRv9qkxdawL", parsed.Nonce)
}
```

**Test 3: Full SCRAM handshake**

```go
func TestScramHandshake(t *testing.T) {
    // Known test vectors from RFC 5802
    password := "pencil"
    salt := []byte{...}
    iterations := 4096

    auth := NewScramAuthenticator(storedKey, serverKey, salt, iterations)

    // Simulate client-first
    serverFirst, err := auth.ProcessClientFirst("n,,n=user,r=clientnonce")
    require.NoError(t, err)

    // Simulate client-final with correct proof
    serverFinal, err := auth.ProcessClientFinal(clientFinalWithValidProof)
    require.NoError(t, err)

    assert.True(t, auth.IsAuthenticated())
}
```

**Test 4: Integration with startup flow**

```go
func TestStartupWithScram(t *testing.T) {
    // Start test server with mock credential provider
    server := NewTestServer(WithCredentialProvider(mockProvider))

    // Connect with psql or pgx
    conn, err := pgx.Connect(ctx, "postgres://user:password@localhost/db")
    require.NoError(t, err)

    // Verify connection works
    var result int
    err = conn.QueryRow(ctx, "SELECT 1").Scan(&result)
    require.NoError(t, err)
}
```

---

## Part 2: Credential Fetching

### gRPC Service Extension

Add to `proto/multipoolerservice.proto`:

```protobuf
message GetAuthCredentialsRequest {
  string database = 1;
  string username = 2;
}

message GetAuthCredentialsResponse {
  // SCRAM-SHA-256$iterations:salt$StoredKey:ServerKey
  string scram_hash = 1;
  bool user_exists = 2;
}

service PoolerGateway {
  // Existing methods...

  // Fetch authentication credentials for a user
  rpc GetAuthCredentials(GetAuthCredentialsRequest)
      returns (GetAuthCredentialsResponse);
}
```

### multipooler Implementation

Query PostgreSQL for password hash:

```sql
SELECT rolpassword FROM pg_authid WHERE rolname = $1
```

**Note:** Requires superuser or `pg_read_all_settings` role to read `pg_authid`. The pool connector already has superuser access.

### multigateway Credential Cache

#### Design Decision: How multigateway Obtains Password Hashes

**Options considered:**

| Option                              | Description                                       | Pros                                 | Cons                                     |
| ----------------------------------- | ------------------------------------------------- | ------------------------------------ | ---------------------------------------- |
| **A. On-demand fetch**              | Fetch hash when client connects, cache with TTL   | Simple, always fresh                 | Latency on first connection per user     |
| **B. Pre-cache via polling**        | Periodically fetch all hashes                     | Fast auth, no per-connection latency | Stale data window, fetches unused hashes |
| **C. Pre-cache via streaming**      | multipooler pushes hash updates                   | Real-time updates, fast auth         | Complex, connection management           |
| **D. Authenticated fetch**          | Gateway must prove it has credentials to get hash | Most secure, defense in depth        | Chicken-and-egg problem                  |
| **E. Treat hashes as non-precious** | Pre-load all hashes freely                        | Simplest, fastest                    | Broader exposure of hashes               |

**How pgbouncer does it:**
pgbouncer uses `auth_file` or `auth_query` to pre-load all user credentials at startup and refresh them periodically. With `auth_query`, it runs a query like `SELECT usename, passwd FROM pg_shadow` to fetch all hashes. This is Option B/E - pre-fetching all hashes, treating them as non-precious from a distribution standpoint.

**Analysis:**

**Option A (On-demand fetch)** is the simplest starting point:

- First connection for a user incurs one gRPC round-trip to multipooler
- Subsequent connections use cached hash until TTL expires
- Auth failure invalidates cache (handles password changes)
- Latency is ~1-2ms for gRPC call, acceptable for connection establishment

**Option B (Pre-cache via polling)** matches pgbouncer's approach:

- Gateway periodically fetches all hashes (e.g., every 30 seconds)
- Zero latency on connection auth
- Risk of stale data if password changes between polls
- May fetch hashes for users who never connect through this gateway

**Option D (Authenticated fetch)** has a chicken-and-egg problem:

- To verify the client's password, we need the hash
- To get the hash securely, we'd need to verify... something
- One solution: gateway authenticates to multipooler with its own credentials (mTLS or shared secret), then is trusted to fetch hashes
- This doesn't prove the gateway has a "valid reason" (client credentials) - it just proves the gateway is authorized

**Regarding hash sensitivity:**

- SCRAM hashes are not plaintext passwords
- However, they ARE password-equivalent for authentication to this system
- An attacker with a hash can compute the SCRAM proof and authenticate
- They're designed to be slow to brute-force (PBKDF2 with 4096 iterations) but not to be public

**Recommendation: Start with Option A (on-demand fetch with caching)**

Rationale:

1. Simple to implement and reason about
2. Minimizes hash exposure (only fetched when needed)
3. Cache TTL provides reasonable freshness vs performance tradeoff
4. Gateway-to-multipooler should use mTLS for transport security regardless
5. Can evolve to Option B (polling) or Option C (streaming) later if latency becomes an issue

**Is this easy to change later?**

**Yes, this is relatively easy to change.** The credential cache is internal to multigateway. The interface to multipooler (GetAuthCredentials gRPC) remains the same regardless of caching strategy. We can:

- Add pre-loading without changing multipooler (just call GetAuthCredentials for all users)
- Add a new `StreamAuthCredentials` gRPC method for push-based updates
- Change TTL or caching behavior without protocol changes

The key architectural decision that's harder to change is **where authentication happens** (multigateway vs multipooler). The caching strategy is an optimization that can evolve.

---

#### Design Decision: Password Change Detection

When a user's password changes in PostgreSQL, how quickly should existing connections be affected, and how do we detect the change?

**The problem:**

- Client authenticates with password P1 at time T0
- Admin changes password to P2 at time T1
- Client continues using connection established at T0
- Should the connection continue working? For how long?

**How PostgreSQL handles it:**
PostgreSQL does NOT disconnect existing connections when passwords change. Once authenticated, a connection remains valid until closed. This is standard behavior that applications depend on.

**How pgbouncer handles it:**
pgbouncer periodically re-queries `auth_query` (default: every 60 seconds). New connections must use the new password, but existing connections continue working.

**Options for multigres:**

| Option                            | Description                                                   | Behavior                                         | Tradeoffs                                          |
| --------------------------------- | ------------------------------------------------------------- | ------------------------------------------------ | -------------------------------------------------- |
| **1. TTL-based cache expiry**     | Cache expires, next auth uses new hash                        | New connections use new password after TTL       | Simple; existing connections unaffected            |
| **2. Auth failure invalidation**  | Wrong password clears cache, retries                          | Self-healing on password mismatch                | May cause brief auth failures during transition    |
| **3. Streaming hash updates**     | multipooler monitors pg_authid, pushes changes                | Near-real-time for new connections               | Complex; still doesn't affect existing connections |
| **4. Hash version token**         | Include hash version in session; multipooler can reject stale | Enables forced re-auth on password change        | More complex; diverges from PostgreSQL semantics   |
| **5. Periodic re-authentication** | Existing connections must re-auth periodically                | Can enforce password changes on long connections | Breaks PostgreSQL compatibility                    |

**Option 4 (Hash version token) in detail:**

This is an interesting approach for security-sensitive deployments:

```protobuf
message GetAuthCredentialsResponse {
  string scram_hash = 1;
  string hash_version = 2;  // e.g., SHA256 of (username, hash, timestamp)
  bool user_exists = 3;
}

message StreamExecuteRequest {
  CallerID caller_id = 1;
  string auth_hash_version = 2;  // Version used when authenticating
  // ...
}
```

multipooler could then:

- Track current hash versions for all users
- Optionally reject requests with stale `auth_hash_version`
- Return an error code indicating "re-authentication required"
- Gateway would need to force the client to reconnect

**Tradeoffs of hash version approach:**

- Pro: Enables immediate password change enforcement
- Pro: Provides audit trail of which password version was used
- Con: Breaks PostgreSQL semantics (connections don't expire on password change)
- Con: Requires client applications to handle reconnection gracefully
- Con: Could cause cascading disconnections if many clients authenticated with old password

**Streaming password change notification:**

multipooler could monitor PostgreSQL for password changes:

```sql
-- Poll for changes (simple)
SELECT rolname, rolpassword FROM pg_authid
WHERE rolpassword IS DISTINCT FROM cached_value;

-- Or use LISTEN/NOTIFY with a trigger (real-time)
CREATE FUNCTION notify_password_change() RETURNS trigger AS $$
BEGIN
  PERFORM pg_notify('password_changed', NEW.rolname);
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;
```

Then push to gateways via streaming gRPC:

```protobuf
rpc StreamCredentialUpdates(StreamCredentialUpdatesRequest)
    returns (stream CredentialUpdate);

message CredentialUpdate {
  string username = 1;
  oneof update {
    string new_scram_hash = 2;
    bool user_deleted = 3;
  }
}
```

**Recommendation for initial implementation:**

1. **Start with Option 1+2 (TTL + auth failure invalidation)**
   - Simple, matches pgbouncer behavior
   - Existing connections continue working (PostgreSQL-compatible)
   - New connections get new password within TTL window

2. **Design the protocol to support Option 4 later**
   - Include `hash_version` in GetAuthCredentialsResponse
   - Include `auth_hash_version` in request messages
   - Initially, multipooler ignores version (doesn't enforce)
   - Can enable enforcement later for security-sensitive deployments

3. **Consider streaming updates as a future optimization**
   - Useful when TTL-based polling creates too much load
   - Useful for near-instant password change propagation

**Is this easy to change later?**

**Mostly yes, but the protocol should be designed upfront:**

- Adding `hash_version` fields later requires proto changes and client updates
- The enforcement policy (ignore vs reject stale) is easy to change
- Streaming updates can be added as a new gRPC method

**Recommendation:** Include `hash_version` in the proto from the start, even if we don't enforce it initially. This gives us flexibility to enable stricter password change enforcement later without protocol changes.

---

#### Cache Implementation

```go
type CredentialCache struct {
    mu      sync.RWMutex
    entries map[credentialKey]*credentialEntry
    ttl     time.Duration
}

type credentialKey struct {
    database string
    username string
}

type credentialEntry struct {
    scramHash   string
    hashVersion string    // For future password change enforcement
    fetchedAt   time.Time
    userExists  bool
}
```

**Cache behavior:**

- TTL default: 60 seconds (configurable)
- Invalidate on auth failure (password may have changed)
- Negative caching for non-existent users (prevents repeated lookups)
- No pre-loading in initial implementation
- hashVersion tracked for future enforcement capability

### Test-Driven Development Approach

**Test 1: gRPC endpoint returns hash**

```go
func TestGetAuthCredentials(t *testing.T) {
    // Setup test PostgreSQL with known user
    setupTestUser(t, "testuser", "testpassword")

    // Call gRPC endpoint
    resp, err := client.GetAuthCredentials(ctx, &GetAuthCredentialsRequest{
        Database: "testdb",
        Username: "testuser",
    })
    require.NoError(t, err)
    assert.True(t, resp.UserExists)
    assert.True(t, strings.HasPrefix(resp.ScramHash, "SCRAM-SHA-256$"))
}
```

**Test 2: Cache behavior**

```go
func TestCredentialCache(t *testing.T) {
    cache := NewCredentialCache(100 * time.Millisecond)

    // First fetch - cache miss
    hash1, err := cache.Get(ctx, "db", "user", fetcher)
    require.NoError(t, err)
    assert.Equal(t, 1, fetcher.CallCount())

    // Second fetch - cache hit
    hash2, err := cache.Get(ctx, "db", "user", fetcher)
    require.NoError(t, err)
    assert.Equal(t, 1, fetcher.CallCount()) // No additional call

    // Wait for TTL
    time.Sleep(150 * time.Millisecond)

    // Third fetch - cache expired
    hash3, err := cache.Get(ctx, "db", "user", fetcher)
    require.NoError(t, err)
    assert.Equal(t, 2, fetcher.CallCount())
}
```

---

## Part 3: Connection Pool Sandboxing

### Pool Connection Lifecycle

```
┌─────────────────────────────────────────────────────────────────┐
│                    Connection Pool (Idle)                       │
│         Connections as superuser, no session auth set           │
└─────────────────────────────────────────────────────────────────┘
                              │
                              │ Checkout for user 'alice'
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│              SET SESSION AUTHORIZATION 'alice'                  │
│                                                                 │
│  • session_user = alice                                         │
│  • current_user = alice                                         │
│  • SET ROLE restricted to alice's memberships                   │
│  • RESET ROLE returns to alice                                  │
└─────────────────────────────────────────────────────────────────┘
                              │
                              │ Execute user queries (filtered)
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                    SQL Security Filter                          │
│                                                                 │
│  BLOCK:                                                         │
│    • SET SESSION AUTHORIZATION (any form)                       │
│                                                                 │
│  REWRITE:                                                       │
│    • RESET SESSION AUTHORIZATION                                │
│      → SET SESSION AUTHORIZATION 'alice'                        │
└─────────────────────────────────────────────────────────────────┘
                              │
                              │ Return to pool
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│              RESET SESSION AUTHORIZATION                        │
│              (trusted path, bypasses filter)                    │
│                                                                 │
│              Connection returns to superuser state              │
└─────────────────────────────────────────────────────────────────┘
```

### SQL Filter Implementation

Location: `go/multipooler/security/session_auth_filter.go`

The SQL parser already recognizes `SET SESSION AUTHORIZATION` and `RESET SESSION AUTHORIZATION` as `VariableSetStmt` with `Name = "session_authorization"`.

```go
// FilterSessionAuthCommands checks parsed SQL for session authorization commands
// and either blocks or rewrites them based on the authenticated user.
func FilterSessionAuthCommands(stmts []ast.Stmt, authUser string) ([]ast.Stmt, error) {
    result := make([]ast.Stmt, len(stmts))
    copy(result, stmts)

    for i, stmt := range result {
        varSet, ok := stmt.(*ast.VariableSetStmt)
        if !ok {
            continue
        }

        if strings.ToLower(varSet.Name) != "session_authorization" {
            continue
        }

        switch varSet.Kind {
        case ast.VAR_RESET:
            // Rewrite: RESET SESSION AUTHORIZATION → SET SESSION AUTHORIZATION 'authUser'
            result[i] = &ast.VariableSetStmt{
                Kind: ast.VAR_SET_VALUE,
                Name: "session_authorization",
                Args: makeStringArg(authUser),
            }
        case ast.VAR_SET_VALUE, ast.VAR_SET_DEFAULT:
            // Block: SET SESSION AUTHORIZATION is not permitted
            return nil, fmt.Errorf("SET SESSION AUTHORIZATION is not permitted")
        }
    }

    return result, nil
}
```

### Test-Driven Development Approach

**Test 1: Block SET SESSION AUTHORIZATION**

```go
func TestBlockSetSessionAuthorization(t *testing.T) {
    stmts := parseSQL(t, "SET SESSION AUTHORIZATION 'mallory'")
    _, err := FilterSessionAuthCommands(stmts, "alice")
    assert.Error(t, err)
    assert.Contains(t, err.Error(), "not permitted")
}
```

**Test 2: Rewrite RESET SESSION AUTHORIZATION**

```go
func TestRewriteResetSessionAuthorization(t *testing.T) {
    stmts := parseSQL(t, "RESET SESSION AUTHORIZATION")
    result, err := FilterSessionAuthCommands(stmts, "alice")
    require.NoError(t, err)

    // Verify rewritten to SET SESSION AUTHORIZATION 'alice'
    varSet := result[0].(*ast.VariableSetStmt)
    assert.Equal(t, "session_authorization", varSet.Name)
    assert.Equal(t, ast.VAR_SET_VALUE, varSet.Kind)
    assert.Equal(t, "alice", extractStringArg(varSet.Args))
}
```

**Test 3: Allow normal SET commands**

```go
func TestAllowNormalSetCommands(t *testing.T) {
    stmts := parseSQL(t, "SET search_path TO public; SET timezone TO 'UTC'")
    result, err := FilterSessionAuthCommands(stmts, "alice")
    require.NoError(t, err)
    assert.Len(t, result, 2)
}
```

**Test 4: Allow SET ROLE (PostgreSQL validates)**

```go
func TestAllowSetRole(t *testing.T) {
    stmts := parseSQL(t, "SET ROLE 'some_role'")
    result, err := FilterSessionAuthCommands(stmts, "alice")
    require.NoError(t, err)
    assert.Len(t, result, 1)
    // SET ROLE is allowed - PostgreSQL will validate against alice's memberships
}
```

**Test 5: Integration - sandbox escape attempt**

```go
func TestSandboxEscapePrevented(t *testing.T) {
    // Connect as alice through the full stack
    conn := connectAsUser(t, "alice", "alicepassword")

    // Attempt to escape via SET SESSION AUTHORIZATION
    _, err := conn.Exec(ctx, "SET SESSION AUTHORIZATION 'postgres'")
    assert.Error(t, err)

    // Attempt to escape via RESET SESSION AUTHORIZATION
    _, err = conn.Exec(ctx, "RESET SESSION AUTHORIZATION")
    require.NoError(t, err) // This succeeds but...

    // Verify still alice (was rewritten)
    var user string
    conn.QueryRow(ctx, "SELECT session_user").Scan(&user)
    assert.Equal(t, "alice", user)
}
```

---

## Part 4: Identity Propagation

### Update Existing Proto Messages

The proto already has `caller_id` fields with TODOs. Populate them:

```go
// In go/multigateway/poolergateway/grpc_query_service.go
req := &poolerservice.StreamExecuteRequest{
    CallerId: &mtrpc.CallerID{
        Principal:    conn.User(),      // authenticated PostgreSQL role
        Component:    "multigateway",
        Subcomponent: conn.Database(),  // database name
    },
    // ... other fields
}
```

### multipooler Validation

```go
// In go/multipooler/grpcpoolerservice/service.go
func (s *Service) StreamExecute(req *StreamExecuteRequest, stream Stream) error {
    authUser := req.CallerId.GetPrincipal()
    if authUser == "" {
        return status.Error(codes.Unauthenticated, "missing caller identity")
    }

    // Get connection from pool
    conn := s.pool.Get()
    defer s.pool.Put(conn)

    // Sandbox the connection
    if err := conn.Exec("SET SESSION AUTHORIZATION $1", authUser); err != nil {
        return err
    }
    defer conn.Exec("RESET SESSION AUTHORIZATION") // Trusted path

    // Execute with filtering
    // ...
}
```

---

## Part 5: Future - Token-Based Authentication

### Vision (Not Implemented Initially)

For longer-lived, distributed authentication across multiple shards and table groups:

**Current flow (per-request credential check):**

```
Client → multigateway [SCRAM] → multipooler [fetch hash] → PostgreSQL
                                           ↓
                              For every new shard connection
```

**Future flow (token-based):**

```
Client → multigateway [SCRAM] → multipooler [issue token]
                ↓
         Token cached at multigateway
                ↓
         Reuse token for all shards in database
```

### Token Structure

```json
{
  "sub": "postgres_user_foo", // PostgreSQL role (for SET SESSION AUTH)
  "identity": "bob@company.com", // Real user identity (for audit logs)
  "databases": ["mydb"], // Authorized databases
  "exp": 1699900000, // Expiration timestamp
  "iat": 1699896400 // Issued at timestamp
}
```

### Token Lifecycle

1. **Issuance**: multipooler issues signed JWT after successful credential verification
2. **Caching**: multigateway caches token, reuses for connections to same database
3. **Refresh**: Opportunistic re-authentication on new connections before expiry
4. **Expiration**: Tokens expire, requiring re-authentication

### Handling Long-Lived Connections

PostgreSQL connections traditionally never expire - changing a password doesn't disconnect existing sessions. With tokens, we need a strategy:

**Options:**

1. **Opportunistic refresh**: On new connections, check if token is near expiry and re-authenticate
2. **Graceful expiration**: Allow existing connections to continue but fail new queries after expiry
3. **GoAway protocol**: Future PostgreSQL wire protocol extension to signal "please reconnect"
   - See: https://www.postgresql.org/message-id/flat/CAER375OvH3_ONmc-SgUFpA6gv_d6eNj2KdZktzo-f_uqNwwWNw@mail.gmail.com

### Benefits of Token-Based Auth

- Reduced gRPC calls for credential verification
- Audit trail with real identity (separate from PostgreSQL role)
- Cross-shard authentication with single credential exchange
- Foundation for OAuth/OIDC integration

---

## Security Model Summary

### Threat: Privilege Escalation via SQL

**Attack:** User sends `SET SESSION AUTHORIZATION 'admin'` or `RESET SESSION AUTHORIZATION`
**Mitigation:** SQL filter blocks SET, rewrites RESET

### Threat: Stored Procedure Escape

**Attack:** SECURITY DEFINER function contains `RESET SESSION AUTHORIZATION`
**Status:** Documented limitation for initial release
**Mitigation:**

- Short-term: Document as known limitation
- Long-term: PostgreSQL extension with ExecutorStart hook

### Threat: Connection Pool Identity Confusion

**Attack:** Connection returned to pool with wrong session user
**Mitigation:**

- Always `SET SESSION AUTHORIZATION` on checkout
- Always `RESET SESSION AUTHORIZATION` on return (trusted path)
- Superuser connections never exposed to user queries

### Threat: Hash Interception

**Attack:** Attacker intercepts password hash in transit
**Mitigation:**

- gRPC between multigateway and multipooler should use TLS
- Hashes are salted and use PBKDF2, not useful for rainbow tables

### Trust Boundaries

- multigateway is trusted to correctly assert authenticated identity
- multipooler is source of truth for credentials
- PostgreSQL superuser access limited to multipooler
- Clients are untrusted (all SQL is filtered)

---

## Implementation Phases

### Phase 1: SCRAM Protocol (TDD) ✅ COMPLETE

**Status:** Completed 2025-12-05

**Tests implemented:**

- `TestParseScramHash` - Parse PostgreSQL password hash format
- `TestParseClientFirstMessage` - Parse SCRAM client-first
- `TestGenerateServerFirstMessage` - Generate server-first with nonce
- `TestVerifyClientProof` - Cryptographic verification
- `TestFullScramHandshake` - End-to-end with test vectors
- `TestSCRAMEndToEnd` - Full integration test using lib/pq driver

**Files created:**

- `go/pgprotocol/auth/password.go` - Hash parsing (122 lines)
- `go/pgprotocol/auth/password_test.go` - Hash parsing tests (178 lines)
- `go/pgprotocol/auth/scram.go` - Message parsing/generation (288 lines)
- `go/pgprotocol/auth/scram_test.go` - Message tests (275 lines)
- `go/pgprotocol/auth/scram_crypto.go` - PBKDF2, HMAC operations (146 lines)
- `go/pgprotocol/auth/scram_crypto_test.go` - Crypto tests with RFC 5802 vectors (401 lines)
- `go/pgprotocol/auth/authenticator.go` - State machine (241 lines)
- `go/pgprotocol/auth/authenticator_test.go` - Authenticator tests (490 lines)
- `go/pgprotocol/server/scram_endtoend_test.go` - End-to-end tests with lib/pq (282 lines)

**Files modified:**

- `go/pgprotocol/server/startup.go` - SCRAM integration (~220 lines added)
- `go/pgprotocol/server/startup_test.go` - SASL message format tests (~170 lines added)
- `go/pgprotocol/server/listener.go` - Added PasswordHashProvider config

**Key interfaces:**

```go
// PasswordHashProvider abstracts credential storage for SCRAM authentication.
type PasswordHashProvider interface {
    GetPasswordHash(ctx context.Context, username string) (*ScramHash, error)
}

// ListenerConfig now accepts PasswordHashProvider for SCRAM auth.
// If nil, trust authentication is used (no password required).
```

**Test coverage:** 69+ unit tests in auth package, 5 SASL message tests, 3 end-to-end tests

### Phase 2: Credential Fetching (TDD) ✅ COMPLETE

**Status:** Completed 2025-12-05

**Tests implemented:**

- `TestGetAuthCredentials_NilPooler` - Returns UNAVAILABLE when pooler is nil
- `TestGetAuthCredentials_ValidatesRequest` - Validates username/database required
- `TestGetAuthCredentials_ExistingUser` - Fetches SCRAM hash for existing user
- `TestGetAuthCredentials_NonExistentUser` - Returns user_exists=false
- `TestGetAuthCredentials_PostgresUser` - Tests postgres superuser lookup
- `TestGetAuthCredentials_InvalidRequest` - Validates empty field rejection

**Files created:**

- `go/multipooler/grpcpoolerservice/service_test.go` - Unit tests
- `go/test/endtoend/multipooler/auth_credentials_test.go` - End-to-end tests

**Files modified:**

- `proto/multipoolerservice.proto` - Added GetAuthCredentials RPC and messages
- `go/multipooler/grpcpoolerservice/service.go` - Implemented GetAuthCredentials
- `go/pb/multipoolerservice/*.go` - Generated proto code

**Key implementation:**

```go
// GetAuthCredentials retrieves SCRAM hash from pg_catalog.pg_authid
rpc GetAuthCredentials(GetAuthCredentialsRequest) returns (GetAuthCredentialsResponse);

message GetAuthCredentialsResponse {
  string scram_hash = 1;   // SCRAM-SHA-256$iterations:salt:stored_key:server_key
  bool user_exists = 2;    // False if user not found
  int32 hash_version = 3;  // Version 1 = SCRAM-SHA-256
}
```

### Phase 3: Multigateway Integration (TDD) ✅ COMPLETE

**Status:** Completed 2025-12-05

Wire up SCRAM authentication in multigateway by creating a PasswordHashProvider
that fetches credentials from multipooler via gRPC.

**Implemented:**

1. `go/multigateway/auth/pooler_hash_provider.go` - PasswordHashProvider using gRPC
2. `go/multigateway/auth/pooler_hash_provider_test.go` - Unit tests for credential fetching
3. `go/multigateway/poolergateway/pooler_gateway.go` - Added GetAuthCredentials method
4. `go/multigateway/init.go` - Wire PasswordHashProvider into ListenerConfig
5. Extended `PasswordHashProvider` interface to include database parameter
6. Updated `ScramAuthenticator` to pass database to credential lookup

**Tests implemented:**

- `TestPoolerHashProvider_ExistingUser` - Valid hash returned
- `TestPoolerHashProvider_NonExistentUser` - Returns ErrUserNotFound
- `TestPoolerHashProvider_UserWithoutPassword` - Returns ErrUserNotFound
- `TestPoolerHashProvider_GRPCError` - Error propagation
- `TestPoolerHashProvider_InvalidHashFormat` - Parse error handling
- `TestPoolerHashProvider_ImplementsInterface` - Interface compliance

**Security note:** Without Phase 4 sandboxing, authenticated users could potentially
use SET SESSION AUTHORIZATION to impersonate others. This is acceptable for initial
validation but sandboxing is required for multi-tenant deployments.

### Phase 4: Connection Pool Sandboxing (TDD)

**Tests first:**

1. `TestBlockSetSessionAuthorization` - SQL blocked
2. `TestRewriteResetSessionAuthorization` - SQL rewritten
3. `TestAllowSetRole` - Not blocked (PostgreSQL validates)
4. `TestPoolCheckoutSetsSessionAuth` - Connection sandboxed
5. `TestPoolReturnResetsSessionAuth` - Connection restored
6. `TestSandboxEscapeAttempt` - Full integration test

**Then implement:**

1. `go/multipooler/security/session_auth_filter.go` - SQL filter
2. Update `go/multipooler/poolerserver/pooler.go` - Integrate filter
3. Update `go/multipooler/grpcpoolerservice/service.go` - Sandbox on checkout

### Phase 5: Identity Propagation ✅ COMPLETE

**Status:** Completed 2025-12-06

**Implemented:**

1. Added `caller_id` field to `query.ExecuteOptions` proto
2. `ScatterConn` populates `CallerID` with `conn.User()` (authenticated username)
3. `grpcQueryService` includes `CallerID` in all gRPC requests to multipooler
4. Multipooler executor uses `SET SESSION AUTHORIZATION` when CallerID is present
5. Fixed `[]byte` to string conversion in query result scanning

**Files modified:**

- `proto/query.proto` - Added caller_id to ExecuteOptions
- `go/multigateway/scatterconn/scatter_conn.go` - Populate CallerID
- `go/multigateway/poolergateway/grpc_query_service.go` - Pass CallerID in requests
- `go/multipooler/grpcpoolerservice/service.go` - Pass options to executor
- `go/multipooler/executor/executor.go` - SET SESSION AUTHORIZATION implementation
- `go/test/endtoend/multigateway_scram_test.go` - Test current_user returns auth user

**Key implementation:**

```go
// In executor.go - executeQueryAsUser()
// Gets dedicated connection, sets session auth, executes query, resets
setAuthSQL := fmt.Sprintf("SET SESSION AUTHORIZATION %s", quoteIdent(username))
conn.ExecContext(ctx, setAuthSQL)
defer conn.ExecContext(ctx, "RESET SESSION AUTHORIZATION")
```

### Phase 6: End-to-End Testing & Caching

1. `go/multigateway/auth/credential_cache.go` - Optional caching layer
2. Test SCRAM auth with real PostgreSQL instance
3. Test role switching (`SET ROLE`) works correctly within sandbox
4. Test session auth blocking with malicious SQL
5. Test connection reuse across different users
6. Test credential cache behavior under load
7. Performance benchmarks vs trust auth baseline

---

## Critical Files

### New Files

**Phase 1 (✅ Complete):**

- `go/pgprotocol/auth/scram.go` - SCRAM protocol implementation
- `go/pgprotocol/auth/scram_crypto.go` - Cryptographic operations
- `go/pgprotocol/auth/password.go` - Password hash parsing
- `go/pgprotocol/auth/authenticator.go` - Auth state machine
- `go/pgprotocol/server/scram_endtoend_test.go` - End-to-end tests with lib/pq

**Phase 2 (✅ Complete):**

- `go/multipooler/grpcpoolerservice/service_test.go` - GetAuthCredentials unit tests
- `go/test/endtoend/multipooler/auth_credentials_test.go` - GetAuthCredentials e2e tests

**Phase 3 (✅ Complete):**

- `go/multigateway/auth/pooler_hash_provider.go` - PasswordHashProvider via gRPC
- `go/multigateway/auth/pooler_hash_provider_test.go` - Unit tests

**Phase 4, 6 (Pending):**

- `go/multipooler/security/session_auth_filter.go` - SQL security filter
- `go/multigateway/auth/credential_cache.go` - Hash caching (optional)

### Modified Files

**Phase 1 (✅ Complete):**

- `go/pgprotocol/server/startup.go` - SCRAM integration
- `go/pgprotocol/server/listener.go` - PasswordHashProvider config

**Phase 2 (✅ Complete):**

- `proto/multipoolerservice.proto` - Added GetAuthCredentials RPC
- `go/multipooler/grpcpoolerservice/service.go` - Implemented GetAuthCredentials

**Phase 3 (✅ Complete):**

- `go/multigateway/init.go` - Wire PasswordHashProvider into pgprotocol Listener
- `go/multigateway/poolergateway/pooler_gateway.go` - Added GetAuthCredentials method
- `go/pgprotocol/auth/authenticator.go` - Extended interface with database parameter
- `go/pgprotocol/server/startup.go` - Pass database to ScramAuthenticator

**Phase 5 (✅ Complete):**

- `proto/query.proto` - Added caller_id to ExecuteOptions
- `go/multigateway/scatterconn/scatter_conn.go` - Populate CallerID
- `go/multigateway/poolergateway/grpc_query_service.go` - Pass CallerID in requests
- `go/multipooler/grpcpoolerservice/service.go` - Pass options to executor
- `go/multipooler/executor/executor.go` - SET SESSION AUTHORIZATION implementation
- `go/test/endtoend/multigateway_scram_test.go` - Test current_user returns auth user

**Phase 4, 6 (Pending):**

- `go/pgprotocol/server/conn.go` - Auth state tracking
- `go/multipooler/poolerserver/pooler.go` - Session auth on checkout/return

### Reference Files (Read Before Implementing)

- `go/pgprotocol/server/packet.go` - Message encoding/decoding patterns
- `go/pgprotocol/protocol/constants.go` - Auth message constants (AuthSASL, etc.)
- `go/multipooler/connstate/connection_state.go` - Connection state patterns
- `go/parser/ast/utility_statements.go` - VariableSetStmt for SET SESSION AUTH
- `go/multigateway/handler/handler.go` - Where SQL parsing happens

---

## References

- [RFC 5802 - SCRAM](https://tools.ietf.org/html/rfc5802)
- [PostgreSQL SCRAM Authentication](https://www.postgresql.org/docs/current/sasl-authentication.html)
- [PostgreSQL SET SESSION AUTHORIZATION](https://www.postgresql.org/docs/current/sql-set-session-authorization.html)
- [pgbouncer auth_query](https://www.pgbouncer.org/config.html#auth_query)
- [PostgreSQL GoAway proposal](https://www.postgresql.org/message-id/flat/CAER375OvH3_ONmc-SgUFpA6gv_d6eNj2KdZktzo-f_uqNwwWNw@mail.gmail.com)
