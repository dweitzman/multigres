# Incremental SCRAM Authentication Implementation Plan for Multigres

**Date:** 2025-12-09
**Status:** Planning
**Target:** Production-ready SCRAM authentication with incremental, mergeable phases

## Executive Summary

This plan outlines a pragmatic, phase-by-phase approach to adding SCRAM-SHA-256 authentication to multigres. The exploration branch (`dw-auth-exploration`) has validated all core components:

- Complete RFC 5802-compliant SCRAM protocol implementation (69+ tests passing)
- SCRAM key passthrough working end-to-end
- Per-user connection pools architecture functional
- Transaction status propagation and reserved connections operational

**The core technical risk has been retired.** What remains is productionizing these validated components through careful integration, simplification where possible, and resolution of one remaining issue (SET ROLE persistence in tests).

The plan prioritizes **independently mergeable phases** that each provide incremental value while maintaining backward compatibility until the final cutover.

### Leveraging Exploration Work

The `dw-auth-exploration` branch contains working implementations of all major components. **This plan explicitly encourages consulting and borrowing code from the exploration branch as needed.** Each phase below includes references to corresponding exploration branch files that can be:

- Directly ported with minimal changes
- Used as reference implementations
- Adapted for production use after review and simplification

When implementing each phase, always check the exploration branch first for existing solutions before writing from scratch.

## Architectural Overview

```
Client → multigateway → multipooler → PostgreSQL
         (SCRAM auth)   (per-user pools)
                        (SCRAM passthrough)
```

### Key Design Decisions

1. **Server-side SCRAM in multigateway**: Authenticate clients locally using hashes fetched from multipooler
2. **Per-user connection pools**: Each authenticated user gets their own pool in multipooler
3. **SCRAM key passthrough**: Extract ClientKey/ServerKey during client auth, use for backend authentication
4. **Credential caching**: TTL-based cache with invalidation on auth failures
5. **Reserved connections for transactions**: Maintain session affinity during BEGIN...COMMIT blocks

### Design Rationale

- **Per-user pools over shared pools**: Simpler security model, eliminates escape risks from SET SESSION AUTHORIZATION
- **SCRAM key passthrough over password caching**: No plaintext passwords in memory, follows industry practice (PgBouncer)
- **Reserved connections for SET ROLE**: Correct and simple over complex state recreation
- **Incremental phases**: Each phase independently testable and mergeable to production

### Guiding Principle

**When in doubt, imitate successful proxies like PgBouncer and Supavisor.** These are battle-tested, production-proven solutions. Follow their patterns unless:

- They conflict with our architecture in fundamental ways
- We have strong evidence they paint us into a corner we'll regret later
- There's a compelling reason specific to multigres to deviate

This approach minimizes risk and leverages years of community learnings.

## Implementation Phases

**Note on PR Size:** Each phase below can potentially be broken into multiple smaller PRs where the work is naturally divisible. Prefer smaller, focused, easy-to-review PRs over large changes. Only group related changes that genuinely need to be together.

### Phase 1: SCRAM Protocol Foundation

**Objective:** Establish tested SCRAM protocol implementation without changing connection behavior

#### Exploration Branch Reference

All code for this phase already exists and is well-tested in `dw-auth-exploration` branch. **Recommendation: Port these files directly with minimal changes.** The implementation is RFC-compliant with 69+ passing tests.

#### What Gets Built

1. Port SCRAM protocol code from exploration branch:
   - `go/pgprotocol/auth/scram.go` - Message parsing/generation (WORKING in exploration)
   - `go/pgprotocol/auth/scram_crypto.go` - RFC 5802 cryptographic operations (WORKING in exploration)
   - `go/pgprotocol/auth/scram_client.go` - Client-side implementation for passthrough (WORKING in exploration)
   - `go/pgprotocol/auth/authenticator.go` - Server-side authenticator state machine (WORKING in exploration)

2. Comprehensive test coverage (port from exploration):
   - Unit tests for all crypto operations with RFC test vectors (69+ tests PASSING in exploration)
   - Protocol parsing tests (client-first, server-first, client-final, server-final messages)
   - SCRAM key extraction and passthrough validation tests
   - Error cases: malformed messages, replay attacks, wrong passwords

#### Testing Approach

- Run existing 69+ unit tests from exploration branch
- Add fuzz testing for message parsers
- Security-focused tests: replay attack prevention, nonce validation, proof verification

#### Success Criteria

- All SCRAM protocol tests passing
- Code review approved with focus on cryptographic correctness
- No integration with existing services yet (pure library code)

#### Merge Readiness

**Ready to merge immediately** - this is pure library code with no service integration. Existing trust authentication continues to work.

---

### Phase 2: Credential Fetching Infrastructure

**Objective:** Add gRPC endpoint for multigateway to fetch SCRAM hashes from multipooler

#### Exploration Branch Reference

Credential fetching infrastructure exists in `dw-auth-exploration`. **Consult these files:**

- `go/multigateway/auth/credential_cache.go` (WORKING in exploration)
- `proto/multipoolerservice.proto` GetAuthCredentials definition (WORKING in exploration)
- `go/multipooler/grpcpoolerservice/service.go` implementation (WORKING in exploration)

#### What Gets Built

1. **Multipooler gRPC service** (`GetAuthCredentials`):
   - Query: `SELECT rolpassword FROM pg_authid WHERE rolname = $1`
   - Parse SCRAM-SHA-256 hash format from PostgreSQL
   - Return ScramHash proto with salt, iterations, ServerKey, StoredKey
   - Location: `go/multipooler/grpcpoolerservice/service.go`
   - **Can borrow from exploration branch implementation**

2. **Multigateway credential cache**:
   - Port `go/multigateway/auth/credential_cache.go` from exploration
   - TTL-based caching (make configurable, default TBD)
   - Negative caching for non-existent users
   - Invalidation on authentication failures

3. **Protobuf definitions**:
   - Port `GetAuthCredentialsRequest/Response` from `proto/multipoolerservice.proto` in exploration

#### Testing Approach

- Integration test: Create user in PostgreSQL, fetch hash via gRPC, verify hash format
- Cache behavior: Verify TTL expiration, negative caching, invalidation
- Security: Ensure hash never logged or exposed in errors
- Performance: Measure gRPC latency, cache hit rates

#### Success Criteria

- GetAuthCredentials endpoint functional and tested
- Hash caching working with proper TTL
- No credential leakage in logs or errors
- Backward compatible (trust auth still works)

#### Merge Readiness

**Ready after Phase 1** - the endpoint exists but isn't used yet. Services continue with trust authentication.

---

### Phase 3: SCRAM Authentication in Multigateway

**Objective:** Implement client-facing SCRAM authentication in multigateway

#### Exploration Branch Reference

Server-side SCRAM authentication is implemented in `dw-auth-exploration`. **Consult:**

- `go/pgprotocol/server/startup.go` - Authentication flow (WORKING in exploration)
- Integration with authenticator.go from Phase 1

#### What Gets Built

1. **Server-side SCRAM flow** in multigateway:
   - Location: `go/pgprotocol/server/startup.go`
   - Flow:
     1. Send AuthenticationSASL with SCRAM-SHA-256 mechanism
     2. Receive client-first-message
     3. Fetch hash from multipooler (via credential cache)
     4. Generate server-first-message
     5. Receive client-final-message
     6. Verify client proof, extract ClientKey
     7. Send server-final-message with server signature
     8. Store extracted ClientKey/ServerKey on connection

2. **Key extraction for passthrough**:
   - During proof verification, extract ClientKey = ClientProof XOR ClientSignature
   - Store ClientKey and ServerKey on `Conn` object
   - Make available via `conn.SCRAMKeys()` method

3. **Fallback to trust** (initially):
   - Feature flag: `--enable-scram-auth=false` (default)
   - When disabled, use existing trust authentication
   - Allows incremental rollout and testing

#### Testing Approach

- End-to-end test: Client connects with correct password, authentication succeeds
- Wrong password: Authentication fails with proper error
- Unknown user: Fast rejection (negative cache)
- Key extraction: Verify ClientKey/ServerKey correctly extracted and available
- Performance: Measure authentication latency vs trust mode

#### Success Criteria

- SCRAM authentication working for client connections
- ClientKey/ServerKey successfully extracted
- Feature flag allows disabling for safety
- All existing integration tests still pass (with feature flag off)

#### Merge Readiness

**Ready after Phase 2, with feature flag OFF by default.** This adds the code path but doesn't change behavior until enabled.

---

### Phase 4: Per-User Connection Pools in Multipooler

**Objective:** Implement per-user pool architecture with SCRAM key passthrough

#### Exploration Branch Reference

Per-user pools are fully implemented in `dw-auth-exploration`. **Consult these files:**

- `go/multipooler/pools/userpool/manager.go` (WORKING in exploration)
- `go/multipooler/pools/userpool/connection.go` (WORKING in exploration)
- `go/multipooler/executor/executor.go` - integration with pools (WORKING in exploration)

#### What Gets Built

1. **Per-user pool manager**:
   - Port `go/multipooler/pools/userpool/manager.go`
   - Generic pool implementation: `Manager[Connection]`
   - Configuration: max pools, connections per pool, global cap, idle timeout

2. **SCRAM passthrough client**:
   - Use SCRAM client from Phase 1 to authenticate to PostgreSQL
   - Create connections as actual user (not superuser)
   - Port `go/multipooler/pools/userpool/connection.go`

3. **Pool lifecycle**:
   - Create pool on first request for user
   - Cache SCRAM keys for creating new connections
   - Idle pool garbage collection
   - Connection reset on return (RESET ROLE, DISCARD ALL)

4. **Integration with executor**:
   - Executor calls pool manager with username + SCRAM keys
   - Gets user-specific connection
   - Returns connection to pool after query

#### Testing Approach

- Create pools for multiple users, verify isolation
- Connection reuse: Same user gets pooled connection
- Reset verification: SET ROLE doesn't leak between queries
- Pool limits: Global cap enforcement, per-user limits
- Idle cleanup: Pools garbage collected after timeout
- SCRAM passthrough: Verify backend connections authenticate as correct user

#### Success Criteria

- Per-user pools functional with SCRAM passthrough
- Connection isolation verified (no privilege leakage)
- Pool sizing and cleanup working
- Performance acceptable (minimal overhead vs shared pool)

#### Merge Readiness

**Ready after Phase 3, initially behind feature flag.** Can be enabled for testing in non-production environments.

#### Integration Points

- Multipooler executor needs to route queries through pool manager
- CallerID in gRPC needs SCRAM keys (protobuf update)

---

### Phase 5: Transaction State Management

**Objective:** Reserve connections during transactions, track session state

#### Exploration Branch Reference

Reserved connections and transaction state tracking work in `dw-auth-exploration`. **Consult:**

- `proto/multipoolerservice.proto` - SessionState message (WORKING in exploration)
- `go/multipooler/executor/executor.go` - Connection reservation logic (WORKING in exploration)
- `go/multigateway/poolergateway/pooler_gateway.go` - SessionState propagation (WORKING in exploration)

#### What Gets Built

1. **Reserved connection tracking**:
   - Port `proto/multipoolerservice.proto` SessionState
   - Reserve connection on BEGIN, release on COMMIT/ROLLBACK
   - Return reserved_connection_id in QueryResult

2. **Session state propagation**:
   - ExecuteOptions includes reserved_connection_id
   - Executor routes to reserved connection if present
   - Handle connection failures (failover, release reservation)

3. **Transaction status propagation** (already working in exploration):
   - PostgreSQL transaction status (I/T/E) propagated through multipooler
   - Multigateway uses status for ReadyForQuery messages
   - Implementation in `go/pgprotocol/server/conn.go`

#### Testing Approach

- BEGIN reserves connection, subsequent queries use same connection
- COMMIT releases reservation
- Connection failure during transaction: Proper error propagation
- Concurrent transactions from same user: Different connections
- Transaction status: Verify I/T/E correctly propagated to client

#### Success Criteria

- Transactions maintain connection affinity
- Transaction status correctly propagated
- Failover handling graceful
- No connection leaks on failures

#### Merge Readiness

**Ready after Phase 4.** This is essential for correctness (transactions, temp tables, prepared statements).

#### Integration Points

- PoolerGateway propagates SessionState back to multigateway Handler
- Multigateway stores SessionState per connection, sends in subsequent requests

---

### Phase 6: Connection Cleanup and Session Variables

**Objective:** Handle SET ROLE, search_path, and other session state

#### Exploration Branch Reference

Connection reset exists in exploration, but SET ROLE persistence has a failing test. **Consult with caution:**

- `go/multipooler/pools/userpool/connection.go` - Reset() method (WORKING in exploration)
- `go/test/endtoend/multigateway_test.go` - TestMultiGateway_SetRole (FAILING in exploration - needs investigation)

**Note:** The SET ROLE test failure needs to be understood and resolved in this phase.

#### What Gets Built

1. **Connection reset on pool return**:
   - `RESET ROLE` - Clear any role changes
   - `DISCARD ALL` - Clear temp tables, prepared statements, session vars
   - Track which statements need reset based on query history

2. **Session state for SET ROLE**:
   - Detect SET ROLE via SQL parsing
   - Mark connection as "tainted" - needs reservation
   - **Approach**: Reserve connection until transaction end (simple, correct)

3. **Investigation of exploration branch SET ROLE test failure**:
   - Debug why SET ROLE persistence test fails in exploration branch
   - Verify whether reserved-connection approach avoids this issue
   - Confirm PostgreSQL semantics for SET ROLE behavior

#### Testing Approach

- SET ROLE in transaction: Connection reserved, role persists
- COMMIT releases connection, role reset for next user
- Verify no role leakage between users in same pool
- Performance: Measure overhead of reset operations

#### Success Criteria

- SET ROLE works correctly without privilege leakage
- Connection reset verified effective
- Exploration branch test failure understood and resolved

#### Known Issue

The exploration branch has a failing test for SET ROLE persistence (`TestMultiGateway_SetRole`). Investigation needed to determine:

- Is this a test issue or implementation bug?
- Does the simple reserved-connection approach avoid this?

#### Merge Readiness

**Ready after Phase 5**, with chosen approach implemented and tested.

---

### Phase 7: Production Hardening

**Objective:** Security review, performance optimization, monitoring

#### What Gets Built

1. **Security audit**:
   - Credential handling: Never log passwords or SCRAM keys
   - Hash storage: Verify proper protection in cache
   - Timing attacks: Ensure constant-time comparisons
   - Replay attacks: Nonce validation comprehensive
   - Connection isolation: Fuzz testing for privilege escalation

2. **Monitoring and metrics**:
   - Authentication success/failure rates
   - Cache hit rates, TTL effectiveness
   - Pool utilization: Connections per user, idle time
   - Connection reservation duration
   - SCRAM key extraction errors

3. **Performance optimization**:
   - Pool warmup for frequently-used accounts
   - Credential cache tuning (TTL, size)
   - Connection reset minimization
   - Prepared statement caching per pool

4. **Configuration**:
   - Pool sizing: default_pool_size, max_user_connections, global_cap
   - Cache TTL, negative cache TTL
   - Idle timeouts, cleanup intervals
   - Feature flags for gradual rollout

#### Testing Approach

- Load testing: N users × M connections, measure latency and throughput
- Security testing: Attempt various bypass techniques
- Failure scenarios: PostgreSQL down, network issues, invalid hashes
- Monitoring: Verify all metrics emitted correctly

#### Success Criteria

- Security review passed
- Performance within 5% of trust mode for cache hits
- Monitoring comprehensive
- Configuration flexible for different deployment scenarios

#### Merge Readiness

**Ready after Phase 6.** This makes SCRAM authentication production-ready.

---

### Phase 8: Migration and Rollout

**Objective:** Cutover from trust to SCRAM authentication

#### What Gets Built

1. **Migration guide**:
   - User account setup (CREATE USER with passwords)
   - pg_hba.conf configuration for SCRAM
   - Feature flag enablement procedure
   - Rollback plan

2. **Gradual rollout strategy**:
   - Week 1: Enable in test environment, monitor
   - Week 2: Enable for internal users in staging
   - Week 3: Enable for subset of production users
   - Week 4: Enable for all users, remove trust fallback

3. **Monitoring during rollout**:
   - Authentication failure rates
   - Performance comparison (trust vs SCRAM)
   - Pool utilization patterns
   - Error rates and types

#### Testing Approach

- Test migration procedure in staging environment
- Verify rollback procedure works
- Load testing with SCRAM vs trust comparison

#### Success Criteria

- SCRAM authentication enabled by default
- Trust authentication removed (or admin-only)
- No production incidents during rollout
- Performance acceptable

#### Merge Readiness

**Ready after Phase 7.** This is the production cutover.

---

## Decision Points

### Decision 1: SET ROLE Handling Approach

**When:** During Phase 6 implementation
**Status:** Recommendation made, pending implementation validation

**Options:**

- **Option A - Reserved connections** (simple, slower): Any SET ROLE reserves connection until transaction end
- **Option B - Session state tracking** (complex, faster): Track current_role, restore on connection reuse

**Recommendation:** Start with Option A, optimize to Option B if performance requires

**Factors:**

- Frequency of SET ROLE in production workload
- Acceptable performance overhead for uncommon case
- Implementation complexity vs value
- Matches PgBouncer behavior (session pooling mode)

---

### Decision 2: Connection Pool Sizing Strategy

**When:** During Phase 4 configuration design
**Status:** Open - requires research and design

**Research Needed:**

- **PgBouncer approach**: How does PgBouncer size pools? What defaults do they use?
- **Supavisor approach**: What pool sizing strategies does Supavisor implement?
- **Single pool baseline**: With single shared pool, typical sizing based on processor count

**Per-User Pool Challenges:**

- Unlike single shared pool, per-user sizing is not processor-bound
- Some users have high request rate + low latency (benefit most from pooling)
- Some users have idle connections while others wait (inefficient resource allocation)
- Need to balance: fair progress for all users vs. efficient resource utilization

**Design Considerations:**

- **Global cap**: Should still be based on processors/system capacity to prevent overload
- **Per-user minimums**: Every user should be able to make progress
- **Dynamic sizing**: Consider feedback loop based on utilization:
  - High utilization + wait times → allocate more connections to that user
  - Low utilization + idle connections → share more evenly with other users
  - Prevent any single user from hogging all resources
- **Fairness vs efficiency**: Balance between fair resource distribution and optimal performance

**Approach:**

1. Research PgBouncer and Supavisor pool sizing strategies
2. Design dynamic sizing algorithm with utilization feedback
3. Start with simple static sizing, add dynamic optimization later if needed
4. Document sizing strategy and provide tuning guide

---

### Decision 3: Credential Cache TTL

**When:** During Phase 2 cache implementation
**Status:** Open - will be decided later

**Options:**

- Short (1 min): Faster password change propagation, more database queries
- Medium (5 min): Balanced approach
- Long (15 min): Better performance, slower password change propagation

**Factors:**

- How often passwords change in production
- Acceptable staleness for security policies
- Database load from credential queries
- Cache hit rate vs freshness tradeoff

**Implementation Approach:**

- Make TTL configurable from the start
- Support manual invalidation on auth failure
- Code should be flexible enough to work with whatever TTL is chosen
- Can start with a reasonable default (e.g., 5 min) and tune based on production data

---

## Challenges and Mitigations

### Challenge 1: SET ROLE Persistence Test Failure

**Issue:** One test failing in exploration branch (`TestMultiGateway_SetRole`) for SET ROLE behavior

**Investigation needed:**

- Root cause: Test issue or implementation bug?
- Does reserved-connection approach avoid this?
- Is the failing behavior actually correct PostgreSQL semantics?

**Mitigation:**

- Phase 6 includes debugging this specific issue
- Have both Option A (reserve) and Option B (track) as fallback approaches
- Extensive testing of SET ROLE edge cases
- Consult PostgreSQL documentation for exact semantics

---

### Challenge 2: Connection Count Explosion

**Issue:** N users × M connections could exceed PostgreSQL max_connections

**Mitigation:**

- Global connection cap enforcement (Phase 4)
- Idle pool garbage collection
- Per-user connection limits
- Clear sizing documentation and monitoring
- Connection pooling best practices guide

---

### Challenge 3: SCRAM Key Security

**Issue:** ClientKey/ServerKey transmitted via gRPC, stored in memory

**Mitigation:**

- Keys never logged
- Keys cleared from memory on pool destruction
- TLS for gRPC connections (production requirement)
- Security audit in Phase 7
- Memory scrubbing on key disposal

---

## Critical Files for Implementation

### Phase 1-3 (Protocol and Authentication)

- `go/pgprotocol/auth/scram.go` - Core SCRAM protocol, message parsing
- `go/pgprotocol/auth/authenticator.go` - Server-side authenticator state machine
- `go/pgprotocol/server/startup.go` - Connection startup and authentication flow
- `go/multigateway/auth/credential_cache.go` - Credential caching with TTL
- `proto/multipoolerservice.proto` - GetAuthCredentials gRPC definition

### Phase 4-6 (Pooling and State)

- `go/multipooler/pools/userpool/manager.go` - Per-user pool manager
- `go/multipooler/pools/userpool/connection.go` - Connection wrapper with reset logic
- `go/multipooler/executor/executor.go` - Query execution with pool routing
- `proto/query.proto` - SessionState for connection reservation
- `go/multigateway/poolergateway/pooler_gateway.go` - SessionState propagation

### Phase 7-8 (Hardening and Rollout)

- Configuration files for feature flags
- Monitoring integration points
- Migration documentation

---

## Appendix: Alternative Approaches Considered

### Alternative 1: Shared Pool with SET SESSION AUTHORIZATION

**Approach:** Single pool with superuser connections, use SET SESSION AUTHORIZATION to impersonate users

**Pros:**

- Lower connection count (single pool)
- Simpler pool management

**Cons:**

- Escape risk: RESET SESSION AUTHORIZATION returns to superuser
- Requires SQL parsing/filtering for every query
- Complex extension needed for stored procedure safety
- No major PostgreSQL proxy uses this approach
- Higher complexity with unclear performance benefit

**Decision:** Not pursued. Per-user pools provide better security model.

---

### Alternative 2: Trust Authentication to Multipooler

**Approach:** Continue trust authentication between multigateway and multipooler

**Pros:**

- Simpler implementation
- Lower latency (no SCRAM handshake)

**Cons:**

- SCRAM key passthrough provides real backend authentication
- Per-user pools require authenticating as actual user
- Trust mode requires pg_hba.conf changes and network security
- Less secure than cryptographic authentication

**Decision:** Not pursued. SCRAM passthrough is industry standard.

---

### Alternative 3: Password Caching for Backend Auth

**Approach:** Store plaintext passwords after client auth, use for backend connections

**Pros:**

- Could support multiple auth methods (not just SCRAM)
- Simpler key management

**Cons:**

- Security risk: Plaintext passwords in memory
- SCRAM protocol never transmits plaintext
- SCRAM key passthrough provides same functionality without plaintext
- Industry practice (PgBouncer) uses key extraction, not password storage

**Decision:** Not pursued. SCRAM key passthrough is more secure.

---

### Alternative 4: Complex Session State Tracking for SET ROLE

**Approach:** Track all session variables and restore them on connection reuse

**Pros:**

- Better performance for SET ROLE patterns
- No connection reservation needed

**Cons:**

- Complex implementation
- Many edge cases (what variables to track?)
- SET ROLE is uncommon in practice
- Risk of state synchronization bugs

**Decision:** Deferred. Start with reserved connections (simple, correct), optimize later if needed.

---

## Success Metrics

### Phase-Level Metrics

- **Phase 1-3:** All tests passing, no regressions
- **Phase 4-6:** Connection isolation verified, no privilege leakage
- **Phase 7:** Security audit passed, performance within 5% of baseline
- **Phase 8:** Zero authentication-related production incidents

### Production Metrics

- Authentication latency: < 10ms for cache hits
- Cache hit rate: > 95%
- Connection pool efficiency: > 80% utilization
- Failed authentication rate: < 0.1% (excluding invalid passwords)
- Mean time to authenticate: < 50ms including cache miss

---

## Next Steps

1. **Start Phase 1:** Port SCRAM protocol code from exploration branch
2. **Create feature branch:** `git checkout -b scram-phase-1-protocol`
3. **Run existing tests:** Verify exploration branch tests still pass
4. **Code review:** Focus on cryptographic correctness and security
