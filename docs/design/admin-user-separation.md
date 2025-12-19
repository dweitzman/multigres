# Admin User Separation Design

This document describes the design for separating the `multigres_admin` user (internal) from the `postgres` user (operator-facing), modeled after Supabase's approach.

## Background

Multigres needs two distinct database users:

1. **`multigres_admin`**: A true superuser used internally by multigres services for administrative operations (connection pool management, health checks, schema migrations)

2. **`postgres`**: A high-privilege but non-superuser role for operators and customers to use for their applications

This separation follows Supabase's proven pattern where `supabase_admin` is the internal superuser and `postgres` is the customer-facing role.

## How Supabase Implements This

### User Creation Flow

1. **During initdb**: The `supabase_admin` user is created as the initial cluster superuser

   ```bash
   initdb -D /data/postgresql --username=supabase_admin
   ```

2. **Post-init migration**: The `postgres` role is created via SQL after PostgreSQL starts

   ```sql
   DO $$
   BEGIN
     IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'postgres') THEN
       CREATE ROLE postgres SUPERUSER LOGIN PASSWORD '...';
       ALTER DATABASE postgres OWNER TO postgres;
     END IF;
   END $$;
   ```

3. **Demotion**: The `postgres` role is demoted from superuser
   ```sql
   ALTER ROLE postgres NOSUPERUSER CREATEDB CREATEROLE LOGIN REPLICATION BYPASSRLS;
   ```

### Resulting Privileges

| Role             | Privileges                                                         |
| ---------------- | ------------------------------------------------------------------ |
| `supabase_admin` | SUPERUSER, CREATEDB, CREATEROLE, REPLICATION, BYPASSRLS            |
| `postgres`       | CREATEDB, CREATEROLE, LOGIN, REPLICATION, BYPASSRLS (no SUPERUSER) |

### supautils: How Supabase Bridges the Privilege Gap

Since `postgres` is demoted from superuser, it cannot perform certain operations that customers need (like creating extensions, publications, event triggers). Supabase addresses this with **supautils**, a PostgreSQL extension that provides controlled privilege escalation.

> **Note**: supautils is Supabase-specific and not directly usable in self-hosted multigres deployments. This section documents how Supabase solves this problem for research and inspiration purposes.

**What supautils enables for the demoted `postgres` role:**

- Creating extensions that normally require superuser (e.g., `hstore`, `pg_cron`)
- Creating logical replication publications
- Creating foreign data wrappers
- Creating event triggers (with protections against privilege escalation)
- Setting certain superuser-only config settings (e.g., `session_replication_role`)
- Managing RLS policies on tables they don't own
- Dropping triggers on specific tables

**How it works:**

- Loaded via `shared_preload_libraries`
- When the privileged role (`postgres`) performs a restricted operation, supautils temporarily switches to the superuser (`supabase_admin`), performs the operation, then switches back
- The resulting objects are owned by `postgres`, not the superuser
- Reserved roles are protected from modification by the privileged role

**Implication for Multigres:**
For self-hosted multigres deployments, operators who need to create extensions or perform other superuser-only operations would need to either:

- Connect as `multigres_admin` for those specific operations
- Or keep `postgres` as a full superuser (by not applying the demotion)

### Password Handling

Both users share the same password, set from a single `POSTGRES_PASSWORD` environment variable. This is intentional:

- Security relies on **network isolation via pg_hba.conf**, not password secrecy
- Password changes update both users atomically in a single transaction
- This simplifies credential management while maintaining proper access control

### Access Control (pg_hba.conf)

The security model relies on pg_hba.conf to control who can connect as which user:

```
# supabase_admin: Only via local Unix socket with SCRAM password
local all  supabase_admin  scram-sha-256

# Other local users: peer authentication (OS user must match DB user)
local all  all             peer

# Localhost TCP (127.0.0.1): Trust - internal services can connect as any user
host  all  all  127.0.0.1/32  trust
host  all  all  ::1/128       trust

# Remote connections: All require SCRAM-SHA-256 password
host  all  all  0.0.0.0/0     scram-sha-256
host  all  all  ::0/0         scram-sha-256
```

### Why Shared Password Is Secure

The shared password doesn't enable privilege escalation because:

1. **Internal services** connect via localhost TCP which uses `trust` auth - they don't need passwords at all
2. **External clients** must connect via the remote path which requires SCRAM authentication, but:
   - They can only connect as users they have credentials for
   - `supabase_admin` is never exposed in client-facing connection strings
   - Even if they know the password, they can't escalate to `supabase_admin` from a remote connection because the credentials alone don't change the pg_hba.conf rules

3. **The `supabase_admin` user** is only accessible via:
   - Local Unix socket (requires physical/container access)
   - Not exposed in any client-facing APIs or connection strings

### Password Changes

When passwords need to be changed, both users must be updated atomically:

```sql
BEGIN;
ALTER USER supabase_admin WITH PASSWORD 'new_password_hash';
ALTER USER postgres WITH PASSWORD 'new_password_hash';
-- ... other service users ...
COMMIT;
```

Supabase provides a `db-passwd.sh` script that:

1. Generates a new random password
2. Updates all database users in a single transaction
3. Updates the `.env` file with the new password
4. Requires container restart to pick up new credentials

## Multigres Implementation

### Mapping to Multigres

| Supabase                    | Multigres                     |
| --------------------------- | ----------------------------- |
| `supabase_admin`            | `multigres_admin`             |
| `postgres`                  | `postgres`                    |
| `POSTGRES_PASSWORD` env var | `--pg-pwfile` or `PGPASSWORD` |

### User Creation

1. **initdb** creates `multigres_admin` as superuser
2. **Multipooler** creates `postgres` user on first startup (idempotent)
3. **postgres** is demoted from superuser immediately after creation

### Security Improvements Over Current Implementation

1. **SCRAM-SHA-256** instead of MD5 for password hashing
2. **Pre-computed hashes** in SQL to avoid plaintext passwords in logs
3. **Proper pg_hba.conf** generation with appropriate access controls

### Future: Password Rotation

A future `multigres passwd` command or admin API could:

1. Generate new SCRAM hash
2. Update both users atomically
3. Require service restart to pick up new credentials

## References

- Supabase postgres image: User creation during initdb and migrations
- Supabase docker db-passwd.sh: Password rotation pattern
- supautils: PostgreSQL extension for controlled privilege escalation (Supabase-specific, documented here for research purposes)
- PostgreSQL SCRAM-SHA-256: RFC 5802, PostgreSQL 10+ default
