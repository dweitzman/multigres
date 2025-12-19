# Plan: Separate multigres_admin User from postgres User

## Overview

Implement admin user separation modeled after Supabase's pattern:

- `multigres_admin`: Created during initdb, used only by multigres internally
- `postgres`: Created post-init, demoted from superuser, for operator/customer use

## Security Requirements

- Use SCRAM-SHA-256 instead of deprecated MD5
- Never put plaintext passwords in SQL queries (use pre-computed SCRAM hashes)

---

## Phase 1: Add SCRAM Hash Generation Function

**File:** `go/pgprotocol/scram/password.go`

```go
// GenerateScramSHA256Hash generates a PostgreSQL SCRAM-SHA-256 password hash.
// The returned string can be used directly in ALTER USER ... WITH PASSWORD '...'
func GenerateScramSHA256Hash(password string, iterations int) (string, error) {
    salt := make([]byte, 16)
    if _, err := rand.Read(salt); err != nil {
        return "", err
    }

    saltedPassword := ComputeSaltedPassword(password, salt, iterations)
    clientKey := ComputeClientKey(saltedPassword)
    storedKey := ComputeStoredKey(clientKey)
    serverKey := ComputeServerKey(saltedPassword)

    return fmt.Sprintf("SCRAM-SHA-256$%d:%s$%s:%s",
        iterations,
        base64.StdEncoding.EncodeToString(salt),
        base64.StdEncoding.EncodeToString(storedKey),
        base64.StdEncoding.EncodeToString(serverKey)), nil
}
```

---

## Phase 2: Update initdb to Create multigres_admin

**File:** `go/cmd/pgctld/command/init.go`

### Change 1: initdb command (line 145)

```go
// Before
cmd := exec.Command("initdb", "-D", dataDir, "--data-checksums",
    "--auth-local=trust", "--auth-host=md5", "-U", pgUser)

// After
cmd := exec.Command("initdb", "-D", dataDir, "--data-checksums",
    "--auth-local=trust", "--auth-host=scram-sha-256", "-U", "multigres_admin")
```

### Change 2: setPostgresPassword function (lines 185-205)

```go
func setPostgresPassword(dataDir string, pgUser string, pgPwfile string) error {
    effectivePassword, err := resolvePassword(pgPwfile)
    if err != nil {
        return fmt.Errorf("failed to resolve password: %w", err)
    }

    // Generate SCRAM hash - no plaintext in SQL
    hash, err := scram.GenerateScramSHA256Hash(effectivePassword, 4096)
    if err != nil {
        return fmt.Errorf("failed to generate SCRAM hash: %w", err)
    }

    cmd := exec.Command("postgres", "--single", "-D", dataDir, pgUser)
    // Password is already hashed - PostgreSQL will store it directly
    sqlCommands := fmt.Sprintf("ALTER USER %s WITH PASSWORD '%s';\n", pgUser, hash)
    cmd.Stdin = strings.NewReader(sqlCommands)
    // ...
}
```

---

## Phase 3: Create postgres User in Multipooler Init (Idempotent)

**File:** `go/multipooler/connpoolmanager/manager.go`

Add bootstrap in `Manager.Open()` after admin pool opens (after line 96):

```go
func (m *Manager) Open(ctx context.Context, logger *slog.Logger, connConfig *ConnectionConfig) {
    // ... existing setup (lines 70-96) ...

    m.adminPool.Open(ctx)

    // NEW: Bootstrap postgres user if it doesn't exist
    if err := m.bootstrapPostgresUser(ctx); err != nil {
        m.logger.WarnContext(ctx, "failed to bootstrap postgres user", "error", err)
        // Non-fatal: log warning but don't prevent startup
    }

    m.logger.InfoContext(ctx, "connection pool manager opened", ...)
}

// bootstrapPostgresUser creates the postgres user if it doesn't exist.
// Runs on every multipooler start but is idempotent (IF NOT EXISTS).
func (m *Manager) bootstrapPostgresUser(ctx context.Context) error {
    conn, err := m.adminPool.Get(ctx)
    if err != nil {
        return fmt.Errorf("failed to get admin connection: %w", err)
    }
    defer m.adminPool.Put(conn)

    // Generate SCRAM hash for postgres user (same password as multigres_admin)
    hash, err := scram.GenerateScramSHA256Hash(m.config.AdminPassword(), 4096)
    if err != nil {
        return fmt.Errorf("failed to generate SCRAM hash: %w", err)
    }

    bootstrapSQL := fmt.Sprintf(`
DO $$
BEGIN
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'postgres') THEN
    -- Create postgres with same password, then demote from superuser
    CREATE ROLE postgres SUPERUSER LOGIN PASSWORD '%s';
    ALTER DATABASE postgres OWNER TO postgres;
    GRANT ALL ON DATABASE postgres TO postgres;
    ALTER ROLE postgres NOSUPERUSER CREATEDB CREATEROLE LOGIN REPLICATION BYPASSRLS;
  END IF;
END $$;
`, hash)

    _, err = conn.Exec(ctx, bootstrapSQL)
    return err
}
```

---

## Phase 4: Update Default Admin User

**File:** `go/multipooler/connpoolmanager/config.go`

```go
// Line 113
adminUser: viperutil.Configure(reg, "connpool.admin.user", viperutil.Options[string]{
    Default:  "multigres_admin",  // Changed from "postgres"
    FlagName: "connpool-admin-user",
    EnvVars:  []string{"CONNPOOL_ADMIN_USER"},
}),
```

---

## Phase 5: Configure pg_hba.conf

**File:** `go/services/pgctld/postgresconfig.go` (or wherever pg_hba.conf is generated)

Generate appropriate pg_hba.conf during init:

```
# multigres_admin: password required for Unix socket
local all  multigres_admin  scram-sha-256

# Other local users: peer authentication (OS user must match)
local all  all              peer

# Localhost TCP: trust (internal services)
host  all  all  127.0.0.1/32  trust
host  all  all  ::1/128       trust

# Remote: all require SCRAM authentication
host  all  all  0.0.0.0/0     scram-sha-256
host  all  all  ::0/0         scram-sha-256
```

---

## Phase 6: Password Change Support (Future)

**Consideration for later implementation:**

When changing passwords, both users must be updated atomically:

```sql
BEGIN;
ALTER USER multigres_admin WITH PASSWORD 'new_scram_hash';
ALTER USER postgres WITH PASSWORD 'new_scram_hash';
COMMIT;
```

This could be exposed via:

- CLI command: `multigres passwd --new-password-file /path/to/file`
- Or admin API endpoint

For now, document that password changes require updating both users.

---

## Files to Modify

| File                                        | Changes                                                           |
| ------------------------------------------- | ----------------------------------------------------------------- |
| `docs/design/multigres-admin-user-plan.md`  | This plan file                                                    |
| `docs/design/admin-user-separation.md`      | Design document with Supabase research                            |
| `go/pgprotocol/scram/password.go`           | Add `GenerateScramSHA256Hash()` function                          |
| `go/cmd/pgctld/command/init.go`             | Use `-U multigres_admin`, `--auth-host=scram-sha-256`, SCRAM hash |
| `go/multipooler/connpoolmanager/manager.go` | Add `bootstrapPostgresUser()` in `Open()`                         |
| `go/multipooler/connpoolmanager/config.go`  | Default admin user → `multigres_admin`                            |
| `go/services/pgctld/postgresconfig.go`      | Generate secure pg_hba.conf                                       |

---

## Migration/Compatibility Notes

- Existing deployments: Continue using `postgres` as admin user via config override
- New deployments: Use `multigres_admin` by default
- Bootstrap SQL is idempotent - safe to run multiple times
- Password changes: Must update both users atomically (document this requirement)
