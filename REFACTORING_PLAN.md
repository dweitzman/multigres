# Multigres Repository Reorganization Plan

## Overview

This document outlines a multi-stage refactoring to establish clear architectural boundaries and improve code organization in the Multigres monorepo.

## Goals

1. **Clear dependency hierarchy**: Establish and enforce boundaries between tools, common infrastructure, and services
2. **Prevent implicit dependencies**: Use depguard to prevent unintended cross-dependencies
3. **Improve discoverability**: Make it obvious what code belongs where
4. **Enable safe evolution**: Set up guardrails for future development

## Organizational Principles

### Directory Structure

```
go/
├── tools/       # Pure utilities (no internal dependencies)
├── common/      # Shared infrastructure & libraries (can depend on tools/pb)
├── services/    # Independent services (cannot depend on each other)
├── pb/          # Generated protobuf code
└── cmd/         # Command entry points
```

### Dependency Rules

```
┌─────────────┐
│  services/  │  → Can import: common/, tools/, pb/
└──────┬──────┘  → Cannot import: other services/
       ↓
┌─────────────┐
│   common/   │  → Can import: tools/, pb/, other common/
└──────┬──────┘  → Cannot import: services/
       ↓
┌─────────────┐
│   tools/    │  → Can import: stdlib, external packages only
└─────────────┘  → Cannot import: any internal multigres packages

      ┌─────┐
      │ pb/ │  → Generated code (imported by common/ and services/)
      └─────┘
```

## Three-Stage Refactoring Plan

### Stage 1: Tools Isolation

**Goal**: Move all pure utilities to `go/tools/` and enforce zero internal dependencies

**Scope**:

- Move `go/netutil/` → `go/tools/netutil/`
- Keep existing tools: `retry/`, `semver/`, `pathutil/`, `stringutil/`, `telemetry/`
- Add depguard rule to prevent tools from importing internal packages

**Changes**:

| Current Location | New Location        | Files Affected | Rationale                                     |
| ---------------- | ------------------- | -------------- | --------------------------------------------- |
| `go/netutil/`    | `go/tools/netutil/` | 3 files        | Generic network utilities, fits tools pattern |

**Imports to Update**: ~3 files import netutil:

- `go/servenv/`
- `go/multipooler/rpcclient/`

**depguard Rule**:

```yaml
tools-isolation:
  files: ["**/go/tools/**/*.go"]
  deny:
    - pkg: "github.com/multigres/multigres/go"
      desc: "tools must not depend on internal multigres packages"
  allow:
    - "$gostd"
    # External packages are allowed (e.g., OpenTelemetry)
```

**Validation**:

- Run `golangci-lint run --disable-all --enable=depguard`
- Ensure all tools tests pass
- Run full test suite

---

### Stage 2: Services Isolation

**Goal**: Move all service implementations to `go/services/` and enforce service independence

**Scope**:

- Create `go/services/` directory
- Move all service packages under it
- Merge `go/admin/server/` into `go/multiadmin/` (fix organizational issue)
- Move `go/timer/` into `go/multipooler/heartbeat/timer/` (colocate with only user)
- Add depguard rule to prevent services from importing each other

**Changes**:

| Current Location   | New Location                               | Notes                                 |
| ------------------ | ------------------------------------------ | ------------------------------------- |
| `go/multiadmin/`   | `go/services/multiadmin/`                  | Main admin service                    |
| `go/admin/server/` | `go/services/multiadmin/`                  | **Merge**: Move files into multiadmin |
| `go/multigateway/` | `go/services/multigateway/`                | Gateway service                       |
| `go/multipooler/`  | `go/services/multipooler/`                 | Pooler service                        |
| `go/multiorch/`    | `go/services/multiorch/`                   | Orchestrator service                  |
| `go/pgctld/`       | `go/services/pgctld/`                      | Postgres control service              |
| `go/timer/`        | `go/services/multipooler/heartbeat/timer/` | **Colocate**: Only used by heartbeat  |

**Special Handling - Admin Merge**:

The `go/admin/server/` package should be merged into `go/services/multiadmin/`:

1. Move files:
   - `go/admin/server/server.go` → `go/services/multiadmin/server.go`
   - `go/admin/server/backup.go` → `go/services/multiadmin/backup.go`
   - `go/admin/server/server_test.go` → `go/services/multiadmin/server_test.go`

2. Update package name from `server` to `multiadmin`

3. Update import in `go/services/multiadmin/init.go`:
   - Remove: `import "github.com/multigres/multigres/go/admin/server"`
   - The code now lives in the same package

4. Delete empty `go/admin/` directory

**Special Handling - Timer Relocation**:

The `go/timer/` package is only used by `multipooler/heartbeat`:

1. Move `go/timer/` → `go/services/multipooler/heartbeat/timer/`
2. Update 3 imports in heartbeat package
3. Delete old `go/timer/` directory

**depguard Rule**:

```yaml
service-isolation:
  files: ["**/go/services/**/*.go"]
  deny:
    - pkg: "github.com/multigres/multigres/go/services"
      desc: "services must not import other services"
  allow:
    - "$gostd"
    - "github.com/multigres/multigres/go/common"
    - "github.com/multigres/multigres/go/tools"
    - "github.com/multigres/multigres/go/pb"
```

**cmd/ Package Updates**:

Each `go/cmd/<service>/` may import its corresponding service:

```yaml
cmd-can-import-services:
  files: ["**/go/cmd/**/*.go"]
  # No restrictions - cmd can import any service
```

**Validation**:

- Run `golangci-lint run --disable-all --enable=depguard`
- Ensure all service tests pass
- Run full test suite including endtoend tests
- Verify all binaries build: `make build` or build each cmd

---

### Stage 3: Common Infrastructure

**Goal**: Move remaining shared code to `go/common/`

**Scope**:

- Move all shared infrastructure and libraries to `go/common/`
- Add depguard rule to prevent common from importing services
- Keep `go/pb/` separate (generated code)

**Changes**:

| Current Location      | New Location                 | Size           | Notes                       |
| --------------------- | ---------------------------- | -------------- | --------------------------- |
| `go/clustermetadata/` | `go/common/clustermetadata/` | Medium         | Topology infrastructure     |
| `go/servenv/`         | `go/common/servenv/`         | Large          | Service framework           |
| `go/viperutil/`       | `go/common/viperutil/`       | Small          | Configuration               |
| `go/mterrors/`        | `go/common/mterrors/`        | Small          | Error handling              |
| `go/event/`           | `go/common/event/`           | Small          | Event system                |
| `go/grpccommon/`      | `go/common/grpccommon/`      | Small          | gRPC utilities              |
| `go/pgprotocol/`      | `go/common/pgprotocol/`      | Large          | PostgreSQL protocol         |
| `go/parser/`          | `go/common/parser/`          | **Very Large** | SQL parser (60+ files)      |
| `go/provisioner/`     | `go/common/provisioner/`     | Medium         | Infrastructure provisioning |
| `go/fakepgdb/`        | `go/common/fakepgdb/`        | Small          | Test utilities              |
| `go/web/`             | `go/common/web/`             | Small          | Web UI templates            |
| `go/plugins/`         | `go/common/plugins/`         | Small          | Plugin system               |
| `go/localproxy/`      | `go/common/localproxy/`      | Small          | Local proxy                 |

**Large Diff Packages** (consider VSCode refactoring):

- `go/parser/` - 60+ files, heavily used by multigateway
- `go/pgprotocol/` - Used extensively by multigateway

**Note**: For large packages like `parser/`, consider using VSCode's "Move Symbol" or manual find/replace in the IDE rather than scripted moves. This ensures proper handling of complex imports.

**depguard Rule**:

```yaml
common-isolation:
  files: ["**/go/common/**/*.go"]
  deny:
    - pkg: "github.com/multigres/multigres/go/services"
      desc: "common packages must not import services"
    - pkg: "github.com/multigres/multigres/go/cmd"
      desc: "common packages must not import cmd packages"
  allow:
    - "$gostd"
    - "github.com/multigres/multigres/go/common"
    - "github.com/multigres/multigres/go/tools"
    - "github.com/multigres/multigres/go/pb"
```

**Validation**:

- Run `golangci-lint run --disable-all --enable=depguard`
- Run full test suite
- Run endtoend tests
- Verify all binaries build

---

## Final Directory Structure

```
go/
├── cmd/
│   ├── multiadmin/
│   ├── multigateway/
│   ├── multipooler/
│   ├── multiorch/
│   └── pgctld/
│
├── services/
│   ├── multiadmin/              # ← Merged from go/admin + go/multiadmin
│   │   ├── init.go
│   │   ├── server.go            # ← Was go/admin/server/server.go
│   │   ├── backup.go            # ← Was go/admin/server/backup.go
│   │   ├── proxy.go
│   │   ├── discovery.go
│   │   └── status.go
│   ├── multigateway/
│   │   ├── init.go
│   │   ├── engine/
│   │   ├── executor/
│   │   ├── handler/
│   │   ├── planner/
│   │   ├── poolergateway/
│   │   └── scatterconn/
│   ├── multipooler/
│   │   ├── init.go
│   │   ├── heartbeat/
│   │   │   └── timer/           # ← Moved from go/timer
│   │   ├── executor/
│   │   ├── grpcconsensusservice/
│   │   ├── grpcmanagerservice/
│   │   ├── grpcpoolerservice/
│   │   ├── manager/
│   │   ├── poolerserver/
│   │   ├── queryservice/
│   │   └── rpcclient/
│   ├── multiorch/
│   │   ├── init.go
│   │   ├── config/
│   │   ├── coordinator/
│   │   ├── recovery/
│   │   └── store/
│   └── pgctld/
│       ├── init.go              # ← Consider adding for consistency
│       ├── command/
│       ├── testutil/
│       └── postgresconfig.go
│
├── common/
│   ├── clustermetadata/
│   │   ├── topo/
│   │   │   ├── etcdtopo/
│   │   │   ├── memorytopo/
│   │   │   └── test/
│   │   └── toporeg/
│   ├── servenv/
│   ├── viperutil/
│   ├── mterrors/
│   ├── event/
│   ├── grpccommon/
│   ├── pgprotocol/
│   │   ├── protocol/
│   │   ├── bufpool/
│   │   └── server/
│   ├── parser/
│   │   └── ast/
│   ├── provisioner/
│   │   └── local/
│   ├── fakepgdb/
│   ├── web/
│   ├── plugins/
│   └── localproxy/
│
├── tools/
│   ├── telemetry/
│   ├── retry/
│   ├── semver/
│   ├── pathutil/
│   ├── stringutil/
│   └── netutil/                 # ← Moved from go/netutil
│
└── pb/                          # Generated protobuf code (unchanged)
    ├── clustermetadata/
    ├── consensus/
    ├── consensusdata/
    ├── multiadmin/
    ├── multipoolermanager/
    ├── multipoolermanagerdata/
    ├── multipoolerservice/
    ├── pgctldservice/
    ├── query/
    └── mtrpc/
```

---

## Complete depguard Configuration

Add to `.golangci.yml`:

```yaml
linters-settings:
  depguard:
    rules:
      # Rule 1: Tools cannot import internal packages
      tools-isolation:
        files: ["**/go/tools/**/*.go"]
        deny:
          - pkg: "github.com/multigres/multigres/go"
            desc: "tools packages must not depend on internal multigres packages"
        allow:
          - "$gostd"

      # Rule 2: Common cannot import services or cmd
      common-isolation:
        files: ["**/go/common/**/*.go"]
        deny:
          - pkg: "github.com/multigres/multigres/go/services"
            desc: "common packages must not import services"
          - pkg: "github.com/multigres/multigres/go/cmd"
            desc: "common packages must not import cmd packages"
        allow:
          - "$gostd"
          - "github.com/multigres/multigres/go/common"
          - "github.com/multigres/multigres/go/tools"
          - "github.com/multigres/multigres/go/pb"

      # Rule 3: Services cannot import other services
      service-isolation:
        files: ["**/go/services/**/*.go"]
        deny:
          - pkg: "github.com/multigres/multigres/go/services"
            desc: "services must not import other services"
        allow:
          - "$gostd"
          - "github.com/multigres/multigres/go/common"
          - "github.com/multigres/multigres/go/tools"
          - "github.com/multigres/multigres/go/pb"

      # Keep existing provisioner rule (if still needed after common/ move)
      provisioner:
        files:
          - "**/go/common/provisioner/*.go"
          - "**/go/common/provisioner/**/*.go"
        deny:
          - pkg: "github.com/multigres/multigres/go"
            desc: "provisioner should use minimal dependencies"
        allow:
          - "$gostd"
          - "github.com/multigres/multigres/go/common/clustermetadata/topo"
          - "github.com/multigres/multigres/go/tools"
          - "github.com/multigres/multigres/go/pb"
          - "github.com/multigres/multigres/go/common/provisioner"
          - "github.com/multigres/multigres/go/common/grpccommon"

      # Keep existing use_modern_packages rule
      use_modern_packages:
        files: ["$all"]
        deny:
          - pkg: "math/rand$"
            desc: "Use math/rand/v2 instead of math/rand for better randomness"
```

---

## Execution Strategy

### Stage 1: Tools (Lowest Risk)

**Estimated Time**: 1-2 hours

1. Create `go/tools/netutil/` directory
2. Move netutil files
3. Update imports in servenv and rpcclient
4. Add `tools-isolation` depguard rule
5. Run: `golangci-lint run --disable-all --enable=depguard`
6. Run tests: `go test ./...`
7. Commit: "refactor: move netutil to tools/ and enforce tools isolation"

### Stage 2: Services (Medium Risk)

**Estimated Time**: 3-4 hours

1. Create `go/services/` directory structure
2. **First**: Merge admin into multiadmin (small, focused change)
   - Move admin/server files to multiadmin
   - Update package declarations
   - Update imports
   - Test multiadmin specifically
   - Commit: "refactor: merge admin/server into multiadmin"
3. **Second**: Move timer into multipooler/heartbeat (small, focused change)
   - Move timer directory
   - Update 3 imports in heartbeat
   - Test multipooler specifically
   - Commit: "refactor: colocate timer with heartbeat"
4. **Third**: Move all services to go/services/
   - Use script or IDE refactoring
   - Update all imports across codebase
   - Update cmd/ imports
   - Test each service
   - Commit: "refactor: move services to go/services/"
5. Add `service-isolation` depguard rule
6. Run: `golangci-lint run --disable-all --enable=depguard`
7. Run full test suite including endtoend
8. Verify builds: `make build` (or equivalent)

### Stage 3: Common (Highest Risk - Large Diff)

**Estimated Time**: 4-6 hours (due to size)

**Option A: IDE-assisted (RECOMMENDED for parser/pgprotocol)**

1. Use VSCode "Move Symbol" or "Rename" refactoring for large packages
2. Let IDE handle import updates automatically
3. Review and commit incrementally

**Option B: Scripted**

1. Create `go/common/` directory structure
2. Move packages incrementally (start with smallest)
3. Update imports using find/replace or script
4. Test after each major package move

**Recommended order** (smallest to largest):

1. Small packages first: `mterrors`, `event`, `grpccommon`, `web`, `plugins`, `localproxy`
2. Medium packages: `viperutil`, `provisioner`, `fakepgdb`, `clustermetadata`
3. Large packages: `servenv`
4. Very large packages: `pgprotocol`, `parser` (consider IDE assistance)

**For each package**:

1. Move package to go/common/
2. Update imports
3. Run tests for affected code
4. Commit

**Final steps**:

1. Add `common-isolation` depguard rule
2. Run: `golangci-lint run --disable-all --enable=depguard`
3. Run full test suite
4. Run endtoend tests
5. Verify all builds

---

## Testing & Validation

### After Each Stage:

1. **Linting**: `golangci-lint run --disable-all --enable=depguard`
2. **Unit tests**: `go test ./...`
3. **Build all binaries**:
   ```bash
   go build ./go/cmd/multiadmin
   go build ./go/cmd/multigateway
   go build ./go/cmd/multipooler
   go build ./go/cmd/multiorch
   go build ./go/cmd/pgctld
   ```
4. **Endtoend tests**: Run endtoend test suite

### Final Validation:

1. Clean build from scratch
2. Full test suite
3. Endtoend tests
4. Manual smoke testing of key workflows
5. Verify depguard rules with `--fix` mode disabled

---

## Rollback Strategy

- Each stage should be committed separately
- If issues arise in Stage N, can revert that stage's commits
- Maintain working state after each stage before proceeding

---

## Notes

- **No external API impact**: This is internal restructuring only
- **Import paths will change**: All imports update from `go/X` to `go/{tools,common,services}/X`
- **Generated code unchanged**: `go/pb/` stays the same
- **Test directories**: May need updates but should be straightforward
- **IDE support**: Use VSCode's refactoring tools for large moves (especially parser)

---

## Success Criteria

1. ✅ All code organized into `tools/`, `common/`, `services/` directories
2. ✅ depguard rules enforcing boundaries (0 violations)
3. ✅ All tests passing
4. ✅ All binaries building successfully
5. ✅ Endtoend tests passing
6. ✅ No cross-service dependencies
7. ✅ Tools have no internal dependencies
8. ✅ Common does not depend on services

---

## Timeline

- **Stage 1 (Tools)**: 1-2 hours
- **Stage 2 (Services)**: 3-4 hours
- **Stage 3 (Common)**: 4-6 hours
- **Total estimated time**: 8-12 hours (can be done incrementally)

---

## Questions/Decisions

- [ ] Should `pgctld` get an `init.go` for consistency? (Current: config files only)
- [ ] Use IDE-assisted refactoring for parser/pgprotocol moves? (Recommended: yes)
- [ ] Any packages that should stay at top level? (Current plan: only cmd/ and pb/)

---

**Document Version**: 1.0
**Last Updated**: 2025-11-19
**Status**: Ready for Stage 1 execution
