# Multigres Directory Structure Refactoring

**Status**: In Progress
**Created**: 2025-11-21
**Last Updated**: 2025-11-21

## Overview

This document outlines a plan to reorganize the multigres monorepo to establish clear architectural boundaries through a hierarchical directory structure enforced by depguard linting rules.

### What We're Doing

Reorganizing code into clearly defined layers:

- `go/tools/` - Pure utilities with no internal dependencies
- `go/common/` - Shared infrastructure and libraries
- `go/services/` - Independent service implementations
- `go/cmd/` - Command entry points
- `go/pb/` - Generated protobuf code (unchanged)

### Why This Matters

The codebase has grown organically with packages at the top level of `go/`. This makes it unclear what depends on what, and there's nothing preventing unintended dependencies (like one service importing another service's code, or a utility importing service-specific logic).

### Goals

1. **Clear dependency hierarchy**: Make it obvious what can depend on what by organizing code into distinct layers
2. **Prevent implicit dependencies**: Use depguard rules to catch violations at lint time
3. **Improve discoverability**: New developers can navigate the codebase and understand where code belongs
4. **Enable safe evolution**: With clear boundaries, we can refactor within a layer without affecting other layers

### Non-Goals

- No changes to package names (only locations change, e.g., `go/parser` → `go/common/parser`)
- No external API changes (this is internal restructuring only)
- No changes to generated code in `go/pb/` (stays as-is)

### Future Considerations

These are noted for future discussion but not part of this plan:

- Consider moving `go/pb/` under `go/common/pb/`
- Consider moving `go/provisioner/` under `go/cmd/multigres/`
- Consider colocating `go/tools/timer` with `multipooler/heartbeat` if it remains single-use

## Organizational Principles

### Directory Structure

```
go/
├── tools/       # Pure utilities (no internal dependencies)
├── common/      # Shared infrastructure & libraries (can depend on tools/pb)
├── services/    # Independent services (cannot depend on each other)
├── pb/          # Generated protobuf code
├── cmd/         # Command entry points (can depend on anything)
├── provisioner/ # Provisioning (special case, stays at top level for now)
└── test/        # Test utilities
```

### Dependency Rules

```
┌─────────────┐
│    cmd/     │  → Can import: anything (entry points)
└──────┬──────┘
       ↓
┌─────────────┐
│  services/  │  → Can import: common/, tools/, pb/
└──────┬──────┘  → Cannot import: other services/, cmd/
       ↓
┌─────────────┐
│   common/   │  → Can import: tools/, pb/, other common/
└──────┬──────┘  → Cannot import: services/, cmd/
       ↓
┌─────────────┐
│   tools/    │  → Can import: stdlib, external packages only
└─────────────┘  → Cannot import: any internal multigres packages
```

### What Goes Where

- **tools/**: Pure utilities with zero internal dependencies (timer, netutil, retry, telemetry, etc.)
- **common/**: Shared infrastructure used by multiple services (parser, pgprotocol, servenv, client libraries, etc.)
- **services/**: Service implementations (multiadmin, multigateway, multipooler, multiorch, pgctld)
- **pb/**: Generated protobuf code (no changes)
- **cmd/**: Entry points that wire up services
- **provisioner/**: Special case for local cluster provisioning (stays top-level for now)

### Why services/ is Separate from cmd/

Keeping service code separate from cmd/ leaves open the option to compile multiple services into a single binary (similar to Vitess's vtcombo pattern), which can be useful for testing and lightweight deployments.

## Current State

### Already Completed ✅

Via PR #258, the following packages have been moved to `go/tools/`:

- `go/tools/timer` - Timer utilities
- `go/tools/netutil` - Network utilities
- `go/tools/grpccommon` - gRPC options
- `go/tools/telemetry` - OpenTelemetry integration
- `go/tools/retry` - Retry/backoff utilities
- `go/tools/asthelpergen` - AST code generation
- `go/tools/ruleguard` - Custom linting rules

### What Still Needs to Move

**To `go/common/`:** (12 packages)

- `go/clustermetadata` → `go/common/clustermetadata`
- `go/event` → `go/common/event`
- `go/fakepgdb` → `go/common/fakepgdb`
- `go/mterrors` → `go/common/mterrors`
- `go/parser` → `go/common/parser`
- `go/pgprotocol` → `go/common/pgprotocol`
- `go/plugins` → `go/common/plugins`
- `go/servenv` → `go/common/servenv`
- `go/viperutil` → `go/common/viperutil`
- `go/web` → `go/common/web`
- `go/multipooler/queryservice` → `go/common/queryservice`
- `go/multipooler/rpcclient` → `go/common/rpcclient`

**To `go/services/`:** (5 packages + 1 merge)

- `go/multiadmin` → `go/services/multiadmin`
- `go/multigateway` → `go/services/multigateway`
- `go/multiorch` → `go/services/multiorch`
- `go/multipooler` → `go/services/multipooler`
- `go/pgctld` → `go/services/pgctld`
- `go/admin/server` → merge into `go/services/multiadmin`

**Staying in Place:**

- `go/pb/` - Generated protobuf code
- `go/cmd/` - Command entry points
- `go/provisioner/` - Local provisioning (for now)
- `go/test/` - Test utilities

**Total Work:** ~18 moves + 1 merge, organized into ~15 PRs

## Execution Strategy

### Guiding Principles

1. **Common packages before services** - Services depend on common packages, so move common/ first
2. **Easy things first, progressively harder** - Start with small/stable packages, save large/active ones for later
3. **Small, independent PRs** - Each PR should be easy to review and unlikely to conflict with active work
4. **Incremental depguard enforcement** - Add rules as soon as constraints become true

### Grouping Strategy

We'll group moves by size and risk:

1. **Small common packages** (5-7 packages in 1-2 PRs) - Low risk, few imports
2. **Client libraries** (2 packages in 1 PR) - Medium risk, needed by services
3. **Large common packages** (5 packages, separate PRs) - Higher risk due to size or wide usage
4. **Admin merge** (1 PR) - Merge admin/server into multiadmin
5. **Services** (5 PRs) - Move each service independently after common packages are done

### Depguard Rules Timeline

- **Immediately**: Add `tools-isolation` rule (already satisfied)
- **After common/ moves complete**: Add `common-isolation` rule (when common/ no longer depends on unmoved packages)
- **After services/ moves complete**: Add `service-isolation` rules (preventing cross-service dependencies)
- **Throughout**: Add rules preventing incoming dependencies to common/ and services/ as soon as practical

### Flexibility

The exact order within each group can be adjusted based on active development to minimize merge conflicts. This plan focuses on packages rather than individual files.

## Detailed Task Breakdown

### Phase 1: Add tools/ isolation rule

- [ ] **PR: Enforce tools/ isolation**
  - Add depguard rule preventing `go/tools/**` from importing any `github.com/multigres/multigres/go` packages
  - Rule allows: stdlib, external packages, other tools packages
  - Validation: `golangci-lint run --disable-all --enable=depguard`

### Phase 2: Move small common packages

- [ ] **PR: Small stable packages**
  - Move `go/event` → `go/common/event`
  - Move `go/fakepgdb` → `go/common/fakepgdb`
  - Move `go/mterrors` → `go/common/mterrors`
  - Move `go/plugins` → `go/common/plugins`
  - Move `go/web` → `go/common/web`
  - Update imports throughout codebase
  - Run tests and build all binaries

### Phase 3: Move client libraries

- [ ] **PR: Client libraries to common/**
  - Move `go/multipooler/queryservice` → `go/common/queryservice`
  - Move `go/multipooler/rpcclient` → `go/common/rpcclient`
  - Update imports in multigateway and other consumers
  - These must move before multipooler moves to services/

### Phase 4: Move large common packages

Each of these should be a separate PR due to size or widespread usage:

- [ ] **PR: Move viperutil**
  - Move `go/viperutil` → `go/common/viperutil`
  - Widely used for configuration

- [ ] **PR: Move clustermetadata**
  - Move `go/clustermetadata` → `go/common/clustermetadata`
  - Critical topology infrastructure

- [ ] **PR: Move servenv**
  - Move `go/servenv` → `go/common/servenv`
  - Service framework, widely used

- [ ] **PR: Move parser**
  - Move `go/parser` → `go/common/parser`
  - Large package (60+ files), SQL parsing

- [ ] **PR: Move pgprotocol**
  - Move `go/pgprotocol` → `go/common/pgprotocol`
  - PostgreSQL wire protocol implementation

### Phase 5: Merge admin into multiadmin

- [ ] **PR: Consolidate admin code**
  - Merge `go/admin/server/` files into `go/multiadmin/`
  - Files: `backup.go`, `server.go`, `server_test.go`
  - Update package declarations from `package server` to `package multiadmin`
  - Remove `go/admin/` directory
  - This creates `go/multiadmin` in its final form before the services/ move

### Phase 6: Move services to services/

Each service should be a separate PR:

- [ ] **PR: Move multiadmin**
  - Move `go/multiadmin` → `go/services/multiadmin`
  - Update `go/cmd/multiadmin/` imports

- [ ] **PR: Move multigateway**
  - Move `go/multigateway` → `go/services/multigateway`
  - Update `go/cmd/multigateway/` imports

- [ ] **PR: Move multiorch**
  - Move `go/multiorch` → `go/services/multiorch`
  - Update `go/cmd/multiorch/` imports

- [ ] **PR: Move multipooler**
  - Move `go/multipooler` → `go/services/multipooler`
  - Update `go/cmd/multipooler/` imports
  - Note: queryservice and rpcclient already moved to common/

- [ ] **PR: Move pgctld**
  - Move `go/pgctld` → `go/services/pgctld`
  - Update `go/cmd/pgctld/` imports
  - Update `go/test/` references

### Phase 7: Add remaining depguard rules

- [ ] **PR: Add common/ isolation rule**
  - Prevent `go/common/**` from importing `go/services/**` or `go/cmd/**`
  - Allow: stdlib, tools, pb, other common packages

- [ ] **PR: Add service isolation rules**
  - Prevent each service from importing other services
  - One rule per service (multiadmin, multigateway, multiorch, multipooler, pgctld)
  - Use file exclusions to allow each service and its cmd to import itself

## Validation & Success Criteria

### Per-PR Validation

Each PR should validate:

1. **Linting passes**: `golangci-lint run --disable-all --enable=depguard`
2. **Tests pass**: `go test ./...`
3. **Binaries build**: All cmd/ binaries compile successfully
4. **Integration tests pass**: Run endtoend test suite where applicable

### Final Success Criteria

When all phases complete, we should have:

- ✅ All code organized into `tools/`, `common/`, `services/` directories
- ✅ Zero depguard violations
- ✅ All tests passing
- ✅ All binaries building successfully
- ✅ Clear dependency hierarchy enforced by tooling
- ✅ No cross-service dependencies
- ✅ Tools packages have no internal dependencies
- ✅ Common packages don't depend on services

### Rollback Strategy

Each PR is independent and can be reverted if issues arise. Since we're only changing import paths (not package names or APIs), rollback is straightforward.

### Document Updates

As PRs merge, update the checkboxes in this document to track progress. This allows anyone (including future Claude sessions) to pick up where we left off.

## Example depguard Configuration

Here's what the final `.golangci.yml` depguard configuration should look like:

```yaml
linters-settings:
  depguard:
    rules:
      # Rule 1: Tools cannot import internal packages
      tools-isolation:
        files: ["**/go/tools/**/*.go"]
        list-mode: lax
        deny:
          - pkg: "github.com/multigres/multigres/go"
            desc: "tools packages must not depend on internal multigres packages (except other tools)"
        allow:
          - $gostd
          - github.com/multigres/multigres/go/tools

      # Rule 2: Common cannot import services or cmd
      common-isolation:
        files: ["**/go/common/**/*.go"]
        list-mode: lax
        deny:
          - pkg: "github.com/multigres/multigres/go/services"
            desc: "common packages must not import services"
          - pkg: "github.com/multigres/multigres/go/cmd"
            desc: "common packages must not import cmd packages"
        allow:
          - $gostd
          - github.com/multigres/multigres/go/common
          - github.com/multigres/multigres/go/tools
          - github.com/multigres/multigres/go/pb

      # Rule 3: Services cannot import other services
      # Each service needs its own rule to allow self-imports
      multiadmin-isolation:
        files:
          - "!**/go/services/multiadmin/*.go"
          - "!**/go/services/multiadmin/**/*.go"
          - "!**/go/cmd/multiadmin/*.go"
          - "!**/go/cmd/multiadmin/**/*.go"
          - "**/*.go"
        list-mode: lax
        deny:
          - pkg: "github.com/multigres/multigres/go/services/multiadmin"
            desc: "multiadmin can only be imported by its own package and cmd"

      # Similar rules for multigateway, multiorch, multipooler, pgctld...

      # Keep existing provisioner rule
      provisioner:
        files:
          - "!$test"
          - "**/go/provisioner/*.go"
          - "**/go/provisioner/**/*.go"
        list-mode: lax
        deny:
          - pkg: "github.com/multigres/multigres/go"
            desc: "provisioners should not depend on multigres logic aside from the topo"
        allow:
          - $gostd
          - github.com/multigres/multigres/go/tools
          - github.com/multigres/multigres/go/pb
          - github.com/multigres/multigres/go/provisioner

      # Keep existing use_modern_packages rule
      use_modern_packages:
        list-mode: lax
        deny:
          - pkg: "math/rand$"
            desc: "Please use math/rand/v2"
```
