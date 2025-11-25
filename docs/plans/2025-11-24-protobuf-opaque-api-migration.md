# Protobuf Opaque API Migration Plan

**Date:** 2025-11-24
**Status:** Proposed

## Executive Summary

Migrate from Protocol Buffers Open Struct API to the new Opaque API to:

1. Enable future read-only view interfaces (reduces defensive copying)
2. Improve performance (lazy decoding, reduced allocations)
3. Prevent common protobuf bugs (pointer comparison, accidental sharing)
4. Align with protobuf defaults (Edition 2024 makes Opaque the default)

**Effort:** ~4-8 hours (mostly testing)
**Risk:** Low (automated tooling, unreleased code)

## Background

### Current State

- **Protobuf module:** v1.36.10 ✅
- **Protoc version:** 25.1 (needs upgrade to 29.0+)
- **Proto files:** 10 files in `proto/` directory
- **Go usage:** ~433 references across 45 files
- **Status:** Unreleased internal code (no compatibility concerns)

### Why Migrate Now

The current Open Struct API exposes proto fields directly, preventing:

- Generation of read-only view interfaces (still requires defensive copying)
- Internal representation optimizations
- Protection against common bugs

The Opaque API uses private fields with accessor methods, which:

- Enables future read-only interface generation via custom tooling
- Reduces memory allocations (57% fewer in benchmarks)
- Enables lazy decoding (58% faster, 87% fewer allocations for submessages)
- Prevents enum comparison bugs and accidental pointer sharing

### What Changes

**Before (Open Struct API):**

```go
type MultiPooler struct {
    Type PoolerType  // Public field
    Id   *ID
}

// Direct field access
pooler.Type = PoolerType_PRIMARY
pooler := &MultiPooler{Type: PoolerType_PRIMARY}
```

**After (Opaque API):**

```go
type MultiPooler struct {
    // private fields
}

// Accessor methods
pooler.SetType(PoolerType_PRIMARY)
pooler := &MultiPooler{}
pooler.SetType(PoolerType_PRIMARY)

// Or use Builder pattern
```

## Migration Plan

### Phase 1: Prerequisites

**1.1 Upgrade protoc to 29.0+**

- Download protoc 29.0+ from https://github.com/protocolbuffers/protobuf/releases
- Update build scripts/Makefiles to use new version
- Test that existing proto compilation still works

**1.2 Install open2opaque tool**

```bash
go get -tool github.com/golang/open2opaque/cmd/open2opaque@latest
```

**1.3 Create feature branch**

```bash
git checkout -b protobuf-opaque-migration
```

### Phase 2: Migration (Automated)

**2.1 Switch proto files to HYBRID API**

```bash
# Hybrid API adds accessor methods while keeping fields visible
# This allows incremental migration
open2opaque setapi -api HYBRID proto/*.proto
```

**2.2 Regenerate proto files**

```bash
# Use your existing proto generation command
# e.g., make proto or buf generate
```

**2.3 Rewrite Go code to use accessors**

```bash
# -levels=red includes all rewrite patterns
# Tool will report confidence levels: green (safe), yellow (review), red (careful review)
open2opaque rewrite -levels=red ./go/...
```

**2.4 Review red-level rewrites**

- Tool will flag complex patterns (e.g., functions expecting mutable pointers)
- Manually review and adjust if needed
- Most common in test files with direct field assignments

**2.5 Run tests**

```bash
go test ./...
```

Fix any issues identified by tests.

**2.6 Switch to full OPAQUE API**

```bash
open2opaque setapi -api OPAQUE proto/*.proto
```

**2.7 Regenerate proto files again**

```bash
# Use your existing proto generation command
```

**2.8 Final test run**

```bash
go test ./...
# Run any integration tests
# Run linters
```

### Phase 3: Validation

**3.1 Run full test suite**

- Unit tests
- Integration tests
- End-to-end tests

**3.2 Manual verification**

- Check that common operations still work
- Verify error handling is preserved
- Test concurrent access patterns

**3.3 Code review**

- Review generated .pb.go changes
- Review rewritten Go code (especially red-level changes)
- Verify no direct field access remains

### Phase 4: Merge

**4.1 Commit changes**

```bash
git add proto/ go/
git commit -m "feat: migrate to protobuf Opaque API

Migrates from Open Struct API to Opaque API for:
- Better performance (lazy decoding, reduced allocations)
- Future support for read-only view interfaces
- Protection against common protobuf bugs

Migration performed using open2opaque automated tooling.
All tests passing."
```

**4.2 Create PR and merge**

- Since code is unreleased, can merge directly to main
- Or follow standard PR review process

## Known Migration Patterns

Based on codebase analysis, these patterns will be automatically rewritten:

### Pattern 1: Direct field assignment

```go
// Before
pm.multipooler.Type = clustermetadatapb.PoolerType_PRIMARY

// After
pm.multipooler.SetType(clustermetadatapb.PoolerType_PRIMARY)
```

### Pattern 2: Struct literals

```go
// Before
&clustermetadata.MultiPooler{
    Id: &clustermetadata.ID{
        Component: clustermetadata.ID_MULTIPOOLER,
    },
}

// After (Builder pattern)
multipooler := &clustermetadata.MultiPooler{}
id := &clustermetadata.ID{}
id.SetComponent(clustermetadata.ID_MULTIPOOLER)
multipooler.SetId(id)
```

### Pattern 3: Clone operations

```go
// Before and After - same
proto.Clone(pm.multipooler.MultiPooler).(*clustermetadatapb.MultiPooler)
// proto.Clone still works with Opaque API
```

## Post-Migration: Future Enhancements

Once migrated to Opaque API, we can optionally:

### Option 1: Read-only interface generation

Create tooling to generate read-only view interfaces:

```go
// Generated interface (getters only)
type MultiPoolerView interface {
    GetType() PoolerType
    GetId() *ID
    // No setters
}

// Opaque MultiPooler implements this automatically
// Can return MultiPoolerView instead of *MultiPooler to prevent mutation
```

This would allow:

- Returning read-only views without defensive copying
- Clear API contracts about mutability
- Better concurrent access safety

**Implementation options:**

1. Custom protoc plugin (generates alongside .pb.go)
2. Post-processing tool via `go generate`
3. Manual interfaces for critical paths only

### Option 2: Performance profiling

With lazy decoding enabled, profile real workloads to measure:

- Allocation reductions
- Decoding time improvements
- Memory usage improvements

## Rollback Plan

If critical issues are discovered post-migration:

1. Revert the commit
2. Regenerate proto files with Open Struct API
3. Original code still works (Open Struct is still supported)

**Note:** Given automated tooling and comprehensive test suite, rollback should not be necessary.

## Success Criteria

- ✅ All tests pass
- ✅ No direct field access remains in codebase
- ✅ Generated .pb.go files use private fields
- ✅ Linters pass
- ✅ No performance regressions in benchmarks

## References

- [Go Protobuf Opaque API Blog Post](https://go.dev/blog/protobuf-opaque)
- [Opaque API Migration Guide](https://protobuf.dev/reference/go/opaque-migration/)
- [Opaque API FAQ](https://protobuf.dev/reference/go/opaque-faq/)
- [open2opaque Tool](https://github.com/golang/open2opaque)

## Timeline

**Estimated time:** 4-8 hours

- Phase 1 (Prerequisites): 30 mins
- Phase 2 (Migration): 1 hour
- Phase 3 (Validation): 2-6 hours (depending on test suite)
- Phase 4 (Merge): 30 mins

Can be completed in a single work session or split across multiple sessions.
