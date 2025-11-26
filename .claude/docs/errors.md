# Error Handling

**Use `mterrors` instead of `fmt.Errorf()`** to preserve stack traces and canonical codes. Exception: `go/tools/` cannot depend on `mterrors`, but callers should wrap errors from tools/third-party libraries.

## Predefined MT Error Codes

Located in `go/common/mterrors/code.go`:

- For common, documented errors, define an MT code: `mterrors.MT05001(stmtName)`
- Numbering follows Vitess pattern: MT03xxx (INVALID_ARGUMENT), MT05xxx (NOT_FOUND), MT09xxx (FAILED_PRECONDITION), MT13xxx (INTERNAL)
- Each code includes an ID, message template, and documentation string

## When to Use What

- `mterrors.MTxxxxx(args...)` - predefined errors with documentation
- `mterrors.Wrapf(err, "context")` - wrapping errors from tools, libraries, or lower layers
- `mterrors.Errorf(code, "message")` - one-off errors without predefined codes

## Checking Errors

- `mterrors.Code(err)` returns the canonical code
- `mterrors.IsError(err, "MT05001")` checks for specific MT codes
- Always check error returns; never discard with `_`
