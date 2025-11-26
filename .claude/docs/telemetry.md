# Telemetry and Logging

## Logging

Use `*Context()` log methods (e.g., `InfoContext()` over `Info()`) to propagate telemetry data like trace IDs.

## Context Usage

Avoid `context.Background()`. Most contexts should inherit from:

- The top-level Cobra command context (in services)
- `t.Context()` (in tests, except `Cleanup()` where context is cancelled)

Propagate existing context to preserve cancellation and telemetry (tracing spans). When unsure, prefer `context.TODO()` over `context.Background()`.

## OpenTelemetry

Follow OpenTelemetry naming conventions for metrics, attributes, and span names. Separate metric definitions from instrumented code. Telemetry data is a form of API—aim for clean, useful, stable output.
