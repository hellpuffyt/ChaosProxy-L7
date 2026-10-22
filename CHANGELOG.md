# Changelog

All notable changes are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project uses
[Semantic Versioning](https://semver.org/) (pre-1.0: minor versions may break config).

## [Unreleased]

### Fixed
- HTTP shutdown: connections still open when `shutdown_grace` ran out were left running in detached tasks
  (a `hang` or slow upstream kept going after `shutdown()` returned, and the log claimed they were dropped).
  They are now owned by the listener, aborted after the grace period, and their upstream requests cancelled
  before `shutdown()` returns.
- Access log: requests that never produced a response (`reset`/`hang` faults, client disconnects, shutdown)
  had no `request` log line. Every request now logs exactly one line; those carry `aborted=true`.

### Added
- `tests/http_concurrency.rs`: 512 simultaneous requests with seven fault kinds (HTTP/1.1 and h2c upstream)
  asserting completion, fault/log agreement, and no leaked permits, tasks, descriptors or upstream connections;
  determinism of fault decisions under racing requests; shutdown abort bound; mid-stream client disconnect.
- `tests/http_malformed.rs`: CL+TE smuggling attempts, ambiguous framing, oversized headers, invalid chunked
  bodies, and garbage/closing/truncating upstreams.

## [0.1.0] - 2026-10-22

### Added
- HTTP/1.1 and h2c (prior knowledge) reverse proxy on one loopback port, HTTP/1.1 or h2c to the upstream.
- HTTP faults: latency + jitter, first-byte delay, bandwidth throttle, TCP reset, injected 504 timeout,
  bounded hang, injected error response. Rules match on path prefix/regex, method, exact headers and an
  optional WebAssembly module; seeded, reproducible percentage rolls; per-rule concurrency caps.
- PostgreSQL v3 wire-protocol proxy with query-level faults (error with SQLSTATE, delay, drop after N rows)
  and startup rejection; correct ordering with pipelined extended-protocol batches.
- WebAssembly matcher ABI v1 on Wasmtime: no imports, fuel, wall-clock and memory limits, fresh instance per
  decision, fail-closed runtime errors.
- Safety rails: loopback-only bind unless allowed in both config and CLI, mandatory upstream allowlist,
  observe/dry-run mode, global concurrency limit, log redaction (no bodies, credentials, query strings, SQL text).
- `chaosproxy check` and `chaosproxy run`, JSON logs, graceful shutdown on SIGINT/SIGTERM.
