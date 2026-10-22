# Changelog

All notable changes are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project uses
[Semantic Versioning](https://semver.org/) (pre-1.0: minor versions may break config).

## [Unreleased]

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
