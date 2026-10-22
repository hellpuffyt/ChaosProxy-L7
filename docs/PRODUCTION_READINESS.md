# Production-readiness report — ChaosProxy-L7 0.1.0

**Classification: Partially Verified.** Everything below was run on one developer machine. GitHub Actions
(Linux/macOS/Windows test matrix, CI PostgreSQL job, MSRV job) has **not run yet**, Windows has not been
exercised at all, and the tool has had no external security review. "Production-ready" would also be the wrong
goal: this is a test-environment tool that must never front production traffic.

- Tested code commit: `ed5b9eea0367dfc4b18f44e5639f6b06507e7ba9` (later commits change documentation only;
  check with `git diff --stat ed5b9ee HEAD`).
- Verification date: 2026-10-22.
- Machine: Apple M5, 10 cores, 16 GB, macOS 27; rustc 1.98.0 stable; PostgreSQL 17.11 (Homebrew).
  The machine was heavily loaded during testing (load average 18–24 from other builds); timing-based tests use
  lower bounds only, so load cannot make them pass spuriously.

## Scope

A loopback-by-default fault-injection proxy for local and CI resilience testing: HTTP/1.1 + h2c reverse proxy,
a PostgreSQL v3 wire-protocol proxy, and sandboxed WebAssembly matchers. See [ARCHITECTURE.md](ARCHITECTURE.md)
and [LIMITATIONS.md](LIMITATIONS.md).

## Supported platforms and prerequisites

| | |
|---|---|
| Rust | 1.97+ with default features (wasmtime 50 requires it; verified with `cargo +1.97 check --all-features --all-targets --locked`); 1.88+ for `--no-default-features` (lib + binary checked, tests not run on 1.88) |
| OS | macOS (tested), Linux and Windows (CI configured, not yet run) |
| Optional | PostgreSQL server binaries (`initdb`, `pg_ctl`, `postgres`) for the live tests |

## Clean-clone setup (as documented in README)

```sh
git clone https://github.com/hellpuffyt/ChaosProxy-L7 && cd ChaosProxy-L7
cargo test --all-features
cargo run --release -- check --config examples/chaos.toml
cargo run --example echo_upstream &
cargo run --release -- run --config examples/chaos.toml --pretty
```

The repository is not yet published, so clean-clone runs cloned the local repository into a fresh scratch
directory with a fresh `CARGO_TARGET_DIR`:

| Commit | Result |
|---|---|
| `37815ef` | `cargo test --all-features` exit 0 (70 passed, 0 failed, 7 ignored); `check` exit 0; demo reproduced the README output exactly; working tree clean after |
| `ed5b9ee` | `cargo test --all-features` exit 0 (77 passed, 0 failed, 7 ignored); live PostgreSQL tests 7/7; `check` exit 0; demo output identical to README; working tree clean after |

## Commands executed (on `ed5b9ee`) and outcomes

| Command | Exit | Result |
|---|---|---|
| `cargo fmt --all --check` | 0 | |
| `cargo clippy --all-targets --all-features --locked -- -D warnings` | 0 | |
| `cargo clippy --all-targets --no-default-features --locked -- -D warnings` | 0 | |
| `RUSTDOCFLAGS="-D warnings" cargo doc --no-deps --all-features --locked` | 0 | |
| `cargo test --all-features --locked` | 0 | 77 passed, 0 failed, 7 ignored (the live PostgreSQL tests) |
| `cargo test --no-default-features --locked` | 0 | 69 passed, 0 failed, 7 ignored |
| `CHAOSPROXY_PG_BIN=$(brew --prefix postgresql@17)/bin cargo test --locked --test pg_live --test pg_live_logs -- --ignored` | 0 | 7 passed, 0 failed |
| `cargo deny check` | 0 | advisories, bans, licenses, sources ok |
| `cargo audit` (241 crates) | 0 | no vulnerabilities |
| `cargo +1.97 check --all-features --all-targets --locked` | 0 | MSRV holds |

**Skipped / not run:** nothing was skipped silently. The 7 live PostgreSQL tests are `#[ignore]`d in a plain
`cargo test` (they need server binaries) and were run explicitly above. No benchmarks exist.

## CI status

`.github/workflows/ci.yml`: lint (fmt, clippy ×2, doc), test matrix (ubuntu/macos/windows), live PostgreSQL on
Ubuntu, MSRV 1.97, cargo-deny. Action SHAs were checked against their release tags with `gh api`.
**Status: not yet run** (repository unpublished).

## Bugs found and fixed during development

| Bug | Fix | Regression test |
|---|---|---|
| PostgreSQL relay read frames with `read_exact` inside `select!`: a splice message winning the race could drop half a server frame | buffered `read_buf` + `decode_frame` (cancellation-safe) | live pipelining test; `pg::conn::tests` |
| Unbounded injected-response queues per PostgreSQL session (memory growth with a slow server and a pipelining client) | capped at 4096 (session closed beyond), names tracked capped at 65,536 | `pg::conn::tests::pending_injections_are_bounded`, `tracked_names_are_bounded_but_known_names_update` |
| Throttled response kept a stalled upstream connection after the client disconnected | pacing task also waits on channel closure | `http::body::tests::throttle_releases_a_stalled_upstream_when_the_client_goes_away` (verified to fail without the fix) |
| Declared MSRV 1.89 was wrong (wasmtime 50 needs 1.97) | `rust-version = "1.97"`, CI MSRV job | MSRV job |
| Log-capture test saw no events when run alongside other tests (thread-local test subscriber vs. concurrent callsite registration) — test harness issue, not product | test moved to its own binary `tests/pg_live_logs.rs` | itself |

## Security review (self-review, not an audit)

- Secrets scan of every committed file and the full history: no tokens, keys, local paths or personal data.
- Network exposure: loopback-only unless both opt-ins (tests: `tests/safety.rs`, `tests/cli.rs`).
- SSRF-style misuse: upstreams must be allowlisted; one upstream per listener; the proxy never follows redirects.
- Log hygiene: no bodies, header values, query strings, PostgreSQL passwords or SQL text (tests with a marker
  value: `tests/safety.rs::logs_never_contain_credentials_bodies_or_query_strings`, `tests/pg_live_logs.rs`).
- Untrusted input: PostgreSQL frames are length-checked against `max_message_bytes` (startup ≤ 10 kB); codec
  property tests feed random bytes. HTTP parsing is hyper's. Regexes use the linear-time `regex` crate.
- WebAssembly: no imports, fuel, epoch deadline, memory limit, fresh instance, fail-closed (`tests/wasm_sandbox.rs`).
- Resource limits: global `max_concurrency`, per-rule caps, bounded fault durations (≤ 10 min), header-read
  timeout, upstream response timeout, bounded per-session queues.
- Known gaps: no client authentication, no TLS, no rate limiting per client, not hardened for untrusted networks.

## Acceptance checklist

| # | Expected behaviour | Command / test | Result | Evidence |
|---|---|---|---|---|
| 1 | Forwards HTTP/1.1 with hop-by-hop stripping and X-Forwarded-* | `tests/http_forwarding.rs::forwards_get_and_rewrites_headers` | pass | 77/0/7 run |
| 2 | Streams 4 MB request / 3 MB response bodies intact | `streams_large_request_and_response_bodies` | pass | |
| 3 | 504 on upstream timeout, 502 on unreachable upstream | `upstream_timeout_returns_504`, `unreachable_upstream_returns_502` | pass | |
| 4 | Client disconnect cancels the upstream request | `client_disconnect_cancels_upstream_request` | pass | |
| 5 | h2c client→proxy and proxy→upstream | `h2c_client_to_proxy`, `h2c_proxy_to_upstream` | pass | |
| 6 | Graceful shutdown completes in-flight requests | `graceful_shutdown_lets_in_flight_requests_finish`; CLI SIGTERM test | pass | |
| 7 | Each HTTP fault kind behaves as documented and only at the HTTP layer | `tests/http_faults.rs` (12 tests) | pass | |
| 8 | Same seed + same order ⇒ same faults; different seed ⇒ different pattern | `percentage_decisions_are_reproducible_by_seed`; demo run twice | pass | identical demo output across runs and clones |
| 9 | Observe mode injects nothing | `observe_mode_logs_but_does_not_inject`, `reject_startup_and_observe_mode` | pass | |
| 10 | Non-loopback bind needs both opt-ins; upstream allowlist enforced | `tests/safety.rs`, `tests/cli.rs` | pass | |
| 11 | Logs contain no credentials, bodies, query strings, SQL text | safety + `pg_live_logs` | pass | |
| 12 | PostgreSQL passthrough with SCRAM, simple + extended protocol | `pg_live::passthrough_with_scram_auth_simple_and_extended_protocol` | pass (PostgreSQL 17.11) | |
| 13 | Injected SQLSTATE error is not executed and the session survives | `injected_error_on_simple_query_is_not_executed_and_session_survives` | pass | |
| 14 | Error on extended `Execute` keeps pipelined neighbours correct | `injected_error_on_extended_execute_and_pipelined_neighbours_are_unaffected` | pass | |
| 15 | Delay, drop-after-N-rows, startup rejection | `pg_live` (3 tests), `pg_offline` | pass | |
| 16 | Malformed PostgreSQL input closes the connection without panicking | `pg_offline::malformed_startup_closes_the_connection`, codec property tests | pass | |
| 17 | Wasm: bad modules refused at load; trap/fuel/time/memory/bad return fail closed | `tests/wasm_sandbox.rs` (8 tests) | pass | |
| 18 | Clean clone follows README and reproduces documented output | manual runs on `37815ef` and `ed5b9ee` | pass | table above |
| 19 | CI green on Linux, macOS, Windows | GitHub Actions | **not verified** | repo unpublished |
| 20 | Works on Windows | — | **not verified** | no Windows machine |

## Deep-audit addendum (after 0.1.0)

Two defects found and fixed with regression tests that fail on the previous code
(`shutdown_aborts_what_outlives_the_grace_period`; the access-log assertion in both
`mixed_faults_under_512_concurrent_requests_*` tests): HTTP connections outliving `shutdown_grace` were never
aborted, and requests without a response had no access-log line. New tests: `tests/http_concurrency.rs` (5) and
`tests/http_malformed.rs` (5); `cargo test --all-features` now reports 87 passed, 7 ignored. Request smuggling
(CL+TE in both orders, obfuscated/duplicated framing headers), oversized headers and invalid chunked bodies were
probed through the proxy: hyper refuses or re-frames them and the upstream never receives both framings or a
smuggled request — no proxy defect found there.

## External requirements for further verification

- GitHub Actions (publishing the repository) for the Linux/Windows matrix and the CI PostgreSQL job.
- A Windows machine for manual testing.
- Clients beyond `curl`, hyper and `tokio-postgres` (for example libpq/psql, JDBC, Go pgx) to confirm PostgreSQL
  protocol compatibility more broadly.
