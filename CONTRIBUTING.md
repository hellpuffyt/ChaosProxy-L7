# Contributing to ChaosProxy-L7

Thanks for helping. Bug reports with a reproducible config are as valuable as code.

## Development setup

You need Rust 1.97+ (stable is fine). Nothing else is required for the main test suite.

```sh
git clone https://github.com/hellpuffyt/ChaosProxy-L7 && cd ChaosProxy-L7
cargo test --all-features                     # unit + integration tests (no external services)
cargo clippy --all-targets --all-features -- -D warnings
cargo fmt --all
```

Live PostgreSQL tests need a PostgreSQL server installation (any recent
version with `initdb`, `pg_ctl`, `postgres`). They create and delete their
own throwaway cluster; nothing is left running:

```sh
CHAOSPROXY_PG_BIN=/usr/lib/postgresql/16/bin cargo test --test pg_live --test pg_live_logs -- --ignored
# macOS + Homebrew: CHAOSPROXY_PG_BIN=$(brew --prefix postgresql@17)/bin
```

Faster builds without wasmtime: `cargo test --no-default-features` (skips the
wasm sandbox tests).

## Ground rules

- Every behaviour change comes with a test; every bug fix with a regression test.
- Never weaken a safety default (loopback bind, upstream allowlist, log redaction,
  wasm limits) without discussion in an issue first.
- Keep the HTTP code (`src/http`) and the PostgreSQL code (`src/pg`) independent.
- Tests that need something not always available must be `#[ignore = "reason"]`
  and run explicitly in CI — never silently pass when the dependency is missing.
- No `unwrap`/`expect`/`panic` outside tests (enforced by clippy).
- Do not commit token-shaped strings, even fake ones in tests.

Good places to start: [docs/good-first-issues.md](docs/good-first-issues.md).

By contributing you agree your work is licensed under MIT OR Apache-2.0, like the project.
