## What and why

## How it was tested
- [ ] `cargo fmt --all --check`
- [ ] `cargo clippy --all-targets --all-features --locked -- -D warnings` (and `--no-default-features`)
- [ ] `cargo test --all-features --locked`
- [ ] If PostgreSQL code changed: `CHAOSPROXY_PG_BIN=<pg bin dir> cargo test --test pg_live --test pg_live_logs -- --ignored`
- [ ] New behaviour has a test; fixed bugs have a regression test

## Safety
- [ ] Defaults stay safe (loopback bind, allowlist, nothing sensitive logged)
- [ ] Docs (README / docs/LIMITATIONS.md) updated if behaviour or limits changed
