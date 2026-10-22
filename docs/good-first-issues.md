# Good first issues

Concrete starter tasks, each small and testable.

1. **`Retry-After` on injected errors.** Add an optional `retry_after` to the HTTP `error` fault and set the header.
   Touches `config.rs`, `rules.rs`, `http/service.rs`; add a case to `tests/http_faults.rs`.
2. **Abort mid-body fault.** A new HTTP fault `abort_body { after_bytes }` that forwards the response and resets the
   connection after N body bytes (the `throttle` task in `http/body.rs` shows how to wrap a body).
3. **Count decisions per rule.** Keep injected/observed/skipped counters per rule and log a summary on shutdown.
   Touches `rules.rs` and `main.rs`.
4. **`chaosproxy check --json`.** Machine-readable output of the parsed config for CI pipelines (`main.rs`).
5. **Fault on `Parse`.** Let PostgreSQL `error` rules optionally fire when a statement is prepared, not only when it
   is executed (`pg/conn.rs`; extend `tests/pg_live.rs`).
