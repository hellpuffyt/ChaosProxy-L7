# Known limitations

Honest list for 0.1.0. If one of these blocks you, an issue or PR is welcome.

## Scope and safety

- **Testing tool only.** Not hardened for untrusted networks, no client authentication, no TLS. Do not run it in
  front of production traffic.
- One upstream per listener; no routing by host or path.
- `max_concurrency` counts HTTP requests and PostgreSQL sessions together; there is no per-client limit.

## HTTP

- No TLS on either side; HTTP/2 only as h2c (prior knowledge). Browsers will use HTTP/1.1.
- `CONNECT` and `Upgrade` (WebSocket) requests get `501 Not Implemented`.
- `request_timeout` covers the upstream's response **headers** only; slow bodies are not timed out.
- A `reset` or `hang` fault on an HTTP/2 connection affects every stream on that connection (it is a TCP-level action).
- Fault determinism assumes requests arrive in the same order. Concurrent clients (or other software polling the
  port) change sequence numbers and therefore which requests are faulted.
- `throttle` paces data frames only; it does not slow the response head.
- Trailers are forwarded by hyper as frames; they were not specifically tested.

## PostgreSQL

- TLS is declined (`N`); clients must allow `sslmode=disable` or `prefer`. GSSAPI encryption is declined too.
- Faults match the SQL text of simple `Query` messages and of statements run by `Execute`. `COPY`, function-call
  (`F`) messages and replication protocols are relayed but cannot be faulted.
- An injected `error` inside an explicit transaction does **not** abort the server-side transaction: the server
  never saw the statement. The `ReadyForQuery` sent with the injected error reports the server's last known
  status (`T`), so the client and server can disagree about whether the transaction failed.
- An injected `error` on `Execute` discards the rest of that extended-protocol batch up to `Sync`, as a real
  server would; messages sent after `Execute` in the same batch never reach the server.
- `delay` holds the query before forwarding (it does not delay the result stream).
- `drop_mid_result` closes the session only if the result has more than `after_rows` rows.
- `CancelRequest` is relayed to the upstream; it is not matched by rules.
- Only protocol 3.0 startup is accepted.
- Per session, at most 4096 injected responses may wait for the server (more closes the session), and at most
  65,536 prepared-statement / portal names are tracked; `Execute`s of untracked names cannot be faulted.

## WebAssembly

- Matchers decide only *whether* a rule applies; they cannot choose or parameterise the fault.
- A new instance per decision costs some microseconds; heavy modules will add latency to matching requests.
- If the epoch-ticker thread cannot be started, only fuel limits apply (an error is logged).

## Platforms

- Developed and tested on macOS (Apple Silicon). CI covers Ubuntu, macOS and Windows for the portable test suite;
  the live PostgreSQL tests run on Ubuntu (CI) and macOS (local). Windows has not been exercised by hand.
- Minimum Rust: 1.97 with the default `wasm` feature (wasmtime 50); 1.88 for `--no-default-features`
  (lib and binary checked; tests not run on 1.88).

## Not done yet

- No benchmarks; no performance claims.
- No release binaries or container image.
- No Docker Compose demo (the demo runs with `cargo run`).
