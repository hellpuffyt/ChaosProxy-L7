# Security policy

ChaosProxy-L7 is a **testing tool that deliberately breaks traffic**. It is
experimental (0.1.0) and has not had an external security review.

## Reporting a vulnerability

Please report privately through GitHub:
<https://github.com/hellpuffyt/ChaosProxy-L7/security/advisories/new>

Do not open a public issue. Include the version or commit, your config with
secrets removed, and steps to reproduce. You will get an acknowledgement as
soon as a maintainer sees it; there is no paid support or guaranteed response time.

## What counts as a vulnerability here

- A way to make the proxy listen on a non-loopback address, or connect to an
  upstream outside `upstream_allowlist`, without both documented opt-ins.
- Request/response bodies, credentials (`Authorization`, `Cookie`, PostgreSQL
  passwords), query strings or SQL text (with `log_query_text = false`)
  appearing in logs.
- A WebAssembly module escaping its sandbox (imports, unbounded CPU, memory or
  time) or causing the proxy to inject a fault after a module error.
- Crashes or unbounded memory growth triggered by malformed HTTP or PostgreSQL
  input from a client or upstream.

## Out of scope

- Faults doing exactly what you configured them to do.
- Running the proxy in front of production traffic, or exposing it to an
  untrusted network: both are unsupported (see README and docs/LIMITATIONS.md).
- No TLS on either side: documented, not a vulnerability.
