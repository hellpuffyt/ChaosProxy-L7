//! Safety rails: loopback-only by default, upstream allowlist, strict validation,
//! and logs that never contain secrets, bodies or query strings.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod common;

use std::io::Write;
use std::sync::{Arc, Mutex};

use chaosproxy::config::Config;
use common::*;
use http::Request;
use http_body_util::Full;

fn base(listen: &str, allow: bool, upstream: &str, allowlist: &str) -> String {
    format!(
        "allow_non_loopback = {allow}\nupstream_allowlist = [{allowlist}]\n[http]\nlisten = \"{listen}\"\nupstream = \"{upstream}\"\n"
    )
}

fn err(text: &str, cli: bool) -> String {
    Config::parse(text, cli)
        .expect_err("should be rejected")
        .to_string()
}

#[test]
fn loopback_is_the_default_and_non_loopback_needs_two_opt_ins() {
    let ok = base(
        "127.0.0.1:8080",
        false,
        "http://127.0.0.1:9000",
        "\"127.0.0.1:9000\"",
    );
    assert!(Config::parse(&ok, false).is_ok());
    let v6 = base(
        "[::1]:8080",
        false,
        "http://127.0.0.1:9000",
        "\"127.0.0.1:9000\"",
    );
    assert!(Config::parse(&v6, false).is_ok());

    let wide = base(
        "0.0.0.0:8080",
        false,
        "http://127.0.0.1:9000",
        "\"127.0.0.1:9000\"",
    );
    let e = err(&wide, false);
    assert!(
        e.contains("non-loopback") && e.contains("--allow-non-loopback"),
        "{e}"
    );
    assert!(
        err(&wide, true).contains("non-loopback"),
        "CLI flag alone is not enough"
    );
    let wide_cfg = base(
        "0.0.0.0:8080",
        true,
        "http://127.0.0.1:9000",
        "\"127.0.0.1:9000\"",
    );
    assert!(
        err(&wide_cfg, false).contains("non-loopback"),
        "config flag alone is not enough"
    );
    assert!(
        Config::parse(&wide_cfg, true).is_ok(),
        "both opt-ins together are accepted"
    );
}

#[test]
fn upstreams_must_be_allowlisted() {
    let missing = base(
        "127.0.0.1:8080",
        false,
        "http://10.1.2.3:80",
        "\"127.0.0.1:9000\"",
    );
    let e = err(&missing, false);
    assert!(
        e.contains("not in upstream_allowlist") && e.contains("10.1.2.3:80"),
        "{e}"
    );
    let default_port = base(
        "127.0.0.1:8080",
        false,
        "http://localhost",
        "\"localhost:80\"",
    );
    assert!(Config::parse(&default_port, false).is_ok());
    let pg = "upstream_allowlist = []\n[postgres]\nlisten = \"127.0.0.1:6543\"\nupstream = \"127.0.0.1:5432\"\n";
    assert!(err(pg, false).contains("PostgreSQL upstream"));
}

#[test]
fn rejects_https_paths_and_bad_rules() {
    let https = base(
        "127.0.0.1:8080",
        false,
        "https://127.0.0.1:9000",
        "\"127.0.0.1:9000\"",
    );
    assert!(err(&https, false).contains("https upstreams are not supported"));
    let path = base(
        "127.0.0.1:8080",
        false,
        "http://127.0.0.1:9000/api",
        "\"127.0.0.1:9000\"",
    );
    assert!(err(&path, false).contains("must not contain a path"));
    let head = base(
        "127.0.0.1:8080",
        false,
        "http://127.0.0.1:9000",
        "\"127.0.0.1:9000\"",
    );
    let cases = [
        (
            "[[http.rules]]\nid = \"a\"\npercentage = 150.0\n[http.rules.fault]\nkind = \"reset\"\n",
            "percentage",
        ),
        (
            "[[http.rules]]\nid = \"a\"\n[http.rules.fault]\nkind = \"error\"\nstatus = 999\n",
            "status",
        ),
        (
            "[[http.rules]]\nid = \"a\"\n[http.rules.fault]\nkind = \"hang\"\nmax = \"2h\"\n",
            "at most",
        ),
        (
            "[[http.rules]]\nid = \"a\"\n[http.rules.fault]\nkind = \"throttle\"\nbytes_per_second = 0\n",
            "bytes_per_second",
        ),
        (
            "[[http.rules]]\nid = \"a\"\n[http.rules.match]\npath_regex = \"(\"\n[http.rules.fault]\nkind = \"reset\"\n",
            "path_regex",
        ),
        (
            "[[http.rules]]\nid = \"a\"\n[http.rules.fault]\nkind = \"reset\"\n[[http.rules]]\nid = \"a\"\n[http.rules.fault]\nkind = \"reset\"\n",
            "duplicate",
        ),
        (
            "[[http.rules]]\nid = \"a b\"\n[http.rules.fault]\nkind = \"reset\"\n",
            "rule id",
        ),
        (
            "[[http.rules]]\nid = \"a\"\n[http.rules.fault]\nkind = \"explode\"\n",
            "unknown variant",
        ),
        (
            "[[http.rules]]\nid = \"a\"\n[http.rules.match]\nwasm = \"nope\"\n[http.rules.fault]\nkind = \"reset\"\n",
            "nope",
        ),
    ];
    for (rules, needle) in cases {
        let e = err(&format!("{head}{rules}"), false);
        assert!(e.contains(needle), "expected {needle:?} in {e:?}");
    }
    assert!(err("upstream_allowlist = []\n", false).contains("at least one of"));
    assert!(err(&format!("{head}typo_field = 1\n"), false).contains("unknown field"));
}

#[test]
fn rejects_bad_postgres_rules_and_sensitive_wasm_headers() {
    let head = "upstream_allowlist = [\"127.0.0.1:5432\"]\n[postgres]\nlisten = \"127.0.0.1:6543\"\nupstream = \"127.0.0.1:5432\"\n";
    let bad_state = format!(
        "{head}[[postgres.rules]]\nid = \"a\"\n[postgres.rules.fault]\nkind = \"error\"\nsqlstate = \"4001\"\n"
    );
    assert!(err(&bad_state, false).contains("sqlstate"));
    let startup_regex = format!(
        "{head}[[postgres.rules]]\nid = \"a\"\n[postgres.rules.match]\nquery_regex = \"x\"\n[postgres.rules.fault]\nkind = \"reject_startup\"\nsqlstate = \"53300\"\n"
    );
    assert!(err(&startup_regex, false).contains("reject_startup cannot use query_regex"));
    let secret_header = format!("{head}[wasm]\nheaders = [\"Authorization\"]\n");
    assert!(err(&secret_header, false).contains("sensitive header"));
}

/// Captures log output of everything run on this (current-thread) runtime.
#[derive(Clone, Default)]
struct Captured(Arc<Mutex<Vec<u8>>>);
impl Write for Captured {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[tokio::test]
async fn logs_never_contain_credentials_bodies_or_query_strings() {
    let cap = Captured::default();
    let writer = cap.clone();
    let subscriber = tracing_subscriber::fmt()
        .json()
        .with_max_level(tracing::Level::DEBUG)
        .with_writer(move || writer.clone())
        .finish();
    let _guard = tracing::subscriber::set_default(subscriber);

    let marker = format!("{}-{}", "LEAKCHECK", 4242);
    let (up, _) = upstream().await;
    let rules = "[[http.rules]]\nid = \"echo-rule\"\n[http.rules.match]\npath_prefix = \"/echo\"\n[http.rules.fault]\nkind = \"latency\"\ndelay = \"1ms\"\n";
    let p = proxy(http_config(up, "", rules)).await;
    let req = Request::post(format!("http://{}/echo?token={marker}", p.local_addr))
        .header("authorization", format!("Bearer {marker}"))
        .header("cookie", format!("session={marker}"))
        .header("x-api-key", marker.clone())
        .body(Full::new(bytes::Bytes::from(format!("password={marker}"))))
        .unwrap();
    let (_, _, body) = send(&client(), req).await;
    assert!(
        String::from_utf8_lossy(&body).contains(&marker),
        "request really carried the marker"
    );
    p.shutdown().await;

    let logs = String::from_utf8(cap.0.lock().unwrap().clone()).unwrap();
    assert!(
        logs.contains("echo-rule"),
        "fault decision should be logged: {logs}"
    );
    assert!(
        logs.contains("/echo?<redacted>"),
        "path logged with query redacted: {logs}"
    );
    assert!(
        !logs.contains(&marker),
        "secret marker leaked into logs:\n{logs}"
    );
}
