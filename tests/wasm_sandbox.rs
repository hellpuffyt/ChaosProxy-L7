//! WebAssembly matcher sandbox: load-time validation refuses bad modules;
//! runtime failures (trap, fuel, time, memory, bad return) fail closed.
#![cfg(feature = "wasm")]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod common;

use std::sync::Arc;
use std::time::{Duration, Instant};

use chaosproxy::config::WasmConfig;
use chaosproxy::http::HttpDeps;
use chaosproxy::rules::ExternalMatcher;
use chaosproxy::wasm::WasmMatchers;
use common::*;
use http::StatusCode;

/// Valid ABI v1 module: says yes when the input contains "beta".
const GOOD: &str = include_str!("../examples/wasm/path_contains_beta.wat");

fn variant(decide_body: &str) -> String {
    format!(
        r#"(module
  (memory (export "memory") 1)
  (func (export "chaos_abi_version") (result i32) (i32.const 1))
  (func (export "chaos_alloc") (param i32) (result i32) (i32.const 1024))
  (func (export "chaos_decide") (param i32 i32) (result i32) {decide_body}))"#
    )
}

fn matchers(cfg: WasmConfig) -> WasmMatchers {
    WasmMatchers::load(&cfg).unwrap()
}

fn load_err(src: &[u8]) -> String {
    let mut m = matchers(WasmConfig::default());
    m.add("m", src).expect_err("must be refused").to_string()
}

#[test]
fn valid_module_decides_from_its_input() {
    let mut m = matchers(WasmConfig::default());
    m.add("beta", GOOD.as_bytes()).unwrap();
    assert_eq!(m.run("beta", b"v1\nGET\n/beta/x\n"), Ok(true));
    assert_eq!(m.run("beta", b"v1\nGET\n/alpha\n"), Ok(false));
    assert_eq!(m.run("beta", b""), Ok(false));
}

#[test]
fn load_time_validation_refuses_bad_modules() {
    assert!(load_err(b"definitely not wasm").contains("invalid module"));
    let wasi = r#"(module (import "wasi_snapshot_preview1" "fd_write" (func (param i32 i32 i32 i32) (result i32)))
        (memory (export "memory") 1))"#;
    assert!(load_err(wasi.as_bytes()).contains("may not import anything"));
    let v2 = variant("(i32.const 0)")
        .replace("(result i32) (i32.const 1))", "(result i32) (i32.const 2))");
    assert!(load_err(v2.as_bytes()).contains("ABI version 2"));
    let no_decide = variant("(i32.const 0)").replace("\"chaos_decide\"", "\"other\"");
    assert!(load_err(no_decide.as_bytes()).contains("chaos_decide"));
    let wrong_type = variant("(i32.const 0)").replace(
        "(param i32) (result i32) (i32.const 1024)",
        "(result i32) (i32.const 1024)",
    );
    assert!(load_err(wrong_type.as_bytes()).contains("chaos_alloc"));
    let no_memory =
        variant("(i32.const 0)").replace("(memory (export \"memory\") 1)", "(memory 1)");
    assert!(load_err(no_memory.as_bytes()).contains("memory"));
    // 64 pages = 4 MiB initial memory, above the 1 MiB default limit.
    let huge = variant("(i32.const 0)").replace(
        "(memory (export \"memory\") 1)",
        "(memory (export \"memory\") 64)",
    );
    assert!(load_err(huge.as_bytes()).contains("instantiation failed"));
}

#[test]
fn runtime_failures_are_errors_not_panics() {
    let mut m = matchers(WasmConfig::default());
    m.add("trap", variant("(unreachable)").as_bytes()).unwrap();
    m.add("bad_ret", variant("(i32.const 7)").as_bytes())
        .unwrap();
    m.add(
        "spin",
        variant("(loop $l (br $l)) (i32.const 1)").as_bytes(),
    )
    .unwrap();
    m.add(
        "grow",
        variant("(drop (memory.grow (i32.const 100))) (i32.const 1)").as_bytes(),
    )
    .unwrap();
    let oob = variant("(i32.const 1)").replace(
        "(i32.const 1024))\n  (func (export \"chaos_decide\")",
        "(i32.const 1073741824))\n  (func (export \"chaos_decide\")",
    );
    m.add("oob", oob.as_bytes()).unwrap();
    let input = b"v1\nGET\n/\n";
    assert!(m.run("trap", input).unwrap_err().contains("chaos_decide"));
    assert!(m.run("bad_ret", input).unwrap_err().contains("returned 7"));
    let t = Instant::now();
    let fuel = m.run("spin", input).unwrap_err();
    assert!(fuel.to_lowercase().contains("fuel"), "{fuel}");
    assert!(t.elapsed() < Duration::from_secs(2));
    assert!(
        m.run("grow", input).is_err(),
        "growing past max_memory_bytes must trap"
    );
    assert!(m.run("oob", input).unwrap_err().contains("out-of-bounds"));
    assert!(
        m.run("missing", input)
            .unwrap_err()
            .contains("unknown module")
    );
}

#[test]
fn wall_clock_limit_stops_a_module_with_plenty_of_fuel() {
    let cfg = WasmConfig {
        fuel: u64::MAX / 2,
        timeout: Duration::from_millis(50),
        ..WasmConfig::default()
    };
    let mut m = matchers(cfg);
    m.add(
        "spin",
        variant("(loop $l (br $l)) (i32.const 1)").as_bytes(),
    )
    .unwrap();
    let t = Instant::now();
    let e = m.run("spin", b"x").unwrap_err();
    assert!(
        t.elapsed() < Duration::from_secs(2),
        "epoch deadline should fire, took {:?}",
        t.elapsed()
    );
    assert!(e.to_lowercase().contains("interrupt"), "{e}");
}

#[test]
fn every_call_gets_a_fresh_instance() {
    // A module that counts calls in a global: if instances were reused it would return 0 then 1.
    let src = r#"(module
  (memory (export "memory") 1)
  (global $n (mut i32) (i32.const 0))
  (func (export "chaos_abi_version") (result i32) (i32.const 1))
  (func (export "chaos_alloc") (param i32) (result i32) (i32.const 1024))
  (func (export "chaos_decide") (param i32 i32) (result i32)
    (global.set $n (i32.add (global.get $n) (i32.const 1)))
    (i32.sub (global.get $n) (i32.const 1))))"#;
    let mut m = matchers(WasmConfig::default());
    m.add("count", src.as_bytes()).unwrap();
    for _ in 0..3 {
        assert_eq!(
            m.run("count", b"x"),
            Ok(false),
            "state leaked between calls"
        );
    }
}

#[test]
fn input_contains_only_allowlisted_headers() {
    let cfg = WasmConfig {
        headers: vec!["x-tenant".into()],
        ..WasmConfig::default()
    };
    let m = matchers(cfg);
    let mut h = http::HeaderMap::new();
    h.insert("x-tenant", "acme".parse().unwrap());
    h.insert("authorization", "Basic abc".parse().unwrap());
    let input = m.input(&chaosproxy::rules::HttpRequestView {
        method: &http::Method::POST,
        path: "/beta",
        headers: &h,
    });
    assert_eq!(
        String::from_utf8(input).unwrap(),
        "v1\nPOST\n/beta\nx-tenant: acme\n"
    );
}

async fn proxy_with(module_src: &str) -> (chaosproxy::http::RunningHttp, Arc<UpstreamStats>) {
    let (up, stats) = upstream().await;
    let rules = "[[http.rules]]\nid = \"wasm-rule\"\n[http.rules.match]\nwasm = \"m\"\n[http.rules.fault]\nkind = \"error\"\nstatus = 418\n";
    // The config only needs to name module "m"; the test loads its source directly.
    let cfg = http_config(
        up,
        "",
        &format!("{rules}\n[wasm.modules]\nm = \"loaded-by-test.wat\"\n"),
    );
    let mut m = WasmMatchers::load(&WasmConfig::default()).unwrap();
    m.add("m", module_src.as_bytes()).unwrap();
    let deps = HttpDeps {
        mode: cfg.mode,
        seed: cfg.seed,
        limiter: Arc::new(tokio::sync::Semaphore::new(16)),
        external: Some(Arc::new(m) as Arc<dyn ExternalMatcher>),
    };
    (
        chaosproxy::http::spawn(cfg.http.unwrap(), deps)
            .await
            .unwrap(),
        stats,
    )
}

#[tokio::test]
async fn proxy_applies_rule_when_module_agrees() {
    let (p, _) = proxy_with(GOOD).await;
    let c = client();
    let (status, headers, _) = get(&c, p.local_addr, "/beta/feature").await;
    assert_eq!(status, StatusCode::IM_A_TEAPOT);
    assert_eq!(headers.get("x-chaosproxy-fault").unwrap(), "wasm-rule");
    assert_eq!(get(&c, p.local_addr, "/stable").await.0, StatusCode::OK);
    p.shutdown().await;
}

#[tokio::test]
async fn proxy_fails_closed_when_module_traps() {
    let (p, stats) = proxy_with(&variant("(unreachable)")).await;
    let (status, headers, _) = get(&client(), p.local_addr, "/beta").await;
    assert_eq!(
        status,
        StatusCode::OK,
        "trap => no fault, request forwarded"
    );
    assert!(headers.get("x-chaosproxy-fault").is_none());
    assert_eq!(stats.hits.load(std::sync::atomic::Ordering::SeqCst), 1);
    p.shutdown().await;
}
