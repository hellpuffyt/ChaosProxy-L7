//! Each HTTP fault kind is injected at the HTTP layer and only there.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod common;

use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use common::*;
use http::StatusCode;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

fn rule(id: &str, extra_match: &str, fault: &str) -> String {
    format!(
        "[[http.rules]]\nid = \"{id}\"\n[http.rules.match]\n{extra_match}\n[http.rules.fault]\n{fault}\n"
    )
}

#[tokio::test]
async fn error_fault_answers_without_contacting_upstream() {
    let (up, stats) = upstream().await;
    let r = rule(
        "boom",
        "path_prefix = \"/api\"",
        "kind = \"error\"\nstatus = 503\nbody = \"injected\"",
    );
    let p = proxy(http_config(up, "", &r)).await;
    let c = client();
    let (status, headers, body) = get(&c, p.local_addr, "/api/users").await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(&body[..], b"injected");
    assert_eq!(headers.get("x-chaosproxy-fault").unwrap(), "boom");
    assert_eq!(
        stats.hits.load(Ordering::SeqCst),
        0,
        "upstream must not be contacted"
    );
    // Non-matching paths are forwarded normally.
    let (status, headers, _) = get(&c, p.local_addr, "/health").await;
    assert_eq!(status, StatusCode::OK);
    assert!(headers.get("x-chaosproxy-fault").is_none());
    assert_eq!(stats.hits.load(Ordering::SeqCst), 1);
    p.shutdown().await;
}

#[tokio::test]
async fn latency_fault_delays_then_forwards() {
    let (up, stats) = upstream().await;
    let r = rule("slow", "", "kind = \"latency\"\ndelay = \"300ms\"");
    let p = proxy(http_config(up, "", &r)).await;
    let t = Instant::now();
    let (status, headers, _) = get(&client(), p.local_addr, "/x").await;
    let took = t.elapsed();
    assert_eq!(status, StatusCode::OK);
    assert_eq!(headers.get("x-chaosproxy-fault").unwrap(), "slow");
    assert!(took >= Duration::from_millis(300), "took {took:?}");
    assert_eq!(stats.hits.load(Ordering::SeqCst), 1);
    p.shutdown().await;
}

#[tokio::test]
async fn first_byte_delay_holds_response_headers() {
    let (up, stats) = upstream().await;
    let r = rule("fb", "", "kind = \"first_byte_delay\"\ndelay = \"300ms\"");
    let p = proxy(http_config(up, "", &r)).await;
    let t = Instant::now();
    let (status, _, _) = get(&client(), p.local_addr, "/x").await;
    assert_eq!(status, StatusCode::OK);
    assert!(t.elapsed() >= Duration::from_millis(300));
    assert_eq!(
        stats.hits.load(Ordering::SeqCst),
        1,
        "forwarded before the delay"
    );
    p.shutdown().await;
}

#[tokio::test]
async fn throttle_limits_bandwidth() {
    let (up, _) = upstream().await;
    let r = rule("bw", "", "kind = \"throttle\"\nbytes_per_second = 100000");
    let p = proxy(http_config(up, "", &r)).await;
    let t = Instant::now();
    let (status, _, body) = get(&client(), p.local_addr, "/big?n=50000").await;
    let took = t.elapsed();
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body.len(), 50_000, "throttling must not lose data");
    // 50 kB at 100 kB/s = 0.5 s minimum.
    assert!(took >= Duration::from_millis(450), "took {took:?}");
    p.shutdown().await;
}

#[tokio::test]
async fn reset_fault_sends_tcp_rst() {
    let (up, stats) = upstream().await;
    let r = rule("rst", "", "kind = \"reset\"");
    let p = proxy(http_config(up, "", &r)).await;
    let mut s = tokio::net::TcpStream::connect(p.local_addr).await.unwrap();
    s.write_all(b"GET / HTTP/1.1\r\nhost: x\r\n\r\n")
        .await
        .unwrap();
    let mut buf = [0u8; 64];
    let res = tokio::time::timeout(Duration::from_secs(3), s.read(&mut buf))
        .await
        .expect("no hang");
    match res {
        Err(e) => assert_eq!(e.kind(), std::io::ErrorKind::ConnectionReset, "got {e:?}"),
        Ok(n) => panic!("expected a reset, read {n} bytes"),
    }
    assert_eq!(stats.hits.load(Ordering::SeqCst), 0);
    p.shutdown().await;
}

#[tokio::test]
async fn timeout_fault_answers_504_after_delay() {
    let (up, stats) = upstream().await;
    let r = rule("to", "", "kind = \"timeout\"\nafter = \"250ms\"");
    let p = proxy(http_config(up, "", &r)).await;
    let t = Instant::now();
    let (status, headers, _) = get(&client(), p.local_addr, "/x").await;
    assert_eq!(status, StatusCode::GATEWAY_TIMEOUT);
    assert_eq!(headers.get("x-chaosproxy-fault").unwrap(), "to");
    assert!(t.elapsed() >= Duration::from_millis(250));
    assert_eq!(stats.hits.load(Ordering::SeqCst), 0);
    p.shutdown().await;
}

#[tokio::test]
async fn hang_fault_never_answers_then_closes() {
    let (up, _) = upstream().await;
    let r = rule("hang", "", "kind = \"hang\"\nmax = \"600ms\"");
    let p = proxy(http_config(up, "", &r)).await;
    let mut s = tokio::net::TcpStream::connect(p.local_addr).await.unwrap();
    s.write_all(b"GET / HTTP/1.1\r\nhost: x\r\n\r\n")
        .await
        .unwrap();
    let mut buf = Vec::new();
    // Nothing for the first 400 ms...
    let early = tokio::time::timeout(Duration::from_millis(400), s.read_buf(&mut buf)).await;
    assert!(early.is_err(), "hang must not answer early");
    // ...then a clean close with zero bytes of response.
    let n = tokio::time::timeout(Duration::from_secs(3), s.read_to_end(&mut buf))
        .await
        .expect("closed")
        .unwrap();
    assert_eq!(n, 0);
    assert!(buf.is_empty());
    p.shutdown().await;
}

/// Records which of 40 sequential requests were faulted.
async fn pattern(seed: u64) -> Vec<bool> {
    let (up, _) = upstream().await;
    let r = rule("half", "", "kind = \"error\"\nstatus = 500");
    let mut cfg = http_config(
        up,
        "",
        &r.replace("id = \"half\"", "id = \"half\"\npercentage = 50.0"),
    );
    cfg.seed = seed;
    let p = proxy(cfg).await;
    let c = client();
    let mut out = Vec::new();
    for _ in 0..40 {
        let (status, _, _) = get(&c, p.local_addr, "/").await;
        out.push(status == StatusCode::INTERNAL_SERVER_ERROR);
    }
    p.shutdown().await;
    out
}

#[tokio::test]
async fn percentage_decisions_are_reproducible_by_seed() {
    let a = pattern(7).await;
    let b = pattern(7).await;
    let other = pattern(8).await;
    assert_eq!(a, b, "same seed, same request order => same faults");
    assert_ne!(a, other, "a different seed should change the pattern");
    let hits = a.iter().filter(|x| **x).count();
    assert!(
        (8..=32).contains(&hits),
        "50% of 40 should land well inside 8..=32, got {hits}"
    );
}

#[tokio::test]
async fn observe_mode_logs_but_does_not_inject() {
    let (up, stats) = upstream().await;
    let r = rule("boom", "", "kind = \"error\"\nstatus = 503");
    let mut cfg = http_config(up, "", &r);
    cfg.mode = chaosproxy::config::Mode::Observe;
    let p = proxy(cfg).await;
    let (status, headers, _) = get(&client(), p.local_addr, "/").await;
    assert_eq!(status, StatusCode::OK);
    assert!(headers.get("x-chaosproxy-fault").is_none());
    assert_eq!(stats.hits.load(Ordering::SeqCst), 1);
    p.shutdown().await;
}

#[tokio::test]
async fn per_rule_cap_limits_concurrent_faults() {
    let (up, _) = upstream().await;
    let r = "[[http.rules]]\nid = \"capped\"\nmax_concurrent = 1\n[http.rules.fault]\nkind = \"latency\"\ndelay = \"500ms\"\n";
    let p = proxy(http_config(up, "", r)).await;
    let addr = p.local_addr;
    let first = tokio::spawn(async move { get(&client(), addr, "/").await });
    tokio::time::sleep(Duration::from_millis(100)).await;
    // The rule is busy with the first request, so this one is forwarded untouched.
    let t = Instant::now();
    let (status, headers, _) = get(&client(), addr, "/").await;
    assert_eq!(status, StatusCode::OK);
    assert!(headers.get("x-chaosproxy-fault").is_none());
    assert!(t.elapsed() < Duration::from_millis(400));
    let (_, h1, _) = first.await.unwrap();
    assert_eq!(h1.get("x-chaosproxy-fault").unwrap(), "capped");
    p.shutdown().await;
}

#[tokio::test]
async fn global_max_concurrency_sheds_with_503() {
    let (up, _) = upstream().await;
    let mut cfg = http_config(up, "", "");
    cfg.max_concurrency = 1;
    let p = proxy(cfg).await;
    let addr = p.local_addr;
    let first = tokio::spawn(async move { get(&client(), addr, "/slow").await });
    tokio::time::sleep(Duration::from_millis(150)).await;
    let (status, _, body) = get(&client(), addr, "/").await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert!(String::from_utf8_lossy(&body).contains("max_concurrency"));
    assert_eq!(first.await.unwrap().0, StatusCode::OK);
    p.shutdown().await;
}

#[tokio::test]
async fn header_matcher_scopes_faults_to_tagged_requests() {
    let (up, _) = upstream().await;
    let r = rule(
        "tagged",
        "[http.rules.match.headers]\nx-chaos = \"on\"",
        "kind = \"error\"\nstatus = 500",
    );
    let p = proxy(http_config(up, "", &r)).await;
    let c = client();
    assert_eq!(get(&c, p.local_addr, "/").await.0, StatusCode::OK);
    let req = http::Request::get(format!("http://{}/", p.local_addr))
        .header("x-chaos", "on")
        .body(http_body_util::Full::default())
        .unwrap();
    assert_eq!(send(&c, req).await.0, StatusCode::INTERNAL_SERVER_ERROR);
    p.shutdown().await;
}
