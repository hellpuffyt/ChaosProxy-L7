//! Normal forwarding (no faults): headers, bodies, timeouts, cancellation, h2c, shutdown.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod common;

use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use bytes::Bytes;
use common::*;
use http::{Request, StatusCode};
use http_body_util::Full;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[tokio::test]
async fn forwards_get_and_rewrites_headers() {
    let (up, stats) = upstream().await;
    let p = proxy(http_config(up, "", "")).await;
    let c = client();
    let req = Request::get(format!("http://{}/headers?x=1", p.local_addr))
        .header("connection", "x-private-hop")
        .header("x-private-hop", "secret")
        .header("x-keep", "kept")
        .body(Full::default())
        .unwrap();
    let (status, headers, body) = send(&c, req).await;
    let body = String::from_utf8(body.to_vec()).unwrap();
    assert_eq!(status, StatusCode::OK);
    assert_eq!(stats.hits.load(Ordering::SeqCst), 1);
    assert!(body.contains("x-keep: kept"), "{body}");
    assert!(
        !body.contains("x-private-hop"),
        "hop-by-hop header leaked: {body}"
    );
    assert!(body.contains("x-forwarded-for: 127.0.0.1"), "{body}");
    assert!(body.contains("x-forwarded-proto: http"), "{body}");
    assert!(
        body.contains(&format!("x-forwarded-host: {}", p.local_addr)),
        "{body}"
    );
    assert!(body.contains(&format!("host: {up}")), "{body}");
    assert!(body.contains("x-request-id: cp-"), "{body}");
    assert!(headers.get("x-chaosproxy-request-id").is_some());
    assert!(headers.get("x-chaosproxy-fault").is_none());
    p.shutdown().await;
}

#[tokio::test]
async fn strips_hop_by_hop_headers_from_responses() {
    let (up, _) = upstream().await;
    let p = proxy(http_config(up, "", "")).await;
    let (status, headers, body) = get(&client(), p.local_addr, "/status").await;
    assert_eq!(status, StatusCode::IM_A_TEAPOT);
    assert_eq!(&body[..], b"teapot");
    assert!(headers.get("x-hop").is_none());
    p.shutdown().await;
}

#[tokio::test]
async fn streams_large_request_and_response_bodies() {
    let (up, _) = upstream().await;
    let p = proxy(http_config(up, "", "")).await;
    let c = client();
    let payload = Bytes::from(vec![7u8; 4 * 1024 * 1024]);
    let req = Request::post(format!("http://{}/echo", p.local_addr))
        .body(Full::new(payload.clone()))
        .unwrap();
    let (status, _, body) = send(&c, req).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, payload);
    let (_, _, big) = get(&c, p.local_addr, "/big?n=3000000").await;
    assert_eq!(big.len(), 3_000_000);
    p.shutdown().await;
}

#[tokio::test]
async fn upstream_timeout_returns_504() {
    let (up, _) = upstream().await;
    let p = proxy(http_config(up, "", "").tap(|c| {
        if let Some(h) = c.http.as_mut() {
            h.request_timeout = Duration::from_millis(200);
        }
    }))
    .await;
    let started = Instant::now();
    let (status, _, _) = get(&client(), p.local_addr, "/slow").await;
    assert_eq!(status, StatusCode::GATEWAY_TIMEOUT);
    assert!(started.elapsed() < Duration::from_millis(700));
    p.shutdown().await;
}

#[tokio::test]
async fn unreachable_upstream_returns_502() {
    // Reserve a port, then close it so nothing listens there.
    let dead = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .unwrap()
        .local_addr()
        .unwrap();
    let p = proxy(http_config(dead, "", "")).await;
    let (status, _, _) = get(&client(), p.local_addr, "/").await;
    assert_eq!(status, StatusCode::BAD_GATEWAY);
    p.shutdown().await;
}

#[tokio::test]
async fn client_disconnect_cancels_upstream_request() {
    let (up, stats) = upstream().await;
    let p = proxy(http_config(up, "", "")).await;
    let mut s = tokio::net::TcpStream::connect(p.local_addr).await.unwrap();
    s.write_all(b"GET /slow HTTP/1.1\r\nhost: x\r\n\r\n")
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;
    drop(s);
    tokio::time::sleep(Duration::from_millis(1200)).await;
    assert!(
        stats.slow_cancelled.load(Ordering::SeqCst),
        "upstream handler should have been dropped"
    );
    assert!(!stats.slow_completed.load(Ordering::SeqCst));
    p.shutdown().await;
}

#[tokio::test]
async fn upgrade_requests_are_refused() {
    let (up, stats) = upstream().await;
    let p = proxy(http_config(up, "", "")).await;
    let req = Request::get(format!("http://{}/ws", p.local_addr))
        .header("connection", "upgrade")
        .header("upgrade", "websocket")
        .body(Full::default())
        .unwrap();
    let (status, _, _) = send(&client(), req).await;
    assert_eq!(status, StatusCode::NOT_IMPLEMENTED);
    assert_eq!(stats.hits.load(Ordering::SeqCst), 0);
    p.shutdown().await;
}

#[tokio::test]
async fn h2c_client_to_proxy() {
    let (up, _) = upstream().await;
    let p = proxy(http_config(up, "", "")).await;
    let (status, _, body) = get(&h2_client(), p.local_addr, "/headers").await;
    assert_eq!(status, StatusCode::OK);
    // Upstream still sees HTTP/1.1 by default.
    assert!(String::from_utf8_lossy(&body).contains("version=HTTP/1.1"));
    p.shutdown().await;
}

#[tokio::test]
async fn h2c_proxy_to_upstream() {
    let (up, _) = upstream().await;
    let p = proxy(http_config(up, "upstream_http2 = true", "")).await;
    let (status, _, body) = get(&client(), p.local_addr, "/headers").await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        String::from_utf8_lossy(&body).contains("version=HTTP/2.0"),
        "{body:?}"
    );
    p.shutdown().await;
}

#[tokio::test]
async fn slow_client_headers_time_out() {
    let (up, _) = upstream().await;
    let p = proxy(http_config(up, "header_read_timeout = \"300ms\"", "")).await;
    let mut s = tokio::net::TcpStream::connect(p.local_addr).await.unwrap();
    s.write_all(b"GET / HTTP/1.1\r\n").await.unwrap(); // never finish the headers
    let mut buf = Vec::new();
    let res = tokio::time::timeout(Duration::from_secs(3), s.read_to_end(&mut buf)).await;
    assert!(
        res.is_ok(),
        "proxy should close a connection that never finishes its headers"
    );
    p.shutdown().await;
}

#[tokio::test]
async fn graceful_shutdown_lets_in_flight_requests_finish() {
    let (up, _) = upstream().await;
    let p = proxy(http_config(up, "", "")).await;
    let addr = p.local_addr;
    let inflight = tokio::spawn(async move { get(&client(), addr, "/slow").await });
    tokio::time::sleep(Duration::from_millis(150)).await;
    p.shutdown().await;
    let (status, _, body) = inflight.await.unwrap();
    assert_eq!(status, StatusCode::OK);
    assert_eq!(&body[..], b"slow done");
    // And the listener is gone.
    assert!(tokio::net::TcpStream::connect(addr).await.is_err());
}

trait Tap: Sized {
    fn tap(mut self, f: impl FnOnce(&mut Self)) -> Self {
        f(&mut self);
        self
    }
}
impl<T> Tap for T {}
