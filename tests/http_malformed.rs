//! Malformed and hostile requests through the proxy, and misbehaving upstreams.
//!
//! The upstream here is a raw TCP listener that records every byte it receives,
//! so the tests see exactly what the proxy put on the wire (request smuggling
//! needs the proxy and the upstream to disagree about message framing).
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod common;

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use common::*;
use http::StatusCode;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

type Captured = Arc<Mutex<Vec<Vec<u8>>>>;

/// How the raw upstream answers.
#[derive(Clone, Copy)]
enum Reply {
    /// `200 OK` with a 2-byte body once the request has been quiet for 200 ms.
    Ok,
    /// Bytes that are not HTTP.
    Garbage,
    /// A head promising 1000 body bytes, then 10 bytes, then close.
    Truncated,
    /// Close immediately after accepting.
    Close,
}

/// Raw upstream; returns its address and every connection's received bytes.
async fn raw_upstream(reply: Reply) -> (SocketAddr, Captured) {
    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = l.local_addr().unwrap();
    let cap: Captured = Arc::default();
    let c2 = Arc::clone(&cap);
    tokio::spawn(async move {
        loop {
            let Ok((mut s, _)) = l.accept().await else {
                return;
            };
            let c3 = Arc::clone(&c2);
            tokio::spawn(async move {
                let idx = {
                    let mut c = c3.lock().unwrap();
                    c.push(Vec::new());
                    c.len() - 1
                };
                if matches!(reply, Reply::Close) {
                    return;
                }
                let mut buf = [0u8; 8192];
                let mut answered = false;
                loop {
                    match tokio::time::timeout(Duration::from_millis(200), s.read(&mut buf)).await {
                        Ok(Ok(0)) | Ok(Err(_)) => break,
                        Ok(Ok(n)) => c3.lock().unwrap()[idx].extend_from_slice(&buf[..n]),
                        Err(_) if answered || c3.lock().unwrap()[idx].is_empty() => break,
                        Err(_) => {
                            answered = true;
                            let bytes: &[u8] = match reply {
                                Reply::Ok => b"HTTP/1.1 200 OK\r\ncontent-length: 2\r\n\r\nok",
                                Reply::Garbage => b"SSH-2.0-not-http\r\n\r\n",
                                Reply::Truncated => {
                                    b"HTTP/1.1 200 OK\r\ncontent-length: 1000\r\n\r\n0123456789"
                                }
                                Reply::Close => unreachable!(),
                            };
                            let _ = s.write_all(bytes).await;
                            if !matches!(reply, Reply::Ok) {
                                break;
                            }
                        }
                    }
                }
            });
        }
    });
    (addr, cap)
}

/// Writes `bytes` (ignoring a write error: the proxy may close early) and
/// reads until the proxy closes or 3 s pass.
async fn raw(addr: SocketAddr, bytes: &[u8]) -> (Vec<u8>, Duration) {
    let t = Instant::now();
    let mut s = TcpStream::connect(addr).await.unwrap();
    let _ = s.write_all(bytes).await;
    let mut out = Vec::new();
    let res = tokio::time::timeout(Duration::from_secs(3), s.read_to_end(&mut out)).await;
    assert!(res.is_ok(), "proxy neither answered nor closed within 3 s");
    (out, t.elapsed())
}

fn text(b: &[u8]) -> String {
    String::from_utf8_lossy(b).into_owned()
}

fn status_of(resp: &[u8]) -> u16 {
    text(resp)
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(0)
}

async fn healthy(addr: SocketAddr) {
    let (resp, _) = raw(
        addr,
        b"GET /after HTTP/1.1\r\nhost: x\r\nconnection: close\r\n\r\n",
    )
    .await;
    assert_eq!(
        status_of(&resp),
        200,
        "proxy unhealthy afterwards: {}",
        text(&resp)
    );
}

/// `Content-Length` together with `Transfer-Encoding: chunked` (CL.TE / TE.CL):
/// the proxy frames the body as chunked (RFC 9112 §6.3), sends the upstream
/// only `Transfer-Encoding`, never both, and closes the client connection, so
/// bytes after the chunked body cannot become a second, smuggled request.
#[tokio::test]
async fn content_length_with_chunked_cannot_smuggle_a_request() {
    let (up, cap) = raw_upstream(Reply::Ok).await;
    let p = proxy(http_config(up, "", "")).await;
    for head in [
        "Content-Length: 4\r\nTransfer-Encoding: chunked",
        "Transfer-Encoding: chunked\r\nContent-Length: 40",
    ] {
        cap.lock().unwrap().clear();
        let req = format!(
            "POST /a HTTP/1.1\r\nHost: x\r\n{head}\r\n\r\n0\r\n\r\nGET /smuggled HTTP/1.1\r\nHost: x\r\n\r\n"
        );
        let (resp, _) = raw(p.local_addr, req.as_bytes()).await;
        let resp = text(&resp);
        assert_eq!(
            resp.matches("HTTP/1.1 ").count(),
            1,
            "one response only: {resp}"
        );
        assert!(
            resp.to_ascii_lowercase().contains("connection: close"),
            "{resp}"
        );
        tokio::time::sleep(Duration::from_millis(400)).await;
        let seen: Vec<String> = cap.lock().unwrap().iter().map(|b| text(b)).collect();
        let all = seen.concat().to_ascii_lowercase();
        assert!(
            !all.contains("/smuggled"),
            "smuggled request reached upstream: {seen:?}"
        );
        assert!(all.contains("transfer-encoding: chunked"), "{seen:?}");
        assert!(
            !all.contains("content-length"),
            "both framings forwarded: {seen:?}"
        );
    }
    healthy(p.local_addr).await;
    p.shutdown().await;
}

/// Ambiguous or unsupported framing is refused with 400 before anything is
/// sent upstream.
#[tokio::test]
async fn ambiguous_framing_is_refused_before_the_upstream() {
    let (up, cap) = raw_upstream(Reply::Ok).await;
    let p = proxy(http_config(up, "", "")).await;
    let cases: [(&str, &[u8]); 5] = [
        (
            "obfuscated TE + CL",
            b"POST /a HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: xchunked\r\nContent-Length: 5\r\n\r\nhello",
        ),
        (
            "chunked not last",
            b"POST /a HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked, identity\r\n\r\n5\r\nhello\r\n0\r\n\r\n",
        ),
        (
            "conflicting CLs",
            b"POST /a HTTP/1.1\r\nHost: x\r\nContent-Length: 5\r\nContent-Length: 6\r\n\r\nhello!",
        ),
        (
            "space before colon",
            b"POST /a HTTP/1.1\r\nHost: x\r\nTransfer-Encoding : chunked\r\n\r\n0\r\n\r\n",
        ),
        (
            "TE on HTTP/1.0",
            b"POST /a HTTP/1.0\r\nHost: x\r\nTransfer-Encoding: chunked\r\n\r\n0\r\n\r\n",
        ),
    ];
    for (name, bytes) in cases {
        let (resp, _) = raw(p.local_addr, bytes).await;
        assert_eq!(status_of(&resp), 400, "{name}: {}", text(&resp));
    }
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(cap.lock().unwrap().is_empty(), "upstream was contacted");
    healthy(p.local_addr).await;
    p.shutdown().await;
}

/// Too many or too large headers: 431 (or a close), the upstream is untouched,
/// and the proxy keeps serving.
#[tokio::test]
async fn oversized_headers_are_refused() {
    let (up, cap) = raw_upstream(Reply::Ok).await;
    let p = proxy(http_config(up, "", "")).await;

    let mut many = b"GET /a HTTP/1.1\r\nHost: x\r\n".to_vec();
    for i in 0..200 {
        many.extend_from_slice(format!("x-h{i}: v\r\n").as_bytes());
    }
    many.extend_from_slice(b"\r\n");
    let (resp, _) = raw(p.local_addr, &many).await;
    assert_eq!(status_of(&resp), 431, "{}", text(&resp));

    let mut huge = b"GET /a HTTP/1.1\r\nHost: x\r\nx-big: ".to_vec();
    huge.extend(std::iter::repeat_n(b'a', 1 << 20));
    huge.extend_from_slice(b"\r\n\r\n");
    let (resp, _) = raw(p.local_addr, &huge).await;
    assert!(
        resp.is_empty() || status_of(&resp) == 431,
        "{}",
        text(&resp[..resp.len().min(200)])
    );

    assert!(cap.lock().unwrap().is_empty(), "upstream was contacted");
    healthy(p.local_addr).await;
    p.shutdown().await;
}

/// A broken chunked body fails the request quickly; the upstream never gets the
/// body bytes and the proxy keeps serving.
#[tokio::test]
async fn invalid_chunked_body_fails_fast() {
    let (up, cap) = raw_upstream(Reply::Ok).await;
    let p = proxy(http_config(up, "", "")).await;
    let (resp, took) = raw(
        p.local_addr,
        b"POST /a HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\n\r\nZZ\r\nhello\r\n0\r\n\r\n",
    )
    .await;
    let status = status_of(&resp);
    assert!(
        resp.is_empty() || status == 400 || status == 502,
        "{}",
        text(&resp)
    );
    assert!(took < Duration::from_secs(2), "took {took:?}");
    tokio::time::sleep(Duration::from_millis(300)).await;
    let all = cap.lock().unwrap().concat();
    assert!(!text(&all).contains("hello"), "body bytes forwarded");
    healthy(p.local_addr).await;
    p.shutdown().await;
}

/// Upstream misbehaviour becomes a clean client-visible failure, never a hang.
#[tokio::test]
async fn upstream_failures_are_reported_not_hung() {
    // Not HTTP at all.
    let (up, _) = raw_upstream(Reply::Garbage).await;
    let p = proxy(http_config(up, "", "")).await;
    let (status, _, _) = get(&client(), p.local_addr, "/").await;
    assert_eq!(status, StatusCode::BAD_GATEWAY);
    p.shutdown().await;

    // Accepts, then closes without a byte.
    let (up, _) = raw_upstream(Reply::Close).await;
    let p = proxy(http_config(up, "", "")).await;
    let (status, _, _) = get(&client(), p.local_addr, "/").await;
    assert_eq!(status, StatusCode::BAD_GATEWAY);
    p.shutdown().await;

    // Promises 1000 bytes, sends 10, closes: the client must see an error,
    // not a complete (short) body and not a hang.
    let (up, _) = raw_upstream(Reply::Truncated).await;
    let p = proxy(http_config(up, "", "")).await;
    let (resp, took) = raw(
        p.local_addr,
        b"GET / HTTP/1.1\r\nhost: x\r\nconnection: close\r\n\r\n",
    )
    .await;
    let resp = text(&resp);
    assert!(resp.contains("content-length: 1000"), "{resp}");
    let body = resp.split("\r\n\r\n").nth(1).unwrap_or("");
    assert!(body.len() < 1000, "the body cannot be complete");
    assert!(took < Duration::from_secs(2), "took {took:?}");
    // The same through a real HTTP client: the body read fails.
    let resp = client()
        .request(
            http::Request::get(format!("http://{}/", p.local_addr))
                .body(http_body_util::Full::default())
                .unwrap(),
        )
        .await
        .unwrap();
    let body = http_body_util::BodyExt::collect(resp.into_body()).await;
    assert!(
        body.is_err(),
        "truncated upstream body must surface as an error"
    );
    p.shutdown().await;
}
