//! PostgreSQL proxy behaviour that needs no real server: encryption probes,
//! malformed startup, startup rejection, unreachable upstream.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::sync::Arc;
use std::time::Duration;

use chaosproxy::config::Config;
use chaosproxy::pg::codec;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::Semaphore;

async fn start(upstream: &str, rules: &str) -> chaosproxy::pg::RunningPg {
    let text = format!(
        "upstream_allowlist = [\"{upstream}\"]\n[postgres]\nlisten = \"127.0.0.1:0\"\nupstream = \"{upstream}\"\nconnect_timeout = \"1s\"\n{rules}"
    );
    let cfg = Config::parse(&text, false).unwrap();
    chaosproxy::pg::spawn(
        cfg.postgres.unwrap(),
        cfg.mode,
        cfg.seed,
        Arc::new(Semaphore::new(16)),
        Duration::from_millis(500),
    )
    .await
    .unwrap()
}

async fn dead_port() -> String {
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    l.local_addr().unwrap().to_string()
}

fn startup(user: &str) -> Vec<u8> {
    let mut body = codec::PROTOCOL_V3.to_be_bytes().to_vec();
    for (k, v) in [("user", user), ("database", "postgres")] {
        body.extend_from_slice(k.as_bytes());
        body.push(0);
        body.extend_from_slice(v.as_bytes());
        body.push(0);
    }
    body.push(0);
    let mut msg = ((body.len() + 4) as i32).to_be_bytes().to_vec();
    msg.extend_from_slice(&body);
    msg
}

async fn read_all(s: &mut TcpStream) -> Vec<u8> {
    let mut buf = Vec::new();
    tokio::time::timeout(Duration::from_secs(3), s.read_to_end(&mut buf))
        .await
        .expect("proxy should close the connection")
        .unwrap();
    buf
}

#[tokio::test]
async fn ssl_and_gss_probes_are_declined_with_n() {
    let p = start(&dead_port().await, "").await;
    let mut s = TcpStream::connect(p.local_addr).await.unwrap();
    for code in [codec::SSL_REQUEST, codec::GSSENC_REQUEST] {
        let mut msg = 8i32.to_be_bytes().to_vec();
        msg.extend_from_slice(&code.to_be_bytes());
        s.write_all(&msg).await.unwrap();
        let mut b = [0u8; 1];
        s.read_exact(&mut b).await.unwrap();
        assert_eq!(&b, b"N");
    }
    p.shutdown(Duration::from_millis(100)).await;
}

#[tokio::test]
async fn reject_startup_sends_fatal_error_with_sqlstate() {
    let rules = "[[postgres.rules]]\nid = \"full\"\n[postgres.rules.match]\nuser = \"app\"\n[postgres.rules.fault]\nkind = \"reject_startup\"\nsqlstate = \"53300\"\nmessage = \"sorry, too many clients\"\n";
    let p = start(&dead_port().await, rules).await;
    let mut s = TcpStream::connect(p.local_addr).await.unwrap();
    s.write_all(&startup("app")).await.unwrap();
    let reply = read_all(&mut s).await;
    let (frame, _) = codec::decode_frame(&reply, 1 << 20).unwrap().unwrap();
    assert_eq!(frame.tag, b'E');
    assert_eq!(codec::error_code(&frame.payload), Some("53300"));
    assert!(frame.payload.windows(5).any(|w| w == b"FATAL"));
    p.shutdown(Duration::from_millis(100)).await;
}

#[tokio::test]
async fn unreachable_upstream_reports_08006() {
    let p = start(&dead_port().await, "").await;
    let mut s = TcpStream::connect(p.local_addr).await.unwrap();
    s.write_all(&startup("anyone")).await.unwrap();
    let reply = read_all(&mut s).await;
    let (frame, _) = codec::decode_frame(&reply, 1 << 20).unwrap().unwrap();
    assert_eq!(codec::error_code(&frame.payload), Some("08006"));
    p.shutdown(Duration::from_millis(100)).await;
}

#[tokio::test]
async fn malformed_startup_closes_the_connection() {
    let p = start(&dead_port().await, "").await;
    for garbage in [
        &b"\x00\x00\x00\x03"[..],
        &b"\x00\x00\x00\x08\x00\x00\x04\xd3"[..],
        &b"\xff\xff\xff\xff"[..],
    ] {
        let mut s = TcpStream::connect(p.local_addr).await.unwrap();
        s.write_all(garbage).await.unwrap();
        let reply = read_all(&mut s).await;
        assert!(reply.is_empty(), "no response to garbage, just a close");
    }
    p.shutdown(Duration::from_millis(100)).await;
}
