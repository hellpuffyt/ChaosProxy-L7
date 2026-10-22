//! Shared helpers for integration tests: a local upstream and a proxy launcher.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic, dead_code)]

use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use bytes::Bytes;
use chaosproxy::config::Config;
use chaosproxy::http::{HttpDeps, RunningHttp};
use http::{Request, Response, StatusCode};
use http_body_util::combinators::BoxBody;
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper::service::service_fn;
use hyper_util::client::legacy::Client;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto;
use tokio::net::TcpListener;
use tokio::sync::Semaphore;

/// Observable upstream state.
#[derive(Default)]
pub struct UpstreamStats {
    pub hits: AtomicUsize,
    pub slow_cancelled: AtomicBool,
    pub slow_completed: AtomicBool,
}

struct CancelFlag(Arc<UpstreamStats>, bool);
impl Drop for CancelFlag {
    fn drop(&mut self) {
        if !self.1 {
            self.0.slow_cancelled.store(true, Ordering::SeqCst);
        }
    }
}

type Body = BoxBody<Bytes, Infallible>;

fn text(s: impl Into<Bytes>) -> Response<Body> {
    Response::new(Full::new(s.into()).boxed())
}

async fn upstream_handler(
    stats: Arc<UpstreamStats>,
    req: Request<Incoming>,
) -> Result<Response<Body>, Infallible> {
    stats.hits.fetch_add(1, Ordering::SeqCst);
    let path = req.uri().path().to_owned();
    let query = req.uri().query().unwrap_or("").to_owned();
    match path.as_str() {
        "/headers" => {
            let mut out = format!("version={:?}\n", req.version());
            for (k, v) in req.headers() {
                out.push_str(&format!("{}: {}\n", k, v.to_str().unwrap_or("?")));
            }
            Ok(text(out))
        }
        "/echo" => {
            let body = req
                .into_body()
                .collect()
                .await
                .map(|c| c.to_bytes())
                .unwrap_or_default();
            Ok(text(body))
        }
        "/big" => {
            let n: usize = query
                .strip_prefix("n=")
                .and_then(|v| v.parse().ok())
                .unwrap_or(1024);
            Ok(text(vec![b'x'; n]))
        }
        "/slow" => {
            let mut flag = CancelFlag(Arc::clone(&stats), false);
            tokio::time::sleep(Duration::from_millis(800)).await;
            flag.1 = true;
            stats.slow_completed.store(true, Ordering::SeqCst);
            Ok(text("slow done"))
        }
        "/status" => {
            let mut r = text("teapot");
            *r.status_mut() = StatusCode::IM_A_TEAPOT;
            r.headers_mut()
                .insert("connection", "x-hop".parse().unwrap());
            r.headers_mut()
                .insert("x-hop", "should-be-stripped".parse().unwrap());
            Ok(r)
        }
        _ => Ok(text(format!("ok {path}"))),
    }
}

/// Starts the upstream on 127.0.0.1:0 (HTTP/1.1 and h2c).
pub async fn upstream() -> (SocketAddr, Arc<UpstreamStats>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let stats = Arc::new(UpstreamStats::default());
    let s2 = Arc::clone(&stats);
    tokio::spawn(async move {
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                return;
            };
            let s3 = Arc::clone(&s2);
            tokio::spawn(async move {
                let svc = service_fn(move |req| upstream_handler(Arc::clone(&s3), req));
                let _ = auto::Builder::new(TokioExecutor::new())
                    .serve_connection(TokioIo::new(stream), svc)
                    .await;
            });
        }
    });
    (addr, stats)
}

/// Builds a full config with an `[http]` section pointing at `upstream`.
pub fn http_config(upstream: SocketAddr, extra_http: &str, rules: &str) -> Config {
    let text = format!(
        r#"
seed = 42
upstream_allowlist = ["{upstream}"]
[http]
listen = "127.0.0.1:0"
upstream = "http://{upstream}"
request_timeout = "2s"
shutdown_grace = "2s"
{extra_http}
{rules}
"#
    );
    Config::parse(&text, false).unwrap_or_else(|e| panic!("config: {e}\n{text}"))
}

/// Starts the proxy from a config.
pub async fn proxy(cfg: Config) -> RunningHttp {
    let deps = HttpDeps {
        mode: cfg.mode,
        seed: cfg.seed,
        limiter: Arc::new(Semaphore::new(cfg.max_concurrency)),
        external: None,
    };
    chaosproxy::http::spawn(cfg.http.unwrap(), deps)
        .await
        .unwrap()
}

pub type TestClient = Client<HttpConnector, Full<Bytes>>;

pub fn client() -> TestClient {
    Client::builder(TokioExecutor::new()).build(HttpConnector::new())
}

pub fn h2_client() -> TestClient {
    Client::builder(TokioExecutor::new())
        .http2_only(true)
        .build(HttpConnector::new())
}

/// GET helper returning (status, headers, body).
pub async fn get(
    c: &TestClient,
    addr: SocketAddr,
    path: &str,
) -> (StatusCode, http::HeaderMap, Bytes) {
    send(
        c,
        Request::get(format!("http://{addr}{path}"))
            .body(Full::default())
            .unwrap(),
    )
    .await
}

pub async fn send(
    c: &TestClient,
    req: Request<Full<Bytes>>,
) -> (StatusCode, http::HeaderMap, Bytes) {
    let resp = c.request(req).await.expect("request");
    let status = resp.status();
    let headers = resp.headers().clone();
    let body = resp.into_body().collect().await.expect("body").to_bytes();
    (status, headers, body)
}
