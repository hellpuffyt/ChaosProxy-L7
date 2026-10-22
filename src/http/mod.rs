//! HTTP/1.1 and h2c (HTTP/2 prior knowledge) reverse proxy with fault injection.
//!
//! The listener serves both protocols on one port (hyper-util's `auto`
//! builder). The upstream is reached over HTTP/1.1, or h2c when
//! `upstream_http2 = true`. TLS is not supported on either side.

pub mod body;
pub mod headers;
pub mod service;

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::time::Duration;

use hyper::service::service_fn;
use hyper_util::client::legacy::Client;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::rt::{TokioExecutor, TokioIo, TokioTimer};
use hyper_util::server::conn::auto;
use hyper_util::server::graceful::GracefulShutdown;
use socket2::SockRef;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Mutex, Semaphore, oneshot, watch};
use tokio::task::JoinHandle;

use crate::config::{HttpConfig, Mode, http_upstream_authority};
use crate::rules::{ExternalMatcher, HttpRules};
use service::{ConnAction, ConnCtx, HttpState};

/// A running HTTP listener.
pub struct RunningHttp {
    /// Actual bound address (useful with port 0).
    pub local_addr: SocketAddr,
    shutdown: watch::Sender<bool>,
    task: JoinHandle<()>,
}

impl RunningHttp {
    /// Stops accepting, lets in-flight requests finish for up to
    /// `shutdown_grace`, then returns.
    pub async fn shutdown(self) {
        let _ = self.shutdown.send(true);
        let _ = self.task.await;
    }
}

/// Everything the HTTP listener needs besides its own config section.
pub struct HttpDeps {
    /// Inject or observe.
    pub mode: Mode,
    /// Decision seed.
    pub seed: u64,
    /// Shared concurrency limiter.
    pub limiter: Arc<Semaphore>,
    /// Optional external matcher (wasm).
    pub external: Option<Arc<dyn ExternalMatcher>>,
}

/// Binds and starts serving. Validation (loopback, allowlist) must already
/// have run; this function does not re-check it.
pub async fn spawn(cfg: HttpConfig, deps: HttpDeps) -> anyhow::Result<RunningHttp> {
    let rules = HttpRules::compile(&cfg.rules, deps.seed).map_err(anyhow::Error::msg)?;
    let upstream_authority = http_upstream_authority(&cfg.upstream)?;
    let mut connector = HttpConnector::new();
    connector.set_nodelay(true);
    connector.set_connect_timeout(Some(Duration::from_secs(5)));
    let client = Client::builder(TokioExecutor::new())
        .pool_idle_timeout(Duration::from_secs(30))
        .pool_max_idle_per_host(32)
        .http2_only(cfg.upstream_http2)
        .build(connector);
    let listener = TcpListener::bind(cfg.listen).await?;
    let local_addr = listener.local_addr()?;
    let state = Arc::new(HttpState {
        cfg,
        mode: deps.mode,
        rules,
        external: deps.external,
        client,
        upstream_authority,
        seq: AtomicU64::new(0),
        limiter: deps.limiter,
    });
    let (tx, rx) = watch::channel(false);
    let task = tokio::spawn(accept_loop(listener, state, rx));
    tracing::info!(target: "chaosproxy", %local_addr, "http listener ready");
    Ok(RunningHttp {
        local_addr,
        shutdown: tx,
        task,
    })
}

async fn accept_loop(
    listener: TcpListener,
    state: Arc<HttpState>,
    mut stop: watch::Receiver<bool>,
) {
    let graceful = GracefulShutdown::new();
    loop {
        tokio::select! {
            _ = stop.changed() => break,
            accepted = listener.accept() => {
                match accepted {
                    Ok((stream, peer)) => serve_connection(stream, peer, Arc::clone(&state), &graceful),
                    Err(e) => {
                        tracing::warn!(target: "chaosproxy", error = %e, "accept failed");
                        tokio::time::sleep(Duration::from_millis(50)).await;
                    }
                }
            }
        }
    }
    drop(listener);
    let grace = state.cfg.shutdown_grace;
    tokio::select! {
        _ = graceful.shutdown() => tracing::info!(target: "chaosproxy", "http listener drained"),
        _ = tokio::time::sleep(grace) => tracing::warn!(target: "chaosproxy", "shutdown_grace elapsed; dropping remaining connections"),
    }
}

fn serve_connection(
    stream: TcpStream,
    peer: SocketAddr,
    state: Arc<HttpState>,
    graceful: &GracefulShutdown,
) {
    let _ = stream.set_nodelay(true);
    // Keep a second handle to the socket so a `reset` fault can set
    // SO_LINGER=0 after hyper has taken ownership of the stream.
    let (stream, dup) = match stream.into_std().and_then(|s| {
        let dup = s.try_clone()?;
        Ok((TcpStream::from_std(s)?, dup))
    }) {
        Ok(pair) => pair,
        Err(e) => {
            tracing::warn!(target: "chaosproxy", error = %e, "could not prepare connection");
            return;
        }
    };
    let (tx, rx) = oneshot::channel();
    let ctx = ConnCtx {
        peer,
        action: Arc::new(Mutex::new(Some(tx))),
    };
    let header_timeout = state.cfg.header_read_timeout;
    let svc_state = Arc::clone(&state);
    let svc = service_fn(move |req| service::handle(Arc::clone(&svc_state), ctx.clone(), req));
    let mut builder = auto::Builder::new(TokioExecutor::new());
    builder
        .http1()
        .timer(TokioTimer::new())
        .header_read_timeout(header_timeout);
    let conn = builder
        .serve_connection(TokioIo::new(stream), svc)
        .into_owned();
    let conn = graceful.watch(conn);
    tokio::spawn(async move {
        tokio::select! {
            res = conn => {
                if let Err(e) = res {
                    tracing::debug!(target: "chaosproxy", error = %e, "connection ended with error");
                }
            }
            action = rx => {
                if let Ok(ConnAction::Reset) = action
                    && let Err(e) = SockRef::from(&dup).set_linger(Some(Duration::ZERO)) {
                        tracing::warn!(target: "chaosproxy", error = %e, "could not set SO_LINGER for reset");
                    }
                // Dropping the hyper connection (inside the select) and then
                // `dup` closes the last descriptor: RST if linger is 0, else FIN.
            }
        }
        drop(dup);
    });
}
