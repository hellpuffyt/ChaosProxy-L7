//! PostgreSQL wire-protocol proxy with query-level faults.
//!
//! This component has nothing to do with the HTTP code: it frames the
//! PostgreSQL v3 protocol itself ([`codec`]) and relays frames between client
//! and server ([`conn`]). TLS is not supported: an `SSLRequest` or
//! `GSSENCRequest` is answered with `N` (not supported), which libpq and most
//! drivers accept with `sslmode=prefer`/`disable`.

pub mod codec;
pub mod conn;

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use tokio::net::TcpListener;
use tokio::sync::{Semaphore, watch};
use tokio::task::{JoinHandle, JoinSet};

use crate::config::{Mode, PgConfig};
use crate::rules::PgRules;

/// Shared state of the PostgreSQL listener.
pub struct PgState {
    pub(crate) cfg: PgConfig,
    pub(crate) mode: Mode,
    pub(crate) rules: PgRules,
    pub(crate) conn_seq: AtomicU64,
    pub(crate) limiter: Arc<Semaphore>,
}

/// A running PostgreSQL listener.
pub struct RunningPg {
    /// Bound address.
    pub local_addr: SocketAddr,
    shutdown: watch::Sender<bool>,
    task: JoinHandle<()>,
}

impl RunningPg {
    /// Stops accepting and waits up to `grace` for open sessions to end;
    /// sessions still open after that are dropped.
    pub async fn shutdown(self, grace: Duration) {
        let _ = self.shutdown.send(true);
        let _ = tokio::time::timeout(grace + Duration::from_millis(100), self.task).await;
    }
}

/// Binds and starts the listener.
pub async fn spawn(
    cfg: PgConfig,
    mode: Mode,
    seed: u64,
    limiter: Arc<Semaphore>,
    grace: Duration,
) -> anyhow::Result<RunningPg> {
    let rules = PgRules::compile(&cfg.rules, seed).map_err(anyhow::Error::msg)?;
    let listener = TcpListener::bind(cfg.listen).await?;
    let local_addr = listener.local_addr()?;
    let state = Arc::new(PgState {
        cfg,
        mode,
        rules,
        conn_seq: AtomicU64::new(0),
        limiter,
    });
    let (tx, mut rx) = watch::channel(false);
    let task = tokio::spawn(async move {
        let mut sessions = JoinSet::new();
        loop {
            tokio::select! {
                _ = rx.changed() => break,
                accepted = listener.accept() => match accepted {
                    Ok((stream, peer)) => {
                        let id = state.conn_seq.fetch_add(1, Ordering::Relaxed);
                        sessions.spawn(conn::run(Arc::clone(&state), stream, peer, id));
                    }
                    Err(e) => {
                        tracing::warn!(target: "chaosproxy::pg", error = %e, "accept failed");
                        tokio::time::sleep(Duration::from_millis(50)).await;
                    }
                },
                Some(_) = sessions.join_next(), if !sessions.is_empty() => {}
            }
        }
        drop(listener);
        let drained = tokio::time::timeout(grace, async {
            while sessions.join_next().await.is_some() {}
        })
        .await;
        if drained.is_err() {
            tracing::warn!(target: "chaosproxy::pg", open = sessions.len(), "shutdown grace elapsed; dropping sessions");
            sessions.abort_all();
        }
    });
    tracing::info!(target: "chaosproxy", %local_addr, "postgres listener ready");
    Ok(RunningPg {
        local_addr,
        shutdown: tx,
        task,
    })
}
