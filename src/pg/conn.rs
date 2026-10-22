//! One proxied PostgreSQL session.
//!
//! Two halves run concurrently:
//!
//! * **client → server** reads frontend frames, tracks prepared statements
//!   and portals, makes fault decisions for `Query` and `Execute`, and
//!   forwards (or withholds) frames;
//! * **server → client** relays backend frames and splices injected messages
//!   in at the correct place.
//!
//! Ordering matters because clients pipeline. Every `Query` and `Sync` the
//! proxy forwards is a *sync point* that the server answers with exactly one
//! `ReadyForQuery`. Injected responses are tied to sync-point numbers, so an
//! injected error is never delivered ahead of responses the client is still
//! waiting for. The startup handshake's `ReadyForQuery` counts as sync point 1.

use std::collections::{HashMap, VecDeque};
use std::net::SocketAddr;
use std::sync::Arc;

use bytes::{Buf, Bytes, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::sync::mpsc;

use super::PgState;
use super::codec::{self, Startup};
use crate::config::{Mode, PgFault};
use crate::redact;
use crate::rules::PgPhase;

/// Messages from the client half to the server half.
#[derive(Debug)]
enum Splice {
    /// Write `bytes` + `ReadyForQuery` once `after` sync points are answered
    /// (used for a simple `Query` that was not forwarded).
    Standalone { after: u64, bytes: Bytes },
    /// Write `bytes` just before the `ReadyForQuery` answering sync point `idx`.
    BeforeRfq { idx: u64, bytes: Bytes },
    /// Close the session after `rows` `DataRow`s of the result ending at sync point `idx`.
    DropAfter { idx: u64, rows: u32 },
}

/// How a session ended (for logging).
#[derive(Debug)]
enum End {
    ClientClosed,
    ServerClosed,
    Dropped,
    Error(String),
}

/// Runs one session to completion. Never panics; all failures are logged.
pub async fn run(state: Arc<PgState>, mut client: TcpStream, peer: SocketAddr, id: u64) {
    let _ = client.set_nodelay(true);
    let Ok(_permit) = Arc::clone(&state.limiter).try_acquire_owned() else {
        tracing::warn!(target: "chaosproxy::pg", conn = id, "max_concurrency reached; refusing session");
        let _ = client
            .write_all(&codec::error_response(
                "FATAL",
                "53300",
                "chaosproxy: max_concurrency reached",
            ))
            .await;
        return;
    };
    match session(&state, &mut client, id).await {
        End::Error(e) => {
            tracing::warn!(target: "chaosproxy::pg", conn = id, %peer, error = %e, "session ended with error")
        }
        end => {
            tracing::info!(target: "chaosproxy::pg", conn = id, %peer, end = ?end, "session closed")
        }
    }
}

async fn session(state: &PgState, client: &mut TcpStream, id: u64) -> End {
    let max = state.cfg.max_message_bytes;
    // Startup phase: answer TLS/GSS probes with 'N' (at most twice), then expect a StartupMessage.
    let mut probes = 0;
    let startup = loop {
        match codec::read_startup(client, max).await {
            Ok(None) => return End::ClientClosed,
            Err(e) => return End::Error(format!("startup: {e}")),
            Ok(Some(Startup::Ssl | Startup::GssEnc)) if probes < 2 => {
                probes += 1;
                if let Err(e) = client.write_all(b"N").await {
                    return End::Error(e.to_string());
                }
            }
            Ok(Some(Startup::Ssl | Startup::GssEnc)) => {
                return End::Error("too many encryption probes".into());
            }
            Ok(Some(Startup::Cancel { raw })) => {
                // Relay the cancel key to the server on a fresh connection.
                if let Ok(mut up) = connect(state).await {
                    let _ = up.write_all(&raw).await;
                }
                return End::ClientClosed;
            }
            Ok(Some(s @ Startup::Startup { .. })) => break s,
        }
    };
    let user = startup.param("user").map(str::to_owned);
    let database = startup.param("database").map(str::to_owned);
    if let Some((rule, fault)) = state.rules.decide(
        id << 32,
        PgPhase::Startup,
        user.as_deref(),
        database.as_deref(),
        None,
    ) {
        let inject = state.mode == Mode::Inject;
        tracing::info!(target: "chaosproxy::fault", conn = id, rule = %rule, fault = "reject_startup", injected = inject, "pg fault decision");
        if inject && let PgFault::RejectStartup { sqlstate, message } = fault {
            let _ = client
                .write_all(&codec::error_response("FATAL", &sqlstate, &message))
                .await;
            return End::Dropped;
        }
    }
    let mut upstream = match connect(state).await {
        Ok(u) => u,
        Err(e) => {
            tracing::warn!(target: "chaosproxy::pg", conn = id, error = %e, "upstream unavailable");
            let _ = client
                .write_all(&codec::error_response(
                    "FATAL",
                    "08006",
                    "chaosproxy: upstream unavailable",
                ))
                .await;
            return End::Error(e);
        }
    };
    let Startup::Startup { raw, .. } = &startup else {
        return End::Error("unreachable startup state".into());
    };
    if let Err(e) = upstream.write_all(raw).await {
        return End::Error(e.to_string());
    }

    let (client_r, client_w) = client.split();
    let (up_r, up_w) = upstream.into_split();
    let (tx, rx) = mpsc::channel(64);
    let ctx = Ctx {
        state,
        id,
        user,
        database,
    };
    // When either half finishes, the other is dropped and both sockets close.
    tokio::select! {
        r = client_to_server(&ctx, client_r, up_w, tx) => r,
        r = server_to_client(max, up_r, client_w, rx) => r,
    }
}

async fn connect(state: &PgState) -> Result<TcpStream, String> {
    let fut = TcpStream::connect(state.cfg.upstream.as_str());
    match tokio::time::timeout(state.cfg.connect_timeout, fut).await {
        Err(_) => Err("connect timeout".into()),
        Ok(Err(e)) => Err(e.to_string()),
        Ok(Ok(s)) => {
            let _ = s.set_nodelay(true);
            Ok(s)
        }
    }
}

struct Ctx<'a> {
    state: &'a PgState,
    id: u64,
    user: Option<String>,
    database: Option<String>,
}

impl Ctx<'_> {
    fn decide(&self, n: u64, sql: &str) -> Option<(String, PgFault)> {
        let seq = (self.id << 32) | (n & 0xFFFF_FFFF);
        let (rule, fault) = self.state.rules.decide(
            seq,
            PgPhase::Query,
            self.user.as_deref(),
            self.database.as_deref(),
            Some(sql),
        )?;
        let inject = self.state.mode == Mode::Inject;
        let kind = match fault {
            PgFault::Error { .. } => "error",
            PgFault::Delay { .. } => "delay",
            PgFault::DropMidResult { .. } => "drop_mid_result",
            PgFault::RejectStartup { .. } => "reject_startup",
        };
        if self.state.cfg.log_query_text {
            tracing::info!(target: "chaosproxy::fault", conn = self.id, rule = %rule, fault = kind, injected = inject,
                sql = %redact::sql_for_log(sql), "pg fault decision");
        } else {
            tracing::info!(target: "chaosproxy::fault", conn = self.id, rule = %rule, fault = kind, injected = inject,
                "pg fault decision");
        }
        inject.then_some((rule, fault))
    }
}

async fn client_to_server(
    ctx: &Ctx<'_>,
    mut client: tokio::net::tcp::ReadHalf<'_>,
    mut up: OwnedWriteHalf,
    tx: mpsc::Sender<Splice>,
) -> End {
    let max = ctx.state.cfg.max_message_bytes;
    let mut syncs: u64 = 1; // the startup ReadyForQuery
    let mut queries: u64 = 0;
    let mut statements: HashMap<String, String> = HashMap::new();
    let mut portals: HashMap<String, String> = HashMap::new();
    // Set while discarding the rest of an extended-protocol batch after an injected error.
    let mut pending_error: Option<Bytes> = None;
    loop {
        let frame = match codec::read_frame(&mut client, max).await {
            Ok(Some(f)) => f,
            Ok(None) => return End::ClientClosed,
            Err(e) => return End::Error(format!("client: {e}")),
        };
        if let Some(err) = &pending_error {
            if frame.tag != b'S' {
                continue;
            }
            syncs += 1;
            if tx
                .send(Splice::BeforeRfq {
                    idx: syncs,
                    bytes: err.clone(),
                })
                .await
                .is_err()
            {
                return End::ServerClosed;
            }
            pending_error = None;
            if let Err(e) = write(&mut up, &frame.encode()).await {
                return e;
            }
            continue;
        }
        match frame.tag {
            b'Q' => {
                let sql = match codec::query_text(&frame.payload) {
                    Ok(s) => s.to_owned(),
                    Err(e) => return End::Error(format!("client: {e}")),
                };
                queries += 1;
                match ctx.decide(queries, &sql) {
                    Some((_, PgFault::Error { sqlstate, message })) => {
                        let bytes = codec::error_response("ERROR", &sqlstate, &message);
                        if tx
                            .send(Splice::Standalone {
                                after: syncs,
                                bytes,
                            })
                            .await
                            .is_err()
                        {
                            return End::ServerClosed;
                        }
                        continue;
                    }
                    Some((_, PgFault::Delay { delay })) => tokio::time::sleep(delay).await,
                    Some((_, PgFault::DropMidResult { after_rows }))
                        if tx
                            .send(Splice::DropAfter {
                                idx: syncs + 1,
                                rows: after_rows,
                            })
                            .await
                            .is_err() =>
                    {
                        return End::ServerClosed;
                    }
                    _ => {}
                }
                syncs += 1;
            }
            b'P' => {
                if let Ok((name, sql)) = codec::parse_message(&frame.payload) {
                    statements.insert(name.to_owned(), sql.to_owned());
                }
            }
            b'B' => {
                if let Ok((portal, stmt)) = codec::bind_message(&frame.payload) {
                    portals.insert(portal.to_owned(), stmt.to_owned());
                }
            }
            b'E' => {
                let sql = codec::execute_portal(&frame.payload)
                    .ok()
                    .and_then(|p| portals.get(p))
                    .and_then(|s| statements.get(s))
                    .cloned();
                if let Some(sql) = sql {
                    queries += 1;
                    match ctx.decide(queries, &sql) {
                        Some((_, PgFault::Error { sqlstate, message })) => {
                            pending_error =
                                Some(codec::error_response("ERROR", &sqlstate, &message));
                            continue;
                        }
                        Some((_, PgFault::Delay { delay })) => tokio::time::sleep(delay).await,
                        Some((_, PgFault::DropMidResult { after_rows }))
                            if tx
                                .send(Splice::DropAfter {
                                    idx: syncs + 1,
                                    rows: after_rows,
                                })
                                .await
                                .is_err() =>
                        {
                            return End::ServerClosed;
                        }
                        _ => {}
                    }
                }
            }
            b'S' => syncs += 1,
            _ => {}
        }
        if let Err(e) = write(&mut up, &frame.encode()).await {
            return e;
        }
        if frame.tag == b'X' {
            return End::ClientClosed;
        }
    }
}

async fn write<W: AsyncWrite + Unpin>(w: &mut W, bytes: &[u8]) -> Result<(), End> {
    w.write_all(bytes)
        .await
        .map_err(|e| End::Error(e.to_string()))
}

/// Server-side relay state.
struct Relay {
    rfq_seen: u64,
    status: u8,
    standalone: VecDeque<(u64, Bytes)>,
    before_rfq: HashMap<u64, Vec<Bytes>>,
    /// (sync point, row limit, rows relayed so far)
    drop_at: Option<(u64, u32, u32)>,
}

async fn server_to_client(
    max: u32,
    mut up: OwnedReadHalf,
    mut client: tokio::net::tcp::WriteHalf<'_>,
    mut rx: mpsc::Receiver<Splice>,
) -> End {
    let mut r = Relay {
        rfq_seen: 0,
        status: b'I',
        standalone: VecDeque::new(),
        before_rfq: HashMap::new(),
        drop_at: None,
    };
    let mut buf = BytesMut::with_capacity(16 * 1024);
    let mut rx_open = true;
    loop {
        // Splices are always applied before the next server frame, so a
        // DropAfter sent before its query was forwarded is never missed.
        while let Ok(s) = rx.try_recv() {
            if let Err(e) = r.splice(s, &mut client).await {
                return e;
            }
        }
        match codec::decode_frame(&buf, max) {
            Err(e) => return End::Error(format!("server: {e}")),
            Ok(Some((frame, used))) => {
                buf.advance(used);
                match r.frame(frame, &mut client).await {
                    Ok(()) => continue,
                    Err(e) => return e,
                }
            }
            Ok(None) => {}
        }
        // Both branches are cancel-safe: `read_buf` keeps partial data in `buf`.
        tokio::select! {
            biased;
            s = rx.recv(), if rx_open => match s {
                None => rx_open = false,
                Some(s) => {
                    if let Err(e) = r.splice(s, &mut client).await {
                        return e;
                    }
                }
            },
            n = up.read_buf(&mut buf) => match n {
                Ok(0) => {
                    return if buf.is_empty() { End::ServerClosed } else { End::Error("server: truncated message".into()) };
                }
                Ok(_) => {}
                Err(e) => return End::Error(format!("server: {e}")),
            },
        }
    }
}

impl Relay {
    async fn splice<W: AsyncWrite + Unpin>(
        &mut self,
        s: Splice,
        client: &mut W,
    ) -> Result<(), End> {
        match s {
            Splice::Standalone { after, bytes } => {
                if self.rfq_seen >= after {
                    write_standalone(client, &bytes, self.status).await?;
                } else {
                    self.standalone.push_back((after, bytes));
                }
            }
            Splice::BeforeRfq { idx, bytes } => self.before_rfq.entry(idx).or_default().push(bytes),
            Splice::DropAfter { idx, rows } => self.drop_at = Some((idx, rows, 0)),
        }
        Ok(())
    }

    async fn frame<W: AsyncWrite + Unpin>(
        &mut self,
        frame: codec::Frame,
        client: &mut W,
    ) -> Result<(), End> {
        if let Some((idx, limit, relayed)) = self.drop_at.as_mut()
            && *idx == self.rfq_seen + 1
            && frame.tag == b'D'
        {
            if *relayed >= *limit {
                return Err(End::Dropped);
            }
            *relayed += 1;
        }
        if frame.tag == b'Z'
            && let Some(extra) = self.before_rfq.remove(&(self.rfq_seen + 1))
        {
            for b in extra {
                write(client, &b).await?;
            }
        }
        write(client, &frame.encode()).await?;
        if frame.tag == b'Z' {
            self.rfq_seen += 1;
            self.status = frame.payload.first().copied().unwrap_or(b'I');
            if self.drop_at.is_some_and(|(idx, _, _)| idx <= self.rfq_seen) {
                self.drop_at = None;
            }
            while self
                .standalone
                .front()
                .is_some_and(|(after, _)| *after <= self.rfq_seen)
            {
                if let Some((_, bytes)) = self.standalone.pop_front() {
                    write_standalone(client, &bytes, self.status).await?;
                }
            }
        }
        Ok(())
    }
}

async fn write_standalone<W: AsyncWrite + Unpin>(
    w: &mut W,
    err: &[u8],
    status: u8,
) -> Result<(), End> {
    // An injected error in an explicit transaction leaves the *server's*
    // transaction untouched; report the server's last known status.
    let mut buf = Vec::with_capacity(err.len() + 6);
    buf.extend_from_slice(err);
    buf.extend_from_slice(&codec::ready_for_query(status));
    write(w, &buf).await
}
