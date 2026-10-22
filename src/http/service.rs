//! Per-request handling: decide, then either forward or inject.

use std::any::Any;
use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use bytes::Bytes;
use http::{HeaderValue, Method, Request, Response, StatusCode, Uri, Version, header};
use http_body_util::BodyExt;
use http_body_util::combinators::BoxBody;
use hyper::body::Incoming;
use hyper_util::client::legacy::Client;
use hyper_util::client::legacy::connect::HttpConnector;
use tokio::sync::{Mutex, Semaphore, oneshot};

use super::body::{self, Guarded, ProxyBody};
use super::headers;
use crate::config::{HttpConfig, Mode};
use crate::redact;
use crate::rules::{ExternalMatcher, HttpRequestView, HttpRules, ResolvedHttpFault};

/// Header naming the rule that disturbed a response.
pub const FAULT_HEADER: &str = "x-chaosproxy-fault";
/// Header carrying the proxy's request id.
pub const REQUEST_ID_HEADER: &str = "x-chaosproxy-request-id";

/// Request body type sent upstream.
pub type UpstreamBody = BoxBody<Bytes, hyper::Error>;

/// What the request handler asks the connection task to do.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnAction {
    /// Abort with TCP RST.
    Reset,
    /// Close normally (FIN) without answering.
    Close,
}

/// Shared state of one HTTP listener.
pub struct HttpState {
    pub(crate) cfg: HttpConfig,
    pub(crate) mode: Mode,
    pub(crate) rules: HttpRules,
    pub(crate) external: Option<Arc<dyn ExternalMatcher>>,
    pub(crate) client: Client<HttpConnector, UpstreamBody>,
    pub(crate) upstream_authority: String,
    pub(crate) seq: AtomicU64,
    pub(crate) limiter: Arc<Semaphore>,
}

/// Per-connection context handed to every request on that connection.
#[derive(Clone)]
pub struct ConnCtx {
    pub(crate) peer: SocketAddr,
    pub(crate) action: Arc<Mutex<Option<oneshot::Sender<ConnAction>>>>,
}

/// Handles one request. Never returns `Err`: every failure becomes a response.
pub async fn handle(
    state: Arc<HttpState>,
    ctx: ConnCtx,
    req: Request<Incoming>,
) -> Result<Response<ProxyBody>, Infallible> {
    let started = Instant::now();
    let seq = state.seq.fetch_add(1, Ordering::Relaxed);
    let request_id = format!("cp-{seq:08x}");
    let method = req.method().clone();
    let path_log = redact::path_for_log(req.uri());

    let resp = route(&state, &ctx, req, seq, &request_id).await;
    let (mut resp, rule, fault, injected) = resp;
    if let Ok(v) = HeaderValue::from_str(&request_id) {
        resp.headers_mut().insert(REQUEST_ID_HEADER, v);
    }
    tracing::info!(
        target: "chaosproxy::access",
        request_id = %request_id,
        method = %method,
        path = %path_log,
        status = resp.status().as_u16(),
        rule = rule.as_deref().unwrap_or("-"),
        fault = fault.unwrap_or("-"),
        injected,
        elapsed_ms = started.elapsed().as_millis() as u64,
        "request"
    );
    Ok(resp)
}

type Routed = (
    Response<ProxyBody>,
    Option<String>,
    Option<&'static str>,
    bool,
);

async fn route(
    state: &Arc<HttpState>,
    ctx: &ConnCtx,
    req: Request<Incoming>,
    seq: u64,
    request_id: &str,
) -> Routed {
    let Ok(permit) = Arc::clone(&state.limiter).try_acquire_owned() else {
        tracing::warn!(target: "chaosproxy::access", request_id, "max_concurrency reached; shedding request");
        return (
            simple(
                StatusCode::SERVICE_UNAVAILABLE,
                "chaosproxy: max_concurrency reached\n",
            ),
            None,
            None,
            false,
        );
    };
    if req.method() == Method::CONNECT || req.headers().contains_key(header::UPGRADE) {
        return (
            simple(
                StatusCode::NOT_IMPLEMENTED,
                "chaosproxy: CONNECT and protocol upgrades are not supported\n",
            ),
            None,
            None,
            false,
        );
    }

    let decision = {
        let view = HttpRequestView {
            method: req.method(),
            path: req.uri().path(),
            headers: req.headers(),
        };
        let (d, _skipped) = state.rules.decide(seq, &view, state.external.as_deref());
        d
    };
    let Some(decision) = decision else {
        let resp = forward(state, ctx, req, request_id, None).await;
        return (guard_body(resp, vec![Box::new(permit)]), None, None, false);
    };

    let rule_id = decision.rule_id.clone();
    let kind = decision.fault.kind();
    let inject = state.mode == Mode::Inject;
    tracing::info!(target: "chaosproxy::fault", request_id, rule = %rule_id, fault = kind, injected = inject,
        "fault decision");
    if !inject {
        let resp = forward(state, ctx, req, request_id, None).await;
        return (
            guard_body(resp, vec![Box::new(permit)]),
            Some(rule_id),
            Some(kind),
            false,
        );
    }

    let guards: Vec<Box<dyn Any + Send + Sync>> = vec![Box::new(permit), Box::new(decision.guard)];
    let resp = match decision.fault {
        ResolvedHttpFault::Error(status, body) => {
            let status = StatusCode::from_u16(status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
            simple(status, body)
        }
        ResolvedHttpFault::Timeout(after) => {
            tokio::time::sleep(after).await;
            simple(
                StatusCode::GATEWAY_TIMEOUT,
                "chaosproxy: injected upstream timeout\n",
            )
        }
        ResolvedHttpFault::Reset => {
            return conn_action(ctx, ConnAction::Reset, Duration::ZERO).await;
        }
        ResolvedHttpFault::Hang(max) => return conn_action(ctx, ConnAction::Close, max).await,
        ResolvedHttpFault::Latency(d) => {
            tokio::time::sleep(d).await;
            forward(state, ctx, req, request_id, None).await
        }
        ResolvedHttpFault::FirstByteDelay(d) => forward(state, ctx, req, request_id, Some(d)).await,
        ResolvedHttpFault::Throttle(bps) => {
            let resp = forward(state, ctx, req, request_id, None).await;
            let (parts, b) = resp.into_parts();
            Response::from_parts(parts, body::throttle(b, bps))
        }
    };
    let mut resp = guard_body(resp, guards);
    if let Ok(v) = HeaderValue::from_str(&rule_id) {
        resp.headers_mut().insert(FAULT_HEADER, v);
    }
    (resp, Some(rule_id), Some(kind), true)
}

/// Asks the connection task to reset/close after `wait`, then never answers.
async fn conn_action(ctx: &ConnCtx, action: ConnAction, wait: Duration) -> Routed {
    tokio::time::sleep(wait).await;
    if let Some(tx) = ctx.action.lock().await.take() {
        let _ = tx.send(action);
    }
    // The connection task drops this future when it tears the connection down.
    std::future::pending::<Routed>().await
}

fn guard_body(
    resp: Response<ProxyBody>,
    guards: Vec<Box<dyn Any + Send + Sync>>,
) -> Response<ProxyBody> {
    let (parts, b) = resp.into_parts();
    Response::from_parts(parts, Guarded::new(b, guards).boxed())
}

fn simple(status: StatusCode, body: impl Into<Bytes>) -> Response<ProxyBody> {
    let mut resp = Response::new(body::full(body));
    *resp.status_mut() = status;
    resp.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("text/plain; charset=utf-8"),
    );
    resp
}

async fn forward(
    state: &Arc<HttpState>,
    ctx: &ConnCtx,
    req: Request<Incoming>,
    request_id: &str,
    first_byte_delay: Option<Duration>,
) -> Response<ProxyBody> {
    let (mut parts, incoming) = req.into_parts();
    let original_host = parts
        .headers
        .get(header::HOST)
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned)
        .or_else(|| parts.uri.authority().map(|a| a.to_string()));
    let pq = parts
        .uri
        .path_and_query()
        .map(|p| p.as_str())
        .unwrap_or("/");
    let uri = match Uri::builder()
        .scheme("http")
        .authority(state.upstream_authority.as_str())
        .path_and_query(pq)
        .build()
    {
        Ok(u) => u,
        Err(_) => {
            return simple(
                StatusCode::BAD_REQUEST,
                "chaosproxy: unusable request target\n",
            );
        }
    };
    parts.uri = uri;
    parts.version = if state.cfg.upstream_http2 {
        Version::HTTP_2
    } else {
        Version::HTTP_11
    };
    headers::strip_hop_by_hop(&mut parts.headers);
    headers::add_forwarded(&mut parts.headers, ctx.peer, original_host.as_deref());
    if state.cfg.upstream_http2 {
        parts.headers.remove(header::HOST);
    } else if let Ok(v) = HeaderValue::from_str(&state.upstream_authority) {
        parts.headers.insert(header::HOST, v);
    }
    if !parts.headers.contains_key("x-request-id")
        && let Ok(v) = HeaderValue::from_str(request_id)
    {
        parts.headers.insert("x-request-id", v);
    }
    let upstream_req = Request::from_parts(parts, incoming.boxed());

    let result = tokio::time::timeout(
        state.cfg.request_timeout,
        state.client.request(upstream_req),
    )
    .await;
    let resp = match result {
        Err(_) => {
            tracing::warn!(target: "chaosproxy::access", request_id, "upstream did not answer within request_timeout");
            return simple(
                StatusCode::GATEWAY_TIMEOUT,
                "chaosproxy: upstream timed out\n",
            );
        }
        Ok(Err(e)) => {
            tracing::warn!(target: "chaosproxy::access", request_id, error = %e, "upstream request failed");
            return simple(
                StatusCode::BAD_GATEWAY,
                "chaosproxy: upstream unavailable\n",
            );
        }
        Ok(Ok(r)) => r,
    };
    if let Some(d) = first_byte_delay {
        tokio::time::sleep(d).await;
    }
    let (mut parts, b) = resp.into_parts();
    headers::strip_hop_by_hop(&mut parts.headers);
    Response::from_parts(
        parts,
        b.map_err(|e| -> crate::BoxError { Box::new(e) }).boxed(),
    )
}
