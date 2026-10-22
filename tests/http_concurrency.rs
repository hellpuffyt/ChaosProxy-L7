//! Concurrency, shutdown, cancellation and log-accuracy checks for the HTTP proxy.
//!
//! Every test in this binary holds `SERIAL` so the process-wide measurements
//! (open file descriptors, live tokio tasks, captured logs) only see one test.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod common;

use std::collections::BTreeMap;
use std::convert::Infallible;
use std::io::Write;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use bytes::Bytes;
use chaosproxy::config::Config;
use chaosproxy::http::{HttpDeps, RunningHttp};
use common::{http_config, upstream};
use http::{Request, Response};
use http_body::Frame;
use http_body_util::BodyExt;
use hyper::body::Incoming;
use hyper::service::service_fn;
use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Barrier, Semaphore, mpsc};

static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

// ---------------------------------------------------------------- log capture

#[derive(Clone, Default)]
struct Logs(Arc<Mutex<Vec<u8>>>);
impl Write for Logs {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// Process-wide JSON log capture (multi-threaded runtimes need a global subscriber).
fn logs() -> &'static Logs {
    static LOGS: OnceLock<Logs> = OnceLock::new();
    LOGS.get_or_init(|| {
        let logs = Logs::default();
        let writer = logs.clone();
        let subscriber = tracing_subscriber::fmt()
            .json()
            .with_max_level(tracing::Level::INFO)
            .with_writer(move || writer.clone())
            .finish();
        tracing::subscriber::set_global_default(subscriber).unwrap();
        logs
    })
}

fn take_logs() -> String {
    let mut buf = logs().0.lock().unwrap();
    let out = String::from_utf8_lossy(&buf).into_owned();
    buf.clear();
    out
}

// ----------------------------------------------------------- resource probes

/// Open descriptors of this process (`None` where `/dev/fd` does not exist).
fn open_fds() -> Option<usize> {
    std::fs::read_dir("/dev/fd").ok().map(|d| d.count())
}

fn alive_tasks() -> usize {
    tokio::runtime::Handle::current()
        .metrics()
        .num_alive_tasks()
}

/// Polls `f` until it holds or `within` elapses; returns whether it held.
async fn eventually(within: Duration, mut f: impl FnMut() -> bool) -> bool {
    let end = Instant::now() + within;
    loop {
        if f() {
            return true;
        }
        if Instant::now() >= end {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

// -------------------------------------------------------------- the upstream

#[derive(Default)]
struct Up {
    open_conns: AtomicUsize,
    stream_dropped: AtomicBool,
}

struct ConnGuard(Arc<Up>);
impl Drop for ConnGuard {
    fn drop(&mut self) {
        self.0.open_conns.fetch_sub(1, Ordering::SeqCst);
    }
}

/// Signals when the streaming body's producer notices the reader is gone.
struct StreamGuard(Arc<Up>);
impl Drop for StreamGuard {
    fn drop(&mut self) {
        self.0.stream_dropped.store(true, Ordering::SeqCst);
    }
}

type UpBody = http_body_util::combinators::BoxBody<Bytes, Infallible>;

async fn up_handler(up: Arc<Up>, req: Request<Incoming>) -> Result<Response<UpBody>, Infallible> {
    let path = req.uri().path().to_owned();
    let full = |s: Bytes| http_body_util::Full::new(s).boxed();
    Ok(match path.as_str() {
        "/sleep" => {
            tokio::time::sleep(Duration::from_millis(30)).await;
            Response::new(full(Bytes::from_static(b"slept")))
        }
        "/big" => Response::new(full(Bytes::from(vec![b'x'; 4096]))),
        // An endless body, one 1 KiB chunk every 10 ms, until nobody reads it.
        "/stream" => {
            let (tx, rx) = mpsc::channel::<Result<Frame<Bytes>, Infallible>>(1);
            let guard = StreamGuard(Arc::clone(&up));
            tokio::spawn(async move {
                let _guard = guard;
                loop {
                    if tx
                        .send(Ok(Frame::data(Bytes::from(vec![b's'; 1024]))))
                        .await
                        .is_err()
                    {
                        return;
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            });
            Response::new(ChanBody(rx).boxed())
        }
        _ => Response::new(full(Bytes::from_static(b"ok"))),
    })
}

/// An `mpsc::Receiver` as a response body.
struct ChanBody(mpsc::Receiver<Result<Frame<Bytes>, Infallible>>);
impl http_body::Body for ChanBody {
    type Data = Bytes;
    type Error = Infallible;
    fn poll_frame(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Result<Frame<Bytes>, Infallible>>> {
        self.0.poll_recv(cx)
    }
}

async fn counting_upstream() -> (SocketAddr, Arc<Up>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let up = Arc::new(Up::default());
    let u2 = Arc::clone(&up);
    tokio::spawn(async move {
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                return;
            };
            u2.open_conns.fetch_add(1, Ordering::SeqCst);
            let guard = ConnGuard(Arc::clone(&u2));
            let u3 = Arc::clone(&u2);
            tokio::spawn(async move {
                let _guard = guard;
                let svc = service_fn(move |req| up_handler(Arc::clone(&u3), req));
                let _ = auto::Builder::new(TokioExecutor::new())
                    .serve_connection(TokioIo::new(stream), svc)
                    .await;
            });
        }
    });
    (addr, up)
}

async fn start(cfg: Config, limiter: Arc<Semaphore>) -> RunningHttp {
    let deps = HttpDeps {
        mode: cfg.mode,
        seed: cfg.seed,
        limiter,
        external: None,
    };
    chaosproxy::http::spawn(cfg.http.unwrap(), deps)
        .await
        .unwrap()
}

// ------------------------------------------------------------- raw client

#[derive(Debug, Clone, PartialEq, Eq)]
enum Outcome {
    /// Status code, `x-chaosproxy-fault`, `x-chaosproxy-request-id`.
    Response(u16, Option<String>, Option<String>),
    /// Connection closed or reset without a response.
    NoResponse,
    /// Nothing happened within the deadline (a deadlock or a leak).
    Stuck,
    /// The request could not even be written: the kernel dropped the
    /// connection from the accept queue before the proxy accepted it.
    NotSent,
}

fn header<'a>(head: &'a str, name: &str) -> Option<&'a str> {
    head.lines().find_map(|l| {
        let (k, v) = l.split_once(':')?;
        k.trim().eq_ignore_ascii_case(name).then(|| v.trim())
    })
}

/// One request on its own connection, `Connection: close`, read to the end.
async fn one(addr: SocketAddr, path: &str) -> Outcome {
    let s = TcpStream::connect(addr).await.unwrap();
    exchange(s, path).await
}

async fn exchange(mut s: TcpStream, path: &str) -> Outcome {
    let req = format!("GET {path} HTTP/1.1\r\nhost: x\r\nconnection: close\r\n\r\n");
    if s.write_all(req.as_bytes()).await.is_err() {
        return Outcome::NotSent;
    }
    let mut buf = Vec::new();
    let read = tokio::time::timeout(Duration::from_secs(20), s.read_to_end(&mut buf)).await;
    // Close with RST so neither side keeps a TIME_WAIT entry: hundreds of them
    // on loopback make later connects from a reused port fail spuriously.
    let _ = socket2::SockRef::from(&s).set_linger(Some(Duration::ZERO));
    drop(s);
    match read {
        Err(_) => Outcome::Stuck,
        Ok(_) if buf.is_empty() => Outcome::NoResponse,
        Ok(_) => {
            let text = String::from_utf8_lossy(&buf);
            let head = text.split("\r\n\r\n").next().unwrap_or("");
            let status = head
                .split_whitespace()
                .nth(1)
                .and_then(|s| s.parse().ok())
                .unwrap_or(0);
            Outcome::Response(
                status,
                header(head, "x-chaosproxy-fault").map(str::to_owned),
                header(head, "x-chaosproxy-request-id").map(str::to_owned),
            )
        }
    }
}

/// Connects, retrying a refused/reset handshake a few times. On macOS loopback
/// ports are handed out sequentially, so a new listener can inherit a port
/// that still has a TIME_WAIT entry for the same 4-tuple from an earlier test,
/// and the kernel then resets the SYN. No request has been sent at that point,
/// so retrying cannot hide a proxy failure.
async fn connect(addr: SocketAddr) -> TcpStream {
    let mut last = None;
    for _ in 0..5 {
        match TcpStream::connect(addr).await {
            Ok(s) => return s,
            Err(e) => last = Some(e),
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("connect {addr}: {last:?}");
}

/// Opens one connection per path (at most 64 handshakes at a time, because
/// macOS caps the accept backlog at 128), waits until all are open, then sends
/// every request at the same moment. Returns (path, outcome) in input order.
async fn fire(addr: SocketAddr, paths: Vec<&'static str>) -> Vec<(&'static str, Outcome)> {
    let n = paths.len();
    let connect_gate = Arc::new(Semaphore::new(8));
    let start_line = Arc::new(Barrier::new(n));
    let mut set = tokio::task::JoinSet::new();
    for (i, path) in paths.into_iter().enumerate() {
        let gate = Arc::clone(&connect_gate);
        let b = Arc::clone(&start_line);
        set.spawn(async move {
            let s = {
                let _permit = gate.acquire().await.unwrap();
                connect(addr).await
            };
            b.wait().await;
            let mut out = exchange(s, path).await;
            // macOS drops a few connections from a full accept queue (backlog
            // 128) during a cold burst, and the proxy never sees them; the
            // write then fails with EPIPE. Nothing reached the proxy, so the
            // request is simply sent again on a new connection.
            for _ in 0..3 {
                if out != Outcome::NotSent {
                    break;
                }
                out = exchange(connect(addr).await, path).await;
            }
            assert_ne!(out, Outcome::NotSent, "{path}: could not send");
            (i, path, out)
        });
    }
    let mut out = Vec::with_capacity(n);
    while let Some(r) = set.join_next().await {
        out.push(r.unwrap());
    }
    out.sort_by_key(|(i, _, _)| *i);
    out.into_iter().map(|(_, p, o)| (p, o)).collect()
}

/// Counts JSON log lines carrying `message` and (optionally) `"rule":"<rule>"`.
fn count(logs: &str, message: &str, rule: Option<&str>) -> usize {
    let msg = format!("\"message\":\"{message}\"");
    logs.lines()
        .filter(|l| l.contains(&msg))
        .filter(|l| rule.is_none_or(|r| l.contains(&format!("\"rule\":\"{r}\""))))
        .count()
}

fn rule(id: &str, prefix: &str, pct: f64, fault: &str) -> String {
    format!(
        "[[http.rules]]\nid = \"{id}\"\npercentage = {pct}\n[http.rules.match]\npath_prefix = \"{prefix}\"\n[http.rules.fault]\n{fault}\n"
    )
}

const RULE_IDS: [&str; 7] = [
    "c-err", "c-lat", "c-fbd", "c-thr", "c-rst", "c-to", "c-hang",
];

// ------------------------------------------------------------------- tests

/// 512 simultaneous requests with seven fault kinds mixed in: every request
/// finishes, every injected fault is visible to the client and in the logs
/// exactly once, and afterwards no permit, task, descriptor or upstream
/// connection is left behind.
async fn mixed_faults_scenario(upstream_http2: bool) {
    let _ = take_logs();
    let (up_addr, up) = counting_upstream().await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    let fds_before = open_fds();
    let tasks_before = alive_tasks();

    let rules = [
        rule("c-err", "/err", 50.0, "kind = \"error\"\nstatus = 503"),
        rule(
            "c-lat",
            "/lat",
            50.0,
            "kind = \"latency\"\ndelay = \"20ms\"\njitter = \"20ms\"",
        ),
        rule(
            "c-fbd",
            "/fbd",
            100.0,
            "kind = \"first_byte_delay\"\ndelay = \"20ms\"",
        ),
        rule(
            "c-thr",
            "/big",
            100.0,
            "kind = \"throttle\"\nbytes_per_second = 200000",
        ),
        rule("c-rst", "/rst", 50.0, "kind = \"reset\""),
        rule("c-to", "/to", 100.0, "kind = \"timeout\"\nafter = \"30ms\""),
        rule("c-hang", "/hang", 100.0, "kind = \"hang\"\nmax = \"50ms\""),
    ]
    .concat();
    let mut cfg = http_config(up_addr, "", &rules);
    cfg.max_concurrency = 4096;
    if let Some(h) = cfg.http.as_mut() {
        h.upstream_http2 = upstream_http2;
    }
    let limiter = Arc::new(Semaphore::new(cfg.max_concurrency));
    let p = start(cfg, Arc::clone(&limiter)).await;
    let addr = p.local_addr;

    let paths = [
        "/err", "/lat", "/fbd", "/big", "/rst", "/to", "/hang", "/sleep",
    ];
    const N: usize = 512;
    let started = Instant::now();
    let results = fire(addr, (0..N).map(|i| paths[i % paths.len()]).collect()).await;
    let elapsed = started.elapsed();
    assert_eq!(results.len(), N);
    assert!(elapsed < Duration::from_secs(15), "took {elapsed:?}");

    // What the clients observed, per rule.
    let mut observed: BTreeMap<&str, usize> = BTreeMap::new();
    let mut bad_gateway = 0;
    for (path, out) in &results {
        match out {
            Outcome::Stuck => panic!("{path}: request stuck"),
            Outcome::NotSent => unreachable!("fire() retries unsent requests"),
            Outcome::NoResponse => {
                let r = match *path {
                    "/rst" => "c-rst",
                    "/hang" => "c-hang",
                    _ => panic!("{path}: no response"),
                };
                *observed.entry(r).or_default() += 1;
            }
            Outcome::Response(status, fault, id) => {
                assert!(id.is_some(), "{path}: missing request id");
                assert!(*path != "/hang", "hang must not answer");
                if let Some(f) = fault {
                    let r = RULE_IDS
                        .iter()
                        .find(|r| *r == f)
                        .unwrap_or_else(|| panic!("unknown fault header {f}"));
                    assert!(*r != "c-rst", "reset must not answer");
                    *observed.entry(r).or_default() += 1;
                }
                let synthetic = match (*path, fault.is_some()) {
                    ("/err", true) => Some(503),
                    ("/to", true) => Some(504),
                    _ => None,
                };
                match synthetic {
                    Some(code) => assert_eq!(*status, code, "{path}"),
                    // A forwarded request. Over HTTP/1.1 every forwarded request
                    // needs its own upstream connection; a burst can overflow the
                    // upstream's accept backlog (128 on macOS), and the proxy then
                    // answers 502 — correct behaviour, counted separately.
                    None if *status == 502 && !upstream_http2 => bad_gateway += 1,
                    None => assert_eq!(*status, 200, "{path} {fault:?}"),
                }
            }
        }
    }
    eprintln!(
        "upstream_http2={upstream_http2}: {N} requests in {elapsed:?}, faults {observed:?}, 502s {bad_gateway}"
    );
    // 64 requests per path; 100% rules fire on every one of them.
    for r in ["c-fbd", "c-thr", "c-to", "c-hang"] {
        assert_eq!(
            observed.get(r),
            Some(&(N / paths.len())),
            "{r}: {observed:?}"
        );
    }
    for r in ["c-err", "c-lat", "c-rst"] {
        let n = observed.get(r).copied().unwrap_or(0);
        assert!((10..=54).contains(&n), "{r} fired {n} times out of 64");
    }

    p.shutdown().await;
    let logs = take_logs();
    // Logs agree with what clients saw: one injected decision per observed fault...
    for r in RULE_IDS {
        let seen = observed.get(r).copied().unwrap_or(0);
        assert_eq!(
            count(&logs, "fault decision", Some(r)),
            seen,
            "fault log lines for {r}"
        );
    }
    // ...and exactly one access-log line per request, including reset and hang.
    assert_eq!(count(&logs, "request", None), N, "access log lines");

    // Nothing left behind.
    assert_eq!(limiter.available_permits(), 4096, "global permits leaked");
    assert!(
        eventually(Duration::from_secs(5), || up
            .open_conns
            .load(Ordering::SeqCst)
            == 0)
        .await,
        "upstream connections still open: {}",
        up.open_conns.load(Ordering::SeqCst)
    );
    assert!(
        eventually(Duration::from_secs(5), || alive_tasks() <= tasks_before).await,
        "tasks before {tasks_before}, after {}",
        alive_tasks()
    );
    if let Some(before) = fds_before {
        assert!(
            eventually(Duration::from_secs(5), || open_fds().unwrap_or(usize::MAX)
                <= before)
            .await,
            "descriptors before {before}, after {:?}",
            open_fds()
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mixed_faults_under_512_concurrent_requests_http1_upstream() {
    let _serial = SERIAL.lock().await;
    mixed_faults_scenario(false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mixed_faults_under_512_concurrent_requests_h2c_upstream() {
    let _serial = SERIAL.lock().await;
    mixed_faults_scenario(true).await;
}

/// Fires `n` requests at once and returns request id -> faulted.
async fn concurrent_pattern(seed: u64, n: usize) -> BTreeMap<String, bool> {
    let (up_addr, _up) = counting_upstream().await;
    let mut cfg = http_config(
        up_addr,
        "",
        &rule("det", "/", 30.0, "kind = \"error\"\nstatus = 500"),
    );
    cfg.seed = seed;
    cfg.max_concurrency = 4096;
    if let Some(h) = cfg.http.as_mut() {
        h.upstream_http2 = true;
    }
    let p = start(cfg, Arc::new(Semaphore::new(4096))).await;
    let results = fire(p.local_addr, vec!["/"; n]).await;
    let mut out = BTreeMap::new();
    for (_, r) in results {
        match r {
            Outcome::Response(status, fault, Some(id)) => {
                assert_eq!(fault.is_some(), status == 500, "{status} {fault:?}");
                out.insert(id, fault.is_some());
            }
            other => panic!("unexpected {other:?}"),
        }
    }
    p.shutdown().await;
    out
}

/// Under concurrency the fault decision is still a pure function of
/// (seed, request sequence number): racing requests may swap sequence numbers,
/// but the set of faulted sequence numbers is identical from run to run.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_decisions_are_a_function_of_seed_and_sequence_number() {
    let _serial = SERIAL.lock().await;
    let a = concurrent_pattern(99, 300).await;
    let b = concurrent_pattern(99, 300).await;
    assert_eq!(a.len(), 300, "every sequence number used exactly once");
    assert_eq!(a, b, "same seed, same faulted sequence numbers");
    let faulted = a.values().filter(|f| **f).count();
    assert!((50..=130).contains(&faulted), "30% of 300, got {faulted}");
    let c = concurrent_pattern(100, 300).await;
    assert_ne!(a, c, "a different seed gives a different pattern");
}

/// Connections still busy when `shutdown_grace` runs out are torn down before
/// `shutdown()` returns, and the upstream request they carried is cancelled.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutdown_aborts_what_outlives_the_grace_period() {
    let _serial = SERIAL.lock().await;
    let (up_addr, stats) = upstream().await;
    let rules = rule(
        "long-hang",
        "/hang",
        100.0,
        "kind = \"hang\"\nmax = \"30s\"",
    );
    let mut cfg = http_config(up_addr, "", &rules);
    if let Some(h) = cfg.http.as_mut() {
        h.shutdown_grace = Duration::from_millis(300);
    }
    let p = start(cfg, Arc::new(Semaphore::new(64))).await;
    let addr = p.local_addr;

    let hanging = tokio::spawn(one(addr, "/hang"));
    let slow = tokio::spawn(one(addr, "/slow")); // upstream answers after 800 ms
    tokio::time::sleep(Duration::from_millis(150)).await;

    let t = Instant::now();
    p.shutdown().await;
    let took = t.elapsed();
    assert!(took < Duration::from_secs(2), "shutdown took {took:?}");

    let hang_out = tokio::time::timeout(Duration::from_secs(2), hanging)
        .await
        .expect("hanging connection must be closed by shutdown")
        .unwrap();
    assert_eq!(hang_out, Outcome::NoResponse);
    let slow_out = tokio::time::timeout(Duration::from_secs(2), slow)
        .await
        .expect("slow connection must be closed by shutdown")
        .unwrap();
    assert_eq!(slow_out, Outcome::NoResponse);
    tokio::time::sleep(Duration::from_millis(1000)).await;
    assert!(
        stats.slow_cancelled.load(Ordering::SeqCst),
        "upstream request must be cancelled"
    );
    assert!(!stats.slow_completed.load(Ordering::SeqCst));
}

/// A client that disconnects while the response body is streaming makes the
/// proxy drop the upstream body (plain forwarding and the throttle path).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mid_stream_disconnect_releases_the_upstream_body() {
    let _serial = SERIAL.lock().await;
    for throttled in [false, true] {
        let (up_addr, up) = counting_upstream().await;
        let rules = if throttled {
            rule(
                "thr",
                "/stream",
                100.0,
                "kind = \"throttle\"\nbytes_per_second = 50000",
            )
        } else {
            String::new()
        };
        let p = start(
            http_config(up_addr, "", &rules),
            Arc::new(Semaphore::new(64)),
        )
        .await;
        let mut s = TcpStream::connect(p.local_addr).await.unwrap();
        s.write_all(b"GET /stream HTTP/1.1\r\nhost: x\r\n\r\n")
            .await
            .unwrap();
        // Read some of the body, then go away.
        let mut got = 0;
        let mut buf = [0u8; 4096];
        while got < 4096 {
            let n = tokio::time::timeout(Duration::from_secs(3), s.read(&mut buf))
                .await
                .expect("body flows")
                .unwrap();
            assert!(n > 0);
            got += n;
        }
        assert!(!up.stream_dropped.load(Ordering::SeqCst));
        drop(s);
        assert!(
            eventually(Duration::from_secs(3), || up
                .stream_dropped
                .load(Ordering::SeqCst))
            .await,
            "throttled={throttled}: upstream body still produced after the client left"
        );
        assert!(
            eventually(Duration::from_secs(3), || up
                .open_conns
                .load(Ordering::SeqCst)
                == 0)
            .await,
            "throttled={throttled}: upstream connection kept"
        );
        p.shutdown().await;
    }
}
