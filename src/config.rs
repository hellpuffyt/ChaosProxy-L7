//! Configuration file format, loading and validation.
//!
//! Validation is strict and happens before any socket is opened: an invalid
//! file refuses to start rather than running with surprising behaviour.

use std::collections::{BTreeMap, HashSet};
use std::net::{IpAddr, SocketAddr};
use std::path::{Path, PathBuf};
use std::time::Duration;

use serde::Deserialize;

/// Longest delay any fault may introduce. Keeps a typo from parking
/// connections for hours.
pub const MAX_FAULT_DURATION: Duration = Duration::from_secs(600);

/// Whether faults are applied or only logged.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum Mode {
    /// Apply matching faults.
    #[default]
    Inject,
    /// Decide and log as if injecting, but forward everything untouched.
    Observe,
}

/// Root of the TOML file.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// `inject` (default) or `observe`.
    #[serde(default)]
    pub mode: Mode,
    /// Seed for every fault decision. Same seed + same request order = same faults.
    #[serde(default)]
    pub seed: u64,
    /// Must be `true` (together with `--allow-non-loopback`) to listen on a
    /// non-loopback address.
    #[serde(default)]
    pub allow_non_loopback: bool,
    /// `host:port` entries the proxy may connect to. Every upstream must be listed.
    pub upstream_allowlist: Vec<String>,
    /// Maximum in-flight HTTP requests plus PostgreSQL connections; excess is shed.
    #[serde(default = "default_max_concurrency")]
    pub max_concurrency: usize,
    /// HTTP listener.
    pub http: Option<HttpConfig>,
    /// PostgreSQL listener.
    pub postgres: Option<PgConfig>,
    /// WebAssembly rule extensions.
    #[serde(default)]
    pub wasm: WasmConfig,
}

fn default_max_concurrency() -> usize {
    256
}

/// HTTP proxy settings.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HttpConfig {
    /// Listen address, e.g. `127.0.0.1:8080`.
    pub listen: SocketAddr,
    /// Upstream base URL. Only `http://` is supported.
    pub upstream: String,
    /// Talk to the upstream with HTTP/2 prior knowledge (h2c) instead of HTTP/1.1.
    #[serde(default)]
    pub upstream_http2: bool,
    /// Time allowed for the upstream to return response headers.
    #[serde(default = "default_request_timeout", with = "humantime_serde")]
    pub request_timeout: Duration,
    /// Time allowed for a client to send request headers (HTTP/1.1).
    #[serde(default = "default_header_timeout", with = "humantime_serde")]
    pub header_read_timeout: Duration,
    /// How long to wait for in-flight requests on shutdown.
    #[serde(default = "default_grace", with = "humantime_serde")]
    pub shutdown_grace: Duration,
    /// Fault rules, evaluated in order; the first that matches and wins its roll applies.
    #[serde(default)]
    pub rules: Vec<HttpRule>,
}

fn default_request_timeout() -> Duration {
    Duration::from_secs(30)
}
fn default_header_timeout() -> Duration {
    Duration::from_secs(10)
}
fn default_grace() -> Duration {
    Duration::from_secs(10)
}

/// One HTTP fault rule.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HttpRule {
    /// Unique id, logged with every decision.
    pub id: String,
    /// Chance (0–100) that a matching request is faulted.
    #[serde(default = "hundred")]
    pub percentage: f64,
    /// Maximum requests this rule may disturb at once (0 = unlimited).
    #[serde(default)]
    pub max_concurrent: usize,
    /// Which requests the rule considers.
    #[serde(default, rename = "match")]
    pub matcher: HttpMatch,
    /// What to do to them.
    pub fault: HttpFault,
}

fn hundred() -> f64 {
    100.0
}

/// Request matcher; all present conditions must hold.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HttpMatch {
    /// Path must start with this prefix.
    pub path_prefix: Option<String>,
    /// Path must match this regular expression.
    pub path_regex: Option<String>,
    /// Method must be one of these (case-insensitive).
    #[serde(default)]
    pub methods: Vec<String>,
    /// Headers that must be present with exactly these values (names case-insensitive).
    #[serde(default)]
    pub headers: BTreeMap<String, String>,
    /// Name of a WebAssembly module (see `[wasm.modules]`) that must also agree.
    pub wasm: Option<String>,
}

/// HTTP fault kinds.
#[derive(Debug, Clone, Deserialize, PartialEq)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum HttpFault {
    /// Wait `delay` plus a uniform random `0..=jitter` before forwarding.
    Latency {
        /// Fixed part.
        #[serde(with = "humantime_serde")]
        delay: Duration,
        /// Random extra, uniform in `[0, jitter]`.
        #[serde(default, with = "humantime_serde")]
        jitter: Duration,
    },
    /// Forward immediately, but hold the upstream's response headers for `delay`.
    FirstByteDelay {
        /// Hold time.
        #[serde(with = "humantime_serde")]
        delay: Duration,
    },
    /// Stream the response body at no more than `bytes_per_second`.
    Throttle {
        /// Bandwidth cap.
        bytes_per_second: u64,
    },
    /// Abort the client's TCP connection with RST (SO_LINGER 0). Not forwarded.
    Reset,
    /// Wait `after`, then answer `504 Gateway Timeout` as if the upstream timed out. Not forwarded.
    Timeout {
        /// Time before the 504.
        #[serde(with = "humantime_serde")]
        after: Duration,
    },
    /// Never answer; close the connection (FIN) after `max`. Not forwarded.
    Hang {
        /// Upper bound on the hang (default 60s).
        #[serde(default = "default_hang", with = "humantime_serde")]
        max: Duration,
    },
    /// Answer with `status` and `body` without contacting the upstream.
    Error {
        /// HTTP status code (100–599).
        status: u16,
        /// Response body.
        #[serde(default)]
        body: String,
    },
}

fn default_hang() -> Duration {
    Duration::from_secs(60)
}

/// PostgreSQL proxy settings.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PgConfig {
    /// Listen address.
    pub listen: SocketAddr,
    /// Upstream server `host:port`.
    pub upstream: String,
    /// Largest message accepted from either side; larger frames close the connection.
    #[serde(default = "default_max_message")]
    pub max_message_bytes: u32,
    /// Timeout for connecting to the upstream.
    #[serde(default = "default_connect_timeout", with = "humantime_serde")]
    pub connect_timeout: Duration,
    /// Log query text in fault decisions (off by default: queries can hold literals).
    #[serde(default)]
    pub log_query_text: bool,
    /// Fault rules, evaluated in order.
    #[serde(default)]
    pub rules: Vec<PgRule>,
}

fn default_max_message() -> u32 {
    16 * 1024 * 1024
}
fn default_connect_timeout() -> Duration {
    Duration::from_secs(5)
}

/// One PostgreSQL fault rule.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PgRule {
    /// Unique id.
    pub id: String,
    /// Chance (0–100) that a matching query or startup is faulted.
    #[serde(default = "hundred")]
    pub percentage: f64,
    /// Matcher.
    #[serde(default, rename = "match")]
    pub matcher: PgMatch,
    /// Fault.
    pub fault: PgFault,
}

/// PostgreSQL matcher; all present conditions must hold.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PgMatch {
    /// Regular expression on the SQL text (simple `Query` or the statement behind an `Execute`).
    pub query_regex: Option<String>,
    /// Startup parameter `user` must equal this.
    pub user: Option<String>,
    /// Startup parameter `database` must equal this.
    pub database: Option<String>,
}

/// PostgreSQL fault kinds.
#[derive(Debug, Clone, Deserialize, PartialEq)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum PgFault {
    /// Do not run the query; answer `ErrorResponse` with this SQLSTATE.
    Error {
        /// Five-character SQLSTATE, e.g. `40001`.
        sqlstate: String,
        /// Error message.
        #[serde(default = "default_pg_message")]
        message: String,
    },
    /// Hold the query for `delay` before forwarding it.
    Delay {
        /// Hold time.
        #[serde(with = "humantime_serde")]
        delay: Duration,
    },
    /// Forward the query, then close both connections after `after_rows` result rows.
    DropMidResult {
        /// Rows relayed before the drop.
        after_rows: u32,
    },
    /// Refuse the connection at startup with a FATAL `ErrorResponse`.
    RejectStartup {
        /// Five-character SQLSTATE, e.g. `53300` (too_many_connections).
        sqlstate: String,
        /// Error message.
        #[serde(default = "default_pg_message")]
        message: String,
    },
}

fn default_pg_message() -> String {
    "fault injected by chaosproxy".to_owned()
}

/// WebAssembly sandbox limits and modules.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WasmConfig {
    /// Fuel per decision (roughly, executed instructions).
    #[serde(default = "default_fuel")]
    pub fuel: u64,
    /// Maximum linear memory per instance.
    #[serde(default = "default_wasm_memory")]
    pub max_memory_bytes: usize,
    /// Wall-clock budget per decision.
    #[serde(default = "default_wasm_timeout", with = "humantime_serde")]
    pub timeout: Duration,
    /// Request headers passed to modules. `authorization`, `cookie` and
    /// `proxy-authorization` are refused.
    #[serde(default)]
    pub headers: Vec<String>,
    /// Module name -> path to a `.wasm` or `.wat` file (relative to the config file).
    #[serde(default)]
    pub modules: BTreeMap<String, PathBuf>,
}

impl Default for WasmConfig {
    fn default() -> Self {
        Self {
            fuel: default_fuel(),
            max_memory_bytes: default_wasm_memory(),
            timeout: default_wasm_timeout(),
            headers: Vec::new(),
            modules: BTreeMap::new(),
        }
    }
}

fn default_fuel() -> u64 {
    1_000_000
}
fn default_wasm_memory() -> usize {
    1024 * 1024
}
fn default_wasm_timeout() -> Duration {
    Duration::from_millis(50)
}

/// Headers that must never reach logs or WebAssembly modules.
pub const SENSITIVE_HEADERS: &[&str] = &[
    "authorization",
    "cookie",
    "proxy-authorization",
    "set-cookie",
];

/// A configuration error, phrased so the user can fix the file.
#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    /// File could not be read.
    #[error("cannot read config {path}: {source}")]
    Read {
        /// Path.
        path: PathBuf,
        /// Cause.
        source: std::io::Error,
    },
    /// TOML or schema error.
    #[error("invalid config: {0}")]
    Parse(#[from] toml::de::Error),
    /// Semantic validation failed.
    #[error("invalid config: {0}")]
    Invalid(String),
}

fn invalid<T>(msg: impl Into<String>) -> Result<T, ConfigError> {
    Err(ConfigError::Invalid(msg.into()))
}

impl Config {
    /// Reads, parses and validates a config file. Relative wasm module paths
    /// are resolved against the file's directory.
    pub fn load(path: &Path, cli_allow_non_loopback: bool) -> Result<Self, ConfigError> {
        let text = std::fs::read_to_string(path).map_err(|source| ConfigError::Read {
            path: path.to_owned(),
            source,
        })?;
        let mut cfg = Self::parse(&text, cli_allow_non_loopback)?;
        let base = path.parent().unwrap_or_else(|| Path::new("."));
        for p in cfg.wasm.modules.values_mut() {
            if p.is_relative() {
                *p = base.join(&*p);
            }
        }
        Ok(cfg)
    }

    /// Parses and validates TOML text.
    pub fn parse(text: &str, cli_allow_non_loopback: bool) -> Result<Self, ConfigError> {
        let cfg: Config = toml::from_str(text)?;
        cfg.validate(cli_allow_non_loopback)?;
        Ok(cfg)
    }

    /// Checks every safety rule and cross-reference.
    pub fn validate(&self, cli_allow_non_loopback: bool) -> Result<(), ConfigError> {
        if self.http.is_none() && self.postgres.is_none() {
            return invalid("configure at least one of [http] or [postgres]");
        }
        if self.max_concurrency == 0 {
            return invalid("max_concurrency must be at least 1");
        }
        let allow = |addr: &SocketAddr| -> Result<(), ConfigError> {
            if addr.ip().is_loopback() {
                return Ok(());
            }
            if self.allow_non_loopback && cli_allow_non_loopback {
                return Ok(());
            }
            invalid(format!(
                "refusing to listen on non-loopback address {addr}: set `allow_non_loopback = true` in the config \
                 AND pass --allow-non-loopback (both are required)"
            ))
        };
        let allowlist: HashSet<&str> = self.upstream_allowlist.iter().map(String::as_str).collect();
        if let Some(h) = &self.http {
            allow(&h.listen)?;
            let authority = http_upstream_authority(&h.upstream)?;
            if !allowlist.contains(authority.as_str()) {
                return invalid(format!(
                    "HTTP upstream {authority} is not in upstream_allowlist; add \"{authority}\" to allow it"
                ));
            }
            check_duration("http.request_timeout", h.request_timeout)?;
            check_duration("http.header_read_timeout", h.header_read_timeout)?;
            check_duration("http.shutdown_grace", h.shutdown_grace)?;
            let mut ids = HashSet::new();
            for r in &h.rules {
                check_id(&mut ids, &r.id)?;
                check_percentage(&r.id, r.percentage)?;
                validate_http_rule(r, &self.wasm)?;
            }
        }
        if let Some(p) = &self.postgres {
            allow(&p.listen)?;
            if !allowlist.contains(p.upstream.as_str()) {
                return invalid(format!(
                    "PostgreSQL upstream {} is not in upstream_allowlist; add \"{}\" to allow it",
                    p.upstream, p.upstream
                ));
            }
            if p.max_message_bytes < 1024 {
                return invalid("postgres.max_message_bytes must be at least 1024");
            }
            check_duration("postgres.connect_timeout", p.connect_timeout)?;
            let mut ids = HashSet::new();
            for r in &p.rules {
                check_id(&mut ids, &r.id)?;
                check_percentage(&r.id, r.percentage)?;
                validate_pg_rule(r)?;
            }
        }
        for h in &self.wasm.headers {
            if SENSITIVE_HEADERS.contains(&h.to_ascii_lowercase().as_str()) {
                return invalid(format!(
                    "wasm.headers may not include sensitive header {h:?}"
                ));
            }
        }
        if self.wasm.fuel == 0 || self.wasm.max_memory_bytes < 65_536 {
            return invalid(
                "wasm.fuel must be > 0 and wasm.max_memory_bytes at least 65536 (one page)",
            );
        }
        check_duration("wasm.timeout", self.wasm.timeout)?;
        Ok(())
    }
}

/// Returns the `host:port` of an `http://` upstream URL, defaulting the port to 80.
pub fn http_upstream_authority(url: &str) -> Result<String, ConfigError> {
    let uri: http::Uri = url.parse().map_err(|e| {
        ConfigError::Invalid(format!("http.upstream {url:?} is not a valid URL: {e}"))
    })?;
    match uri.scheme_str() {
        Some("http") => {}
        Some("https") => {
            return invalid(
                "https upstreams are not supported (no TLS to the upstream); use http://",
            );
        }
        _ => return invalid(format!("http.upstream {url:?} must start with http://")),
    }
    let host = uri
        .host()
        .ok_or_else(|| ConfigError::Invalid(format!("http.upstream {url:?} has no host")))?;
    if uri.path() != "/" && !uri.path().is_empty() {
        return invalid("http.upstream must not contain a path; requests keep their own path");
    }
    let port = uri.port_u16().unwrap_or(80);
    let host = match host.parse::<IpAddr>() {
        Ok(IpAddr::V6(v6)) => format!("[{v6}]"),
        _ => host
            .trim_start_matches('[')
            .trim_end_matches(']')
            .to_owned(),
    };
    Ok(format!("{host}:{port}"))
}

fn check_id(ids: &mut HashSet<String>, id: &str) -> Result<(), ConfigError> {
    if id.is_empty()
        || !id
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
    {
        return invalid(format!(
            "rule id {id:?} must be non-empty and use only letters, digits, '-' and '_'"
        ));
    }
    if !ids.insert(id.to_owned()) {
        return invalid(format!("duplicate rule id {id:?}"));
    }
    Ok(())
}

fn check_percentage(id: &str, p: f64) -> Result<(), ConfigError> {
    if !(0.0..=100.0).contains(&p) {
        return invalid(format!("rule {id}: percentage must be between 0 and 100"));
    }
    Ok(())
}

fn check_duration(what: &str, d: Duration) -> Result<(), ConfigError> {
    if d > MAX_FAULT_DURATION {
        return invalid(format!(
            "{what} must be at most {}s",
            MAX_FAULT_DURATION.as_secs()
        ));
    }
    Ok(())
}

fn validate_http_rule(r: &HttpRule, wasm: &WasmConfig) -> Result<(), ConfigError> {
    if let Some(re) = &r.matcher.path_regex {
        regex::Regex::new(re)
            .map_err(|e| ConfigError::Invalid(format!("rule {}: bad path_regex: {e}", r.id)))?;
    }
    if let Some(p) = &r.matcher.path_prefix
        && !p.starts_with('/')
    {
        return invalid(format!("rule {}: path_prefix must start with '/'", r.id));
    }
    for m in &r.matcher.methods {
        http::Method::from_bytes(m.to_ascii_uppercase().as_bytes())
            .map_err(|_| ConfigError::Invalid(format!("rule {}: bad method {m:?}", r.id)))?;
    }
    for name in r.matcher.headers.keys() {
        http::HeaderName::from_bytes(name.to_ascii_lowercase().as_bytes()).map_err(|_| {
            ConfigError::Invalid(format!("rule {}: bad header name {name:?}", r.id))
        })?;
    }
    if let Some(m) = &r.matcher.wasm {
        if !cfg!(feature = "wasm") {
            return invalid(format!(
                "rule {}: uses wasm module {m:?} but this build has no `wasm` feature",
                r.id
            ));
        }
        if !wasm.modules.contains_key(m) {
            return invalid(format!(
                "rule {}: wasm module {m:?} is not defined in [wasm.modules]",
                r.id
            ));
        }
    }
    match &r.fault {
        HttpFault::Latency { delay, jitter } => {
            check_duration(&format!("rule {} delay", r.id), *delay)?;
            check_duration(&format!("rule {} jitter", r.id), *jitter)?;
        }
        HttpFault::FirstByteDelay { delay } => {
            check_duration(&format!("rule {} delay", r.id), *delay)?
        }
        HttpFault::Timeout { after } => check_duration(&format!("rule {} after", r.id), *after)?,
        HttpFault::Hang { max } => check_duration(&format!("rule {} max", r.id), *max)?,
        HttpFault::Throttle { bytes_per_second } => {
            if *bytes_per_second == 0 {
                return invalid(format!("rule {}: bytes_per_second must be > 0", r.id));
            }
        }
        HttpFault::Error { status, .. } => {
            if !(100..=599).contains(status) {
                return invalid(format!("rule {}: status must be 100-599", r.id));
            }
        }
        HttpFault::Reset => {}
    }
    Ok(())
}

fn check_sqlstate(id: &str, s: &str) -> Result<(), ConfigError> {
    if s.len() != 5
        || !s
            .chars()
            .all(|c| c.is_ascii_digit() || c.is_ascii_uppercase())
    {
        return invalid(format!(
            "rule {id}: sqlstate {s:?} must be 5 characters of 0-9/A-Z"
        ));
    }
    Ok(())
}

fn validate_pg_rule(r: &PgRule) -> Result<(), ConfigError> {
    if let Some(re) = &r.matcher.query_regex {
        regex::Regex::new(re)
            .map_err(|e| ConfigError::Invalid(format!("rule {}: bad query_regex: {e}", r.id)))?;
    }
    match &r.fault {
        PgFault::Error { sqlstate, .. } => check_sqlstate(&r.id, sqlstate)?,
        PgFault::RejectStartup { sqlstate, .. } => {
            check_sqlstate(&r.id, sqlstate)?;
            if r.matcher.query_regex.is_some() {
                return invalid(format!(
                    "rule {}: reject_startup cannot use query_regex",
                    r.id
                ));
            }
        }
        PgFault::Delay { delay } => check_duration(&format!("rule {} delay", r.id), *delay)?,
        PgFault::DropMidResult { .. } => {}
    }
    Ok(())
}
