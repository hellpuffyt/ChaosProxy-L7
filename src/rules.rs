//! Deterministic fault decisions.
//!
//! A decision is a pure function of `(seed, listener stream, sequence number,
//! request attributes)` plus the per-rule concurrency counters. Rules are
//! tried in order; the first one whose matcher holds, whose percentage roll
//! succeeds and whose cap has room wins. At most one fault applies per request
//! or query.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use regex::Regex;

use crate::config::{HttpFault, HttpRule, PgFault, PgRule};
use crate::rng::SplitMix64;

/// Stream ids keep HTTP and PostgreSQL decisions independent.
pub const STREAM_HTTP: u64 = 1;
/// Stream id for PostgreSQL decisions.
pub const STREAM_PG: u64 = 2;

/// Counts how many requests a rule is currently disturbing.
#[derive(Debug, Default)]
pub struct Cap {
    max: usize,
    active: AtomicUsize,
}

impl Cap {
    fn new(max: usize) -> Arc<Self> {
        Arc::new(Self {
            max,
            active: AtomicUsize::new(0),
        })
    }

    /// Reserves a slot, or returns `None` when the rule is at its cap.
    fn try_acquire(self: &Arc<Self>) -> Option<CapGuard> {
        if self.max == 0 {
            self.active.fetch_add(1, Ordering::Relaxed);
            return Some(CapGuard(Arc::clone(self)));
        }
        let mut cur = self.active.load(Ordering::Relaxed);
        loop {
            if cur >= self.max {
                return None;
            }
            match self.active.compare_exchange_weak(
                cur,
                cur + 1,
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                Ok(_) => return Some(CapGuard(Arc::clone(self))),
                Err(actual) => cur = actual,
            }
        }
    }

    /// Requests currently held by this rule.
    pub fn active(&self) -> usize {
        self.active.load(Ordering::Relaxed)
    }
}

/// Releases a rule slot when dropped.
#[derive(Debug)]
pub struct CapGuard(Arc<Cap>);

impl Drop for CapGuard {
    fn drop(&mut self) {
        self.0.active.fetch_sub(1, Ordering::AcqRel);
    }
}

/// HTTP request attributes visible to matchers.
#[derive(Debug)]
pub struct HttpRequestView<'a> {
    /// Method.
    pub method: &'a http::Method,
    /// Path (no query).
    pub path: &'a str,
    /// Headers.
    pub headers: &'a http::HeaderMap,
}

/// External matcher hook (implemented by the wasm module set).
pub trait ExternalMatcher: Send + Sync {
    /// Returns `Ok(true)` when module `name` wants the rule to apply. Errors
    /// are treated as "no" by the caller (fail closed: no fault).
    fn matches(&self, name: &str, req: &HttpRequestView<'_>) -> Result<bool, String>;
}

/// A compiled HTTP rule.
#[derive(Debug)]
pub struct CompiledHttpRule {
    /// Source rule.
    pub rule: HttpRule,
    path_regex: Option<Regex>,
    methods: Vec<http::Method>,
    headers: Vec<(http::HeaderName, String)>,
    cap: Arc<Cap>,
}

/// Outcome of an HTTP decision.
#[derive(Debug)]
pub struct HttpDecision {
    /// Rule id.
    pub rule_id: String,
    /// The fault, with randomness already resolved (latency includes jitter).
    pub fault: ResolvedHttpFault,
    /// Holds the rule's concurrency slot for as long as the fault is active.
    pub guard: CapGuard,
}

/// An HTTP fault with all random parts fixed.
#[derive(Debug, Clone, PartialEq)]
pub enum ResolvedHttpFault {
    /// Sleep this long, then forward.
    Latency(Duration),
    /// Forward, then hold response headers this long.
    FirstByteDelay(Duration),
    /// Pace the response body.
    Throttle(u64),
    /// TCP RST.
    Reset,
    /// 504 after this long.
    Timeout(Duration),
    /// No answer; close after this long.
    Hang(Duration),
    /// Synthetic response.
    Error(u16, String),
}

impl ResolvedHttpFault {
    /// Short name for logs and the `x-chaosproxy-fault` header.
    pub fn kind(&self) -> &'static str {
        match self {
            Self::Latency(_) => "latency",
            Self::FirstByteDelay(_) => "first_byte_delay",
            Self::Throttle(_) => "throttle",
            Self::Reset => "reset",
            Self::Timeout(_) => "timeout",
            Self::Hang(_) => "hang",
            Self::Error(..) => "error",
        }
    }
}

/// Every HTTP rule, compiled once at startup.
#[derive(Debug)]
pub struct HttpRules {
    rules: Vec<CompiledHttpRule>,
    seed: u64,
}

/// Why a rule that matched did not fire (logged in observe/debug paths).
#[derive(Debug, PartialEq, Eq)]
pub enum Skip {
    /// The percentage roll failed.
    Roll,
    /// The rule is at `max_concurrent`.
    Cap,
}

impl HttpRules {
    /// Compiles rules. Config validation has already checked regexes, so a
    /// failure here is reported rather than panicking.
    pub fn compile(rules: &[HttpRule], seed: u64) -> Result<Self, String> {
        let mut out = Vec::with_capacity(rules.len());
        for r in rules {
            let path_regex = match &r.matcher.path_regex {
                Some(s) => Some(Regex::new(s).map_err(|e| format!("rule {}: {e}", r.id))?),
                None => None,
            };
            let methods = r
                .matcher
                .methods
                .iter()
                .map(|m| http::Method::from_bytes(m.to_ascii_uppercase().as_bytes()))
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| format!("rule {}: {e}", r.id))?;
            let headers = r
                .matcher
                .headers
                .iter()
                .map(|(k, v)| {
                    http::HeaderName::from_bytes(k.to_ascii_lowercase().as_bytes())
                        .map(|n| (n, v.clone()))
                })
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| format!("rule {}: {e}", r.id))?;
            out.push(CompiledHttpRule {
                rule: r.clone(),
                path_regex,
                methods,
                headers,
                cap: Cap::new(r.max_concurrent),
            });
        }
        Ok(Self { rules: out, seed })
    }

    /// Decides the fault (if any) for request number `seq`.
    pub fn decide(
        &self,
        seq: u64,
        req: &HttpRequestView<'_>,
        external: Option<&dyn ExternalMatcher>,
    ) -> (Option<HttpDecision>, Vec<(String, Skip)>) {
        let mut rng = SplitMix64::for_decision(self.seed, STREAM_HTTP, seq);
        let mut skipped = Vec::new();
        for r in &self.rules {
            if !r.matches(req, external) {
                continue;
            }
            // Always consume the roll so later rules see a stable sequence.
            let hit = rng.roll_percent(r.rule.percentage);
            if !hit {
                skipped.push((r.rule.id.clone(), Skip::Roll));
                continue;
            }
            let Some(guard) = r.cap.try_acquire() else {
                skipped.push((r.rule.id.clone(), Skip::Cap));
                continue;
            };
            let fault = resolve_http(&r.rule.fault, &mut rng);
            return (
                Some(HttpDecision {
                    rule_id: r.rule.id.clone(),
                    fault,
                    guard,
                }),
                skipped,
            );
        }
        (None, skipped)
    }

    /// Active count for a rule id (for tests and diagnostics).
    pub fn active(&self, id: &str) -> Option<usize> {
        self.rules
            .iter()
            .find(|r| r.rule.id == id)
            .map(|r| r.cap.active())
    }
}

impl CompiledHttpRule {
    fn matches(&self, req: &HttpRequestView<'_>, external: Option<&dyn ExternalMatcher>) -> bool {
        let m = &self.rule.matcher;
        if let Some(p) = &m.path_prefix
            && !req.path.starts_with(p.as_str())
        {
            return false;
        }
        if let Some(re) = &self.path_regex
            && !re.is_match(req.path)
        {
            return false;
        }
        if !self.methods.is_empty() && !self.methods.contains(req.method) {
            return false;
        }
        for (name, value) in &self.headers {
            match req.headers.get(name) {
                Some(v) if v.as_bytes() == value.as_bytes() => {}
                _ => return false,
            }
        }
        if let Some(module) = &m.wasm {
            let Some(ext) = external else {
                return false;
            };
            match ext.matches(module, req) {
                Ok(yes) => return yes,
                Err(e) => {
                    tracing::warn!(target: "chaosproxy::wasm", rule = %self.rule.id, module = %module, error = %e,
                        "wasm matcher failed; rule skipped (fail closed: no fault)");
                    return false;
                }
            }
        }
        true
    }
}

fn resolve_http(f: &HttpFault, rng: &mut SplitMix64) -> ResolvedHttpFault {
    match f {
        HttpFault::Latency { delay, jitter } => {
            let extra = rng.up_to(u64::try_from(jitter.as_micros()).unwrap_or(u64::MAX));
            ResolvedHttpFault::Latency(*delay + Duration::from_micros(extra))
        }
        HttpFault::FirstByteDelay { delay } => ResolvedHttpFault::FirstByteDelay(*delay),
        HttpFault::Throttle { bytes_per_second } => ResolvedHttpFault::Throttle(*bytes_per_second),
        HttpFault::Reset => ResolvedHttpFault::Reset,
        HttpFault::Timeout { after } => ResolvedHttpFault::Timeout(*after),
        HttpFault::Hang { max } => ResolvedHttpFault::Hang(*max),
        HttpFault::Error { status, body } => ResolvedHttpFault::Error(*status, body.clone()),
    }
}

/// Compiled PostgreSQL rules.
#[derive(Debug)]
pub struct PgRules {
    rules: Vec<(PgRule, Option<Regex>)>,
    seed: u64,
}

/// What a PostgreSQL decision applies to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PgPhase {
    /// Connection startup (only `reject_startup` rules).
    Startup,
    /// A query (all other rules).
    Query,
}

impl PgRules {
    /// Compiles rules.
    pub fn compile(rules: &[PgRule], seed: u64) -> Result<Self, String> {
        let rules = rules
            .iter()
            .map(|r| {
                let re = match &r.matcher.query_regex {
                    Some(s) => Some(Regex::new(s).map_err(|e| format!("rule {}: {e}", r.id))?),
                    None => None,
                };
                Ok((r.clone(), re))
            })
            .collect::<Result<Vec<_>, String>>()?;
        Ok(Self { rules, seed })
    }

    /// Decides for `phase`. `seq` must be unique per decision on this listener
    /// (the proxy combines connection and query counters).
    pub fn decide(
        &self,
        seq: u64,
        phase: PgPhase,
        user: Option<&str>,
        database: Option<&str>,
        sql: Option<&str>,
    ) -> Option<(String, PgFault)> {
        let mut rng = SplitMix64::for_decision(self.seed, STREAM_PG, seq);
        for (r, re) in &self.rules {
            let is_startup = matches!(r.fault, PgFault::RejectStartup { .. });
            if is_startup != (phase == PgPhase::Startup) {
                continue;
            }
            if let Some(u) = &r.matcher.user
                && user != Some(u.as_str())
            {
                continue;
            }
            if let Some(d) = &r.matcher.database
                && database != Some(d.as_str())
            {
                continue;
            }
            if let Some(re) = re {
                match sql {
                    Some(s) if re.is_match(s) => {}
                    _ => continue,
                }
            }
            if rng.roll_percent(r.percentage) {
                return Some((r.id.clone(), r.fault.clone()));
            }
        }
        None
    }
}

#[cfg(test)]
mod tests;
