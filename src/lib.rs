//! ChaosProxy-L7: a loopback-by-default fault-injection proxy for testing how
//! applications cope with misbehaving HTTP services and PostgreSQL servers.
//!
//! The crate is split into a forwarding path and a fault layer that never
//! share responsibilities:
//!
//! * [`http`] forwards HTTP/1.1 and h2c traffic and asks [`rules`] whether a
//!   request should be disturbed;
//! * [`pg`] relays the PostgreSQL wire protocol frame by frame (see
//!   [`pg::codec`]) and applies query-level faults;
//! * [`rules`] holds the deterministic decision logic (matching, percentage
//!   rolls with a seeded generator, per-rule caps);
//! * [`wasm`] (feature `wasm`) lets a sandboxed WebAssembly module take part
//!   in matching.
//!
//! Safety rails live in [`config`] (loopback-only binding unless explicitly
//! allowed twice, an upstream allowlist) and [`redact`] (what may be logged).
//!
//! **Never point ChaosProxy at production traffic.** It exists to break
//! things on purpose.

#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used, clippy::panic))]
#![warn(missing_docs)]

pub mod config;
pub mod http;
pub mod pg;
pub mod redact;
pub mod rng;
pub mod rules;
#[cfg(feature = "wasm")]
pub mod wasm;

/// Boxed error type used across the crate's async boundaries.
pub type BoxError = Box<dyn std::error::Error + Send + Sync + 'static>;
