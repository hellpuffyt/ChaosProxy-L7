//! Sandboxed WebAssembly rule matchers (ABI v1), using Wasmtime.
//!
//! A module takes part in matching for rules that set `match.wasm`. It sees
//! a small text summary of the request and answers yes or no.
//!
//! # ABI v1
//!
//! The module must import **nothing** and export:
//!
//! | export | type | meaning |
//! |---|---|---|
//! | `memory` | memory | linear memory |
//! | `chaos_abi_version` | `() -> i32` | must return `1` |
//! | `chaos_alloc` | `(len: i32) -> i32` | returns a pointer to `len` writable bytes |
//! | `chaos_decide` | `(ptr: i32, len: i32) -> i32` | `1` = apply the rule, `0` = skip; anything else is an error |
//!
//! Input (UTF-8, `\n`-separated): `v1`, the method, the path (no query
//! string), then one `name: value` line per header listed in
//! `[wasm] headers` that is present. Sensitive headers are refused by config
//! validation. Input is truncated to 8 KiB.
//!
//! # Sandbox
//!
//! * No imports at all, so no WASI: no files, clock, network or randomness.
//! * Every decision runs in a fresh `Store` + instance (no state between requests).
//! * Fuel limit per decision, wall-clock limit via epoch interruption,
//!   linear-memory limit via `StoreLimits` (growth beyond it traps).
//!
//! Loading is strict: invalid bytes, imports, missing exports, a wrong ABI
//! version or an initial memory above the limit refuse startup. At runtime
//! every failure (trap, fuel or time exhaustion, bad return value) fails
//! **closed**: the rule does not apply, the request is forwarded normally,
//! and a warning is logged.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use wasmtime::{
    Config, Engine, InstancePre, Linker, Module, Store, StoreLimits, StoreLimitsBuilder,
};

use crate::config::WasmConfig;
use crate::rules::{ExternalMatcher, HttpRequestView};

/// Largest input passed to a module.
pub const MAX_INPUT: usize = 8 * 1024;
const TICK: Duration = Duration::from_millis(5);

/// Loading errors (startup refuses to continue).
#[derive(Debug, thiserror::Error)]
pub enum WasmLoadError {
    /// Wasmtime engine setup failed.
    #[error("wasm engine: {0}")]
    Engine(String),
    /// A module failed validation.
    #[error("wasm module {name:?}: {reason}")]
    Module {
        /// Module name from the config.
        name: String,
        /// What is wrong.
        reason: String,
    },
}

/// All configured modules, ready to run.
pub struct WasmMatchers {
    engine: Engine,
    modules: HashMap<String, InstancePre<StoreLimits>>,
    fuel: u64,
    max_memory: usize,
    deadline_ticks: u64,
    headers: Vec<http::HeaderName>,
    stop: Arc<AtomicBool>,
}

impl Drop for WasmMatchers {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
    }
}

impl std::fmt::Debug for WasmMatchers {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WasmMatchers")
            .field("modules", &self.modules.keys())
            .finish()
    }
}

impl WasmMatchers {
    /// Compiles and validates every module in `cfg.modules`.
    pub fn load(cfg: &WasmConfig) -> Result<Self, WasmLoadError> {
        let mut config = Config::new();
        config.consume_fuel(true);
        config.epoch_interruption(true);
        let engine = Engine::new(&config).map_err(|e| WasmLoadError::Engine(e.to_string()))?;
        let headers = cfg
            .headers
            .iter()
            .filter_map(|h| http::HeaderName::from_bytes(h.to_ascii_lowercase().as_bytes()).ok())
            .collect();
        let deadline_ticks = (cfg.timeout.as_micros() / TICK.as_micros()).max(1) as u64;
        let stop = Arc::new(AtomicBool::new(false));
        spawn_ticker(engine.clone(), Arc::clone(&stop));
        let mut this = Self {
            engine,
            modules: HashMap::new(),
            fuel: cfg.fuel,
            max_memory: cfg.max_memory_bytes,
            deadline_ticks,
            headers,
            stop,
        };
        for (name, path) in &cfg.modules {
            let bytes = std::fs::read(path).map_err(|e| WasmLoadError::Module {
                name: name.clone(),
                reason: format!("cannot read {}: {e}", path.display()),
            })?;
            this.add(name, &bytes)?;
        }
        Ok(this)
    }

    /// Validates and adds one module from `.wasm` or `.wat` bytes.
    pub fn add(&mut self, name: &str, bytes: &[u8]) -> Result<(), WasmLoadError> {
        let err = |reason: String| WasmLoadError::Module {
            name: name.to_owned(),
            reason,
        };
        let module =
            Module::new(&self.engine, bytes).map_err(|e| err(format!("invalid module: {e:#}")))?;
        if let Some(imp) = module.imports().next() {
            return Err(err(format!(
                "imports {}::{}; ABI v1 modules may not import anything (no WASI)",
                imp.module(),
                imp.name()
            )));
        }
        let linker: Linker<StoreLimits> = Linker::new(&self.engine);
        let pre = linker
            .instantiate_pre(&module)
            .map_err(|e| err(format!("cannot prepare instance: {e:#}")))?;
        // Trial run: exports, types, ABI version, memory limit.
        let mut store = self.store().map_err(&err)?;
        let instance = pre.instantiate(&mut store).map_err(|e| {
            err(format!(
                "instantiation failed (initial memory above max_memory_bytes?): {e:#}"
            ))
        })?;
        if instance.get_memory(&mut store, "memory").is_none() {
            return Err(err("must export `memory`".into()));
        }
        let version = instance
            .get_typed_func::<(), i32>(&mut store, "chaos_abi_version")
            .map_err(|e| {
                err(format!(
                    "missing or mistyped `chaos_abi_version: () -> i32`: {e}"
                ))
            })?
            .call(&mut store, ())
            .map_err(|e| err(format!("`chaos_abi_version` failed: {e:#}")))?;
        if version != 1 {
            return Err(err(format!(
                "ABI version {version} is not supported (this build supports 1)"
            )));
        }
        instance
            .get_typed_func::<i32, i32>(&mut store, "chaos_alloc")
            .map_err(|e| {
                err(format!(
                    "missing or mistyped `chaos_alloc: (i32) -> i32`: {e}"
                ))
            })?;
        instance
            .get_typed_func::<(i32, i32), i32>(&mut store, "chaos_decide")
            .map_err(|e| {
                err(format!(
                    "missing or mistyped `chaos_decide: (i32, i32) -> i32`: {e}"
                ))
            })?;
        self.modules.insert(name.to_owned(), pre);
        Ok(())
    }

    fn store(&self) -> Result<Store<StoreLimits>, String> {
        let limits = StoreLimitsBuilder::new()
            .memory_size(self.max_memory)
            .instances(1)
            .memories(1)
            .tables(4)
            .table_elements(10_000)
            .trap_on_grow_failure(true)
            .build();
        let mut store = Store::new(&self.engine, limits);
        store.limiter(|l| l);
        store.set_fuel(self.fuel).map_err(|e| e.to_string())?;
        store.set_epoch_deadline(self.deadline_ticks);
        Ok(store)
    }

    /// Builds the ABI v1 input for a request.
    pub fn input(&self, req: &HttpRequestView<'_>) -> Vec<u8> {
        let mut s = format!("v1\n{}\n{}\n", req.method, req.path);
        for h in &self.headers {
            if let Some(v) = req.headers.get(h) {
                s.push_str(h.as_str());
                s.push_str(": ");
                s.push_str(&String::from_utf8_lossy(v.as_bytes()));
                s.push('\n');
            }
        }
        let mut b = s.into_bytes();
        b.truncate(MAX_INPUT);
        b
    }

    /// Runs module `name` on raw input bytes.
    pub fn run(&self, name: &str, input: &[u8]) -> Result<bool, String> {
        let pre = self
            .modules
            .get(name)
            .ok_or_else(|| format!("unknown module {name:?}"))?;
        let mut store = self.store()?;
        let instance = pre
            .instantiate(&mut store)
            .map_err(|e| format!("instantiate: {e:#}"))?;
        let memory = instance
            .get_memory(&mut store, "memory")
            .ok_or("module has no memory")?;
        let alloc = instance
            .get_typed_func::<i32, i32>(&mut store, "chaos_alloc")
            .map_err(|e| e.to_string())?;
        let decide = instance
            .get_typed_func::<(i32, i32), i32>(&mut store, "chaos_decide")
            .map_err(|e| e.to_string())?;
        let len = i32::try_from(input.len()).map_err(|_| "input too large")?;
        let ptr = alloc
            .call(&mut store, len)
            .map_err(|e| format!("chaos_alloc: {e:#}"))?;
        let offset = usize::try_from(ptr)
            .map_err(|_| format!("chaos_alloc returned negative pointer {ptr}"))?;
        memory
            .write(&mut store, offset, input)
            .map_err(|_| format!("chaos_alloc returned out-of-bounds pointer {ptr}"))?;
        match decide
            .call(&mut store, (ptr, len))
            .map_err(|e| format!("chaos_decide: {e:#}"))?
        {
            1 => Ok(true),
            0 => Ok(false),
            other => Err(format!("chaos_decide returned {other} (expected 0 or 1)")),
        }
    }
}

impl ExternalMatcher for WasmMatchers {
    fn matches(&self, name: &str, req: &HttpRequestView<'_>) -> Result<bool, String> {
        self.run(name, &self.input(req))
    }
}

fn spawn_ticker(engine: Engine, stop: Arc<AtomicBool>) {
    let spawned = std::thread::Builder::new()
        .name("chaosproxy-wasm-epoch".into())
        .spawn(move || {
            while !stop.load(Ordering::Relaxed) {
                std::thread::sleep(TICK);
                engine.increment_epoch();
            }
        });
    if let Err(e) = spawned {
        tracing::error!(target: "chaosproxy::wasm", error = %e,
            "could not start epoch thread; wasm time limits will not fire (fuel limits still apply)");
    }
}
