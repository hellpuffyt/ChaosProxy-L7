//! `chaosproxy` command-line entry point.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use chaosproxy::config::{Config, Mode};
use clap::{Parser, Subcommand};
use tokio::sync::Semaphore;

#[derive(Parser)]
#[command(
    name = "chaosproxy",
    version,
    about = "Loopback-by-default Layer 7 fault-injection proxy (HTTP/1.1, h2c, PostgreSQL)"
)]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Validate a config file and print what it would do.
    Check {
        /// Config file.
        #[arg(short, long)]
        config: PathBuf,
        /// Also allow non-loopback listen addresses (the config must agree).
        #[arg(long)]
        allow_non_loopback: bool,
    },
    /// Run the proxy until Ctrl-C / SIGTERM.
    Run {
        /// Config file.
        #[arg(short, long)]
        config: PathBuf,
        /// Allow non-loopback listen addresses; the config must ALSO set `allow_non_loopback = true`.
        #[arg(long)]
        allow_non_loopback: bool,
        /// Log fault decisions but forward everything untouched (forces `mode = "observe"`).
        #[arg(long)]
        dry_run: bool,
        /// Human-readable logs instead of JSON lines.
        #[arg(long)]
        pretty: bool,
    },
}

fn main() -> std::process::ExitCode {
    let cli = Cli::parse();
    match cli.cmd {
        Cmd::Check {
            config,
            allow_non_loopback,
        } => match check(&config, allow_non_loopback) {
            Ok(()) => std::process::ExitCode::SUCCESS,
            Err(e) => {
                eprintln!("error: {e:#}");
                std::process::ExitCode::from(2)
            }
        },
        Cmd::Run {
            config,
            allow_non_loopback,
            dry_run,
            pretty,
        } => {
            init_logging(pretty);
            let rt = match tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
            {
                Ok(rt) => rt,
                Err(e) => {
                    eprintln!("error: cannot start runtime: {e}");
                    return std::process::ExitCode::FAILURE;
                }
            };
            match rt.block_on(run(&config, allow_non_loopback, dry_run)) {
                Ok(()) => std::process::ExitCode::SUCCESS,
                Err(e) => {
                    eprintln!("error: {e:#}");
                    std::process::ExitCode::from(2)
                }
            }
        }
    }
}

fn init_logging(pretty: bool) {
    let filter = tracing_subscriber::EnvFilter::try_from_env("CHAOSPROXY_LOG")
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    let builder = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stderr);
    if pretty {
        builder.init();
    } else {
        builder
            .json()
            .flatten_event(true)
            .with_current_span(false)
            .init();
    }
}

fn check(path: &std::path::Path, allow_non_loopback: bool) -> anyhow::Result<()> {
    let cfg = Config::load(path, allow_non_loopback)?;
    #[cfg(feature = "wasm")]
    let wasm = chaosproxy::wasm::WasmMatchers::load(&cfg.wasm)?;
    println!("config OK: {}", path.display());
    println!(
        "mode: {:?}, seed: {}, max_concurrency: {}",
        cfg.mode, cfg.seed, cfg.max_concurrency
    );
    if let Some(h) = &cfg.http {
        println!(
            "http: {} -> {} ({} rules)",
            h.listen,
            h.upstream,
            h.rules.len()
        );
        for r in &h.rules {
            println!("  - {} ({}%): {:?}", r.id, r.percentage, r.fault);
        }
    }
    if let Some(p) = &cfg.postgres {
        println!(
            "postgres: {} -> {} ({} rules)",
            p.listen,
            p.upstream,
            p.rules.len()
        );
        for r in &p.rules {
            println!("  - {} ({}%): {:?}", r.id, r.percentage, r.fault);
        }
    }
    #[cfg(feature = "wasm")]
    println!("wasm modules: {wasm:?}");
    Ok(())
}

async fn run(
    path: &std::path::Path,
    allow_non_loopback: bool,
    dry_run: bool,
) -> anyhow::Result<()> {
    let mut cfg = Config::load(path, allow_non_loopback)?;
    if dry_run {
        cfg.mode = Mode::Observe;
    }
    if cfg.mode == Mode::Observe {
        tracing::warn!(target: "chaosproxy", "observe mode: faults are logged, not injected");
    }
    let limiter = Arc::new(Semaphore::new(cfg.max_concurrency));

    #[cfg(feature = "wasm")]
    let external: Option<Arc<dyn chaosproxy::rules::ExternalMatcher>> =
        if cfg.wasm.modules.is_empty() {
            None
        } else {
            Some(Arc::new(
                chaosproxy::wasm::WasmMatchers::load(&cfg.wasm).context("loading wasm modules")?,
            ))
        };
    #[cfg(not(feature = "wasm"))]
    let external: Option<Arc<dyn chaosproxy::rules::ExternalMatcher>> = None;

    let mut grace = Duration::from_secs(10);
    let http = match cfg.http.clone() {
        Some(h) => {
            grace = h.shutdown_grace;
            let deps = chaosproxy::http::HttpDeps {
                mode: cfg.mode,
                seed: cfg.seed,
                limiter: Arc::clone(&limiter),
                external,
            };
            Some(
                chaosproxy::http::spawn(h, deps)
                    .await
                    .context("starting http listener")?,
            )
        }
        None => None,
    };
    let pg = match cfg.postgres.clone() {
        Some(p) => Some(
            chaosproxy::pg::spawn(p, cfg.mode, cfg.seed, Arc::clone(&limiter), grace)
                .await
                .context("starting postgres listener")?,
        ),
        None => None,
    };

    shutdown_signal().await;
    tracing::info!(target: "chaosproxy", "shutting down");
    if let Some(h) = http {
        h.shutdown().await;
    }
    if let Some(p) = pg {
        p.shutdown(grace).await;
    }
    Ok(())
}

async fn shutdown_signal() {
    let ctrl_c = async {
        let _ = tokio::signal::ctrl_c().await;
    };
    #[cfg(unix)]
    let term = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut s) => {
                s.recv().await;
            }
            Err(_) => std::future::pending::<()>().await,
        }
    };
    #[cfg(not(unix))]
    let term = std::future::pending::<()>();
    tokio::select! {
        _ = ctrl_c => {}
        _ = term => {}
    }
}
