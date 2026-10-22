//! The real `chaosproxy` binary: `check`, startup refusal, and a full run
//! with JSON logs and signal-driven shutdown.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod common;

use std::io::{BufRead, BufReader};
use std::process::{Command, Stdio};
use std::time::Duration;

use common::*;

const BIN: &str = env!("CARGO_BIN_EXE_chaosproxy");

fn temp_config(name: &str, text: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("chaosproxy-cli-{}-{name}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let p = dir.join("chaos.toml");
    std::fs::write(&p, text).unwrap();
    p
}

#[test]
fn check_accepts_the_shipped_examples() {
    // chaos.toml uses a WebAssembly matcher, so it only validates in builds
    // with the default `wasm` feature; without it, `check` must say why.
    let with_wasm = ["examples/chaos.toml", "examples/postgres.toml"];
    let without_wasm = ["examples/postgres.toml"];
    let ok = if cfg!(feature = "wasm") {
        &with_wasm[..]
    } else {
        &without_wasm[..]
    };
    for cfg in ok {
        let out = Command::new(BIN)
            .args(["check", "--config", cfg])
            .output()
            .unwrap();
        assert!(
            out.status.success(),
            "{cfg}: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        assert!(String::from_utf8_lossy(&out.stdout).contains("config OK"));
    }
    if !cfg!(feature = "wasm") {
        let out = Command::new(BIN)
            .args(["check", "--config", "examples/chaos.toml"])
            .output()
            .unwrap();
        assert_eq!(out.status.code(), Some(2));
        assert!(String::from_utf8_lossy(&out.stderr).contains("no `wasm` feature"));
    }
}

#[test]
fn check_rejects_non_loopback_with_exit_code_2_and_a_fix() {
    let p = temp_config(
        "wide",
        "upstream_allowlist = [\"127.0.0.1:9000\"]\n[http]\nlisten = \"0.0.0.0:8080\"\nupstream = \"http://127.0.0.1:9000\"\n",
    );
    let out = Command::new(BIN)
        .args(["check", "--config"])
        .arg(&p)
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(2));
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(
        err.contains("allow_non_loopback = true") && err.contains("--allow-non-loopback"),
        "{err}"
    );
}

#[test]
fn missing_config_file_is_reported() {
    let out = Command::new(BIN)
        .args(["run", "--config", "/definitely/missing.toml"])
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(2));
    assert!(String::from_utf8_lossy(&out.stderr).contains("cannot read config"));
}

#[cfg(unix)]
#[tokio::test]
async fn run_serves_traffic_logs_json_and_stops_on_sigterm() {
    let (up, _) = upstream().await;
    let p = temp_config(
        "run",
        &format!(
            "upstream_allowlist = [\"{up}\"]\n[http]\nlisten = \"127.0.0.1:0\"\nupstream = \"http://{up}\"\n\
             [[http.rules]]\nid = \"teapot\"\n[http.rules.match]\npath_prefix = \"/tea\"\n[http.rules.fault]\nkind = \"error\"\nstatus = 418\n"
        ),
    );
    let mut child = Command::new(BIN)
        .args(["run", "--config"])
        .arg(&p)
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let stderr = child.stderr.take().unwrap();
    let (tx, rx) = std::sync::mpsc::channel::<String>();
    std::thread::spawn(move || {
        for line in BufReader::new(stderr).lines().map_while(Result::ok) {
            let _ = tx.send(line);
        }
    });
    let mut addr = None;
    let mut seen = Vec::new();
    while addr.is_none() {
        let line = rx
            .recv_timeout(Duration::from_secs(10))
            .expect("startup log line");
        let v: serde_json_lite::Value = serde_json_lite::parse(&line);
        if v.get("message") == Some("http listener ready") {
            addr = v.get("local_addr").map(str::to_owned);
        }
        seen.push(line);
    }
    let addr: std::net::SocketAddr = addr.unwrap().parse().unwrap();
    let c = client();
    assert_eq!(get(&c, addr, "/tea").await.0, http::StatusCode::IM_A_TEAPOT);
    assert_eq!(get(&c, addr, "/coffee").await.0, http::StatusCode::OK);

    // SIGTERM via the `kill` utility (no unsafe libc calls in tests).
    let status = Command::new("kill")
        .args(["-TERM", &child.id().to_string()])
        .status()
        .unwrap();
    assert!(status.success());
    let exit = tokio::task::spawn_blocking(move || child.wait())
        .await
        .unwrap()
        .unwrap();
    assert!(exit.success(), "graceful exit expected, got {exit:?}");
    while let Ok(l) = rx.recv_timeout(Duration::from_millis(200)) {
        seen.push(l);
    }
    let all = seen.join("\n");
    assert!(all.contains("\"rule\":\"teapot\""), "{all}");
    assert!(all.contains("shutting down"), "{all}");
    for line in &seen {
        assert!(line.starts_with('{'), "every log line is JSON: {line}");
    }
}

/// Minimal flat-JSON field reader so the test needs no extra dependency.
mod serde_json_lite {
    pub struct Value(String);
    pub fn parse(s: &str) -> Value {
        Value(s.to_owned())
    }
    impl Value {
        pub fn get(&self, key: &str) -> Option<&str> {
            let pat = format!("\"{key}\":\"");
            let start = self.0.find(&pat)? + pat.len();
            let end = self.0[start..].find('"')? + start;
            Some(&self.0[start..end])
        }
    }
}
