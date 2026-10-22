//! Throwaway PostgreSQL cluster + proxy helpers for the live tests.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic, dead_code)]

use std::path::PathBuf;
use std::process::Command;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use chaosproxy::config::Config;
use tokio_postgres::{Client, NoTls};

pub struct Cluster {
    bin: PathBuf,
    dir: PathBuf,
    port: u16,
    pub password: String,
}

static NEXT: AtomicU32 = AtomicU32::new(0);

impl Cluster {
    pub fn start() -> Self {
        let bin = PathBuf::from(
            std::env::var("CHAOSPROXY_PG_BIN")
                .expect("CHAOSPROXY_PG_BIN must point at the PostgreSQL bin directory"),
        );
        let dir = std::env::temp_dir().join(format!(
            "chaosproxy-pg-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::SeqCst)
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let password = format!("pw{}", std::process::id() ^ 0x5a5a);
        let pwfile = dir.join("pwfile");
        std::fs::write(&pwfile, &password).unwrap();
        let data = dir.join("data");
        let out = Command::new(bin.join("initdb"))
            .args([
                "-D",
                data.to_str().unwrap(),
                "-U",
                "postgres",
                "--auth=scram-sha-256",
                "--no-sync",
            ])
            .arg(format!("--pwfile={}", pwfile.display()))
            .output()
            .expect("run initdb");
        assert!(
            out.status.success(),
            "initdb failed: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        let port = {
            let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            l.local_addr().unwrap().port()
        };
        let opts = format!(
            "-p {port} -c listen_addresses=127.0.0.1 -c unix_socket_directories='' -c fsync=off"
        );
        let out = Command::new(bin.join("pg_ctl"))
            .args(["-D", data.to_str().unwrap(), "-o", &opts, "-l"])
            .arg(dir.join("log"))
            .args(["-w", "start"])
            .output()
            .expect("run pg_ctl");
        assert!(
            out.status.success(),
            "pg_ctl start failed: {}\n{}",
            String::from_utf8_lossy(&out.stderr),
            std::fs::read_to_string(dir.join("log")).unwrap_or_default()
        );
        Self {
            bin,
            dir,
            port,
            password,
        }
    }

    pub fn addr(&self) -> String {
        format!("127.0.0.1:{}", self.port)
    }
}

impl Drop for Cluster {
    fn drop(&mut self) {
        let _ = Command::new(self.bin.join("pg_ctl"))
            .args([
                "-D",
                self.dir.join("data").to_str().unwrap_or(""),
                "-m",
                "immediate",
                "stop",
            ])
            .output();
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

pub async fn proxy(cluster: &Cluster, extra: &str) -> chaosproxy::pg::RunningPg {
    let text = format!(
        "seed = 3\nupstream_allowlist = [\"{a}\"]\n[postgres]\nlisten = \"127.0.0.1:0\"\nupstream = \"{a}\"\n{extra}",
        a = cluster.addr()
    );
    let cfg = Config::parse(&text, false).unwrap();
    chaosproxy::pg::spawn(
        cfg.postgres.unwrap(),
        cfg.mode,
        cfg.seed,
        Arc::new(tokio::sync::Semaphore::new(64)),
        Duration::from_secs(1),
    )
    .await
    .unwrap()
}

pub async fn connect(
    addr: std::net::SocketAddr,
    password: &str,
    user: &str,
) -> Result<Client, tokio_postgres::Error> {
    let conn_str = format!(
        "host=127.0.0.1 port={} user={user} password={password} dbname=postgres sslmode=disable connect_timeout=5",
        addr.port()
    );
    let (client, connection) = tokio_postgres::connect(&conn_str, NoTls).await?;
    tokio::spawn(async move {
        let _ = connection.await;
    });
    Ok(client)
}

pub fn rule(id: &str, regex: &str, fault: &str) -> String {
    format!(
        "[[postgres.rules]]\nid = \"{id}\"\n[postgres.rules.match]\nquery_regex = '{regex}'\n[postgres.rules.fault]\n{fault}\n"
    )
}

pub fn sqlstate(e: &tokio_postgres::Error) -> Option<String> {
    e.as_db_error().map(|d| d.code().code().to_owned())
}
