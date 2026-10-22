//! Log redaction against a real PostgreSQL server. Kept in its own test
//! binary: `tracing`'s thread-local test subscriber does not reliably see
//! events while sibling tests hit the same callsites on other threads.
//!
//! ```sh
//! CHAOSPROXY_PG_BIN=/usr/lib/postgresql/16/bin cargo test --test pg_live_logs -- --ignored
//! ```
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod pgcommon;

use std::sync::{Arc, Mutex};
use std::time::Duration;

use pgcommon::*;

#[derive(Clone, Default)]
struct Captured(Arc<Mutex<Vec<u8>>>);
impl std::io::Write for Captured {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[tokio::test]
#[ignore = "needs PostgreSQL server binaries: set CHAOSPROXY_PG_BIN and pass --ignored"]
async fn passwords_and_sql_text_are_not_logged_by_default() {
    let cap = Captured::default();
    let w = cap.clone();
    let sub = tracing_subscriber::fmt()
        .json()
        .with_max_level(tracing::Level::DEBUG)
        .with_writer(move || w.clone())
        .finish();
    let _g = tracing::subscriber::set_default(sub);
    let c = Cluster::start();
    let marker = format!("{}{}", "SQLMARK", 777);
    let p = proxy(
        &c,
        &rule("seen", "SQLMARK", "kind = \"delay\"\ndelay = \"1ms\""),
    )
    .await;
    let db = connect(p.local_addr, &c.password, "postgres")
        .await
        .unwrap();
    db.simple_query(&format!("SELECT '{marker}'"))
        .await
        .unwrap();
    drop(db);
    p.shutdown(Duration::from_millis(300)).await;
    let logs = String::from_utf8(cap.0.lock().unwrap().clone()).unwrap();
    assert!(
        logs.contains("\"rule\":\"seen\""),
        "decision logged: {logs}"
    );
    assert!(!logs.contains(&marker), "SQL text leaked: {logs}");
    assert!(!logs.contains(&c.password), "password leaked: {logs}");
}
