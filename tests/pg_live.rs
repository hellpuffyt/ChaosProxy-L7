//! PostgreSQL proxy against a REAL throwaway server.
//!
//! Ignored by default. Run with the directory holding `initdb`, `pg_ctl` and
//! `postgres`:
//!
//! ```sh
//! CHAOSPROXY_PG_BIN=/usr/lib/postgresql/16/bin cargo test --test pg_live -- --ignored
//! ```
//!
//! Each test creates its own cluster in a temporary directory (SCRAM
//! password auth, TCP on a free loopback port, no Unix socket) and stops it
//! with `pg_ctl stop -m immediate` when the test ends, pass or fail.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod pgcommon;

use std::sync::Arc;
use std::time::{Duration, Instant};

use chaosproxy::config::Config;
use pgcommon::*;

#[tokio::test]
#[ignore = "needs PostgreSQL server binaries: set CHAOSPROXY_PG_BIN and pass --ignored"]
async fn passthrough_with_scram_auth_simple_and_extended_protocol() {
    let c = Cluster::start();
    let p = proxy(&c, "").await;
    let db = connect(p.local_addr, &c.password, "postgres")
        .await
        .expect("SCRAM auth through the proxy");
    db.batch_execute("CREATE TABLE t (id int PRIMARY KEY, name text)")
        .await
        .unwrap();
    let stmt = db.prepare("INSERT INTO t VALUES ($1, $2)").await.unwrap();
    for i in 0..50 {
        db.execute(&stmt, &[&i, &format!("row{i}")]).await.unwrap();
    }
    let rows = db
        .query("SELECT id, name FROM t WHERE id >= $1 ORDER BY id", &[&10])
        .await
        .unwrap();
    assert_eq!(rows.len(), 40);
    let first: String = rows[0].get(1);
    assert_eq!(first, "row10");
    let simple = db.simple_query("SELECT count(*) FROM t").await.unwrap();
    assert!(simple.iter().any(
        |m| matches!(m, tokio_postgres::SimpleQueryMessage::Row(r) if r.get(0) == Some("50"))
    ));
    p.shutdown(Duration::from_millis(200)).await;
}

#[tokio::test]
#[ignore = "needs PostgreSQL server binaries: set CHAOSPROXY_PG_BIN and pass --ignored"]
async fn injected_error_on_simple_query_is_not_executed_and_session_survives() {
    let c = Cluster::start();
    let direct = connect(c.addr().parse().unwrap(), &c.password, "postgres")
        .await
        .unwrap();
    direct
        .batch_execute("CREATE TABLE orders (id int)")
        .await
        .unwrap();
    let p = proxy(
        &c,
        &rule(
            "ser",
            "(?i)^insert into orders",
            "kind = \"error\"\nsqlstate = \"40001\"\nmessage = \"injected serialization failure\"",
        ),
    )
    .await;
    let db = connect(p.local_addr, &c.password, "postgres")
        .await
        .unwrap();
    let e = db
        .batch_execute("INSERT INTO orders VALUES (1)")
        .await
        .expect_err("injected");
    assert_eq!(sqlstate(&e).as_deref(), Some("40001"));
    let n: i64 = direct
        .query_one("SELECT count(*) FROM orders", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(n, 0, "the faulted statement must not reach the server");
    let one: i32 = db.query_one("SELECT 1", &[]).await.unwrap().get(0);
    assert_eq!(one, 1, "session still usable after the injected error");
    p.shutdown(Duration::from_millis(200)).await;
}

#[tokio::test]
#[ignore = "needs PostgreSQL server binaries: set CHAOSPROXY_PG_BIN and pass --ignored"]
async fn injected_error_on_extended_execute_and_pipelined_neighbours_are_unaffected() {
    let c = Cluster::start();
    let direct = connect(c.addr().parse().unwrap(), &c.password, "postgres")
        .await
        .unwrap();
    direct
        .batch_execute("CREATE TABLE orders (id int)")
        .await
        .unwrap();
    let p = proxy(
        &c,
        &rule(
            "ser",
            "(?i)^insert into orders",
            "kind = \"error\"\nsqlstate = \"40P01\"",
        ),
    )
    .await;
    let db = connect(p.local_addr, &c.password, "postgres")
        .await
        .unwrap();
    let ins = db
        .prepare("INSERT INTO orders VALUES ($1)")
        .await
        .expect("Parse is not faulted");
    let sel = db.prepare("SELECT $1::int + 1").await.unwrap();
    // tokio-postgres pipelines these three requests on one connection.
    let (a, b, d) = tokio::join!(
        db.query_one(&sel, &[&1i32]),
        db.execute(&ins, &[&7i32]),
        db.query_one(&sel, &[&41i32])
    );
    assert_eq!(a.unwrap().get::<_, i32>(0), 2);
    assert_eq!(sqlstate(&b.expect_err("faulted")).as_deref(), Some("40P01"));
    assert_eq!(d.unwrap().get::<_, i32>(0), 42);
    let n: i64 = direct
        .query_one("SELECT count(*) FROM orders", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(n, 0);
    p.shutdown(Duration::from_millis(200)).await;
}

#[tokio::test]
#[ignore = "needs PostgreSQL server binaries: set CHAOSPROXY_PG_BIN and pass --ignored"]
async fn delay_fault_holds_the_query() {
    let c = Cluster::start();
    let p = proxy(
        &c,
        &rule("slow", "^SELECT 42", "kind = \"delay\"\ndelay = \"400ms\""),
    )
    .await;
    let db = connect(p.local_addr, &c.password, "postgres")
        .await
        .unwrap();
    let t = Instant::now();
    let v: i32 = db.query_one("SELECT 42", &[]).await.unwrap().get(0);
    assert_eq!(v, 42);
    assert!(t.elapsed() >= Duration::from_millis(400));
    let t = Instant::now();
    db.query_one("SELECT 1", &[]).await.unwrap();
    assert!(
        t.elapsed() < Duration::from_millis(300),
        "non-matching queries are not delayed"
    );
    p.shutdown(Duration::from_millis(200)).await;
}

#[tokio::test]
#[ignore = "needs PostgreSQL server binaries: set CHAOSPROXY_PG_BIN and pass --ignored"]
async fn drop_mid_result_closes_the_session_after_n_rows() {
    let c = Cluster::start();
    let p = proxy(
        &c,
        &rule(
            "drop",
            "generate_series",
            "kind = \"drop_mid_result\"\nafter_rows = 5",
        ),
    )
    .await;
    let db = connect(p.local_addr, &c.password, "postgres")
        .await
        .unwrap();
    let e = db
        .simple_query("SELECT * FROM generate_series(1, 1000)")
        .await
        .expect_err("dropped");
    assert!(
        e.as_db_error().is_none(),
        "a dropped connection is not a server error: {e}"
    );
    assert!(db.is_closed() || db.simple_query("SELECT 1").await.is_err());
    // New sessions are unaffected for non-matching queries.
    let db2 = connect(p.local_addr, &c.password, "postgres")
        .await
        .unwrap();
    db2.simple_query("SELECT 1").await.unwrap();
    p.shutdown(Duration::from_millis(200)).await;
}

#[tokio::test]
#[ignore = "needs PostgreSQL server binaries: set CHAOSPROXY_PG_BIN and pass --ignored"]
async fn reject_startup_and_observe_mode() {
    let c = Cluster::start();
    let r = "[[postgres.rules]]\nid = \"full\"\n[postgres.rules.match]\nuser = \"postgres\"\n[postgres.rules.fault]\nkind = \"reject_startup\"\nsqlstate = \"53300\"\n";
    let p = proxy(&c, r).await;
    let e = connect(p.local_addr, &c.password, "postgres")
        .await
        .expect_err("rejected");
    assert_eq!(sqlstate(&e).as_deref(), Some("53300"));
    p.shutdown(Duration::from_millis(200)).await;

    // Observe mode: same rule, no effect.
    let text = format!(
        "mode = \"observe\"\nupstream_allowlist = [\"{a}\"]\n[postgres]\nlisten = \"127.0.0.1:0\"\nupstream = \"{a}\"\n{r}",
        a = c.addr()
    );
    let cfg = Config::parse(&text, false).unwrap();
    let p = chaosproxy::pg::spawn(
        cfg.postgres.unwrap(),
        cfg.mode,
        0,
        Arc::new(tokio::sync::Semaphore::new(8)),
        Duration::from_secs(1),
    )
    .await
    .unwrap();
    let db = connect(p.local_addr, &c.password, "postgres")
        .await
        .expect("observe mode does not reject");
    db.simple_query("SELECT 1").await.unwrap();
    p.shutdown(Duration::from_millis(200)).await;
}
