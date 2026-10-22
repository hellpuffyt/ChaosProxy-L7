//! A tiny local upstream for trying ChaosProxy-L7.
//!
//! ```sh
//! cargo run --example echo_upstream            # listens on 127.0.0.1:19000
//! cargo run --example echo_upstream -- 127.0.0.1:9100
//! ```
//!
//! Every response body is `<METHOD> <path> <n> bytes`, so you can see what
//! arrived. `/big?n=<bytes>` returns that many bytes (useful with `throttle`).

use std::convert::Infallible;

use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper::service::service_fn;
use hyper::{Request, Response};
use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto;
use tokio::net::TcpListener;

async fn handle(req: Request<Incoming>) -> Result<Response<Full<Bytes>>, Infallible> {
    let method = req.method().clone();
    let path = req.uri().path().to_owned();
    if path == "/big" {
        let n = req
            .uri()
            .query()
            .and_then(|q| q.strip_prefix("n="))
            .and_then(|n| n.parse::<usize>().ok())
            .unwrap_or(1024)
            .min(64 * 1024 * 1024);
        return Ok(Response::new(Full::new(Bytes::from(vec![b'x'; n]))));
    }
    let len = match req.into_body().collect().await {
        Ok(b) => b.to_bytes().len(),
        Err(_) => 0,
    };
    Ok(Response::new(Full::new(Bytes::from(format!(
        "{method} {path} {len} bytes\n"
    )))))
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let addr = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "127.0.0.1:19000".into());
    let listener = TcpListener::bind(&addr).await?;
    eprintln!(
        "echo upstream listening on http://{}",
        listener.local_addr()?
    );
    loop {
        let (stream, _) = listener.accept().await?;
        tokio::spawn(async move {
            let _ = auto::Builder::new(TokioExecutor::new())
                .serve_connection(TokioIo::new(stream), service_fn(handle))
                .await;
        });
    }
}
