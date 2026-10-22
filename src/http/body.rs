//! Response body types: boxing, resource guards, and bandwidth throttling.

use std::any::Any;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use bytes::Bytes;
use http_body::{Body, Frame, SizeHint};
use http_body_util::BodyExt;
use http_body_util::combinators::BoxBody;
use tokio::sync::mpsc;

use crate::BoxError;

/// Body type returned to clients.
pub type ProxyBody = BoxBody<Bytes, BoxError>;

/// A complete in-memory body.
pub fn full(data: impl Into<Bytes>) -> ProxyBody {
    http_body_util::Full::new(data.into())
        .map_err(|never| match never {})
        .boxed()
}

/// Wraps a body and keeps `guards` alive until the body is fully sent or
/// dropped (for example when the client disconnects). Used to hold the
/// global concurrency permit and the rule's cap slot for the whole response.
pub struct Guarded {
    inner: ProxyBody,
    _guards: Vec<Box<dyn Any + Send + Sync>>,
}

impl Guarded {
    /// Creates a guarded body.
    pub fn new(inner: ProxyBody, guards: Vec<Box<dyn Any + Send + Sync>>) -> Self {
        Self {
            inner,
            _guards: guards,
        }
    }
}

impl Body for Guarded {
    type Data = Bytes;
    type Error = BoxError;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, BoxError>>> {
        Pin::new(&mut self.inner).poll_frame(cx)
    }

    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }

    fn size_hint(&self) -> SizeHint {
        self.inner.size_hint()
    }
}

/// A body fed by a channel (used by the throttle task).
struct ChannelBody {
    rx: mpsc::Receiver<Result<Frame<Bytes>, BoxError>>,
}

impl Body for ChannelBody {
    type Data = Bytes;
    type Error = BoxError;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, BoxError>>> {
        self.rx.poll_recv(cx)
    }
}

/// Re-emits `inner` at no more than `bytes_per_second`.
///
/// Data frames are split into chunks of about 1/20 s worth of bytes; after each
/// chunk the task sleeps for `chunk_len / bytes_per_second`. The channel has
/// capacity 1, so the task only reads from the upstream as fast as the client
/// accepts data (backpressure is preserved). If the client goes away the
/// receiver is dropped, the send fails and the task stops, dropping the
/// upstream body.
pub fn throttle(mut inner: ProxyBody, bytes_per_second: u64) -> ProxyBody {
    let (tx, rx) = mpsc::channel(1);
    let chunk = usize::try_from((bytes_per_second / 20).max(1)).unwrap_or(usize::MAX);
    tokio::spawn(async move {
        loop {
            // Stop as soon as the client is gone, even if the upstream body is
            // stalled; dropping `inner` releases the upstream connection.
            let frame = tokio::select! {
                f = inner.frame() => f,
                _ = tx.closed() => return,
            };
            let Some(frame) = frame else { return };
            let frame = match frame {
                Ok(f) => f,
                Err(e) => {
                    let _ = tx.send(Err(e)).await;
                    return;
                }
            };
            let data = match frame.into_data() {
                Ok(d) => d,
                Err(other) => {
                    if tx.send(Ok(other)).await.is_err() {
                        return;
                    }
                    continue;
                }
            };
            let mut rest = data;
            while !rest.is_empty() {
                let n = rest.len().min(chunk);
                let piece = rest.split_to(n);
                if tx.send(Ok(Frame::data(piece))).await.is_err() {
                    return;
                }
                let secs = n as f64 / bytes_per_second as f64;
                tokio::time::sleep(Duration::from_secs_f64(secs)).await;
            }
        }
    });
    ChannelBody { rx }.boxed()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    /// A body that never produces a frame and records when it is dropped.
    struct Stalled(Arc<AtomicBool>);
    impl Drop for Stalled {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }
    impl Body for Stalled {
        type Data = Bytes;
        type Error = BoxError;
        fn poll_frame(
            self: Pin<&mut Self>,
            _: &mut Context<'_>,
        ) -> Poll<Option<Result<Frame<Bytes>, BoxError>>> {
            Poll::Pending
        }
    }

    #[tokio::test]
    async fn throttle_releases_a_stalled_upstream_when_the_client_goes_away() {
        let dropped = Arc::new(AtomicBool::new(false));
        let body = throttle(Stalled(Arc::clone(&dropped)).boxed(), 1000);
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(!dropped.load(Ordering::SeqCst));
        drop(body); // client disconnects
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(
            dropped.load(Ordering::SeqCst),
            "upstream body must be dropped"
        );
    }

    #[tokio::test]
    async fn throttle_preserves_every_byte() {
        let data = Bytes::from(vec![9u8; 10_000]);
        let body = throttle(full(data.clone()), 1_000_000);
        let out = body.collect().await.expect("collect").to_bytes();
        assert_eq!(out, data);
    }
}
