//! Header rewriting for the forwarding path (RFC 9110 §7.6.1 hop-by-hop rules).

use http::header::{self, HeaderMap, HeaderName, HeaderValue};
use std::net::SocketAddr;

/// Headers that apply to a single connection and must not be forwarded.
const HOP_BY_HOP: &[&str] = &[
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailer",
    "transfer-encoding",
    "upgrade",
    "proxy-connection",
];

/// Removes hop-by-hop headers, including any named in `Connection`.
pub fn strip_hop_by_hop(headers: &mut HeaderMap) {
    let listed: Vec<HeaderName> = headers
        .get_all(header::CONNECTION)
        .iter()
        .filter_map(|v| v.to_str().ok())
        .flat_map(|v| v.split(','))
        .filter_map(|name| HeaderName::from_bytes(name.trim().as_bytes()).ok())
        .collect();
    for name in &listed {
        headers.remove(name);
    }
    for name in HOP_BY_HOP {
        headers.remove(*name);
    }
}

/// Adds `X-Forwarded-For` (appending the peer), `X-Forwarded-Proto` and
/// `X-Forwarded-Host` (the client-facing host, if known).
pub fn add_forwarded(headers: &mut HeaderMap, peer: SocketAddr, original_host: Option<&str>) {
    let peer_ip = peer.ip().to_string();
    let xff = HeaderName::from_static("x-forwarded-for");
    let value = match headers.get(&xff).and_then(|v| v.to_str().ok()) {
        Some(prev) if !prev.is_empty() => format!("{prev}, {peer_ip}"),
        _ => peer_ip,
    };
    if let Ok(v) = HeaderValue::from_str(&value) {
        headers.insert(xff, v);
    }
    headers.insert(
        HeaderName::from_static("x-forwarded-proto"),
        HeaderValue::from_static("http"),
    );
    if let Some(host) = original_host.and_then(|h| HeaderValue::from_str(h).ok()) {
        headers.insert(HeaderName::from_static("x-forwarded-host"), host);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strips_standard_and_connection_listed_headers() {
        let mut h = HeaderMap::new();
        h.insert(
            header::CONNECTION,
            HeaderValue::from_static("keep-alive, x-secret-hop"),
        );
        h.insert("keep-alive", HeaderValue::from_static("timeout=5"));
        h.insert("x-secret-hop", HeaderValue::from_static("1"));
        h.insert(
            header::TRANSFER_ENCODING,
            HeaderValue::from_static("chunked"),
        );
        h.insert(header::UPGRADE, HeaderValue::from_static("websocket"));
        h.insert("x-keep", HeaderValue::from_static("yes"));
        strip_hop_by_hop(&mut h);
        assert_eq!(h.len(), 1);
        assert_eq!(h.get("x-keep").map(|v| v.as_bytes()), Some(&b"yes"[..]));
    }

    #[test]
    fn forwarded_for_appends() {
        let mut h = HeaderMap::new();
        h.insert("x-forwarded-for", HeaderValue::from_static("10.0.0.1"));
        add_forwarded(
            &mut h,
            "127.0.0.1:5000".parse().expect("addr"),
            Some("example.test"),
        );
        assert_eq!(
            h.get("x-forwarded-for").map(|v| v.as_bytes()),
            Some(&b"10.0.0.1, 127.0.0.1"[..])
        );
        assert_eq!(
            h.get("x-forwarded-host").map(|v| v.as_bytes()),
            Some(&b"example.test"[..])
        );
        assert_eq!(
            h.get("x-forwarded-proto").map(|v| v.as_bytes()),
            Some(&b"http"[..])
        );
    }
}
