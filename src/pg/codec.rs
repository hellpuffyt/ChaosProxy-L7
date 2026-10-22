//! PostgreSQL wire protocol (v3) framing — only what the proxy needs.
//!
//! The proxy does not interpret most messages; it relays them as opaque
//! frames. It parses: the untagged startup-phase messages (`SSLRequest`,
//! `GSSENCRequest`, `CancelRequest`, `StartupMessage`), simple `Query` (`Q`),
//! extended-protocol `Parse` (`P`), `Bind` (`B`), `Execute` (`E`), `Sync`
//! (`S`), `Terminate` (`X`) from the client, and `DataRow` (`D`),
//! `ReadyForQuery` (`Z`) from the server. It constructs `ErrorResponse` and
//! `ReadyForQuery`. Reference: PostgreSQL documentation, "Frontend/Backend
//! Protocol", Message Formats.

use bytes::{BufMut, Bytes, BytesMut};
use tokio::io::{AsyncRead, AsyncReadExt};

/// Protocol version 3.0 as sent in `StartupMessage`.
pub const PROTOCOL_V3: i32 = 196_608;
/// `SSLRequest` magic.
pub const SSL_REQUEST: i32 = 80_877_103;
/// `GSSENCRequest` magic.
pub const GSSENC_REQUEST: i32 = 80_877_104;
/// `CancelRequest` magic.
pub const CANCEL_REQUEST: i32 = 80_877_102;

/// Framing errors. Any of these closes the connection.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum CodecError {
    /// Declared length below the protocol minimum.
    #[error("message length {0} is smaller than the 4-byte length field")]
    TooShort(i32),
    /// Declared length above `max_message_bytes`.
    #[error("message length {len} exceeds max_message_bytes {max}")]
    TooLong {
        /// Declared length.
        len: i64,
        /// Configured maximum.
        max: u32,
    },
    /// Unknown startup-phase request code.
    #[error("unsupported startup request code {0}")]
    UnknownStartup(i32),
    /// A message body did not have the documented structure.
    #[error("malformed {0} message")]
    Malformed(&'static str),
    /// Peer closed mid-message.
    #[error("connection closed in the middle of a message")]
    Truncated,
    /// I/O error.
    #[error("i/o: {0}")]
    Io(String),
}

impl From<std::io::Error> for CodecError {
    fn from(e: std::io::Error) -> Self {
        if e.kind() == std::io::ErrorKind::UnexpectedEof {
            Self::Truncated
        } else {
            Self::Io(e.to_string())
        }
    }
}

/// A tagged protocol message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Frame {
    /// Message type byte.
    pub tag: u8,
    /// Body without tag and length.
    pub payload: Bytes,
}

impl Frame {
    /// Serialises the frame back to wire format.
    pub fn encode(&self) -> Bytes {
        encode(self.tag, &self.payload)
    }
}

/// First message of a connection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Startup {
    /// `SSLRequest` (client wants TLS).
    Ssl,
    /// `GSSENCRequest` (client wants GSSAPI encryption).
    GssEnc,
    /// `CancelRequest`; `raw` is the complete message to relay.
    Cancel {
        /// Wire bytes.
        raw: Bytes,
    },
    /// `StartupMessage`.
    Startup {
        /// Wire bytes to relay to the upstream.
        raw: Bytes,
        /// Parameters (user, database, ...).
        params: Vec<(String, String)>,
    },
}

impl Startup {
    /// Looks up a startup parameter.
    pub fn param(&self, key: &str) -> Option<&str> {
        match self {
            Self::Startup { params, .. } => params
                .iter()
                .find(|(k, _)| k == key)
                .map(|(_, v)| v.as_str()),
            _ => None,
        }
    }
}

fn check_len(len: i32, header: i32, max: u32) -> Result<usize, CodecError> {
    if len < header {
        return Err(CodecError::TooShort(len));
    }
    if i64::from(len) > i64::from(max) {
        return Err(CodecError::TooLong {
            len: i64::from(len),
            max,
        });
    }
    usize::try_from(len - header).map_err(|_| CodecError::TooShort(len))
}

/// Reads the untagged first message. `Ok(None)` means the client closed
/// before sending anything.
pub async fn read_startup<R: AsyncRead + Unpin>(
    r: &mut R,
    max: u32,
) -> Result<Option<Startup>, CodecError> {
    let mut len_buf = [0u8; 4];
    match r.read_exact(&mut len_buf).await {
        Ok(_) => {}
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e.into()),
    }
    let len = i32::from_be_bytes(len_buf);
    if len < 8 {
        return Err(CodecError::TooShort(len));
    }
    let body_len = check_len(len, 4, max.min(10_000))?;
    let mut body = vec![0u8; body_len];
    r.read_exact(&mut body).await?;
    let code = i32::from_be_bytes([body[0], body[1], body[2], body[3]]);
    let mut raw = BytesMut::with_capacity(4 + body_len);
    raw.put_slice(&len_buf);
    raw.put_slice(&body);
    let raw = raw.freeze();
    match code {
        SSL_REQUEST if body_len == 4 => Ok(Some(Startup::Ssl)),
        GSSENC_REQUEST if body_len == 4 => Ok(Some(Startup::GssEnc)),
        CANCEL_REQUEST if body_len == 12 => Ok(Some(Startup::Cancel { raw })),
        PROTOCOL_V3 => {
            let params = parse_params(&body[4..])?;
            Ok(Some(Startup::Startup { raw, params }))
        }
        SSL_REQUEST | GSSENC_REQUEST | CANCEL_REQUEST => {
            Err(CodecError::Malformed("startup request"))
        }
        other => Err(CodecError::UnknownStartup(other)),
    }
}

fn parse_params(mut b: &[u8]) -> Result<Vec<(String, String)>, CodecError> {
    let mut out = Vec::new();
    loop {
        let (k, rest) = cstr(b).ok_or(CodecError::Malformed("StartupMessage"))?;
        if k.is_empty() {
            if !rest.is_empty() {
                return Err(CodecError::Malformed("StartupMessage"));
            }
            return Ok(out);
        }
        let (v, rest) = cstr(rest).ok_or(CodecError::Malformed("StartupMessage"))?;
        out.push((k.to_owned(), v.to_owned()));
        b = rest;
    }
}

/// Splits a NUL-terminated UTF-8 string off the front of `b`.
fn cstr(b: &[u8]) -> Option<(&str, &[u8])> {
    let nul = b.iter().position(|&c| c == 0)?;
    let s = std::str::from_utf8(&b[..nul]).ok()?;
    Some((s, &b[nul + 1..]))
}

/// Reads one tagged message. `Ok(None)` = clean EOF between messages.
pub async fn read_frame<R: AsyncRead + Unpin>(
    r: &mut R,
    max: u32,
) -> Result<Option<Frame>, CodecError> {
    let mut tag = [0u8; 1];
    match r.read_exact(&mut tag).await {
        Ok(_) => {}
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e.into()),
    }
    let mut len_buf = [0u8; 4];
    r.read_exact(&mut len_buf).await?;
    let body_len = check_len(i32::from_be_bytes(len_buf), 4, max)?;
    let mut payload = vec![0u8; body_len];
    r.read_exact(&mut payload).await?;
    Ok(Some(Frame {
        tag: tag[0],
        payload: Bytes::from(payload),
    }))
}

/// Incremental decoder over a buffer: `Ok(Some((frame, consumed)))`, or
/// `Ok(None)` if more bytes are needed. Shares validation with [`read_frame`].
pub fn decode_frame(buf: &[u8], max: u32) -> Result<Option<(Frame, usize)>, CodecError> {
    if buf.len() < 5 {
        return Ok(None);
    }
    let len = i32::from_be_bytes([buf[1], buf[2], buf[3], buf[4]]);
    let body_len = check_len(len, 4, max)?;
    let total = 5 + body_len;
    if buf.len() < total {
        return Ok(None);
    }
    Ok(Some((
        Frame {
            tag: buf[0],
            payload: Bytes::copy_from_slice(&buf[5..total]),
        },
        total,
    )))
}

/// Encodes a tagged message.
pub fn encode(tag: u8, payload: &[u8]) -> Bytes {
    let mut b = BytesMut::with_capacity(5 + payload.len());
    b.put_u8(tag);
    // Payloads are bounded by max_message_bytes (< i32::MAX) everywhere they
    // come from, so the cast cannot truncate in practice; saturate anyway.
    b.put_i32(i32::try_from(payload.len() + 4).unwrap_or(i32::MAX));
    b.put_slice(payload);
    b.freeze()
}

/// SQL text of a simple `Query`.
pub fn query_text(payload: &[u8]) -> Result<&str, CodecError> {
    match cstr(payload) {
        Some((s, [])) => Ok(s),
        _ => Err(CodecError::Malformed("Query")),
    }
}

/// `(statement name, SQL)` of a `Parse`.
pub fn parse_message(payload: &[u8]) -> Result<(&str, &str), CodecError> {
    let (name, rest) = cstr(payload).ok_or(CodecError::Malformed("Parse"))?;
    let (sql, _) = cstr(rest).ok_or(CodecError::Malformed("Parse"))?;
    Ok((name, sql))
}

/// `(portal, statement)` of a `Bind`.
pub fn bind_message(payload: &[u8]) -> Result<(&str, &str), CodecError> {
    let (portal, rest) = cstr(payload).ok_or(CodecError::Malformed("Bind"))?;
    let (stmt, _) = cstr(rest).ok_or(CodecError::Malformed("Bind"))?;
    Ok((portal, stmt))
}

/// Portal name of an `Execute`.
pub fn execute_portal(payload: &[u8]) -> Result<&str, CodecError> {
    let (portal, rest) = cstr(payload).ok_or(CodecError::Malformed("Execute"))?;
    if rest.len() != 4 {
        return Err(CodecError::Malformed("Execute"));
    }
    Ok(portal)
}

/// Builds an `ErrorResponse` frame.
pub fn error_response(severity: &str, sqlstate: &str, message: &str) -> Bytes {
    let mut p = BytesMut::new();
    for (code, val) in [
        (b'S', severity),
        (b'V', severity),
        (b'C', sqlstate),
        (b'M', message),
    ] {
        p.put_u8(code);
        p.put_slice(val.replace('\0', "").as_bytes());
        p.put_u8(0);
    }
    p.put_u8(0);
    encode(b'E', &p)
}

/// Builds a `ReadyForQuery` frame (`I` idle, `T` in transaction, `E` failed transaction).
pub fn ready_for_query(status: u8) -> Bytes {
    encode(b'Z', &[status])
}

/// SQLSTATE (`C` field) of a server `ErrorResponse`, if present.
pub fn error_code(payload: &[u8]) -> Option<&str> {
    let mut b = payload;
    while let Some((&code, rest)) = b.split_first() {
        if code == 0 {
            return None;
        }
        let (val, rest) = cstr(rest)?;
        if code == b'C' {
            return Some(val);
        }
        b = rest;
    }
    None
}

#[cfg(test)]
mod tests;
