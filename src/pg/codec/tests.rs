use super::*;
use proptest::prelude::*;

fn startup_bytes(params: &[(&str, &str)]) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(&PROTOCOL_V3.to_be_bytes());
    for (k, v) in params {
        body.extend_from_slice(k.as_bytes());
        body.push(0);
        body.extend_from_slice(v.as_bytes());
        body.push(0);
    }
    body.push(0);
    let mut out = ((body.len() + 4) as i32).to_be_bytes().to_vec();
    out.extend_from_slice(&body);
    out
}

#[tokio::test]
async fn reads_startup_message_and_params() {
    let bytes = startup_bytes(&[("user", "app"), ("database", "db1")]);
    let mut r = &bytes[..];
    let s = read_startup(&mut r, 1 << 20)
        .await
        .expect("ok")
        .expect("some");
    assert_eq!(s.param("user"), Some("app"));
    assert_eq!(s.param("database"), Some("db1"));
    match s {
        Startup::Startup { raw, .. } => assert_eq!(&raw[..], &bytes[..]),
        other => panic!("unexpected {other:?}"),
    }
}

#[tokio::test]
async fn reads_ssl_gss_and_cancel_requests() {
    let mut ssl = 8i32.to_be_bytes().to_vec();
    ssl.extend_from_slice(&SSL_REQUEST.to_be_bytes());
    assert_eq!(
        read_startup(&mut &ssl[..], 1 << 20).await,
        Ok(Some(Startup::Ssl))
    );
    let mut gss = 8i32.to_be_bytes().to_vec();
    gss.extend_from_slice(&GSSENC_REQUEST.to_be_bytes());
    assert_eq!(
        read_startup(&mut &gss[..], 1 << 20).await,
        Ok(Some(Startup::GssEnc))
    );
    let mut cancel = 16i32.to_be_bytes().to_vec();
    cancel.extend_from_slice(&CANCEL_REQUEST.to_be_bytes());
    cancel.extend_from_slice(&[0, 0, 0, 1, 0, 0, 0, 2]);
    assert!(matches!(
        read_startup(&mut &cancel[..], 1 << 20).await,
        Ok(Some(Startup::Cancel { .. }))
    ));
}

#[tokio::test]
async fn rejects_malformed_startup() {
    // Length too small.
    let bad = 3i32.to_be_bytes();
    assert_eq!(
        read_startup(&mut &bad[..], 1 << 20).await,
        Err(CodecError::TooShort(3))
    );
    // Unknown protocol code.
    let mut unk = 8i32.to_be_bytes().to_vec();
    unk.extend_from_slice(&1234i32.to_be_bytes());
    assert_eq!(
        read_startup(&mut &unk[..], 1 << 20).await,
        Err(CodecError::UnknownStartup(1234))
    );
    // Parameter list without terminator.
    let mut body = PROTOCOL_V3.to_be_bytes().to_vec();
    body.extend_from_slice(b"user\0app");
    let mut msg = ((body.len() + 4) as i32).to_be_bytes().to_vec();
    msg.extend_from_slice(&body);
    assert_eq!(
        read_startup(&mut &msg[..], 1 << 20).await,
        Err(CodecError::Malformed("StartupMessage"))
    );
    // Truncated: declares 100 bytes, sends 10.
    let mut trunc = 100i32.to_be_bytes().to_vec();
    trunc.extend_from_slice(&[0u8; 6]);
    assert_eq!(
        read_startup(&mut &trunc[..], 1 << 20).await,
        Err(CodecError::Truncated)
    );
    // Empty input = client went away.
    assert_eq!(read_startup(&mut &[][..], 1 << 20).await, Ok(None));
}

#[tokio::test]
async fn frame_length_limits() {
    let mut neg = vec![b'Q'];
    neg.extend_from_slice(&(-1i32).to_be_bytes());
    assert_eq!(
        read_frame(&mut &neg[..], 1024).await,
        Err(CodecError::TooShort(-1))
    );
    let mut big = vec![b'Q'];
    big.extend_from_slice(&5000i32.to_be_bytes());
    assert!(matches!(
        read_frame(&mut &big[..], 1024).await,
        Err(CodecError::TooLong { .. })
    ));
    assert_eq!(read_frame(&mut &[][..], 1024).await, Ok(None));
}

#[test]
fn query_parse_bind_execute_parsers() {
    assert_eq!(query_text(b"SELECT 1\0"), Ok("SELECT 1"));
    assert!(query_text(b"SELECT 1").is_err());
    assert!(query_text(b"SELECT 1\0junk").is_err());
    assert_eq!(
        parse_message(b"s1\0SELECT $1\0\0\0"),
        Ok(("s1", "SELECT $1"))
    );
    assert_eq!(bind_message(b"p1\0s1\0\0\0"), Ok(("p1", "s1")));
    assert_eq!(execute_portal(b"p1\0\0\0\0\0"), Ok("p1"));
    assert!(execute_portal(b"p1\0\0").is_err());
}

#[test]
fn error_response_round_trips_sqlstate() {
    let frame = error_response("ERROR", "40001", "boom");
    let (f, used) = decode_frame(&frame, 1024).expect("ok").expect("complete");
    assert_eq!(used, frame.len());
    assert_eq!(f.tag, b'E');
    assert_eq!(error_code(&f.payload), Some("40001"));
}

#[test]
fn error_response_strips_nul_bytes() {
    let frame = error_response("ERROR", "40001", "a\0b");
    let (f, _) = decode_frame(&frame, 1024).expect("ok").expect("complete");
    assert!(f.payload.windows(3).any(|w| w == b"Mab"));
}

proptest! {
    #[test]
    fn encode_decode_round_trip(tag in any::<u8>(), payload in proptest::collection::vec(any::<u8>(), 0..2048)) {
        let wire = encode(tag, &payload);
        let (f, used) = decode_frame(&wire, 1 << 20).expect("valid").expect("complete");
        prop_assert_eq!(used, wire.len());
        prop_assert_eq!(f.tag, tag);
        prop_assert_eq!(&f.payload[..], &payload[..]);
        // Any strict prefix is "need more bytes", never an error or a frame.
        for cut in 0..wire.len() {
            prop_assert_eq!(decode_frame(&wire[..cut], 1 << 20), Ok(None));
        }
    }

    #[test]
    fn decoder_never_panics_on_garbage(bytes in proptest::collection::vec(any::<u8>(), 0..64)) {
        let _ = decode_frame(&bytes, 1024);
        let _ = error_code(&bytes);
        let _ = query_text(&bytes);
        let _ = parse_message(&bytes);
        let _ = bind_message(&bytes);
        let _ = execute_portal(&bytes);
    }
}
