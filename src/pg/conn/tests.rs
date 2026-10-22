use super::*;

fn relay() -> Relay {
    Relay {
        rfq_seen: 0,
        status: b'I',
        standalone: VecDeque::new(),
        before_rfq: HashMap::new(),
        drop_at: None,
    }
}

fn frame(tag: u8, payload: &[u8]) -> codec::Frame {
    codec::Frame {
        tag,
        payload: Bytes::copy_from_slice(payload),
    }
}

#[tokio::test]
async fn injected_error_is_written_just_before_its_ready_for_query() {
    let mut r = relay();
    let mut out: Vec<u8> = Vec::new();
    let err = codec::error_response("ERROR", "40001", "x");
    r.splice(
        Splice::BeforeRfq {
            idx: 1,
            bytes: err.clone(),
        },
        &mut out,
    )
    .await
    .unwrap();
    r.frame(frame(b'2', b""), &mut out).await.unwrap(); // BindComplete
    r.frame(frame(b'Z', b"I"), &mut out).await.unwrap();
    let mut expected = codec::encode(b'2', b"").to_vec();
    expected.extend_from_slice(&err);
    expected.extend_from_slice(&codec::ready_for_query(b'I'));
    assert_eq!(out, expected);
}

#[tokio::test]
async fn standalone_waits_for_earlier_sync_points_and_reuses_status() {
    let mut r = relay();
    let mut out: Vec<u8> = Vec::new();
    let err = codec::error_response("ERROR", "40001", "x");
    r.splice(
        Splice::Standalone {
            after: 1,
            bytes: err.clone(),
        },
        &mut out,
    )
    .await
    .unwrap();
    assert!(out.is_empty(), "must wait for sync point 1");
    r.frame(frame(b'Z', b"T"), &mut out).await.unwrap();
    let mut expected = codec::ready_for_query(b'T').to_vec();
    expected.extend_from_slice(&err);
    expected.extend_from_slice(&codec::ready_for_query(b'T'));
    assert_eq!(out, expected);
}

#[tokio::test]
async fn drop_after_counts_only_rows_of_the_targeted_result() {
    let mut r = relay();
    let mut sink = tokio::io::sink();
    r.splice(Splice::DropAfter { idx: 2, rows: 1 }, &mut sink)
        .await
        .unwrap();
    // Rows of result 1 (rfq_seen = 0, idx 2 targets the second result) are not counted.
    for _ in 0..3 {
        r.frame(frame(b'D', b"row"), &mut sink).await.unwrap();
    }
    r.frame(frame(b'Z', b"I"), &mut sink).await.unwrap();
    r.frame(frame(b'D', b"row"), &mut sink).await.unwrap();
    assert!(matches!(
        r.frame(frame(b'D', b"row"), &mut sink).await,
        Err(End::Dropped)
    ));
}

#[tokio::test]
async fn pending_injections_are_bounded() {
    let mut r = relay();
    let mut sink = tokio::io::sink();
    for _ in 0..MAX_PENDING_SPLICES {
        r.splice(
            Splice::Standalone {
                after: 9,
                bytes: Bytes::from_static(b"e"),
            },
            &mut sink,
        )
        .await
        .unwrap();
    }
    let over = r
        .splice(
            Splice::Standalone {
                after: 9,
                bytes: Bytes::from_static(b"e"),
            },
            &mut sink,
        )
        .await;
    assert!(matches!(over, Err(End::Error(ref m)) if m.contains("injected responses waiting")));

    let mut r = relay();
    for i in 0..MAX_PENDING_SPLICES as u64 {
        r.splice(
            Splice::BeforeRfq {
                idx: i + 2,
                bytes: Bytes::new(),
            },
            &mut sink,
        )
        .await
        .unwrap();
    }
    let over = r
        .splice(
            Splice::BeforeRfq {
                idx: u64::MAX,
                bytes: Bytes::new(),
            },
            &mut sink,
        )
        .await;
    assert!(matches!(over, Err(End::Error(_))));
}

#[test]
fn tracked_names_are_bounded_but_known_names_update() {
    let mut m = HashMap::new();
    for i in 0..MAX_TRACKED_NAMES {
        track(&mut m, &format!("s{i}"), "SELECT 1");
    }
    track(&mut m, "new", "SELECT 2");
    assert!(!m.contains_key("new"));
    track(&mut m, "s0", "SELECT 3");
    assert_eq!(m.get("s0").map(String::as_str), Some("SELECT 3"));
}
