use super::*;
use crate::config::{HttpMatch, PgMatch};
use proptest::prelude::*;

fn rule(id: &str, pct: f64, cap: usize, matcher: HttpMatch, fault: HttpFault) -> HttpRule {
    HttpRule {
        id: id.into(),
        percentage: pct,
        max_concurrent: cap,
        matcher,
        fault,
    }
}

fn view<'a>(
    method: &'a http::Method,
    path: &'a str,
    headers: &'a http::HeaderMap,
) -> HttpRequestView<'a> {
    HttpRequestView {
        method,
        path,
        headers,
    }
}

fn err_fault() -> HttpFault {
    HttpFault::Error {
        status: 503,
        body: String::new(),
    }
}

#[test]
fn first_matching_rule_wins() {
    let rules = HttpRules::compile(
        &[
            rule(
                "a",
                100.0,
                0,
                HttpMatch {
                    path_prefix: Some("/api".into()),
                    ..Default::default()
                },
                err_fault(),
            ),
            rule("b", 100.0, 0, HttpMatch::default(), HttpFault::Reset),
        ],
        1,
    )
    .expect("compile");
    let h = http::HeaderMap::new();
    let (d, _) = rules.decide(0, &view(&http::Method::GET, "/api/x", &h), None);
    assert_eq!(d.expect("decision").rule_id, "a");
    let (d, _) = rules.decide(0, &view(&http::Method::GET, "/other", &h), None);
    assert_eq!(d.expect("decision").rule_id, "b");
}

#[test]
fn methods_and_headers_must_match() {
    let mut headers = std::collections::BTreeMap::new();
    headers.insert("X-Chaos".to_string(), "on".to_string());
    let rules = HttpRules::compile(
        &[rule(
            "a",
            100.0,
            0,
            HttpMatch {
                methods: vec!["post".into()],
                headers,
                ..Default::default()
            },
            err_fault(),
        )],
        1,
    )
    .expect("compile");
    let mut h = http::HeaderMap::new();
    assert!(
        rules
            .decide(0, &view(&http::Method::POST, "/", &h), None)
            .0
            .is_none()
    );
    h.insert("x-chaos", http::HeaderValue::from_static("on"));
    assert!(
        rules
            .decide(0, &view(&http::Method::GET, "/", &h), None)
            .0
            .is_none()
    );
    assert!(
        rules
            .decide(0, &view(&http::Method::POST, "/", &h), None)
            .0
            .is_some()
    );
    h.insert("x-chaos", http::HeaderValue::from_static("off"));
    assert!(
        rules
            .decide(0, &view(&http::Method::POST, "/", &h), None)
            .0
            .is_none()
    );
}

#[test]
fn cap_limits_concurrent_faults_and_releases_on_drop() {
    let rules = HttpRules::compile(&[rule("a", 100.0, 2, HttpMatch::default(), err_fault())], 1)
        .expect("compile");
    let h = http::HeaderMap::new();
    let v = view(&http::Method::GET, "/", &h);
    let d1 = rules.decide(1, &v, None).0.expect("first");
    let d2 = rules.decide(2, &v, None).0.expect("second");
    let (d3, skipped) = rules.decide(3, &v, None);
    assert!(d3.is_none());
    assert_eq!(skipped, vec![("a".to_string(), Skip::Cap)]);
    assert_eq!(rules.active("a"), Some(2));
    drop(d1);
    assert!(rules.decide(4, &v, None).0.is_some());
    drop(d2);
}

#[test]
fn jitter_is_bounded_and_deterministic() {
    let rules = HttpRules::compile(
        &[rule(
            "lat",
            100.0,
            0,
            HttpMatch::default(),
            HttpFault::Latency {
                delay: Duration::from_millis(100),
                jitter: Duration::from_millis(50),
            },
        )],
        99,
    )
    .expect("compile");
    let h = http::HeaderMap::new();
    let v = view(&http::Method::GET, "/", &h);
    for seq in 0..200 {
        let a = rules.decide(seq, &v, None).0.expect("a").fault;
        let b = rules.decide(seq, &v, None).0.expect("b").fault;
        assert_eq!(a, b, "same seq must give the same jitter");
        let ResolvedHttpFault::Latency(d) = a else {
            panic!("expected latency");
        };
        assert!(d >= Duration::from_millis(100) && d <= Duration::from_millis(150));
    }
}

struct Fixed(Result<bool, String>);
impl ExternalMatcher for Fixed {
    fn matches(&self, _: &str, _: &HttpRequestView<'_>) -> Result<bool, String> {
        self.0.clone()
    }
}

#[test]
fn external_matcher_errors_fail_closed() {
    let rules = HttpRules::compile(
        &[rule(
            "w",
            100.0,
            0,
            HttpMatch {
                wasm: Some("m".into()),
                ..Default::default()
            },
            err_fault(),
        )],
        1,
    )
    .expect("compile");
    let h = http::HeaderMap::new();
    let v = view(&http::Method::GET, "/", &h);
    assert!(rules.decide(0, &v, Some(&Fixed(Ok(true)))).0.is_some());
    assert!(rules.decide(0, &v, Some(&Fixed(Ok(false)))).0.is_none());
    assert!(
        rules
            .decide(0, &v, Some(&Fixed(Err("trap".into()))))
            .0
            .is_none()
    );
    assert!(
        rules.decide(0, &v, None).0.is_none(),
        "no matcher available = no fault"
    );
}

#[test]
fn pg_rules_respect_phase_user_and_regex() {
    let rules = PgRules::compile(
        &[
            PgRule {
                id: "start".into(),
                percentage: 100.0,
                matcher: PgMatch {
                    user: Some("app".into()),
                    ..Default::default()
                },
                fault: PgFault::RejectStartup {
                    sqlstate: "53300".into(),
                    message: "full".into(),
                },
            },
            PgRule {
                id: "ins".into(),
                percentage: 100.0,
                matcher: PgMatch {
                    query_regex: Some("(?i)^insert".into()),
                    ..Default::default()
                },
                fault: PgFault::Error {
                    sqlstate: "40001".into(),
                    message: "x".into(),
                },
            },
        ],
        5,
    )
    .expect("compile");
    assert_eq!(
        rules
            .decide(0, PgPhase::Startup, Some("app"), None, None)
            .map(|d| d.0),
        Some("start".into())
    );
    assert!(
        rules
            .decide(0, PgPhase::Startup, Some("other"), None, None)
            .is_none()
    );
    assert_eq!(
        rules
            .decide(
                1,
                PgPhase::Query,
                Some("app"),
                None,
                Some("INSERT INTO t VALUES (1)")
            )
            .map(|d| d.0),
        Some("ins".into())
    );
    assert!(
        rules
            .decide(1, PgPhase::Query, Some("app"), None, Some("SELECT 1"))
            .is_none()
    );
}

proptest! {
    #[test]
    fn decisions_are_a_pure_function_of_seed_and_seq(seed in any::<u64>(), seq in any::<u64>(), pct in 0.0f64..=100.0) {
        let rules = HttpRules::compile(&[rule("p", pct, 0, HttpMatch::default(), err_fault())], seed).expect("compile");
        let h = http::HeaderMap::new();
        let v = view(&http::Method::GET, "/", &h);
        let a = rules.decide(seq, &v, None).0.map(|d| d.rule_id);
        let b = rules.decide(seq, &v, None).0.map(|d| d.rule_id);
        prop_assert_eq!(a, b);
    }
}
