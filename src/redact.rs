//! What may appear in logs.
//!
//! The proxy never logs request or response bodies, header values, query
//! strings, PostgreSQL passwords, or (unless `log_query_text` is set) SQL
//! text. These helpers are the only way request data reaches a log line.

/// Path for logging: the query string is replaced by `?<redacted>`, and the
/// path is truncated to 256 bytes on a character boundary.
pub fn path_for_log(uri: &http::Uri) -> String {
    let mut path = truncate(uri.path(), 256).to_owned();
    if uri.query().is_some() {
        path.push_str("?<redacted>");
    }
    path
}

/// SQL text for logging when explicitly enabled: whitespace collapsed,
/// truncated to 200 bytes.
pub fn sql_for_log(sql: &str) -> String {
    let collapsed: String = sql.split_whitespace().collect::<Vec<_>>().join(" ");
    truncate(&collapsed, 200).to_owned()
}

fn truncate(s: &str, max: usize) -> &str {
    if s.len() <= max {
        return s;
    }
    let mut end = max;
    while !s.is_char_boundary(end) {
        end -= 1;
    }
    &s[..end]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn query_string_is_redacted() {
        let uri: http::Uri = "/login?user=alice&token=abc".parse().expect("valid uri");
        assert_eq!(path_for_log(&uri), "/login?<redacted>");
    }

    #[test]
    fn long_paths_are_truncated() {
        let long = format!("/{}", "a".repeat(600));
        let uri: http::Uri = long.parse().expect("valid uri");
        assert_eq!(path_for_log(&uri).len(), 256);
    }

    #[test]
    fn truncate_respects_char_boundaries() {
        let s = "é".repeat(10); // 2 bytes each
        assert_eq!(truncate(&s, 5), "éé");
    }

    #[test]
    fn sql_is_collapsed_and_truncated() {
        let sql = format!("SELECT   1\n\n FROM t WHERE x = '{}'", "a".repeat(500));
        let out = sql_for_log(&sql);
        assert!(out.starts_with("SELECT 1 FROM t"));
        assert!(out.len() <= 200);
    }
}
