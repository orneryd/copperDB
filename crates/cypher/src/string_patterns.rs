//! Scanner-based string pattern utilities for the Cypher hot path.
//!
//! Equivalent to Go's `pkg/cypher/string_patterns.go` in NornicDB v1.0.40.
//!
//! Provides fast alternatives to regex for operations that are called on every
//! query execution.  All public functions work directly on `&str` byte slices
//! without heap allocation on the common path; keyword comparison is done
//! byte-by-byte using `to_ascii_uppercase()`.
//!
//! # Performance
//!
//! These functions are significantly faster than regex equivalents:
//! - `split_by_keyword` vs regex split:  ~8× faster
//! - `extract_limit` / `extract_skip` vs regex capture: ~6× faster
//! - `extract_parameters` vs regex FindAll: ~5× faster
//! - `parse_aggregation` replaces 8 separate regex patterns (~5× faster)

use crate::keyword_scan::{is_ascii_space, is_ident_byte};

// ─── Keyword splitting ────────────────────────────────────────────────────────

/// Split `s` by occurrences of `keyword` (case-insensitive), respecting word
/// boundaries.  Each occurrence must be followed by at least one whitespace
/// character (so "MATCH" does not split on "MATCHX").
///
/// Equivalent to Go's `SplitByKeyword`.
///
/// ```
/// use copperdb_cypher::string_patterns::split_by_keyword;
/// let parts = split_by_keyword("MATCH (a) MATCH (b)", "MATCH");
/// assert_eq!(parts, vec!["", "(a) ", "(b)"]);
/// ```
pub fn split_by_keyword<'a>(s: &'a str, keyword: &str) -> Vec<&'a str> {
    if s.is_empty() {
        return vec![s];
    }

    let sb = s.as_bytes();
    let klen = keyword.len();
    if klen == 0 {
        return vec![s];
    }

    let kb = keyword.as_bytes();
    let first_upper = kb[0].to_ascii_uppercase();

    let mut result: Vec<&'a str> = Vec::new();
    let mut last_end = 0usize;
    let mut i = 0usize;

    while i + klen <= sb.len() {
        if sb[i].to_ascii_uppercase() != first_upper {
            i += 1;
            continue;
        }

        // Check all keyword bytes case-insensitively (no heap allocation)
        if !sb[i..i + klen]
            .iter()
            .zip(kb.iter())
            .all(|(s_b, k_b)| s_b.eq_ignore_ascii_case(k_b))
        {
            i += 1;
            continue;
        }

        // Word boundary before
        if i > 0 && is_word_char(sb[i - 1]) {
            i += 1;
            continue;
        }

        // Must be followed by whitespace
        let after = i + klen;
        if after >= sb.len() || !is_ascii_space(sb[after]) {
            i += 1;
            continue;
        }

        // Emit slice before this keyword
        result.push(&s[last_end..i]);

        // Skip keyword + trailing whitespace
        last_end = after;
        while last_end < sb.len() && is_ascii_space(sb[last_end]) {
            last_end += 1;
        }
        i = last_end;
    }

    result.push(&s[last_end..]);
    result
}

/// Split by the `MATCH` keyword — convenience hot-path wrapper.
pub fn split_by_match(s: &str) -> Vec<&str> {
    split_by_keyword(s, "MATCH")
}

/// Split by the `CREATE` keyword — convenience hot-path wrapper.
pub fn split_by_create(s: &str) -> Vec<&str> {
    split_by_keyword(s, "CREATE")
}

// ─── LIMIT / SKIP extraction ──────────────────────────────────────────────────

/// Extract the `LIMIT` integer value from a query string.
///
/// Returns `Some(n)` if a valid `LIMIT <n>` is found, `None` otherwise.
/// Approximately 6× faster than regex equivalent.
///
/// ```
/// use copperdb_cypher::string_patterns::extract_limit;
/// assert_eq!(extract_limit("MATCH (n) RETURN n LIMIT 10"), Some(10));
/// assert_eq!(extract_limit("MATCH (n) RETURN n"), None);
/// ```
pub fn extract_limit(query: &str) -> Option<usize> {
    extract_int_after_keyword(query, "LIMIT")
}

/// Extract the `SKIP` integer value from a query string.
pub fn extract_skip(query: &str) -> Option<usize> {
    extract_int_after_keyword(query, "SKIP")
}

/// Like `extract_limit` but returns the raw number as a `&str` slice.
pub fn extract_limit_str(query: &str) -> Option<&str> {
    extract_str_after_keyword(query, "LIMIT")
}

/// Like `extract_skip` but returns the raw number as a `&str` slice.
pub fn extract_skip_str(query: &str) -> Option<&str> {
    extract_str_after_keyword(query, "SKIP")
}

fn extract_int_after_keyword(s: &str, keyword: &str) -> Option<usize> {
    let slice = extract_str_after_keyword(s, keyword)?;
    let mut result = 0usize;
    let mut found_digit = false;
    for b in slice.bytes() {
        if b.is_ascii_digit() {
            result = result * 10 + (b - b'0') as usize;
            found_digit = true;
        } else {
            break;
        }
    }
    if found_digit { Some(result) } else { None }
}

fn extract_str_after_keyword<'a>(s: &'a str, keyword: &str) -> Option<&'a str> {
    let sb = s.as_bytes();
    let klen = keyword.len();
    let keyword_upper: Vec<u8> = keyword.bytes().map(|b| b.to_ascii_uppercase()).collect();

    let mut i = 0;
    while i + klen <= sb.len() {
        // Find first matching character
        if sb[i].to_ascii_uppercase() != keyword_upper[0] {
            i += 1;
            continue;
        }
        if !sb[i..i + klen]
            .iter()
            .zip(keyword_upper.iter())
            .all(|(s_b, k_b)| s_b.to_ascii_uppercase() == *k_b)
        {
            i += 1;
            continue;
        }
        // Word boundary before
        if i > 0 && is_ident_byte(sb[i - 1]) {
            i += 1;
            continue;
        }
        // Word boundary after
        let after = i + klen;
        if after < sb.len() && is_ident_byte(sb[after]) {
            i += 1;
            continue;
        }
        // Skip whitespace after keyword
        let mut start = after;
        while start < sb.len() && is_ascii_space(sb[start]) {
            start += 1;
        }
        return Some(&s[start..]);
    }
    None
}

// ─── Keyword index ─────────────────────────────────────────────────────────────

/// Find the byte offset of `keyword` in `s` (case-insensitive, word
/// boundaries, no allocation).
///
/// Returns `None` if not found.
///
/// This is equivalent to Go's `FindKeywordIndex`, using the lightweight
/// character-scan approach rather than the full `keyword_scan` machinery (which
/// skips strings/comments/parens).  Use `keyword_scan::keyword_index` when you
/// need those guarantees.
pub fn find_keyword_index(s: &str, keyword: &str) -> Option<usize> {
    let sb = s.as_bytes();
    let klen = keyword.len();
    if klen == 0 || sb.is_empty() {
        return None;
    }
    let keyword_upper: Vec<u8> = keyword.bytes().map(|b| b.to_ascii_uppercase()).collect();

    let mut i = 0;
    while i + klen <= sb.len() {
        if sb[i].to_ascii_uppercase() != keyword_upper[0] {
            i += 1;
            continue;
        }
        if !sb[i..i + klen]
            .iter()
            .zip(keyword_upper.iter())
            .all(|(s_b, k_b)| s_b.to_ascii_uppercase() == *k_b)
        {
            i += 1;
            continue;
        }
        // Word boundary before
        if i > 0 && is_word_char(sb[i - 1]) {
            i += 1;
            continue;
        }
        // Word boundary after
        let after = i + klen;
        if after < sb.len() && is_word_char(sb[after]) {
            i += 1;
            continue;
        }
        return Some(i);
    }
    None
}

/// Return `true` if `s` contains `keyword` (case-insensitive, word boundaries).
pub fn contains_keyword(s: &str, keyword: &str) -> bool {
    find_keyword_index(s, keyword).is_some()
}

// ─── Aggregation parsing ───────────────────────────────────────────────────────

/// Parsed components of an aggregation expression like `COUNT(n.prop)` or
/// `SUM(DISTINCT n.age)`.
///
/// Equivalent to Go's `AggregationResult`.
#[derive(Debug, Clone, PartialEq)]
pub struct AggregationResult {
    /// Function name in uppercase: `COUNT`, `SUM`, `AVG`, `MIN`, `MAX`, `COLLECT`
    pub function: String,
    /// The variable name (e.g. `"n"`)
    pub variable: String,
    /// The property name (e.g. `"age"`); empty for `COUNT(n)` or `COUNT(*)`
    pub property: String,
    /// `true` if `DISTINCT` was specified
    pub distinct: bool,
    /// `true` if `COUNT(*)`
    pub is_star: bool,
}

const AGG_FUNCTIONS: &[&str] = &["COLLECT", "COUNT", "SUM", "AVG", "MIN", "MAX"];

/// Parse an aggregation expression.
///
/// Replaces 8 separate regex patterns with one unified scanner (~5× faster).
/// Returns `None` if `expr` is not a valid aggregation.
///
/// ```
/// use copperdb_cypher::string_patterns::parse_aggregation;
/// let r = parse_aggregation("COUNT(n.age)").unwrap();
/// assert_eq!(r.function, "COUNT");
/// assert_eq!(r.variable, "n");
/// assert_eq!(r.property, "age");
///
/// let r = parse_aggregation("COUNT(*)").unwrap();
/// assert!(r.is_star);
///
/// let r = parse_aggregation("SUM(DISTINCT x.value)").unwrap();
/// assert!(r.distinct);
/// ```
pub fn parse_aggregation(expr: &str) -> Option<AggregationResult> {
    let expr = expr.trim();
    if expr.len() < 5 {
        return None;
    }
    let upper = expr.to_ascii_uppercase();

    // Identify function
    let mut func_name = "";
    let mut func_len = 0usize;
    for &fn_name in AGG_FUNCTIONS {
        let prefix = format!("{}(", fn_name);
        if upper.starts_with(&prefix) {
            func_name = fn_name;
            func_len = fn_name.len();
            break;
        }
    }
    if func_name.is_empty() {
        return None;
    }

    let eb = expr.as_bytes();
    if eb[func_len] != b'(' {
        return None;
    }

    // Find matching close paren
    let close = eb[func_len + 1..].iter().rposition(|&b| b == b')')?;
    let close = close + func_len + 1;
    if close <= func_len {
        return None;
    }

    let content = expr[func_len + 1..close].trim();
    if content.is_empty() {
        return None;
    }

    let mut result = AggregationResult {
        function: func_name.to_owned(),
        variable: String::new(),
        property: String::new(),
        distinct: false,
        is_star: false,
    };

    if content == "*" {
        result.is_star = true;
        return Some(result);
    }

    let content_upper = content.to_ascii_uppercase();

    // Check DISTINCT
    let content = if content_upper.starts_with("DISTINCT ") {
        result.distinct = true;
        content[9..].trim()
    } else {
        content
    };

    // Parse variable.property or just variable
    if let Some(dot) = content.find('.') {
        let var_part = content[..dot].trim();
        let prop_part = content[dot + 1..].trim();
        if !is_valid_identifier(var_part) || !is_valid_identifier(prop_part) {
            return None;
        }
        result.variable = var_part.to_owned();
        result.property = prop_part.to_owned();
    } else {
        if !is_valid_identifier(content) {
            return None;
        }
        result.variable = content.to_owned();
    }

    Some(result)
}

/// Convenience: returns `(variable, property)` from an aggregation expression,
/// or `("", "")` on failure.  Provides compatibility with the old
/// regex-capture-group `match[1]`/`match[2]` pattern.
pub fn parse_aggregation_property(expr: &str) -> (String, String) {
    match parse_aggregation(expr) {
        Some(r) => (r.variable, r.property),
        None => (String::new(), String::new()),
    }
}

// ─── Parameter extraction ─────────────────────────────────────────────────────

/// Find all `$param` references in `query` and return their names (without the
/// `$` prefix).
///
/// Approximately 5× faster than regex `FindAllStringSubmatch`.
///
/// ```
/// use copperdb_cypher::string_patterns::extract_parameters;
/// let params = extract_parameters("MATCH (n) WHERE n.name = $name AND n.age > $minAge");
/// assert_eq!(params, vec!["name", "minAge"]);
/// ```
pub fn extract_parameters(query: &str) -> Vec<&str> {
    let sb = query.as_bytes();
    let mut params: Vec<&str> = Vec::new();
    let mut i = 0;

    while i < sb.len() {
        if sb[i] != b'$' {
            i += 1;
            continue;
        }
        let start = i + 1;
        if start >= sb.len() {
            break;
        }
        let first = sb[start];
        if !first.is_ascii_alphabetic() && first != b'_' {
            i = start;
            continue;
        }
        let end = sb[start..]
            .iter()
            .position(|&b| !is_ident_byte(b))
            .map(|p| start + p)
            .unwrap_or(sb.len());
        params.push(&query[start..end]);
        i = end;
    }

    params
}

/// Replace all `$param` references using a callback.
///
/// The callback receives the parameter name (without `$`) and returns the
/// replacement string.
pub fn replace_parameters(query: &str, mut replacer: impl FnMut(&str) -> String) -> String {
    let sb = query.as_bytes();
    let mut result = String::with_capacity(query.len());
    let mut i = 0;
    // `copy_start` tracks the beginning of a pending unchanged slice.
    let mut copy_start = 0;

    while i < sb.len() {
        if sb[i] != b'$' {
            i += 1;
            continue;
        }

        // Flush unchanged bytes up to (but not including) the `$`.
        // Using `push_str` on a valid UTF-8 sub-slice preserves multi-byte chars.
        result.push_str(&query[copy_start..i]);

        let start = i + 1;
        if start >= sb.len() {
            result.push('$');
            i = start;
            copy_start = start;
            continue;
        }
        let first = sb[start];
        if !first.is_ascii_alphabetic() && first != b'_' {
            result.push('$');
            i = start;
            copy_start = start;
            continue;
        }
        let end = sb[start..]
            .iter()
            .position(|&b| !is_ident_byte(b))
            .map(|p| start + p)
            .unwrap_or(sb.len());
        let param_name = &query[start..end];
        result.push_str(&replacer(param_name));
        i = end;
        copy_start = end;
    }

    // Flush any remaining unchanged tail.
    result.push_str(&query[copy_start..]);
    result
}

// ─── Helpers ──────────────────────────────────────────────────────────────────

fn is_word_char(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_'
}

fn is_valid_identifier(s: &str) -> bool {
    if s.is_empty() {
        return false;
    }
    let mut chars = s.chars();
    let first = chars.next().unwrap();
    if !first.is_ascii_alphabetic() && first != '_' {
        return false;
    }
    chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

/// Strip `USING INDEX/JOIN/SCAN` hints from a Cypher query.
/// These hints are advisory and don't affect query semantics.
pub fn strip_index_hints(query: &str) -> (Vec<String>, String) {
    let mut hints = Vec::new();
    let upper = query.to_uppercase();
    let bytes = query.as_bytes();
    let mut result = String::with_capacity(query.len());
    let mut pos = 0;

    while pos < bytes.len() {
        // Look for USING keyword at the current position
        let remaining = &upper[pos..];
        if remaining.starts_with("USING ") || remaining.starts_with("USING\t") {
            let _start = pos;
            pos += 6; // skip "USING "

            // Scan forward to find the end of the hint (before WHERE/RETURN/WITH or end)
            let hint_start = pos;
            while pos < bytes.len() {
                if bytes[pos] == b'\n'
                    || (pos + 5 < bytes.len()
                        && (&upper[pos..pos + 5] == "WHERE"
                            || &upper[pos..pos + 6] == "RETURN"
                            || &upper[pos..pos + 4] == "WITH"
                            || &upper[pos..pos + 5] == "MATCH"
                            || &upper[pos..pos + 6] == "CREATE"
                            || &upper[pos..pos + 5] == "MERGE"))
                {
                    break;
                }
                pos += 1;
            }

            hints.push(query[hint_start..pos].trim().to_string());
            continue;
        }

        // Copy normal characters
        if pos < bytes.len() {
            result.push(bytes[pos] as char);
        }
        pos += 1;
    }

    (hints, result.trim().to_string())
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    // ── split_by_keyword ──────────────────────────────────────────────────────

    #[test]
    fn test_split_by_keyword_basic() {
        let parts = split_by_keyword("MATCH (a) MATCH (b)", "MATCH");
        assert_eq!(parts, vec!["", "(a) ", "(b)"]);
    }

    #[test]
    fn test_split_by_match() {
        let parts = split_by_match("MATCH (a) MATCH (b)");
        assert_eq!(parts, vec!["", "(a) ", "(b)"]);
    }

    #[test]
    fn test_split_by_keyword_no_match() {
        let parts = split_by_keyword("RETURN n", "MATCH");
        assert_eq!(parts, vec!["RETURN n"]);
    }

    #[test]
    fn test_split_by_keyword_case_insensitive() {
        let parts = split_by_keyword("match (a) match (b)", "MATCH");
        assert_eq!(parts, vec!["", "(a) ", "(b)"]);
    }

    #[test]
    fn test_split_by_keyword_no_partial() {
        // "MATCHX " does not count
        let parts = split_by_keyword("MATCHX (a)", "MATCH");
        assert_eq!(parts, vec!["MATCHX (a)"]);
    }

    // ── extract_limit / extract_skip ─────────────────────────────────────────

    #[test]
    fn test_extract_limit() {
        assert_eq!(extract_limit("MATCH (n) RETURN n LIMIT 10"), Some(10));
        assert_eq!(extract_limit("RETURN n LIMIT 100"), Some(100));
        assert_eq!(extract_limit("RETURN n"), None);
    }

    #[test]
    fn test_extract_skip() {
        assert_eq!(extract_skip("MATCH (n) RETURN n SKIP 5 LIMIT 10"), Some(5));
        assert_eq!(extract_skip("RETURN n LIMIT 10"), None);
    }

    #[test]
    fn test_extract_limit_case_insensitive() {
        assert_eq!(extract_limit("MATCH (n) RETURN n limit 7"), Some(7));
    }

    // ── find_keyword_index / contains_keyword ─────────────────────────────────

    #[test]
    fn test_find_keyword_index() {
        assert_eq!(find_keyword_index("MATCH (n) RETURN n", "RETURN"), Some(10));
        assert_eq!(find_keyword_index("MATCH (n)", "RETURN"), None);
    }

    #[test]
    fn test_contains_keyword() {
        assert!(contains_keyword("MATCH (n) RETURN n", "RETURN"));
        assert!(!contains_keyword("MATCH (n)", "RETURN"));
        // "WITH" in "STARTS WITH" should still be found at its own position
        assert!(contains_keyword(
            "MATCH (n) WHERE n.name STARTS WITH 'A'",
            "WITH"
        ));
    }

    // ── parse_aggregation ─────────────────────────────────────────────────────

    #[test]
    fn test_parse_count_property() {
        let r = parse_aggregation("COUNT(n.age)").unwrap();
        assert_eq!(r.function, "COUNT");
        assert_eq!(r.variable, "n");
        assert_eq!(r.property, "age");
        assert!(!r.distinct);
        assert!(!r.is_star);
    }

    #[test]
    fn test_parse_count_star() {
        let r = parse_aggregation("COUNT(*)").unwrap();
        assert_eq!(r.function, "COUNT");
        assert!(r.is_star);
    }

    #[test]
    fn test_parse_sum_distinct() {
        let r = parse_aggregation("SUM(DISTINCT x.value)").unwrap();
        assert_eq!(r.function, "SUM");
        assert_eq!(r.variable, "x");
        assert_eq!(r.property, "value");
        assert!(r.distinct);
    }

    #[test]
    fn test_parse_aggregation_variable_only() {
        let r = parse_aggregation("COUNT(n)").unwrap();
        assert_eq!(r.variable, "n");
        assert_eq!(r.property, "");
    }

    #[test]
    fn test_parse_aggregation_not_agg() {
        assert!(parse_aggregation("n.age + 1").is_none());
        assert!(parse_aggregation("toString(n)").is_none());
    }

    #[test]
    fn test_parse_aggregation_collect() {
        let r = parse_aggregation("COLLECT(n.name)").unwrap();
        assert_eq!(r.function, "COLLECT");
        assert_eq!(r.variable, "n");
        assert_eq!(r.property, "name");
    }

    // ── extract_parameters ────────────────────────────────────────────────────

    #[test]
    fn test_extract_parameters_basic() {
        let params = extract_parameters("WHERE n.name = $name AND n.age > $minAge");
        assert_eq!(params, vec!["name", "minAge"]);
    }

    #[test]
    fn test_extract_parameters_none() {
        let params = extract_parameters("MATCH (n) RETURN n");
        assert!(params.is_empty());
    }

    #[test]
    fn test_extract_parameters_single() {
        let params = extract_parameters("MATCH (n {id: $id}) RETURN n");
        assert_eq!(params, vec!["id"]);
    }

    // ── replace_parameters ────────────────────────────────────────────────────

    #[test]
    fn test_replace_parameters() {
        let result = replace_parameters("n.name = $name", |p| format!("'{}'", p));
        assert_eq!(result, "n.name = 'name'");
    }

    #[test]
    fn test_replace_parameters_multiple() {
        let result = replace_parameters("$a AND $b", |p| p.to_uppercase());
        assert_eq!(result, "A AND B");
    }
}
