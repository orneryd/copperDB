//! Allocation-free keyword scanning for the Cypher hot path.
//!
//! Equivalent to Go's `pkg/cypher/keyword_scan.go` in NornicDB v1.0.40.
//!
//! Provides high-performance, zero-allocation keyword searching with:
//! - Case-insensitive matching
//! - Flexible whitespace between keyword tokens (e.g. "ORDER BY")
//! - Skipping over string literals, backtick identifiers, and comments
//! - Optional skipping over nested `()`, `[]`, `{}` regions
//! - Word-boundary enforcement (e.g. "MATCH" does not match inside "REMATCH")
//!
//! This replaces any regex-based keyword detection that was previously used in
//! hot-path routing.

// ─── Options ─────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum KeywordBoundaryMode {
    /// Require identifier-boundary characters (alphanumeric / underscore) on
    /// either side of the match to not be present.  This is the default.
    Word,
    /// Require plain ASCII whitespace on either side.
    Whitespace,
}

#[derive(Debug, Clone, Copy)]
pub struct KeywordScanOpts {
    pub skip_parens: bool,
    pub skip_brackets: bool,
    pub skip_braces: bool,
    pub skip_strings: bool,
    pub skip_backticks: bool,
    pub skip_comments: bool,
    pub boundary: KeywordBoundaryMode,
}

impl Default for KeywordScanOpts {
    fn default() -> Self {
        KeywordScanOpts {
            skip_parens: true,
            skip_brackets: true,
            skip_braces: false,
            skip_strings: true,
            skip_backticks: true,
            skip_comments: true,
            boundary: KeywordBoundaryMode::Word,
        }
    }
}

// ─── Public API ───────────────────────────────────────────────────────────────

/// Find the byte offset of `keyword` in `s`, using default options (skips
/// strings, comments, parens, brackets; does NOT skip braces).
///
/// Returns `None` if not found.
///
/// This is the primary hot-path entry point — **zero allocations**.
pub fn keyword_index(s: &str, keyword: &str) -> Option<usize> {
    keyword_index_from(s, keyword, 0, KeywordScanOpts::default())
}

/// Like `keyword_index` but also skips nested `{…}` regions.
///
/// Use this when splitting clause boundaries, e.g. to find the `RETURN` that
/// follows a `CALL { … }` subquery.
pub fn top_level_keyword_index(s: &str, keyword: &str) -> Option<usize> {
    let opts = KeywordScanOpts {
        skip_braces: true,
        ..KeywordScanOpts::default()
    };
    keyword_index_from(s, keyword, 0, opts)
}

/// General-purpose keyword finder with configurable options.
///
/// # Parameters
/// - `s`       — the query string to search (raw bytes, UTF-8 assumed ASCII for hot path)
/// - `keyword` — the keyword to find (may contain internal spaces, e.g. `"ORDER BY"`)
/// - `from`    — start offset within `s`
/// - `opts`    — scanning options
///
/// # Returns
/// Byte offset of the first match, or `None`.
pub fn keyword_index_from(
    s: &str,
    keyword: &str,
    from: usize,
    opts: KeywordScanOpts,
) -> Option<usize> {
    let sb = s.as_bytes();
    let kb = keyword.as_bytes();

    // Trim leading/trailing whitespace from the keyword pattern.
    let ks = trim_ws_start(kb);
    let ke = trim_ws_end(kb);
    if ks >= ke {
        return None;
    }

    let from = from.min(sb.len());
    if from >= sb.len() {
        return None;
    }

    let first = ascii_upper(kb[ks]);

    let mut paren_depth: i32 = 0;
    let mut bracket_depth: i32 = 0;
    let mut brace_depth: i32 = 0;

    let mut in_single_quote = false;
    let mut in_double_quote = false;
    let mut in_backtick = false;
    let mut in_line_comment = false;
    let mut in_block_comment = false;

    let mut i = from;
    while i < sb.len() {
        let c = sb[i];

        // ── Comments ──────────────────────────────────────────────────────────
        if opts.skip_comments {
            if in_line_comment {
                if c == b'\n' {
                    in_line_comment = false;
                }
                i += 1;
                continue;
            }
            if in_block_comment {
                if c == b'*' && i + 1 < sb.len() && sb[i + 1] == b'/' {
                    in_block_comment = false;
                    i += 2;
                } else {
                    i += 1;
                }
                continue;
            }
        }

        // ── Strings ───────────────────────────────────────────────────────────
        if opts.skip_strings {
            if in_single_quote {
                if c == b'\\' && i + 1 < sb.len() {
                    i += 2; // skip escaped char
                    continue;
                }
                if c == b'\'' {
                    // SQL-style doubled quote ('') counts as escaped
                    if i + 1 < sb.len() && sb[i + 1] == b'\'' {
                        i += 2;
                        continue;
                    }
                    in_single_quote = false;
                }
                i += 1;
                continue;
            }
            if in_double_quote {
                if c == b'\\' && i + 1 < sb.len() {
                    i += 2;
                    continue;
                }
                if c == b'"' {
                    if i + 1 < sb.len() && sb[i + 1] == b'"' {
                        i += 2;
                        continue;
                    }
                    in_double_quote = false;
                }
                i += 1;
                continue;
            }
        }

        // ── Backticks ─────────────────────────────────────────────────────────
        if opts.skip_backticks && in_backtick {
            if c == b'`' {
                if i + 1 < sb.len() && sb[i + 1] == b'`' {
                    i += 2;
                    continue;
                }
                in_backtick = false;
            }
            i += 1;
            continue;
        }

        // ── Start comment / string / backtick ─────────────────────────────────
        if opts.skip_comments && c == b'/' && i + 1 < sb.len() {
            if sb[i + 1] == b'/' {
                in_line_comment = true;
                i += 2;
                continue;
            }
            if sb[i + 1] == b'*' {
                in_block_comment = true;
                i += 2;
                continue;
            }
        }

        if opts.skip_strings {
            if c == b'\'' {
                in_single_quote = true;
                i += 1;
                continue;
            }
            if c == b'"' {
                in_double_quote = true;
                i += 1;
                continue;
            }
        }
        if opts.skip_backticks && c == b'`' {
            in_backtick = true;
            i += 1;
            continue;
        }

        // ── Depth tracking ────────────────────────────────────────────────────
        match c {
            b'(' => paren_depth += 1,
            b')' if paren_depth > 0 => paren_depth -= 1,
            b'[' => bracket_depth += 1,
            b']' if bracket_depth > 0 => bracket_depth -= 1,
            b'{' => brace_depth += 1,
            b'}' if brace_depth > 0 => brace_depth -= 1,
            _ => {}
        }

        if (opts.skip_parens && paren_depth > 0)
            || (opts.skip_brackets && bracket_depth > 0)
            || (opts.skip_braces && brace_depth > 0)
        {
            i += 1;
            continue;
        }

        // ── Candidate match ───────────────────────────────────────────────────
        if ascii_upper(c) != first {
            i += 1;
            continue;
        }

        if !keyword_left_boundary_ok(sb, i, opts.boundary) {
            i += 1;
            continue;
        }

        match keyword_match_at(sb, i, kb, ks, ke) {
            Some(end_pos) if keyword_right_boundary_ok(sb, end_pos, opts.boundary) => {
                return Some(i);
            }
            _ => {}
        }

        i += 1;
    }

    None
}

/// Check whether `s` starts with `keyword_upper` (a pre-uppercased keyword)
/// case-insensitively, and that the next character is not an identifier
/// character (word boundary).
///
/// Zero allocations.
pub fn starts_with_keyword_fold(s: &str, keyword_upper: &str) -> bool {
    let sb = s.as_bytes();
    let kb = keyword_upper.as_bytes();
    if sb.len() < kb.len() {
        return false;
    }
    for (i, &k) in kb.iter().enumerate() {
        if ascii_upper(sb[i]) != k {
            return false;
        }
    }
    if sb.len() == kb.len() {
        return true;
    }
    !is_ident_byte(sb[kb.len()])
}

// ─── Internal helpers ─────────────────────────────────────────────────────────

/// Try to match `keyword[ks..ke]` at position `pos` in `s`, allowing flexible
/// whitespace between keyword tokens.  Returns the end position after the
/// match, or `None`.
fn keyword_match_at(s: &[u8], pos: usize, keyword: &[u8], ks: usize, ke: usize) -> Option<usize> {
    let mut j = pos;
    let mut k = ks;

    while k < ke {
        let ck = keyword[k];
        if is_ascii_space(ck) {
            // Skip whitespace block in keyword definition
            while k < ke && is_ascii_space(keyword[k]) {
                k += 1;
            }
            // Require at least one whitespace in the source
            if j >= s.len() || !is_ascii_space(s[j]) {
                return None;
            }
            while j < s.len() && is_ascii_space(s[j]) {
                j += 1;
            }
            continue;
        }
        if j >= s.len() {
            return None;
        }
        if ascii_upper(s[j]) != ascii_upper(ck) {
            return None;
        }
        j += 1;
        k += 1;
    }

    Some(j)
}

fn keyword_left_boundary_ok(s: &[u8], pos: usize, mode: KeywordBoundaryMode) -> bool {
    if pos == 0 {
        return true;
    }
    let prev = s[pos - 1];
    match mode {
        KeywordBoundaryMode::Whitespace => is_ascii_space(prev),
        KeywordBoundaryMode::Word => {
            if prev == b':' {
                return false; // label colon — not a boundary
            }
            !is_ident_byte(prev)
        }
    }
}

fn keyword_right_boundary_ok(s: &[u8], end_pos: usize, mode: KeywordBoundaryMode) -> bool {
    if end_pos >= s.len() {
        return true;
    }
    let next = s[end_pos];
    match mode {
        KeywordBoundaryMode::Whitespace => is_ascii_space(next),
        KeywordBoundaryMode::Word => {
            if next == b':' {
                return false;
            }
            !is_ident_byte(next)
        }
    }
}

/// Trim leading whitespace index.
fn trim_ws_start(s: &[u8]) -> usize {
    let mut i = 0;
    while i < s.len() && is_ascii_space(s[i]) {
        i += 1;
    }
    i
}

/// Trim trailing whitespace — returns exclusive end.
fn trim_ws_end(s: &[u8]) -> usize {
    let mut i = s.len();
    while i > 0 && is_ascii_space(s[i - 1]) {
        i -= 1;
    }
    i
}

// ─── Character helpers (ASCII-only, no allocation) ────────────────────────────

#[inline(always)]
pub fn is_ascii_space(b: u8) -> bool {
    matches!(b, b' ' | b'\t' | b'\n' | b'\r')
}

#[inline(always)]
pub fn ascii_upper(b: u8) -> u8 {
    if b.is_ascii_lowercase() {
        b - (b'a' - b'A')
    } else {
        b
    }
}

#[inline(always)]
pub fn is_ident_byte(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_' || b >= 0x80
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_keyword_index_basic() {
        assert_eq!(keyword_index("MATCH (n) RETURN n", "MATCH"), Some(0));
        assert_eq!(keyword_index("MATCH (n) RETURN n", "RETURN"), Some(10));
        assert_eq!(keyword_index("MATCH (n) RETURN n", "WHERE"), None);
    }

    #[test]
    fn test_keyword_index_case_insensitive() {
        assert_eq!(keyword_index("match (n) return n", "MATCH"), Some(0));
        assert_eq!(keyword_index("Match (n) Return n", "RETURN"), Some(10));
    }

    #[test]
    fn test_keyword_index_word_boundary() {
        // "REMATCH" should NOT match MATCH
        assert_eq!(keyword_index("REMATCH (n)", "MATCH"), None);
        // "MATCHX" should NOT match MATCH
        assert_eq!(keyword_index("MATCHX (n)", "MATCH"), None);
        // Word followed by colon (label) should NOT match
        assert_eq!(keyword_index("(n:MATCH)", "MATCH"), None);
    }

    #[test]
    fn test_keyword_index_skips_string_literals() {
        // RETURN inside a string should not be found as the RETURN clause
        let q = "MATCH (n {desc: 'RETURN value'}) RETURN n";
        assert_eq!(keyword_index(q, "RETURN"), Some(33));
    }

    #[test]
    fn test_keyword_index_skips_nested_parens() {
        let q = "MATCH (n) WHERE (n.age > 10 RETURN 1) RETURN n";
        // The RETURN inside parens should be skipped; only top-level RETURN is found
        assert_eq!(keyword_index(q, "RETURN"), Some(38));
    }

    #[test]
    fn test_top_level_keyword_skips_braces() {
        let q = "CALL { RETURN 1 } RETURN n";
        // top_level_keyword_index skips {…}
        assert_eq!(top_level_keyword_index(q, "RETURN"), Some(18));
        // keyword_index (default) does NOT skip braces, so it finds the inner one
        assert_eq!(keyword_index(q, "RETURN"), Some(7));
    }

    #[test]
    fn test_keyword_index_two_word_keyword() {
        let q = "MATCH (n) ORDER BY n.age RETURN n";
        assert_eq!(keyword_index(q, "ORDER BY"), Some(10));
    }

    #[test]
    fn test_keyword_index_from_offset() {
        let q = "MATCH (n) MATCH (m) RETURN n, m";
        // From offset 0 we get first MATCH
        assert_eq!(
            keyword_index_from(q, "MATCH", 0, KeywordScanOpts::default()),
            Some(0)
        );
        // From offset 1 we skip the first, get second
        assert_eq!(
            keyword_index_from(q, "MATCH", 1, KeywordScanOpts::default()),
            Some(10)
        );
    }

    #[test]
    fn test_starts_with_keyword_fold() {
        assert!(starts_with_keyword_fold("MATCH (n)", "MATCH"));
        assert!(starts_with_keyword_fold("match (n)", "MATCH"));
        assert!(!starts_with_keyword_fold("MATCHX", "MATCH"));
        assert!(!starts_with_keyword_fold("REMA", "MATCH"));
    }

    #[test]
    fn test_keyword_scan_no_panic_empty() {
        assert_eq!(keyword_index("", "MATCH"), None);
        assert_eq!(keyword_index("MATCH (n)", ""), None);
    }

    #[test]
    fn test_keyword_index_skips_line_comments() {
        let q = "MATCH (n) // RETURN fake\nRETURN n";
        assert_eq!(keyword_index(q, "RETURN"), Some(25));
    }

    #[test]
    fn test_keyword_index_skips_block_comments() {
        let q = "MATCH (n) /* RETURN fake */ RETURN n";
        // "MATCH (n) /* RETURN fake */ RETURN n"
        //  0         1         2         3
        //  0123456789012345678901234567890123456
        // The first RETURN is inside a block comment (offset ~11), skipped.
        // The real RETURN is at offset 28 (after the trailing space).
        assert_eq!(keyword_index(q, "RETURN"), Some(28));
    }
}
