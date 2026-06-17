use crate::{keyword_scan::is_ascii_space, CypherError};

#[inline]
fn is_single_char_token(byte: u8) -> bool {
    matches!(
        byte,
        b'(' | b')'
            | b'['
            | b']'
            | b'{'
            | b'}'
            | b':'
            | b','
            | b'.'
            | b'='
            | b'<'
            | b'>'
            | b'-'
            | b'+'
            | b'*'
            | b'/'
    )
}

/// Tokenize a Cypher string.
///
/// Rules:
/// - Split on whitespace.
/// - Split on single-char tokens (see `SINGLE_CHAR_TOKENS`), each becomes its own token.
/// - Quoted strings (`'...'` or `"..."`) are kept as a single token including the quotes.
///   String scanning uses the same escape-aware logic as [`crate::keyword_scan`] so that
///   keywords inside quoted values are never mistaken for clause delimiters.
/// - `<>`, `<=`, `>=`, `!=`, `=~` are kept as two-character tokens.
pub fn tokenize(input: &str) -> Result<Vec<&str>, CypherError> {
    let sb = input.as_bytes();
    let len = sb.len();
    let mut tokens: Vec<&str> = Vec::with_capacity((len / 3).max(8));
    let mut i = 0;

    while i < len {
        let b = sb[i];

        if is_ascii_space(b) {
            i += 1;
            continue;
        }

        if b == b'\'' || b == b'"' {
            let quote = b;
            let start = i;
            i += 1;
            loop {
                if i >= len {
                    return Err(CypherError::UnterminatedString);
                }
                let c = sb[i];
                if c == b'\\' && i + 1 < len {
                    i += 2;
                    continue;
                }
                if c == quote {
                    if i + 1 < len && sb[i + 1] == quote {
                        i += 2;
                        continue;
                    }
                    i += 1;
                    break;
                }
                i += 1;
            }
            tokens.push(&input[start..i]);
            continue;
        }

        // Backtick-quoted identifier: `anything.here` — treated as single token
        if b == b'`' {
            let start = i;
            i += 1;
            while i < len {
                if sb[i] == b'`' {
                    i += 1;
                    break;
                }
                i += 1;
            }
            if i > len || sb.get(i - 1).copied() != Some(b'`') {
                // Unterminated backtick — fall through to normal scanning
                i = start;
            } else {
                tokens.push(&input[start..i]);
                continue;
            }
        }

        if i + 1 < len {
            let pair = (b, sb[i + 1]);
            if matches!(
                pair,
                (b'<', b'>') | (b'<', b'=') | (b'>', b'=') | (b'!', b'=') | (b'=', b'~')
                | (b'+', b'=')
            ) {
                tokens.push(&input[i..i + 2]);
                i += 2;
                continue;
            }
        }

        if is_single_char_token(b) {
            tokens.push(&input[i..i + 1]);
            i += 1;
            continue;
        }

        let start = i;
        while i < len {
            let c = sb[i];
            if is_ascii_space(c) {
                break;
            }
            if is_single_char_token(c) {
                break;
            }
            i += 1;
        }
        if i > start {
            tokens.push(&input[start..i]);
        }
    }

    Ok(tokens)
}
