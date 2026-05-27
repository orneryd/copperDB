use crate::{keyword_scan::is_ascii_space, CypherError};

const SINGLE_CHAR_TOKENS: &[char] = &[
    '(', ')', '[', ']', '{', '}', ':', ',', '.', '=', '<', '>', '-', '+', '*', '/',
];

/// Tokenize a Cypher string.
///
/// Rules:
/// - Split on whitespace.
/// - Split on single-char tokens (see `SINGLE_CHAR_TOKENS`), each becomes its own token.
/// - Quoted strings (`'...'` or `"..."`) are kept as a single token including the quotes.
///   String scanning uses the same escape-aware logic as [`crate::keyword_scan`] so that
///   keywords inside quoted values are never mistaken for clause delimiters.
/// - `<>`, `<=`, `>=`, `!=`, `=~` are kept as two-character tokens.
pub fn tokenize(input: &str) -> Result<Vec<String>, CypherError> {
    let sb = input.as_bytes();
    let len = sb.len();
    let mut tokens: Vec<String> = Vec::new();
    let mut i = 0;

    while i < len {
        let b = sb[i];

        if is_ascii_space(b) {
            i += 1;
            continue;
        }

        if b == b'\'' || b == b'"' {
            let quote = b;
            let mut s = String::new();
            s.push(b as char);
            i += 1;
            loop {
                if i >= len {
                    return Err(CypherError::UnterminatedString);
                }
                let c = sb[i];
                if c == b'\\' && i + 1 < len {
                    s.push(c as char);
                    s.push(sb[i + 1] as char);
                    i += 2;
                    continue;
                }
                if c == quote {
                    if i + 1 < len && sb[i + 1] == quote {
                        s.push(c as char);
                        s.push(c as char);
                        i += 2;
                        continue;
                    }
                    s.push(c as char);
                    i += 1;
                    break;
                }
                s.push(c as char);
                i += 1;
            }
            tokens.push(s);
            continue;
        }

        if i + 1 < len {
            let pair = (b, sb[i + 1]);
            if matches!(
                pair,
                (b'<', b'>') | (b'<', b'=') | (b'>', b'=') | (b'!', b'=') | (b'=', b'~')
            ) {
                tokens.push(format!("{}{}", b as char, sb[i + 1] as char));
                i += 2;
                continue;
            }
        }

        if b.is_ascii() && SINGLE_CHAR_TOKENS.contains(&(b as char)) {
            tokens.push((b as char).to_string());
            i += 1;
            continue;
        }

        let start = i;
        while i < len {
            let c = sb[i];
            if is_ascii_space(c) {
                break;
            }
            if c.is_ascii() && SINGLE_CHAR_TOKENS.contains(&(c as char)) {
                break;
            }
            i += 1;
        }
        if i > start {
            tokens.push(input[start..i].to_owned());
        }
    }

    Ok(tokens)
}
