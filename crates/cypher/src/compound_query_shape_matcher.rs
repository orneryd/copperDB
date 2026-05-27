use crate::shape_matcher::{ShapeCaptures, ShapeKind, ShapeMatch, ShapeProbe};
use crate::string_patterns::find_keyword_index;

pub fn match_compound_query_shape(query: &str) -> (ShapeMatch, bool) {
    let trimmed = query.trim();
    if trimmed.is_empty() {
        return (ShapeMatch::unknown("compound_query_matcher", trimmed, "empty query"), false);
    }

    if let (match_info, true) = match_compound_create_delete_rel_shape(trimmed) {
        return (match_info, true);
    }
    if let (match_info, true) = match_compound_prop_create_delete_rel_shape(trimmed) {
        return (match_info, true);
    }
    if let (match_info, true) = match_compound_prop_create_delete_return_count_rel_shape(trimmed) {
        return (match_info, true);
    }

    (
        ShapeMatch::unknown(
            "compound_query_matcher",
            trimmed,
            "no matching compound hot-path shape",
        ),
        false,
    )
}

pub fn match_compound_prop_create_delete_return_count_rel_shape(query: &str) -> (ShapeMatch, bool) {
    const MATCHER: &str = "compound_query_prop_create_delete_return_count_rel";
    let mut shape_match = base_shape_match(MATCHER, query);

    if find_keyword_index(query, "MATCH") != Some(0) {
        shape_match.probe.reject_reason = "missing leading MATCH clause".into();
        return (shape_match, false);
    }

    let create_idx = find_keyword_index(query, "CREATE");
    let with_idx = find_keyword_index(query, "WITH");
    let delete_idx = find_keyword_index(query, "DELETE");
    let return_idx = find_keyword_index(query, "RETURN");
    if !matches!(
        (create_idx, with_idx, delete_idx, return_idx),
        (Some(create_idx), Some(with_idx), Some(delete_idx), Some(return_idx))
            if create_idx > 0 && with_idx > create_idx && delete_idx > with_idx && return_idx > delete_idx
    ) {
        shape_match.probe.reject_reason = "compound property WITH/DELETE/RETURN shape not found".into();
        return (shape_match, false);
    }

    let create_idx = create_idx.unwrap();
    let with_idx = with_idx.unwrap();
    let delete_idx = delete_idx.unwrap();
    let return_idx = return_idx.unwrap();

    let match_section = query["MATCH".len()..create_idx].trim();
    let (left_node, right_node) = match split_top_level_comma_shape(match_section) {
        Some(nodes) => nodes,
        None => {
            shape_match.probe.reject_reason = "expected two MATCH node patterns".into();
            return (shape_match, false);
        }
    };

    let left = match parse_labeled_node_pattern(&left_node) {
        Some(node) => node,
        None => {
            shape_match.probe.reject_reason = "invalid left MATCH node pattern".into();
            return (shape_match, false);
        }
    };
    let right = match parse_labeled_node_pattern(&right_node) {
        Some(node) => node,
        None => {
            shape_match.probe.reject_reason = "invalid right MATCH node pattern".into();
            return (shape_match, false);
        }
    };
    let create_match = match parse_create_relationship_clause(&query[create_idx..with_idx]) {
        Some(pattern) => pattern,
        None => {
            shape_match.probe.reject_reason = "invalid CREATE relationship clause".into();
            return (shape_match, false);
        }
    };
    let with_var = match first_clause_word(&query[with_idx + "WITH".len()..delete_idx]) {
        Some(value) => value,
        None => {
            shape_match.probe.reject_reason = "missing WITH variable".into();
            return (shape_match, false);
        }
    };
    let delete_var = match first_clause_word(&query[delete_idx + "DELETE".len()..return_idx]) {
        Some(value) => value,
        None => {
            shape_match.probe.reject_reason = "missing DELETE variable".into();
            return (shape_match, false);
        }
    };
    let count_part = query[return_idx + "RETURN".len()..].replace([' ', '\t'], "");
    let upper = count_part.to_ascii_uppercase();
    if !upper.starts_with("COUNT(") || !count_part.ends_with(')') {
        shape_match.probe.reject_reason = "RETURN clause is not COUNT(var)".into();
        return (shape_match, false);
    }
    let count_var = count_part["COUNT(".len()..count_part.len() - 1].trim().to_string();
    if count_var.is_empty() {
        shape_match.probe.reject_reason = "missing COUNT variable".into();
        return (shape_match, false);
    }
    if create_match.rel_var.is_empty()
        || !with_var.eq_ignore_ascii_case(&create_match.rel_var)
        || !delete_var.eq_ignore_ascii_case(&create_match.rel_var)
        || !count_var.eq_ignore_ascii_case(&create_match.rel_var)
    {
        shape_match.probe.reject_reason = "relationship variable mismatch across WITH/DELETE/RETURN".into();
        return (shape_match, false);
    }

    shape_match.kind = ShapeKind::CompoundPropCreateDeleteReturnCountRel;
    add_prop_shape_captures(&mut shape_match.captures, &left, &right, &create_match);
    shape_match.captures.add_string("with_var", with_var.clone());
    shape_match.captures.add_string("delete_var", delete_var.clone());
    shape_match.captures.add_string("count_var", count_var.clone());
    shape_match.probe.matched = true;
    shape_match
        .probe
        .captured_fields
        .extend([
            ("label1".into(), left.label.clone()),
            ("label2".into(), right.label.clone()),
            ("prop1".into(), left.prop_key.clone()),
            ("prop2".into(), right.prop_key.clone()),
            ("value1".into(), left.prop_value.clone()),
            ("value2".into(), right.prop_value.clone()),
            ("rel_var".into(), create_match.rel_var.clone()),
            ("rel_type".into(), create_match.rel_type.clone()),
            ("with_var".into(), with_var),
            ("delete_var".into(), delete_var),
            ("count_var".into(), count_var),
        ]);
    (shape_match, true)
}

fn match_compound_create_delete_rel_shape(query: &str) -> (ShapeMatch, bool) {
    const MATCHER: &str = "compound_query_create_delete_rel";
    let mut shape_match = base_shape_match(MATCHER, query);

    if find_keyword_index(query, "MATCH") != Some(0) {
        shape_match.probe.reject_reason = "missing leading MATCH clause".into();
        return (shape_match, false);
    }

    let with_idx = find_keyword_index(query, "WITH");
    let limit_idx = find_keyword_index(query, "LIMIT");
    let create_idx = find_keyword_index(query, "CREATE");
    let delete_idx = find_keyword_index(query, "DELETE");
    if !matches!(
        (with_idx, limit_idx, create_idx, delete_idx),
        (Some(with_idx), Some(limit_idx), Some(create_idx), Some(delete_idx))
            if with_idx > 0 && limit_idx > with_idx && create_idx > limit_idx && delete_idx > create_idx
    ) {
        shape_match.probe.reject_reason = "compound WITH/LIMIT/CREATE/DELETE shape not found".into();
        return (shape_match, false);
    }

    let with_idx = with_idx.unwrap();
    let limit_idx = limit_idx.unwrap();
    let create_idx = create_idx.unwrap();
    let delete_idx = delete_idx.unwrap();

    let match_section = query["MATCH".len()..with_idx].trim();
    let (left_node, right_node) = match split_top_level_comma_shape(match_section) {
        Some(nodes) => nodes,
        None => {
            shape_match.probe.reject_reason = "expected two MATCH node patterns".into();
            return (shape_match, false);
        }
    };
    let left = match parse_labeled_node_pattern(&left_node) {
        Some(node) => node,
        None => {
            shape_match.probe.reject_reason = "invalid left MATCH node pattern".into();
            return (shape_match, false);
        }
    };
    let right = match parse_labeled_node_pattern(&right_node) {
        Some(node) => node,
        None => {
            shape_match.probe.reject_reason = "invalid right MATCH node pattern".into();
            return (shape_match, false);
        }
    };
    let create_match = match parse_create_relationship_clause(&query[create_idx..delete_idx]) {
        Some(pattern) => pattern,
        None => {
            shape_match.probe.reject_reason = "invalid CREATE relationship clause".into();
            return (shape_match, false);
        }
    };
    let delete_var = match first_clause_word(&query[delete_idx + "DELETE".len()..]) {
        Some(value) => value,
        None => {
            shape_match.probe.reject_reason = "missing DELETE variable".into();
            return (shape_match, false);
        }
    };
    let limit_fields: Vec<&str> = query[limit_idx + "LIMIT".len()..create_idx]
        .split_whitespace()
        .collect();
    if limit_fields.is_empty() {
        shape_match.probe.reject_reason = "missing LIMIT literal".into();
        return (shape_match, false);
    }
    let limit = match limit_fields[0].parse::<i64>() {
        Ok(value) if value >= 0 => value,
        _ => {
            shape_match.probe.reject_reason = "invalid LIMIT literal".into();
            return (shape_match, false);
        }
    };

    shape_match.kind = ShapeKind::CompoundCreateDeleteRel;
    shape_match.captures.add_string("label1", left.label.clone());
    shape_match.captures.add_string("label2", right.label.clone());
    shape_match.captures.add_string("rel_var", create_match.rel_var.clone());
    shape_match.captures.add_string("rel_type", create_match.rel_type.clone());
    shape_match.captures.add_string("delete_var", delete_var.clone());
    shape_match.captures.add_int("limit", limit);
    shape_match.probe.matched = true;
    shape_match.probe.captured_fields.extend([
        ("label1".into(), left.label),
        ("label2".into(), right.label),
        ("rel_var".into(), create_match.rel_var),
        ("rel_type".into(), create_match.rel_type),
        ("delete_var".into(), delete_var),
        ("limit".into(), limit.to_string()),
    ]);
    (shape_match, true)
}

fn match_compound_prop_create_delete_rel_shape(query: &str) -> (ShapeMatch, bool) {
    const MATCHER: &str = "compound_query_prop_create_delete_rel";
    let mut shape_match = base_shape_match(MATCHER, query);

    if find_keyword_index(query, "MATCH") != Some(0) {
        shape_match.probe.reject_reason = "missing leading MATCH clause".into();
        return (shape_match, false);
    }

    let create_idx = find_keyword_index(query, "CREATE");
    let delete_idx = find_keyword_index(query, "DELETE");
    if !matches!((create_idx, delete_idx), (Some(create_idx), Some(delete_idx)) if create_idx > 0 && delete_idx > create_idx) {
        shape_match.probe.reject_reason = "compound property CREATE/DELETE shape not found".into();
        return (shape_match, false);
    }

    let create_idx = create_idx.unwrap();
    let delete_idx = delete_idx.unwrap();

    let match_section = query["MATCH".len()..create_idx].trim();
    let (left_node, right_node) = match split_top_level_comma_shape(match_section) {
        Some(nodes) => nodes,
        None => {
            shape_match.probe.reject_reason = "expected two MATCH node patterns".into();
            return (shape_match, false);
        }
    };
    let left = match parse_labeled_node_pattern(&left_node) {
        Some(node) => node,
        None => {
            shape_match.probe.reject_reason = "invalid left MATCH node pattern".into();
            return (shape_match, false);
        }
    };
    let right = match parse_labeled_node_pattern(&right_node) {
        Some(node) => node,
        None => {
            shape_match.probe.reject_reason = "invalid right MATCH node pattern".into();
            return (shape_match, false);
        }
    };
    let create_match = match parse_create_relationship_clause(&query[create_idx..delete_idx]) {
        Some(pattern) => pattern,
        None => {
            shape_match.probe.reject_reason = "invalid CREATE relationship clause".into();
            return (shape_match, false);
        }
    };
    let delete_var = match first_clause_word(&query[delete_idx + "DELETE".len()..]) {
        Some(value) => value,
        None => {
            shape_match.probe.reject_reason = "missing DELETE variable".into();
            return (shape_match, false);
        }
    };

    shape_match.kind = ShapeKind::CompoundPropCreateDeleteRel;
    add_prop_shape_captures(&mut shape_match.captures, &left, &right, &create_match);
    shape_match.captures.add_string("delete_var", delete_var.clone());
    shape_match.probe.matched = true;
    shape_match.probe.captured_fields.extend([
        ("label1".into(), left.label),
        ("label2".into(), right.label),
        ("prop1".into(), left.prop_key),
        ("prop2".into(), right.prop_key),
        ("value1".into(), left.prop_value),
        ("value2".into(), right.prop_value),
        ("rel_var".into(), create_match.rel_var),
        ("rel_type".into(), create_match.rel_type),
        ("delete_var".into(), delete_var),
    ]);
    (shape_match, true)
}

fn add_prop_shape_captures(
    captures: &mut ShapeCaptures,
    left: &ParsedNodePattern,
    right: &ParsedNodePattern,
    create_match: &ParsedCreatePattern,
) {
    captures.add_string("label1", left.label.clone());
    captures.add_string("label2", right.label.clone());
    captures.add_string("prop1", left.prop_key.clone());
    captures.add_string("prop2", right.prop_key.clone());
    captures.add_string("value1", left.prop_value.clone());
    captures.add_string("value2", right.prop_value.clone());
    captures.add_string("rel_var", create_match.rel_var.clone());
    captures.add_string("rel_type", create_match.rel_type.clone());
}

fn base_shape_match(matcher: &str, query: &str) -> ShapeMatch {
    ShapeMatch {
        kind: ShapeKind::Unknown,
        captures: ShapeCaptures::new(),
        probe: ShapeProbe {
            matcher: matcher.to_string(),
            matched: false,
            reject_reason: String::new(),
            normalized_query: query.trim().to_string(),
            captured_fields: Default::default(),
        },
    }
}

#[derive(Debug, Clone)]
struct ParsedNodePattern {
    label: String,
    prop_key: String,
    prop_value: String,
}

#[derive(Debug, Clone)]
struct ParsedCreatePattern {
    rel_var: String,
    rel_type: String,
}

fn split_top_level_comma_shape(s: &str) -> Option<(String, String)> {
    let mut depth_paren = 0;
    let mut depth_brace = 0;
    let mut depth_bracket = 0;
    let mut in_single = false;
    let mut in_double = false;

    for (index, ch) in s.char_indices() {
        match ch {
            '\'' if !in_double => in_single = !in_single,
            '"' if !in_single => in_double = !in_double,
            '(' if !in_single && !in_double => depth_paren += 1,
            ')' if !in_single && !in_double => depth_paren -= 1,
            '{' if !in_single && !in_double => depth_brace += 1,
            '}' if !in_single && !in_double => depth_brace -= 1,
            '[' if !in_single && !in_double => depth_bracket += 1,
            ']' if !in_single && !in_double => depth_bracket -= 1,
            ',' if !in_single && !in_double && depth_paren == 0 && depth_brace == 0 && depth_bracket == 0 => {
                let left = s[..index].trim();
                let right = s[index + 1..].trim();
                if left.is_empty() || right.is_empty() {
                    return None;
                }
                return Some((left.to_string(), right.to_string()));
            }
            _ => {}
        }
    }
    None
}

fn first_clause_word(s: &str) -> Option<String> {
    s.split_whitespace().next().map(|value| value.trim().to_string())
}

fn parse_labeled_node_pattern(s: &str) -> Option<ParsedNodePattern> {
    let s = s.trim();
    if !s.starts_with('(') || !s.ends_with(')') {
        return None;
    }
    let inner = s[1..s.len() - 1].trim();
    if inner.is_empty() {
        return None;
    }
    let (_var_name, rest) = parse_identifier_token(inner)?;
    let rest = rest.trim();
    if !rest.starts_with(':') {
        return None;
    }
    let (label, rest) = parse_identifier_token(rest[1..].trim())?;
    let mut node = ParsedNodePattern {
        label,
        prop_key: String::new(),
        prop_value: String::new(),
    };
    let rest = rest.trim();
    if rest.is_empty() {
        return Some(node);
    }
    if !rest.starts_with('{') || !rest.ends_with('}') {
        return None;
    }
    let (prop_key, prop_value) = parse_single_property_assignment(rest[1..rest.len() - 1].trim())?;
    node.prop_key = prop_key;
    node.prop_value = prop_value;
    Some(node)
}

fn parse_single_property_assignment(s: &str) -> Option<(String, String)> {
    let (key, value) = s.split_once(':')?;
    let key = key.trim();
    let value = value.trim();
    if key.is_empty() || value.is_empty() {
        return None;
    }
    let (key, rest) = parse_identifier_token(key)?;
    if !rest.trim().is_empty() {
        return None;
    }
    Some((key, value.to_string()))
}

fn parse_create_relationship_clause(s: &str) -> Option<ParsedCreatePattern> {
    let s = s.trim();
    if !s.to_ascii_uppercase().starts_with("CREATE") {
        return None;
    }
    let body = s["CREATE".len()..].trim();
    let (_left, rest) = parse_bare_node_reference(body)?;
    let rest = rest.trim();
    if !rest.starts_with('-') {
        return None;
    }
    let rest = rest[1..].trim();
    if !rest.starts_with('[') {
        return None;
    }
    let (rel_spec, rest) = extract_bracket_section(rest)?;
    let rest = rest.trim();
    if !rest.starts_with("->") {
        return None;
    }
    let (_right, tail) = parse_bare_node_reference(rest[2..].trim())?;
    if !tail.trim().is_empty() {
        return None;
    }
    let (rel_var, rel_type) = rel_spec.trim().split_once(':')?;
    let (rel_var, rel_var_rest) = parse_identifier_token(rel_var.trim())?;
    let (rel_type, rel_type_rest) = parse_identifier_token(rel_type.trim())?;
    if !rel_var_rest.trim().is_empty() || !rel_type_rest.trim().is_empty() {
        return None;
    }
    Some(ParsedCreatePattern { rel_var, rel_type })
}

fn parse_bare_node_reference(s: &str) -> Option<(String, &str)> {
    let s = s.trim();
    if !s.starts_with('(') {
        return None;
    }
    let (inside, rest) = extract_paren_section(s)?;
    let inside = inside.trim();
    if inside.is_empty() {
        return None;
    }
    let (name, trailing) = parse_identifier_token(inside)?;
    if !trailing.trim().is_empty() {
        return None;
    }
    Some((name, rest))
}

fn parse_identifier_token(s: &str) -> Option<(String, &str)> {
    let s = s.trim();
    let first = s.as_bytes().first().copied()?;
    if !is_word_char(first) || first.is_ascii_digit() {
        return None;
    }
    let mut end = 1;
    while end < s.len() && is_word_char(s.as_bytes()[end]) {
        end += 1;
    }
    Some((s[..end].to_string(), &s[end..]))
}

fn extract_paren_section(s: &str) -> Option<(&str, &str)> {
    if !s.starts_with('(') {
        return None;
    }
    let mut depth = 0;
    let mut in_single = false;
    let mut in_double = false;
    for (index, ch) in s.char_indices() {
        match ch {
            '\'' if !in_double => in_single = !in_single,
            '"' if !in_single => in_double = !in_double,
            '(' if !in_single && !in_double => depth += 1,
            ')' if !in_single && !in_double => {
                depth -= 1;
                if depth == 0 {
                    return Some((&s[1..index], &s[index + 1..]));
                }
            }
            _ => {}
        }
    }
    None
}

fn extract_bracket_section(s: &str) -> Option<(&str, &str)> {
    if !s.starts_with('[') {
        return None;
    }
    let mut depth = 0;
    let mut in_single = false;
    let mut in_double = false;
    for (index, ch) in s.char_indices() {
        match ch {
            '\'' if !in_double => in_single = !in_single,
            '"' if !in_single => in_double = !in_double,
            '[' if !in_single && !in_double => depth += 1,
            ']' if !in_single && !in_double => {
                depth -= 1;
                if depth == 0 {
                    return Some((&s[1..index], &s[index + 1..]));
                }
            }
            _ => {}
        }
    }
    None
}

fn is_word_char(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || byte == b'_'
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pending_shape_execution_todo;

    #[test]
    fn test_match_compound_query_shape_create_delete_rel() {
        let (shape_match, ok) = match_compound_query_shape(
            "MATCH (a:Actor), (m:Movie) WITH a, m LIMIT 1 CREATE (a)-[r:TEMP_REL]->(m) DELETE r",
        );
        assert!(ok);
        assert_eq!(shape_match.kind, ShapeKind::CompoundCreateDeleteRel);
        assert!(shape_match.probe.matched);
        assert_eq!(shape_match.captures.string("label1"), "Actor");
        assert_eq!(shape_match.captures.string("label2"), "Movie");
        assert_eq!(shape_match.captures.string("rel_var"), "r");
        assert_eq!(shape_match.captures.string("rel_type"), "TEMP_REL");
        assert_eq!(shape_match.captures.string("delete_var"), "r");
        assert_eq!(shape_match.captures.int("limit"), 1);
        assert!(pending_shape_execution_todo(shape_match.kind).is_some());
    }

    #[test]
    fn test_match_compound_query_shape_prop_create_delete_rel() {
        let (shape_match, ok) = match_compound_query_shape(
            "MATCH (p1:Person {id: 1}), (p2:Person {id: 2}) CREATE (p1)-[r:TEMP_KNOWS]->(p2) DELETE r",
        );
        assert!(ok);
        assert_eq!(shape_match.kind, ShapeKind::CompoundPropCreateDeleteRel);
        assert!(shape_match.probe.matched);
        assert_eq!(shape_match.captures.string("label1"), "Person");
        assert_eq!(shape_match.captures.string("prop1"), "id");
        assert_eq!(shape_match.captures.string("value1"), "1");
        assert_eq!(shape_match.captures.string("value2"), "2");
        assert_eq!(shape_match.captures.string("rel_type"), "TEMP_KNOWS");
    }

    #[test]
    fn test_match_compound_query_shape_prop_create_delete_return_count_rel() {
        let (shape_match, ok) = match_compound_query_shape(
            "MATCH (s:Supplier {supplierID: 1}), (p:Product {productID: 2}) CREATE (s)-[r:TEST_REL]->(p) WITH r DELETE r RETURN count(r)",
        );
        assert!(ok);
        assert_eq!(
            shape_match.kind,
            ShapeKind::CompoundPropCreateDeleteReturnCountRel
        );
        assert!(shape_match.probe.matched);
        assert_eq!(shape_match.captures.string("rel_var"), "r");
        assert_eq!(shape_match.captures.string("with_var"), "r");
        assert_eq!(shape_match.captures.string("delete_var"), "r");
        assert_eq!(shape_match.captures.string("count_var"), "r");
    }

    #[test]
    fn test_match_compound_query_shape_rejects_bad_delete_var() {
        let (shape_match, ok) = match_compound_prop_create_delete_return_count_rel_shape(
            "MATCH (s:Supplier {supplierID: 1}), (p:Product {productID: 2}) CREATE (s)-[r:TEST_REL]->(p) WITH r DELETE x RETURN count(r)",
        );
        assert!(!ok);
        assert_eq!(shape_match.kind, ShapeKind::Unknown);
        assert!(!shape_match.probe.matched);
        assert!(shape_match.probe.reject_reason.contains("mismatch"));
    }
}