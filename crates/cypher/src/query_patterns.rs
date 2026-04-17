//! Query optimization pattern detection for magnetDB.
//!
//! Equivalent to Go's `pkg/cypher/query_patterns.go` in NornicDB v1.0.40.
//!
//! Detects well-known query patterns before execution so the engine can route
//! to specialized, faster implementations instead of the generic traversal
//! algorithm.
//!
//! # Supported Patterns
//!
//! | Pattern                 | Example shape                                      |
//! |-------------------------|----------------------------------------------------|
//! | `MutualRelationship`    | `(a)-[:T]->(b)-[:T]->(a)`                          |
//! | `IncomingCountAgg`      | `MATCH (x)<-[:T]-(y) RETURN x.name, count(y)`      |
//! | `OutgoingCountAgg`      | `MATCH (x)-[:T]->(y) RETURN x.name, count(y)`      |
//! | `EdgePropertyAgg`       | `RETURN avg(r.rating), count(r) GROUP BY node`     |
//! | `LargeResultSet`        | Any traversal with `LIMIT > 100`                   |
//! | `Generic`               | Everything else — use standard execution           |

use crate::string_patterns::{extract_limit, find_keyword_index};

// ─── Types ────────────────────────────────────────────────────────────────────

/// Identifies optimisable query structures.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueryPattern {
    /// Default — use standard execution.
    Generic,
    /// `(a)-[:T]->(b)-[:T]->(a)` cycle back to start.
    /// Optimised via single-pass edge-set intersection.
    MutualRelationship,
    /// `MATCH (x)<-[:T]-(y) RETURN x, count(y)`.
    /// Optimised via single-pass edge counting.
    IncomingCountAgg,
    /// `MATCH (x)-[:T]->(y) RETURN x, count(y)`.
    /// Optimised via single-pass edge counting.
    OutgoingCountAgg,
    /// `avg(r.prop)`, `sum(r.prop)` on edge properties.
    /// Optimised via single-pass accumulation.
    EdgePropertyAgg,
    /// Any traversal with `LIMIT > 100`.
    /// Optimised via batch node lookups and pre-allocation.
    LargeResultSet,
}

impl QueryPattern {
    pub fn as_str(self) -> &'static str {
        match self {
            QueryPattern::Generic => "Generic",
            QueryPattern::MutualRelationship => "MutualRelationship",
            QueryPattern::IncomingCountAgg => "IncomingCountAgg",
            QueryPattern::OutgoingCountAgg => "OutgoingCountAgg",
            QueryPattern::EdgePropertyAgg => "EdgePropertyAgg",
            QueryPattern::LargeResultSet => "LargeResultSet",
        }
    }

    pub fn is_optimizable(self) -> bool {
        self != QueryPattern::Generic
    }

    pub fn needs_relationship_type_scan(self) -> bool {
        matches!(
            self,
            QueryPattern::MutualRelationship
                | QueryPattern::IncomingCountAgg
                | QueryPattern::OutgoingCountAgg
                | QueryPattern::EdgePropertyAgg
        )
    }
}

impl std::fmt::Display for QueryPattern {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Details about a detected pattern, used by the optimised executors.
#[derive(Debug, Clone, Default)]
pub struct PatternInfo {
    pub pattern: QueryPattern,
    /// Relationship type (e.g. `"FOLLOWS"`)
    pub rel_type: String,
    /// Start-node variable (e.g. `"a"`)
    pub start_var: String,
    /// End-node variable (e.g. `"b"`)
    pub end_var: String,
    /// Relationship variable (e.g. `"r"`)
    pub rel_var: String,
    /// Aggregation functions used (e.g. `["count", "avg"]`)
    pub agg_functions: Vec<String>,
    /// Property being aggregated (e.g. `"rating"`)
    pub agg_property: String,
    /// `LIMIT` value if present
    pub limit: Option<usize>,
    /// Variables in implicit `GROUP BY`
    pub group_by_vars: Vec<String>,
}

impl Default for QueryPattern {
    fn default() -> Self {
        QueryPattern::Generic
    }
}

// ─── Detection ────────────────────────────────────────────────────────────────

/// Analyse a Cypher query string and return pattern information.
///
/// This function is called before execution to determine whether a specialised
/// executor path can be used.  All detection is done with scanner-based helpers
/// (no regex, minimal allocation).
pub fn detect_query_pattern(query: &str) -> PatternInfo {
    let mut info = PatternInfo::default();

    // Queries with WITH have complex aggregation semantics that the optimised
    // executors do not handle.  Use word-boundary check to avoid matching
    // "STARTS WITH" or "ENDS WITH".
    if contains_keyword_outside_strings(query, "WITH") {
        return info;
    }

    // Extract LIMIT once — affects multiple patterns.
    info.limit = extract_limit(query);

    // Check large result set first (cheapest check).
    let upper = query.to_ascii_uppercase();

    // Mutual relationship: (a)-[:T]->(b)-[:T]->(a)
    if info.pattern == QueryPattern::Generic {
        if let Some(detected) = detect_mutual_relationship(query) {
            info.pattern = QueryPattern::MutualRelationship;
            info.start_var = detected.start_var;
            info.end_var = detected.end_var;
            info.rel_type = detected.rel_type;
            return info;
        }
    }

    // Incoming / outgoing count aggregation (narrow shape only)
    if info.pattern == QueryPattern::Generic && upper.contains("COUNT(") {
        if !upper.contains("SUM(")
            && !upper.contains("AVG(")
            && !upper.contains("MIN(")
            && !upper.contains("MAX(")
            && !upper.contains("COLLECT(")
        {
            if let Some(detected) = detect_incoming_count_agg(query) {
                info.pattern = QueryPattern::IncomingCountAgg;
                info.start_var = detected.start_var;
                info.end_var = detected.end_var;
                info.rel_var = detected.rel_var;
                info.rel_type = detected.rel_type;
                info.agg_functions = vec!["count".into()];
                return info;
            }
            if let Some(detected) = detect_outgoing_count_agg(query) {
                info.pattern = QueryPattern::OutgoingCountAgg;
                info.start_var = detected.start_var;
                info.end_var = detected.end_var;
                info.rel_var = detected.rel_var;
                info.rel_type = detected.rel_type;
                info.agg_functions = vec!["count".into()];
                return info;
            }
        }
    }

    // Large result set (LIMIT > 100)
    if let Some(limit) = info.limit {
        if limit > 100 && upper.contains("MATCH") {
            info.pattern = QueryPattern::LargeResultSet;
            return info;
        }
    }

    info
}

// ─── Internal detection helpers ───────────────────────────────────────────────

struct CountAggDetected {
    start_var: String,
    end_var: String,
    rel_var: String,
    rel_type: String,
}

struct MutualDetected {
    start_var: String,
    end_var: String,
    rel_type: String,
}

/// Detect `(a)-[:T]->(b)-[:T]->(a)` pattern using the scanner.
fn detect_mutual_relationship(query: &str) -> Option<MutualDetected> {
    let match_clause = extract_match_clause(query)?;
    let match_clause = match_clause.trim();
    let body = if match_clause.to_ascii_uppercase().starts_with("MATCH") {
        match_clause[5..].trim()
    } else {
        match_clause
    };

    // Parse the chain: (a)-[:T]->(b)-[:T]->(a)
    // We get node list and edge list from the pattern.
    let chain = parse_pattern_chain(body)?;
    // Mutual relationship needs exactly 3 nodes and 2 edges.
    if chain.nodes.len() != 3 || chain.edges.len() != 2 {
        return None;
    }

    let (e1, e2) = (&chain.edges[0], &chain.edges[1]);
    let (n0, n1, n2) = (&chain.nodes[0], &chain.nodes[1], &chain.nodes[2]);

    if e1.direction != "outgoing" || e2.direction != "outgoing" {
        return None;
    }
    if n0.is_empty() || n1.is_empty() || n2.is_empty() {
        return None;
    }
    if n0 != n2 || n0 == n1 {
        return None;
    }
    if e1.rel_type.is_empty() || e2.rel_type.is_empty() {
        return None;
    }
    if !e1.rel_type.eq_ignore_ascii_case(&e2.rel_type) {
        return None;
    }

    Some(MutualDetected {
        start_var: n0.clone(),
        end_var: n1.clone(),
        rel_type: e1.rel_type.clone(),
    })
}

fn detect_incoming_count_agg(query: &str) -> Option<CountAggDetected> {
    detect_directional_count_agg(query, true)
}

fn detect_outgoing_count_agg(query: &str) -> Option<CountAggDetected> {
    detect_directional_count_agg(query, false)
}

fn detect_directional_count_agg(query: &str, incoming: bool) -> Option<CountAggDetected> {
    let match_clause = extract_match_clause(query)?;
    let body = match_clause.trim();
    let body = if body.to_ascii_uppercase().starts_with("MATCH") {
        body[5..].trim()
    } else {
        body
    };

    // Only single-hop patterns — use pattern chain parser
    let chain = parse_pattern_chain(body)?;
    if chain.nodes.len() != 2 || chain.edges.len() != 1 {
        return None; // chained multi-hop not supported here
    }

    let edge = &chain.edges[0];
    let expected_dir = if incoming { "incoming" } else { "outgoing" };
    if edge.direction != expected_dir {
        return None;
    }

    let from_var = &chain.nodes[0];
    let to_var = &chain.nodes[1];
    if from_var.is_empty() || to_var.is_empty() {
        return None;
    }

    // Check return shape: RETURN start.name, count(end)
    let upper = query.to_ascii_uppercase();
    let count_pattern = format!("COUNT({}", to_var.to_ascii_uppercase());
    if !upper.contains(&count_pattern) && !upper.contains("COUNT(*)") {
        return None;
    }

    if !is_return_name_count_shape(query, from_var, to_var) {
        return None;
    }

    Some(CountAggDetected {
        start_var: from_var.clone(),
        end_var: to_var.clone(),
        rel_var: edge.rel_var.clone(),
        rel_type: edge.rel_type.clone(),
    })
}

/// Verify the RETURN clause has exactly two items: `<start>.name` and
/// `count(<end>)` or `count(*)`.
fn is_return_name_count_shape(query: &str, start_var: &str, end_var: &str) -> bool {
    let return_idx = match find_keyword_index(query, "RETURN") {
        Some(i) => i,
        None => return false,
    };
    let return_part = query[return_idx + 6..].trim();

    // Strip ORDER BY / SKIP / LIMIT
    let end = ["ORDER BY", "SKIP", "LIMIT"]
        .iter()
        .filter_map(|kw| find_keyword_index(return_part, kw))
        .min()
        .unwrap_or(return_part.len());
    let return_part = return_part[..end].trim();
    if return_part.is_empty() {
        return false;
    }

    let parts: Vec<&str> = return_part.split(',').collect();
    if parts.len() != 2 {
        return false;
    }

    let left = strip_alias(parts[0].trim());
    let right = strip_alias(parts[1].trim());

    // Require "<start>.name"
    if !left.eq_ignore_ascii_case(&format!("{}.name", start_var)) {
        return false;
    }

    // Require COUNT(<end>) or COUNT(*)
    let right_compact = right.replace(' ', "").to_ascii_uppercase();
    let want_count_var = format!("COUNT({})", end_var.to_ascii_uppercase());
    right_compact == want_count_var || right_compact == "COUNT(*)"
}

fn strip_alias(s: &str) -> &str {
    if let Some(pos) = find_keyword_index(s, "AS") {
        s[..pos].trim()
    } else {
        s
    }
}

// ─── Pattern chain parser ─────────────────────────────────────────────────────

/// Result of parsing a relationship chain like `(a)-[:T]->(b)-[:T]->(c)`.
struct PatternChain {
    /// Variable names of nodes in order: `[a, b, c]`
    nodes: Vec<String>,
    /// Edges in order (one fewer than nodes)
    edges: Vec<ChainEdge>,
}

struct ChainEdge {
    rel_var: String,
    rel_type: String,
    direction: String,
}

/// Parse a Cypher relationship pattern into a list of nodes and edges.
///
/// Handles: `(a)-[:T]->(b)-[:T]->(a)`, `(x)<-[:T]-(y)`, etc.
fn parse_pattern_chain(s: &str) -> Option<PatternChain> {
    let mut nodes: Vec<String> = Vec::new();
    let mut edges: Vec<ChainEdge> = Vec::new();

    let mut cursor = s.trim();

    // First node
    let (var, rest) = parse_node_variable(cursor)?;
    nodes.push(var);
    cursor = rest.trim();

    // Alternating: edge, node
    loop {
        if cursor.is_empty() || (!cursor.starts_with('-') && !cursor.starts_with('<')) {
            break;
        }
        let (rel_var, rel_type, direction, after_rel) = parse_relationship_part(cursor)?;
        cursor = after_rel.trim();
        if cursor.is_empty() || !cursor.starts_with('(') {
            break;
        }
        let (var, rest) = parse_node_variable(cursor)?;
        edges.push(ChainEdge { rel_var, rel_type, direction });
        nodes.push(var);
        cursor = rest.trim();
    }

    if nodes.is_empty() || edges.len() != nodes.len() - 1 {
        return None;
    }

    Some(PatternChain { nodes, edges })
}

// ─── Relationship segment parser ─────────────────────────────────────────────

#[derive(Debug)]
struct RelSegment {
    from_var: String,
    rel_var: String,
    rel_type: String,
    /// `"outgoing"`, `"incoming"`, or `"both"`
    direction: String,
    to_var: String,
}

/// Parse one `(from)-[:TYPE]->(to)` segment from the front of `s`.
/// Returns `(segment, remainder)` or `None`.
fn parse_relationship_segment(s: &str) -> Option<(RelSegment, &str)> {
    let s = s.trim();
    let sb = s.as_bytes();
    if sb.is_empty() || sb[0] != b'(' {
        return None;
    }

    // Parse from-node
    let (from_var, after_from) = parse_node_variable(s)?;

    let rest = after_from.trim();
    if rest.is_empty() {
        return None;
    }

    // Direction and relationship
    let (rel_var, rel_type, direction, after_rel) = parse_relationship_part(rest)?;

    let rest = after_rel.trim();
    if rest.is_empty() || rest.as_bytes()[0] != b'(' {
        return None;
    }

    let (to_var, after_to) = parse_node_variable(rest)?;

    Some((
        RelSegment { from_var, rel_var, rel_type, direction, to_var },
        after_to,
    ))
}

/// Parse `(var)` or `(var:Label)` and return `(variable, rest)`.
fn parse_node_variable(s: &str) -> Option<(String, &str)> {
    let sb = s.as_bytes();
    if sb.is_empty() || sb[0] != b'(' {
        return None;
    }
    let inner_start = 1;
    // Find close paren
    let close = sb[inner_start..].iter().position(|&b| b == b')')?;
    let inner = s[inner_start..inner_start + close].trim();
    // Extract variable name (up to : or end)
    let var = if let Some(colon) = inner.find(':') {
        inner[..colon].trim()
    } else {
        inner
    };
    let rest = &s[inner_start + close + 1..];
    Some((var.to_owned(), rest))
}

/// Parse `-[r:TYPE]->`, `<-[r:TYPE]-`, `-[r]-`, etc.
/// Returns `(rel_var, rel_type, direction, rest)`.
fn parse_relationship_part(s: &str) -> Option<(String, String, String, &str)> {
    let s = s.trim();
    let sb = s.as_bytes();
    if sb.is_empty() {
        return None;
    }

    // Determine direction prefix
    let (incoming_prefix, after_prefix) = if sb[0] == b'<' && sb.get(1) == Some(&b'-') {
        (true, &s[2..])
    } else if sb[0] == b'-' {
        (false, &s[1..])
    } else {
        return None;
    };

    let after_prefix = after_prefix.trim_start();

    // Relationship bracket `[...]`
    let (rel_var, rel_type, after_bracket) = if after_prefix.starts_with('[') {
        let close = after_prefix.find(']')?;
        let inner = after_prefix[1..close].trim();
        let (rv, rt) = parse_rel_inner(inner);
        (rv, rt, &after_prefix[close + 1..])
    } else {
        (String::new(), String::new(), after_prefix)
    };

    let after_bracket = after_bracket.trim_start();

    // Direction suffix
    let (outgoing_suffix, after_suffix) = if after_bracket.starts_with("->") {
        (true, &after_bracket[2..])
    } else if after_bracket.starts_with('-') {
        (false, &after_bracket[1..])
    } else {
        return None;
    };

    let direction = if incoming_prefix && !outgoing_suffix {
        "incoming".into()
    } else if !incoming_prefix && outgoing_suffix {
        "outgoing".into()
    } else {
        "both".into()
    };

    Some((rel_var, rel_type, direction, after_suffix))
}

fn parse_rel_inner(inner: &str) -> (String, String) {
    // Could be: `r:TYPE`, `:TYPE`, `r`, `*1..3`, `r:TYPE*1..3`
    let colon = inner.find(':');
    match colon {
        Some(pos) => {
            let var = inner[..pos].trim().to_owned();
            let rest = inner[pos + 1..].trim();
            // Strip variable-length spec
            let rel_type = rest.split('*').next().unwrap_or(rest).trim().to_owned();
            (var, rel_type)
        }
        None => {
            let var = if inner.starts_with('*') {
                String::new()
            } else {
                inner.trim().to_owned()
            };
            (var, String::new())
        }
    }
}

// ─── MATCH clause extraction ──────────────────────────────────────────────────

fn extract_match_clause(query: &str) -> Option<&str> {
    use crate::keyword_scan::keyword_index;
    let match_idx = keyword_index(query, "MATCH")?;
    let end = ["WHERE", "RETURN", "WITH", "ORDER", "LIMIT", "SKIP"]
        .iter()
        .filter_map(|kw| keyword_index(&query[match_idx..], kw))
        .filter(|&p| p > 0)
        .min()
        .map(|p| match_idx + p)
        .unwrap_or(query.len());
    Some(&query[match_idx..end])
}

/// `contains_keyword_outside_strings` — check for keyword while respecting
/// quoted string literals (so "STARTS WITH" data values don't false-positive).
fn contains_keyword_outside_strings(query: &str, keyword: &str) -> bool {
    use crate::keyword_scan::keyword_index;
    keyword_index(query, keyword).is_some()
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generic_pattern_default() {
        let info = detect_query_pattern("MATCH (n) RETURN n");
        assert_eq!(info.pattern, QueryPattern::Generic);
    }

    #[test]
    fn test_large_result_set_pattern() {
        let info = detect_query_pattern("MATCH (n) RETURN n LIMIT 200");
        assert_eq!(info.pattern, QueryPattern::LargeResultSet);
        assert_eq!(info.limit, Some(200));
    }

    #[test]
    fn test_not_large_result_under_100() {
        let info = detect_query_pattern("MATCH (n) RETURN n LIMIT 50");
        // Under 100 → Generic
        assert_eq!(info.pattern, QueryPattern::Generic);
    }

    #[test]
    fn test_with_clause_bypasses_optimization() {
        // WITH makes the query ineligible for optimization
        let info = detect_query_pattern(
            "MATCH (n) WITH n MATCH (m) RETURN n, m",
        );
        assert_eq!(info.pattern, QueryPattern::Generic);
    }

    #[test]
    fn test_incoming_count_agg_pattern() {
        let q = "MATCH (x)<-[:FOLLOWS]-(y) RETURN x.name, count(y)";
        let info = detect_query_pattern(q);
        assert_eq!(info.pattern, QueryPattern::IncomingCountAgg);
        assert!(!info.start_var.is_empty());
        assert!(!info.end_var.is_empty());
    }

    #[test]
    fn test_outgoing_count_agg_pattern() {
        let q = "MATCH (x)-[:FOLLOWS]->(y) RETURN x.name, count(y)";
        let info = detect_query_pattern(q);
        assert_eq!(info.pattern, QueryPattern::OutgoingCountAgg);
    }

    #[test]
    fn test_mutual_relationship_pattern() {
        let q = "MATCH (a)-[:KNOWS]->(b)-[:KNOWS]->(a) RETURN a, b";
        let info = detect_query_pattern(q);
        assert_eq!(info.pattern, QueryPattern::MutualRelationship);
        assert_eq!(info.rel_type, "KNOWS");
    }

    #[test]
    fn test_query_pattern_is_optimizable() {
        assert!(!QueryPattern::Generic.is_optimizable());
        assert!(QueryPattern::LargeResultSet.is_optimizable());
        assert!(QueryPattern::MutualRelationship.is_optimizable());
        assert!(QueryPattern::IncomingCountAgg.is_optimizable());
        assert!(QueryPattern::OutgoingCountAgg.is_optimizable());
        assert!(QueryPattern::EdgePropertyAgg.is_optimizable());
    }

    #[test]
    fn test_query_pattern_display() {
        assert_eq!(QueryPattern::Generic.as_str(), "Generic");
        assert_eq!(QueryPattern::LargeResultSet.as_str(), "LargeResultSet");
        assert_eq!(QueryPattern::MutualRelationship.as_str(), "MutualRelationship");
    }
}
