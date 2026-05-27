use copperdb_cypher::Parser;
use std::alloc::{GlobalAlloc, Layout, System};
use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::time::{Duration, Instant};

struct CountingAllocator;

#[global_allocator]
static GLOBAL_ALLOCATOR: CountingAllocator = CountingAllocator;

static ALLOC_CALLS: AtomicU64 = AtomicU64::new(0);
static REALLOC_CALLS: AtomicU64 = AtomicU64::new(0);
static ALLOC_BYTES: AtomicU64 = AtomicU64::new(0);
static REALLOC_BYTES: AtomicU64 = AtomicU64::new(0);

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOC_CALLS.fetch_add(1, AtomicOrdering::Relaxed);
        ALLOC_BYTES.fetch_add(layout.size() as u64, AtomicOrdering::Relaxed);
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        REALLOC_CALLS.fetch_add(1, AtomicOrdering::Relaxed);
        REALLOC_BYTES.fetch_add(new_size as u64, AtomicOrdering::Relaxed);
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[derive(Clone, Copy)]
struct AllocationSnapshot {
    alloc_calls: u64,
    realloc_calls: u64,
    alloc_bytes: u64,
    realloc_bytes: u64,
}

impl AllocationSnapshot {
    fn capture() -> Self {
        Self {
            alloc_calls: ALLOC_CALLS.load(AtomicOrdering::Relaxed),
            realloc_calls: REALLOC_CALLS.load(AtomicOrdering::Relaxed),
            alloc_bytes: ALLOC_BYTES.load(AtomicOrdering::Relaxed),
            realloc_bytes: REALLOC_BYTES.load(AtomicOrdering::Relaxed),
        }
    }

    fn delta(self, earlier: Self) -> Self {
        Self {
            alloc_calls: self.alloc_calls - earlier.alloc_calls,
            realloc_calls: self.realloc_calls - earlier.realloc_calls,
            alloc_bytes: self.alloc_bytes - earlier.alloc_bytes,
            realloc_bytes: self.realloc_bytes - earlier.realloc_bytes,
        }
    }
}

struct ParserBenchmarkCase {
    name: &'static str,
    query: &'static str,
}

struct ValidationMicroCase {
    name: &'static str,
    query: &'static str,
}

const PARSER_BENCHMARK_CASES: &[ParserBenchmarkCase] = &[
    ParserBenchmarkCase {
        name: "simple_match",
        query: "MATCH (n) RETURN n",
    },
    ParserBenchmarkCase {
        name: "match_with_label",
        query: "MATCH (n:Person) RETURN n",
    },
    ParserBenchmarkCase {
        name: "match_with_properties",
        query: "MATCH (n:Person {name: 'Alice'}) RETURN n",
    },
    ParserBenchmarkCase {
        name: "match_with_variable",
        query: "MATCH (p:Person) RETURN p",
    },
    ParserBenchmarkCase {
        name: "match_where_equals",
        query: "MATCH (n:Person) WHERE n.name = 'Bob' RETURN n",
    },
    ParserBenchmarkCase {
        name: "match_where_gt",
        query: "MATCH (n:Person) WHERE n.age > 25 RETURN n",
    },
    ParserBenchmarkCase {
        name: "match_where_and",
        query: "MATCH (n:Person) WHERE n.age > 25 AND n.name = 'Alice' RETURN n",
    },
    ParserBenchmarkCase {
        name: "match_where_or",
        query: "MATCH (n:Person) WHERE n.age > 25 OR n.name = 'Alice' RETURN n",
    },
    ParserBenchmarkCase {
        name: "match_where_is_null",
        query: "MATCH (n:Person) WHERE n.email IS NULL RETURN n",
    },
    ParserBenchmarkCase {
        name: "match_where_is_not_null",
        query: "MATCH (n:Person) WHERE n.email IS NOT NULL RETURN n",
    },
    ParserBenchmarkCase {
        name: "match_where_in",
        query: "MATCH (n:Person) WHERE n.age IN [25, 30, 35] RETURN n",
    },
    ParserBenchmarkCase {
        name: "match_where_starts_with",
        query: "MATCH (n:Person) WHERE n.name STARTS WITH 'A' RETURN n",
    },
    ParserBenchmarkCase {
        name: "match_where_contains",
        query: "MATCH (n:Person) WHERE n.name CONTAINS 'lic' RETURN n",
    },
    ParserBenchmarkCase {
        name: "match_relationship",
        query: "MATCH (a)-[r]->(b) RETURN a, r, b",
    },
    ParserBenchmarkCase {
        name: "match_typed_relationship",
        query: "MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN a, b",
    },
    ParserBenchmarkCase {
        name: "match_variable_length",
        query: "MATCH (a)-[*1..3]->(b) RETURN a, b",
    },
    ParserBenchmarkCase {
        name: "match_reverse_relationship",
        query: "MATCH (a)<-[r]-(b) RETURN a, b",
    },
    ParserBenchmarkCase {
        name: "create_node",
        query: "CREATE (n:Person {name: 'Alice'})",
    },
    ParserBenchmarkCase {
        name: "create_with_return",
        query: "CREATE (n:Person {name: 'Alice'}) RETURN n",
    },
    ParserBenchmarkCase {
        name: "merge_node",
        query: "MERGE (n:Person {name: 'Alice'})",
    },
    ParserBenchmarkCase {
        name: "set_property",
        query: "MATCH (n:Person {name: 'Alice'}) SET n.age = 30",
    },
    ParserBenchmarkCase {
        name: "delete_node",
        query: "MATCH (n:Person {name: 'Alice'}) DELETE n",
    },
    ParserBenchmarkCase {
        name: "detach_delete",
        query: "MATCH (n:Person {name: 'Alice'}) DETACH DELETE n",
    },
    ParserBenchmarkCase {
        name: "return_alias",
        query: "MATCH (n:Person) RETURN n.name AS name",
    },
    ParserBenchmarkCase {
        name: "return_distinct",
        query: "MATCH (n:Person) RETURN DISTINCT n.city",
    },
    ParserBenchmarkCase {
        name: "return_limit",
        query: "MATCH (n:Person) RETURN n LIMIT 10",
    },
    ParserBenchmarkCase {
        name: "return_skip",
        query: "MATCH (n:Person) RETURN n SKIP 5",
    },
    ParserBenchmarkCase {
        name: "return_order_by",
        query: "MATCH (n:Person) RETURN n ORDER BY n.name",
    },
    ParserBenchmarkCase {
        name: "return_order_desc",
        query: "MATCH (n:Person) RETURN n ORDER BY n.age DESC",
    },
    ParserBenchmarkCase {
        name: "with_simple",
        query: "MATCH (n:Person) WITH n RETURN n",
    },
    ParserBenchmarkCase {
        name: "with_where",
        query: "MATCH (n:Person) WITH n WHERE n.age > 25 RETURN n",
    },
    ParserBenchmarkCase {
        name: "count_all",
        query: "MATCH (n:Person) RETURN count(*)",
    },
    ParserBenchmarkCase {
        name: "count_nodes",
        query: "MATCH (n:Person) RETURN count(n)",
    },
    ParserBenchmarkCase {
        name: "sum",
        query: "MATCH (n:Person) RETURN sum(n.age)",
    },
    ParserBenchmarkCase {
        name: "avg",
        query: "MATCH (n:Person) RETURN avg(n.age)",
    },
    ParserBenchmarkCase {
        name: "unwind_list",
        query: "UNWIND [1, 2, 3] AS x RETURN x",
    },
    ParserBenchmarkCase {
        name: "optional_match",
        query: "MATCH (n:Person) OPTIONAL MATCH (n)-[:KNOWS]->(m) RETURN n, m",
    },
    ParserBenchmarkCase {
        name: "call_procedure",
        query: "CALL db.labels()",
    },
];

const VALIDATION_STRUCTURAL_MICRO_CASES: &[ValidationMicroCase] = &[
    ValidationMicroCase {
        name: "where_single_compare",
        query: "MATCH (n) WHERE n.age > 25 RETURN n",
    },
    ValidationMicroCase {
        name: "where_and_two_compares",
        query: "MATCH (n) WHERE n.age > 25 AND n.score > 10 RETURN n",
    },
    ValidationMicroCase {
        name: "where_or_two_compares",
        query: "MATCH (n) WHERE n.age > 25 OR n.score > 10 RETURN n",
    },
    ValidationMicroCase {
        name: "rel_minimal_directed",
        query: "MATCH (a)-[]->(b) RETURN a",
    },
    ValidationMicroCase {
        name: "rel_with_variable",
        query: "MATCH (a)-[r]->(b) RETURN a",
    },
    ValidationMicroCase {
        name: "rel_with_type",
        query: "MATCH (a)-[:KNOWS]->(b) RETURN a",
    },
    ValidationMicroCase {
        name: "rel_with_var_and_type",
        query: "MATCH (a)-[r:KNOWS]->(b) RETURN a",
    },
    ValidationMicroCase {
        name: "rel_with_properties",
        query: "MATCH (a)-[r {weight: 1}]->(b) RETURN a",
    },
    ValidationMicroCase {
        name: "rel_variable_length",
        query: "MATCH (a)-[*1..3]->(b) RETURN a",
    },
];

struct ParserBenchmarkResult {
    name: &'static str,
    time: Duration,
    status: String,
}

struct ParserStageBenchmarkResult {
    name: &'static str,
    tokenize_time: Duration,
    validate_shallow_time: Duration,
    validate_core_time: Duration,
    parse_core_time: Duration,
    status: String,
}

#[derive(Clone, Copy)]
struct TokenizeMetrics {
    time: Duration,
    token_count: usize,
    punctuation_tokens: usize,
    operator_tokens: usize,
    string_literals: usize,
    alloc_calls: u64,
    realloc_calls: u64,
    alloc_bytes: u64,
}

#[derive(Default)]
struct TokenizeTotals {
    time: Duration,
    token_count: usize,
    punctuation_tokens: usize,
    operator_tokens: usize,
    string_literals: usize,
    alloc_calls: u64,
    realloc_calls: u64,
    alloc_bytes: u64,
}

#[derive(Default)]
struct FamilyTotals {
    time: Duration,
    success_count: usize,
    case_count: usize,
}

fn is_punctuation_token(token: &str) -> bool {
    matches!(
        token,
        "(" | ")"
            | "["
            | "]"
            | "{"
            | "}"
            | ":"
            | ","
            | "."
            | "="
            | "<"
            | ">"
            | "-"
            | "+"
            | "*"
            | "/"
    )
}

fn is_operator_token(token: &str) -> bool {
    matches!(token, "<>" | "<=" | ">=" | "!=" | "=~")
}

fn validation_family(case: &ParserBenchmarkCase) -> &'static str {
    match case.name {
        "simple_match" | "match_with_label" | "match_with_properties" | "match_with_variable" => {
            "match_nodes"
        }
        "match_where_equals"
        | "match_where_gt"
        | "match_where_and"
        | "match_where_or"
        | "match_where_is_null"
        | "match_where_is_not_null"
        | "match_where_in"
        | "match_where_starts_with"
        | "match_where_contains" => "where_expr",
        "match_relationship"
        | "match_typed_relationship"
        | "match_variable_length"
        | "match_reverse_relationship"
        | "optional_match" => "relationships",
        "create_node" | "create_with_return" | "merge_node" | "set_property" | "delete_node"
        | "detach_delete" => "writes",
        "return_alias" | "return_distinct" | "return_limit" | "return_skip" | "return_order_by"
        | "return_order_desc" | "with_simple" | "with_where" => "projection",
        "count_all" | "count_nodes" | "sum" | "avg" => "aggregates",
        "unwind_list" => "unwind",
        "call_procedure" => "call",
        _ => {
            let _ = case.query;
            "other"
        }
    }
}

fn validation_hotspot_subfamily(case: &ParserBenchmarkCase) -> Option<&'static str> {
    match case.name {
        "match_where_equals" | "match_where_gt" => Some("where_compare"),
        "match_where_and" | "match_where_or" => Some("where_logical"),
        "match_where_is_null" | "match_where_is_not_null" => Some("where_null"),
        "match_where_in" => Some("where_in_list"),
        "match_where_starts_with" | "match_where_contains" => Some("where_string_ops"),
        "match_relationship" | "match_typed_relationship" => Some("rel_simple"),
        "match_variable_length" => Some("rel_variable_length"),
        "match_reverse_relationship" => Some("rel_reverse"),
        "optional_match" => Some("rel_optional_match"),
        _ => None,
    }
}

fn validation_micro_hotspot(case: &ParserBenchmarkCase) -> Option<&'static str> {
    match case.name {
        "match_where_and" => Some("where_and_case"),
        "match_where_or" => Some("where_or_case"),
        "match_relationship" => Some("rel_untyped_case"),
        "match_typed_relationship" => Some("rel_typed_case"),
        _ => None,
    }
}

fn measure_median_parse_time(
    parser: &Parser,
    query: &str,
    samples: usize,
) -> Result<Duration, String> {
    let sample_count = samples.max(1);
    let mut durations = Vec::with_capacity(sample_count);

    for _ in 0..sample_count {
        let start = Instant::now();
        parser.parse(query).map_err(|error| error.to_string())?;
        durations.push(start.elapsed());
    }

    durations.sort_unstable();
    Ok(durations[durations.len() / 2])
}

fn measure_median_validation_time(
    parser: &Parser,
    query: &str,
    samples: usize,
) -> Result<Duration, String> {
    let sample_count = samples.max(1);
    let mut durations = Vec::with_capacity(sample_count);

    for _ in 0..sample_count {
        let start = Instant::now();
        parser.validate(query).map_err(|error| error.to_string())?;
        durations.push(start.elapsed());
    }

    durations.sort_unstable();
    Ok(durations[durations.len() / 2])
}

fn measure_median_validate_shallow_time(
    parser: &Parser,
    query: &str,
    samples: usize,
) -> Result<Duration, String> {
    let sample_count = samples.max(1);
    let mut durations = Vec::with_capacity(sample_count);

    for _ in 0..sample_count {
        let start = Instant::now();
        parser
            .validate_shallow(query)
            .map_err(|error| error.to_string())?;
        durations.push(start.elapsed());
    }

    durations.sort_unstable();
    Ok(durations[durations.len() / 2])
}

fn measure_median_tokenize_time(
    parser: &Parser,
    query: &str,
    samples: usize,
) -> Result<Duration, String> {
    let sample_count = samples.max(1);
    let mut durations = Vec::with_capacity(sample_count);

    for _ in 0..sample_count {
        let start = Instant::now();
        let tokens = parser
            .tokenize_only(query)
            .map_err(|error| error.to_string())?;
        std::hint::black_box(tokens.len());
        durations.push(start.elapsed());
    }

    durations.sort_unstable();
    Ok(durations[durations.len() / 2])
}

fn measure_median_validate_core_time(
    parser: &Parser,
    query: &str,
    samples: usize,
) -> Result<Duration, String> {
    let sample_count = samples.max(1);
    let mut durations = Vec::with_capacity(sample_count);

    for _ in 0..sample_count {
        let tokens = parser
            .tokenize_only(query)
            .map_err(|error| error.to_string())?;
        let start = Instant::now();
        parser
            .validate_tokenized(tokens)
            .map_err(|error| error.to_string())?;
        durations.push(start.elapsed());
    }

    durations.sort_unstable();
    Ok(durations[durations.len() / 2])
}

fn measure_median_parse_core_time(
    parser: &Parser,
    query: &str,
    samples: usize,
) -> Result<Duration, String> {
    let sample_count = samples.max(1);
    let mut durations = Vec::with_capacity(sample_count);

    for _ in 0..sample_count {
        let tokens = parser
            .tokenize_only(query)
            .map_err(|error| error.to_string())?;
        let start = Instant::now();
        let parsed = parser
            .parse_tokenized(tokens)
            .map_err(|error| error.to_string())?;
        std::hint::black_box(parsed.clauses.len());
        durations.push(start.elapsed());
    }

    durations.sort_unstable();
    Ok(durations[durations.len() / 2])
}

fn measure_median_tokenize_metrics(
    parser: &Parser,
    query: &str,
    samples: usize,
) -> Result<TokenizeMetrics, String> {
    let sample_count = samples.max(1);
    let mut samples_out = Vec::with_capacity(sample_count);

    for _ in 0..sample_count {
        let before = AllocationSnapshot::capture();
        let start = Instant::now();
        let tokens = parser
            .tokenize_only(query)
            .map_err(|error| error.to_string())?;
        let elapsed = start.elapsed();
        let delta = AllocationSnapshot::capture().delta(before);

        let mut punctuation_tokens = 0usize;
        let mut operator_tokens = 0usize;
        let mut string_literals = 0usize;
        for token in &tokens {
            if is_punctuation_token(token) {
                punctuation_tokens += 1;
            }
            if is_operator_token(token) {
                operator_tokens += 1;
            }
            if token.starts_with('"') || token.starts_with('\'') {
                string_literals += 1;
            }
        }

        samples_out.push(TokenizeMetrics {
            time: elapsed,
            token_count: tokens.len(),
            punctuation_tokens,
            operator_tokens,
            string_literals,
            alloc_calls: delta.alloc_calls,
            realloc_calls: delta.realloc_calls,
            alloc_bytes: delta.alloc_bytes + delta.realloc_bytes,
        });
    }

    samples_out.sort_by(|left, right| match left.time.cmp(&right.time) {
        Ordering::Equal => left.token_count.cmp(&right.token_count),
        other => other,
    });
    Ok(samples_out[samples_out.len() / 2])
}

#[test]
#[ignore = "benchmark-style parser timing report; run explicitly"]
fn parser_benchmark_report() {
    const SAMPLES_PER_QUERY: usize = 5;

    let parser = Parser::new();
    let mut results = Vec::with_capacity(PARSER_BENCHMARK_CASES.len());
    let mut total = Duration::default();
    let mut success_count = 0usize;

    for case in PARSER_BENCHMARK_CASES {
        let result = match measure_median_parse_time(&parser, case.query, SAMPLES_PER_QUERY) {
            Ok(time) => {
                total += time;
                success_count += 1;
                ParserBenchmarkResult {
                    name: case.name,
                    time,
                    status: "ok".into(),
                }
            }
            Err(error) => ParserBenchmarkResult {
                name: case.name,
                time: Duration::default(),
                status: format!("failed: {error}"),
            },
        };
        results.push(result);
    }

    println!("\n{}", "=".repeat(80));
    println!("CYPHER PARSER BENCHMARK REPORT");
    println!("{}", "=".repeat(80));
    println!("\n{:<30} | {:>12} | {}", "Query", "CopperDB", "Status");
    println!("{}", "-".repeat(80));

    for result in &results {
        let mut name = result.name.to_string();
        if name.len() > 30 {
            name.truncate(27);
            name.push_str("...");
        }
        println!("{:<30} | {:>12?} | {}", name, result.time, result.status);
    }

    println!("{}", "-".repeat(80));
    println!("{:<30} | {:>12?} |", "TOTAL", total);
    println!("{}", "=".repeat(80));
    println!(
        "\nSummary: parsed {} / {} query classes successfully\n",
        success_count,
        results.len()
    );
    println!(
        "PARSER_REPORT_SUMMARY mode=parse parser=CopperDB total_ns={} success={} total_cases={}",
        total.as_nanos(),
        success_count,
        results.len()
    );

    assert!(
        success_count > 0,
        "expected at least one query class to parse"
    );
}

#[test]
#[ignore = "benchmark-style validation timing report; run explicitly"]
fn parser_validation_benchmark_report() {
    const SAMPLES_PER_QUERY: usize = 5;

    let parser = Parser::new();
    let mut results = Vec::with_capacity(PARSER_BENCHMARK_CASES.len());
    let mut total = Duration::default();
    let mut success_count = 0usize;

    for case in PARSER_BENCHMARK_CASES {
        let result = match measure_median_validation_time(&parser, case.query, SAMPLES_PER_QUERY) {
            Ok(time) => {
                total += time;
                success_count += 1;
                ParserBenchmarkResult {
                    name: case.name,
                    time,
                    status: "ok".into(),
                }
            }
            Err(error) => ParserBenchmarkResult {
                name: case.name,
                time: Duration::default(),
                status: format!("failed: {error}"),
            },
        };
        results.push(result);
    }

    println!("\n{}", "=".repeat(80));
    println!("CYPHER VALIDATION BENCHMARK REPORT");
    println!("{}", "=".repeat(80));
    println!("\n{:<30} | {:>12} | {}", "Query", "CopperDB", "Status");
    println!("{}", "-".repeat(80));

    for result in &results {
        let mut name = result.name.to_string();
        if name.len() > 30 {
            name.truncate(27);
            name.push_str("...");
        }
        println!("{:<30} | {:>12?} | {}", name, result.time, result.status);
    }

    println!("{}", "-".repeat(80));
    println!("{:<30} | {:>12?} |", "TOTAL", total);
    println!("{}", "=".repeat(80));
    println!(
        "\nSummary: validated {} / {} query classes successfully\n",
        success_count,
        results.len()
    );
    println!(
        "PARSER_REPORT_SUMMARY mode=validate parser=CopperDB total_ns={} success={} total_cases={}",
        total.as_nanos(),
        success_count,
        results.len()
    );

    assert!(
        success_count > 0,
        "expected at least one query class to validate"
    );
}

#[test]
#[ignore = "benchmark-style shallow validation timing report; run explicitly"]
fn parser_validation_shallow_benchmark_report() {
    const SAMPLES_PER_QUERY: usize = 5;

    let parser = Parser::new();
    let mut results = Vec::with_capacity(PARSER_BENCHMARK_CASES.len());
    let mut total = Duration::default();
    let mut success_count = 0usize;

    for case in PARSER_BENCHMARK_CASES {
        let result =
            match measure_median_validate_shallow_time(&parser, case.query, SAMPLES_PER_QUERY) {
                Ok(time) => {
                    total += time;
                    success_count += 1;
                    ParserBenchmarkResult {
                        name: case.name,
                        time,
                        status: "ok".into(),
                    }
                }
                Err(error) => ParserBenchmarkResult {
                    name: case.name,
                    time: Duration::default(),
                    status: format!("failed: {error}"),
                },
            };
        results.push(result);
    }

    println!("\n{}", "=".repeat(80));
    println!("CYPHER SHALLOW VALIDATION BENCHMARK REPORT");
    println!("{}", "=".repeat(80));
    println!("\n{:<30} | {:>12} | {}", "Query", "CopperDB", "Status");
    println!("{}", "-".repeat(80));

    for result in &results {
        let mut name = result.name.to_string();
        if name.len() > 30 {
            name.truncate(27);
            name.push_str("...");
        }
        println!("{:<30} | {:>12?} | {}", name, result.time, result.status);
    }

    println!("{}", "-".repeat(80));
    println!("{:<30} | {:>12?} |", "TOTAL", total);
    println!("{}", "=".repeat(80));
    println!(
        "\nSummary: shallow-validated {} / {} query classes successfully\n",
        success_count,
        results.len()
    );
    println!(
        "PARSER_REPORT_SUMMARY mode=validate_shallow parser=CopperDB total_ns={} success={} total_cases={}",
        total.as_nanos(),
        success_count,
        results.len()
    );

    assert!(
        success_count > 0,
        "expected at least one query class to shallow-validate"
    );
}

#[test]
#[ignore = "benchmark-style stage timing report; run explicitly"]
fn parser_stage_benchmark_report() {
    const SAMPLES_PER_QUERY: usize = 5;

    let parser = Parser::new();
    let mut results = Vec::with_capacity(PARSER_BENCHMARK_CASES.len());
    let mut tokenize_total = Duration::default();
    let mut validate_shallow_total = Duration::default();
    let mut validate_core_total = Duration::default();
    let mut parse_core_total = Duration::default();
    let mut success_count = 0usize;

    for case in PARSER_BENCHMARK_CASES {
        let result = match (
            measure_median_tokenize_time(&parser, case.query, SAMPLES_PER_QUERY),
            measure_median_validate_shallow_time(&parser, case.query, SAMPLES_PER_QUERY),
            measure_median_validate_core_time(&parser, case.query, SAMPLES_PER_QUERY),
            measure_median_parse_core_time(&parser, case.query, SAMPLES_PER_QUERY),
        ) {
            (
                Ok(tokenize_time),
                Ok(validate_shallow_time),
                Ok(validate_core_time),
                Ok(parse_core_time),
            ) => {
                tokenize_total += tokenize_time;
                validate_shallow_total += validate_shallow_time;
                validate_core_total += validate_core_time;
                parse_core_total += parse_core_time;
                success_count += 1;
                ParserStageBenchmarkResult {
                    name: case.name,
                    tokenize_time,
                    validate_shallow_time,
                    validate_core_time,
                    parse_core_time,
                    status: "ok".into(),
                }
            }
            (Err(error), _, _, _)
            | (_, Err(error), _, _)
            | (_, _, Err(error), _)
            | (_, _, _, Err(error)) => ParserStageBenchmarkResult {
                name: case.name,
                tokenize_time: Duration::default(),
                validate_shallow_time: Duration::default(),
                validate_core_time: Duration::default(),
                parse_core_time: Duration::default(),
                status: format!("failed: {error}"),
            },
        };
        results.push(result);
    }

    println!("\n{}", "=".repeat(112));
    println!("CYPHER PARSER STAGE BENCHMARK REPORT");
    println!("{}", "=".repeat(112));
    println!(
        "\n{:<30} | {:>12} | {:>16} | {:>14} | {:>11} | {}",
        "Query", "tokenize", "validate_shallow", "validate_core", "parse_core", "Status"
    );
    println!("{}", "-".repeat(112));

    for result in &results {
        let mut name = result.name.to_string();
        if name.len() > 30 {
            name.truncate(27);
            name.push_str("...");
        }
        println!(
            "{:<30} | {:>12?} | {:>16?} | {:>14?} | {:>11?} | {}",
            name,
            result.tokenize_time,
            result.validate_shallow_time,
            result.validate_core_time,
            result.parse_core_time,
            result.status
        );
    }

    println!("{}", "-".repeat(112));
    println!(
        "{:<30} | {:>12?} | {:>16?} | {:>14?} | {:>11?} |",
        "TOTAL", tokenize_total, validate_shallow_total, validate_core_total, parse_core_total,
    );
    println!("{}", "=".repeat(112));
    println!(
        "\nSummary: stage-timed {} / {} query classes successfully\n",
        success_count,
        results.len()
    );
    println!(
        "PARSER_REPORT_SUMMARY mode=tokenize parser=CopperDB total_ns={} success={} total_cases={}",
        tokenize_total.as_nanos(),
        success_count,
        results.len()
    );
    println!(
        "PARSER_REPORT_SUMMARY mode=validate_shallow parser=CopperDB total_ns={} success={} total_cases={}",
        validate_shallow_total.as_nanos(),
        success_count,
        results.len()
    );
    println!(
        "PARSER_REPORT_SUMMARY mode=validate_core parser=CopperDB total_ns={} success={} total_cases={}",
        validate_core_total.as_nanos(),
        success_count,
        results.len()
    );
    println!(
        "PARSER_REPORT_SUMMARY mode=parse_core parser=CopperDB total_ns={} success={} total_cases={}",
        parse_core_total.as_nanos(),
        success_count,
        results.len()
    );

    assert!(
        success_count > 0,
        "expected at least one query class to stage-benchmark"
    );
}

#[test]
#[ignore = "benchmark-style tokenizer timing report; run explicitly"]
fn tokenizer_microbenchmark_report() {
    const SAMPLES_PER_QUERY: usize = 5;

    let parser = Parser::new();
    let mut totals = TokenizeTotals::default();

    println!("\n{}", "=".repeat(120));
    println!("CYPHER TOKENIZER MICROBENCHMARK REPORT");
    println!("{}", "=".repeat(120));
    println!(
        "\n{:<24} | {:>10} | {:>6} | {:>6} | {:>6} | {:>7} | {:>8} | {:>10} | {:>8} | {}",
        "Query",
        "time",
        "tokens",
        "punct",
        "ops",
        "strings",
        "allocs",
        "reallocs",
        "alloc_b",
        "chars/tok"
    );
    println!("{}", "-".repeat(120));

    for case in PARSER_BENCHMARK_CASES {
        let metrics = measure_median_tokenize_metrics(&parser, case.query, SAMPLES_PER_QUERY)
            .unwrap_or_else(|error| panic!("tokenize failed for {}: {}", case.name, error));
        totals.time += metrics.time;
        totals.token_count += metrics.token_count;
        totals.punctuation_tokens += metrics.punctuation_tokens;
        totals.operator_tokens += metrics.operator_tokens;
        totals.string_literals += metrics.string_literals;
        totals.alloc_calls += metrics.alloc_calls;
        totals.realloc_calls += metrics.realloc_calls;
        totals.alloc_bytes += metrics.alloc_bytes;

        let chars_per_token = case.query.len() as f64 / metrics.token_count.max(1) as f64;
        println!(
            "{:<24} | {:>10?} | {:>6} | {:>6} | {:>6} | {:>7} | {:>8} | {:>10} | {:>8} | {:>9.2}",
            case.name,
            metrics.time,
            metrics.token_count,
            metrics.punctuation_tokens,
            metrics.operator_tokens,
            metrics.string_literals,
            metrics.alloc_calls,
            metrics.realloc_calls,
            metrics.alloc_bytes,
            chars_per_token,
        );
    }

    println!("{}", "-".repeat(120));
    println!(
        "{:<24} | {:>10?} | {:>6} | {:>6} | {:>6} | {:>7} | {:>8} | {:>10} | {:>8} | {:>9.2}",
        "TOTAL",
        totals.time,
        totals.token_count,
        totals.punctuation_tokens,
        totals.operator_tokens,
        totals.string_literals,
        totals.alloc_calls,
        totals.realloc_calls,
        totals.alloc_bytes,
        PARSER_BENCHMARK_CASES
            .iter()
            .map(|case| case.query.len())
            .sum::<usize>() as f64
            / totals.token_count.max(1) as f64,
    );
    println!("{}", "=".repeat(120));
    println!(
        "PARSER_REPORT_SUMMARY mode=tokenize_micro parser=CopperDB total_ns={} success={} total_cases={}",
        totals.time.as_nanos(),
        PARSER_BENCHMARK_CASES.len(),
        PARSER_BENCHMARK_CASES.len()
    );
    println!(
        "TOKENIZER_REPORT_SUMMARY parser=CopperDB total_tokens={} punctuation_tokens={} operator_tokens={} string_literals={} allocs={} reallocs={} alloc_bytes={}",
        totals.token_count,
        totals.punctuation_tokens,
        totals.operator_tokens,
        totals.string_literals,
        totals.alloc_calls,
        totals.realloc_calls,
        totals.alloc_bytes,
    );
}

#[test]
#[ignore = "benchmark-style validation family report; run explicitly"]
fn parser_validation_family_benchmark_report() {
    const SAMPLES_PER_QUERY: usize = 5;

    let parser = Parser::new();
    let mut family_totals: BTreeMap<&'static str, FamilyTotals> = BTreeMap::new();

    for case in PARSER_BENCHMARK_CASES {
        let time = measure_median_validate_core_time(&parser, case.query, SAMPLES_PER_QUERY)
            .unwrap_or_else(|error| panic!("validate core failed for {}: {}", case.name, error));
        let family = validation_family(case);
        let totals = family_totals.entry(family).or_default();
        totals.time += time;
        totals.success_count += 1;
        totals.case_count += 1;
    }

    println!("\n{}", "=".repeat(84));
    println!("CYPHER VALIDATION CORE FAMILY REPORT");
    println!("{}", "=".repeat(84));
    println!(
        "\n{:<18} | {:>12} | {:>8} | {:>12}",
        "Family", "total", "cases", "avg/case"
    );
    println!("{}", "-".repeat(84));

    let mut total = Duration::default();
    let mut total_cases = 0usize;
    for (family, stats) in &family_totals {
        total += stats.time;
        total_cases += stats.case_count;
        let avg =
            Duration::from_nanos((stats.time.as_nanos() / stats.case_count.max(1) as u128) as u64);
        println!(
            "{:<18} | {:>12?} | {:>8} | {:>12?}",
            family, stats.time, stats.case_count, avg,
        );
    }

    println!("{}", "-".repeat(84));
    println!("{:<18} | {:>12?} | {:>8} |", "TOTAL", total, total_cases);
    println!("{}", "=".repeat(84));

    for (family, stats) in &family_totals {
        println!(
            "PARSER_REPORT_SUMMARY mode=validate_core_family parser=CopperDB family={} total_ns={} success={} total_cases={}",
            family,
            stats.time.as_nanos(),
            stats.success_count,
            stats.case_count,
        );
    }
}

#[test]
#[ignore = "benchmark-style validation hotspot subfamily report; run explicitly"]
fn parser_validation_hotspot_benchmark_report() {
    const SAMPLES_PER_QUERY: usize = 5;

    let parser = Parser::new();
    let mut family_totals: BTreeMap<&'static str, FamilyTotals> = BTreeMap::new();

    for case in PARSER_BENCHMARK_CASES {
        let Some(family) = validation_hotspot_subfamily(case) else {
            continue;
        };

        let time = measure_median_validate_core_time(&parser, case.query, SAMPLES_PER_QUERY)
            .unwrap_or_else(|error| panic!("validate core failed for {}: {}", case.name, error));
        let totals = family_totals.entry(family).or_default();
        totals.time += time;
        totals.success_count += 1;
        totals.case_count += 1;
    }

    println!("\n{}", "=".repeat(92));
    println!("CYPHER VALIDATION HOTSPOT REPORT");
    println!("{}", "=".repeat(92));
    println!(
        "\n{:<20} | {:>12} | {:>8} | {:>12}",
        "Hotspot", "total", "cases", "avg/case"
    );
    println!("{}", "-".repeat(92));

    let mut total = Duration::default();
    let mut total_cases = 0usize;
    for (family, stats) in &family_totals {
        total += stats.time;
        total_cases += stats.case_count;
        let avg =
            Duration::from_nanos((stats.time.as_nanos() / stats.case_count.max(1) as u128) as u64);
        println!(
            "{:<20} | {:>12?} | {:>8} | {:>12?}",
            family, stats.time, stats.case_count, avg,
        );
    }

    println!("{}", "-".repeat(92));
    println!("{:<20} | {:>12?} | {:>8} |", "TOTAL", total, total_cases);
    println!("{}", "=".repeat(92));

    for (family, stats) in &family_totals {
        println!(
            "PARSER_REPORT_SUMMARY mode=validate_core_hotspot parser=CopperDB family={} total_ns={} success={} total_cases={}",
            family,
            stats.time.as_nanos(),
            stats.success_count,
            stats.case_count,
        );
    }
}

#[test]
#[ignore = "benchmark-style validation micro-hotspot report; run explicitly"]
fn parser_validation_micro_hotspot_benchmark_report() {
    const SAMPLES_PER_QUERY: usize = 5;

    let parser = Parser::new();
    let mut family_totals: BTreeMap<&'static str, FamilyTotals> = BTreeMap::new();

    for case in PARSER_BENCHMARK_CASES {
        let Some(family) = validation_micro_hotspot(case) else {
            continue;
        };

        let time = measure_median_validate_core_time(&parser, case.query, SAMPLES_PER_QUERY)
            .unwrap_or_else(|error| panic!("validate core failed for {}: {}", case.name, error));
        let totals = family_totals.entry(family).or_default();
        totals.time += time;
        totals.success_count += 1;
        totals.case_count += 1;
    }

    println!("\n{}", "=".repeat(96));
    println!("CYPHER VALIDATION MICRO-HOTSPOT REPORT");
    println!("{}", "=".repeat(96));
    println!(
        "\n{:<22} | {:>12} | {:>8} | {:>12}",
        "MicroHotspot", "total", "cases", "avg/case"
    );
    println!("{}", "-".repeat(96));

    let mut total = Duration::default();
    let mut total_cases = 0usize;
    for (family, stats) in &family_totals {
        total += stats.time;
        total_cases += stats.case_count;
        let avg =
            Duration::from_nanos((stats.time.as_nanos() / stats.case_count.max(1) as u128) as u64);
        println!(
            "{:<22} | {:>12?} | {:>8} | {:>12?}",
            family, stats.time, stats.case_count, avg,
        );
    }

    println!("{}", "-".repeat(96));
    println!("{:<22} | {:>12?} | {:>8} |", "TOTAL", total, total_cases);
    println!("{}", "=".repeat(96));

    for (family, stats) in &family_totals {
        println!(
            "PARSER_REPORT_SUMMARY mode=validate_core_micro_hotspot parser=CopperDB family={} total_ns={} success={} total_cases={}",
            family,
            stats.time.as_nanos(),
            stats.success_count,
            stats.case_count,
        );
    }
}

#[test]
#[ignore = "benchmark-style validation structural microcase report; run explicitly"]
fn parser_validation_structural_microcase_report() {
    const SAMPLES_PER_QUERY: usize = 5;

    let parser = Parser::new();

    println!("\n{}", "=".repeat(108));
    println!("CYPHER VALIDATION STRUCTURAL MICROCASE REPORT");
    println!("{}", "=".repeat(108));
    println!(
        "\n{:<24} | {:>12} | {}",
        "Microcase", "validate_core", "Query"
    );
    println!("{}", "-".repeat(108));

    let mut total = Duration::default();
    for case in VALIDATION_STRUCTURAL_MICRO_CASES {
        let time = measure_median_validate_core_time(&parser, case.query, SAMPLES_PER_QUERY)
            .unwrap_or_else(|error| panic!("validate core failed for {}: {}", case.name, error));
        total += time;
        println!("{:<24} | {:>12?} | {}", case.name, time, case.query);
        println!(
            "PARSER_REPORT_SUMMARY mode=validate_core_structural parser=CopperDB case={} total_ns={} success=1 total_cases=1",
            case.name,
            time.as_nanos(),
        );
    }

    println!("{}", "-".repeat(108));
    println!("{:<24} | {:>12?} |", "TOTAL", total);
    println!("{}", "=".repeat(108));
}
