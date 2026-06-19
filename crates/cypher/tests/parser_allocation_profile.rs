use copperdb_cypher::{
    BinaryExpression, Clause, EdgePattern, Expression, LiteralValue, NodePattern, Parser, Pattern,
    PropertyEntry, Query, ReturnItem, SetItem,
};
use std::alloc::{GlobalAlloc, Layout, System};
use std::hint::black_box;
use std::mem::size_of;
use std::sync::atomic::{AtomicU64, Ordering};

struct AllocationCase {
    name: &'static str,
    query: &'static str,
}

const ALLOCATION_CASES: &[AllocationCase] = &[
    AllocationCase {
        name: "simple_match",
        query: "MATCH (n) RETURN n",
    },
    AllocationCase {
        name: "match_with_label",
        query: "MATCH (n:Person) RETURN n",
    },
    AllocationCase {
        name: "match_with_properties",
        query: "MATCH (n:Person {name: 'Alice'}) RETURN n",
    },
    AllocationCase {
        name: "match_with_variable",
        query: "MATCH (p:Person) RETURN p",
    },
    AllocationCase {
        name: "match_where_equals",
        query: "MATCH (n:Person) WHERE n.name = 'Bob' RETURN n",
    },
    AllocationCase {
        name: "match_where_gt",
        query: "MATCH (n:Person) WHERE n.age > 25 RETURN n",
    },
    AllocationCase {
        name: "match_where_and",
        query: "MATCH (n:Person) WHERE n.age > 25 AND n.name = 'Alice' RETURN n",
    },
    AllocationCase {
        name: "match_where_or",
        query: "MATCH (n:Person) WHERE n.age > 25 OR n.name = 'Alice' RETURN n",
    },
    AllocationCase {
        name: "match_where_is_null",
        query: "MATCH (n:Person) WHERE n.email IS NULL RETURN n",
    },
    AllocationCase {
        name: "match_where_is_not_null",
        query: "MATCH (n:Person) WHERE n.email IS NOT NULL RETURN n",
    },
    AllocationCase {
        name: "match_where_in",
        query: "MATCH (n:Person) WHERE n.age IN [25, 30, 35] RETURN n",
    },
    AllocationCase {
        name: "match_where_starts_with",
        query: "MATCH (n:Person) WHERE n.name STARTS WITH 'A' RETURN n",
    },
    AllocationCase {
        name: "match_where_contains",
        query: "MATCH (n:Person) WHERE n.name CONTAINS 'lic' RETURN n",
    },
    AllocationCase {
        name: "match_relationship",
        query: "MATCH (a)-[r]->(b) RETURN a, r, b",
    },
    AllocationCase {
        name: "match_typed_relationship",
        query: "MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN a, b",
    },
    AllocationCase {
        name: "match_variable_length",
        query: "MATCH (a)-[*1..3]->(b) RETURN a, b",
    },
    AllocationCase {
        name: "match_reverse_relationship",
        query: "MATCH (a)<-[r]-(b) RETURN a, b",
    },
    AllocationCase {
        name: "create_node",
        query: "CREATE (n:Person {name: 'Alice'})",
    },
    AllocationCase {
        name: "create_with_return",
        query: "CREATE (n:Person {name: 'Alice'}) RETURN n",
    },
    AllocationCase {
        name: "merge_node",
        query: "MERGE (n:Person {name: 'Alice'})",
    },
    AllocationCase {
        name: "set_property",
        query: "MATCH (n:Person {name: 'Alice'}) SET n.age = 30",
    },
    AllocationCase {
        name: "delete_node",
        query: "MATCH (n:Person {name: 'Alice'}) DELETE n",
    },
    AllocationCase {
        name: "detach_delete",
        query: "MATCH (n:Person {name: 'Alice'}) DETACH DELETE n",
    },
    AllocationCase {
        name: "return_alias",
        query: "MATCH (n:Person) RETURN n.name AS name",
    },
    AllocationCase {
        name: "return_distinct",
        query: "MATCH (n:Person) RETURN DISTINCT n.city",
    },
    AllocationCase {
        name: "return_limit",
        query: "MATCH (n:Person) RETURN n LIMIT 10",
    },
    AllocationCase {
        name: "return_skip",
        query: "MATCH (n:Person) RETURN n SKIP 5",
    },
    AllocationCase {
        name: "return_order_by",
        query: "MATCH (n:Person) RETURN n ORDER BY n.name",
    },
    AllocationCase {
        name: "return_order_desc",
        query: "MATCH (n:Person) RETURN n ORDER BY n.age DESC",
    },
    AllocationCase {
        name: "with_simple",
        query: "MATCH (n:Person) WITH n RETURN n",
    },
    AllocationCase {
        name: "with_where",
        query: "MATCH (n:Person) WITH n WHERE n.age > 25 RETURN n",
    },
    AllocationCase {
        name: "count_all",
        query: "MATCH (n:Person) RETURN count(*)",
    },
    AllocationCase {
        name: "count_nodes",
        query: "MATCH (n:Person) RETURN count(n)",
    },
    AllocationCase {
        name: "sum",
        query: "MATCH (n:Person) RETURN sum(n.age)",
    },
    AllocationCase {
        name: "avg",
        query: "MATCH (n:Person) RETURN avg(n.age)",
    },
    AllocationCase {
        name: "unwind_list",
        query: "UNWIND [1, 2, 3] AS x RETURN x",
    },
    AllocationCase {
        name: "optional_match",
        query: "MATCH (n:Person) OPTIONAL MATCH (n)-[:KNOWS]->(m) RETURN n, m",
    },
    AllocationCase {
        name: "call_procedure",
        query: "CALL db.labels()",
    },
];

struct CountingAllocator;

#[global_allocator]
static GLOBAL_ALLOCATOR: CountingAllocator = CountingAllocator;

static ALLOC_CALLS: AtomicU64 = AtomicU64::new(0);
static DEALLOC_CALLS: AtomicU64 = AtomicU64::new(0);
static REALLOC_CALLS: AtomicU64 = AtomicU64::new(0);
static ALLOC_BYTES: AtomicU64 = AtomicU64::new(0);
static DEALLOC_BYTES: AtomicU64 = AtomicU64::new(0);
static REALLOC_BYTES: AtomicU64 = AtomicU64::new(0);

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOC_CALLS.fetch_add(1, Ordering::Relaxed);
        ALLOC_BYTES.fetch_add(layout.size() as u64, Ordering::Relaxed);
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        DEALLOC_CALLS.fetch_add(1, Ordering::Relaxed);
        DEALLOC_BYTES.fetch_add(layout.size() as u64, Ordering::Relaxed);
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        REALLOC_CALLS.fetch_add(1, Ordering::Relaxed);
        REALLOC_BYTES.fetch_add(new_size as u64, Ordering::Relaxed);
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[derive(Clone, Copy)]
struct AllocationSnapshot {
    alloc_calls: u64,
    dealloc_calls: u64,
    realloc_calls: u64,
    alloc_bytes: u64,
    dealloc_bytes: u64,
    realloc_bytes: u64,
}

impl AllocationSnapshot {
    fn capture() -> Self {
        Self {
            alloc_calls: ALLOC_CALLS.load(Ordering::Relaxed),
            dealloc_calls: DEALLOC_CALLS.load(Ordering::Relaxed),
            realloc_calls: REALLOC_CALLS.load(Ordering::Relaxed),
            alloc_bytes: ALLOC_BYTES.load(Ordering::Relaxed),
            dealloc_bytes: DEALLOC_BYTES.load(Ordering::Relaxed),
            realloc_bytes: REALLOC_BYTES.load(Ordering::Relaxed),
        }
    }

    fn delta(self, earlier: Self) -> Self {
        Self {
            alloc_calls: self.alloc_calls - earlier.alloc_calls,
            dealloc_calls: self.dealloc_calls - earlier.dealloc_calls,
            realloc_calls: self.realloc_calls - earlier.realloc_calls,
            alloc_bytes: self.alloc_bytes - earlier.alloc_bytes,
            dealloc_bytes: self.dealloc_bytes - earlier.dealloc_bytes,
            realloc_bytes: self.realloc_bytes - earlier.realloc_bytes,
        }
    }
}

#[derive(Default, Clone, Copy)]
struct QueryShapeStats {
    clauses: usize,
    patterns: usize,
    nodes: usize,
    edges: usize,
    property_maps: usize,
    property_entries: usize,
    expression_nodes: usize,
    boxed_expression_edges: usize,
    literals: usize,
    list_literals: usize,
    map_literals: usize,
    function_calls: usize,
}

impl QueryShapeStats {
    fn merge(&mut self, other: QueryShapeStats) {
        self.clauses += other.clauses;
        self.patterns += other.patterns;
        self.nodes += other.nodes;
        self.edges += other.edges;
        self.property_maps += other.property_maps;
        self.property_entries += other.property_entries;
        self.expression_nodes += other.expression_nodes;
        self.boxed_expression_edges += other.boxed_expression_edges;
        self.literals += other.literals;
        self.list_literals += other.list_literals;
        self.map_literals += other.map_literals;
        self.function_calls += other.function_calls;
    }
}

fn profile_query_shape(query: &Query) -> QueryShapeStats {
    let mut stats = QueryShapeStats {
        clauses: query.clauses.len(),
        ..QueryShapeStats::default()
    };

    for clause in &query.clauses {
        match clause {
            Clause::Match(match_clause) | Clause::OptionalMatch(match_clause) => {
                stats.merge(profile_pattern(&match_clause.pattern));
            }
            Clause::Create(create_clause) => {
                stats.merge(profile_pattern(&create_clause.pattern));
            }
            Clause::Merge(merge_clause) => {
                stats.merge(profile_pattern(&merge_clause.pattern));
            }
            Clause::Call(call_clause) => {
                for expression in &call_clause.args {
                    stats.merge(profile_expression(expression));
                }
            }
            Clause::Return(return_clause) => {
                for item in &return_clause.items {
                    stats.merge(profile_return_item(item));
                }
                for order_item in &return_clause.order_by {
                    stats.merge(profile_expression(&order_item.expression));
                }
            }
            Clause::Where(where_clause) => {
                stats.merge(profile_expression(&where_clause.expression));
            }
            Clause::Set(set_clause) => {
                for item in &set_clause.items {
                    stats.merge(profile_set_item(item));
                }
            }
            Clause::With(with_clause) => {
                for item in &with_clause.items {
                    stats.merge(profile_return_item(item));
                }
                if let Some(where_clause) = &with_clause.where_clause {
                    stats.merge(profile_expression(&where_clause.expression));
                }
            }
            Clause::Unwind(unwind_clause) => {
                stats.merge(profile_expression(&unwind_clause.expression));
            }
            Clause::CreateDecayProfile(_)
            | Clause::AlterDecayProfile(_)
            | Clause::CreatePromotionProfile(_)
            | Clause::AlterPromotionProfile(_) => {}
            Clause::CreatePromotionPolicy(policy_clause) => {
                for when_clause in &policy_clause.when_clauses {
                    black_box(&when_clause.predicate);
                }
            }
            Clause::Delete(_)
            | Clause::Remove(_)
            | Clause::CreateConstraint(_)
            | Clause::DropConstraint(_)
            | Clause::ShowConstraints(_)
            | Clause::CreateIndex(_)
            | Clause::DropIndex(_)
            | Clause::ShowIndexes(_)
            | Clause::DropDecayProfile(_)
            | Clause::ShowDecayProfiles(_)
            | Clause::DropPromotionProfile(_)
            | Clause::ShowPromotionProfiles(_)
            | Clause::AlterPromotionPolicy(_)
            | Clause::DropPromotionPolicy(_)
            | Clause::ShowPromotionPolicies(_)
            | Clause::Foreach(_)
            | Clause::Subquery(_)
            | Clause::WhereExists(_) => {}
        }
    }

    stats
}

fn profile_pattern(pattern: &Pattern) -> QueryShapeStats {
    let mut stats = QueryShapeStats {
        patterns: 1,
        nodes: pattern.nodes.len(),
        edges: pattern.edges.len(),
        ..QueryShapeStats::default()
    };

    for node in &pattern.nodes {
        stats.merge(profile_node_pattern(node));
    }
    for edge in &pattern.edges {
        stats.merge(profile_edge_pattern(edge));
    }
    stats
}

fn profile_node_pattern(node: &NodePattern) -> QueryShapeStats {
    let mut stats = QueryShapeStats {
        property_maps: usize::from(!node.properties.is_empty()),
        property_entries: node.properties.len(),
        ..QueryShapeStats::default()
    };

    for property in &node.properties {
        stats.merge(profile_expression(&property.value));
    }
    stats
}

fn profile_edge_pattern(edge: &EdgePattern) -> QueryShapeStats {
    let mut stats = QueryShapeStats {
        property_maps: usize::from(!edge.properties.is_empty()),
        property_entries: edge.properties.len(),
        ..QueryShapeStats::default()
    };

    for property in &edge.properties {
        stats.merge(profile_expression(&property.value));
    }
    stats
}

fn profile_return_item(item: &ReturnItem) -> QueryShapeStats {
    profile_expression(&item.expression)
}

fn profile_set_item(item: &SetItem) -> QueryShapeStats {
    match item {
        SetItem::Property { value, .. }
        | SetItem::MapAssignment { value, .. }
        | SetItem::MapMerge { value, .. } => profile_expression(value),
        SetItem::DynamicLabel { expression, .. } => profile_expression(expression),
        SetItem::Label { .. } => QueryShapeStats::default(),
    }
}

fn profile_expression(expression: &Expression) -> QueryShapeStats {
    let mut stats = QueryShapeStats {
        expression_nodes: 1,
        ..QueryShapeStats::default()
    };

    match expression {
        Expression::PropertyAccess { .. }
        | Expression::Parameter(_)
        | Expression::ParameterPropertyAccess { .. }
        | Expression::Variable(_) => {}
        Expression::Comparison { operands, .. }
        | Expression::And(operands)
        | Expression::Or(operands)
        | Expression::Add(operands)
        | Expression::Subtract(operands)
        | Expression::Multiply(operands)
        | Expression::Divide(operands)
        | Expression::Modulo(operands)
        | Expression::Xor(operands) => {
            stats.boxed_expression_edges += 2;
            stats.merge(profile_expression(&operands.left));
            stats.merge(profile_expression(&operands.right));
        }
        Expression::InList { operands, .. } => {
            stats.boxed_expression_edges += 2;
            stats.merge(profile_expression(&operands.left));
            stats.merge(profile_expression(&operands.right));
        }
        Expression::Between {
            expression,
            lower,
            upper,
        } => {
            stats.boxed_expression_edges += 3;
            stats.merge(profile_expression(expression));
            stats.merge(profile_expression(lower));
            stats.merge(profile_expression(upper));
        }
        Expression::Literal(_) => {
            stats.literals += 1;
        }
        Expression::FunctionCall { args, .. } => {
            stats.function_calls += 1;
            for arg in args {
                stats.merge(profile_expression(arg));
            }
        }
        Expression::ListLiteral(items) => {
            stats.list_literals += 1;
            for item in items {
                stats.merge(profile_expression(item));
            }
        }
        Expression::ListComprehension(lc) => {
            stats.list_literals += 1;
            stats.merge(profile_expression(&lc.list));
            stats.merge(profile_expression(&lc.expression));
            if let Some(pred) = &lc.predicate {
                stats.merge(profile_expression(pred));
            }
        }
        Expression::PatternComprehension(_) => {
            stats.list_literals += 1;
        }
        Expression::Reduce(re) => {
            stats.merge(profile_expression(&re.initial));
            stats.merge(profile_expression(&re.list));
            stats.merge(profile_expression(&re.expression));
        }
        Expression::MapLiteral(entries) => {
            stats.map_literals += 1;
            for entry in entries {
                stats.merge(profile_expression(&entry.value));
            }
        }
        Expression::Not(inner)
        | Expression::IsNull(inner)
        | Expression::IsNotNull(inner) => {
            stats.boxed_expression_edges += 1;
            stats.merge(profile_expression(inner));
        }
        Expression::Case(case) => {
            if let Some(expr) = &case.expression {
                stats.merge(profile_expression(expr));
            }
            for alt in &case.alternatives {
                stats.merge(profile_expression(&alt.condition));
                stats.merge(profile_expression(&alt.result));
            }
            if let Some(default) = &case.default {
                stats.merge(profile_expression(default));
            }
        }
        Expression::PatternExists { .. } => {}
        Expression::BracketAccess { .. } => {}
    }

    stats
}

#[test]
#[ignore = "profiling-style parse allocation report; run explicitly"]
fn parser_allocation_profile_report() {
    let parser = Parser::new();

    let warmup = parser
        .parse("MATCH (n) RETURN n")
        .expect("warmup parse should succeed");
    black_box(warmup.clauses.len());

    println!("\n{}", "=".repeat(112));
    println!("COPPERDB PARSE ALLOCATION PROFILE");
    println!("{}", "=".repeat(112));
    println!("AST TYPE SIZES (bytes)");
    println!("{}", "-".repeat(112));
    println!("Query: {}", size_of::<Query>());
    println!("Clause: {}", size_of::<Clause>());
    println!("Pattern: {}", size_of::<Pattern>());
    println!("NodePattern: {}", size_of::<NodePattern>());
    println!("EdgePattern: {}", size_of::<EdgePattern>());
    println!("Expression: {}", size_of::<Expression>());
    println!("BinaryExpression: {}", size_of::<BinaryExpression>());
    println!("ReturnItem: {}", size_of::<ReturnItem>());
    println!("SetItem: {}", size_of::<SetItem>());
    println!("LiteralValue: {}", size_of::<LiteralValue>());
    println!("PropertyEntry: {}", size_of::<PropertyEntry>());
    println!("Vec<PropertyEntry>: {}", size_of::<Vec<PropertyEntry>>());
    println!("String: {}", size_of::<String>());
    println!("Box<Expression>: {}", size_of::<Box<Expression>>());

    println!("\n{}", "-".repeat(112));
    println!(
        "{:<24} | {:>9} | {:>10} | {:>9} | {:>6} | {:>6} | {:>7} | {:>8} | {:>5} | {:>5}",
        "Query",
        "allocs",
        "alloc_b",
        "reallocs",
        "exprs",
        "boxes",
        "maps",
        "map_ent",
        "nodes",
        "edges"
    );
    println!("{}", "-".repeat(112));

    let mut total_alloc_calls = 0u64;
    let mut total_alloc_bytes = 0u64;
    let mut total_realloc_calls = 0u64;
    let mut total_stats = QueryShapeStats::default();

    for case in ALLOCATION_CASES {
        let before = AllocationSnapshot::capture();
        let query = parser.parse(case.query).unwrap_or_else(|error| {
            panic!("parse failed for {}: {}", case.name, error);
        });
        let stats = profile_query_shape(&query);
        black_box(query.clauses.len());
        let delta = AllocationSnapshot::capture().delta(before);

        total_alloc_calls += delta.alloc_calls;
        total_alloc_bytes += delta.alloc_bytes + delta.realloc_bytes;
        total_realloc_calls += delta.realloc_calls;
        total_stats.merge(stats);

        println!(
            "{:<24} | {:>9} | {:>10} | {:>9} | {:>6} | {:>6} | {:>7} | {:>8} | {:>5} | {:>5}",
            case.name,
            delta.alloc_calls,
            delta.alloc_bytes + delta.realloc_bytes,
            delta.realloc_calls,
            stats.expression_nodes,
            stats.boxed_expression_edges,
            stats.property_maps,
            stats.property_entries,
            stats.nodes,
            stats.edges,
        );
    }

    println!("{}", "-".repeat(112));
    println!(
        "{:<24} | {:>9} | {:>10} | {:>9} | {:>6} | {:>6} | {:>7} | {:>8} | {:>5} | {:>5}",
        "TOTAL",
        total_alloc_calls,
        total_alloc_bytes,
        total_realloc_calls,
        total_stats.expression_nodes,
        total_stats.boxed_expression_edges,
        total_stats.property_maps,
        total_stats.property_entries,
        total_stats.nodes,
        total_stats.edges,
    );
    println!("{}", "=".repeat(112));

    assert!(
        total_alloc_calls > 0,
        "expected parse profiling to record allocations"
    );
}
