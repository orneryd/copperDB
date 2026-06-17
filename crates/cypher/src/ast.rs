use std::collections::HashMap;

use serde_json::Value;

#[derive(Debug, Clone, PartialEq)]
pub enum QueryType {
    Match,
    Create,
    Merge,
    Delete,
    Remove,
    Set,
    Return,
    With,
    Ddl,
}

#[derive(Debug, Clone)]
pub struct Query {
    pub query_type: QueryType,
    pub clauses: Vec<Clause>,
    pub parameters: HashMap<String, Value>,
}

#[derive(Debug, Clone)]
pub enum Clause {
    Match(MatchClause),
    OptionalMatch(MatchClause),
    Call(CallClause),
    Return(ReturnClause),
    Where(WhereClause),
    Set(SetClause),
    Remove(RemoveClause),
    Delete(DeleteClause),
    Merge(MergeClause),
    With(WithClause),
    Unwind(UnwindClause),
    Foreach(ForeachClause),
    Create(CreateClause),
    CreateConstraint(CreateConstraintClause),
    DropConstraint(DropConstraintClause),
    ShowConstraints(ShowConstraintsClause),
    CreateIndex(CreateIndexClause),
    DropIndex(DropIndexClause),
    ShowIndexes(ShowIndexesClause),
    CreateDecayProfile(CreateDecayProfileClause),
    AlterDecayProfile(AlterDecayProfileClause),
    DropDecayProfile(DropDecayProfileClause),
    ShowDecayProfiles(ShowDecayProfilesClause),
    CreatePromotionProfile(CreatePromotionProfileClause),
    AlterPromotionProfile(AlterPromotionProfileClause),
    DropPromotionProfile(DropPromotionProfileClause),
    ShowPromotionProfiles(ShowPromotionProfilesClause),
    CreatePromotionPolicy(CreatePromotionPolicyClause),
    AlterPromotionPolicy(AlterPromotionPolicyClause),
    DropPromotionPolicy(DropPromotionPolicyClause),
    ShowPromotionPolicies(ShowPromotionPoliciesClause),
}

#[derive(Debug, Clone)]
pub struct MatchClause {
    pub pattern: Pattern,
    pub optional: bool,
}

#[derive(Debug, Clone)]
pub struct CreateClause {
    pub pattern: Pattern,
}

#[derive(Debug, Clone)]
pub struct MergeClause {
    pub pattern: Pattern,
    pub on_create: Vec<SetItem>,
    pub on_match: Vec<SetItem>,
}

#[derive(Debug, Clone)]
pub struct CallClause {
    pub procedure: String,
    pub args: Vec<Expression>,
    pub yield_items: Vec<ReturnItem>,
}

#[derive(Debug, Clone)]
pub struct ReturnClause {
    pub items: Vec<ReturnItem>,
    pub order_by: Vec<OrderItem>,
    pub skip: Option<Expression>,
    pub limit: Option<Expression>,
    pub distinct: bool,
}

#[derive(Debug, Clone)]
pub struct WhereClause {
    pub expression: Expression,
}

#[derive(Debug, Clone)]
pub struct SetClause {
    pub items: Vec<SetItem>,
}

#[derive(Debug, Clone)]
pub struct RemoveClause {
    pub items: Vec<RemoveItem>,
}

#[derive(Debug, Clone)]
pub struct DeleteClause {
    pub variables: Vec<String>,
    pub detach: bool,
}

#[derive(Debug, Clone)]
pub struct WithClause {
    pub items: Vec<ReturnItem>,
    pub order_by: Vec<OrderItem>,
    pub skip: Option<Expression>,
    pub limit: Option<Expression>,
    pub where_clause: Option<WhereClause>,
}

#[derive(Debug, Clone)]
pub struct UnwindClause {
    pub expression: Expression,
    pub variable: String,
}

#[derive(Debug, Clone)]
pub struct ForeachClause {
    pub variable: String,
    pub list: Expression,
    pub updates: Vec<Clause>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConstraintKind {
    Unique,
    Exists,
    NodeKey,
    RelationshipKey,
    Type(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConstraintEntityType {
    Node,
    Relationship,
}

#[derive(Debug, Clone)]
pub struct ConstraintEntry {
    pub properties: Vec<String>,
    pub kind: ConstraintKind,
}

#[derive(Debug, Clone)]
pub struct CreateConstraintClause {
    pub name: String,
    pub if_not_exists: bool,
    pub entity_type: ConstraintEntityType,
    pub label: String,
    pub entries: Vec<ConstraintEntry>,
}

#[derive(Debug, Clone)]
pub struct DropConstraintClause {
    pub name: String,
    pub if_exists: bool,
}

#[derive(Debug, Clone)]
pub struct ShowConstraintsClause;

#[derive(Debug, Clone)]
pub struct CreateIndexClause {
    pub name: String,
    pub if_not_exists: bool,
    pub kind: IndexKind,
    pub entity_type: IndexEntityType,
    pub label: String,
    pub properties: Vec<String>,
    pub options: HashMap<String, Value>,
}

#[derive(Debug, Clone)]
pub struct DropIndexClause {
    pub name: String,
    pub if_exists: bool,
}

#[derive(Debug, Clone)]
pub struct ShowIndexesClause {
    pub kind: Option<IndexKind>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexEntityType {
    Node,
    Relationship,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexKind {
    Range,
    Temporal,
    FullText,
    Vector,
}

#[derive(Debug, Clone)]
pub struct CreateDecayProfileClause {
    pub name: String,
    pub options: HashMap<String, Value>,
    pub target: Option<KnowledgePolicyTarget>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgePolicyTarget {
    pub target_labels: Vec<String>,
    pub target_edge_type: Option<String>,
    pub is_wildcard: bool,
    pub is_edge: bool,
}

#[derive(Debug, Clone)]
pub struct AlterDecayProfileClause {
    pub name: String,
    pub options: HashMap<String, Value>,
}

#[derive(Debug, Clone)]
pub struct DropDecayProfileClause {
    pub name: String,
    pub if_exists: bool,
}

#[derive(Debug, Clone)]
pub struct ShowDecayProfilesClause;

#[derive(Debug, Clone)]
pub struct CreatePromotionProfileClause {
    pub name: String,
    pub options: HashMap<String, Value>,
}

#[derive(Debug, Clone)]
pub struct AlterPromotionProfileClause {
    pub name: String,
    pub options: HashMap<String, Value>,
}

#[derive(Debug, Clone)]
pub struct DropPromotionProfileClause {
    pub name: String,
    pub if_exists: bool,
}

#[derive(Debug, Clone)]
pub struct ShowPromotionProfilesClause;

#[derive(Debug, Clone)]
pub struct PromotionWhenClause {
    pub profile_ref: String,
    pub predicate: String,
    pub order: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PromotionOnAccessMutationKind {
    SetLastAccessedNow,
    IncrementAccessCount,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PromotionOnAccessMutation {
    pub kind: PromotionOnAccessMutationKind,
}

#[derive(Debug, Clone)]
pub struct CreatePromotionPolicyClause {
    pub name: String,
    pub target: KnowledgePolicyTarget,
    pub enabled: bool,
    pub on_access_mutations: Vec<PromotionOnAccessMutation>,
    pub when_clauses: Vec<PromotionWhenClause>,
}

#[derive(Debug, Clone)]
pub struct AlterPromotionPolicyClause {
    pub name: String,
    pub enabled: bool,
}

#[derive(Debug, Clone)]
pub struct DropPromotionPolicyClause {
    pub name: String,
    pub if_exists: bool,
}

#[derive(Debug, Clone)]
pub struct ShowPromotionPoliciesClause;

#[derive(Debug, Clone)]
pub struct Pattern {
    pub path_variable: Option<String>,
    pub shortest_path: bool,
    pub nodes: Vec<NodePattern>,
    pub edges: Vec<EdgePattern>,
    pub segment_edge_counts: Vec<usize>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PatternSegment {
    pub node_start: usize,
    pub node_len: usize,
    pub edge_start: usize,
    pub edge_len: usize,
}

impl Pattern {
    pub fn segments(&self) -> Vec<PatternSegment> {
        if self.nodes.is_empty() {
            return Vec::new();
        }

        let segment_edge_counts = if self.segment_edge_counts.is_empty() {
            vec![self.edges.len()]
        } else {
            self.segment_edge_counts.clone()
        };

        let mut segments = Vec::with_capacity(segment_edge_counts.len());
        let mut node_start = 0;
        let mut edge_start = 0;

        for edge_len in segment_edge_counts {
            let node_len = edge_len + 1;
            segments.push(PatternSegment {
                node_start,
                node_len,
                edge_start,
                edge_len,
            });
            node_start += node_len;
            edge_start += edge_len;
        }

        segments
    }

    pub fn split_segments(&self) -> Vec<Pattern> {
        let segments = self.segments();
        let preserve_path_variable = segments.len() == 1;

        segments
            .into_iter()
            .map(|segment| Pattern {
                path_variable: preserve_path_variable
                    .then(|| self.path_variable.clone())
                    .flatten(),
                shortest_path: self.shortest_path,
                nodes: self.nodes[segment.node_start..segment.node_start + segment.node_len]
                    .to_vec(),
                edges: self.edges[segment.edge_start..segment.edge_start + segment.edge_len]
                    .to_vec(),
                segment_edge_counts: vec![segment.edge_len],
            })
            .collect()
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct PropertyEntry {
    pub key: String,
    pub value: Expression,
}

#[derive(Debug, Clone)]
pub struct NodePattern {
    pub variable: Option<String>,
    pub labels: Vec<String>,
    pub properties: Vec<PropertyEntry>,
}

#[derive(Debug, Clone)]
pub struct EdgePattern {
    pub variable: Option<String>,
    pub rel_type: Option<String>,
    pub direction: EdgeDirection,
    pub properties: Vec<PropertyEntry>,
    pub min_hops: Option<u32>,
    pub max_hops: Option<u32>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum EdgeDirection {
    Both,
    Outgoing,
    Incoming,
}

#[derive(Debug, Clone, PartialEq)]
pub enum LiteralValue {
    String(String),
    Integer(i64),
    Float(f64),
    Bool(bool),
    Null,
}

#[derive(Debug, Clone, PartialEq)]
pub struct BinaryExpression {
    pub left: Expression,
    pub right: Expression,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Expression {
    PropertyAccess {
        variable: String,
        property: String,
    },
    Comparison {
        operands: Box<BinaryExpression>,
        op: String,
    },
    InList {
        operands: Box<BinaryExpression>,
        negated: bool,
    },
    Between {
        expression: Box<Expression>,
        lower: Box<Expression>,
        upper: Box<Expression>,
    },
    Literal(LiteralValue),
    Parameter(String),
    ParameterPropertyAccess {
        parameter: String,
        property: String,
    },
    FunctionCall {
        name: String,
        args: Vec<Expression>,
        distinct: bool,
    },
    ListLiteral(Vec<Expression>),
    ListComprehension(ListComprehension),
    Reduce(ReduceExpression),
    MapLiteral(Vec<PropertyEntry>),
    Variable(String),
    And(Box<BinaryExpression>),
    Or(Box<BinaryExpression>),
    Not(Box<Expression>),
    IsNull(Box<Expression>),
    IsNotNull(Box<Expression>),
    Add(Box<BinaryExpression>),
    Subtract(Box<BinaryExpression>),
    Multiply(Box<BinaryExpression>),
    Divide(Box<BinaryExpression>),
    Modulo(Box<BinaryExpression>),
    Xor(Box<BinaryExpression>),
    PatternExists {
        variable: String,
        rel_type: String,
        target_variable: String,
    },
    Case(CaseExpression),
}

#[derive(Debug, Clone, PartialEq)]
pub struct CaseExpression {
    /// For simple CASE: CASE expr WHEN ... (None for searched CASE)
    pub expression: Option<Box<Expression>>,
    /// WHEN ... THEN ... pairs
    pub alternatives: Vec<CaseAlternative>,
    /// Optional ELSE result
    pub default: Option<Box<Expression>>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CaseAlternative {
    pub condition: Expression,
    pub result: Expression,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ListComprehension {
    pub variable: String,
    pub list: Box<Expression>,
    pub predicate: Option<Box<Expression>>,
    pub expression: Box<Expression>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ReduceExpression {
    pub accumulator: String,
    pub initial: Box<Expression>,
    pub variable: String,
    pub list: Box<Expression>,
    pub expression: Box<Expression>,
}

#[derive(Debug, Clone)]
pub struct ReturnItem {
    pub expression: Expression,
    pub alias: Option<String>,
}

#[derive(Debug, Clone)]
pub struct OrderItem {
    pub expression: Expression,
    pub descending: bool,
}

#[derive(Debug, Clone)]
pub enum SetItem {
    Property {
        variable: String,
        property: String,
        value: Expression,
    },
    MapAssignment {
        variable: String,
        value: Expression,
    },
    MapMerge {
        variable: String,
        value: Expression,
    },
    Label {
        variable: String,
        label: String,
    },
    DynamicLabel {
        variable: String,
        expression: Expression,
    },
}

#[derive(Debug, Clone)]
pub enum RemoveItem {
    Property { variable: String, property: String },
    Label { variable: String, label: String },
}
