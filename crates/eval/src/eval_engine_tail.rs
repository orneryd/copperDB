use super::*;
use copperdb_storage::EdgeAdjacencyDirection;

impl EvalEngine {
    pub(crate) fn persist_node_props(
        &self,
        props: &HashMap<String, Value>,
    ) -> Result<(), EvalError> {
        let now = now_unix_ms();
        let mut record = node_record_from_props(props)?;
        if let Some(existing) = self.storage.get_node_record(&record.id)? {
            record.created_at_unix_ms = existing.created_at_unix_ms;
            record.updated_at_unix_ms = now;
        } else {
            record.created_at_unix_ms = now;
            record.updated_at_unix_ms = now;
        }
        self.storage.put_node_record(&record)?;
        Ok(())
    }

    pub(crate) fn persist_edge_props(
        &self,
        props: &HashMap<String, Value>,
    ) -> Result<(), EvalError> {
        let now = now_unix_ms();
        let mut record = edge_record_from_props(props)?;
        if let Some(existing) = self.storage.get_edge_record(&record.id)? {
            record.created_at_unix_ms = existing.created_at_unix_ms;
            record.updated_at_unix_ms = now;
        } else {
            record.created_at_unix_ms = now;
            record.updated_at_unix_ms = now;
        }
        self.storage.put_edge_record(&record)?;
        Ok(())
    }

    pub(crate) fn persist_edge_record(
        &self,
        mut edge: EdgeRecord,
    ) -> Result<EdgeRecord, EvalError> {
        let now = now_unix_ms();
        if let Some(existing) = self.storage.get_edge_record(&edge.id)? {
            edge.created_at_unix_ms = existing.created_at_unix_ms;
            edge.updated_at_unix_ms = now;
        } else {
            edge.created_at_unix_ms = now;
            edge.updated_at_unix_ms = now;
        }
        self.storage.put_edge_record(&edge)?;
        Ok(edge)
    }

    pub(crate) fn relationship_candidates(
        &self,
        node_id: &str,
        edge: &EdgePattern,
        expected_props: &HashMap<String, Value>,
        row: &Row,
        params: &HashMap<String, Value>,
        where_expression: Option<&Expression>,
    ) -> Result<Vec<EdgeRecord>, EvalError> {
        let simple_range_predicate =
            extract_relationship_range_predicate(edge, where_expression, row, params)?;
        let candidates = if let Some(range_predicate) = simple_range_predicate {
            self.relationship_candidates_by_range(node_id, edge, &range_predicate, expected_props)?
        } else {
            self.storage.get_adjacent_edges(
                node_id,
                match edge.direction {
                    EdgeDirection::Outgoing => EdgeAdjacencyDirection::Outgoing,
                    EdgeDirection::Incoming => EdgeAdjacencyDirection::Incoming,
                    EdgeDirection::Both => EdgeAdjacencyDirection::Both,
                },
                edge.rel_type.as_deref(),
            )?
        };
        let resolver = self.knowledge_policy_resolver()?;
        let mut visible = Vec::with_capacity(candidates.len());
        for candidate in candidates {
            if self.edge_visible_under_policy(&candidate, &resolver)? {
                visible.push(candidate);
            }
        }
        visible.sort_by(|a, b| a.id.cmp(&b.id));
        visible.dedup_by(|a, b| a.id == b.id);
        for edge in &visible {
            self.apply_on_access_for_edge(edge, &resolver)?;
        }
        Ok(visible)
    }

    pub(crate) fn lookup_edges(
        &self,
        edge_type: Option<&str>,
    ) -> Result<Vec<EdgeRecord>, EvalError> {
        let resolver = self.knowledge_policy_resolver()?;
        let mut visible = Vec::new();
        for edge in IndexCatalog::new(self.storage.as_ref()).lookup_edges(edge_type)? {
            if self.edge_visible_under_policy(&edge, &resolver)? {
                self.apply_on_access_for_edge(&edge, &resolver)?;
                visible.push(edge);
            }
        }
        Ok(visible)
    }

    fn relationship_candidates_by_range(
        &self,
        node_id: &str,
        edge: &EdgePattern,
        predicate: &RelationshipRangePredicate,
        expected_props: &HashMap<String, Value>,
    ) -> Result<Vec<EdgeRecord>, EvalError> {
        let mut candidates = IndexCatalog::new(self.storage.as_ref()).lookup_edges_by_range(
            edge.rel_type.as_deref(),
            &predicate.property,
            predicate.comparison,
            &predicate.value,
            expected_props,
        )?;
        candidates.retain(|candidate| edge_matches_node(candidate, node_id, &edge.direction));
        Ok(candidates)
    }
}

struct RelationshipRangePredicate {
    property: String,
    comparison: CatalogRangeIndexComparison,
    value: Value,
}

fn extract_relationship_range_predicate(
    edge: &EdgePattern,
    where_expression: Option<&Expression>,
    row: &Row,
    params: &HashMap<String, Value>,
) -> Result<Option<RelationshipRangePredicate>, EvalError> {
    let Some(variable) = edge.variable.as_deref() else {
        return Ok(None);
    };
    let Some(edge_type) = edge.rel_type.as_deref() else {
        return Ok(None);
    };
    if edge_type.is_empty() {
        return Ok(None);
    }
    let Some(expression) = where_expression else {
        return Ok(None);
    };

    match expression {
        Expression::Comparison { operands, op } => {
            let Some(comparison) = range_comparison_from_operator(op) else {
                return Ok(None);
            };

            if let Expression::PropertyAccess {
                variable: left_variable,
                property,
            } = &operands.left
            {
                if left_variable == variable {
                    let value = eval_expression(&operands.right, row, params)?;
                    return Ok(is_range_comparable_value(&value).then(|| {
                        RelationshipRangePredicate {
                            property: property.clone(),
                            comparison,
                            value,
                        }
                    }));
                }
            }

            if let Expression::PropertyAccess {
                variable: right_variable,
                property,
            } = &operands.right
            {
                if right_variable == variable {
                    let value = eval_expression(&operands.left, row, params)?;
                    return Ok(is_range_comparable_value(&value).then(|| {
                        RelationshipRangePredicate {
                            property: property.clone(),
                            comparison: invert_range_comparison(comparison),
                            value,
                        }
                    }));
                }
            }

            Ok(None)
        }
        _ => Ok(None),
    }
}

fn edge_matches_node(edge: &EdgeRecord, node_id: &str, direction: &EdgeDirection) -> bool {
    match direction {
        EdgeDirection::Outgoing => edge.start_node == node_id,
        EdgeDirection::Incoming => edge.end_node == node_id,
        EdgeDirection::Both => edge.start_node == node_id || edge.end_node == node_id,
    }
}
