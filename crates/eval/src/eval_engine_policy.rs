use super::*;

impl EvalEngine {
    pub(crate) fn execute_call_clause(
        &self,
        call: &copperdb_cypher::CallClause,
        params: &HashMap<String, Value>,
    ) -> Result<EvalResult, EvalError> {
        let result = if call
            .procedure
            .eq_ignore_ascii_case("db.labels")
        {
            self.execute_db_labels_call(call)
        } else if call
            .procedure
            .eq_ignore_ascii_case("db.relationshipTypes")
        {
            self.execute_db_relationship_types_call(call)
        } else if call
            .procedure
            .eq_ignore_ascii_case("dbms.procedures")
        {
            self.execute_dbms_procedures_call(call)
        } else if call
            .procedure
            .eq_ignore_ascii_case("nornicdb.knowledgepolicy.resolve")
        {
            self.execute_knowledge_policy_resolve_call(call, params)
        } else if call
            .procedure
            .eq_ignore_ascii_case("db.index.fulltext.queryNodes")
        {
            self.execute_fulltext_query_nodes_call(call, params)
        } else if call
            .procedure
            .eq_ignore_ascii_case("db.index.vector.queryNodes")
        {
            self.execute_vector_query_nodes_call(call, params)
        } else {
            Err(EvalError::ExecutionError(format!(
                "CALL {} is not supported yet",
                call.procedure
            )))
        }?;

        self.project_call_result(call, result, params)
    }

    fn execute_db_labels_call(
        &self,
        call: &copperdb_cypher::CallClause,
    ) -> Result<EvalResult, EvalError> {
        if !call.args.is_empty() {
            return Err(EvalError::ExecutionError(
                "db.labels expects no arguments".to_string(),
            ));
        }

        let mut labels = self
            .storage
            .all_node_records()?
            .into_iter()
            .flat_map(|node| node.labels)
            .collect::<Vec<_>>();
        labels.sort();
        labels.dedup();

        Ok(EvalResult {
            columns: vec!["label".to_string()],
            rows: labels
                .into_iter()
                .map(|label| {
                    let mut row = Row::new();
                    row.insert("label".to_string(), Value::String(label));
                    row
                })
                .collect(),
            stats: QueryStats::default(),
        })
    }

    fn execute_db_relationship_types_call(
        &self,
        call: &copperdb_cypher::CallClause,
    ) -> Result<EvalResult, EvalError> {
        if !call.args.is_empty() {
            return Err(EvalError::ExecutionError(
                "db.relationshipTypes expects no arguments".to_string(),
            ));
        }

        let mut relationship_types = self
            .storage
            .all_edges()?
            .into_iter()
            .map(|edge| edge.edge_type)
            .collect::<Vec<_>>();
        relationship_types.sort();
        relationship_types.dedup();

        Ok(EvalResult {
            columns: vec!["relationshipType".to_string()],
            rows: relationship_types
                .into_iter()
                .map(|relationship_type| {
                    let mut row = Row::new();
                    row.insert(
                        "relationshipType".to_string(),
                        Value::String(relationship_type),
                    );
                    row
                })
                .collect(),
            stats: QueryStats::default(),
        })
    }

    fn execute_dbms_procedures_call(
        &self,
        call: &copperdb_cypher::CallClause,
    ) -> Result<EvalResult, EvalError> {
        if !call.args.is_empty() {
            return Err(EvalError::ExecutionError(
                "dbms.procedures expects no arguments".to_string(),
            ));
        }

        let rows = builtin_procedure_rows();
        Ok(EvalResult {
            columns: vec![
                "name".to_string(),
                "signature".to_string(),
                "description".to_string(),
                "mode".to_string(),
            ],
            rows,
            stats: QueryStats::default(),
        })
    }

    fn project_call_result(
        &self,
        call: &copperdb_cypher::CallClause,
        result: EvalResult,
        params: &HashMap<String, Value>,
    ) -> Result<EvalResult, EvalError> {
        if call.yield_items.is_empty() {
            return Ok(result);
        }

        if call.yield_items.len() == 1
            && call.yield_items[0].alias.is_none()
            && matches!(
                &call.yield_items[0].expression,
                Expression::Variable(name) if name == "*"
            )
        {
            return Ok(result);
        }

        let columns = call.yield_items.iter().map(column_name).collect();
        let rows = result
            .rows
            .iter()
            .map(|row| project_row(row, &call.yield_items, params))
            .collect::<Result<Vec<_>, _>>()?;

        Ok(EvalResult {
            columns,
            rows,
            stats: result.stats,
        })
    }

    fn execute_knowledge_policy_resolve_call(
        &self,
        call: &copperdb_cypher::CallClause,
        params: &HashMap<String, Value>,
    ) -> Result<EvalResult, EvalError> {
        if call.args.len() != 3 {
            return Err(EvalError::ExecutionError(
                "nornicdb.knowledgepolicy.resolve expects 3 arguments: entityId, labelsCsv, edgeType"
                    .to_string(),
            ));
        }

        let row = Row::new();
        let entity_id = eval_expression(&call.args[0], &row, params)?;
        let labels_csv = eval_expression(&call.args[1], &row, params)?;
        let edge_type = eval_expression(&call.args[2], &row, params)?;

        let entity_id = entity_id.as_str().unwrap_or_default().trim().to_string();
        let labels_csv = labels_csv.as_str().unwrap_or_default().trim().to_string();
        let edge_type = edge_type.as_str().unwrap_or_default().trim().to_string();

        let inspection = if !entity_id.is_empty() {
            self.inspect_knowledge_policy_by_id(&entity_id, params)?
        } else if !edge_type.is_empty() {
            self.inspect_knowledge_policy_for_edge_type(&edge_type, params)?
        } else {
            let labels = labels_csv
                .split(',')
                .map(str::trim)
                .filter(|label| !label.is_empty())
                .map(str::to_string)
                .collect::<Vec<_>>();
            self.inspect_knowledge_policy_for_labels(&labels, params)?
        };

        Ok(EvalResult {
            columns: vec![
                "entityId".to_string(),
                "targetKind".to_string(),
                "targetLabels".to_string(),
                "targetEdgeType".to_string(),
                "decayBinding".to_string(),
                "promotionPolicy".to_string(),
                "matchedPromotionProfile".to_string(),
                "matchedPromotionPredicate".to_string(),
                "scoreFrom".to_string(),
                "anchorUnixMs".to_string(),
                "accessCount".to_string(),
                "lastAccessedAtUnixMs".to_string(),
                "baseScore".to_string(),
                "finalScore".to_string(),
                "visibilityThreshold".to_string(),
                "suppressed".to_string(),
                "dryRun".to_string(),
                "explanation".to_string(),
            ],
            rows: vec![inspection.into_row()],
            stats: QueryStats::default(),
        })
    }

    fn execute_vector_query_nodes_call(
        &self,
        call: &copperdb_cypher::CallClause,
        params: &HashMap<String, Value>,
    ) -> Result<EvalResult, EvalError> {
        if call.args.len() != 3 {
            return Err(EvalError::ExecutionError(
                "db.index.vector.queryNodes expects 3 arguments: indexName, limit, queryVector"
                    .to_string(),
            ));
        }

        let row = Row::new();
        let index_name = eval_expression(&call.args[0], &row, params)?;
        let limit = eval_expression(&call.args[1], &row, params)?;
        let query_vector = eval_expression(&call.args[2], &row, params)?;

        let index_name = call_arg_string(&index_name, "indexName")?;
        let limit = call_arg_usize(&limit, "limit")?;
        let query_vector = call_arg_vector(&query_vector, "queryVector")?;

        let catalog = IndexCatalog::new(self.storage.as_ref());
        let index = catalog
            .get(&index_name)?
            .ok_or_else(|| EvalError::ExecutionError(format!("index not found: {index_name}")))?;

        if index.kind != copperdb_indexing::CatalogIndexKind::Vector {
            return Err(EvalError::ExecutionError(format!(
                "index {index_name} is not a vector index"
            )));
        }

        if index.entity_type != copperdb_indexing::CatalogIndexEntityType::Node {
            return Err(EvalError::ExecutionError(format!(
                "db.index.vector.queryNodes only supports node indexes: {index_name}"
            )));
        }

        let property = index.properties.first().ok_or_else(|| {
            EvalError::ExecutionError(format!(
                "vector index {index_name} is missing a target property"
            ))
        })?;

        let mut ranked = if index.label.is_empty() {
            self.storage.all_node_records()?
        } else {
            self.storage.get_nodes_by_label(&index.label)?
        }
        .into_iter()
        .filter_map(|node| {
            let vector = node_vector_for_property(&node, property)?;
            let score = cosine_similarity(&query_vector, &vector)?;
            Some((node, score))
        })
        .collect::<Vec<_>>();

        ranked.sort_by(|(left_node, left_score), (right_node, right_score)| {
            right_score
                .total_cmp(left_score)
                .then(left_node.id.cmp(&right_node.id))
        });
        ranked.truncate(limit);

        let rows = ranked
            .into_iter()
            .map(|(node, score)| {
                let mut row = Row::new();
                row.insert(
                    "node".to_string(),
                    Value::Object(node_record_to_props(&node).into_iter().collect()),
                );
                row.insert("score".to_string(), Value::from(score as f64));
                row
            })
            .collect();

        Ok(EvalResult {
            columns: vec!["node".to_string(), "score".to_string()],
            rows,
            stats: QueryStats::default(),
        })
    }

    fn execute_fulltext_query_nodes_call(
        &self,
        call: &copperdb_cypher::CallClause,
        params: &HashMap<String, Value>,
    ) -> Result<EvalResult, EvalError> {
        if !(call.args.len() == 2 || call.args.len() == 3) {
            return Err(EvalError::ExecutionError(
                "db.index.fulltext.queryNodes expects 2 or 3 arguments: indexName, queryString, optionsMap"
                    .to_string(),
            ));
        }

        let row = Row::new();
        let index_name = eval_expression(&call.args[0], &row, params)?;
        let query_text = eval_expression(&call.args[1], &row, params)?;
        let options = if call.args.len() == 3 {
            Some(eval_expression(&call.args[2], &row, params)?)
        } else {
            None
        };

        let index_name = call_arg_string(&index_name, "indexName")?;
        let query_text = call_arg_string(&query_text, "queryString")?;
        let options = call_arg_fulltext_options(options.as_ref())?;

        let indexes = resolve_fulltext_node_indexes(self.storage.as_ref(), &index_name)?;

        let fetch_limit = options
            .limit
            .map(|limit| options.skip.saturating_add(limit))
            .unwrap_or(usize::MAX);

        let mut merged: HashMap<String, (NodeRecord, usize, usize)> = HashMap::new();
        let mut ordinal = 0usize;
        for index in indexes {
            for (node, score) in self.storage.search_fulltext_nodes_by_properties(
                &index.label,
                &index.properties,
                &query_text,
                fetch_limit,
            )? {
                let first_seen = ordinal;
                ordinal = ordinal.saturating_add(1);
                merged
                    .entry(node.id.clone())
                    .and_modify(|(_, existing_score, existing_ordinal)| {
                        *existing_score += score;
                        *existing_ordinal = (*existing_ordinal).min(first_seen);
                    })
                    .or_insert((node, score, first_seen));
            }
        }

        let mut ranked: Vec<(NodeRecord, usize, usize)> = merged.into_values().collect();
        ranked.sort_by(|(left_node, left_score, left_ordinal), (right_node, right_score, right_ordinal)| {
            right_score
                .cmp(left_score)
                .then(left_ordinal.cmp(right_ordinal))
                .then(left_node.id.cmp(&right_node.id))
        });

        if options.skip > 0 {
            ranked = ranked.into_iter().skip(options.skip).collect();
        }
        if let Some(limit) = options.limit {
            ranked.truncate(limit);
        }

        let rows = ranked
            .into_iter()
            .map(|(node, score, _)| {
                let mut row = Row::new();
                row.insert(
                    "node".to_string(),
                    Value::Object(node_record_to_props(&node).into_iter().collect()),
                );
                row.insert("score".to_string(), Value::from(score as f64));
                row
            })
            .collect();

        Ok(EvalResult {
            columns: vec!["node".to_string(), "score".to_string()],
            rows,
            stats: QueryStats::default(),
        })
    }

    fn inspect_knowledge_policy_by_id(
        &self,
        entity_id: &str,
        params: &HashMap<String, Value>,
    ) -> Result<KnowledgePolicyInspection, EvalError> {
        let resolver = self.knowledge_policy_resolver()?;
        if let Some(node) = self.storage.get_node_record(entity_id)? {
            return self.inspect_node_policy(&resolver, &node, params, false);
        }
        if let Some(edge) = self.storage.get_edge_record(entity_id)? {
            return self.inspect_edge_policy(&resolver, &edge, params, false);
        }
        Err(EvalError::ExecutionError(format!(
            "entity {entity_id:?} not found"
        )))
    }

    fn inspect_knowledge_policy_for_labels(
        &self,
        labels: &[String],
        params: &HashMap<String, Value>,
    ) -> Result<KnowledgePolicyInspection, EvalError> {
        let resolver = self.knowledge_policy_resolver()?;
        let binding = resolver.resolve_node(labels);
        let promotion_policy = binding
            .as_ref()
            .and_then(|compiled| compiled.promotion_policy.clone())
            .or_else(|| resolver.resolve_node_promotion(labels));

        self.inspect_resolved_target(
            None,
            "NODE".to_string(),
            labels.to_vec(),
            None,
            binding.as_ref(),
            promotion_policy.as_ref(),
            None,
            None,
            params,
            true,
        )
    }

    fn inspect_knowledge_policy_for_edge_type(
        &self,
        edge_type: &str,
        params: &HashMap<String, Value>,
    ) -> Result<KnowledgePolicyInspection, EvalError> {
        let resolver = self.knowledge_policy_resolver()?;
        let binding = resolver.resolve_edge(edge_type);
        let promotion_policy = binding
            .as_ref()
            .and_then(|compiled| compiled.promotion_policy.clone())
            .or_else(|| resolver.resolve_edge_promotion(edge_type));

        self.inspect_resolved_target(
            None,
            "EDGE".to_string(),
            Vec::new(),
            Some(edge_type.to_string()),
            binding.as_ref(),
            promotion_policy.as_ref(),
            None,
            None,
            params,
            true,
        )
    }

    fn inspect_node_policy(
        &self,
        resolver: &Resolver,
        node: &NodeRecord,
        params: &HashMap<String, Value>,
        dry_run: bool,
    ) -> Result<KnowledgePolicyInspection, EvalError> {
        let access_metadata = self.knowledge_policy_access_metadata(&node.id)?;

        self.inspect_node_policy_with_access_metadata(
            resolver,
            node,
            access_metadata,
            params,
            dry_run,
        )
    }

    fn inspect_node_policy_with_access_metadata(
        &self,
        resolver: &Resolver,
        node: &NodeRecord,
        access_metadata: Option<KnowledgePolicyAccessMetadata>,
        params: &HashMap<String, Value>,
        dry_run: bool,
    ) -> Result<KnowledgePolicyInspection, EvalError> {
        let binding = resolver.resolve_node(&node.labels);
        let promotion_policy = binding
            .as_ref()
            .and_then(|compiled| compiled.promotion_policy.clone())
            .or_else(|| resolver.resolve_node_promotion(&node.labels));

        self.inspect_resolved_target(
            Some(node.id.clone()),
            "NODE".to_string(),
            node.labels.clone(),
            None,
            binding.as_ref(),
            promotion_policy.as_ref(),
            Some((
                node.created_at_unix_ms,
                node.updated_at_unix_ms,
                &node.properties,
            )),
            access_metadata,
            params,
            dry_run,
        )
    }

    fn inspect_edge_policy(
        &self,
        resolver: &Resolver,
        edge: &EdgeRecord,
        params: &HashMap<String, Value>,
        dry_run: bool,
    ) -> Result<KnowledgePolicyInspection, EvalError> {
        let access_metadata = self.knowledge_policy_access_metadata(&edge.id)?;

        self.inspect_edge_policy_with_access_metadata(
            resolver,
            edge,
            access_metadata,
            params,
            dry_run,
        )
    }

    fn inspect_edge_policy_with_access_metadata(
        &self,
        resolver: &Resolver,
        edge: &EdgeRecord,
        access_metadata: Option<KnowledgePolicyAccessMetadata>,
        params: &HashMap<String, Value>,
        dry_run: bool,
    ) -> Result<KnowledgePolicyInspection, EvalError> {
        let binding = resolver.resolve_edge(&edge.edge_type);
        let promotion_policy = binding
            .as_ref()
            .and_then(|compiled| compiled.promotion_policy.clone())
            .or_else(|| resolver.resolve_edge_promotion(&edge.edge_type));

        self.inspect_resolved_target(
            Some(edge.id.clone()),
            "EDGE".to_string(),
            Vec::new(),
            Some(edge.edge_type.clone()),
            binding.as_ref(),
            promotion_policy.as_ref(),
            Some((
                edge.created_at_unix_ms,
                edge.updated_at_unix_ms,
                &edge.properties,
            )),
            access_metadata,
            params,
            dry_run,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn inspect_resolved_target(
        &self,
        entity_id: Option<String>,
        target_kind: String,
        target_labels: Vec<String>,
        target_edge_type: Option<String>,
        binding: Option<&CompiledBinding>,
        promotion_policy: Option<&copperdb_knowledgepolicy::PromotionPolicyDef>,
        entity_state: Option<(i64, i64, &BTreeMap<String, Value>)>,
        access_metadata: Option<KnowledgePolicyAccessMetadata>,
        params: &HashMap<String, Value>,
        dry_run: bool,
    ) -> Result<KnowledgePolicyInspection, EvalError> {
        let Some(binding) = binding else {
            let explanation = if let Some(policy) = promotion_policy {
                format!(
                    "policy-only target using promotion policy {}; no decay binding applies so final score defaults to 1.0",
                    policy.name
                )
            } else {
                "no decay binding or promotion policy matched; final score defaults to 1.0"
                    .to_string()
            };
            return Ok(KnowledgePolicyInspection {
                entity_id,
                target_kind,
                target_labels,
                target_edge_type,
                decay_binding: None,
                promotion_policy: promotion_policy.map(|policy| policy.name.clone()),
                matched_promotion_profile: None,
                matched_promotion_predicate: None,
                score_from: None,
                anchor_unix_ms: None,
                access_count: access_metadata
                    .as_ref()
                    .map(|metadata| metadata.access_count),
                last_accessed_at_unix_ms: access_metadata
                    .as_ref()
                    .and_then(|metadata| metadata.last_accessed_at_unix_ms),
                base_score: 1.0,
                final_score: 1.0,
                visibility_threshold: 0.0,
                suppressed: false,
                dry_run,
                explanation,
            });
        };

        let (anchor_unix_ms, matched_rule, score) =
            if let Some((created_at_unix_ms, updated_at_unix_ms, properties)) = entity_state {
                let anchor_unix_ms = binding_anchor_unix_ms(
                    binding,
                    created_at_unix_ms,
                    updated_at_unix_ms,
                    access_metadata
                        .as_ref()
                        .and_then(|metadata| metadata.last_accessed_at_unix_ms),
                    properties,
                );
                let matched_rule =
                    matched_promotion_rule(binding, properties, access_metadata.as_ref(), params)?;
                let score = score_binding(
                    binding,
                    anchor_unix_ms,
                    now_unix_ms(),
                    matched_rule.as_ref().map(|rule| &rule.profile),
                );
                (anchor_unix_ms, matched_rule, score)
            } else {
                (
                    None,
                    None,
                    score_binding(binding, None, now_unix_ms(), None),
                )
            };

        let explanation = if dry_run {
            format!(
                "dry-run target resolution using decay binding {}{}",
                binding.decay_binding.name,
                promotion_policy
                    .map(|policy| format!(" and promotion policy {}", policy.name))
                    .unwrap_or_default()
            )
        } else if score.suppressed {
            format!(
                "final score {:.4} is below visibility threshold {:.4}",
                score.final_score, binding.visibility_threshold
            )
        } else if let Some(rule) = &matched_rule {
            format!(
                "promotion predicate {:?} matched profile {} and produced final score {:.4}",
                rule.predicate, rule.profile.name, score.final_score
            )
        } else {
            format!(
                "no promotion predicate matched; final score {:.4} remains visible against threshold {:.4}",
                score.final_score, binding.visibility_threshold
            )
        };

        Ok(KnowledgePolicyInspection {
            entity_id,
            target_kind,
            target_labels,
            target_edge_type,
            decay_binding: Some(binding.decay_binding.name.clone()),
            promotion_policy: promotion_policy.map(|policy| policy.name.clone()),
            matched_promotion_profile: matched_rule.as_ref().map(|rule| rule.profile.name.clone()),
            matched_promotion_predicate: matched_rule.as_ref().map(|rule| rule.predicate.clone()),
            score_from: Some(format!("{:?}", binding.score_from).to_ascii_uppercase()),
            anchor_unix_ms,
            access_count: access_metadata
                .as_ref()
                .map(|metadata| metadata.access_count),
            last_accessed_at_unix_ms: access_metadata
                .as_ref()
                .and_then(|metadata| metadata.last_accessed_at_unix_ms),
            base_score: score.base_score,
            final_score: score.final_score,
            visibility_threshold: binding.visibility_threshold,
            suppressed: score.suppressed,
            dry_run,
            explanation,
        })
    }

    pub fn node_visible_with_access_metadata(
        &self,
        node: &NodeRecord,
        access_metadata: Option<KnowledgePolicyAccessMetadata>,
        params: &HashMap<String, Value>,
    ) -> Result<bool, EvalError> {
        let resolver = self.knowledge_policy_resolver()?;
        Ok(!self
            .inspect_node_policy_with_access_metadata(
                &resolver,
                node,
                access_metadata,
                params,
                false,
            )?
            .suppressed)
    }

    pub fn edge_visible_with_access_metadata(
        &self,
        edge: &EdgeRecord,
        access_metadata: Option<KnowledgePolicyAccessMetadata>,
        params: &HashMap<String, Value>,
    ) -> Result<bool, EvalError> {
        let resolver = self.knowledge_policy_resolver()?;
        Ok(!self
            .inspect_edge_policy_with_access_metadata(
                &resolver,
                edge,
                access_metadata,
                params,
                false,
            )?
            .suppressed)
    }

    pub fn node_access_metadata_after_read(
        &self,
        node: &NodeRecord,
        access_metadata: Option<KnowledgePolicyAccessMetadata>,
    ) -> Result<Option<KnowledgePolicyAccessMetadata>, EvalError> {
        let resolver = self.knowledge_policy_resolver()?;
        let policy = resolver
            .resolve_node(&node.labels)
            .and_then(|binding| binding.promotion_policy)
            .or_else(|| resolver.resolve_node_promotion(&node.labels));
        Ok(access_metadata_after_policy_access(
            access_metadata,
            policy.as_ref(),
            now_unix_ms(),
        ))
    }

    pub fn edge_access_metadata_after_read(
        &self,
        edge: &EdgeRecord,
        access_metadata: Option<KnowledgePolicyAccessMetadata>,
    ) -> Result<Option<KnowledgePolicyAccessMetadata>, EvalError> {
        let resolver = self.knowledge_policy_resolver()?;
        let policy = resolver
            .resolve_edge(&edge.edge_type)
            .and_then(|binding| binding.promotion_policy)
            .or_else(|| resolver.resolve_edge_promotion(&edge.edge_type));
        Ok(access_metadata_after_policy_access(
            access_metadata,
            policy.as_ref(),
            now_unix_ms(),
        ))
    }
}

fn call_arg_string(value: &Value, arg_name: &str) -> Result<String, EvalError> {
    value.as_str().map(str::to_string).ok_or_else(|| {
        EvalError::ExecutionError(format!("CALL argument {arg_name} must be a string"))
    })
}

fn call_arg_usize(value: &Value, arg_name: &str) -> Result<usize, EvalError> {
    if let Some(value) = value.as_u64() {
        return usize::try_from(value).map_err(|_| {
            EvalError::ExecutionError(format!(
                "CALL argument {arg_name} is too large for this platform"
            ))
        });
    }

    Err(EvalError::ExecutionError(format!(
        "CALL argument {arg_name} must be a non-negative integer"
    )))
}

fn call_arg_vector(value: &Value, arg_name: &str) -> Result<Vec<f32>, EvalError> {
    let vector = value_to_vector(value).ok_or_else(|| {
        EvalError::ExecutionError(format!("CALL argument {arg_name} must be a numeric array"))
    })?;

    if vector.is_empty() {
        return Err(EvalError::ExecutionError(format!(
            "CALL argument {arg_name} must not be empty"
        )));
    }

    Ok(vector)
}

struct FulltextCallOptions {
    skip: usize,
    limit: Option<usize>,
}

fn builtin_procedure_rows() -> Vec<Row> {
    let mut procedures = vec![
        (
            "db.index.fulltext.queryNodes",
            "db.index.fulltext.queryNodes(indexName :: STRING, query :: STRING, options = {} :: MAP) :: (node :: NODE, score :: FLOAT)",
            "Fulltext search on nodes",
            "READ",
        ),
        (
            "db.index.vector.queryNodes",
            "db.index.vector.queryNodes(indexName :: STRING, numberOfResults :: INTEGER, query :: LIST<FLOAT>|STRING|$param) :: (node :: NODE, score :: FLOAT)",
            "Vector search on nodes",
            "READ",
        ),
        (
            "db.labels",
            "db.labels() :: (label :: STRING)",
            "Lists all labels in the database",
            "READ",
        ),
        (
            "db.relationshipTypes",
            "db.relationshipTypes() :: (relationshipType :: STRING)",
            "Lists all relationship types in the database",
            "READ",
        ),
        (
            "dbms.procedures",
            "dbms.procedures() :: (name :: STRING, signature :: STRING, description :: STRING, mode :: STRING)",
            "Lists procedures",
            "DBMS",
        ),
        (
            "nornicdb.knowledgepolicy.resolve",
            "nornicdb.knowledgepolicy.resolve(entityId :: STRING = '', labelsCsv :: STRING = '', edgeType :: STRING = '') :: (entityId :: STRING, targetKind :: STRING, targetLabels :: STRING, targetEdgeType :: STRING, decayBinding :: STRING, promotionPolicy :: STRING, matchedPromotionProfile :: STRING, matchedPromotionPredicate :: STRING, scoreFrom :: STRING, anchorUnixMs :: INTEGER, accessCount :: INTEGER, lastAccessedAtUnixMs :: INTEGER, baseScore :: FLOAT, finalScore :: FLOAT, visibilityThreshold :: FLOAT, suppressed :: BOOLEAN, dryRun :: BOOLEAN, explanation :: STRING)",
            "Resolves the effective knowledge-layer scoring policy for an entity, label set, or edge type",
            "READ",
        ),
    ];
    procedures.sort_by(|left, right| left.0.cmp(right.0));

    procedures
        .into_iter()
        .map(|(name, signature, description, mode)| {
            let mut row = Row::new();
            row.insert("name".to_string(), Value::String(name.to_string()));
            row.insert(
                "signature".to_string(),
                Value::String(signature.to_string()),
            );
            row.insert(
                "description".to_string(),
                Value::String(description.to_string()),
            );
            row.insert("mode".to_string(), Value::String(mode.to_string()));
            row
        })
        .collect()
}

fn resolve_fulltext_node_indexes(
    storage: &StorageEngine,
    index_name: &str,
) -> Result<Vec<copperdb_indexing::CatalogIndexDefinition>, EvalError> {
    let catalog = IndexCatalog::new(storage);
    if let Some(index) = catalog.get(index_name)? {
        if index.kind != copperdb_indexing::CatalogIndexKind::FullText {
            return Err(EvalError::ExecutionError(format!(
                "index {index_name} is not a fulltext index"
            )));
        }
        if index.entity_type != copperdb_indexing::CatalogIndexEntityType::Node {
            return Err(EvalError::ExecutionError(format!(
                "db.index.fulltext.queryNodes only supports node indexes: {index_name}"
            )));
        }
        return Ok(vec![index]);
    }

    if index_name.eq_ignore_ascii_case("default") || index_name.eq_ignore_ascii_case("node_search") {
        let indexes = catalog
            .list()?
            .into_iter()
            .filter(|index| {
                index.kind == copperdb_indexing::CatalogIndexKind::FullText
                    && index.entity_type == copperdb_indexing::CatalogIndexEntityType::Node
            })
            .collect::<Vec<_>>();
        if !indexes.is_empty() {
            return Ok(indexes);
        }
    }

    Err(EvalError::ExecutionError(format!(
        "there is no such fulltext schema index: {index_name}"
    )))
}

fn call_arg_fulltext_options(value: Option<&Value>) -> Result<FulltextCallOptions, EvalError> {
    let Some(value) = value else {
        return Ok(FulltextCallOptions {
            skip: 0,
            limit: None,
        });
    };

    let map = value.as_object().ok_or_else(|| {
        EvalError::ExecutionError(
            "CALL argument optionsMap must be a MAP with optional skip/limit keys".to_string(),
        )
    })?;

    let skip = match map.get("skip") {
        Some(value) => call_arg_usize(value, "optionsMap.skip")?,
        None => 0,
    };
    let limit = match map.get("limit") {
        Some(value) => Some(call_arg_usize(value, "optionsMap.limit")?),
        None => None,
    };

    Ok(FulltextCallOptions { skip, limit })
}

fn node_vector_for_property(node: &NodeRecord, property: &str) -> Option<Vec<f32>> {
    if let Some(vector) = node
        .named_embeddings
        .get(property)
        .filter(|vector| !vector.is_empty())
    {
        return Some(vector.clone());
    }

    if let Some(vector) = node
        .properties
        .get(property)
        .and_then(value_to_vector)
        .filter(|vector| !vector.is_empty())
    {
        return Some(vector);
    }

    node.chunk_embeddings
        .iter()
        .find(|vector| !vector.is_empty())
        .cloned()
}

fn value_to_vector(value: &Value) -> Option<Vec<f32>> {
    value.as_array().and_then(|items| {
        items
            .iter()
            .map(|item| item.as_f64().map(|component| component as f32))
            .collect::<Option<Vec<_>>>()
    })
}

fn cosine_similarity(left: &[f32], right: &[f32]) -> Option<f32> {
    if left.len() != right.len() || left.is_empty() {
        return None;
    }

    let mut dot = 0.0f32;
    let mut left_norm = 0.0f32;
    let mut right_norm = 0.0f32;

    for (left_component, right_component) in left.iter().zip(right.iter()) {
        dot += left_component * right_component;
        left_norm += left_component * left_component;
        right_norm += right_component * right_component;
    }

    if left_norm == 0.0 || right_norm == 0.0 {
        return None;
    }

    Some(dot / (left_norm.sqrt() * right_norm.sqrt()))
}
