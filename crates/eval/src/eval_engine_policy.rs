use super::*;

impl EvalEngine {
    pub(crate) fn execute_call_clause(
        &self,
        call: &copperdb_cypher::CallClause,
        params: &HashMap<String, Value>,
    ) -> Result<EvalResult, EvalError> {
        if call.procedure.eq_ignore_ascii_case("nornicdb.knowledgepolicy.resolve") {
            return self.execute_knowledge_policy_resolve_call(call, params);
        }

        Err(EvalError::ExecutionError(format!(
            "CALL {} is not supported yet",
            call.procedure
        )))
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

        Ok(self.inspect_resolved_target(
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
        )?)
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

        Ok(self.inspect_resolved_target(
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
        )?)
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
                "no decay binding or promotion policy matched; final score defaults to 1.0".to_string()
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
                access_count: access_metadata.as_ref().map(|metadata| metadata.access_count),
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

        let (anchor_unix_ms, matched_rule, score) = if let Some((created_at_unix_ms, updated_at_unix_ms, properties)) =
            entity_state
        {
            let anchor_unix_ms = binding_anchor_unix_ms(
                binding,
                created_at_unix_ms,
                updated_at_unix_ms,
                access_metadata
                    .as_ref()
                    .and_then(|metadata| metadata.last_accessed_at_unix_ms),
                properties,
            );
            let matched_rule = matched_promotion_rule(
                binding,
                properties,
                access_metadata.as_ref(),
                params,
            )?;
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
            access_count: access_metadata.as_ref().map(|metadata| metadata.access_count),
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
            .inspect_node_policy_with_access_metadata(&resolver, node, access_metadata, params, false)?
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
            .inspect_edge_policy_with_access_metadata(&resolver, edge, access_metadata, params, false)?
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