#![allow(clippy::too_many_arguments)]

use super::*;

impl CopperDb {
    pub async fn execute_distributed_as(
        &self,
        cypher: &str,
        params: HashMap<String, Value>,
        roles: &[String],
        placement: &PlacementKey,
        consistency: ConsistencyLevel,
        request_region: Option<&str>,
        transport: Arc<dyn ReplicaTransport>,
    ) -> Result<DistributedQueryResult, CopperDbError> {
        self.execute_distributed_with_read_fence_as(
            cypher,
            params,
            roles,
            placement,
            consistency,
            request_region,
            None,
            transport,
        )
        .await
    }

    pub async fn execute_distributed_with_read_fence_as(
        &self,
        cypher: &str,
        params: HashMap<String, Value>,
        roles: &[String],
        placement: &PlacementKey,
        consistency: ConsistencyLevel,
        request_region: Option<&str>,
        read_fence: Option<LogicalTransactionId>,
        transport: Arc<dyn ReplicaTransport>,
    ) -> Result<DistributedQueryResult, CopperDbError> {
        let parsed = Parser::new().parse(cypher)?;
        self.enforce_compliance(&parsed, roles)?;

        if !is_mutating_query(&parsed.query_type) {
            if let Some(shape) = distributed_shortest_path_query_shape(&parsed) {
                let (result, bfs) = self
                    .execute_distributed_shortest_path_query(
                        &shape,
                        &params,
                        placement,
                        consistency,
                        request_region,
                        read_fence,
                        transport,
                    )
                    .await?;
                return Ok(DistributedQueryResult {
                    result,
                    write_outcome: None,
                    read_outcome: Some(DistributedReadOutcome {
                        plan: bfs.plan,
                        responded_by: bfs.responded_by,
                        failed_replicas: bfs.failed_replicas,
                        value: None,
                    }),
                });
            }
            if let Some(shape) = distributed_direct_path_query_shape(&parsed) {
                let (result, read_outcome) = self
                    .execute_distributed_direct_path_query(
                        &shape,
                        &params,
                        placement,
                        consistency,
                        request_region,
                        read_fence,
                        transport,
                    )
                    .await?;
                return Ok(DistributedQueryResult {
                    result,
                    write_outcome: None,
                    read_outcome: Some(read_outcome),
                });
            }
            if let Some(shape) = distributed_leading_path_query_shape(&parsed) {
                let (result, read_outcome) = self
                    .execute_distributed_leading_path_query(
                        &shape,
                        &params,
                        placement,
                        consistency,
                        request_region,
                        read_fence,
                        transport,
                    )
                    .await?;
                return Ok(DistributedQueryResult {
                    result,
                    write_outcome: None,
                    read_outcome: Some(read_outcome),
                });
            }
        }

        let coordinator = self.build_cassandra_coordinator(transport)?;
        let mut write_outcome = None;
        let mut read_outcome = None;
        if is_mutating_query(&parsed.query_type) {
            write_outcome = Some(
                coordinator
                    .write(
                        placement,
                        consistency,
                        Command::CypherMutation {
                            database: self.config.default_database.clone(),
                            query: cypher.to_string(),
                            params: Value::Object(params.clone().into_iter().collect()),
                        },
                        request_region,
                    )
                    .await?,
            );
        } else {
            let plan = self.plan_distributed_read(placement, consistency, request_region)?;
            read_outcome = Some(DistributedReadOutcome {
                plan,
                responded_by: Vec::new(),
                failed_replicas: Vec::new(),
                value: None,
            });
        }

        Ok(DistributedQueryResult {
            result: self.execute_as(cypher, params, roles)?,
            write_outcome,
            read_outcome,
        })
    }

    pub async fn distributed_bfs_path_as(
        &self,
        start_node_id: &str,
        end_node_id: &str,
        rel_type: Option<&str>,
        direction: EdgeDirection,
        placement: &PlacementKey,
        consistency: ConsistencyLevel,
        request_region: Option<&str>,
        transport: Arc<dyn ReplicaTransport>,
    ) -> Result<DistributedBfsResult, CopperDbError> {
        self.distributed_bfs_path_with_read_fence_as(
            start_node_id,
            end_node_id,
            rel_type,
            direction,
            placement,
            consistency,
            request_region,
            None,
            transport,
        )
        .await
    }

    pub async fn distributed_bfs_path_with_read_fence_as(
        &self,
        start_node_id: &str,
        end_node_id: &str,
        rel_type: Option<&str>,
        direction: EdgeDirection,
        placement: &PlacementKey,
        consistency: ConsistencyLevel,
        request_region: Option<&str>,
        read_fence: Option<LogicalTransactionId>,
        transport: Arc<dyn ReplicaTransport>,
    ) -> Result<DistributedBfsResult, CopperDbError> {
        let plan = self.plan_distributed_read(placement, consistency, request_region)?;
        let mut responded_by = BTreeSet::new();
        let mut failed_replicas = BTreeSet::new();
        let mut access_writes = BTreeMap::new();

        let start_exists = self
            .distributed_graph_node_exists(
                &plan,
                transport.as_ref(),
                start_node_id,
                read_fence,
                &mut responded_by,
                &mut failed_replicas,
            )
            .await?;
        let end_exists = self
            .distributed_graph_node_exists(
                &plan,
                transport.as_ref(),
                end_node_id,
                read_fence,
                &mut responded_by,
                &mut failed_replicas,
            )
            .await?;

        if responded_by.len() < plan.required_responses {
            return Err(ReplicationError::NoQuorum {
                required: plan.required_responses,
                received: responded_by.len(),
            }
            .into());
        }

        let path = if start_exists && end_exists {
            let params = HashMap::new();
            self.distributed_bfs_path(
                &plan,
                transport.as_ref(),
                start_node_id,
                end_node_id,
                rel_type,
                &direction,
                &params,
                read_fence,
                &mut access_writes,
                &mut responded_by,
                &mut failed_replicas,
            )
            .await?
        } else {
            None
        };

        if responded_by.len() < plan.required_responses {
            return Err(ReplicationError::NoQuorum {
                required: plan.required_responses,
                received: responded_by.len(),
            }
            .into());
        }

        self.flush_distributed_access_writes(
            placement,
            consistency,
            request_region,
            transport.clone(),
            access_writes,
        )
        .await?;

        Ok(DistributedBfsResult {
            plan,
            responded_by: responded_by.into_iter().collect(),
            failed_replicas: failed_replicas.into_iter().collect(),
            path,
        })
    }

    pub async fn distributed_bfs_query_as(
        &self,
        start_node_id: &str,
        end_node_id: &str,
        rel_type: Option<&str>,
        direction: EdgeDirection,
        placement: &PlacementKey,
        consistency: ConsistencyLevel,
        request_region: Option<&str>,
        transport: Arc<dyn ReplicaTransport>,
    ) -> Result<(QueryResult, DistributedBfsResult), CopperDbError> {
        self.distributed_bfs_query_with_read_fence_as(
            start_node_id,
            end_node_id,
            rel_type,
            direction,
            placement,
            consistency,
            request_region,
            None,
            transport,
        )
        .await
    }

    pub async fn distributed_bfs_query_with_read_fence_as(
        &self,
        start_node_id: &str,
        end_node_id: &str,
        rel_type: Option<&str>,
        direction: EdgeDirection,
        placement: &PlacementKey,
        consistency: ConsistencyLevel,
        request_region: Option<&str>,
        read_fence: Option<LogicalTransactionId>,
        transport: Arc<dyn ReplicaTransport>,
    ) -> Result<(QueryResult, DistributedBfsResult), CopperDbError> {
        let bfs = self
            .distributed_bfs_path_with_read_fence_as(
                start_node_id,
                end_node_id,
                rel_type,
                direction.clone(),
                placement,
                consistency,
                request_region,
                read_fence,
                transport.clone(),
            )
            .await?;

        let path_value = if let Some(path) = &bfs.path {
            let params = HashMap::new();
            let mut access_writes = BTreeMap::new();
            Some(
                self.materialize_distributed_path_value(
                    &bfs.plan,
                    transport.as_ref(),
                    path,
                    &direction,
                    &params,
                    read_fence,
                    &mut access_writes,
                )
                .await?,
            )
        } else {
            None
        };

        Ok((distributed_bfs_query_result(path_value.as_ref()), bfs))
    }

    async fn execute_distributed_shortest_path_query(
        &self,
        shape: &DistributedShortestPathQueryShape,
        params: &HashMap<String, Value>,
        placement: &PlacementKey,
        consistency: ConsistencyLevel,
        request_region: Option<&str>,
        read_fence: Option<LogicalTransactionId>,
        transport: Arc<dyn ReplicaTransport>,
    ) -> Result<(QueryResult, DistributedBfsResult), CopperDbError> {
        let plan = self.plan_distributed_read(placement, consistency, request_region)?;
        let mut responded_by = BTreeSet::new();
        let mut failed_replicas = BTreeSet::new();
        let mut access_writes = BTreeMap::new();

        let start_candidates = self
            .distributed_resolve_node_candidates(
                &plan,
                transport.as_ref(),
                &shape.start_selector,
                params,
                read_fence,
                &mut access_writes,
                &mut responded_by,
                &mut failed_replicas,
            )
            .await?;
        let end_candidates = self
            .distributed_resolve_node_candidates(
                &plan,
                transport.as_ref(),
                &shape.end_selector,
                params,
                read_fence,
                &mut access_writes,
                &mut responded_by,
                &mut failed_replicas,
            )
            .await?;

        if responded_by.len() < plan.required_responses {
            return Err(ReplicationError::NoQuorum {
                required: plan.required_responses,
                received: responded_by.len(),
            }
            .into());
        }

        let start_ids = distributed_node_ids(&start_candidates);
        let end_ids = distributed_node_ids(&end_candidates);
        let mut best_path: Option<DistributedPath> = None;

        for start_node_id in &start_ids {
            for end_node_id in &end_ids {
                let candidate = self
                    .distributed_bfs_path(
                        &plan,
                        transport.as_ref(),
                        start_node_id,
                        end_node_id,
                        shape.rel_type.as_deref(),
                        &shape.direction,
                        params,
                        read_fence,
                        &mut access_writes,
                        &mut responded_by,
                        &mut failed_replicas,
                    )
                    .await?;
                if let Some(candidate) = candidate {
                    let replace = best_path
                        .as_ref()
                        .map(|current| candidate.edge_ids.len() < current.edge_ids.len())
                        .unwrap_or(true);
                    if replace {
                        best_path = Some(candidate);
                    }
                }
            }
        }

        if responded_by.len() < plan.required_responses {
            return Err(ReplicationError::NoQuorum {
                required: plan.required_responses,
                received: responded_by.len(),
            }
            .into());
        }

        let bfs = DistributedBfsResult {
            plan,
            responded_by: responded_by.into_iter().collect(),
            failed_replicas: failed_replicas.into_iter().collect(),
            path: best_path,
        };

        let path_value = if let Some(path) = &bfs.path {
            Some(
                self.materialize_distributed_path_value(
                    &bfs.plan,
                    transport.as_ref(),
                    path,
                    &shape.direction,
                    params,
                    read_fence,
                    &mut access_writes,
                )
                .await?,
            )
        } else {
            None
        };

        self.flush_distributed_access_writes(
            placement,
            consistency,
            request_region,
            transport.clone(),
            access_writes,
        )
        .await?;

        Ok((
            distributed_shortest_path_result(shape, path_value.as_ref())?,
            bfs,
        ))
    }

    async fn execute_distributed_direct_path_query(
        &self,
        shape: &DistributedDirectPathQueryShape,
        params: &HashMap<String, Value>,
        placement: &PlacementKey,
        consistency: ConsistencyLevel,
        request_region: Option<&str>,
        read_fence: Option<LogicalTransactionId>,
        transport: Arc<dyn ReplicaTransport>,
    ) -> Result<(QueryResult, DistributedReadOutcome), CopperDbError> {
        let plan = self.plan_distributed_read(placement, consistency, request_region)?;
        let mut responded_by = BTreeSet::new();
        let mut failed_replicas = BTreeSet::new();
        let mut access_writes = BTreeMap::new();
        let mut path_values = self
            .distributed_direct_path_values(
                &plan,
                transport.as_ref(),
                &shape.pattern,
                params,
                read_fence,
                &mut access_writes,
                &mut responded_by,
                &mut failed_replicas,
            )
            .await?;

        if shape.optional && path_values.is_empty() {
            path_values.push(Value::Null);
        }

        if responded_by.len() < plan.required_responses {
            return Err(ReplicationError::NoQuorum {
                required: plan.required_responses,
                received: responded_by.len(),
            }
            .into());
        }

        self.flush_distributed_access_writes(
            placement,
            consistency,
            request_region,
            transport.clone(),
            access_writes,
        )
        .await?;

        Ok((
            distributed_path_query_result(&shape.return_items, &shape.path_variable, &path_values)?,
            DistributedReadOutcome {
                plan,
                responded_by: responded_by.into_iter().collect(),
                failed_replicas: failed_replicas.into_iter().collect(),
                value: None,
            },
        ))
    }

    async fn execute_distributed_leading_path_query(
        &self,
        shape: &DistributedLeadingPathQueryShape,
        params: &HashMap<String, Value>,
        placement: &PlacementKey,
        consistency: ConsistencyLevel,
        request_region: Option<&str>,
        read_fence: Option<LogicalTransactionId>,
        transport: Arc<dyn ReplicaTransport>,
    ) -> Result<(QueryResult, DistributedReadOutcome), CopperDbError> {
        let plan = self.plan_distributed_read(placement, consistency, request_region)?;
        let mut responded_by = BTreeSet::new();
        let mut failed_replicas = BTreeSet::new();
        let mut access_writes = BTreeMap::new();

        let mut base_rows = vec![HashMap::new()];
        for leading_step in &shape.leading_steps {
            match leading_step {
                DistributedLeadingStep::Match(leading_match) => {
                    let mut next_rows = Vec::new();
                    for base_row in &base_rows {
                        match leading_match {
                            DistributedLeadingMatch::Node { selector, variable } => {
                                let matched_nodes = self
                                    .distributed_resolve_node_candidates_for_row(
                                        &plan,
                                        transport.as_ref(),
                                        selector,
                                        params,
                                        base_row,
                                        read_fence,
                                        &mut access_writes,
                                        &mut responded_by,
                                        &mut failed_replicas,
                                    )
                                    .await?;
                                if responded_by.len() < plan.required_responses {
                                    return Err(ReplicationError::NoQuorum {
                                        required: plan.required_responses,
                                        received: responded_by.len(),
                                    }
                                    .into());
                                }
                                for matched_node in matched_nodes {
                                    let mut row = base_row.clone();
                                    if let Some(variable) = variable {
                                        row.insert(variable.clone(), matched_node);
                                    }
                                    next_rows.push(row);
                                }
                            }
                            DistributedLeadingMatch::Relationship {
                                pattern,
                                start_variable,
                                end_variable,
                                edge_variable,
                            } => {
                                let matched_paths = self
                                    .distributed_direct_path_values_for_row(
                                        &plan,
                                        transport.as_ref(),
                                        pattern,
                                        params,
                                        base_row,
                                        read_fence,
                                        &mut access_writes,
                                        &mut responded_by,
                                        &mut failed_replicas,
                                    )
                                    .await?;
                                for matched_path in matched_paths {
                                    let Some(nodes) = distributed_path_nodes(&matched_path) else {
                                        continue;
                                    };
                                    let Some(relationships) =
                                        distributed_path_relationships(&matched_path)
                                    else {
                                        continue;
                                    };
                                    if nodes.len() < 2 || relationships.is_empty() {
                                        continue;
                                    }

                                    let mut row = base_row.clone();
                                    if let Some(variable) = start_variable {
                                        row.insert(variable.clone(), nodes[0].clone());
                                    }
                                    if let Some(variable) = end_variable {
                                        row.insert(
                                            variable.clone(),
                                            nodes[nodes.len() - 1].clone(),
                                        );
                                    }
                                    if let Some(variable) = edge_variable {
                                        if relationships.len() != 1 {
                                            continue;
                                        }
                                        row.insert(variable.clone(), relationships[0].clone());
                                    }
                                    next_rows.push(row);
                                }
                            }
                        }
                    }
                    base_rows = next_rows;
                }
                DistributedLeadingStep::OptionalMatch(leading_match) => {
                    let mut next_rows = Vec::new();
                    for base_row in &base_rows {
                        match leading_match {
                            DistributedLeadingMatch::Node { selector, variable } => {
                                let matched_nodes = self
                                    .distributed_resolve_node_candidates_for_row(
                                        &plan,
                                        transport.as_ref(),
                                        selector,
                                        params,
                                        base_row,
                                        read_fence,
                                        &mut access_writes,
                                        &mut responded_by,
                                        &mut failed_replicas,
                                    )
                                    .await?;
                                if responded_by.len() < plan.required_responses {
                                    return Err(ReplicationError::NoQuorum {
                                        required: plan.required_responses,
                                        received: responded_by.len(),
                                    }
                                    .into());
                                }
                                if matched_nodes.is_empty() {
                                    let mut row = base_row.clone();
                                    if let Some(variable) = variable {
                                        if !row.contains_key(variable) {
                                            row.insert(variable.clone(), Value::Null);
                                        }
                                    }
                                    next_rows.push(row);
                                    continue;
                                }
                                for matched_node in matched_nodes {
                                    let mut row = base_row.clone();
                                    if let Some(variable) = variable {
                                        row.insert(variable.clone(), matched_node);
                                    }
                                    next_rows.push(row);
                                }
                            }
                            DistributedLeadingMatch::Relationship {
                                pattern,
                                start_variable,
                                end_variable,
                                edge_variable,
                            } => {
                                let matched_paths = self
                                    .distributed_direct_path_values_for_row(
                                        &plan,
                                        transport.as_ref(),
                                        pattern,
                                        params,
                                        base_row,
                                        read_fence,
                                        &mut access_writes,
                                        &mut responded_by,
                                        &mut failed_replicas,
                                    )
                                    .await?;
                                let mut matched_any = false;
                                for matched_path in matched_paths {
                                    let Some(nodes) = distributed_path_nodes(&matched_path) else {
                                        continue;
                                    };
                                    let Some(relationships) =
                                        distributed_path_relationships(&matched_path)
                                    else {
                                        continue;
                                    };
                                    if nodes.len() < 2 || relationships.is_empty() {
                                        continue;
                                    }

                                    let mut row = base_row.clone();
                                    if let Some(variable) = start_variable {
                                        row.insert(variable.clone(), nodes[0].clone());
                                    }
                                    if let Some(variable) = end_variable {
                                        row.insert(
                                            variable.clone(),
                                            nodes[nodes.len() - 1].clone(),
                                        );
                                    }
                                    if let Some(variable) = edge_variable {
                                        if relationships.len() != 1 {
                                            continue;
                                        }
                                        row.insert(variable.clone(), relationships[0].clone());
                                    }
                                    matched_any = true;
                                    next_rows.push(row);
                                }
                                if !matched_any {
                                    let mut row = base_row.clone();
                                    distributed_bind_optional_leading_match_nulls(
                                        &mut row,
                                        leading_match,
                                    );
                                    next_rows.push(row);
                                }
                            }
                        }
                    }
                    base_rows = next_rows;
                }
                DistributedLeadingStep::Where(where_clause) => {
                    let mut filtered_rows = Vec::new();
                    for row in base_rows {
                        let keep = eval_predicate(&where_clause.expression, &row, params)
                            .map_err(|err| CopperDbError::Eval(err.to_string()))?;
                        if keep {
                            filtered_rows.push(row);
                        }
                    }
                    base_rows = filtered_rows;
                }
                DistributedLeadingStep::With(with_clause) => {
                    let mut projected_rows = base_rows
                        .into_iter()
                        .map(|row| distributed_project_row(&row, &with_clause.items, params))
                        .collect::<Result<Vec<_>, CopperDbError>>()?;

                    if let Some(limit) = with_clause.limit {
                        projected_rows.truncate(limit.max(0) as usize);
                    }

                    if let Some(where_clause) = &with_clause.where_clause {
                        let mut filtered_rows = Vec::new();
                        for row in projected_rows {
                            let keep = eval_predicate(&where_clause.expression, &row, params)
                                .map_err(|err| CopperDbError::Eval(err.to_string()))?;
                            if keep {
                                filtered_rows.push(row);
                            }
                        }
                        base_rows = filtered_rows;
                    } else {
                        base_rows = projected_rows;
                    }
                }
            }
            if base_rows.is_empty() {
                break;
            }
        }

        let mut path_values = Vec::new();
        for base_row in base_rows {
            let row_path_values = self
                .distributed_direct_path_values_for_row(
                    &plan,
                    transport.as_ref(),
                    &shape.path_shape.pattern,
                    params,
                    &base_row,
                    read_fence,
                    &mut access_writes,
                    &mut responded_by,
                    &mut failed_replicas,
                )
                .await?;
            if row_path_values.is_empty() {
                if shape.path_shape.optional {
                    path_values.push(Value::Null);
                }
            } else {
                path_values.extend(row_path_values);
            }
        }

        if responded_by.len() < plan.required_responses {
            return Err(ReplicationError::NoQuorum {
                required: plan.required_responses,
                received: responded_by.len(),
            }
            .into());
        }

        self.flush_distributed_access_writes(
            placement,
            consistency,
            request_region,
            transport.clone(),
            access_writes,
        )
        .await?;

        Ok((
            distributed_path_query_result(
                &shape.path_shape.return_items,
                &shape.path_shape.path_variable,
                &path_values,
            )?,
            DistributedReadOutcome {
                plan,
                responded_by: responded_by.into_iter().collect(),
                failed_replicas: failed_replicas.into_iter().collect(),
                value: None,
            },
        ))
    }

    async fn distributed_direct_path_values(
        &self,
        plan: &DistributedReadPlan,
        transport: &dyn ReplicaTransport,
        pattern: &DistributedDirectPathPattern,
        params: &HashMap<String, Value>,
        read_fence: Option<LogicalTransactionId>,
        access_writes: &mut DistributedAccessWrites,
        responded_by: &mut BTreeSet<String>,
        failed_replicas: &mut BTreeSet<String>,
    ) -> Result<Vec<Value>, CopperDbError> {
        let mut path_values = Vec::new();

        match pattern {
            DistributedDirectPathPattern::SingleNode { selector } => {
                let nodes = self
                    .distributed_resolve_node_candidates(
                        plan,
                        transport,
                        selector,
                        params,
                        read_fence,
                        access_writes,
                        responded_by,
                        failed_replicas,
                    )
                    .await?;
                if responded_by.len() < plan.required_responses {
                    return Err(ReplicationError::NoQuorum {
                        required: plan.required_responses,
                        received: responded_by.len(),
                    }
                    .into());
                }
                path_values.extend(nodes.into_iter().map(|node| {
                    Value::Object(
                        [
                            ("nodes".to_string(), Value::Array(vec![node])),
                            ("relationships".to_string(), Value::Array(Vec::new())),
                            ("length".to_string(), Value::from(0)),
                        ]
                        .into_iter()
                        .collect(),
                    )
                }));
            }
            DistributedDirectPathPattern::RelationshipPath {
                start_selector,
                end_selector,
                rel_type,
                direction,
                edge_properties,
                min_hops,
                max_hops,
            } => {
                let start_nodes = self
                    .distributed_resolve_node_candidates(
                        plan,
                        transport,
                        start_selector,
                        params,
                        read_fence,
                        access_writes,
                        responded_by,
                        failed_replicas,
                    )
                    .await?;
                let end_nodes = self
                    .distributed_resolve_node_candidates(
                        plan,
                        transport,
                        end_selector,
                        params,
                        read_fence,
                        access_writes,
                        responded_by,
                        failed_replicas,
                    )
                    .await?;
                if responded_by.len() < plan.required_responses {
                    return Err(ReplicationError::NoQuorum {
                        required: plan.required_responses,
                        received: responded_by.len(),
                    }
                    .into());
                }

                let end_ids = distributed_node_ids(&end_nodes)
                    .into_iter()
                    .collect::<HashSet<_>>();
                for start_node_id in distributed_node_ids(&start_nodes) {
                    for path in self
                        .distributed_relationship_paths(
                            plan,
                            transport,
                            &start_node_id,
                            &end_ids,
                            rel_type.as_deref(),
                            direction,
                            edge_properties,
                            *min_hops,
                            *max_hops,
                            params,
                            read_fence,
                            access_writes,
                            responded_by,
                            failed_replicas,
                        )
                        .await?
                    {
                        path_values.push(
                            self.materialize_distributed_path_value(
                                plan,
                                transport,
                                &path,
                                direction,
                                params,
                                read_fence,
                                access_writes,
                            )
                            .await?,
                        );
                    }
                }

                if responded_by.len() < plan.required_responses {
                    return Err(ReplicationError::NoQuorum {
                        required: plan.required_responses,
                        received: responded_by.len(),
                    }
                    .into());
                }
            }
        }

        Ok(path_values)
    }

    async fn distributed_direct_path_values_for_row(
        &self,
        plan: &DistributedReadPlan,
        transport: &dyn ReplicaTransport,
        pattern: &DistributedDirectPathPattern,
        params: &HashMap<String, Value>,
        base_row: &HashMap<String, Value>,
        read_fence: Option<LogicalTransactionId>,
        access_writes: &mut DistributedAccessWrites,
        responded_by: &mut BTreeSet<String>,
        failed_replicas: &mut BTreeSet<String>,
    ) -> Result<Vec<Value>, CopperDbError> {
        let mut path_values = Vec::new();

        match pattern {
            DistributedDirectPathPattern::SingleNode { selector } => {
                let nodes = self
                    .distributed_resolve_node_candidates_for_row(
                        plan,
                        transport,
                        selector,
                        params,
                        base_row,
                        read_fence,
                        access_writes,
                        responded_by,
                        failed_replicas,
                    )
                    .await?;
                if responded_by.len() < plan.required_responses {
                    return Err(ReplicationError::NoQuorum {
                        required: plan.required_responses,
                        received: responded_by.len(),
                    }
                    .into());
                }
                path_values.extend(nodes.into_iter().map(|node| {
                    Value::Object(
                        [
                            ("nodes".to_string(), Value::Array(vec![node])),
                            ("relationships".to_string(), Value::Array(Vec::new())),
                            ("length".to_string(), Value::from(0)),
                        ]
                        .into_iter()
                        .collect(),
                    )
                }));
            }
            DistributedDirectPathPattern::RelationshipPath {
                start_selector,
                end_selector,
                rel_type,
                direction,
                edge_properties,
                min_hops,
                max_hops,
            } => {
                let start_nodes = self
                    .distributed_resolve_node_candidates_for_row(
                        plan,
                        transport,
                        start_selector,
                        params,
                        base_row,
                        read_fence,
                        access_writes,
                        responded_by,
                        failed_replicas,
                    )
                    .await?;
                let end_nodes = self
                    .distributed_resolve_node_candidates_for_row(
                        plan,
                        transport,
                        end_selector,
                        params,
                        base_row,
                        read_fence,
                        access_writes,
                        responded_by,
                        failed_replicas,
                    )
                    .await?;
                if responded_by.len() < plan.required_responses {
                    return Err(ReplicationError::NoQuorum {
                        required: plan.required_responses,
                        received: responded_by.len(),
                    }
                    .into());
                }

                let end_ids = distributed_node_ids(&end_nodes)
                    .into_iter()
                    .collect::<HashSet<_>>();
                for start_node_id in distributed_node_ids(&start_nodes) {
                    for path in self
                        .distributed_relationship_paths(
                            plan,
                            transport,
                            &start_node_id,
                            &end_ids,
                            rel_type.as_deref(),
                            direction,
                            edge_properties,
                            *min_hops,
                            *max_hops,
                            params,
                            read_fence,
                            access_writes,
                            responded_by,
                            failed_replicas,
                        )
                        .await?
                    {
                        path_values.push(
                            self.materialize_distributed_path_value(
                                plan,
                                transport,
                                &path,
                                direction,
                                params,
                                read_fence,
                                access_writes,
                            )
                            .await?,
                        );
                    }
                }

                if responded_by.len() < plan.required_responses {
                    return Err(ReplicationError::NoQuorum {
                        required: plan.required_responses,
                        received: responded_by.len(),
                    }
                    .into());
                }
            }
        }

        Ok(path_values)
    }

    async fn distributed_resolve_node_candidates_for_row(
        &self,
        plan: &DistributedReadPlan,
        transport: &dyn ReplicaTransport,
        selector: &DistributedNodeSelector,
        params: &HashMap<String, Value>,
        base_row: &HashMap<String, Value>,
        read_fence: Option<LogicalTransactionId>,
        access_writes: &mut DistributedAccessWrites,
        responded_by: &mut BTreeSet<String>,
        failed_replicas: &mut BTreeSet<String>,
    ) -> Result<Vec<Value>, CopperDbError> {
        match selector {
            DistributedNodeSelector::Bound {
                variable,
                labels,
                properties,
            } => Ok(base_row
                .get(variable)
                .filter(|value| distributed_node_matches(value, labels, properties))
                .cloned()
                .into_iter()
                .collect()),
            _ => {
                self.distributed_resolve_node_candidates(
                    plan,
                    transport,
                    selector,
                    params,
                    read_fence,
                    access_writes,
                    responded_by,
                    failed_replicas,
                )
                .await
            }
        }
    }

    async fn distributed_relationship_paths(
        &self,
        plan: &DistributedReadPlan,
        transport: &dyn ReplicaTransport,
        start_node_id: &str,
        end_ids: &HashSet<String>,
        rel_type: Option<&str>,
        direction: &EdgeDirection,
        edge_properties: &BTreeMap<String, Value>,
        min_hops: u32,
        max_hops: u32,
        params: &HashMap<String, Value>,
        read_fence: Option<LogicalTransactionId>,
        access_writes: &mut DistributedAccessWrites,
        responded_by: &mut BTreeSet<String>,
        failed_replicas: &mut BTreeSet<String>,
    ) -> Result<Vec<DistributedPath>, CopperDbError> {
        let mut frontier = VecDeque::from([(
            start_node_id.to_string(),
            0_u32,
            vec![start_node_id.to_string()],
            Vec::<String>::new(),
        )]);
        let mut visited = HashSet::from([(start_node_id.to_string(), 0_u32)]);
        let mut paths = Vec::new();

        while let Some((current_node_id, depth, path_node_ids, path_edge_ids)) =
            frontier.pop_front()
        {
            if depth >= min_hops && end_ids.contains(&current_node_id) {
                paths.push(DistributedPath {
                    node_ids: path_node_ids.clone(),
                    edge_ids: path_edge_ids.clone(),
                });
            }

            if depth >= max_hops {
                continue;
            }

            let mut edges = self
                .distributed_graph_edges_from_node(
                    plan,
                    transport,
                    &current_node_id,
                    rel_type,
                    direction,
                    params,
                    read_fence,
                    access_writes,
                    responded_by,
                    failed_replicas,
                )
                .await?;
            edges.sort_by(|left, right| left.id.cmp(&right.id));

            for edge in edges {
                if !distributed_edge_matches(&edge, edge_properties) {
                    continue;
                }
                let Some(next_node_id) =
                    distributed_related_node_id(&current_node_id, &edge, direction)
                else {
                    continue;
                };
                let next_depth = depth + 1;
                if !visited.insert((next_node_id.clone(), next_depth)) {
                    continue;
                }
                let mut next_node_ids = path_node_ids.clone();
                next_node_ids.push(next_node_id.clone());
                let mut next_edge_ids = path_edge_ids.clone();
                next_edge_ids.push(edge.id.clone());
                frontier.push_back((next_node_id, next_depth, next_node_ids, next_edge_ids));
            }
        }

        Ok(paths)
    }

    async fn distributed_resolve_node_candidates(
        &self,
        plan: &DistributedReadPlan,
        transport: &dyn ReplicaTransport,
        selector: &DistributedNodeSelector,
        params: &HashMap<String, Value>,
        read_fence: Option<LogicalTransactionId>,
        access_writes: &mut DistributedAccessWrites,
        responded_by: &mut BTreeSet<String>,
        failed_replicas: &mut BTreeSet<String>,
    ) -> Result<Vec<Value>, CopperDbError> {
        match selector {
            DistributedNodeSelector::LiteralId(node_id) => Ok(self
                .distributed_graph_node_value(
                    plan,
                    transport,
                    node_id,
                    params,
                    read_fence,
                    access_writes,
                    responded_by,
                    failed_replicas,
                )
                .await?
                .into_iter()
                .collect()),
            DistributedNodeSelector::Pattern { labels, properties } => {
                let primary_label = labels.first().expect("selector labels are non-empty");
                if let Some(Value::String(node_id)) = properties.get("_id") {
                    let node = self
                        .distributed_graph_node_value(
                            plan,
                            transport,
                            node_id,
                            params,
                            read_fence,
                            access_writes,
                            responded_by,
                            failed_replicas,
                        )
                        .await?;
                    return Ok(node
                        .into_iter()
                        .filter(|node| distributed_node_matches(node, labels, properties))
                        .collect());
                }

                let mut candidates = if let Some((property, value)) = properties.iter().next() {
                    self.distributed_graph_nodes_by_property(
                        plan,
                        transport,
                        primary_label,
                        property,
                        value,
                        params,
                        read_fence,
                        access_writes,
                        responded_by,
                        failed_replicas,
                    )
                    .await?
                } else {
                    self.distributed_graph_nodes_by_label(
                        plan,
                        transport,
                        primary_label,
                        params,
                        read_fence,
                        access_writes,
                        responded_by,
                        failed_replicas,
                    )
                    .await?
                };

                if candidates.is_empty() && !properties.is_empty() {
                    candidates = self
                        .distributed_graph_nodes_by_label(
                            plan,
                            transport,
                            primary_label,
                            params,
                            read_fence,
                            access_writes,
                            responded_by,
                            failed_replicas,
                        )
                        .await?;
                }

                Ok(candidates
                    .into_iter()
                    .filter(|node| distributed_node_matches(node, labels, properties))
                    .collect())
            }
            DistributedNodeSelector::Bound { .. } => Ok(Vec::new()),
        }
    }

    async fn materialize_distributed_path_value(
        &self,
        plan: &DistributedReadPlan,
        transport: &dyn ReplicaTransport,
        path: &DistributedPath,
        direction: &EdgeDirection,
        params: &HashMap<String, Value>,
        read_fence: Option<LogicalTransactionId>,
        access_writes: &mut DistributedAccessWrites,
    ) -> Result<Value, CopperDbError> {
        let mut responded_by = BTreeSet::new();
        let mut failed_replicas = BTreeSet::new();

        let mut node_values = Vec::with_capacity(path.node_ids.len());
        for node_id in &path.node_ids {
            node_values.push(
                self.distributed_graph_node_value(
                    plan,
                    transport,
                    node_id,
                    params,
                    read_fence,
                    access_writes,
                    &mut responded_by,
                    &mut failed_replicas,
                )
                .await?
                .unwrap_or(Value::Null),
            );
        }

        let mut edge_values = Vec::with_capacity(path.edge_ids.len());
        for (index, edge_id) in path.edge_ids.iter().enumerate() {
            let Some(node_id) = path.node_ids.get(index) else {
                break;
            };
            let edge = self
                .distributed_graph_edge_value(
                    plan,
                    transport,
                    node_id,
                    edge_id,
                    direction,
                    params,
                    read_fence,
                    access_writes,
                    &mut responded_by,
                    &mut failed_replicas,
                )
                .await?
                .unwrap_or(Value::Null);
            edge_values.push(edge);
        }

        Ok(Value::Object(
            [
                ("nodes".to_string(), Value::Array(node_values)),
                (
                    "relationships".to_string(),
                    Value::Array(edge_values.clone()),
                ),
                ("length".to_string(), Value::from(edge_values.len() as i64)),
            ]
            .into_iter()
            .collect(),
        ))
    }

    async fn distributed_graph_node_exists(
        &self,
        plan: &DistributedReadPlan,
        transport: &dyn ReplicaTransport,
        node_id: &str,
        read_fence: Option<LogicalTransactionId>,
        responded_by: &mut BTreeSet<String>,
        failed_replicas: &mut BTreeSet<String>,
    ) -> Result<bool, CopperDbError> {
        for replica in &plan.replicas {
            match transport
                .graph_node(&replica.node_id, node_id, read_fence)
                .await
            {
                Ok(Some(_)) => {
                    responded_by.insert(replica.node_id.clone());
                    return Ok(true);
                }
                Ok(None) => {
                    responded_by.insert(replica.node_id.clone());
                }
                Err(_) => {
                    failed_replicas.insert(replica.node_id.clone());
                }
            }
        }
        Ok(false)
    }

    async fn distributed_graph_node_value(
        &self,
        plan: &DistributedReadPlan,
        transport: &dyn ReplicaTransport,
        node_id: &str,
        params: &HashMap<String, Value>,
        read_fence: Option<LogicalTransactionId>,
        access_writes: &mut DistributedAccessWrites,
        responded_by: &mut BTreeSet<String>,
        failed_replicas: &mut BTreeSet<String>,
    ) -> Result<Option<Value>, CopperDbError> {
        for replica in &plan.replicas {
            match transport
                .graph_node(&replica.node_id, node_id, read_fence)
                .await
            {
                Ok(Some(bytes)) => {
                    responded_by.insert(replica.node_id.clone());
                    let props: BTreeMap<String, Value> = rmp_serde::from_slice(&bytes)
                        .map_err(|error| CopperDbError::Storage(error.to_string()))?;
                    let value = Value::Object(props.into_iter().collect());
                    if let Some(node) = distributed_node_record(&value) {
                        let access_metadata = self
                            .distributed_graph_access_metadata(
                                plan,
                                transport,
                                &node.id,
                                read_fence,
                                responded_by,
                                failed_replicas,
                            )
                            .await?;
                        if !self.eval.node_visible_with_access_metadata(
                            &node,
                            access_metadata.clone(),
                            params,
                        )? {
                            return Ok(None);
                        }
                        self.record_distributed_node_access(&node, access_metadata, access_writes)?;
                    }
                    return Ok(Some(value));
                }
                Ok(None) => {
                    responded_by.insert(replica.node_id.clone());
                }
                Err(_) => {
                    failed_replicas.insert(replica.node_id.clone());
                }
            }
        }
        Ok(None)
    }

    async fn distributed_graph_nodes_by_label(
        &self,
        plan: &DistributedReadPlan,
        transport: &dyn ReplicaTransport,
        label: &str,
        params: &HashMap<String, Value>,
        read_fence: Option<LogicalTransactionId>,
        access_writes: &mut DistributedAccessWrites,
        responded_by: &mut BTreeSet<String>,
        failed_replicas: &mut BTreeSet<String>,
    ) -> Result<Vec<Value>, CopperDbError> {
        let mut nodes = BTreeMap::new();
        for replica in &plan.replicas {
            match transport
                .graph_nodes_by_label(&replica.node_id, label, read_fence)
                .await
            {
                Ok(raw_nodes) => {
                    responded_by.insert(replica.node_id.clone());
                    for raw in raw_nodes {
                        let props: BTreeMap<String, Value> = rmp_serde::from_slice(&raw)
                            .map_err(|error| CopperDbError::Storage(error.to_string()))?;
                        let value = Value::Object(props.into_iter().collect());
                        let Some(node) = distributed_node_record(&value) else {
                            continue;
                        };
                        let access_metadata = self
                            .distributed_graph_access_metadata(
                                plan,
                                transport,
                                &node.id,
                                read_fence,
                                responded_by,
                                failed_replicas,
                            )
                            .await?;
                        if !self.eval.node_visible_with_access_metadata(
                            &node,
                            access_metadata.clone(),
                            params,
                        )? {
                            continue;
                        }
                        self.record_distributed_node_access(&node, access_metadata, access_writes)?;
                        if let Some(node_id) = distributed_node_id(&value) {
                            nodes.insert(node_id, value);
                        }
                    }
                }
                Err(_) => {
                    failed_replicas.insert(replica.node_id.clone());
                }
            }
        }
        Ok(nodes.into_values().collect())
    }

    async fn distributed_graph_nodes_by_property(
        &self,
        plan: &DistributedReadPlan,
        transport: &dyn ReplicaTransport,
        label: &str,
        property: &str,
        value: &Value,
        params: &HashMap<String, Value>,
        read_fence: Option<LogicalTransactionId>,
        access_writes: &mut DistributedAccessWrites,
        responded_by: &mut BTreeSet<String>,
        failed_replicas: &mut BTreeSet<String>,
    ) -> Result<Vec<Value>, CopperDbError> {
        let mut nodes = BTreeMap::new();
        for replica in &plan.replicas {
            match transport
                .graph_nodes_by_property(&replica.node_id, label, property, value, read_fence)
                .await
            {
                Ok(raw_nodes) => {
                    responded_by.insert(replica.node_id.clone());
                    for raw in raw_nodes {
                        let props: BTreeMap<String, Value> = rmp_serde::from_slice(&raw)
                            .map_err(|error| CopperDbError::Storage(error.to_string()))?;
                        let value = Value::Object(props.into_iter().collect());
                        let Some(node) = distributed_node_record(&value) else {
                            continue;
                        };
                        let access_metadata = self
                            .distributed_graph_access_metadata(
                                plan,
                                transport,
                                &node.id,
                                read_fence,
                                responded_by,
                                failed_replicas,
                            )
                            .await?;
                        if !self.eval.node_visible_with_access_metadata(
                            &node,
                            access_metadata.clone(),
                            params,
                        )? {
                            continue;
                        }
                        self.record_distributed_node_access(&node, access_metadata, access_writes)?;
                        if let Some(node_id) = distributed_node_id(&value) {
                            nodes.insert(node_id, value);
                        }
                    }
                }
                Err(_) => {
                    failed_replicas.insert(replica.node_id.clone());
                }
            }
        }
        Ok(nodes.into_values().collect())
    }

    async fn distributed_graph_edge_value(
        &self,
        plan: &DistributedReadPlan,
        transport: &dyn ReplicaTransport,
        node_id: &str,
        edge_id: &str,
        direction: &EdgeDirection,
        params: &HashMap<String, Value>,
        read_fence: Option<LogicalTransactionId>,
        access_writes: &mut DistributedAccessWrites,
        responded_by: &mut BTreeSet<String>,
        failed_replicas: &mut BTreeSet<String>,
    ) -> Result<Option<Value>, CopperDbError> {
        let edges = self
            .distributed_graph_edges_from_node(
                plan,
                transport,
                node_id,
                None,
                &EdgeDirection::Both,
                params,
                read_fence,
                access_writes,
                responded_by,
                failed_replicas,
            )
            .await?;
        let edge = edges.into_iter().find(|edge| {
            edge.id == edge_id
                && match direction {
                    EdgeDirection::Outgoing | EdgeDirection::Both => true,
                    EdgeDirection::Incoming => true,
                }
        });
        Ok(edge.map(|edge| distributed_edge_to_value(&edge)))
    }

    async fn distributed_graph_edges_from_node(
        &self,
        plan: &DistributedReadPlan,
        transport: &dyn ReplicaTransport,
        node_id: &str,
        rel_type: Option<&str>,
        direction: &EdgeDirection,
        params: &HashMap<String, Value>,
        read_fence: Option<LogicalTransactionId>,
        access_writes: &mut DistributedAccessWrites,
        responded_by: &mut BTreeSet<String>,
        failed_replicas: &mut BTreeSet<String>,
    ) -> Result<Vec<copperdb_storage::EdgeRecord>, CopperDbError> {
        let mut edges = BTreeMap::new();
        for replica in &plan.replicas {
            match direction {
                EdgeDirection::Outgoing => match transport
                    .graph_edges_from_node(&replica.node_id, node_id, rel_type, read_fence)
                    .await
                {
                    Ok(replica_edges) => {
                        responded_by.insert(replica.node_id.clone());
                        for edge in replica_edges {
                            edges.insert(edge.id.clone(), edge);
                        }
                    }
                    Err(_) => {
                        failed_replicas.insert(replica.node_id.clone());
                    }
                },
                EdgeDirection::Incoming => match transport
                    .graph_edges_to_node(&replica.node_id, node_id, rel_type, read_fence)
                    .await
                {
                    Ok(replica_edges) => {
                        responded_by.insert(replica.node_id.clone());
                        for edge in replica_edges {
                            edges.insert(edge.id.clone(), edge);
                        }
                    }
                    Err(_) => {
                        failed_replicas.insert(replica.node_id.clone());
                    }
                },
                EdgeDirection::Both => {
                    let outgoing = transport
                        .graph_edges_from_node(&replica.node_id, node_id, rel_type, read_fence)
                        .await;
                    let incoming = transport
                        .graph_edges_to_node(&replica.node_id, node_id, rel_type, read_fence)
                        .await;
                    match (outgoing, incoming) {
                        (Ok(mut outgoing), Ok(incoming)) => {
                            responded_by.insert(replica.node_id.clone());
                            outgoing.extend(incoming);
                            for edge in outgoing {
                                edges.insert(edge.id.clone(), edge);
                            }
                        }
                        (Ok(replica_edges), Err(_)) | (Err(_), Ok(replica_edges)) => {
                            responded_by.insert(replica.node_id.clone());
                            failed_replicas.insert(replica.node_id.clone());
                            for edge in replica_edges {
                                edges.insert(edge.id.clone(), edge);
                            }
                        }
                        (Err(_), Err(_)) => {
                            failed_replicas.insert(replica.node_id.clone());
                        }
                    }
                }
            }
        }
        let mut visible_edges = Vec::new();
        for edge in edges.into_values() {
            let access_metadata = self
                .distributed_graph_access_metadata(
                    plan,
                    transport,
                    &edge.id,
                    read_fence,
                    responded_by,
                    failed_replicas,
                )
                .await?;
            if self.eval.edge_visible_with_access_metadata(
                &edge,
                access_metadata.clone(),
                params,
            )? {
                self.record_distributed_edge_access(&edge, access_metadata, access_writes)?;
                visible_edges.push(edge);
            }
        }
        Ok(visible_edges)
    }

    fn record_distributed_node_access(
        &self,
        node: &copperdb_storage::NodeRecord,
        access_metadata: Option<KnowledgePolicyAccessMetadata>,
        access_writes: &mut DistributedAccessWrites,
    ) -> Result<(), CopperDbError> {
        let current = access_writes.get(&node.id).cloned().or(access_metadata);
        if let Some(updated) = self.eval.node_access_metadata_after_read(node, current)? {
            access_writes.insert(node.id.clone(), updated);
        }
        Ok(())
    }

    fn record_distributed_edge_access(
        &self,
        edge: &copperdb_storage::EdgeRecord,
        access_metadata: Option<KnowledgePolicyAccessMetadata>,
        access_writes: &mut DistributedAccessWrites,
    ) -> Result<(), CopperDbError> {
        let current = access_writes.get(&edge.id).cloned().or(access_metadata);
        if let Some(updated) = self.eval.edge_access_metadata_after_read(edge, current)? {
            access_writes.insert(edge.id.clone(), updated);
        }
        Ok(())
    }

    async fn flush_distributed_access_writes(
        &self,
        placement: &PlacementKey,
        consistency: ConsistencyLevel,
        request_region: Option<&str>,
        transport: Arc<dyn ReplicaTransport>,
        access_writes: DistributedAccessWrites,
    ) -> Result<(), CopperDbError> {
        if access_writes.is_empty() {
            return Ok(());
        }

        let coordinator = self.build_cassandra_coordinator(transport)?;
        for (entity_id, metadata) in access_writes {
            coordinator
                .write(
                    placement,
                    consistency,
                    Command::PutKnowledgePolicyAccessMetadata {
                        entity_id,
                        metadata,
                    },
                    request_region,
                )
                .await?;
        }

        Ok(())
    }

    async fn distributed_graph_access_metadata(
        &self,
        plan: &DistributedReadPlan,
        transport: &dyn ReplicaTransport,
        entity_id: &str,
        read_fence: Option<LogicalTransactionId>,
        responded_by: &mut BTreeSet<String>,
        failed_replicas: &mut BTreeSet<String>,
    ) -> Result<Option<copperdb_storage::KnowledgePolicyAccessMetadata>, CopperDbError> {
        for replica in &plan.replicas {
            match transport
                .graph_access_metadata(&replica.node_id, entity_id, read_fence)
                .await
            {
                Ok(metadata) => {
                    responded_by.insert(replica.node_id.clone());
                    if metadata.is_some() {
                        return Ok(metadata);
                    }
                }
                Err(_) => {
                    failed_replicas.insert(replica.node_id.clone());
                }
            }
        }
        Ok(None)
    }

    async fn distributed_bfs_path(
        &self,
        plan: &DistributedReadPlan,
        transport: &dyn ReplicaTransport,
        start_node_id: &str,
        end_node_id: &str,
        rel_type: Option<&str>,
        direction: &EdgeDirection,
        params: &HashMap<String, Value>,
        read_fence: Option<LogicalTransactionId>,
        access_writes: &mut DistributedAccessWrites,
        responded_by: &mut BTreeSet<String>,
        failed_replicas: &mut BTreeSet<String>,
    ) -> Result<Option<DistributedPath>, CopperDbError> {
        if start_node_id == end_node_id {
            return Ok(Some(DistributedPath {
                node_ids: vec![start_node_id.to_string()],
                edge_ids: Vec::new(),
            }));
        }

        let mut frontier = VecDeque::from([start_node_id.to_string()]);
        let mut visited = HashSet::from([start_node_id.to_string()]);
        let mut predecessors: HashMap<String, (String, String)> = HashMap::new();

        while let Some(current_node_id) = frontier.pop_front() {
            let mut edges = self
                .distributed_graph_edges_from_node(
                    plan,
                    transport,
                    &current_node_id,
                    rel_type,
                    direction,
                    params,
                    read_fence,
                    access_writes,
                    responded_by,
                    failed_replicas,
                )
                .await?;
            edges.sort_by(|left, right| left.id.cmp(&right.id));

            for edge in edges {
                let next_node_id = match direction {
                    EdgeDirection::Outgoing => edge.end_node.clone(),
                    EdgeDirection::Incoming => edge.start_node.clone(),
                    EdgeDirection::Both if edge.start_node == current_node_id => {
                        edge.end_node.clone()
                    }
                    EdgeDirection::Both if edge.end_node == current_node_id => {
                        edge.start_node.clone()
                    }
                    EdgeDirection::Both => continue,
                };
                if !visited.insert(next_node_id.clone()) {
                    continue;
                }
                predecessors.insert(
                    next_node_id.clone(),
                    (current_node_id.clone(), edge.id.clone()),
                );
                if next_node_id == end_node_id {
                    let mut node_ids = vec![end_node_id.to_string()];
                    let mut edge_ids = Vec::new();
                    let mut cursor = end_node_id.to_string();
                    while let Some((previous_node_id, edge_id)) = predecessors.get(&cursor) {
                        edge_ids.push(edge_id.clone());
                        node_ids.push(previous_node_id.clone());
                        cursor = previous_node_id.clone();
                    }
                    node_ids.reverse();
                    edge_ids.reverse();
                    return Ok(Some(DistributedPath { node_ids, edge_ids }));
                }
                frontier.push_back(next_node_id);
            }
        }

        Ok(None)
    }
}
