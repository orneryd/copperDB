use super::*;
impl CopperDb {
    /// Create a new in-memory (temporary) database instance.
    pub fn open_temporary() -> Result<Self, CopperDbError> {
        let storage = Arc::new(StorageEngine::open_temporary()?);
        Self::from_storage(storage, DatabaseConfig::default())
    }

    /// Create a persistent database at the given path.
    pub fn open(config: DatabaseConfig) -> Result<Self, CopperDbError> {
        let storage = Arc::new(open_storage(&config)?);
        Self::from_storage(storage, config)
    }

    fn from_storage(
        storage: Arc<StorageEngine>,
        config: DatabaseConfig,
    ) -> Result<Self, CopperDbError> {
        let eval = EvalEngine::new(Arc::clone(&storage));
        let audit_log = Arc::new(AuditLog::new(Arc::clone(&storage), AuditConfig::default())?);
        let compliance = Arc::new(ComplianceManager::new(Arc::clone(&storage)));
        Ok(Self {
            config,
            storage,
            eval,
            tx_manager: Arc::new(TransactionManager::new()),
            query_cache: Arc::new(QueryCache::new(
                1024,
                Some(std::time::Duration::from_secs(300)),
            )),
            audit_log,
            compliance,
        })
    }

    /// Execute a Cypher query string as an embedded admin caller.
    pub fn execute(
        &self,
        cypher: &str,
        params: HashMap<String, Value>,
    ) -> Result<QueryResult, CopperDbError> {
        self.execute_as(cypher, params, &["admin".to_string()])
    }

    /// Execute a Cypher query as a caller with the provided normalized role names.
    pub fn execute_as(
        &self,
        cypher: &str,
        params: HashMap<String, Value>,
        roles: &[String],
    ) -> Result<QueryResult, CopperDbError> {
        let start = Instant::now();

        if self.config.log_queries {
            tracing::info!(query = cypher, "executing query");
        }

        let _flush_guard = self.storage.hold_flush();

        let hash = QueryCache::<copperdb_cypher::Query>::key(cypher, &[]);
        let parsed = if let Some(cached) = self.query_cache.get(hash) {
            cached
        } else {
            let parser = Parser::new();
            let q = match parser.parse(cypher) {
                Ok(q) => q,
                Err(err) => {
                    self.record_query_audit(
                        cypher,
                        "PARSE",
                        false,
                        Some(err.to_string()),
                        None,
                        0,
                    )?;
                    return Err(err.into());
                }
            };
            self.query_cache.put(hash, q.clone());
            q
        };

        if let Err(err) = self.enforce_compliance(&parsed, roles) {
            self.record_query_audit(
                cypher,
                query_action(&parsed.query_type),
                false,
                Some(err.to_string()),
                Some(hash),
                start.elapsed().as_millis() as u64,
            )?;
            return Err(err.into());
        }

        let pattern_info = detect_query_pattern(cypher);
        let (compound_shape, compound_ok) = match_compound_query_shape(cypher);
        let (pipeline_clauses, pipeline_ok) = can_execute_as_pipeline(cypher);
        let eval_result = match self.eval.execute_with_routes(
            &parsed,
            &params,
            &pattern_info,
            compound_ok.then_some(&compound_shape),
            pipeline_ok.then_some(pipeline_clauses.as_slice()),
        ) {
            Ok(result) => result,
            Err(err) => {
                self.record_query_audit(
                    cypher,
                    query_action(&parsed.query_type),
                    false,
                    Some(err.to_string()),
                    Some(hash),
                    start.elapsed().as_millis() as u64,
                )?;
                return Err(err.into());
            }
        };

        let elapsed_ms = start.elapsed().as_millis() as u64;
        let mut stats = ResultStats::from(eval_result.stats);
        stats.execution_time_ms = elapsed_ms;
        self.record_query_audit(
            cypher,
            query_action(&parsed.query_type),
            true,
            None,
            Some(hash),
            elapsed_ms,
        )?;

        Ok(QueryResult {
            columns: eval_result.columns,
            rows: eval_result.rows,
            stats,
        })
    }
}

#[path = "distributed.rs"]
mod distributed;

impl CopperDb {
    /// Flush all pending writes to disk.
    pub fn flush(&self) -> Result<(), CopperDbError> {
        self.storage.flush()?;
        Ok(())
    }

    /// Return the on-disk size in bytes.
    pub fn size_on_disk(&self) -> u64 {
        self.storage.size_on_disk()
    }

    /// Access the transaction manager.
    pub fn tx_manager(&self) -> &Arc<TransactionManager> {
        &self.tx_manager
    }

    /// Access the storage engine directly.
    pub fn storage(&self) -> &Arc<StorageEngine> {
        &self.storage
    }

    /// Access the durable audit log.
    pub fn audit_log(&self) -> &Arc<AuditLog> {
        &self.audit_log
    }

    /// Access the durable compliance policy manager.
    pub fn compliance_manager(&self) -> &Arc<ComplianceManager> {
        &self.compliance
    }

    pub fn load_distributed_topology(&self) -> Result<TopologyRegistry, CopperDbError> {
        self.storage.load_topology_registry().map_err(Into::into)
    }

    pub fn plan_distributed_write(
        &self,
        placement: &PlacementKey,
        consistency: ConsistencyLevel,
        request_region: Option<&str>,
    ) -> Result<DistributedWritePlan, CopperDbError> {
        self.load_distributed_topology()?
            .plan_write_with_consistency(
                placement,
                DistributedWriteMode::DynamoQuorum,
                consistency,
                request_region,
            )
            .map_err(|error| CopperDbError::Replication(error.to_string()))
    }

    pub fn plan_distributed_read(
        &self,
        placement: &PlacementKey,
        consistency: ConsistencyLevel,
        request_region: Option<&str>,
    ) -> Result<DistributedReadPlan, CopperDbError> {
        self.load_distributed_topology()?
            .plan_read(placement, consistency, request_region)
            .map_err(|error| CopperDbError::Replication(error.to_string()))
    }

    pub fn register_fabric_database(&self, database: &FabricDatabase) -> Result<(), CopperDbError> {
        self.storage.register_fabric_database(database)?;
        Ok(())
    }

    pub fn list_fabric_databases(&self) -> Result<Vec<FabricDatabase>, CopperDbError> {
        self.storage.list_fabric_databases().map_err(Into::into)
    }

    pub fn load_fabric_database(
        &self,
        tenant: &str,
        database: &str,
    ) -> Result<Option<FabricDatabase>, CopperDbError> {
        Ok(self
            .list_fabric_databases()?
            .into_iter()
            .find(|fabric| fabric.tenant == tenant && fabric.database == database))
    }

    pub fn plan_fabric_reads(
        &self,
        database: &FabricDatabase,
        consistency: ConsistencyLevel,
        request_region: Option<&str>,
    ) -> Result<Vec<DistributedReadPlan>, CopperDbError> {
        database
            .validate()
            .map_err(|error| CopperDbError::Replication(error.to_string()))?;
        let topology = self.load_distributed_topology()?;
        database
            .placement_keys()
            .iter()
            .map(|placement| {
                topology
                    .plan_read(placement, consistency, request_region)
                    .map_err(|error| CopperDbError::Replication(error.to_string()))
            })
            .collect()
    }

    pub fn plan_fabric_query_reads(
        &self,
        database: &FabricDatabase,
        request: FabricReadRequest,
    ) -> Result<FabricReadPlan, CopperDbError> {
        FabricTopology::new(self.load_distributed_topology()?)
            .plan_fabric_query_reads(database, request)
            .map_err(|error| CopperDbError::Replication(error.to_string()))
    }

    pub fn merge_fabric_rows(
        &self,
        rows: Vec<FabricRowBatch>,
        options: FabricRowMergeOptions,
    ) -> FabricMergedRows {
        merge_fabric_rows(rows, options)
    }

    pub fn merge_fabric_aggregates(
        &self,
        rows: Vec<FabricRowBatch>,
        options: FabricAggregateOptions,
    ) -> FabricMergedRows {
        merge_fabric_aggregates(rows, options)
    }

    pub fn merge_fabric_paths(
        &self,
        paths: Vec<FabricPathBatch>,
        options: FabricPathMergeOptions,
    ) -> FabricMergedPaths {
        merge_fabric_paths(paths, options)
    }

    pub fn merge_fabric_ranked_search(
        &self,
        batches: Vec<RrfSearchBatch>,
        config: RrfConfig,
    ) -> RrfSearchOutcome {
        merge_rrf_search_batches(batches, config)
    }

    pub fn hydrate_fabric_ranked_search(
        &self,
        outcome: RrfSearchOutcome,
        hydration: Vec<RrfHydrationRecord>,
        policy: RrfSearchPolicy,
    ) -> RrfHydratedSearchOutcome {
        hydrate_rrf_search_outcome(outcome, hydration, policy)
    }

    pub fn execute_fabric_ranked_search(
        &self,
        database: &FabricDatabase,
        batches: Vec<RrfSearchBatch>,
        hydration: Vec<RrfHydrationRecord>,
        config: RrfConfig,
        policy: RrfSearchPolicy,
    ) -> Result<FabricRankedSearchExecution, CopperDbError> {
        let plans = self.plan_fabric_searches(database)?;
        Ok(execute_planned_fabric_ranked_search(
            plans, batches, hydration, config, policy,
        ))
    }

    pub async fn execute_fabric_ranked_search_with_transport(
        &self,
        database: &FabricDatabase,
        query: SearchQuery,
        hydration: Vec<RrfHydrationRecord>,
        config: RrfConfig,
        policy: RrfSearchPolicy,
        transport: Arc<dyn RankedSearchTransport>,
    ) -> Result<FabricRankedSearchExecution, CopperDbError> {
        let plans = self.plan_fabric_searches(database)?;
        let collected = collect_planned_fabric_ranked_batches(plans.clone(), query, transport)
            .await
            .map_err(|error| CopperDbError::Replication(error.to_string()))?;
        let mut execution = execute_planned_fabric_ranked_search(
            plans,
            collected.batches,
            hydration,
            config,
            policy,
        );
        execution.responded_nodes = collected.responded_nodes;
        execution.failed_nodes = collected.failed_nodes;
        Ok(execution)
    }

    pub async fn fetch_fabric_ranked_hydration_with_transport(
        &self,
        outcome: &RrfSearchOutcome,
        consistency: ConsistencyLevel,
        transport: Arc<dyn HydrationTransport>,
    ) -> Result<Vec<RrfHydrationRecord>, CopperDbError> {
        let mut by_placement: BTreeMap<PlacementKey, Vec<_>> = BTreeMap::new();
        for hit in &outcome.results {
            by_placement
                .entry(hit.global_id.placement.clone())
                .or_default()
                .push(hit.global_id.clone());
        }

        let mut requests = Vec::new();
        for (placement, global_ids) in by_placement {
            let plan = self.plan_distributed_read(&placement, consistency, None)?;
            requests.push(FabricHydrationRequest {
                node_id: plan.coordinator.node_id,
                placement,
                global_ids,
            });
        }

        let collected = collect_fabric_hydration_records(requests, transport)
            .await
            .map_err(|error| CopperDbError::Replication(error.to_string()))?;
        Ok(collected.records)
    }

    pub async fn execute_fabric_ranked_search_with_full_transport(
        &self,
        database: &FabricDatabase,
        query: SearchQuery,
        hydration_consistency: ConsistencyLevel,
        config: RrfConfig,
        policy: RrfSearchPolicy,
        ranked_transport: Arc<dyn RankedSearchTransport>,
        hydration_transport: Arc<dyn HydrationTransport>,
    ) -> Result<FabricRankedSearchExecution, CopperDbError> {
        let plans = self.plan_fabric_searches(database)?;
        let collected =
            collect_planned_fabric_ranked_batches(plans.clone(), query, ranked_transport)
                .await
                .map_err(|error| CopperDbError::Replication(error.to_string()))?;
        let merged = merge_rrf_search_batches(collected.batches.clone(), config);
        let hydration = self
            .fetch_fabric_ranked_hydration_with_transport(
                &merged,
                hydration_consistency,
                hydration_transport,
            )
            .await?;
        let mut execution = execute_planned_fabric_ranked_search(
            plans,
            collected.batches,
            hydration,
            config,
            policy,
        );
        execution.responded_nodes = collected.responded_nodes;
        execution.failed_nodes = collected.failed_nodes;
        Ok(execution)
    }

    pub fn plan_fabric_searches(
        &self,
        database: &FabricDatabase,
    ) -> Result<Vec<DistributedSearchPlan>, CopperDbError> {
        database
            .validate()
            .map_err(|error| CopperDbError::Replication(error.to_string()))?;
        let topology = self.load_distributed_topology()?;
        database
            .placement_keys()
            .iter()
            .map(|placement| {
                topology
                    .plan_search(placement)
                    .map_err(|error| CopperDbError::Replication(error.to_string()))
            })
            .collect()
    }

    pub fn open_repair_queue(&self) -> Result<Arc<DurableRepairQueue>, CopperDbError> {
        Ok(Arc::new(DurableRepairQueue::open(
            self.repair_queue_path(),
        )?))
    }

    pub fn build_cassandra_coordinator(
        &self,
        transport: Arc<dyn ReplicaTransport>,
    ) -> Result<CassandraCoordinator, CopperDbError> {
        Ok(CassandraCoordinator::with_repair_queue(
            self.load_distributed_topology()?,
            transport,
            self.open_repair_queue()?,
        ))
    }

    pub async fn replay_repairs(
        &self,
        transport: Arc<dyn ReplicaTransport>,
        max_records: usize,
    ) -> Result<RepairReplayReport, CopperDbError> {
        self.open_repair_queue()?
            .replay_batch(transport, max_records)
            .await
            .map_err(Into::into)
    }

    pub fn build_repair_worker(
        &self,
        transport: Arc<dyn ReplicaTransport>,
        config: RepairWorkerConfig,
    ) -> Result<ScheduledRepairWorker, CopperDbError> {
        Ok(ScheduledRepairWorker::new(
            self.open_repair_queue()?,
            transport,
            config,
        ))
    }

    /// Build a compliance reporter over the durable audit trail.
    pub fn compliance_reporter(&self) -> ComplianceReporter {
        ComplianceReporter::new(Arc::clone(&self.audit_log))
    }

    fn repair_queue_path(&self) -> PathBuf {
        self.config
            .distributed_repair_queue_dir
            .as_ref()
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from(&self.config.data_dir).join("replication-repair"))
    }

    fn enforce_compliance(
        &self,
        query: &copperdb_cypher::Query,
        roles: &[String],
    ) -> Result<(), copperdb_compliance::ComplianceError> {
        let mut labels = Vec::new();
        let mut properties = Vec::new();
        collect_compliance_terms(query, &mut labels, &mut properties);
        labels.sort();
        labels.dedup();
        properties.sort();
        properties.dedup();

        for label in labels {
            self.compliance.check_label_access(&label, roles)?;
        }
        for property in properties {
            self.compliance.check_property_access(&property, roles)?;
        }
        Ok(())
    }

    fn record_query_audit(
        &self,
        cypher: &str,
        action: &str,
        success: bool,
        reason: Option<String>,
        query_hash: Option<u64>,
        elapsed_ms: u64,
    ) -> Result<(), CopperDbError> {
        let mut event = Event {
            event_type: audit_event_type(action),
            user_id: Some("embedded".into()),
            username: Some("embedded".into()),
            resource: Some("cypher_query".into()),
            resource_id: query_hash.map(|hash| format!("{hash:016x}")),
            action: Some(action.into()),
            success,
            reason,
            data_classification: Some("DATABASE".into()),
            ..Event::new(EventType::DataRead)
        };
        event
            .metadata
            .insert("database".into(), self.config.default_database.clone());
        event
            .metadata
            .insert("query_length".into(), cypher.len().to_string());
        event
            .metadata
            .insert("elapsed_ms".into(), elapsed_ms.to_string());
        self.audit_log.record(event)?;
        Ok(())
    }
}

fn open_storage(config: &DatabaseConfig) -> Result<StorageEngine, CopperDbError> {
    match &config.storage_encryption_master_key {
        Some(master_key) => {
            let provider = new_provider(ProviderFactoryConfig {
                provider: "local".into(),
                key_uri: config.storage_encryption_key_uri.clone(),
                master_key: master_key.clone(),
                audit_signing_key: None,
            })
            .map_err(|err| CopperDbError::Init(err.to_string()))?;
            StorageEngine::open_encrypted(
                &config.data_dir,
                provider,
                config.storage_encryption_key_uri.clone(),
            )
            .map_err(|e| CopperDbError::Storage(e.to_string()))
        }
        None => {
            StorageEngine::open(&config.data_dir).map_err(|e| CopperDbError::Storage(e.to_string()))
        }
    }
}

fn query_action(query_type: &QueryType) -> &'static str {
    match query_type {
        QueryType::Match | QueryType::Return | QueryType::With => "READ",
        QueryType::Create => "CREATE",
        QueryType::Merge | QueryType::Set | QueryType::Remove | QueryType::Ddl => "UPDATE",
        QueryType::Delete => "DELETE",
    }
}

fn is_mutating_query(query_type: &QueryType) -> bool {
    matches!(
        query_type,
        QueryType::Create
            | QueryType::Merge
            | QueryType::Set
            | QueryType::Remove
            | QueryType::Delete
            | QueryType::Ddl
    )
}

fn audit_event_type(action: &str) -> EventType {
    match action {
        "CREATE" => EventType::DataCreate,
        "UPDATE" => EventType::DataUpdate,
        "DELETE" => EventType::DataDelete,
        "EXPORT" => EventType::DataExport,
        _ => EventType::DataRead,
    }
}

#[derive(Debug, Clone)]
pub(crate) struct DistributedShortestPathQueryShape {
    path_variable: String,
    start_selector: DistributedNodeSelector,
    end_selector: DistributedNodeSelector,
    rel_type: Option<String>,
    direction: EdgeDirection,
    return_items: Vec<ReturnItem>,
}

#[derive(Debug, Clone)]
struct DistributedDirectPathQueryShape {
    optional: bool,
    path_variable: String,
    pattern: DistributedDirectPathPattern,
    return_items: Vec<ReturnItem>,
}

#[derive(Debug, Clone)]
struct DistributedLeadingPathQueryShape {
    leading_steps: Vec<DistributedLeadingStep>,
    path_shape: DistributedDirectPathQueryShape,
}

#[derive(Debug, Clone)]
enum DistributedLeadingStep {
    Match(DistributedLeadingMatch),
    OptionalMatch(DistributedLeadingMatch),
    With(WithClause),
    Where(WhereClause),
}

#[derive(Debug, Clone)]
enum DistributedLeadingMatch {
    Node {
        selector: DistributedNodeSelector,
        variable: Option<String>,
    },
    Relationship {
        pattern: DistributedDirectPathPattern,
        start_variable: Option<String>,
        end_variable: Option<String>,
        edge_variable: Option<String>,
    },
}

#[derive(Debug, Clone)]
enum DistributedDirectPathPattern {
    SingleNode {
        selector: DistributedNodeSelector,
    },
    RelationshipPath {
        start_selector: DistributedNodeSelector,
        end_selector: DistributedNodeSelector,
        rel_type: Option<String>,
        direction: EdgeDirection,
        edge_properties: BTreeMap<String, Value>,
        min_hops: u32,
        max_hops: u32,
    },
}

#[derive(Debug, Clone)]
enum DistributedNodeSelector {
    LiteralId(String),
    Pattern {
        labels: Vec<String>,
        properties: BTreeMap<String, Value>,
    },
    Bound {
        variable: String,
        labels: Vec<String>,
        properties: BTreeMap<String, Value>,
    },
}

pub(crate) fn distributed_shortest_path_query_shape(
    query: &copperdb_cypher::Query,
) -> Option<DistributedShortestPathQueryShape> {
    if query.clauses.len() != 2 {
        return None;
    }
    let Clause::Match(match_clause) = &query.clauses[0] else {
        return None;
    };
    let Clause::Return(return_clause) = &query.clauses[1] else {
        return None;
    };
    if !match_clause.pattern.shortest_path
        || match_clause.pattern.nodes.len() != 2
        || match_clause.pattern.edges.len() != 1
        || return_clause.distinct
        || return_clause.skip.is_some()
        || return_clause.limit.is_some()
        || !return_clause.order_by.is_empty()
    {
        return None;
    }

    let path_variable = match_clause.pattern.path_variable.clone()?;
    let start_selector = distributed_node_selector(&match_clause.pattern.nodes[0], &[])?;
    let end_selector = distributed_node_selector(&match_clause.pattern.nodes[1], &[])?;
    let edge = &match_clause.pattern.edges[0];

    if !return_clause
        .items
        .iter()
        .all(|item| supported_distributed_path_return_expression(&item.expression, &path_variable))
    {
        return None;
    }

    Some(DistributedShortestPathQueryShape {
        path_variable,
        start_selector,
        end_selector,
        rel_type: edge.rel_type.clone(),
        direction: edge.direction.clone(),
        return_items: return_clause.items.clone(),
    })
}

fn distributed_direct_path_query_shape(
    query: &copperdb_cypher::Query,
) -> Option<DistributedDirectPathQueryShape> {
    distributed_direct_path_query_shape_with_bound_nodes(query, &[])
}

fn distributed_direct_path_query_shape_with_bound_nodes(
    query: &copperdb_cypher::Query,
    bound_variables: &[String],
) -> Option<DistributedDirectPathQueryShape> {
    if query.clauses.len() != 2 {
        return None;
    }
    let (match_clause, optional) = match &query.clauses[0] {
        Clause::Match(match_clause) => (match_clause, false),
        Clause::OptionalMatch(match_clause) => (match_clause, true),
        _ => return None,
    };
    let Clause::Return(return_clause) = &query.clauses[1] else {
        return None;
    };
    if match_clause.pattern.shortest_path
        || return_clause.distinct
        || return_clause.skip.is_some()
        || return_clause.limit.is_some()
        || !return_clause.order_by.is_empty()
    {
        return None;
    }

    let path_variable = match_clause.pattern.path_variable.clone()?;
    if !return_clause
        .items
        .iter()
        .all(|item| supported_distributed_path_return_expression(&item.expression, &path_variable))
    {
        return None;
    }

    let pattern = match (
        &match_clause.pattern.nodes[..],
        &match_clause.pattern.edges[..],
    ) {
        ([node], []) => DistributedDirectPathPattern::SingleNode {
            selector: distributed_node_selector(node, bound_variables)?,
        },
        ([start, end], [edge]) => {
            let min_hops = edge.min_hops.unwrap_or(1);
            let max_hops = edge.max_hops.unwrap_or(1).max(min_hops);
            DistributedDirectPathPattern::RelationshipPath {
                start_selector: distributed_node_selector(start, bound_variables)?,
                end_selector: distributed_node_selector(end, bound_variables)?,
                rel_type: edge.rel_type.clone(),
                direction: edge.direction.clone(),
                edge_properties: distributed_literal_properties(&edge.properties)?,
                min_hops,
                max_hops,
            }
        }
        _ => return None,
    };

    Some(DistributedDirectPathQueryShape {
        optional,
        path_variable,
        pattern,
        return_items: return_clause.items.clone(),
    })
}

fn distributed_leading_path_query_shape(
    query: &copperdb_cypher::Query,
) -> Option<DistributedLeadingPathQueryShape> {
    if query.clauses.len() < 3 {
        return None;
    }
    let Clause::Return(return_clause) = query.clauses.last()? else {
        return None;
    };
    let path_clause = match &query.clauses[query.clauses.len() - 2] {
        Clause::Match(match_clause) => Clause::Match(match_clause.clone()),
        Clause::OptionalMatch(match_clause) => Clause::OptionalMatch(match_clause.clone()),
        _ => return None,
    };
    if return_clause.distinct
        || return_clause.skip.is_some()
        || return_clause.limit.is_some()
        || !return_clause.order_by.is_empty()
    {
        return None;
    }

    let mut bound_variables = Vec::new();
    let mut leading_steps = Vec::new();
    for clause in &query.clauses[..query.clauses.len() - 2] {
        match clause {
            Clause::OptionalMatch(leading_match_clause) => {
                let leading_match =
                    distributed_leading_match(&leading_match_clause.pattern, &bound_variables)?;
                leading_steps.push(DistributedLeadingStep::OptionalMatch(leading_match));
                distributed_extend_bound_variables(
                    &mut bound_variables,
                    &leading_match_clause.pattern,
                );
            }
            Clause::With(with_clause) => {
                if !distributed_supported_leading_with_clause(with_clause) {
                    return None;
                }
                leading_steps.push(DistributedLeadingStep::With(with_clause.clone()));
                bound_variables = with_clause
                    .items
                    .iter()
                    .map(distributed_leading_with_column_name)
                    .collect();
            }
            Clause::Where(where_clause) => {
                if leading_steps.is_empty() {
                    return None;
                }
                leading_steps.push(DistributedLeadingStep::Where(where_clause.clone()));
            }
            Clause::Match(leading_match_clause) => {
                let leading_match =
                    distributed_leading_match(&leading_match_clause.pattern, &bound_variables)?;
                leading_steps.push(DistributedLeadingStep::Match(leading_match));
                distributed_extend_bound_variables(
                    &mut bound_variables,
                    &leading_match_clause.pattern,
                );
            }
            _ => return None,
        }
    }

    let path_query = copperdb_cypher::Query {
        query_type: QueryType::Match,
        clauses: vec![path_clause, Clause::Return(return_clause.clone())],
        parameters: HashMap::new(),
    };

    Some(DistributedLeadingPathQueryShape {
        leading_steps,
        path_shape: distributed_direct_path_query_shape_with_bound_nodes(
            &path_query,
            &bound_variables,
        )?,
    })
}

fn distributed_literal_properties(
    properties: &[copperdb_cypher::PropertyEntry],
) -> Option<BTreeMap<String, Value>> {
    properties
        .iter()
        .map(|entry| match &entry.value {
            Expression::Literal(value) => Some((
                entry.key.clone(),
                match value {
                    copperdb_cypher::LiteralValue::String(value) => Value::String(value.clone()),
                    copperdb_cypher::LiteralValue::Integer(value) => Value::from(*value),
                    copperdb_cypher::LiteralValue::Float(value) => Value::from(*value),
                    copperdb_cypher::LiteralValue::Bool(value) => Value::Bool(*value),
                    copperdb_cypher::LiteralValue::Null => Value::Null,
                },
            )),
            _ => None,
        })
        .collect::<Option<BTreeMap<_, _>>>()
}

fn distributed_leading_match(
    pattern: &Pattern,
    bound_variables: &[String],
) -> Option<DistributedLeadingMatch> {
    if pattern.shortest_path || pattern.path_variable.is_some() {
        return None;
    }

    match (&pattern.nodes[..], &pattern.edges[..]) {
        ([node], []) => Some(DistributedLeadingMatch::Node {
            selector: distributed_node_selector(node, bound_variables)?,
            variable: node.variable.clone(),
        }),
        ([start, end], [edge]) => {
            let min_hops = edge.min_hops.unwrap_or(1);
            let max_hops = edge.max_hops.unwrap_or(1).max(min_hops);
            if max_hops > 1 && edge.variable.is_some() {
                return None;
            }
            Some(DistributedLeadingMatch::Relationship {
                pattern: DistributedDirectPathPattern::RelationshipPath {
                    start_selector: distributed_node_selector(start, bound_variables)?,
                    end_selector: distributed_node_selector(end, bound_variables)?,
                    rel_type: edge.rel_type.clone(),
                    direction: edge.direction.clone(),
                    edge_properties: distributed_literal_properties(&edge.properties)?,
                    min_hops,
                    max_hops,
                },
                start_variable: start.variable.clone(),
                end_variable: end.variable.clone(),
                edge_variable: edge.variable.clone(),
            })
        }
        _ => None,
    }
}

fn distributed_extend_bound_variables(bound_variables: &mut Vec<String>, pattern: &Pattern) {
    for node in &pattern.nodes {
        if let Some(variable) = &node.variable {
            if !bound_variables.iter().any(|bound| bound == variable) {
                bound_variables.push(variable.clone());
            }
        }
    }
}

fn distributed_bind_optional_leading_match_nulls(
    row: &mut HashMap<String, Value>,
    leading_match: &DistributedLeadingMatch,
) {
    match leading_match {
        DistributedLeadingMatch::Node { variable, .. } => {
            if let Some(variable) = variable {
                if !row.contains_key(variable) {
                    row.insert(variable.clone(), Value::Null);
                }
            }
        }
        DistributedLeadingMatch::Relationship {
            start_variable,
            end_variable,
            edge_variable,
            ..
        } => {
            for variable in [start_variable, end_variable, edge_variable]
                .into_iter()
                .flatten()
            {
                if !row.contains_key(variable) {
                    row.insert(variable.clone(), Value::Null);
                }
            }
        }
    }
}

fn distributed_supported_leading_with_clause(with_clause: &WithClause) -> bool {
    with_clause
        .items
        .iter()
        .all(distributed_supported_leading_with_item)
}

fn distributed_supported_leading_with_item(item: &ReturnItem) -> bool {
    item.alias.is_some()
        || matches!(
            item.expression,
            Expression::Variable(_) | Expression::PropertyAccess { .. }
        )
}

fn distributed_leading_with_column_name(item: &ReturnItem) -> String {
    if let Some(alias) = &item.alias {
        return alias.clone();
    }
    match &item.expression {
        Expression::Variable(variable) => variable.clone(),
        Expression::PropertyAccess { variable, property } => format!("{variable}.{property}"),
        _ => "expr".to_string(),
    }
}

fn distributed_project_row(
    row: &HashMap<String, Value>,
    items: &[ReturnItem],
    params: &HashMap<String, Value>,
) -> Result<HashMap<String, Value>, CopperDbError> {
    let mut projected = HashMap::new();
    for item in items {
        projected.insert(
            distributed_leading_with_column_name(item),
            eval_expression(&item.expression, row, params)
                .map_err(|err| CopperDbError::Eval(err.to_string()))?,
        );
    }
    Ok(projected)
}

fn distributed_node_selector(
    node: &copperdb_cypher::NodePattern,
    bound_variables: &[String],
) -> Option<DistributedNodeSelector> {
    let literal_properties = distributed_literal_properties(&node.properties)?;

    if let Some(variable) = &node.variable {
        if bound_variables.iter().any(|bound| bound == variable) {
            return Some(DistributedNodeSelector::Bound {
                variable: variable.clone(),
                labels: node.labels.clone(),
                properties: literal_properties,
            });
        }
    }

    if node.labels.is_empty() {
        return match literal_properties.get("_id") {
            Some(Value::String(node_id)) => {
                Some(DistributedNodeSelector::LiteralId(node_id.clone()))
            }
            _ => None,
        };
    }

    Some(DistributedNodeSelector::Pattern {
        labels: node.labels.clone(),
        properties: literal_properties,
    })
}

fn supported_distributed_path_return_expression(
    expression: &Expression,
    path_variable: &str,
) -> bool {
    match expression {
        Expression::Variable(variable) => variable == path_variable,
        Expression::FunctionCall { name, args, .. }
            if args.len() == 1
                && matches!(&args[0], Expression::Variable(variable) if variable == path_variable) =>
        {
            matches!(
                name.to_ascii_lowercase().as_str(),
                "nodes" | "relationships" | "length"
            )
        }
        _ => false,
    }
}

fn distributed_shortest_path_result(
    shape: &DistributedShortestPathQueryShape,
    path_value: Option<&Value>,
) -> Result<QueryResult, CopperDbError> {
    let path_values = path_value.cloned().into_iter().collect::<Vec<_>>();
    distributed_path_query_result(&shape.return_items, &shape.path_variable, &path_values)
}

fn distributed_path_query_result(
    return_items: &[ReturnItem],
    path_variable: &str,
    path_values: &[Value],
) -> Result<QueryResult, CopperDbError> {
    let columns = return_items
        .iter()
        .map(distributed_return_column_name)
        .collect::<Vec<_>>();
    let rows = path_values
        .iter()
        .map(|path_value| {
            return_items
                .iter()
                .map(|item| {
                    Ok((
                        distributed_return_column_name(item),
                        distributed_return_value(&item.expression, path_variable, path_value)?,
                    ))
                })
                .collect::<Result<HashMap<_, _>, CopperDbError>>()
        })
        .collect::<Result<Vec<_>, CopperDbError>>()?;

    Ok(QueryResult {
        columns,
        rows,
        stats: ResultStats::default(),
    })
}

fn distributed_bfs_query_result(path_value: Option<&Value>) -> QueryResult {
    let columns = vec![
        "path".into(),
        "nodes(path)".into(),
        "relationships(path)".into(),
        "length(path)".into(),
    ];
    let Some(path_value) = path_value else {
        return QueryResult {
            columns,
            rows: Vec::new(),
            stats: ResultStats::default(),
        };
    };

    let row = HashMap::from([
        ("path".into(), path_value.clone()),
        (
            "nodes(path)".into(),
            distributed_return_value(
                &Expression::FunctionCall {
                    name: "nodes".into(),
                    args: vec![Expression::Variable("path".into())],
                    distinct: false,
                },
                "path",
                path_value,
            )
            .unwrap_or(Value::Array(Vec::new())),
        ),
        (
            "relationships(path)".into(),
            distributed_return_value(
                &Expression::FunctionCall {
                    name: "relationships".into(),
                    args: vec![Expression::Variable("path".into())],
                    distinct: false,
                },
                "path",
                path_value,
            )
            .unwrap_or(Value::Array(Vec::new())),
        ),
        (
            "length(path)".into(),
            distributed_return_value(
                &Expression::FunctionCall {
                    name: "length".into(),
                    args: vec![Expression::Variable("path".into())],
                    distinct: false,
                },
                "path",
                path_value,
            )
            .unwrap_or(Value::Null),
        ),
    ]);

    QueryResult {
        columns,
        rows: vec![row],
        stats: ResultStats::default(),
    }
}

fn distributed_return_column_name(item: &ReturnItem) -> String {
    if let Some(alias) = &item.alias {
        return alias.clone();
    }
    match &item.expression {
        Expression::Variable(variable) => variable.clone(),
        Expression::FunctionCall { name, args, .. } if !args.is_empty() => {
            format!("{name}({})", distributed_expression_name(&args[0]))
        }
        _ => distributed_expression_name(&item.expression),
    }
}

fn distributed_expression_name(expression: &Expression) -> String {
    match expression {
        Expression::Variable(variable) => variable.clone(),
        Expression::FunctionCall { name, .. } => name.clone(),
        _ => "expr".to_string(),
    }
}

fn distributed_return_value(
    expression: &Expression,
    path_variable: &str,
    path_value: &Value,
) -> Result<Value, CopperDbError> {
    match expression {
        Expression::Variable(variable) if variable == path_variable => Ok(path_value.clone()),
        Expression::FunctionCall { name, args, .. }
            if args.len() == 1
                && matches!(&args[0], Expression::Variable(variable) if variable == path_variable) =>
        {
            match name.to_ascii_lowercase().as_str() {
                "nodes" => Ok(match path_value {
                    Value::Object(path_map) => path_map
                        .get("nodes")
                        .cloned()
                        .unwrap_or_else(|| Value::Array(Vec::new())),
                    _ => Value::Array(Vec::new()),
                }),
                "relationships" => Ok(match path_value {
                    Value::Object(path_map) => path_map
                        .get("relationships")
                        .cloned()
                        .unwrap_or_else(|| Value::Array(Vec::new())),
                    _ => Value::Array(Vec::new()),
                }),
                "length" => Ok(match path_value {
                    Value::Object(path_map) => {
                        path_map.get("length").cloned().unwrap_or(Value::Null)
                    }
                    _ => Value::Null,
                }),
                other => Err(CopperDbError::Eval(format!(
                    "unsupported distributed path return function: {other}"
                ))),
            }
        }
        _ => Err(CopperDbError::Eval(
            "unsupported distributed shortestPath return expression".to_string(),
        )),
    }
}

fn distributed_edge_to_value(edge: &copperdb_storage::EdgeRecord) -> Value {
    Value::Object(
        edge.properties
            .iter()
            .map(|(key, value)| (key.clone(), value.clone()))
            .chain([
                ("_id".to_string(), Value::String(edge.id.clone())),
                ("_type".to_string(), Value::String(edge.edge_type.clone())),
                ("_start".to_string(), Value::String(edge.start_node.clone())),
                ("_end".to_string(), Value::String(edge.end_node.clone())),
                (
                    "_created_at_unix_ms".to_string(),
                    Value::from(edge.created_at_unix_ms),
                ),
                (
                    "_updated_at_unix_ms".to_string(),
                    Value::from(edge.updated_at_unix_ms),
                ),
            ])
            .collect(),
    )
}

fn distributed_node_record(node: &Value) -> Option<copperdb_storage::NodeRecord> {
    let Value::Object(map) = node else {
        return None;
    };
    let id = map.get("_id")?.as_str()?.to_string();
    let labels = map
        .get("_labels")?
        .as_array()?
        .iter()
        .map(|value| value.as_str().map(str::to_string))
        .collect::<Option<Vec<_>>>()?;
    let created_at_unix_ms = map.get("_created_at_unix_ms")?.as_i64()?;
    let updated_at_unix_ms = map.get("_updated_at_unix_ms")?.as_i64()?;
    let properties = map
        .iter()
        .filter(|(key, _)| {
            !matches!(
                key.as_str(),
                "_id" | "_labels" | "_created_at_unix_ms" | "_updated_at_unix_ms"
            )
        })
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect();
    Some(copperdb_storage::NodeRecord {
        id,
        labels,
        properties,
        created_at_unix_ms,
        updated_at_unix_ms,
    })
}

fn distributed_node_id(node: &Value) -> Option<String> {
    let Value::Object(map) = node else {
        return None;
    };
    match map.get("_id") {
        Some(Value::String(node_id)) => Some(node_id.clone()),
        _ => None,
    }
}

fn distributed_node_ids(nodes: &[Value]) -> Vec<String> {
    let mut ids = nodes
        .iter()
        .filter_map(distributed_node_id)
        .collect::<Vec<_>>();
    ids.sort();
    ids.dedup();
    ids
}

fn distributed_path_nodes(path_value: &Value) -> Option<&[Value]> {
    let Value::Object(map) = path_value else {
        return None;
    };
    map.get("nodes")
        .and_then(Value::as_array)
        .map(Vec::as_slice)
}

fn distributed_path_relationships(path_value: &Value) -> Option<&[Value]> {
    let Value::Object(map) = path_value else {
        return None;
    };
    map.get("relationships")
        .and_then(Value::as_array)
        .map(Vec::as_slice)
}

fn distributed_node_matches(
    node: &Value,
    labels: &[String],
    properties: &BTreeMap<String, Value>,
) -> bool {
    let Value::Object(map) = node else {
        return false;
    };

    let label_match = labels.iter().all(|label| {
        map.get("_labels")
            .and_then(Value::as_array)
            .map(|values| {
                values
                    .iter()
                    .any(|value| value.as_str() == Some(label.as_str()))
            })
            .unwrap_or(false)
    });
    let prop_match = properties.iter().all(|(key, value)| {
        map.get(key)
            .map(|candidate| candidate == value)
            .unwrap_or(false)
    });

    label_match && prop_match
}

fn distributed_edge_matches(
    edge: &copperdb_storage::EdgeRecord,
    properties: &BTreeMap<String, Value>,
) -> bool {
    properties.iter().all(|(key, value)| {
        edge.properties
            .get(key)
            .map(|candidate| candidate == value)
            .unwrap_or(false)
    })
}

fn distributed_related_node_id(
    current_node_id: &str,
    edge: &copperdb_storage::EdgeRecord,
    direction: &EdgeDirection,
) -> Option<String> {
    match direction {
        EdgeDirection::Outgoing if edge.start_node == current_node_id => {
            Some(edge.end_node.clone())
        }
        EdgeDirection::Incoming if edge.end_node == current_node_id => {
            Some(edge.start_node.clone())
        }
        EdgeDirection::Both if edge.start_node == current_node_id => Some(edge.end_node.clone()),
        EdgeDirection::Both if edge.end_node == current_node_id => Some(edge.start_node.clone()),
        _ => None,
    }
}
