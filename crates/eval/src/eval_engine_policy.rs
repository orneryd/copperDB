use super::*;
use crate::procedure_registry::BuiltinProcedure;

const MAX_FULLTEXT_VOCABULARY_TERMS: usize = 2_048;
const MAX_FULLTEXT_VOCABULARY_ENTRIES: usize = 16_384;

pub(crate) struct ProcedureGraphServices<'a> {
    pub(crate) read: &'a dyn GraphReadService,
    pub(crate) write: &'a dyn GraphWriteService,
}

impl EvalEngine {
    pub(crate) fn execute_call_clause(
        &self,
        request_context: &copperdb_util::RequestContext,
        call: &copperdb_cypher::CallClause,
        params: &HashMap<String, Value>,
        rows: &[Row],
    ) -> Result<EvalResult, EvalError> {
        request_context.check_active()?;
        let descriptor = self
            .procedure_registry
            .get(&call.procedure)
            .ok_or_else(|| {
                EvalError::ExecutionError(format!("CALL {} is not supported yet", call.procedure))
            })?;
        let result = if let Some(implementation) = descriptor.builtin_implementation() {
            self.execute_builtin_procedure(implementation, request_context, call, params, rows)
        } else {
            let denied_graph_write = DeniedGraphWriteService;
            let graph_write: &dyn GraphWriteService = if self.package_graph_write_enabled {
                self.storage.as_ref()
            } else {
                &denied_graph_write
            };
            self.execute_extension_procedure(
                descriptor,
                request_context,
                call,
                params,
                rows,
                ProcedureGraphServices {
                    read: self.storage.as_ref(),
                    write: graph_write,
                },
            )
        }?;
        request_context.check_active()?;

        self.project_call_result(call, result, params)
    }

    fn execute_builtin_procedure(
        &self,
        implementation: BuiltinProcedure,
        request_context: &copperdb_util::RequestContext,
        call: &copperdb_cypher::CallClause,
        params: &HashMap<String, Value>,
        rows: &[Row],
    ) -> Result<EvalResult, EvalError> {
        match implementation {
            BuiltinProcedure::DbLabels => self.execute_db_labels_call(call),
            BuiltinProcedure::DbRelationshipTypes => self.execute_db_relationship_types_call(call),
            BuiltinProcedure::DbPropertyKeys => self.execute_db_property_keys_call(call),
            BuiltinProcedure::DbConstraints => self.execute_db_constraints_call(call),
            BuiltinProcedure::DbIndexes => self.execute_db_indexes_call(call),
            BuiltinProcedure::DbPing => self.execute_db_ping_call(call),
            BuiltinProcedure::DbInfo => self.execute_db_info_call(call),
            BuiltinProcedure::DbSchemaNodeProperties => {
                self.execute_db_schema_node_properties_call(call)
            }
            BuiltinProcedure::DbSchemaRelProperties => {
                self.execute_db_schema_rel_properties_call(call)
            }
            BuiltinProcedure::DbSchemaVisualization => {
                self.execute_db_schema_visualization_call(call)
            }
            BuiltinProcedure::NornicDbVersion => self.execute_nornicdb_version_call(call),
            BuiltinProcedure::NornicDbStats => self.execute_nornicdb_stats_call(call),
            BuiltinProcedure::NornicDbDecayInfo => self.execute_nornicdb_decay_info_call(call),
            BuiltinProcedure::NornicDbKnowledgePolicyInfo => {
                self.execute_nornicdb_knowledgepolicy_info_call(call)
            }
            BuiltinProcedure::DbmsProcedures => {
                self.execute_dbms_procedures_call(request_context, call)
            }
            BuiltinProcedure::DbmsFunctions => self.execute_dbms_functions_call(call),
            BuiltinProcedure::DbmsComponents => self.execute_dbms_components_call(call),
            BuiltinProcedure::DbmsInfo => self.execute_dbms_info_call(call),
            BuiltinProcedure::DbmsListConfig => self.execute_dbms_list_config_call(call),
            BuiltinProcedure::DbmsClientConfig => self.execute_dbms_client_config_call(call),
            BuiltinProcedure::DbmsListConnections => self.execute_dbms_list_connections_call(call),
            BuiltinProcedure::FulltextListAnalyzers => {
                self.execute_fulltext_list_analyzers_call(call)
            }
            BuiltinProcedure::KnowledgePolicyResolve => {
                self.execute_knowledge_policy_resolve_call(call, params)
            }
            BuiltinProcedure::KnowledgePolicyProfiles => {
                self.execute_nornicdb_knowledgepolicy_profiles_call(call)
            }
            BuiltinProcedure::KnowledgePolicyPolicies => {
                self.execute_nornicdb_knowledgepolicy_policies_call(call)
            }
            BuiltinProcedure::FulltextQueryNodes => {
                self.execute_fulltext_query_nodes_call(request_context, call, params)
            }
            BuiltinProcedure::FulltextQueryRelationships => {
                self.execute_fulltext_query_relationships_call(request_context, call, params)
            }
            BuiltinProcedure::VectorQueryNodes => {
                self.execute_vector_query_nodes_call(request_context, call, params)
            }
            BuiltinProcedure::VectorQueryRelationships => {
                self.execute_vector_query_relationships_call(request_context, call, params)
            }
            BuiltinProcedure::DbRetrieve => {
                self.execute_db_retrieve_call(request_context, call, params, false)
            }
            BuiltinProcedure::DbRRetrieve => {
                self.execute_db_retrieve_call(request_context, call, params, true)
            }
            BuiltinProcedure::SetNodeVectorProperty => {
                self.execute_set_node_vector_property(call, params, rows)
            }
            BuiltinProcedure::SetRelationshipVectorProperty => {
                self.execute_set_relationship_vector_property(call, params, rows)
            }
        }
    }

    pub(crate) fn execute_extension_procedure(
        &self,
        descriptor: &ProcedureDescriptor,
        request_context: &copperdb_util::RequestContext,
        call: &copperdb_cypher::CallClause,
        params: &HashMap<String, Value>,
        rows: &[Row],
        graph: ProcedureGraphServices<'_>,
    ) -> Result<EvalResult, EvalError> {
        let execution_context = CURRENT_EXECUTION_CONTEXT.with(|slot| slot.borrow().clone());
        for capability in descriptor.required_capabilities() {
            if !execution_context
                .capabilities
                .iter()
                .any(|actual| actual == capability)
            {
                return Err(EvalError::ExecutionError(format!(
                    "procedure {} requires capability {}",
                    descriptor.canonical_name(),
                    capability
                )));
            }
        }
        for role in descriptor.required_roles() {
            if !execution_context
                .caller_roles
                .iter()
                .any(|actual| actual == role)
            {
                return Err(EvalError::ExecutionError(format!(
                    "procedure {} requires role {}",
                    descriptor.canonical_name(),
                    role
                )));
            }
        }
        let handler = descriptor
            .extension_handler()
            .expect("extension descriptor must have a handler");
        let mut columns = None;
        let mut result_rows = Vec::new();
        let mut stats = QueryStats::default();
        for row in rows {
            request_context.check_active()?;
            let args = call
                .args
                .iter()
                .map(|arg| self.evaluate_expression(arg, row, params))
                .collect::<Result<Vec<_>, _>>()?;
            let context = ProcedureCallContext {
                row,
                params,
                capabilities: &execution_context.capabilities,
                caller_roles: &execution_context.caller_roles,
                database: execution_context.database.as_deref(),
                request_context,
                graph_read: graph.read,
                graph_write: graph.write,
                import_files: self.import_files.as_ref(),
            };
            let output =
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| handler(&context, &args)))
                    .map_err(|_| {
                        EvalError::ExecutionError(format!(
                            "extension procedure {} panicked",
                            descriptor.canonical_name()
                        ))
                    })?
                    .map_err(|error| match error {
                        ProcedureError::RequestCancelled(cancelled) => {
                            EvalError::RequestCancelled(cancelled)
                        }
                        ProcedureError::Message(message) => EvalError::ExecutionError(format!(
                            "extension procedure {} failed: {}",
                            descriptor.canonical_name(),
                            message
                        )),
                    })?;
            stats.nodes_created += output.stats.nodes_created;
            stats.nodes_deleted += output.stats.nodes_deleted;
            stats.relationships_created += output.stats.relationships_created;
            stats.relationships_deleted += output.stats.relationships_deleted;
            stats.properties_set += output.stats.properties_set;
            if let Some(expected) = &columns {
                if expected != &output.columns {
                    return Err(EvalError::ExecutionError(format!(
                        "extension procedure {} returned inconsistent columns",
                        descriptor.canonical_name()
                    )));
                }
            } else {
                columns = Some(output.columns.clone());
            }
            for output_row in output.rows {
                let mut merged = row.clone();
                merged.extend(output_row);
                result_rows.push(merged);
            }
            request_context.check_active()?;
        }
        Ok(
            ProcedureOutput::new(columns.unwrap_or_default(), result_rows)
                .with_stats(stats)
                .into(),
        )
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
        request_context: &copperdb_util::RequestContext,
        call: &copperdb_cypher::CallClause,
    ) -> Result<EvalResult, EvalError> {
        if !call.args.is_empty() {
            return Err(EvalError::ExecutionError(
                "dbms.procedures expects no arguments".to_string(),
            ));
        }

        let language = request_context
            .language_preferences()
            .iter()
            .find_map(|preference| {
                copperdb_localization::LanguageTag::parse(preference)
                    .ok()
                    .flatten()
            })
            .or_else(|| {
                copperdb_localization::LanguageTag::parse("en-US").expect("source locale is valid")
            })
            .expect("source locale is defined");
        let mut rows = self
            .procedure_registry
            .descriptors()
            .iter()
            .filter(|descriptor| !descriptor.is_hidden())
            .map(|descriptor| {
                let mut row = Row::new();
                row.insert(
                    "name".to_string(),
                    Value::String(descriptor.name().to_string()),
                );
                row.insert(
                    "signature".to_string(),
                    Value::String(descriptor.signature().to_string()),
                );
                row.insert(
                    "description".to_string(),
                    Value::String(descriptor.localized_description(&language).to_string()),
                );
                row.insert(
                    "mode".to_string(),
                    Value::String(descriptor.mode().as_str().to_string()),
                );
                if let Some(package_id) = descriptor.package_id() {
                    row.insert("package".to_string(), Value::String(package_id.to_string()));
                }
                row
            })
            .collect::<Vec<_>>();
        rows.sort_by(|left, right| left["name"].as_str().cmp(&right["name"].as_str()));
        Ok(EvalResult {
            columns: vec![
                "name".to_string(),
                "signature".to_string(),
                "description".to_string(),
                "mode".to_string(),
                "package".to_string(),
            ],
            rows,
            stats: QueryStats::default(),
        })
    }

    fn execute_dbms_functions_call(
        &self,
        call: &copperdb_cypher::CallClause,
    ) -> Result<EvalResult, EvalError> {
        if !call.args.is_empty() {
            return Err(EvalError::ExecutionError(
                "dbms.functions expects no arguments".to_string(),
            ));
        }

        let mut functions = self
            .function_registry
            .descriptors()
            .iter()
            .filter(|descriptor| !descriptor.is_hidden())
            .flat_map(|descriptor| {
                std::iter::once(descriptor.name())
                    .chain(descriptor.aliases().iter().map(String::as_str))
                    .map(|name| {
                        let signature = descriptor.signature().replacen(descriptor.name(), name, 1);
                        let mut row = Row::new();
                        row.insert("name".to_string(), Value::String(name.to_string()));
                        row.insert("signature".to_string(), Value::String(signature));
                        row.insert(
                            "description".to_string(),
                            Value::String(descriptor.description_for_name(name).to_string()),
                        );
                        row.insert(
                            "category".to_string(),
                            Value::String(descriptor.category().to_string()),
                        );
                        if let Some(package_id) = descriptor.package_id() {
                            row.insert(
                                "package".to_string(),
                                Value::String(package_id.to_string()),
                            );
                        }
                        row
                    })
            })
            .collect::<Vec<_>>();
        functions.sort_by(|left, right| {
            left.get("name")
                .and_then(Value::as_str)
                .cmp(&right.get("name").and_then(Value::as_str))
        });
        Ok(EvalResult {
            columns: vec![
                "name".to_string(),
                "signature".to_string(),
                "description".to_string(),
                "category".to_string(),
                "package".to_string(),
            ],
            rows: functions,
            stats: QueryStats::default(),
        })
    }

    fn execute_dbms_components_call(
        &self,
        call: &copperdb_cypher::CallClause,
    ) -> Result<EvalResult, EvalError> {
        if !call.args.is_empty() {
            return Err(EvalError::ExecutionError(
                "dbms.components expects no arguments".to_string(),
            ));
        }
        let mut row = Row::new();
        row.insert("name".to_string(), Value::String("CopperDB".to_string()));
        row.insert(
            "versions".to_string(),
            Value::Array(vec![Value::String("0.1.0".to_string())]),
        );
        row.insert(
            "edition".to_string(),
            Value::String("community".to_string()),
        );
        Ok(EvalResult {
            columns: vec![
                "name".to_string(),
                "versions".to_string(),
                "edition".to_string(),
            ],
            rows: vec![row],
            stats: QueryStats::default(),
        })
    }

    fn execute_dbms_info_call(
        &self,
        call: &copperdb_cypher::CallClause,
    ) -> Result<EvalResult, EvalError> {
        if !call.args.is_empty() {
            return Err(EvalError::ExecutionError(
                "dbms.info expects no arguments".to_string(),
            ));
        }
        let mut row = Row::new();
        row.insert(
            "id".to_string(),
            Value::String("copperdb-instance".to_string()),
        );
        row.insert("name".to_string(), Value::String("CopperDB".to_string()));
        row.insert(
            "creationDate".to_string(),
            Value::String("2024-01-01T00:00:00Z".to_string()),
        );
        Ok(EvalResult {
            columns: vec![
                "id".to_string(),
                "name".to_string(),
                "creationDate".to_string(),
            ],
            rows: vec![row],
            stats: QueryStats::default(),
        })
    }

    fn execute_dbms_list_config_call(
        &self,
        call: &copperdb_cypher::CallClause,
    ) -> Result<EvalResult, EvalError> {
        if !call.args.is_empty() {
            return Err(EvalError::ExecutionError(
                "dbms.listConfig expects no arguments".to_string(),
            ));
        }
        let configs: Vec<(&str, &str, Value, bool)> = vec![
            (
                "nornicdb.version",
                "NornicDB version",
                Value::String("0.1.0".to_string()),
                false,
            ),
            (
                "nornicdb.bolt.enabled",
                "Bolt protocol enabled",
                Value::Bool(true),
                false,
            ),
            (
                "nornicdb.http.enabled",
                "HTTP API enabled",
                Value::Bool(true),
                false,
            ),
        ];
        let rows: Vec<Row> = configs
            .into_iter()
            .map(|(name, desc, val, dynamic)| {
                let mut row = Row::new();
                row.insert("name".to_string(), Value::String(name.to_string()));
                row.insert("description".to_string(), Value::String(desc.to_string()));
                row.insert("value".to_string(), val.clone());
                row.insert("dynamic".to_string(), Value::Bool(dynamic));
                row
            })
            .collect();
        Ok(EvalResult {
            columns: vec![
                "name".to_string(),
                "description".to_string(),
                "value".to_string(),
                "dynamic".to_string(),
            ],
            rows,
            stats: QueryStats::default(),
        })
    }

    fn execute_dbms_client_config_call(
        &self,
        call: &copperdb_cypher::CallClause,
    ) -> Result<EvalResult, EvalError> {
        if !call.args.is_empty() {
            return Err(EvalError::ExecutionError(
                "dbms.clientConfig expects no arguments".to_string(),
            ));
        }
        let configs = vec![
            ("server.bolt.advertised_address", "localhost:7687"),
            ("server.http.advertised_address", "localhost:7474"),
        ];
        let rows: Vec<Row> = configs
            .into_iter()
            .map(|(name, val)| {
                let mut row = Row::new();
                row.insert("name".to_string(), Value::String(name.to_string()));
                row.insert("value".to_string(), Value::String(val.to_string()));
                row
            })
            .collect();
        Ok(EvalResult {
            columns: vec!["name".to_string(), "value".to_string()],
            rows,
            stats: QueryStats::default(),
        })
    }

    fn execute_dbms_list_connections_call(
        &self,
        call: &copperdb_cypher::CallClause,
    ) -> Result<EvalResult, EvalError> {
        if !call.args.is_empty() {
            return Err(EvalError::ExecutionError(
                "dbms.listConnections expects no arguments".to_string(),
            ));
        }
        Ok(EvalResult {
            columns: vec![
                "connectionId".to_string(),
                "connectTime".to_string(),
                "connector".to_string(),
                "username".to_string(),
                "userAgent".to_string(),
                "clientAddress".to_string(),
            ],
            rows: vec![],
            stats: QueryStats::default(),
        })
    }

    fn execute_fulltext_list_analyzers_call(
        &self,
        call: &copperdb_cypher::CallClause,
    ) -> Result<EvalResult, EvalError> {
        if !call.args.is_empty() {
            return Err(EvalError::ExecutionError(
                "db.index.fulltext.listAvailableAnalyzers expects no arguments".to_string(),
            ));
        }
        let analyzers = vec![
            (
                "standard-no-stop-words",
                "Standard analyzer without stop words",
            ),
            ("simple", "Simple analyzer with lowercase tokenizer"),
            ("whitespace", "Whitespace analyzer"),
            (
                "keyword",
                "Keyword analyzer - entire string as single token",
            ),
            ("url-or-email", "URL or email analyzer"),
        ];
        let rows: Vec<Row> = analyzers
            .into_iter()
            .map(|(analyzer, desc)| {
                let mut row = Row::new();
                row.insert("analyzer".to_string(), Value::String(analyzer.to_string()));
                row.insert("description".to_string(), Value::String(desc.to_string()));
                row
            })
            .collect();
        Ok(EvalResult {
            columns: vec!["analyzer".to_string(), "description".to_string()],
            rows,
            stats: QueryStats::default(),
        })
    }

    fn execute_db_property_keys_call(
        &self,
        call: &copperdb_cypher::CallClause,
    ) -> Result<EvalResult, EvalError> {
        if !call.args.is_empty() {
            return Err(EvalError::ExecutionError(
                "db.propertyKeys expects no arguments".to_string(),
            ));
        }

        let mut keys: Vec<String> = Vec::new();
        for node in self.storage.all_node_records()? {
            keys.extend(node.properties.keys().cloned());
        }
        for edge in self.storage.all_edges()? {
            keys.extend(edge.properties.keys().cloned());
        }
        keys.sort();
        keys.dedup();

        Ok(EvalResult {
            columns: vec!["propertyKey".to_string()],
            rows: keys
                .into_iter()
                .map(|key| {
                    let mut row = Row::new();
                    row.insert("propertyKey".to_string(), Value::String(key));
                    row
                })
                .collect(),
            stats: QueryStats::default(),
        })
    }

    fn execute_db_constraints_call(
        &self,
        call: &copperdb_cypher::CallClause,
    ) -> Result<EvalResult, EvalError> {
        if !call.args.is_empty() {
            return Err(EvalError::ExecutionError(
                "db.constraints expects no arguments".to_string(),
            ));
        }

        let constraints = self.storage.load_constraints()?;
        let rows = constraints
            .into_iter()
            .map(|constraint| {
                let mut row = Row::new();
                row.insert("name".to_string(), Value::String(constraint.name));
                let constraint_type = match constraint.constraint_type {
                    ConstraintType::Unique => "UNIQUENESS",
                    ConstraintType::Exists => "NODE_PROPERTY_EXISTENCE",
                    ConstraintType::NodeKey => "NODE_KEY",
                    ConstraintType::Type => "RELATIONSHIP_PROPERTY_TYPE",
                    ConstraintType::Relationship => "RELATIONSHIP_PROPERTY_EXISTENCE",
                    ConstraintType::Temporal => "TEMPORAL_NO_OVERLAP",
                    ConstraintType::Domain => "DOMAIN",
                };
                row.insert(
                    "type".to_string(),
                    Value::String(constraint_type.to_string()),
                );
                row.insert(
                    "labelsOrTypes".to_string(),
                    Value::Array(vec![Value::String(constraint.label)]),
                );
                row.insert(
                    "properties".to_string(),
                    Value::Array(
                        constraint
                            .properties
                            .iter()
                            .map(|p| Value::String(p.clone()))
                            .collect(),
                    ),
                );
                row.insert("propertyType".to_string(), Value::Null);
                row
            })
            .collect();

        Ok(EvalResult {
            columns: vec![
                "name".to_string(),
                "type".to_string(),
                "labelsOrTypes".to_string(),
                "properties".to_string(),
                "propertyType".to_string(),
            ],
            rows,
            stats: QueryStats::default(),
        })
    }

    fn execute_db_indexes_call(
        &self,
        call: &copperdb_cypher::CallClause,
    ) -> Result<EvalResult, EvalError> {
        if !call.args.is_empty() {
            return Err(EvalError::ExecutionError(
                "db.indexes expects no arguments".to_string(),
            ));
        }

        let indexes = self.storage.load_index_definitions()?;
        let rows = indexes
            .into_iter()
            .map(|index| {
                let mut row = Row::new();
                row.insert("name".to_string(), Value::String(index.name));
                let index_type = match index.kind {
                    copperdb_storage::IndexKind::Range => "RANGE",
                    copperdb_storage::IndexKind::Temporal => "TEMPORAL",
                    copperdb_storage::IndexKind::FullText => "FULLTEXT",
                    copperdb_storage::IndexKind::Vector => "VECTOR",
                };
                row.insert("type".to_string(), Value::String(index_type.to_string()));
                row.insert(
                    "labelsOrTypes".to_string(),
                    Value::Array(vec![Value::String(index.label)]),
                );
                row.insert(
                    "properties".to_string(),
                    Value::Array(
                        index
                            .properties
                            .iter()
                            .map(|p| Value::String(p.clone()))
                            .collect(),
                    ),
                );
                row.insert("state".to_string(), Value::String("ONLINE".to_string()));
                row
            })
            .collect();

        Ok(EvalResult {
            columns: vec![
                "name".to_string(),
                "type".to_string(),
                "labelsOrTypes".to_string(),
                "properties".to_string(),
                "state".to_string(),
            ],
            rows,
            stats: QueryStats::default(),
        })
    }

    fn execute_db_ping_call(
        &self,
        call: &copperdb_cypher::CallClause,
    ) -> Result<EvalResult, EvalError> {
        if !call.args.is_empty() {
            return Err(EvalError::ExecutionError(
                "db.ping expects no arguments".to_string(),
            ));
        }

        let mut row = Row::new();
        row.insert("success".to_string(), Value::Bool(true));
        Ok(EvalResult {
            columns: vec!["success".to_string()],
            rows: vec![row],
            stats: QueryStats::default(),
        })
    }

    fn execute_db_info_call(
        &self,
        call: &copperdb_cypher::CallClause,
    ) -> Result<EvalResult, EvalError> {
        if !call.args.is_empty() {
            return Err(EvalError::ExecutionError(
                "db.info expects no arguments".to_string(),
            ));
        }

        let node_count = self.storage.all_node_records()?.len() as u64;
        let edge_count = self.storage.all_edges()?.len() as u64;

        let mut row = Row::new();
        row.insert("id".to_string(), Value::String("copperdb".to_string()));
        row.insert("name".to_string(), Value::String("copperdb".to_string()));
        row.insert(
            "creationDate".to_string(),
            Value::String("2025-01-01T00:00:00Z".to_string()),
        );
        row.insert("nodeCount".to_string(), Value::from(node_count));
        row.insert("relationshipCount".to_string(), Value::from(edge_count));

        Ok(EvalResult {
            columns: vec![
                "id".to_string(),
                "name".to_string(),
                "creationDate".to_string(),
                "nodeCount".to_string(),
                "relationshipCount".to_string(),
            ],
            rows: vec![row],
            stats: QueryStats::default(),
        })
    }

    fn execute_nornicdb_version_call(
        &self,
        call: &copperdb_cypher::CallClause,
    ) -> Result<EvalResult, EvalError> {
        if !call.args.is_empty() {
            return Err(EvalError::ExecutionError(
                "nornicdb.version expects no arguments".to_string(),
            ));
        }

        let mut row = Row::new();
        row.insert("version".to_string(), Value::String("0.1.0".to_string()));
        row.insert("build".to_string(), Value::String("dev".to_string()));
        row.insert(
            "edition".to_string(),
            Value::String("community".to_string()),
        );

        Ok(EvalResult {
            columns: vec![
                "version".to_string(),
                "build".to_string(),
                "edition".to_string(),
            ],
            rows: vec![row],
            stats: QueryStats::default(),
        })
    }

    fn execute_nornicdb_stats_call(
        &self,
        call: &copperdb_cypher::CallClause,
    ) -> Result<EvalResult, EvalError> {
        if !call.args.is_empty() {
            return Err(EvalError::ExecutionError(
                "nornicdb.stats expects no arguments".to_string(),
            ));
        }

        let nodes = self.storage.all_node_records()?;
        let edges = self.storage.all_edges()?;

        let node_count = nodes.len() as u64;
        let edge_count = edges.len() as u64;

        let mut label_set: HashSet<String> = HashSet::new();
        for node in &nodes {
            for label in &node.labels {
                label_set.insert(label.clone());
            }
        }
        let label_count = label_set.len() as u64;

        let mut rel_type_set: HashSet<String> = HashSet::new();
        for edge in &edges {
            rel_type_set.insert(edge.edge_type.clone());
        }
        let rel_type_count = rel_type_set.len() as u64;

        let mut row = Row::new();
        row.insert("nodes".to_string(), Value::from(node_count));
        row.insert("relationships".to_string(), Value::from(edge_count));
        row.insert("labels".to_string(), Value::from(label_count));
        row.insert("relationshipTypes".to_string(), Value::from(rel_type_count));

        Ok(EvalResult {
            columns: vec![
                "nodes".to_string(),
                "relationships".to_string(),
                "labels".to_string(),
                "relationshipTypes".to_string(),
            ],
            rows: vec![row],
            stats: QueryStats::default(),
        })
    }

    fn execute_nornicdb_decay_info_call(
        &self,
        call: &copperdb_cypher::CallClause,
    ) -> Result<EvalResult, EvalError> {
        if !call.args.is_empty() {
            return Err(EvalError::ExecutionError(
                "nornicdb.decay.info expects no arguments".to_string(),
            ));
        }

        let decay_profiles = self
            .storage
            .load_decay_profile_schemas()
            .unwrap_or_default();
        let decay_bindings = self
            .storage
            .load_decay_profile_binding_schemas()
            .unwrap_or_default();
        let enabled = !decay_profiles.is_empty() || !decay_bindings.is_empty();

        let mut row = Row::new();
        row.insert("enabled".to_string(), Value::Bool(enabled));
        row.insert(
            "system".to_string(),
            Value::String("knowledge-layer scoring (decay profile bundles + bindings)".to_string()),
        );
        row.insert(
            "configuredVia".to_string(),
            Value::String(
                "CREATE DECAY PROFILE ... OPTIONS / CREATE DECAY PROFILE ... FOR ... APPLY DDL"
                    .to_string(),
            ),
        );

        Ok(EvalResult {
            columns: vec![
                "enabled".to_string(),
                "system".to_string(),
                "configuredVia".to_string(),
            ],
            rows: vec![row],
            stats: QueryStats::default(),
        })
    }

    fn execute_nornicdb_knowledgepolicy_info_call(
        &self,
        call: &copperdb_cypher::CallClause,
    ) -> Result<EvalResult, EvalError> {
        if !call.args.is_empty() {
            return Err(EvalError::ExecutionError(
                "nornicdb.knowledgepolicy.info expects no arguments".to_string(),
            ));
        }

        let decay_profiles = self
            .storage
            .load_decay_profile_schemas()
            .unwrap_or_default();
        let decay_bindings = self
            .storage
            .load_decay_profile_binding_schemas()
            .unwrap_or_default();
        let promotion_profiles = self
            .storage
            .load_promotion_profile_schemas()
            .unwrap_or_default();
        let promotion_policies = self
            .storage
            .load_promotion_policy_schemas()
            .unwrap_or_default();

        let enabled = !decay_profiles.is_empty()
            || !decay_bindings.is_empty()
            || !promotion_profiles.is_empty()
            || !promotion_policies.is_empty();

        let mut row = Row::new();
        row.insert("enabled".to_string(), Value::Bool(enabled));
        row.insert(
            "system".to_string(),
            Value::String("knowledge-layer profile and policy catalog".to_string()),
        );
        row.insert(
            "decayProfiles".to_string(),
            Value::from(decay_profiles.len() as u64),
        );
        row.insert(
            "decayBindings".to_string(),
            Value::from(decay_bindings.len() as u64),
        );
        row.insert(
            "promotionProfiles".to_string(),
            Value::from(promotion_profiles.len() as u64),
        );
        row.insert(
            "promotionPolicies".to_string(),
            Value::from(promotion_policies.len() as u64),
        );
        row.insert(
            "configuredVia".to_string(),
            Value::String(
                "CREATE DECAY PROFILE / CREATE PROMOTION PROFILE / CREATE PROMOTION POLICY DDL"
                    .to_string(),
            ),
        );

        Ok(EvalResult {
            columns: vec![
                "enabled".to_string(),
                "system".to_string(),
                "decayProfiles".to_string(),
                "decayBindings".to_string(),
                "promotionProfiles".to_string(),
                "promotionPolicies".to_string(),
                "configuredVia".to_string(),
            ],
            rows: vec![row],
            stats: QueryStats::default(),
        })
    }

    fn execute_nornicdb_knowledgepolicy_profiles_call(
        &self,
        call: &copperdb_cypher::CallClause,
    ) -> Result<EvalResult, EvalError> {
        if !call.args.is_empty() {
            return Err(EvalError::ExecutionError(
                "nornicdb.knowledgepolicy.profiles expects no arguments".to_string(),
            ));
        }
        let bundles = self
            .storage
            .load_decay_profile_schemas()
            .unwrap_or_default();
        let bindings = self
            .storage
            .load_decay_profile_binding_schemas()
            .unwrap_or_default();
        let bundle_by_name: HashMap<String, &DecayProfileSchema> =
            bundles.iter().map(|b| (b.name.clone(), b)).collect();
        let columns = vec![
            "kind",
            "Name",
            "HalfLifeSeconds",
            "VisibilityThreshold",
            "ScoreFloor",
            "Function",
            "Scope",
            "DecayEnabled",
            "ScoreFrom",
            "ScoreFromProperty",
            "Enabled",
            "TargetLabels",
            "TargetEdgeType",
            "IsWildcard",
            "IsEdge",
            "ProfileRef",
            "NoDecay",
            "Order",
        ]
        .into_iter()
        .map(String::from)
        .collect();
        let mut rows: Vec<Row> = Vec::new();
        for bundle in &bundles {
            let mut row = Row::new();
            row.insert("kind".to_string(), Value::String("bundle".to_string()));
            row.insert("Name".to_string(), Value::String(bundle.name.clone()));
            row.insert(
                "HalfLifeSeconds".to_string(),
                Value::from(bundle.half_life_seconds),
            );
            row.insert(
                "VisibilityThreshold".to_string(),
                Value::from(bundle.visibility_threshold),
            );
            row.insert("ScoreFloor".to_string(), Value::from(bundle.score_floor));
            row.insert(
                "Function".to_string(),
                Value::String(bundle.function.clone()),
            );
            row.insert("Scope".to_string(), Value::String(bundle.scope.clone()));
            row.insert(
                "DecayEnabled".to_string(),
                Value::Bool(bundle.decay_enabled),
            );
            row.insert(
                "ScoreFrom".to_string(),
                Value::String(bundle.score_from.clone()),
            );
            row.insert(
                "ScoreFromProperty".to_string(),
                bundle
                    .score_from_property
                    .as_ref()
                    .map(|p| Value::String(p.clone()))
                    .unwrap_or(Value::Null),
            );
            row.insert("Enabled".to_string(), Value::Bool(bundle.enabled));
            row.insert("TargetLabels".to_string(), Value::Null);
            row.insert("TargetEdgeType".to_string(), Value::String(String::new()));
            row.insert("IsWildcard".to_string(), Value::Bool(false));
            row.insert("IsEdge".to_string(), Value::Bool(false));
            row.insert("ProfileRef".to_string(), Value::String(String::new()));
            row.insert("NoDecay".to_string(), Value::Bool(false));
            row.insert("Order".to_string(), Value::from(0i64));
            rows.push(row);
        }
        for binding in &bindings {
            let half_life = binding
                .profile_ref
                .as_ref()
                .and_then(|r| bundle_by_name.get(r))
                .map(|b| b.half_life_seconds)
                .unwrap_or(0);
            let score_floor = binding
                .profile_ref
                .as_ref()
                .and_then(|r| bundle_by_name.get(r))
                .map(|b| b.score_floor)
                .unwrap_or(0.0);
            let scope = if binding.is_edge { "EDGE" } else { "NODE" };
            let mut row = Row::new();
            row.insert("kind".to_string(), Value::String("binding".to_string()));
            row.insert("Name".to_string(), Value::String(binding.name.clone()));
            row.insert("HalfLifeSeconds".to_string(), Value::from(half_life));
            row.insert(
                "VisibilityThreshold".to_string(),
                binding
                    .visibility_threshold
                    .map(Value::from)
                    .unwrap_or(Value::Null),
            );
            row.insert("ScoreFloor".to_string(), Value::from(score_floor));
            row.insert("Function".to_string(), Value::String(String::new()));
            row.insert("Scope".to_string(), Value::String(scope.to_string()));
            row.insert("DecayEnabled".to_string(), Value::Bool(!binding.no_decay));
            row.insert("ScoreFrom".to_string(), Value::String(String::new()));
            row.insert(
                "ScoreFromProperty".to_string(),
                Value::String(String::new()),
            );
            row.insert("Enabled".to_string(), Value::Bool(true));
            row.insert(
                "TargetLabels".to_string(),
                Value::Array(
                    binding
                        .target_labels
                        .iter()
                        .map(|l| Value::String(l.clone()))
                        .collect(),
                ),
            );
            row.insert(
                "TargetEdgeType".to_string(),
                binding
                    .target_edge_type
                    .as_ref()
                    .map(|t| Value::String(t.clone()))
                    .unwrap_or(Value::String(String::new())),
            );
            row.insert("IsWildcard".to_string(), Value::Bool(binding.is_wildcard));
            row.insert("IsEdge".to_string(), Value::Bool(binding.is_edge));
            row.insert(
                "ProfileRef".to_string(),
                binding
                    .profile_ref
                    .as_ref()
                    .map(|r| Value::String(r.clone()))
                    .unwrap_or(Value::String(String::new())),
            );
            row.insert("NoDecay".to_string(), Value::Bool(binding.no_decay));
            row.insert("Order".to_string(), Value::from(binding.order));
            rows.push(row);
        }
        Ok(EvalResult {
            columns,
            rows,
            stats: QueryStats::default(),
        })
    }

    fn execute_nornicdb_knowledgepolicy_policies_call(
        &self,
        call: &copperdb_cypher::CallClause,
    ) -> Result<EvalResult, EvalError> {
        if !call.args.is_empty() {
            return Err(EvalError::ExecutionError(
                "nornicdb.knowledgepolicy.policies expects no arguments".to_string(),
            ));
        }
        let profiles = self
            .storage
            .load_promotion_profile_schemas()
            .unwrap_or_default();
        let policies = self
            .storage
            .load_promotion_policy_schemas()
            .unwrap_or_default();
        let columns = vec![
            "kind",
            "Name",
            "Scope",
            "Multiplier",
            "ScoreFloor",
            "ScoreCap",
            "Enabled",
            "TargetLabels",
            "TargetEdgeType",
            "IsWildcard",
            "IsEdge",
        ]
        .into_iter()
        .map(String::from)
        .collect();
        let mut rows: Vec<Row> = Vec::new();
        for profile in &profiles {
            let mut row = Row::new();
            row.insert("kind".to_string(), Value::String("profile".to_string()));
            row.insert("Name".to_string(), Value::String(profile.name.clone()));
            row.insert("Scope".to_string(), Value::String(profile.scope.clone()));
            row.insert("Multiplier".to_string(), Value::from(profile.multiplier));
            row.insert("ScoreFloor".to_string(), Value::from(profile.score_floor));
            row.insert("ScoreCap".to_string(), Value::from(profile.score_cap));
            row.insert("Enabled".to_string(), Value::Bool(profile.enabled));
            row.insert("TargetLabels".to_string(), Value::Null);
            row.insert("TargetEdgeType".to_string(), Value::String(String::new()));
            row.insert("IsWildcard".to_string(), Value::Bool(false));
            row.insert("IsEdge".to_string(), Value::Bool(false));
            rows.push(row);
        }
        for policy in &policies {
            let scope = if policy.is_edge { "EDGE" } else { "NODE" };
            let mut row = Row::new();
            row.insert("kind".to_string(), Value::String("policy".to_string()));
            row.insert("Name".to_string(), Value::String(policy.name.clone()));
            row.insert("Scope".to_string(), Value::String(scope.to_string()));
            row.insert("Multiplier".to_string(), Value::Null);
            row.insert("ScoreFloor".to_string(), Value::Null);
            row.insert("ScoreCap".to_string(), Value::Null);
            row.insert("Enabled".to_string(), Value::Bool(policy.enabled));
            row.insert(
                "TargetLabels".to_string(),
                Value::Array(
                    policy
                        .target_labels
                        .iter()
                        .map(|l| Value::String(l.clone()))
                        .collect(),
                ),
            );
            row.insert(
                "TargetEdgeType".to_string(),
                policy
                    .target_edge_type
                    .as_ref()
                    .map(|t| Value::String(t.clone()))
                    .unwrap_or(Value::String(String::new())),
            );
            row.insert("IsWildcard".to_string(), Value::Bool(policy.is_wildcard));
            row.insert("IsEdge".to_string(), Value::Bool(policy.is_edge));
            rows.push(row);
        }
        Ok(EvalResult {
            columns,
            rows,
            stats: QueryStats::default(),
        })
    }

    fn execute_db_schema_node_properties_call(
        &self,
        call: &copperdb_cypher::CallClause,
    ) -> Result<EvalResult, EvalError> {
        if !call.args.is_empty() {
            return Err(EvalError::ExecutionError(
                "db.schema.nodeProperties expects no arguments".to_string(),
            ));
        }

        let nodes = self.storage.all_node_records()?;
        let mut label_props: HashMap<String, HashSet<String>> = HashMap::new();
        for node in &nodes {
            for label in &node.labels {
                let entry = label_props.entry(label.clone()).or_default();
                for prop in node.properties.keys() {
                    entry.insert(prop.clone());
                }
            }
        }

        let mut rows: Vec<Row> = Vec::new();
        let mut labels: Vec<_> = label_props.keys().cloned().collect();
        labels.sort();
        for label in labels {
            let mut props: Vec<_> = label_props[&label].iter().cloned().collect();
            props.sort();
            for prop in props {
                let mut row = Row::new();
                row.insert("nodeLabel".to_string(), Value::String(label.clone()));
                row.insert("propertyName".to_string(), Value::String(prop));
                row.insert("propertyType".to_string(), Value::String("ANY".to_string()));
                rows.push(row);
            }
        }

        Ok(EvalResult {
            columns: vec![
                "nodeLabel".to_string(),
                "propertyName".to_string(),
                "propertyType".to_string(),
            ],
            rows,
            stats: QueryStats::default(),
        })
    }

    fn execute_db_schema_rel_properties_call(
        &self,
        call: &copperdb_cypher::CallClause,
    ) -> Result<EvalResult, EvalError> {
        if !call.args.is_empty() {
            return Err(EvalError::ExecutionError(
                "db.schema.relProperties expects no arguments".to_string(),
            ));
        }

        let edges = self.storage.all_edges()?;
        let mut type_props: HashMap<String, HashSet<String>> = HashMap::new();
        for edge in &edges {
            let entry = type_props.entry(edge.edge_type.clone()).or_default();
            for prop in edge.properties.keys() {
                entry.insert(prop.clone());
            }
        }

        let mut rows: Vec<Row> = Vec::new();
        let mut rel_types: Vec<_> = type_props.keys().cloned().collect();
        rel_types.sort();
        for rel_type in rel_types {
            let mut props: Vec<_> = type_props[&rel_type].iter().cloned().collect();
            props.sort();
            for prop in props {
                let mut row = Row::new();
                row.insert("relType".to_string(), Value::String(rel_type.clone()));
                row.insert("propertyName".to_string(), Value::String(prop));
                row.insert("propertyType".to_string(), Value::String("ANY".to_string()));
                rows.push(row);
            }
        }

        Ok(EvalResult {
            columns: vec![
                "relType".to_string(),
                "propertyName".to_string(),
                "propertyType".to_string(),
            ],
            rows,
            stats: QueryStats::default(),
        })
    }

    fn execute_db_schema_visualization_call(
        &self,
        call: &copperdb_cypher::CallClause,
    ) -> Result<EvalResult, EvalError> {
        if !call.args.is_empty() {
            return Err(EvalError::ExecutionError(
                "db.schema.visualization expects no arguments".to_string(),
            ));
        }

        let nodes = self.storage.all_node_records()?;
        let edges = self.storage.all_edges()?;

        let mut label_set: HashSet<String> = HashSet::new();
        for node in &nodes {
            for label in &node.labels {
                label_set.insert(label.clone());
            }
        }
        let mut labels: Vec<_> = label_set.into_iter().collect();
        labels.sort();
        let schema_nodes: Vec<Value> = labels
            .into_iter()
            .map(|label| {
                let mut map = serde_json::Map::new();
                map.insert("label".to_string(), Value::String(label));
                Value::Object(map)
            })
            .collect();

        let mut rel_type_set: HashSet<String> = HashSet::new();
        for edge in &edges {
            rel_type_set.insert(edge.edge_type.clone());
        }
        let mut rel_types: Vec<_> = rel_type_set.into_iter().collect();
        rel_types.sort();
        let schema_rels: Vec<Value> = rel_types
            .into_iter()
            .map(|rel_type| {
                let mut map = serde_json::Map::new();
                map.insert("type".to_string(), Value::String(rel_type));
                Value::Object(map)
            })
            .collect();

        let mut row = Row::new();
        row.insert("nodes".to_string(), Value::Array(schema_nodes));
        row.insert("relationships".to_string(), Value::Array(schema_rels));

        Ok(EvalResult {
            columns: vec!["nodes".to_string(), "relationships".to_string()],
            rows: vec![row],
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
        let procedure_columns = &result.columns;
        let rows = result
            .rows
            .iter()
            .map(|row| {
                let mut normalized = row.clone();
                for column in procedure_columns {
                    normalized.entry(column.clone()).or_insert(Value::Null);
                }
                let mut projected = project_row(&normalized, &call.yield_items, params)?;
                for (key, value) in row {
                    if !procedure_columns.contains(key) {
                        projected.insert(key.clone(), value.clone());
                    }
                }
                Ok::<Row, EvalError>(projected)
            })
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
        request_context: &copperdb_util::RequestContext,
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

        // Load persisted index options (vector.dimensions, vector.similarity_function, etc.)
        let index_options = self
            .storage
            .load_index_options(&index_name)
            .map_err(|e| EvalError::ExecutionError(format!("failed to load index options: {e}")))?;

        // Validate query vector dimensions if specified in options
        if let Some(expected_dims) = resolve_vector_dimensions(&index_options)
            && query_vector.len() as u64 != expected_dims
        {
            return Err(EvalError::ExecutionError(format!(
                "vector index {index_name} expects {expected_dims} dimensions, got {}",
                query_vector.len()
            )));
        }

        let (matches, _) = (self.vector_index_query)(
            request_context.cancellation(),
            &index_name,
            &query_vector,
            limit,
        )
        .map_err(|error| EvalError::ExecutionError(error.to_string()))?;
        let mut rows = Vec::with_capacity(matches.len());
        for (id, score) in matches {
            let Some(node) = self.storage.get_node_record(&id)? else {
                continue;
            };
            let mut row = Row::new();
            row.insert(
                "node".to_string(),
                Value::Object(node_record_to_props(&node).into_iter().collect()),
            );
            row.insert("score".to_string(), Value::from(score as f64));
            rows.push(row);
        }

        Ok(EvalResult {
            columns: vec!["node".to_string(), "score".to_string()],
            rows,
            stats: QueryStats::default(),
        })
    }

    fn execute_db_retrieve_call(
        &self,
        request_context: &copperdb_util::RequestContext,
        call: &copperdb_cypher::CallClause,
        params: &HashMap<String, Value>,
        _rerank: bool,
    ) -> Result<EvalResult, EvalError> {
        if call.args.len() != 1 {
            return Err(EvalError::ExecutionError(
                "db.retrieve expects one request map".to_string(),
            ));
        }
        let request = eval_expression(&call.args[0], &Row::new(), params)?;
        let request = request.as_object().ok_or_else(|| {
            EvalError::ExecutionError("db.retrieve request must be a map".to_string())
        })?;
        let query = request
            .get("query")
            .or_else(|| request.get("text"))
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|query| !query.is_empty())
            .ok_or_else(|| EvalError::ExecutionError("db.retrieve query required".to_string()))?;
        let strict = retrieval_bool(request, "failClosed", "fail_closed")?.unwrap_or(false);
        validate_retrieval_options(request, strict)?;
        let limit = retrieval_limit(request, strict)?;
        let rrf_k = retrieval_number(request, "rrfK", "rrf_k", 60.0);
        let vector_weight = retrieval_number(request, "vectorWeight", "vector_weight", 1.0);
        let bm25_weight = retrieval_number(request, "bm25Weight", "bm25_weight", 1.0);
        let min_rrf_score = retrieval_number(request, "minRRFScore", "min_rrf_score", 0.0);
        let labels =
            retrieval_strings(request, "labels").or_else(|| retrieval_strings(request, "types"));
        let filters = retrieval_filters(request);
        let embedding = request
            .get("embedding")
            .or_else(|| request.get("queryEmbedding"));
        let embedding = match embedding {
            Some(embedding) => retrieval_embedding(embedding, strict).map(|embedding| {
                if embedding.is_empty() {
                    None
                } else {
                    Some(embedding)
                }
            })?,
            None if strict => {
                return Err(EvalError::ExecutionError(
                    "db.retrieve strictPolicy requires a valid embedding".to_string(),
                ));
            }
            None => None,
        };

        let catalog = IndexCatalog::new(self.storage.as_ref());
        let indexes = catalog.list()?;
        let mut hits: BTreeMap<String, (NodeRecord, usize, usize, f64)> = BTreeMap::new();
        for index in indexes {
            if index.entity_type != copperdb_indexing::CatalogIndexEntityType::Node {
                continue;
            }
            match index.kind {
                copperdb_indexing::CatalogIndexKind::FullText => {
                    for (position, (node, score)) in self
                        .storage
                        .search_fulltext_nodes_by_properties_with_cancellation(
                            &index.label,
                            &index.properties,
                            query,
                            limit,
                            request_context.cancellation(),
                        )?
                        .into_iter()
                        .enumerate()
                    {
                        let rank = position + 1;
                        if !retrieval_node_matches(&node, labels.as_deref(), &filters) {
                            continue;
                        }
                        hits.entry(node.id.clone())
                            .and_modify(|entry| {
                                entry.2 = entry.2.max(rank);
                                entry.3 = entry.3.max(score);
                            })
                            .or_insert((node, 0, rank, score));
                    }
                }
                copperdb_indexing::CatalogIndexKind::Vector => {
                    let Some(embedding) = embedding.as_ref() else {
                        continue;
                    };
                    let (matches, _) = (self.vector_index_query)(
                        request_context.cancellation(),
                        &index.name,
                        embedding,
                        limit,
                    )
                    .map_err(|error| EvalError::ExecutionError(error.to_string()))?;
                    for (position, (id, score)) in matches.into_iter().enumerate() {
                        let Some(node) = self.storage.get_node_record(&id)? else {
                            continue;
                        };
                        if !retrieval_node_matches(&node, labels.as_deref(), &filters) {
                            continue;
                        }
                        let rank = position + 1;
                        hits.entry(id)
                            .and_modify(|entry| {
                                entry.1 = entry.1.max(rank);
                                entry.3 = entry.3.max(score as f64);
                            })
                            .or_insert((node, rank, 0, score as f64));
                    }
                }
                _ => {}
            }
        }
        let mut hits = hits.into_values().collect::<Vec<_>>();
        hits.sort_by(|left, right| {
            let left_score = retrieval_rrf(left.1, left.2, rrf_k, vector_weight, bm25_weight);
            let right_score = retrieval_rrf(right.1, right.2, rrf_k, vector_weight, bm25_weight);
            right_score
                .total_cmp(&left_score)
                .then_with(|| left.0.id.cmp(&right.0.id))
        });
        hits.truncate(limit);
        let rows = hits
            .into_iter()
            .filter(|(_, vector_rank, bm25_rank, _)| {
                retrieval_rrf(*vector_rank, *bm25_rank, rrf_k, vector_weight, bm25_weight)
                    >= min_rrf_score
            })
            .map(|(node, vector_rank, bm25_rank, score)| {
                let rrf_score =
                    retrieval_rrf(vector_rank, bm25_rank, rrf_k, vector_weight, bm25_weight);
                let mut row = Row::new();
                row.insert(
                    "node".to_string(),
                    Value::Object(node_record_to_props(&node).into_iter().collect()),
                );
                row.insert("score".to_string(), Value::from(score));
                row.insert("rrf_score".to_string(), Value::from(rrf_score));
                row.insert("vector_rank".to_string(), Value::from(vector_rank));
                row.insert("bm25_rank".to_string(), Value::from(bm25_rank));
                row.insert(
                    "search_method".to_string(),
                    Value::String(
                        match (vector_rank > 0, bm25_rank > 0) {
                            (true, true) => "rrf",
                            (true, false) => "vector_only",
                            (false, true) => "bm25_only",
                            (false, false) => "fallback",
                        }
                        .to_string(),
                    ),
                );
                row.insert("fallback_triggered".to_string(), Value::Bool(false));
                row
            })
            .collect();
        Ok(EvalResult {
            columns: vec![
                "node".to_string(),
                "score".to_string(),
                "rrf_score".to_string(),
                "vector_rank".to_string(),
                "bm25_rank".to_string(),
                "search_method".to_string(),
                "fallback_triggered".to_string(),
            ],
            rows,
            stats: QueryStats::default(),
        })
    }

    fn execute_fulltext_query_nodes_call(
        &self,
        request_context: &copperdb_util::RequestContext,
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
        let query = self.parse_fulltext_query_cached(&query_text)?;

        let indexes = resolve_fulltext_node_indexes(self.storage.as_ref(), &index_name)?;

        let fetch_limit = usize::MAX;

        let mut merged: HashMap<String, (NodeRecord, f64)> = HashMap::new();
        for index in indexes {
            let candidate_terms = match query.primary_terms() {
                Some(terms) => terms,
                None => {
                    let vocabulary = self.storage.fulltext_node_vocabulary_with_cancellation(
                        &index.label,
                        &index.properties,
                        MAX_FULLTEXT_VOCABULARY_TERMS,
                        MAX_FULLTEXT_VOCABULARY_ENTRIES,
                        request_context.cancellation(),
                    )?;
                    if vocabulary.truncated {
                        return Err(EvalError::ExecutionError(
                            "Lucene query vocabulary expansion exceeded the configured limit"
                                .to_string(),
                        ));
                    }
                    query
                        .expand_candidate_terms(&vocabulary.terms)
                        .map_err(|error| EvalError::ExecutionError(error.to_string()))?
                }
            };
            if candidate_terms.is_empty() {
                continue;
            }
            let candidate_query = candidate_terms.join(" ");
            for (position, (node, score)) in self
                .storage
                .search_fulltext_nodes_by_properties_with_cancellation(
                    &index.label,
                    &index.properties,
                    &candidate_query,
                    fetch_limit,
                    request_context.cancellation(),
                )?
                .into_iter()
                .enumerate()
            {
                if position % 256 == 0 {
                    request_context.check_active()?;
                }
                let document = copperdb_search::lucene::FulltextDocument::from_fields(
                    index.properties.iter().filter_map(|property| {
                        node.properties.get(property).map(|value| {
                            let text = match value {
                                Value::String(value) => value.clone(),
                                other => other.to_string(),
                            };
                            (property.clone(), text)
                        })
                    }),
                );
                if copperdb_search::lucene::evaluate_fulltext_query(&query, &document)
                    .map_err(|error| EvalError::ExecutionError(error.to_string()))?
                    .is_none()
                {
                    continue;
                }
                merged
                    .entry(node.id.clone())
                    .and_modify(|(_, existing_score)| {
                        *existing_score += score;
                    })
                    .or_insert((node, score));
            }
        }

        request_context.check_active()?;
        let mut ranked: Vec<(NodeRecord, f64)> = merged.into_values().collect();
        ranked.sort_by(|(left_node, left_score), (right_node, right_score)| {
            right_score
                .total_cmp(left_score)
                .then(left_node.id.cmp(&right_node.id))
        });
        request_context.check_active()?;

        if options.skip > 0 {
            ranked = ranked.into_iter().skip(options.skip).collect();
        }
        if let Some(limit) = options.limit {
            ranked.truncate(limit);
        }

        let rows = ranked
            .into_iter()
            .enumerate()
            .map(|(position, (node, score))| {
                if position % 256 == 0 {
                    request_context.check_active()?;
                }
                let mut row = Row::new();
                row.insert(
                    "node".to_string(),
                    Value::Object(node_record_to_props(&node).into_iter().collect()),
                );
                row.insert("score".to_string(), Value::from(score));
                Ok(row)
            })
            .collect::<Result<Vec<_>, EvalError>>()?;

        Ok(EvalResult {
            columns: vec!["node".to_string(), "score".to_string()],
            rows,
            stats: QueryStats::default(),
        })
    }

    fn execute_fulltext_query_relationships_call(
        &self,
        request_context: &copperdb_util::RequestContext,
        call: &copperdb_cypher::CallClause,
        params: &HashMap<String, Value>,
    ) -> Result<EvalResult, EvalError> {
        if !(call.args.len() == 2 || call.args.len() == 3) {
            return Err(EvalError::ExecutionError(
                "db.index.fulltext.queryRelationships expects 2 or 3 arguments: indexName, queryString, optionsMap"
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

        let catalog = IndexCatalog::new(self.storage.as_ref());
        let all_indexes = catalog.list()?;
        let rel_indexes: Vec<_> = all_indexes
            .into_iter()
            .filter(|idx| {
                idx.entity_type == copperdb_indexing::CatalogIndexEntityType::Relationship
                    && idx.kind == copperdb_indexing::CatalogIndexKind::FullText
                    && idx.name == index_name
            })
            .collect();

        if rel_indexes.is_empty() {
            return Err(EvalError::ExecutionError(format!(
                "fulltext relationship index not found: {index_name}"
            )));
        }

        let query = self.parse_fulltext_query_cached(&query_text)?;
        let mut results: HashMap<String, (EdgeRecord, f64)> = HashMap::new();

        for index in &rel_indexes {
            let candidate_terms = match query.primary_terms() {
                Some(terms) => terms,
                None => {
                    let vocabulary = self
                        .storage
                        .fulltext_relationship_vocabulary_with_cancellation(
                            &index.label,
                            &index.properties,
                            MAX_FULLTEXT_VOCABULARY_TERMS,
                            MAX_FULLTEXT_VOCABULARY_ENTRIES,
                            request_context.cancellation(),
                        )?;
                    if vocabulary.truncated {
                        return Err(EvalError::ExecutionError(
                            "Lucene query vocabulary expansion exceeded the configured limit"
                                .to_string(),
                        ));
                    }
                    query
                        .expand_candidate_terms(&vocabulary.terms)
                        .map_err(|error| EvalError::ExecutionError(error.to_string()))?
                }
            };
            if candidate_terms.is_empty() {
                continue;
            }
            for (position, edge) in self
                .storage
                .search_fulltext_relationships_by_properties_with_cancellation(
                    &index.label,
                    &index.properties,
                    &candidate_terms,
                    request_context.cancellation(),
                )?
                .into_iter()
                .enumerate()
            {
                if position % 256 == 0 {
                    request_context.check_active()?;
                }
                let document = copperdb_search::lucene::FulltextDocument::from_fields(
                    index.properties.iter().filter_map(|property| {
                        edge.properties.get(property).map(|value| {
                            let text = match value {
                                Value::String(value) => value.clone(),
                                other => other.to_string(),
                            };
                            (property.clone(), text)
                        })
                    }),
                );
                let score = copperdb_search::lucene::evaluate_fulltext_query(&query, &document)
                    .map_err(|error| EvalError::ExecutionError(error.to_string()))?;
                if let Some(score) = score {
                    results
                        .entry(edge.id.clone())
                        .and_modify(|(_, existing_score)| {
                            *existing_score = existing_score.max(score)
                        })
                        .or_insert((edge, score));
                }
            }
        }

        request_context.check_active()?;
        let mut results: Vec<(EdgeRecord, f64)> = results.into_values().collect();
        results
            .sort_by(|(a, a_score), (b, b_score)| b_score.total_cmp(a_score).then(a.id.cmp(&b.id)));
        request_context.check_active()?;

        if options.skip > 0 {
            results = results.into_iter().skip(options.skip).collect();
        }
        if let Some(limit) = options.limit {
            results.truncate(limit);
        }

        let rows = results
            .into_iter()
            .enumerate()
            .map(|(position, (edge, score))| {
                if position % 256 == 0 {
                    request_context.check_active()?;
                }
                let mut row = Row::new();
                let mut props: HashMap<String, Value> =
                    edge.properties.clone().into_iter().collect();
                props.insert("_id".to_string(), Value::String(edge.id.clone()));
                props.insert("_type".to_string(), Value::String(edge.edge_type.clone()));
                row.insert(
                    "relationship".to_string(),
                    Value::Object(props.into_iter().collect()),
                );
                row.insert("score".to_string(), Value::from(score));
                Ok(row)
            })
            .collect::<Result<Vec<_>, EvalError>>()?;

        Ok(EvalResult {
            columns: vec!["relationship".to_string(), "score".to_string()],
            rows,
            stats: QueryStats::default(),
        })
    }

    fn execute_vector_query_relationships_call(
        &self,
        request_context: &copperdb_util::RequestContext,
        call: &copperdb_cypher::CallClause,
        params: &HashMap<String, Value>,
    ) -> Result<EvalResult, EvalError> {
        if call.args.len() != 3 {
            return Err(EvalError::ExecutionError(
                "db.index.vector.queryRelationships expects 3 arguments: indexName, limit, queryVector"
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
        if index.entity_type != copperdb_indexing::CatalogIndexEntityType::Relationship {
            return Err(EvalError::ExecutionError(format!(
                "db.index.vector.queryRelationships only supports relationship indexes: {index_name}"
            )));
        }

        let (matches, _) = (self.vector_index_query)(
            request_context.cancellation(),
            &index_name,
            &query_vector,
            limit,
        )
        .map_err(|error| EvalError::ExecutionError(error.to_string()))?;
        let mut rows = Vec::with_capacity(matches.len());
        for (id, score) in matches {
            let Some(edge) = self.storage.get_edge_record(&id)? else {
                continue;
            };
            let mut row = Row::new();
            let mut props: HashMap<String, Value> = edge.properties.clone().into_iter().collect();
            props.insert("_id".to_string(), Value::String(edge.id));
            props.insert("_type".to_string(), Value::String(edge.edge_type));
            row.insert(
                "relationship".to_string(),
                Value::Object(props.into_iter().collect()),
            );
            row.insert("score".to_string(), Value::from(score as f64));
            rows.push(row);
        }

        Ok(EvalResult {
            columns: vec!["relationship".to_string(), "score".to_string()],
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

    pub fn node_visible_with_resolver_and_access_metadata(
        &self,
        resolver: &Resolver,
        node: &NodeRecord,
        access_metadata: Option<KnowledgePolicyAccessMetadata>,
        params: &HashMap<String, Value>,
    ) -> Result<bool, EvalError> {
        Ok(!self
            .inspect_node_policy_with_access_metadata(
                resolver,
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

    fn execute_set_node_vector_property(
        &self,
        call: &copperdb_cypher::CallClause,
        params: &HashMap<String, Value>,
        rows: &[Row],
    ) -> Result<EvalResult, EvalError> {
        if call.args.len() < 3 {
            return Err(EvalError::ExecutionError(
                "db.create.setNodeVectorProperty requires 3 arguments: node, propertyName, vector"
                    .into(),
            ));
        }
        for row in rows {
            let prop_name = call_arg_string(
                &eval_expression(&call.args[1], row, params)?,
                "propertyName",
            )?;
            let vector_val = eval_expression(&call.args[2], row, params)?;
            if let Value::Object(props) = eval_expression(&call.args[0], row, params)? {
                let mut persisted: HashMap<String, Value> = props.clone().into_iter().collect();
                persisted.insert(prop_name, vector_val);
                self.persist_node_props(&persisted)?;
            }
        }
        Ok(EvalResult {
            columns: vec![],
            rows: rows.to_vec(),
            stats: QueryStats::default(),
        })
    }

    fn execute_set_relationship_vector_property(
        &self,
        call: &copperdb_cypher::CallClause,
        params: &HashMap<String, Value>,
        rows: &[Row],
    ) -> Result<EvalResult, EvalError> {
        if call.args.len() < 3 {
            return Err(EvalError::ExecutionError(
                "db.create.setRelationshipVectorProperty requires 3 arguments: rel, propertyName, vector".into(),
            ));
        }
        for row in rows {
            let prop_name = call_arg_string(
                &eval_expression(&call.args[1], row, params)?,
                "propertyName",
            )?;
            let vector_val = eval_expression(&call.args[2], row, params)?;
            if let Value::Object(props) = eval_expression(&call.args[0], row, params)? {
                let mut persisted: HashMap<String, Value> = props.clone().into_iter().collect();
                persisted.insert(prop_name, vector_val);
                self.persist_edge_props(&persisted)?;
            }
        }
        Ok(EvalResult {
            columns: vec![],
            rows: rows.to_vec(),
            stats: QueryStats::default(),
        })
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

fn retrieval_bool(
    request: &serde_json::Map<String, Value>,
    camel_case: &str,
    snake_case: &str,
) -> Result<Option<bool>, EvalError> {
    request
        .get(camel_case)
        .or_else(|| request.get(snake_case))
        .map(|value| {
            value.as_bool().ok_or_else(|| {
                EvalError::ExecutionError(format!("db.retrieve {camel_case} must be a boolean"))
            })
        })
        .transpose()
}

fn retrieval_embedding(value: &Value, strict: bool) -> Result<Vec<f32>, EvalError> {
    let vector = value.as_array().and_then(|values| {
        values
            .iter()
            .map(|value| value.as_f64())
            .collect::<Option<Vec<_>>>()
    });
    let valid = vector.filter(|values| {
        !values.is_empty()
            && values.iter().all(|value| {
                value.is_finite() && *value >= f32::MIN as f64 && *value <= f32::MAX as f64
            })
    });
    match valid {
        Some(values) => Ok(values.into_iter().map(|value| value as f32).collect()),
        None if strict => Err(EvalError::ExecutionError(
            "db.retrieve strictPolicy requires a non-empty finite numeric embedding".to_string(),
        )),
        None => Ok(Vec::new()),
    }
}

fn retrieval_limit(
    request: &serde_json::Map<String, Value>,
    strict: bool,
) -> Result<usize, EvalError> {
    match request
        .get("limit")
        .and_then(Value::as_u64)
        .and_then(|value| usize::try_from(value).ok())
    {
        Some(value) if (1..=10_000).contains(&value) => Ok(value),
        Some(_) if strict => Err(EvalError::ExecutionError(
            "db.retrieve strictPolicy limit must be an integer from 1 to 10000".to_string(),
        )),
        Some(_) => Ok(10),
        None if request.contains_key("limit") && strict => Err(EvalError::ExecutionError(
            "db.retrieve strictPolicy limit must be an integer from 1 to 10000".to_string(),
        )),
        None => Ok(10),
    }
}

fn retrieval_number(
    request: &serde_json::Map<String, Value>,
    camel_case: &str,
    snake_case: &str,
    default: f64,
) -> f64 {
    request
        .get(camel_case)
        .or_else(|| request.get(snake_case))
        .and_then(Value::as_f64)
        .filter(|value| value.is_finite())
        .unwrap_or(default)
}

fn retrieval_strings(request: &serde_json::Map<String, Value>, key: &str) -> Option<Vec<String>> {
    request.get(key).and_then(Value::as_array).map(|values| {
        values
            .iter()
            .filter_map(Value::as_str)
            .map(str::to_string)
            .collect()
    })
}

fn retrieval_filters(request: &serde_json::Map<String, Value>) -> BTreeMap<String, Vec<String>> {
    request
        .get("filters")
        .or_else(|| request.get("propertyFilters"))
        .or_else(|| request.get("property_filters"))
        .and_then(Value::as_object)
        .map(|filters| {
            filters
                .iter()
                .map(|(property, values)| {
                    let values = values
                        .as_array()
                        .map(|values| values.iter().map(retrieval_filter_value).collect())
                        .unwrap_or_else(|| vec![retrieval_filter_value(values)]);
                    (property.clone(), values)
                })
                .collect()
        })
        .unwrap_or_default()
}

fn retrieval_node_matches(
    node: &NodeRecord,
    labels: Option<&[String]>,
    filters: &BTreeMap<String, Vec<String>>,
) -> bool {
    labels.is_none_or(|labels| {
        labels.is_empty()
            || labels.iter().any(|label| {
                node.labels
                    .iter()
                    .any(|candidate| candidate.eq_ignore_ascii_case(label))
            })
    }) && filters.iter().all(|(property, values)| {
        node.properties
            .get(property)
            .is_some_and(|value| match value {
                Value::String(actual) => values.iter().any(|expected| actual == expected),
                value => {
                    let actual = value.to_string();
                    values.contains(&actual)
                }
            })
    })
}

fn retrieval_filter_value(value: &Value) -> String {
    value
        .as_str()
        .map(str::to_string)
        .unwrap_or_else(|| value.to_string())
}

fn validate_retrieval_options(
    request: &serde_json::Map<String, Value>,
    strict: bool,
) -> Result<(), EvalError> {
    for (camel_case, snake_case, minimum, maximum, exclusive_minimum) in [
        ("rrfK", "rrf_k", 0.0, 10_000.0, true),
        ("vectorWeight", "vector_weight", 0.0, 100.0, true),
        ("bm25Weight", "bm25_weight", 0.0, 100.0, true),
        ("minRRFScore", "min_rrf_score", 0.0, 1.0, false),
        ("minSimilarity", "min_similarity", -1.0, 1.0, false),
        (
            "initialOverfetchRatio",
            "initial_overfetch_ratio",
            1.0,
            100.0,
            false,
        ),
        (
            "maxOverfetchRatio",
            "max_overfetch_ratio",
            1.0,
            100.0,
            false,
        ),
        (
            "overfetchGrowthFactor",
            "overfetch_growth_factor",
            1.0,
            100.0,
            true,
        ),
        (
            "rerankMinScore",
            "rerank_min_score",
            f64::NEG_INFINITY,
            f64::INFINITY,
            false,
        ),
    ] {
        if let Some(value) = request.get(camel_case).or_else(|| request.get(snake_case)) {
            let valid = value.as_f64().is_some_and(|value| {
                value.is_finite()
                    && if exclusive_minimum {
                        value > minimum
                    } else {
                        value >= minimum
                    }
                    && value <= maximum
            });
            if strict && !valid {
                return Err(EvalError::ExecutionError(format!(
                    "db.retrieve strictPolicy {camel_case} is invalid"
                )));
            }
        }
    }
    if strict {
        for (camel_case, snake_case) in [
            ("adaptiveOverfetch", "adaptive_overfetch"),
            ("fallbackEnabled", "fallback_enabled"),
        ] {
            let _ = retrieval_bool(request, camel_case, snake_case)?;
        }
        for (camel_case, snake_case) in [
            ("candidateTarget", "candidate_target"),
            ("maxCandidateLimit", "max_candidate_limit"),
            ("rerankTopK", "rerank_top_k"),
        ] {
            if let Some(value) = request.get(camel_case).or_else(|| request.get(snake_case))
                && !value
                    .as_u64()
                    .is_some_and(|value| (1..=10_000).contains(&value))
            {
                return Err(EvalError::ExecutionError(format!(
                    "db.retrieve strictPolicy {camel_case} is invalid"
                )));
            }
        }
    }
    Ok(())
}

fn retrieval_rrf(
    vector_rank: usize,
    bm25_rank: usize,
    k: f64,
    vector_weight: f64,
    bm25_weight: f64,
) -> f64 {
    let mut score = 0.0;
    if vector_rank > 0 {
        score += vector_weight / (k + vector_rank as f64);
    }
    if bm25_rank > 0 {
        score += bm25_weight / (k + bm25_rank as f64);
    }
    score
}

struct FulltextCallOptions {
    skip: usize,
    limit: Option<usize>,
}

#[cfg(test)]
pub(crate) fn legacy_builtin_procedure_rows() -> Vec<Row> {
    let mut procedures = vec![
        (
            "db.constraints",
            "db.constraints() :: (name :: STRING, type :: STRING, labelsOrTypes :: LIST<STRING>, properties :: LIST<STRING>, propertyType :: STRING)",
            "Lists all constraints in the database",
            "READ",
        ),
        (
            "db.index.fulltext.listAvailableAnalyzers",
            "db.index.fulltext.listAvailableAnalyzers() :: (analyzer :: STRING, description :: STRING)",
            "Lists available fulltext analyzers",
            "READ",
        ),
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
            "db.index.fulltext.queryRelationships",
            "db.index.fulltext.queryRelationships(indexName :: STRING, query :: STRING, options = {} :: MAP) :: (relationship :: RELATIONSHIP, score :: FLOAT)",
            "Fulltext search on relationships",
            "READ",
        ),
        (
            "db.index.vector.queryRelationships",
            "db.index.vector.queryRelationships(indexName :: STRING, numberOfResults :: INTEGER, query :: LIST<FLOAT>|STRING|$param) :: (relationship :: RELATIONSHIP, score :: FLOAT)",
            "Vector search on relationships",
            "READ",
        ),
        (
            "db.indexes",
            "db.indexes() :: (name :: STRING, type :: STRING, labelsOrTypes :: LIST<STRING>, properties :: LIST<STRING>, state :: STRING)",
            "Lists all indexes in the database",
            "READ",
        ),
        (
            "db.info",
            "db.info() :: (id :: STRING, name :: STRING, creationDate :: STRING, nodeCount :: INTEGER, relationshipCount :: INTEGER)",
            "Returns database information",
            "READ",
        ),
        (
            "db.labels",
            "db.labels() :: (label :: STRING)",
            "Lists all labels in the database",
            "READ",
        ),
        (
            "db.ping",
            "db.ping() :: (success :: BOOLEAN)",
            "Checks database connectivity",
            "READ",
        ),
        (
            "db.propertyKeys",
            "db.propertyKeys() :: (propertyKey :: STRING)",
            "Lists all property keys in the database",
            "READ",
        ),
        (
            "db.relationshipTypes",
            "db.relationshipTypes() :: (relationshipType :: STRING)",
            "Lists all relationship types in the database",
            "READ",
        ),
        (
            "db.schema.nodeProperties",
            "db.schema.nodeProperties() :: (nodeLabel :: STRING, propertyName :: STRING, propertyType :: STRING)",
            "Returns node properties by label",
            "READ",
        ),
        (
            "db.schema.relProperties",
            "db.schema.relProperties() :: (relType :: STRING, propertyName :: STRING, propertyType :: STRING)",
            "Returns relationship properties by type",
            "READ",
        ),
        (
            "db.schema.visualization",
            "db.schema.visualization() :: (nodes :: LIST<MAP>, relationships :: LIST<MAP>)",
            "Visualizes schema",
            "READ",
        ),
        (
            "dbms.clientConfig",
            "dbms.clientConfig() :: (name :: STRING, value :: ANY)",
            "Returns client configuration",
            "DBMS",
        ),
        (
            "dbms.components",
            "dbms.components() :: (name :: STRING, versions :: LIST<STRING>, edition :: STRING)",
            "Lists DBMS components",
            "DBMS",
        ),
        (
            "dbms.functions",
            "dbms.functions() :: (name :: STRING, signature :: STRING, description :: STRING, category :: STRING, package :: STRING)",
            "Lists functions",
            "DBMS",
        ),
        (
            "dbms.procedures",
            "dbms.procedures() :: (name :: STRING, signature :: STRING, description :: STRING, mode :: STRING, package :: STRING)",
            "Lists procedures",
            "DBMS",
        ),
        (
            "dbms.info",
            "dbms.info() :: (id :: STRING, name :: STRING, creationDate :: STRING)",
            "Returns DBMS information",
            "DBMS",
        ),
        (
            "dbms.listConfig",
            "dbms.listConfig() :: (name :: STRING, description :: STRING, value :: ANY, dynamic :: BOOLEAN)",
            "Lists DBMS configuration",
            "DBMS",
        ),
        (
            "dbms.listConnections",
            "dbms.listConnections() :: (connectionId :: STRING, connectTime :: STRING, connector :: STRING, username :: STRING, userAgent :: STRING, clientAddress :: STRING)",
            "Lists active DBMS connections",
            "DBMS",
        ),
        (
            "nornicdb.decay.info",
            "nornicdb.decay.info() :: (enabled :: BOOLEAN, system :: STRING, configuredVia :: STRING)",
            "Returns knowledge-layer scoring configuration",
            "READ",
        ),
        (
            "nornicdb.knowledgepolicy.info",
            "nornicdb.knowledgepolicy.info() :: (enabled :: BOOLEAN, system :: STRING, decayProfiles :: INTEGER, decayBindings :: INTEGER, promotionProfiles :: INTEGER, promotionPolicies :: INTEGER, configuredVia :: STRING)",
            "Returns knowledge-layer profile and policy catalog counts",
            "READ",
        ),
        (
            "nornicdb.knowledgepolicy.resolve",
            "nornicdb.knowledgepolicy.resolve(entityId :: STRING = '', labelsCsv :: STRING = '', edgeType :: STRING = '') :: (entityId :: STRING, targetKind :: STRING, targetLabels :: STRING, targetEdgeType :: STRING, decayBinding :: STRING, promotionPolicy :: STRING, matchedPromotionProfile :: STRING, matchedPromotionPredicate :: STRING, scoreFrom :: STRING, anchorUnixMs :: INTEGER, accessCount :: INTEGER, lastAccessedAtUnixMs :: INTEGER, baseScore :: FLOAT, finalScore :: FLOAT, visibilityThreshold :: FLOAT, suppressed :: BOOLEAN, dryRun :: BOOLEAN, explanation :: STRING)",
            "Resolves the effective knowledge-layer scoring policy for an entity, label set, or edge type",
            "READ",
        ),
        (
            "nornicdb.knowledgepolicy.policies",
            "nornicdb.knowledgepolicy.policies() :: (kind :: STRING, Name :: STRING, Scope :: STRING, Multiplier :: FLOAT, ScoreFloor :: FLOAT, ScoreCap :: FLOAT, Enabled :: BOOLEAN, TargetLabels :: LIST<STRING>, TargetEdgeType :: STRING, IsWildcard :: BOOLEAN, IsEdge :: BOOLEAN)",
            "Returns knowledge-layer promotion profiles and policies",
            "READ",
        ),
        (
            "nornicdb.knowledgepolicy.profiles",
            "nornicdb.knowledgepolicy.profiles() :: (kind :: STRING, Name :: STRING, HalfLifeSeconds :: INTEGER, VisibilityThreshold :: FLOAT, ScoreFloor :: FLOAT, Function :: STRING, Scope :: STRING, DecayEnabled :: BOOLEAN, ScoreFrom :: STRING, ScoreFromProperty :: STRING, Enabled :: BOOLEAN, TargetLabels :: LIST<STRING>, TargetEdgeType :: STRING, IsWildcard :: BOOLEAN, IsEdge :: BOOLEAN, ProfileRef :: STRING, NoDecay :: BOOLEAN, Order :: INTEGER)",
            "Returns knowledge-layer decay bundles and bindings",
            "READ",
        ),
        (
            "nornicdb.stats",
            "nornicdb.stats() :: (nodes :: INTEGER, relationships :: INTEGER, labels :: INTEGER, relationshipTypes :: INTEGER)",
            "Returns NornicDB stats",
            "READ",
        ),
        (
            "nornicdb.version",
            "nornicdb.version() :: (version :: STRING, build :: STRING, edition :: STRING)",
            "Returns NornicDB version",
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

#[cfg(test)]
pub(crate) fn builtin_function_rows() -> Vec<Row> {
    let mut functions = vec![
        (
            "abs",
            "abs(input :: NUMBER) :: NUMBER",
            "Returns the absolute value of a number",
            "Numeric",
        ),
        (
            "avg",
            "avg(input :: NUMBER) :: NUMBER",
            "Returns the average of numeric values",
            "Aggregating",
        ),
        (
            "stdev",
            "stdev(input :: NUMBER) :: FLOAT",
            "Returns the sample standard deviation of numeric values",
            "Aggregating",
        ),
        (
            "stdevp",
            "stdevp(input :: NUMBER) :: FLOAT",
            "Returns the population standard deviation of numeric values",
            "Aggregating",
        ),
        (
            "ceil",
            "ceil(input :: NUMBER) :: NUMBER",
            "Returns the smallest integer greater than or equal to the input",
            "Numeric",
        ),
        (
            "coalesce",
            "coalesce(input :: ANY...) :: ANY",
            "Returns the first non-null value in the list",
            "Scalar",
        ),
        (
            "collect",
            "collect(input :: ANY) :: LIST<ANY>",
            "Collects values into a list",
            "Aggregating",
        ),
        (
            "contains",
            "contains(input :: STRING, substring :: STRING) :: BOOLEAN",
            "Returns whether the string contains the substring",
            "String",
        ),
        (
            "count",
            "count(input :: ANY) :: INTEGER",
            "Returns the number of values or rows",
            "Aggregating",
        ),
        (
            "date",
            "date() :: STRING",
            "Returns the current date",
            "Temporal",
        ),
        (
            "datetime",
            "datetime() :: STRING",
            "Returns the current datetime",
            "Temporal",
        ),
        (
            "duration",
            "duration() :: STRING",
            "Returns the current duration since epoch",
            "Temporal",
        ),
        (
            "elementId",
            "elementId(input :: NODE|RELATIONSHIP) :: STRING",
            "Returns the element id of a node or relationship",
            "Scalar",
        ),
        (
            "endsWith",
            "endsWith(input :: STRING, substring :: STRING) :: BOOLEAN",
            "Returns whether the string ends with the substring",
            "String",
        ),
        (
            "exists",
            "exists(input :: ANY) :: BOOLEAN",
            "Returns whether the value is not null",
            "Scalar",
        ),
        (
            "floor",
            "floor(input :: NUMBER) :: NUMBER",
            "Returns the largest integer less than or equal to the input",
            "Numeric",
        ),
        (
            "head",
            "head(input :: LIST<ANY>) :: ANY",
            "Returns the first element of a list",
            "List",
        ),
        (
            "id",
            "id(input :: NODE|RELATIONSHIP) :: STRING",
            "Returns the internal id of a node or relationship",
            "Scalar",
        ),
        (
            "keys",
            "keys(input :: NODE|RELATIONSHIP|MAP) :: LIST<STRING>",
            "Returns the property keys of a node, relationship, or map",
            "Scalar",
        ),
        (
            "labels",
            "labels(input :: NODE) :: LIST<STRING>",
            "Returns the labels of a node",
            "Scalar",
        ),
        (
            "last",
            "last(input :: LIST<ANY>) :: ANY",
            "Returns the last element of a list",
            "List",
        ),
        (
            "left",
            "left(input :: STRING, length :: INTEGER) :: STRING",
            "Returns the leftmost characters of a string",
            "String",
        ),
        (
            "length",
            "length(input :: PATH) :: INTEGER",
            "Returns the length of a path",
            "Scalar",
        ),
        (
            "ltrim",
            "ltrim(input :: STRING) :: STRING",
            "Returns the string with leading whitespace removed",
            "String",
        ),
        (
            "max",
            "max(input :: NUMBER) :: NUMBER",
            "Returns the maximum of numeric values",
            "Aggregating",
        ),
        (
            "min",
            "min(input :: NUMBER) :: NUMBER",
            "Returns the minimum of numeric values",
            "Aggregating",
        ),
        (
            "nodes",
            "nodes(input :: PATH) :: LIST<NODE>",
            "Returns the nodes in a path",
            "Scalar",
        ),
        (
            "now",
            "now() :: INTEGER",
            "Returns the current timestamp in milliseconds",
            "Temporal",
        ),
        (
            "properties",
            "properties(input :: NODE|RELATIONSHIP|MAP) :: MAP",
            "Returns the properties of a node, relationship, or map",
            "Scalar",
        ),
        (
            "range",
            "range(start :: INTEGER, end :: INTEGER [, step :: INTEGER]) :: LIST<INTEGER>",
            "Creates a list of integers in the given range",
            "List",
        ),
        (
            "relationships",
            "relationships(input :: PATH) :: LIST<RELATIONSHIP>",
            "Returns the relationships in a path",
            "Scalar",
        ),
        (
            "replace",
            "replace(input :: STRING, from :: STRING, to :: STRING) :: STRING",
            "Replaces all occurrences of a substring",
            "String",
        ),
        (
            "reverse",
            "reverse(input :: LIST<ANY>) :: LIST<ANY>",
            "Returns the list in reverse order",
            "List",
        ),
        (
            "right",
            "right(input :: STRING, length :: INTEGER) :: STRING",
            "Returns the rightmost characters of a string",
            "String",
        ),
        (
            "round",
            "round(input :: NUMBER) :: NUMBER",
            "Returns the nearest integer to the input",
            "Numeric",
        ),
        (
            "rtrim",
            "rtrim(input :: STRING) :: STRING",
            "Returns the string with trailing whitespace removed",
            "String",
        ),
        (
            "size",
            "size(input :: LIST<ANY>|STRING) :: INTEGER",
            "Returns the size of a list or string",
            "List",
        ),
        (
            "split",
            "split(input :: STRING, delimiter :: STRING) :: LIST<STRING>",
            "Splits a string by the delimiter",
            "String",
        ),
        (
            "startsWith",
            "startsWith(input :: STRING, substring :: STRING) :: BOOLEAN",
            "Returns whether the string starts with the substring",
            "String",
        ),
        (
            "substring",
            "substring(input :: STRING, start :: INTEGER [, length :: INTEGER]) :: STRING",
            "Returns a substring of the input",
            "String",
        ),
        (
            "sum",
            "sum(input :: NUMBER) :: NUMBER",
            "Returns the sum of numeric values",
            "Aggregating",
        ),
        (
            "tail",
            "tail(input :: LIST<ANY>) :: LIST<ANY>",
            "Returns the list without the first element",
            "List",
        ),
        (
            "timestamp",
            "timestamp() :: INTEGER",
            "Returns the current timestamp in milliseconds",
            "Temporal",
        ),
        (
            "toBoolean",
            "toBoolean(input :: ANY) :: BOOLEAN",
            "Converts a value to boolean",
            "Scalar",
        ),
        (
            "toFloat",
            "toFloat(input :: ANY) :: FLOAT",
            "Converts a value to float",
            "Scalar",
        ),
        (
            "toInteger",
            "toInteger(input :: ANY) :: INTEGER",
            "Converts a value to integer",
            "Scalar",
        ),
        (
            "toLower",
            "toLower(input :: STRING) :: STRING",
            "Returns the string in lowercase",
            "String",
        ),
        (
            "toString",
            "toString(input :: ANY) :: STRING",
            "Converts a value to string",
            "Scalar",
        ),
        (
            "toUpper",
            "toUpper(input :: STRING) :: STRING",
            "Returns the string in uppercase",
            "String",
        ),
        (
            "trim",
            "trim(input :: STRING) :: STRING",
            "Returns the string with leading and trailing whitespace removed",
            "String",
        ),
        (
            "type",
            "type(input :: RELATIONSHIP) :: STRING",
            "Returns the type of a relationship",
            "Scalar",
        ),
    ];
    functions.sort_by(|left, right| left.0.cmp(right.0));

    functions
        .into_iter()
        .map(|(name, signature, description, category)| {
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
            row.insert("category".to_string(), Value::String(category.to_string()));
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

    if index_name.eq_ignore_ascii_case("default") || index_name.eq_ignore_ascii_case("node_search")
    {
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

pub(super) fn vector_from_node(node: &NodeRecord, property: &str) -> Option<Vec<f32>> {
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

pub(super) fn edge_vector_for_property(edge: &EdgeRecord, property: &str) -> Option<Vec<f32>> {
    edge.properties.get(property).and_then(value_to_vector)
}

fn value_to_vector(value: &Value) -> Option<Vec<f32>> {
    value.as_array().and_then(|items| {
        items
            .iter()
            .map(|item| item.as_f64().map(|component| component as f32))
            .collect::<Option<Vec<_>>>()
    })
}

/// Resolve the expected vector dimensions from index options.
fn resolve_vector_dimensions(options: &Option<HashMap<String, serde_json::Value>>) -> Option<u64> {
    options.as_ref().and_then(|opts| {
        opts.get("indexConfig")
            .and_then(|cfg| cfg.as_object())
            .and_then(|cfg| cfg.get("vector.dimensions"))
            .and_then(|v| v.as_u64())
    })
}
