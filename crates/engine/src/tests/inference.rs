use super::*;
use copperdb_auth::{Privilege, Role, User, ROLE_ADMIN};
use copperdb_inference::{
    Evidence, HeimdallReviewState, InferenceError, Provenance, ProviderReview, ReviewProvider,
    Suggestion,
};
use copperdb_util::RequestContext;

struct ApprovingProvider;

impl ReviewProvider for ApprovingProvider {
    fn review(
        &self,
        _request_context: &RequestContext,
        suggestions: &[Suggestion],
    ) -> Result<Vec<ProviderReview>, InferenceError> {
        Ok(suggestions
            .iter()
            .map(|suggestion| ProviderReview {
                suggestion_id: suggestion.id.clone(),
                approved: true,
                reasoning: "approved by configured provider".into(),
                relationship_type_override: None,
                output_digest: "configured-provider-output".into(),
            })
            .collect())
    }
}

#[test]
fn engine_uses_resolved_default_off_materialization_switch() {
    let database = CopperDb::open_temporary().unwrap();
    for id in ["source", "target"] {
        database
            .storage()
            .put_node_record(&NodeRecord {
                id: id.into(),
                labels: vec!["Entity".into()],
                properties: BTreeMap::new(),
                named_embeddings: BTreeMap::new(),
                chunk_embeddings: Vec::new(),
                embed_meta: Default::default(),
                created_at_unix_ms: 1,
                updated_at_unix_ms: 1,
            })
            .unwrap();
    }
    let repository = database.suggestion_repository();
    let mut suggestion_id = String::new();
    for index in 0..3 {
        suggestion_id = repository
            .record_evidence(Evidence {
                id: format!("e{index}"),
                database: "copperdb".into(),
                source_id: "source".into(),
                target_id: "target".into(),
                relationship_type: "RELATES_TO".into(),
                signal: "similarity".into(),
                score: 0.8,
                session_id: format!("s{}", index.min(1)),
                request_id: None,
                observed_at_unix_ms: 1_000 + index,
                reason: "similarity".into(),
                provenance: Provenance {
                    algorithm: "similarity".into(),
                    algorithm_version: "1".into(),
                    input_digest: "input".into(),
                    ..Provenance::default()
                },
                metadata: BTreeMap::new(),
            })
            .unwrap()
            .id;
    }
    let user = User {
        id: "admin".into(),
        username: "admin".into(),
        email: None,
        roles: vec![Role {
            name: ROLE_ADMIN.into(),
            privileges: Privilege::ADMIN,
            databases: vec!["*".into()],
        }],
        metadata: HashMap::new(),
        disabled: false,
        created_at_unix_ms: 1,
        updated_at_unix_ms: 1,
        last_login_unix_ms: None,
    };

    assert!(matches!(
        database.materialize_suggestion(&suggestion_id, &user, None),
        Err(CopperDbError::Eval(message)) if message.contains("disabled")
    ));
    assert!(database.storage().all_edges().unwrap().is_empty());
}

#[test]
fn embedding_updates_drive_durable_fail_closed_suggestions() {
    let mut global = copperdb_config::Config::default();
    global.features.auto_links_enabled = true;
    let runtime_config = copperdb_config::resolve_per_database_config(&global, &BTreeMap::new())
        .expect("enabled inference config resolves");
    let directory = tempfile::tempdir().unwrap();
    let database = CopperDb::open(DatabaseConfig {
        data_dir: directory.path().join("db").to_string_lossy().into_owned(),
        runtime_config,
        ..DatabaseConfig::default()
    })
    .unwrap();
    database
        .execute(
            "CREATE VECTOR INDEX inference_embedding FOR (n:Entity) ON (n.embedding) OPTIONS {indexConfig: {`vector.dimensions`: 2}}",
            HashMap::new(),
        )
        .unwrap();
    let node = |id: &str| NodeRecord {
        id: id.into(),
        labels: vec!["Entity".into()],
        properties: BTreeMap::new(),
        named_embeddings: BTreeMap::new(),
        chunk_embeddings: vec![vec![1.0, 0.0]],
        embed_meta: Default::default(),
        created_at_unix_ms: 1,
        updated_at_unix_ms: 1,
    };
    database.storage().put_node_record(&node("source")).unwrap();
    database.storage().put_node_record(&node("target")).unwrap();

    for _ in 0..3 {
        database
            .storage()
            .update_node_embedding(&node("source"))
            .unwrap();
    }

    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
    loop {
        let evidence = database
            .storage()
            .get_nodes_by_label("_InferenceEvidence")
            .unwrap();
        if evidence.len() == 3 {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "inference events timed out"
        );
        std::thread::yield_now();
    }
    assert_eq!(
        database
            .storage()
            .get_nodes_by_label("_InferenceSuggestion")
            .unwrap()
            .len(),
        1
    );
    assert!(database.storage().all_edges().unwrap().is_empty());
}

#[test]
fn configured_heimdall_provider_durably_approves_production_suggestion() {
    let mut global = copperdb_config::Config::default();
    global.features.auto_links_enabled = true;
    let runtime_config = copperdb_config::resolve_per_database_config(&global, &BTreeMap::new())
        .expect("enabled inference config resolves");
    let directory = tempfile::tempdir().unwrap();
    let database = CopperDb::open_with_inference_provider(
        DatabaseConfig {
            data_dir: directory.path().join("db").to_string_lossy().into_owned(),
            runtime_config,
            ..DatabaseConfig::default()
        },
        Arc::new(ApprovingProvider),
    )
    .unwrap();
    database
        .execute(
            "CREATE VECTOR INDEX inference_embedding FOR (n:Entity) ON (n.embedding) OPTIONS {indexConfig: {`vector.dimensions`: 2}}",
            HashMap::new(),
        )
        .unwrap();
    let node = |id: &str| NodeRecord {
        id: id.into(),
        labels: vec!["Entity".into()],
        properties: BTreeMap::new(),
        named_embeddings: BTreeMap::new(),
        chunk_embeddings: vec![vec![1.0, 0.0]],
        embed_meta: Default::default(),
        created_at_unix_ms: 1,
        updated_at_unix_ms: 1,
    };
    database.storage().put_node_record(&node("source")).unwrap();
    database.storage().put_node_record(&node("target")).unwrap();
    for _ in 0..3 {
        database
            .storage()
            .update_node_embedding(&node("source"))
            .unwrap();
    }

    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
    let approved = loop {
        let records = database
            .storage()
            .get_nodes_by_label("_InferenceSuggestion")
            .unwrap();
        if let Some(payload) = records
            .first()
            .and_then(|record| record.properties.get("payload"))
        {
            let suggestion: Suggestion = serde_json::from_value(payload.clone()).unwrap();
            if suggestion.heimdall_review == HeimdallReviewState::Approved {
                break suggestion;
            }
        }
        assert!(
            std::time::Instant::now() < deadline,
            "Heimdall review timed out"
        );
        std::thread::yield_now();
    };
    assert_eq!(approved.evidence_count, 3);
    assert!(database.storage().all_edges().unwrap().is_empty());
}
