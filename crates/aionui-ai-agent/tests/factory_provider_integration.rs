use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use aion_agent::session::SessionManager;
use aion_types::message::ContentBlock;
use aionui_ai_agent::AcpSessionSyncService;
use aionui_ai_agent::AcpSkillManager;
use aionui_ai_agent::factory::{AgentFactoryDeps, build_agent_factory};
use aionui_ai_agent::registry::AgentRegistry;
use aionui_ai_agent::session_context::{
    AgentSessionContext, AgentSessionKind, AionrsSessionBuildContext, ConversationContext, WorkspaceContext,
};
use aionui_ai_agent::types::{BuildTaskOptions, SendMessageData};
use aionui_api_types::{AgentErrorCode, AionrsBuildExtra};
use aionui_common::{AgentType, ProviderWithModel, encrypt_string};
use aionui_db::{
    CreateProviderParams, IAcpSessionRepository, IProviderRepository, SqliteAcpSessionRepository,
    SqliteAgentMetadataRepository, SqliteProviderRepository, UpdateProviderParams, init_database_memory,
};
use aionui_realtime::BroadcastEventBus;
use axum::{Json, Router, routing::post};
use serde_json::{Value, json};

const TEST_USER_ID: &str = "system_default_user";

fn test_encryption_key() -> [u8; 32] {
    [0xABu8; 32]
}

async fn setup() -> (
    Arc<dyn IProviderRepository>,
    Arc<AgentRegistry>,
    Arc<AcpSessionSyncService>,
) {
    let db = init_database_memory().await.unwrap();
    let pool = db.pool().clone();
    let provider_repo: Arc<dyn IProviderRepository> = Arc::new(SqliteProviderRepository::new(pool.clone()));
    let metadata_repo = Arc::new(SqliteAgentMetadataRepository::new(pool.clone()));
    let registry = AgentRegistry::new(metadata_repo);
    registry.hydrate().await.unwrap();
    let session_repo: Arc<dyn IAcpSessionRepository> = Arc::new(SqliteAcpSessionRepository::new(pool));
    let acp_agent_service = AcpSessionSyncService::new(session_repo);
    (provider_repo, registry, acp_agent_service)
}

async fn insert_test_provider(repo: &dyn IProviderRepository, id: &str, platform: &str) {
    insert_provider(repo, id, platform, "https://api.example.com/v1").await;
}

async fn insert_provider(repo: &dyn IProviderRepository, id: &str, platform: &str, base_url: &str) {
    let key = test_encryption_key();
    let encrypted_api_key = encrypt_string("sk-test-key-12345", &key).unwrap();
    repo.create(CreateProviderParams {
        id: Some(id),
        user_id: TEST_USER_ID,
        platform,
        name: "Test Provider",
        base_url,
        api_key_encrypted: &encrypted_api_key,
        models: r#"["gpt-4o","gpt-5.4"]"#,
        enabled: true,
        capabilities: "[]",
        context_limit: None,
        model_protocols: None,
        model_enabled: None,
        model_health: None,
        model_settings: "{}",
        bedrock_config: None,
        is_full_url: false,
    })
    .await
    .unwrap();
}

fn make_factory(
    provider_repo: Arc<dyn IProviderRepository>,
    agent_registry: Arc<AgentRegistry>,
    acp_agent_service: Arc<AcpSessionSyncService>,
    data_dir: &Path,
) -> aionui_ai_agent::task_manager::AgentFactory {
    let skill_paths = Arc::new(aionui_extension::resolve_skill_paths(data_dir, data_dir));
    // These provider integration tests only build aionrs tasks, which never touch
    // the session spawner. It just has to be constructable (the field is no longer
    // optional now that claude/codex always use the direct-CLI session path).
    let process_registry = Arc::new(aionui_process::FileRegistryStore::new(data_dir));
    let session_spawner: Arc<dyn aionui_process::Spawner> = Arc::new(aionui_process::RealSpawner::new(
        process_registry,
        uuid::Uuid::now_v7(),
        aionui_process::local_machine_id(data_dir),
    ));
    build_agent_factory(AgentFactoryDeps {
        skill_manager: AcpSkillManager::new(skill_paths),
        provider_repo,
        encryption_key: test_encryption_key(),
        agent_registry,
        acp_agent_service,
        data_dir: data_dir.to_path_buf(),
        dump_prompts: false,
        broadcaster: Arc::new(BroadcastEventBus::new(16)),
        backend_binary_path: Arc::new(data_dir.join("aioncore")),
        mcp_server_repo: None,
        session_spawner,
        // No hook bridge in this test: it exercises provider wiring, not the
        // Antigravity permission path.
        antigravity_hook_base_url: None,
        antigravity_hook_tokens: Arc::new(aionui_ai_agent::antigravity_hook::HookTokenRegistry::new()),
    })
}

fn make_aionrs_options(
    conversation_id: &str,
    workspace: &str,
    model: ProviderWithModel,
    config: AionrsBuildExtra,
) -> BuildTaskOptions {
    BuildTaskOptions::new(AgentSessionContext {
        conversation: ConversationContext {
            conversation_id: conversation_id.to_owned(),
            user_id: "system_default_user".to_owned(),
            agent_type: AgentType::Aionrs,
            source: None,
        },
        workspace: WorkspaceContext {
            path: workspace.to_owned(),
            stored_path: workspace.to_owned(),
            is_custom: !workspace.is_empty(),
        },
        model,
        skills: vec![],
        runtime_env: vec![],
        team: None,
        kind: AgentSessionKind::Aionrs(Box::new(AionrsSessionBuildContext {
            config,
            team: None,
            belongs_to_team: false,
        })),
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn aionrs_factory_returns_error_for_missing_provider() {
    let (provider_repo, agent_registry, acp_agent_service) = setup().await;
    let directory = tempfile::tempdir().unwrap();
    let factory = make_factory(provider_repo, agent_registry, acp_agent_service, directory.path());

    let options = make_aionrs_options(
        "conv-test-1",
        "",
        ProviderWithModel {
            provider_id: "nonexistent-provider".into(),
            model: "gpt-4o".into(),
            use_model: None,
        },
        AionrsBuildExtra::default(),
    );

    let result = factory(options).await;
    match result {
        Ok(_) => panic!("Expected error for missing provider, got Ok"),
        Err(e) => {
            let err_msg = e.to_string();
            assert!(
                err_msg.contains("not found"),
                "Expected 'not found' error, got: {err_msg}"
            );
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn aionrs_factory_resolves_provider_from_db() {
    let (provider_repo, agent_registry, acp_agent_service) = setup().await;
    insert_test_provider(&*provider_repo, "prov-001", "openai").await;
    let directory = tempfile::tempdir().unwrap();
    let factory = make_factory(provider_repo, agent_registry, acp_agent_service, directory.path());

    let options = make_aionrs_options(
        "conv-test-2",
        "/tmp/test-workspace",
        ProviderWithModel {
            provider_id: "prov-001".into(),
            model: "gpt-4o".into(),
            use_model: None,
        },
        AionrsBuildExtra::default(),
    );

    let result = factory(options).await;
    assert!(result.is_ok(), "Expected Ok, got: {:?}", result.err());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn aionrs_factory_respects_use_model_override() {
    let (provider_repo, agent_registry, acp_agent_service) = setup().await;
    insert_test_provider(&*provider_repo, "prov-002", "openai").await;
    let directory = tempfile::tempdir().unwrap();
    let factory = make_factory(provider_repo, agent_registry, acp_agent_service, directory.path());

    let options = make_aionrs_options(
        "conv-test-3",
        "/tmp/test-workspace",
        ProviderWithModel {
            provider_id: "prov-002".into(),
            model: "gpt-4o".into(),
            use_model: Some("gpt-5.4".into()),
        },
        AionrsBuildExtra::default(),
    );

    let result = factory(options).await;
    assert!(result.is_ok(), "Expected Ok, got: {:?}", result.err());
}

#[tokio::test]
async fn live_aionrs_runtime_refreshes_the_model_reasoning_policy_between_turns() {
    let directory = tempfile::tempdir().unwrap();
    let tool_file = directory.path().join("tool-input.txt");
    std::fs::write(&tool_file, "test tool output").unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base_url = format!("http://{}", listener.local_addr().unwrap());
    let (provider_repo, agent_registry, acp_agent_service) = setup().await;
    let provider_id = "prov-live-reasoning";
    insert_provider(&*provider_repo, provider_id, "openai", &base_url).await;
    let provider_repo_for_mock = Arc::clone(&provider_repo);
    let (request_tx, mut requests) = tokio::sync::mpsc::unbounded_channel::<Value>();
    let request_number = Arc::new(AtomicUsize::new(0));
    let router = Router::new().route(
        "/chat/completions",
        post(move |Json(body): Json<Value>| {
            let request_number = request_number.fetch_add(1, Ordering::Relaxed);
            let arguments = json!({"file_path": tool_file}).to_string();
            let repository = Arc::clone(&provider_repo_for_mock);
            request_tx.send(body).unwrap();
            async move {
                let (delta, finish_reason) = if request_number == 1 {
                    // Flip the active model's setting after its first request,
                    // while this turn is still waiting for its tool call.
                    update_model_settings(
                        &*repository,
                        provider_id,
                        r#"{"gpt-4o":{"reasoning_tool_round_only":true},"gpt-5.4":{"reasoning_tool_round_only":false}}"#,
                    )
                    .await;
                    (
                        json!({
                            "reasoning_content": "tool reasoning",
                            "tool_calls": [{
                                "index": 0,
                                "id": "call_read",
                                "type": "function",
                                "function": {"name": "Read", "arguments": arguments}
                            }]
                        }),
                        "tool_calls",
                    )
                } else {
                    (
                        json!({
                            "content": "Done",
                            "reasoning_content": format!("reply reasoning {request_number}")
                        }),
                        "stop",
                    )
                };
                let chunk = json!({"choices": [{"index": 0, "delta": delta, "finish_reason": finish_reason}]});
                (
                    [("content-type", "text/event-stream")],
                    format!("data: {chunk}\n\ndata: [DONE]\n\n"),
                )
            }
        }),
    );
    let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });

    let factory = make_factory(
        provider_repo.clone(),
        agent_registry,
        acp_agent_service,
        directory.path(),
    );
    let conversation_id = "conv-live-reasoning";
    let agent = factory(make_aionrs_options(
        conversation_id,
        directory.path().to_str().unwrap(),
        ProviderWithModel {
            provider_id: provider_id.into(),
            model: "gpt-4o".into(),
            use_model: Some("gpt-5.4".into()),
        },
        AionrsBuildExtra {
            session_mode: Some("yolo".into()),
            ..AionrsBuildExtra::default()
        },
    ))
    .await
    .unwrap();
    let send = |text: &str| {
        tokio::time::timeout(
            Duration::from_secs(10),
            agent.send_message(SendMessageData {
                content: text.into(),
                msg_id: text.into(),
                turn_id: None,
                files: Vec::new(),
                inject_skills: Vec::new(),
            }),
        )
    };

    send("first").await.unwrap().unwrap();
    assert_eq!(requests.try_recv().unwrap()["model"], "gpt-5.4");

    update_model_settings(
        &*provider_repo,
        provider_id,
        r#"{"gpt-5.4":{"reasoning_tool_round_only":true}}"#,
    )
    .await;
    send("use a tool").await.unwrap().unwrap();
    assert!(reasoning_values(&requests.try_recv().unwrap()).is_empty());
    let continuation = requests.try_recv().unwrap();
    assert_eq!(reasoning_values(&continuation), ["tool reasoning"]);
    assert!(continuation["messages"].as_array().unwrap().iter().any(|message| {
        message["role"] == "tool"
            && message["tool_call_id"] == "call_read"
            && message["content"].as_str().unwrap().contains("test tool output")
    }));

    send("apply the disabled policy").await.unwrap().unwrap();
    assert_eq!(
        reasoning_values(&requests.try_recv().unwrap()),
        ["reply reasoning 0", "tool reasoning", "reply reasoning 2"]
    );

    update_model_settings(
        &*provider_repo,
        provider_id,
        r#"{"gpt-5.4":{"reasoning_tool_round_only":true}}"#,
    )
    .await;
    send("ordinary follow-up").await.unwrap().unwrap();
    assert!(reasoning_values(&requests.try_recv().unwrap()).is_empty());
    let session = SessionManager::new(directory.path().join("aionrs-sessions"), 100)
        .load(conversation_id)
        .unwrap();
    assert!(
        session
            .messages
            .iter()
            .flat_map(|message| &message.content)
            .any(|block| { matches!(block, ContentBlock::Thinking { thinking, .. } if thinking == "tool reasoning") })
    );

    update_model_settings(
        &*provider_repo,
        provider_id,
        r#"{"gpt-5.4":{"reasoning_tool_round_only":"true"}}"#,
    )
    .await;
    let error = send("invalid policy").await.unwrap().unwrap_err();
    assert_eq!(error.stream_error().code, Some(AgentErrorCode::AionuiInternalError));
    assert_eq!(
        error.stream_error().detail.as_deref(),
        Some("Stored model settings are invalid")
    );
    assert!(requests.try_recv().is_err());

    provider_repo.delete(TEST_USER_ID, provider_id).await.unwrap();
    let error = send("missing provider").await.unwrap().unwrap_err();
    assert_eq!(
        error.stream_error().detail.as_deref(),
        Some("The model provider no longer exists")
    );
    assert!(requests.try_recv().is_err());
    server.abort();
}

async fn update_model_settings(repository: &dyn IProviderRepository, provider_id: &str, settings: &str) {
    repository
        .update(
            TEST_USER_ID,
            provider_id,
            UpdateProviderParams {
                model_settings: Some(settings),
                ..UpdateProviderParams::default()
            },
        )
        .await
        .unwrap();
}

fn reasoning_values(body: &Value) -> Vec<&str> {
    body["messages"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|message| message["reasoning_content"].as_str())
        .collect()
}
