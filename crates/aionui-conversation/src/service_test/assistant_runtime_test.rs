use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use aionui_ai_agent::task_manager::AgentFactory;
use aionui_ai_agent::{AgentSessionKind, WorkerTaskManagerImpl};
use aionui_db::{
    AssistantRuntimeConfig, CreateMcpServerParams, IAssistantRuntimeConfigRepository, IMcpServerRepository,
    SqliteAcpSessionRepository, SqliteAgentMetadataRepository, SqliteAssistantRuntimeConfigRepository,
    SqliteConversationRepository, SqliteMcpServerRepository,
};
use serde_json::{Value, json};
use tokio::sync::oneshot;

use super::{
    AgentInstance, AgentType, BlockingCancelAgent, BuildTaskOptions, ConversationService, IConversationRepository,
    IWorkerTaskManager, MockBroadcaster, RecordingSkillResolver, ScriptedAgent, SqliteAssistantDefinitionRepository,
    SqliteAssistantOverlayRepository, SqliteAssistantPreferenceRepository, StaticAssistantDispatcher,
    create_assistant_backed_conversation, init_database_memory, make_send_req, seed_test_user,
    upsert_test_assistant_definition,
};
use crate::service::ConversationSendOrigin;

#[tokio::test]
async fn published_assistant_settings_take_effect_at_the_next_user_turn_only() {
    let database = init_database_memory().await.unwrap();
    let pool = database.pool();
    let user = "config-test-user";
    let assistant = "config-test-assistant";
    seed_test_user(pool, user).await;
    let workspace = tempfile::tempdir().unwrap();
    let conversations = Arc::new(SqliteConversationRepository::new(pool.clone()));
    let definitions = Arc::new(SqliteAssistantDefinitionRepository::new(pool.clone()));
    let published = Arc::new(SqliteAssistantRuntimeConfigRepository::new(pool.clone()));
    let resolver = Arc::new(RecordingSkillResolver::new(vec!["old-skill".into()]));
    upsert_test_assistant_definition(&definitions, "test-definition", assistant, "aionrs", "auto", "auto").await;

    let options = Arc::new(Mutex::new(Vec::<BuildTaskOptions>::new()));
    let (first_agent, started) = oneshot::channel();
    let first_agent = Mutex::new(Some(first_agent));
    let factory: AgentFactory = Arc::new({
        let options = Arc::clone(&options);
        move |input: BuildTaskOptions| {
            let id = input.conversation_id().to_owned();
            let workspace = input.context.workspace.path.clone();
            options.lock().unwrap().push(input);
            let first_agent = first_agent.lock().unwrap().take();
            Box::pin(async move {
                let instance = if let Some(started) = first_agent {
                    let mut agent = BlockingCancelAgent::new_with_type(&id, AgentType::Aionrs);
                    agent.workspace = workspace;
                    let agent = Arc::new(agent);
                    let _ = started.send(Arc::clone(&agent));
                    AgentInstance::Mock(agent)
                } else {
                    let mut agent = ScriptedAgent::new(&id, vec![]).with_agent_type(AgentType::Aionrs);
                    agent.workspace = workspace;
                    AgentInstance::Mock(Arc::new(agent))
                };
                Ok(instance)
            })
        }
    });
    let tasks: Arc<dyn IWorkerTaskManager> = Arc::new(WorkerTaskManagerImpl::new(factory));
    let svc = ConversationService::new(
        workspace.path().to_owned(),
        Arc::new(MockBroadcaster::new()),
        resolver.clone(),
        tasks.clone(),
        conversations.clone(),
        Arc::new(SqliteAgentMetadataRepository::new(pool.clone())),
        Arc::new(SqliteAcpSessionRepository::new(pool.clone())),
    )
    .with_assistant_runtime_config_repo(published.clone());
    svc.with_assistant_definition_repo(definitions);
    svc.with_assistant_state_repo(Arc::new(SqliteAssistantOverlayRepository::new(pool.clone())));
    svc.with_assistant_preference_repo(Arc::new(SqliteAssistantPreferenceRepository::new(pool.clone())));
    svc.with_assistant_dispatcher(Arc::new(StaticAssistantDispatcher {
        rules: HashMap::from([(assistant.to_owned(), "old rule".to_owned())]),
    }));
    let mcps = Arc::new(SqliteMcpServerRepository::new(pool.clone()));
    let server = mcps
        .create(CreateMcpServerParams {
            user_id: user,
            name: "next-turn-mcp",
            backend_alias: None,
            description: None,
            enabled: true,
            transport_type: "streamable_http",
            transport_config: r#"{"url":"http://127.0.0.1:8999/mcp"}"#,
            tools: None,
            original_json: None,
            builtin: false,
            purpose: None,
        })
        .await
        .unwrap();
    svc.with_mcp_server_repo(mcps);

    let conversation = create_assistant_backed_conversation(&svc, user, Some("aionrs"), "aionrs", assistant).await;
    let id = conversation.id.as_str();
    let view_count = resolver.views.lock().unwrap().len();
    svc.warmup(user, id, &tasks).await.unwrap();
    assert_eq!(resolver.views.lock().unwrap().len(), view_count);
    svc.send_message_for_session(user, id, make_send_req(), &tasks, ConversationSendOrigin::DesktopLocal)
        .await
        .unwrap();
    let first = tokio::time::timeout(Duration::from_secs(5), started)
        .await
        .unwrap()
        .unwrap();

    let new_settings = AssistantRuntimeConfig {
        skills_revision: 1,
        mcps_revision: 1,
        rules_revision: 1,
        skills_mode: "fixed".into(),
        mcps_mode: "fixed".into(),
        skill_ids: vec!["new-skill".into()],
        disabled_builtin_skill_ids: vec!["old-skill".into()],
        mcp_ids: vec![server.id.clone()],
        rules_by_locale: BTreeMap::from([("en-US".into(), "new rule".into())]),
        ..Default::default()
    };
    published.put_for_user(user, assistant, &new_settings).await.unwrap();
    assert!(svc.runtime_state().is_claimed(id));
    let old = conversations.get(user, id).await.unwrap().unwrap();
    let old_extra: Value = serde_json::from_str(&old.extra).unwrap();
    assert_eq!(old_extra["skills"], json!(["old-skill"]));
    assert_eq!(old_extra["preset_rules"], "old rule");
    assert_eq!(old_extra["mcp_server_ids"], json!([]));
    first.finish_notify.notify_one();
    wait_for_turn(&svc, id).await;

    send_next_user_turn(&svc, &tasks, user, id).await;
    send_next_user_turn(&svc, &tasks, user, id).await;
    {
        let built = options.lock().unwrap();
        assert_eq!(
            built.len(),
            2,
            "unchanged effective settings must reuse the second runtime"
        );
        assert_eq!(built[0].context.skills, ["old-skill"]);
        assert_eq!(built[1].context.skills, ["new-skill"]);
        let AgentSessionKind::Aionrs(config) = &built[1].context.kind else {
            panic!("expected AionRS runtime");
        };
        assert_eq!(config.config.preset_rules.as_deref(), Some("new rule"));
        assert_eq!(config.config.mcp_server_ids.as_deref(), Some(&[server.id.clone()][..]));
    }
    let effective = conversations.get(user, id).await.unwrap().unwrap();
    let effective_extra: Value = serde_json::from_str(&effective.extra).unwrap();
    assert_eq!(effective_extra["skills"], json!(["new-skill"]));
    assert_eq!(effective_extra["preset_rules"], "new rule");
    assert_eq!(effective_extra["mcp_server_ids"], json!([server.id]));

    let removed_settings = AssistantRuntimeConfig {
        skills_revision: 2,
        mcps_revision: 2,
        rules_revision: 2,
        skill_ids: vec![],
        mcp_ids: vec![],
        rules_by_locale: BTreeMap::new(),
        ..new_settings
    };
    published
        .put_for_user(user, assistant, &removed_settings)
        .await
        .unwrap();
    send_next_user_turn(&svc, &tasks, user, id).await;
    let built = options.lock().unwrap();
    assert_eq!(built.len(), 3);
    let AgentSessionKind::Aionrs(config) = &built[2].context.kind else {
        panic!("expected AionRS runtime");
    };
    assert_eq!(config.config.preset_rules, None);
    assert_eq!(config.config.mcp_server_ids.as_deref(), Some(&[][..]));
    assert!(config.config.session_mcp_servers.is_empty());
    assert!(built[2].context.skills.is_empty());
    assert_eq!(built[0].context.model.model, built[2].context.model.model);
    assert_eq!(
        resolver.view_removals.lock().unwrap().as_slice(),
        &[(user.to_owned(), id.to_owned())]
    );
}

async fn send_next_user_turn(svc: &ConversationService, tasks: &Arc<dyn IWorkerTaskManager>, user: &str, id: &str) {
    svc.send_message_for_session(
        user,
        id,
        make_send_req(),
        tasks,
        ConversationSendOrigin::AuthenticatedUser,
    )
    .await
    .unwrap();
    wait_for_turn(svc, id).await;
}

async fn wait_for_turn(svc: &ConversationService, id: &str) {
    tokio::time::timeout(Duration::from_secs(5), svc.runtime_state().wait_until_unclaimed(id))
        .await
        .unwrap();
}
