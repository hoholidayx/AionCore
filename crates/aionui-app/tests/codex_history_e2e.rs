mod common;

use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use aionui_app::{AppConfig, AppServices, create_router};
use aionui_auth::SYSTEM_DEFAULT_USER_ID as OWNER;
use aionui_conversation::ConversationError;
use aionui_conversation::service::CodexHistoryDeletion;
use aionui_db::{IAcpSessionRepository, SqliteAcpSessionRepository, init_database_memory};
use axum::http::StatusCode;
use serde_json::json;
use tower::ServiceExt;

use common::{body_json, delete_with_token, get_with_token, json_with_token, setup_and_login};

const THREAD: &str = "00000000-0000-4000-8000-000000000001";

#[derive(Default)]
struct CodexDeleteProbe {
    fail: AtomicBool,
    calls: AtomicUsize,
}

#[async_trait::async_trait]
impl CodexHistoryDeletion for CodexDeleteProbe {
    async fn delete(&self, thread_id: &str, _: &Path) -> Result<(), String> {
        assert_eq!(thread_id, THREAD);
        self.calls.fetch_add(1, Ordering::SeqCst);
        if self.fail.load(Ordering::SeqCst) {
            return Err("injected Codex timeout".into());
        }
        Ok(())
    }
}

async fn setup(root: &Path) -> (axum::Router, AppServices, Arc<CodexDeleteProbe>) {
    let db = init_database_memory().await.unwrap();
    let config = AppConfig {
        data_dir: root.to_owned(),
        work_dir: root.to_owned(),
        ..Default::default()
    };
    let mut services = AppServices::from_config(db, &config).await.unwrap();
    let probe = Arc::new(CodexDeleteProbe::default());
    services.conversation_service = services
        .conversation_service
        .clone()
        .with_codex_history_deletion(Some(probe.clone()));
    (create_router(&services).await.unwrap(), services, probe)
}

async fn create_bound(app: &axum::Router, services: &AppServices, token: &str, csrf: &str, root: &Path) -> String {
    let response = app.clone().oneshot(json_with_token(
        "POST", "/api/conversations",
        json!({"type": "acp", "name": "Test", "extra": {"backend": "codex", "agent_id": "8e1acf31", "agent_source": "builtin"}}),
        token, csrf,
    )).await.unwrap();
    assert_eq!(response.status(), StatusCode::CREATED);
    let body = body_json(response).await;
    let id = body["data"]["id"].as_str().unwrap();
    let repo = SqliteAcpSessionRepository::new(services.database.pool().clone());
    repo.update_session_id_for_user(OWNER, id, THREAD).await.unwrap();
    repo.record_fresh_codex_thread(OWNER, id, THREAD, root.to_str().unwrap())
        .await
        .unwrap();
    id.to_owned()
}

async fn options(app: &axum::Router, uri: &str, token: &str) -> serde_json::Value {
    let response = app
        .clone()
        .oneshot(get_with_token(&format!("{uri}/delete-options"), token))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    body_json(response).await["data"].clone()
}

#[tokio::test]
async fn failed_codex_and_local_deletion_resume_at_the_correct_stage() {
    let temp = tempfile::tempdir().unwrap();
    let (mut app, mut services, probe) = setup(temp.path()).await;
    let (token, csrf) = setup_and_login(&mut app, &services, "admin", "StrongP@ss1").await;
    let id = create_bound(&app, &services, &token, &csrf, temp.path()).await;
    let uri = format!("/api/conversations/{id}");
    let both = format!("{uri}/with-codex-history");
    let stale_row = services.conversation_repo.get(OWNER, &id).await.unwrap().unwrap();
    let pool = services.database.pool().clone();
    sqlx::query("INSERT INTO messages (id, conversation_id, type, created_at) VALUES ('msg-retained', ?, 'text', 1)")
        .bind(&id)
        .execute(&pool)
        .await
        .unwrap();
    assert_eq!(options(&app, &uri, &token).await, "available");

    probe.fail.store(true, Ordering::SeqCst);
    let response = app
        .clone()
        .oneshot(delete_with_token(&both, &token, &csrf))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
    assert_eq!(options(&app, &uri, &token).await, "retry_required");
    let messages: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM messages WHERE conversation_id = ?")
        .bind(&id)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(messages, 1);

    // Rebuild routing and discard the runtime state. The retry state comes from SQLite.
    services.conversation_service = services.conversation_service.clone().with_runtime_state(Arc::default());
    let app = create_router(&services).await.unwrap();
    assert_eq!(options(&app, &uri, &token).await, "retry_required");
    let response = app
        .clone()
        .oneshot(json_with_token(
            "POST",
            &format!("{uri}/runtime/ensure"),
            json!({}),
            &token,
            &csrf,
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::CONFLICT);
    let response = app
        .clone()
        .oneshot(json_with_token(
            "POST",
            &format!("{uri}/messages"),
            json!({"content": "must not restart a pending thread"}),
            &token,
            &csrf,
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::CONFLICT);

    probe.fail.store(false, Ordering::SeqCst);
    sqlx::query("CREATE TRIGGER reject_delete BEFORE DELETE ON conversations BEGIN SELECT RAISE(ABORT, 'injected failure'); END")
        .execute(&pool).await.unwrap();
    let response = app
        .clone()
        .oneshot(delete_with_token(&both, &token, &csrf))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    assert_eq!(options(&app, &uri, &token).await, "retry_required");
    assert_eq!(probe.calls.load(Ordering::SeqCst), 2);

    sqlx::query("DROP TRIGGER reject_delete").execute(&pool).await.unwrap();
    let response = app
        .clone()
        .oneshot(delete_with_token(&both, &token, &csrf))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let response = app.oneshot(delete_with_token(&both, &token, &csrf)).await.unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    assert_eq!(probe.calls.load(Ordering::SeqCst), 2);
    assert!(matches!(
        services
            .conversation_service
            .build_task_options_for_runtime(&stale_row, None)
            .await,
        Err(ConversationError::NotFound { id: missing }) if missing == id
    ));
}

#[tokio::test]
async fn scoped_delete_rejects_foreign_and_shared_targets_and_serializes_retries() {
    let temp = tempfile::tempdir().unwrap();
    let (mut app, services, probe) = setup(temp.path()).await;
    let (token, csrf) = setup_and_login(&mut app, &services, "admin", "StrongP@ss1").await;
    let (other_token, other_csrf) = setup_and_login(&mut app, &services, "bob", "StrongP@ss2").await;
    let untracked = services
        .conversation_service
        .create(
            OWNER,
            serde_json::from_value(json!({
                "type": "acp", "extra": {"backend": "codex", "agent_id": "8e1acf31", "agent_source": "builtin"}
            }))
            .unwrap(),
        )
        .await
        .unwrap();
    let repo = SqliteAcpSessionRepository::new(services.database.pool().clone());
    repo.update_session_id_for_user(OWNER, &untracked.id, "00000000-0000-4000-8000-000000000002")
        .await
        .unwrap();
    repo.record_fresh_codex_thread(
        OWNER,
        &untracked.id,
        "00000000-0000-4000-8000-000000000002",
        temp.path().to_str().unwrap(),
    )
    .await
    .unwrap();
    assert_eq!(
        options(&app, &format!("/api/conversations/{}", untracked.id), &token).await,
        "unavailable"
    );
    let id = create_bound(&app, &services, &token, &csrf, temp.path()).await;
    let other_id = create_bound(&app, &services, &token, &csrf, temp.path()).await;
    let uri = format!("/api/conversations/{id}");
    let both = format!("{uri}/with-codex-history");
    let mut no_csrf = delete_with_token(&both, &token, &csrf);
    no_csrf.headers_mut().remove("x-csrf-token");
    assert_eq!(
        app.clone().oneshot(no_csrf).await.unwrap().status(),
        StatusCode::FORBIDDEN
    );
    let mut runtime_request = delete_with_token(&both, &token, &csrf);
    runtime_request.headers_mut().remove("x-csrf-token");
    runtime_request
        .headers_mut()
        .insert(aionui_auth::RUNTIME_TOKEN_HEADER, "runtime".parse().unwrap());
    assert_eq!(
        app.clone().oneshot(runtime_request).await.unwrap().status(),
        StatusCode::FORBIDDEN
    );
    let response = app
        .clone()
        .oneshot(delete_with_token(&both, &other_token, &other_csrf))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    assert_eq!(options(&app, &uri, &token).await, "unavailable");
    let response = app
        .clone()
        .oneshot(delete_with_token(&both, &token, &csrf))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::CONFLICT);
    assert_eq!(probe.calls.load(Ordering::SeqCst), 0);

    let other_uri = format!("/api/conversations/{other_id}");
    let response = app
        .clone()
        .oneshot(delete_with_token(&other_uri, &token, &csrf))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(probe.calls.load(Ordering::SeqCst), 0);
    let archive = app
        .clone()
        .oneshot(json_with_token(
            "POST",
            &format!("/api/sidebar/conversation/{id}/archive"),
            json!({}),
            &token,
            &csrf,
        ))
        .await
        .unwrap();
    assert_eq!(archive.status(), StatusCode::OK);
    assert_eq!(options(&app, &uri, &token).await, "available");
    let (first, second) = tokio::join!(
        app.clone().oneshot(delete_with_token(&both, &token, &csrf)),
        app.oneshot(delete_with_token(&both, &token, &csrf)),
    );
    let mut statuses = [first.unwrap().status().as_u16(), second.unwrap().status().as_u16()];
    statuses.sort_unstable();
    assert_eq!(statuses, [200, 404]);
    assert_eq!(probe.calls.load(Ordering::SeqCst), 1);
}
