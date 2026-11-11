mod common;

use axum::http::StatusCode;
use serde_json::json;
use tower::ServiceExt;

use aionui_app::{AppConfig, AppServices, create_router};
use aionui_db::{IDotsRepository, SqliteDotsRepository};
use common::{body_json, delete_with_token, get_request, get_with_token, json_with_token, setup_and_login};

#[tokio::test]
async fn dots_default_is_protected_and_only_its_owner_can_create_dots_chats() {
    let db = aionui_db::init_database_memory().await.unwrap();
    let root = tempfile::tempdir().unwrap();
    let settings = root.path().join("dots-bridge.json");
    std::fs::write(root.path().join("token"), "mock-token").unwrap();
    std::fs::write(&settings, r#"{"url":"http://127.0.0.1:1","token_file":"token"}"#).unwrap();
    let config = AppConfig {
        data_dir: root.path().to_path_buf(),
        work_dir: root.path().to_path_buf(),
        ..Default::default()
    };

    let services = AppServices::from_config(db.clone(), &config).await.unwrap();
    let repo = SqliteDotsRepository::new(db.pool().clone());
    let default = services
        .conversation_service
        .dots()
        .unwrap()
        .default_id()
        .unwrap()
        .to_owned();
    assert_eq!(repo.defaults().await.unwrap().len(), 1);
    let mut app = create_router(&services).await.unwrap();
    let (owner, csrf) = setup_and_login(&mut app, &services, "admin", "StrongP@ss1").await;
    let (other, other_csrf) = setup_and_login(&mut app, &services, "visitor", "StrongP@ss2").await;

    let catalog = app
        .clone()
        .oneshot(get_with_token("/api/assistants", &owner))
        .await
        .unwrap();
    let catalog = body_json(catalog).await;
    let dots = catalog["data"]
        .as_array()
        .unwrap()
        .iter()
        .find(|entry| entry["agent"]["acp_backend"] == "dots")
        .unwrap_or_else(|| panic!("Dots must be available in the owner assistant catalog"));
    let assistant_id = dots["id"].as_str().unwrap();

    let mut chats = Vec::new();
    for name in ["First Dots chat", "Second Dots chat"] {
        let request = json_with_token(
            "POST",
            "/api/conversations",
            json!({
                "name": name, "assistant": {"id": assistant_id},
                "extra": {"dots_default": true}
            }),
            &owner,
            &csrf,
        );
        let response = app.clone().oneshot(request).await.unwrap();
        let status = response.status();
        let body = body_json(response).await;
        assert!(status.is_success(), "{status}: {body}");
        assert_ne!(body["data"]["extra"]["dots_default"], true);
        chats.push(body["data"]["id"].as_str().unwrap().to_owned());
    }
    assert_ne!(chats[0], chats[1]);
    assert_eq!(repo.defaults().await.unwrap().len(), 1);

    let uri = format!("/api/conversations/{default}");
    assert_eq!(
        app.clone()
            .oneshot(delete_with_token(&uri, &owner, &csrf))
            .await
            .unwrap()
            .status(),
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        app.clone()
            .oneshot(get_with_token(&uri, &other))
            .await
            .unwrap()
            .status(),
        StatusCode::NOT_FOUND
    );
    let attachment_uri = format!("{uri}/dots-attachments/art_00000000000000000000000000000001");
    assert_eq!(
        app.clone()
            .oneshot(get_request(&attachment_uri))
            .await
            .unwrap()
            .status(),
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        app.clone()
            .oneshot(get_with_token(&attachment_uri, &other))
            .await
            .unwrap()
            .status(),
        StatusCode::NOT_FOUND
    );
    let foreign_create = json_with_token(
        "POST",
        "/api/conversations",
        json!({
            "type": "acp", "extra": {"backend": "dots"}
        }),
        &other,
        &other_csrf,
    );
    assert_eq!(
        app.oneshot(foreign_create).await.unwrap().status(),
        StatusCode::NOT_FOUND
    );

    let restarted = AppServices::from_config(db.clone(), &config).await.unwrap();
    assert_eq!(
        restarted.conversation_service.dots().unwrap().default_id(),
        Some(default.as_str())
    );
    std::fs::write(&settings, "invalid config").unwrap();
    let unavailable = AppServices::from_config(db.clone(), &config).await.unwrap();
    assert!(unavailable.conversation_service.dots().is_none());
    assert_eq!(repo.defaults().await.unwrap()[0].1, default);
    std::fs::remove_file(settings).unwrap();
    let disabled = AppServices::from_config(db, &config).await.unwrap();
    assert!(repo.defaults().await.unwrap().is_empty());
    for chat in chats {
        assert!(
            disabled
                .conversation_service
                .get("system_default_user", &chat)
                .await
                .is_ok()
        );
    }
}
