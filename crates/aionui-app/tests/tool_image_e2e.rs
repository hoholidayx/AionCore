//! Exercise conversation-owned image lookup through the authenticated HTTP router.

mod common;

use aionui_ai_agent::{RuntimeTokenScope, TEAM_RUNTIME_TOKEN_SESSION_GENERATION};
use aionui_auth::{RUNTIME_CONVERSATION_ID_HEADER, RUNTIME_TOKEN_HEADER, RUNTIME_USER_ID_HEADER};
use aionui_db::models::MessageRow;
use axum::Router;
use axum::http::{StatusCode, header};
use base64::Engine;
use http_body_util::BodyExt;
use serde_json::{Value, json};
use tower::ServiceExt;

use common::{body_json, build_app_with_file_roots, get_request, get_with_token, json_with_token, setup_and_login};

const PNG: &str = "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAAAAAA6fptVAAAACklEQVR4nGOoBwAAgQCA05RTSgAAAABJRU5ErkJggg==";

async fn create_conversation(app: &Router, token: &str, csrf: &str) -> String {
    let response = app
        .clone()
        .oneshot(json_with_token(
            "POST",
            "/api/conversations",
            json!({"type": "acp", "name": "Image history", "extra": {"backend": "codex"}}),
            token,
            csrf,
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::CREATED);
    body_json(response).await["data"]["id"].as_str().unwrap().to_owned()
}

#[tokio::test]
async fn codex_image_is_served_only_to_the_message_owner() {
    let file_root = tempfile::tempdir().unwrap();
    let codex_home = tempfile::tempdir().unwrap();
    let image_dir = codex_home.path().join("generated_images").join("thread");
    std::fs::create_dir_all(&image_dir).unwrap();
    let png = base64::engine::general_purpose::STANDARD.decode(PNG).unwrap();
    let path = image_dir.join("result.png");
    std::fs::write(&path, &png).unwrap();
    let invalid = image_dir.join("invalid.png");
    std::fs::write(&invalid, b"<svg onload='alert(1)'>").unwrap();
    let mismatched = image_dir.join("mismatched.png");
    std::fs::write(&mismatched, b"\xff\xd8\xff\xe0").unwrap();
    let oversized = image_dir.join("oversized.png");
    let file = std::fs::File::create(&oversized).unwrap();
    file.set_len(33 * 1024 * 1024).unwrap();
    drop(file);

    let (mut app, services) = build_app_with_file_roots(vec![file_root.path().to_path_buf()]).await;
    let (owner_token, csrf) = setup_and_login(&mut app, &services, "admin", "StrongP@ss1").await;
    let (foreign_token, _) = setup_and_login(&mut app, &services, "foreign", "OtherP@ss1").await;
    let owner = services.user_repo.find_by_username("admin").await.unwrap().unwrap();
    let conversation_id = create_conversation(&app, &owner_token, &csrf).await;
    let fork_id = create_conversation(&app, &owner_token, &csrf).await;
    let message_route = |conversation: &str, id: &str| format!("/api/conversations/{conversation}/messages/{id}/image");
    let repo = &services.conversation_repo;

    let row = |id: &str, kind: &str, status: &str, content: Value| MessageRow {
        id: id.to_owned(),
        conversation_id: conversation_id.clone(),
        msg_id: Some(id.to_owned()),
        r#type: kind.to_owned(),
        content: content.to_string(),
        position: Some("left".into()),
        status: Some(status.to_owned()),
        hidden: false,
        created_at: 1000,
        backend_turn_id: None,
    };
    let content = |path: &std::path::Path| {
        json!({"name": "imageGeneration", "args": {"type": "imageGeneration"}, "status": "completed",
               "image": {"path": path, "mime_type": "image/png"}})
    };
    repo.insert_message(&owner.id, &row("image", "tool_call", "finish", content(&path)))
        .await
        .unwrap();
    repo.copy_messages_up_to(&owner.id, &conversation_id, &fork_id, (1000, "image"))
        .await
        .unwrap();

    for item in [
        row("text", "text", "finish", content(&path)),
        row("unfinished", "tool_call", "work", content(&path)),
        row("invalid", "tool_call", "finish", content(&invalid)),
        row("mismatched", "tool_call", "finish", content(&mismatched)),
        row("oversized", "tool_call", "finish", content(&oversized)),
        row(
            "missing",
            "tool_call",
            "finish",
            content(&image_dir.join("missing.png")),
        ),
    ] {
        repo.insert_message(&owner.id, &item).await.unwrap();
        let response = app
            .clone()
            .oneshot(get_with_token(&message_route(&conversation_id, &item.id), &owner_token))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND, "{}", item.id);
        assert_eq!(body_json(response).await["error"], "Generated image not found");
    }

    let old = app
        .clone()
        .oneshot(json_with_token(
            "POST",
            "/api/fs/image-base64",
            json!({"path": path}),
            &owner_token,
            &csrf,
        ))
        .await
        .unwrap();
    assert_eq!(old.status(), StatusCode::FORBIDDEN);

    // Forked tool cards keep the call id, even though the copied row id changes.
    for id in [&conversation_id, &fork_id] {
        let image = app
            .clone()
            .oneshot(get_with_token(&message_route(id, "image"), &owner_token))
            .await
            .unwrap();
        assert_eq!(image.status(), StatusCode::OK);
        assert_eq!(image.headers()[header::CONTENT_TYPE], "image/png");
        assert_eq!(image.headers()[header::CACHE_CONTROL], "private, no-store");
        assert_eq!(image.headers()[header::X_CONTENT_TYPE_OPTIONS], "nosniff");
        assert_eq!(image.into_body().collect().await.unwrap().to_bytes().as_ref(), png);
    }

    let route = message_route(&conversation_id, "image");
    let foreign = app
        .clone()
        .oneshot(get_with_token(&route, &foreign_token))
        .await
        .unwrap();
    assert_eq!(foreign.status(), StatusCode::NOT_FOUND);
    assert_eq!(body_json(foreign).await["error"], "Generated image not found");
    let unauthenticated = app.clone().oneshot(get_request(&route)).await.unwrap();
    assert_eq!(unauthenticated.status(), StatusCode::UNAUTHORIZED);

    let helper = services.runtime_token_service.issue(
        &owner.id,
        &conversation_id,
        TEAM_RUNTIME_TOKEN_SESSION_GENERATION,
        [RuntimeTokenScope::ConversationHelper],
    );
    let mut helper_request = get_request(&route);
    let headers = helper_request.headers_mut();
    headers.insert(RUNTIME_USER_ID_HEADER, owner.id.parse().unwrap());
    headers.insert(RUNTIME_CONVERSATION_ID_HEADER, conversation_id.parse().unwrap());
    headers.insert(RUNTIME_TOKEN_HEADER, helper.token.parse().unwrap());
    assert_eq!(
        app.oneshot(helper_request).await.unwrap().status(),
        StatusCode::UNAUTHORIZED
    );
}
