//! Exercise the Rust tool hook with a mock model and a separate MCP protocol handler.

mod common;

use std::sync::{Arc, Mutex};
use std::time::Duration;

use aionui_ai_agent::async_image::{AsyncImageQueue, AsyncImageRequest};
use aionui_ai_agent::{RuntimeTokenScope, TEAM_RUNTIME_TOKEN_SESSION_GENERATION};
use aionui_app::{AppConfig, AppServices, create_router};
use aionui_auth::{RUNTIME_CONVERSATION_ID_HEADER, RUNTIME_TOKEN_HEADER, RUNTIME_USER_ID_HEADER};
use axum::extract::{Json, State};
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::{Router, routing::post};
use base64::Engine;
use http_body_util::BodyExt;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tokio::sync::watch;
use tower::ServiceExt;

use common::{body_json, get_request, get_with_token, json_with_token, setup_and_login};

const PNG: &str = "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAAAAAA6fptVAAAACklEQVR4nGOoBwAAgQCA05RTSgAAAABJRU5ErkJggg==";

#[derive(Default)]
struct Upstream {
    model_requests: Mutex<Vec<Value>>,
    submissions: Mutex<Vec<Value>>,
}

async fn model(State(state): State<Arc<Upstream>>, Json(body): Json<Value>) -> Response {
    let messages = body["messages"].as_array().unwrap();
    let after_tool = messages.last().is_some_and(|message| message["role"] == "tool");
    let edit = messages
        .last()
        .is_some_and(|message| message["content"].to_string().contains("Edit the generated square."));
    let probe = messages
        .last()
        .is_some_and(|message| message["content"].to_string().contains("Check backend visibility."));
    let (call_id, arguments) = if edit {
        let result = messages.iter().rev().find(|message| message["role"] == "tool").unwrap();
        let receipt: Value = serde_json::from_str(result["content"].as_str().unwrap()).unwrap();
        (
            "call_edit_mock",
            json!({"prompt": "Make the square darker", "references": [receipt["attachment_id"]]}),
        )
    } else {
        ("call_image_mock", json!({"prompt": "Draw a small square"}))
    };
    state.model_requests.lock().unwrap().push(body);
    let (delta, reason) = if probe {
        (json!({"content": "Backend visibility checked."}), "stop")
    } else if after_tool {
        (json!({"content": "Task queued."}), "stop")
    } else {
        (
            json!({"tool_calls": [{
                "index": 0, "id": call_id, "type": "function",
                "function": {"name": "image_generate_async", "arguments": arguments.to_string()}
            }]}),
            "tool_calls",
        )
    };
    let chunks = [
        json!({"id": "mock", "choices": [{"index": 0, "delta": delta, "finish_reason": null}]}),
        json!({"id": "mock", "choices": [{"index": 0, "delta": {}, "finish_reason": reason}]}),
    ];
    let body = format!("data: {}\n\ndata: {}\n\ndata: [DONE]\n\n", chunks[0], chunks[1]);
    ([(header::CONTENT_TYPE, "text/event-stream")], body).into_response()
}

async fn mcp(State(state): State<Arc<Upstream>>, Json(body): Json<Value>) -> Response {
    let tool = |name: &str, fields: &[&str]| {
        let props: serde_json::Map<String, Value> = fields
            .iter()
            .map(|field| ((*field).to_owned(), json!({"type": "string"})))
            .collect();
        json!({"name": name, "inputSchema": {"type": "object", "properties": props, "required": fields}})
    };
    let arguments = &body["params"]["arguments"];
    let result = match body["method"].as_str().unwrap() {
        "initialize" => json!({
            "protocolVersion": "2025-03-26", "capabilities": {"tools": {}},
            "serverInfo": {"name": "local-image-fixture", "version": "1"}
        }),
        "notifications/initialized" => return StatusCode::ACCEPTED.into_response(),
        "tools/list" => {
            let mut submit = tool("image_submit", &["request_id", "prompt"]);
            submit["inputSchema"]["properties"]["images"] = json!({"type": "array", "items": {"type": "string"}});
            json!({"tools": [
                submit, tool("image_status", &["task_id"]), tool("image_result", &["task_id"]), tool("unrelated", &[])
            ]})
        }
        "tools/call" => match body["params"]["name"].as_str().unwrap() {
            "image_submit" => {
                state.submissions.lock().unwrap().push(arguments.clone());
                json!({"structuredContent": {"task_id": arguments["request_id"], "state": "queued"}, "content": []})
            }
            "image_status" => {
                json!({"structuredContent": {"task_id": arguments["task_id"], "state": "completed"}, "content": []})
            }
            "image_result" => {
                let hash = format!(
                    "{:x}",
                    Sha256::digest(base64::engine::general_purpose::STANDARD.decode(PNG).unwrap())
                );
                json!({
                    "structuredContent": {"task_id": arguments["task_id"], "width": 1, "height": 1, "sha256": hash},
                    "content": [{"type": "image", "mimeType": "image/png", "data": PNG}]
                })
            }
            _ => panic!("Unexpected MCP call"),
        },
        _ => panic!("Unexpected MCP request"),
    };
    Json(json!({"jsonrpc": "2.0", "id": body["id"], "result": result})).into_response()
}

async fn post_json(app: &Router, route: &str, body: Value, auth: (&str, &str), expected: StatusCode) -> Value {
    let response = app
        .clone()
        .oneshot(json_with_token("POST", route, body, auth.0, auth.1))
        .await
        .unwrap();
    let status = response.status();
    let value = body_json(response).await;
    assert_eq!(status, expected, "{route}: {value}");
    value["data"].clone()
}

#[tokio::test]
async fn model_receives_a_receipt_and_delayed_image_stays_in_its_original_conversation() {
    let upstream = Arc::new(Upstream::default());
    let router = Router::new()
        .route("/v1/chat/completions", post(model))
        .route("/mcp", post(mcp))
        .with_state(upstream.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base_url = format!("http://{}", listener.local_addr().unwrap());
    let upstream_task = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });

    let data = tempfile::tempdir().unwrap();
    let config = AppConfig {
        data_dir: data.path().join("data"),
        work_dir: data.path().join("workspace"),
        ..Default::default()
    };
    let db = aionui_db::init_database_memory().await.unwrap();
    let services = AppServices::from_config(db, &config).await.unwrap();
    let mut app = create_router(&services).await.unwrap();
    let (token, csrf) = setup_and_login(&mut app, &services, "admin", "StrongP@ss1").await;
    let auth = (token.as_str(), csrf.as_str());

    let provider = post_json(
        &app,
        "/api/providers",
        json!({"platform":"openai", "name":"Test model", "base_url":format!("{base_url}/v1"),
               "api_key":"local-test", "models":["stub"], "model_protocols":{"stub":"openai"}}),
        auth,
        StatusCode::CREATED,
    )
    .await;
    let server = post_json(
        &app,
        "/api/mcp/servers",
        json!({"name":"image-fixture", "purpose":"async_image",
               "transport":{"type":"http", "url":format!("{base_url}/mcp")}}),
        auth,
        StatusCode::CREATED,
    )
    .await;
    let server_id = server["id"].as_str().unwrap();
    post_json(
        &app,
        &format!("/api/mcp/servers/{server_id}/toggle"),
        json!({}),
        auth,
        StatusCode::OK,
    )
    .await;
    let default_response = app
        .clone()
        .oneshot(json_with_token(
            "PUT",
            "/api/mcp/image-default",
            json!({"server_id":server_id}),
            auth.0,
            auth.1,
        ))
        .await
        .unwrap();
    assert_eq!(default_response.status(), StatusCode::OK);
    let assistant = post_json(
        &app,
        "/api/assistants",
        json!({"id":"image-fixture-assistant","name":"Image assistant","agent_id":"632f31d2"}),
        auth,
        StatusCode::CREATED,
    )
    .await;
    let assistant_path = format!("/api/assistants/{}", assistant["id"].as_str().unwrap());
    let new_conversation = json!({
        "type":"aionrs",
        "assistant":{"id":assistant["id"]},
        "model":{"provider_id":provider["id"], "model":"stub"},
        "extra":{
            "session_mode":"yolo",
            "selected_mcp_server_ids":[server_id],
            "selected_session_mcp_servers":[{
                "id":"inline-image-fixture",
                "name":"image-fixture",
                "transport":{"type":"http","url":"http://127.0.0.1:1/mcp"}
            }]
        }
    });
    let original = post_json(
        &app,
        "/api/conversations",
        new_conversation.clone(),
        auth,
        StatusCode::CREATED,
    )
    .await;
    let mut switched_request = new_conversation;
    switched_request["extra"]["image_backend_id"] = json!(server_id);
    let switched = post_json(&app, "/api/conversations", switched_request, auth, StatusCode::CREATED).await;
    assert_eq!(switched["extra"]["image_backend_id"], server_id);
    let original = original["id"].as_str().unwrap();
    let switched = switched["id"].as_str().unwrap();
    post_json(
        &app,
        &format!("/api/conversations/{original}/messages"),
        json!({"content": "Create a small square."}),
        auth,
        StatusCode::ACCEPTED,
    )
    .await;

    let model_requests = tokio::time::timeout(Duration::from_secs(12), async {
        loop {
            let calls = upstream.model_requests.lock().unwrap().clone();
            if calls.len() >= 2 {
                break calls;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .unwrap_or_else(|_| {
        panic!(
            "Model never received a tool result; {} model requests",
            upstream.model_requests.lock().unwrap().len()
        )
    });
    let tools = model_requests[0]["tools"].to_string();
    assert!(tools.contains("image_generate_async"));
    assert!(tools.contains("unrelated"));
    assert!(!tools.contains("image_submit") && !tools.contains("image_status") && !tools.contains("image_result"));
    let result = model_requests[1]["messages"]
        .as_array()
        .unwrap()
        .iter()
        .find(|message| message["role"] == "tool")
        .expect("model receives receipt");
    let receipt: Value = serde_json::from_str(result["content"].as_str().unwrap()).unwrap();
    assert_eq!(receipt["state"], "pending");
    let task = receipt["task_id"].as_str().unwrap();
    assert_eq!(receipt["attachment_id"], format!("async-image:{task}"));
    assert!(!model_requests[1].to_string().contains(PNG));

    let switched_history = format!("/api/conversations/{switched}/messages");
    let response = app
        .clone()
        .oneshot(get_with_token(&switched_history, &token))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert!(!body_json(response).await.to_string().contains(task));

    let (shutdown, receiver) = watch::channel(false);
    let jobs = services.async_image_jobs.clone();
    let worker = tokio::spawn(async move { jobs.run(receiver).await });
    let image_route = format!("/api/conversations/{original}/async-images/{task}");
    let image = tokio::time::timeout(Duration::from_secs(12), async {
        loop {
            let result = app.clone().oneshot(get_with_token(&image_route, &token)).await.unwrap();
            if result.status() == StatusCode::OK {
                break result;
            }
            assert_eq!(result.status(), StatusCode::NOT_FOUND);
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("worker completes image");
    assert_eq!(image.headers()[header::CONTENT_TYPE], "image/png");
    assert_eq!(image.headers()[header::CACHE_CONTROL], "private, no-store");
    assert_eq!(
        image.into_body().collect().await.unwrap().to_bytes().as_ref(),
        base64::engine::general_purpose::STANDARD.decode(PNG).unwrap()
    );
    assert_eq!(
        app.clone().oneshot(get_request(&image_route)).await.unwrap().status(),
        StatusCode::UNAUTHORIZED
    );
    let user = services.user_repo.find_by_username("admin").await.unwrap().unwrap();
    let helper = services.runtime_token_service.issue(
        &user.id,
        original,
        TEAM_RUNTIME_TOKEN_SESSION_GENERATION,
        [RuntimeTokenScope::ConversationHelper],
    );
    let helper_get = |path: &str| {
        let mut request = get_request(path);
        let headers = request.headers_mut();
        headers.insert(RUNTIME_USER_ID_HEADER, user.id.parse().unwrap());
        headers.insert(RUNTIME_CONVERSATION_ID_HEADER, original.parse().unwrap());
        headers.insert(RUNTIME_TOKEN_HEADER, helper.token.parse().unwrap());
        request
    };
    assert_eq!(
        app.clone()
            .oneshot(helper_get(&format!("/api/conversations/{original}")))
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );
    assert_eq!(
        app.clone().oneshot(helper_get(&image_route)).await.unwrap().status(),
        StatusCode::UNAUTHORIZED
    );
    let switched_image = format!("/api/conversations/{switched}/async-images/{task}");
    assert_eq!(
        app.clone()
            .oneshot(get_with_token(&switched_image, &token))
            .await
            .unwrap()
            .status(),
        StatusCode::NOT_FOUND
    );

    let history = app
        .clone()
        .oneshot(get_with_token(
            &format!("/api/conversations/{original}/messages"),
            &token,
        ))
        .await
        .unwrap();
    let history = body_json(history).await;
    let cards = history["data"]["items"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|message| message["content"].to_string().contains("\"async_image\""))
        .collect::<Vec<_>>();
    assert_eq!(cards.len(), 1, "{history}");
    assert!(cards[0].to_string().contains(task));
    assert!(!cards[0].to_string().contains(PNG));
    assert_eq!(
        upstream.submissions.lock().unwrap().as_slice(),
        &[json!({"request_id":task, "prompt":"Draw a small square"})]
    );

    let second = post_json(
        &app,
        "/api/mcp/servers",
        json!({"name":"another-image-fixture", "purpose":"async_image",
               "transport":{"type":"http", "url":format!("{base_url}/mcp")}}),
        auth,
        StatusCode::CREATED,
    )
    .await;
    let second_id = second["id"].as_str().unwrap();
    let update_path = format!("/api/conversations/{original}");
    let save_backend = |choice: Value| {
        app.clone().oneshot(json_with_token(
            "PATCH",
            &update_path,
            json!({"extra":{"image_backend_id":choice}}),
            auth.0,
            auth.1,
        ))
    };
    let disabled = save_backend(json!(second_id)).await.unwrap();
    assert_eq!(disabled.status(), StatusCode::BAD_REQUEST);
    assert!(
        body_json(disabled)
            .await
            .to_string()
            .contains("selected image backend is unavailable")
    );
    let invalid = save_backend(json!(true)).await.unwrap();
    assert_eq!(invalid.status(), StatusCode::BAD_REQUEST);
    assert!(
        body_json(invalid)
            .await
            .to_string()
            .contains("must be a nonempty backend ID or null")
    );
    post_json(
        &app,
        &format!("/api/mcp/servers/{second_id}/toggle"),
        json!({}),
        auth,
        StatusCode::OK,
    )
    .await;
    let saved = save_backend(json!(second_id)).await.unwrap();
    assert_eq!(saved.status(), StatusCode::OK);
    assert_eq!(body_json(saved).await["data"]["extra"]["image_backend_id"], second_id);

    post_json(
        &app,
        &format!("/api/conversations/{original}/messages"),
        json!({"content": "Edit the generated square."}),
        auth,
        StatusCode::ACCEPTED,
    )
    .await;
    let edit = tokio::time::timeout(Duration::from_secs(12), async {
        loop {
            let request = upstream.submissions.lock().unwrap().get(1).cloned();
            if let Some(request) = request {
                break request;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("edit uses the completed image reference");
    assert_eq!(edit["prompt"], "Make the square darker");
    assert_eq!(edit["images"], json!([PNG]));
    assert_ne!(edit["request_id"], task);
    let edit_backend = sqlx::query_scalar::<_, String>("SELECT server_id FROM async_image_jobs WHERE task_id = ?")
        .bind(edit["request_id"].as_str().unwrap())
        .fetch_one(services.database.pool())
        .await
        .unwrap();
    assert_eq!(edit_backend, second_id);
    assert!(
        upstream
            .model_requests
            .lock()
            .unwrap()
            .iter()
            .skip(2)
            .any(|request| request["tools"].to_string().contains(second_id))
    );
    let cleared = save_backend(Value::Null).await.unwrap();
    assert_eq!(cleared.status(), StatusCode::OK);
    assert!(body_json(cleared).await["data"]["extra"]["image_backend_id"].is_null());
    assert!(
        upstream
            .model_requests
            .lock()
            .unwrap()
            .iter()
            .all(|request| !request.to_string().contains(PNG))
    );

    let set_allowed = |ids: Value| {
        app.clone().oneshot(json_with_token(
            "PUT",
            &assistant_path,
            json!({"defaults":{"async_image_backend_ids":ids}}),
            auth.0,
            auth.1,
        ))
    };
    assert_eq!(set_allowed(json!([second_id])).await.unwrap().status(), StatusCode::OK);
    assert_eq!(
        save_backend(json!(server_id)).await.unwrap().status(),
        StatusCode::BAD_REQUEST
    );
    let forged_call = AsyncImageRequest {
        user_id: user.id.clone(),
        conversation_id: switched.to_owned(),
        turn_id: "restricted-turn".into(),
        tool_call_id: "restricted-call".into(),
        prompt: "Draw a square.".into(),
        requested_server_id: Some(server_id.into()),
        size: None,
        preset: None,
        references: Vec::new(),
    };
    assert_eq!(
        services.async_image_jobs.enqueue(forged_call).await.err().unwrap().code,
        "backend_unavailable"
    );

    for (allowed, expected_backend) in [(json!([second_id]), Some(second_id)), (json!([]), None)] {
        assert_eq!(set_allowed(allowed).await.unwrap().status(), StatusCode::OK);
        let count = upstream.model_requests.lock().unwrap().len();
        post_json(
            &app,
            &format!("/api/conversations/{switched}/messages"),
            json!({"content":"Check backend visibility."}),
            auth,
            StatusCode::ACCEPTED,
        )
        .await;
        let tools = tokio::time::timeout(Duration::from_secs(12), async {
            loop {
                let response = app
                    .clone()
                    .oneshot(get_with_token(&format!("/api/conversations/{switched}"), &token))
                    .await
                    .unwrap();
                if body_json(response).await["data"]["status"] == "finished"
                    && let Some(request) = upstream.model_requests.lock().unwrap().get(count)
                {
                    break request["tools"].to_string();
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .expect("model received the scoped tool inventory");
        assert_eq!(tools.contains("image_generate_async"), expected_backend.is_some());
        assert!(!tools.contains(server_id));
        if let Some(id) = expected_backend {
            assert!(tools.contains(id));
        }
    }

    shutdown.send(true).unwrap();
    worker.await.unwrap();
    upstream_task.abort();
}
