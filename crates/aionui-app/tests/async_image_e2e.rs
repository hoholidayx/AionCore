//! Exercise the Rust tool hook with a mock model and a separate MCP protocol handler.

mod common;

use std::sync::{Arc, Mutex};
use std::time::Duration;

use aionui_app::{AppConfig, AppServices, create_router};
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
    let (delta, reason) = if after_tool {
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
        json!({"name":"image-fixture", "purpose":"async_image", "is_default":true,
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
    let new_conversation = json!({
        "type":"aionrs", "model":{"provider_id":provider["id"], "model":"stub"}, "extra":{"session_mode":"yolo"}
    });
    let original = post_json(
        &app,
        "/api/conversations",
        new_conversation.clone(),
        auth,
        StatusCode::CREATED,
    )
    .await;
    let switched = post_json(&app, "/api/conversations", new_conversation, auth, StatusCode::CREATED).await;
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
    assert_eq!(
        image.into_body().collect().await.unwrap().to_bytes().as_ref(),
        base64::engine::general_purpose::STANDARD.decode(PNG).unwrap()
    );
    assert_eq!(
        app.clone().oneshot(get_request(&image_route)).await.unwrap().status(),
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
    assert!(
        upstream
            .model_requests
            .lock()
            .unwrap()
            .iter()
            .all(|request| !request.to_string().contains(PNG))
    );

    shutdown.send(true).unwrap();
    worker.await.unwrap();
    upstream_task.abort();
}
