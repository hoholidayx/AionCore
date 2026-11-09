//! Exercise the real AionRS runtime against local model and MCP protocol fixtures.

mod common;

use std::sync::Arc;
use std::time::Duration;

use aionui_ai_agent::{RuntimeTokenScope, TEAM_RUNTIME_TOKEN_SESSION_GENERATION};
use aionui_app::{AppConfig, AppServices, create_router};
use aionui_auth::{RUNTIME_CONVERSATION_ID_HEADER, RUNTIME_TOKEN_HEADER, RUNTIME_USER_ID_HEADER};
use axum::body::Body;
use axum::extract::{Json, Path, State};
use axum::http::{Request, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::{Router, routing::post};
use serde_json::{Value, json};
use tokio::sync::mpsc;
use tower::ServiceExt;

use common::{body_json, get_with_token, json_with_token, setup_and_login};

struct ModelFixture {
    requests: mpsc::UnboundedSender<Value>,
}

async fn model(State(state): State<Arc<ModelFixture>>, Json(request): Json<Value>) -> Response {
    let last = request["messages"].as_array().unwrap().last().unwrap();
    let (delta, reason) = if last["role"] == "user" && last["content"].to_string().contains("first user turn") {
        let tool_name = request["tools"]
            .as_array()
            .unwrap()
            .iter()
            .filter_map(|tool| tool["function"]["name"].as_str())
            .find(|name| name.ends_with("before_tool"))
            .expect("old tool must be offered to the model");
        (
            json!({"tool_calls":[{
                "index":0,"id":"call_before_fixture","type":"function",
                "function":{"name":tool_name,"arguments":"{}"}
            }]}),
            "tool_calls",
        )
    } else {
        (json!({"content":"Fixture reply"}), "stop")
    };
    state.requests.send(request).unwrap();
    let chunks = [
        json!({"id":"stub","choices":[{"index":0,"delta":delta,"finish_reason":null}]}),
        json!({"id":"stub","choices":[{"index":0,"delta":{},"finish_reason":reason}]}),
    ];
    let body = format!("data: {}\n\ndata: {}\n\ndata: [DONE]\n\n", chunks[0], chunks[1]);
    ([(header::CONTENT_TYPE, "text/event-stream")], body).into_response()
}

async fn mcp(Path(tool_name): Path<String>, Json(request): Json<Value>) -> Response {
    let result = match request["method"].as_str().unwrap() {
        "initialize" => json!({
            "protocolVersion":"2025-03-26", "capabilities":{"tools":{}},
            "serverInfo":{"name":"fixture","version":"1"}
        }),
        "notifications/initialized" => return StatusCode::ACCEPTED.into_response(),
        "tools/list" => json!({"tools":[{"name":tool_name,"inputSchema":{"type":"object"}}]}),
        "tools/call" => json!({"content":[{"type":"text","text":"fixture MCP result"}]}),
        other => panic!("unexpected MCP method {other}"),
    };
    Json(json!({"jsonrpc":"2.0","id":request["id"],"result":result})).into_response()
}

async fn send_json(
    app: &Router,
    method: &str,
    path: &str,
    body: Value,
    (token, csrf): (&str, &str),
    expected: StatusCode,
) -> Value {
    let response = app
        .clone()
        .oneshot(json_with_token(method, path, body, token, csrf))
        .await
        .unwrap();
    let status = response.status();
    let value = body_json(response).await;
    assert_eq!(status, expected, "{method} {path}: {value}");
    value["data"].clone()
}

async fn skill_status(
    app: &Router,
    services: &AppServices,
    user_id: &str,
    conversation_id: &str,
    skill: &str,
) -> StatusCode {
    let issue = services.runtime_token_service.issue(
        user_id,
        conversation_id,
        TEAM_RUNTIME_TOKEN_SESSION_GENERATION,
        [RuntimeTokenScope::ConversationHelper],
    );
    let request = Request::get(format!("/api/runtime/skills/{skill}"))
        .header(RUNTIME_USER_ID_HEADER, user_id)
        .header(RUNTIME_CONVERSATION_ID_HEADER, conversation_id)
        .header(RUNTIME_TOKEN_HEADER, issue.token)
        .body(Body::empty())
        .unwrap();
    app.clone().oneshot(request).await.unwrap().status()
}

fn system_text(request: &Value) -> String {
    request["messages"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|message| message["role"] == "system")
        .map(|message| message["content"].to_string())
        .collect::<Vec<_>>()
        .join("\n")
}

async fn wait_for_finished_turn(app: &Router, path: &str, token: &str) {
    tokio::time::timeout(Duration::from_secs(12), async {
        loop {
            let response = app.clone().oneshot(get_with_token(path, token)).await.unwrap();
            if body_json(response).await["data"]["status"] == "finished" {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("conversation turn settled");
}

#[tokio::test]
async fn an_existing_conversation_adopts_a_saved_assistant_configuration_on_its_next_turn() {
    let (requests, mut captured) = mpsc::unbounded_channel();
    let upstream = Arc::new(ModelFixture { requests });
    let router = Router::new()
        .route("/v1/chat/completions", post(model))
        .route("/mcp/{tool_name}", post(mcp))
        .with_state(upstream.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });

    let temp = tempfile::tempdir().unwrap();
    let config = AppConfig {
        data_dir: temp.path().join("data"),
        work_dir: temp.path().join("workspace"),
        ..Default::default()
    };
    let db = aionui_db::init_database_memory().await.unwrap();
    let services = AppServices::from_config(db, &config).await.unwrap();
    let mut app = create_router(&services).await.unwrap();
    let (token, csrf) = setup_and_login(&mut app, &services, "admin", "StrongP@ss1").await;
    let auth = (token.as_str(), csrf.as_str());
    let user = services.user_repo.find_by_username("admin").await.unwrap().unwrap();

    let provider = send_json(
        &app,
        "POST",
        "/api/providers",
        json!({
            "platform":"openai", "name":"Fixture model", "base_url":format!("{base}/v1"),
            "api_key":"local-test", "models":["stub"], "model_protocols":{"stub":"openai"}
        }),
        auth,
        StatusCode::CREATED,
    )
    .await;
    let mut mcp_ids = Vec::new();
    for (skill, tool) in [("before-skill", "before_tool"), ("after-skill", "after_tool")] {
        let folder = temp.path().join("import-source").join(skill);
        std::fs::create_dir_all(&folder).unwrap();
        std::fs::write(
            folder.join("SKILL.md"),
            format!("---\nname: {skill}\ndescription: Fixture {skill}\n---\nFixture body"),
        )
        .unwrap();
        send_json(
            &app,
            "POST",
            "/api/skills/import",
            json!({"skill_path": folder}),
            auth,
            StatusCode::OK,
        )
        .await;
        let mcp = send_json(
            &app,
            "POST",
            "/api/mcp/servers",
            json!({
                "name": tool, "transport":{"type":"http", "url":format!("{base}/mcp/{tool}")}
            }),
            auth,
            StatusCode::CREATED,
        )
        .await;
        mcp_ids.push(mcp["id"].clone());
    }
    let assistant = send_json(
        &app,
        "POST",
        "/api/assistants",
        json!({
            "name":"Follow fixture", "agent_id":"632f31d2"
        }),
        auth,
        StatusCode::CREATED,
    )
    .await;
    let assistant_path = format!("/api/assistants/{}", assistant["id"].as_str().unwrap());
    send_json(&app, "PUT", &assistant_path, json!({
        "defaults":{"skills":{"mode":"fixed","value":["before-skill"]},"mcps":{"mode":"fixed","value":[mcp_ids[0]]}},
        "rules":{"kind":"write","locale":"en-US","content":"Original fixture rule"}
    }), auth, StatusCode::OK).await;
    let conversation = send_json(
        &app,
        "POST",
        "/api/conversations",
        json!({
            "type":"aionrs", "assistant":{"id":assistant["id"],"locale":"en-US"},
            "model":{"provider_id":provider["id"],"model":"stub"}, "extra":{"session_mode":"default"}
        }),
        auth,
        StatusCode::CREATED,
    )
    .await;
    let id = conversation["id"].as_str().unwrap();
    let path = format!("/api/conversations/{id}");
    let messages = format!("{path}/messages");
    send_json(
        &app,
        "POST",
        &messages,
        json!({"content":"first user turn"}),
        auth,
        StatusCode::ACCEPTED,
    )
    .await;
    let first = tokio::time::timeout(Duration::from_secs(12), captured.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(system_text(&first).contains("Original fixture rule"));
    assert!(system_text(&first).contains("before-skill"));
    assert!(first["tools"].to_string().contains("before_tool"));
    let confirmations = format!("{path}/confirmations");
    let pending = tokio::time::timeout(Duration::from_secs(12), async {
        loop {
            let response = app
                .clone()
                .oneshot(get_with_token(&confirmations, &token))
                .await
                .unwrap();
            let value = body_json(response).await;
            if let Some(call_id) = value["data"][0]["call_id"].as_str() {
                break call_id.to_owned();
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("old tool is waiting for approval");

    send_json(
        &app,
        "PUT",
        &assistant_path,
        json!({
            "defaults":{"skills":{"mode":"fixed","value":["after-skill"]},"mcps":{"mode":"fixed","value":[mcp_ids[1]]}},
            "rules":{"kind":"clear"}
        }),
        auth,
        StatusCode::OK,
    )
    .await;
    assert_eq!(
        skill_status(&app, &services, &user.id, id, "before-skill").await,
        StatusCode::OK
    );
    assert_eq!(
        skill_status(&app, &services, &user.id, id, "after-skill").await,
        StatusCode::FORBIDDEN
    );

    send_json(
        &app,
        "POST",
        &format!("{confirmations}/{pending}/confirm"),
        json!({"msg_id":"", "data":{"value":"proceed_once"}}),
        auth,
        StatusCode::OK,
    )
    .await;
    let continued = tokio::time::timeout(Duration::from_secs(12), captured.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(system_text(&continued).contains("Original fixture rule"));
    assert!(continued["messages"].to_string().contains("fixture MCP result"));
    wait_for_finished_turn(&app, &path, &token).await;

    send_json(
        &app,
        "POST",
        &messages,
        json!({"content":"second user turn"}),
        auth,
        StatusCode::ACCEPTED,
    )
    .await;
    let second = tokio::time::timeout(Duration::from_secs(12), captured.recv())
        .await
        .unwrap()
        .unwrap();
    let new_system = system_text(&second);
    assert!(new_system.contains("after-skill"));
    assert!(!new_system.contains("before-skill") && !new_system.contains("Original fixture rule"));
    let tools = second["tools"].to_string();
    assert!(tools.contains("after_tool") && !tools.contains("before_tool"));
    assert!(second["messages"].to_string().contains("first user turn"));
    assert_eq!(
        skill_status(&app, &services, &user.id, id, "before-skill").await,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        skill_status(&app, &services, &user.id, id, "after-skill").await,
        StatusCode::OK
    );
    wait_for_finished_turn(&app, &path, &token).await;
    server.abort();
}
