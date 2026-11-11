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
    model_stream(delta, reason)
}

fn model_stream(delta: Value, reason: &str) -> Response {
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

async fn wait_for_confirmation(app: &Router, path: &str, token: &str) -> String {
    tokio::time::timeout(Duration::from_secs(12), async {
        loop {
            let response = app.clone().oneshot(get_with_token(path, token)).await.unwrap();
            let value = body_json(response).await;
            if let Some(call_id) = value["data"][0]["call_id"].as_str() {
                return call_id.to_owned();
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("tool waiting for approval")
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
    let pending = wait_for_confirmation(&app, &confirmations, &token).await;

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

#[cfg(unix)]
#[tokio::test]
async fn mem0_tools_bind_assistant_scopes_and_report_handshake_failures() {
    use std::io::Write;
    use std::os::unix::fs::OpenOptionsExt;
    use std::sync::Mutex;

    use aionui_db::{IAssistantDefinitionRepository, SqliteAssistantDefinitionRepository};
    use axum::http::HeaderMap;

    const KEY: &str = "fixture-mem0-service-key-not-for-production";
    const SEARCH_ARGS: &str = r#"{"query":"fixture preference","includeExpired":true,"since":"2026-11-01","until":"2026-11-02T08:00:00+08:00","timeField":"updated_at"}"#;
    const UPDATE_ARGS: &str = r#"{"memoryId":"fixture-id","category":"rp.habit"}"#;
    const LIST_ARGS: &str =
        r#"{"categories":["work.todo"],"filters":{"priority":{"gte":2}},"limit":2,"cursor":"fixture-cursor"}"#;
    const INVALID_SEARCH_ARGS: &str = r#"{"query":"fixture preference","limit":"fixture-private-value"}"#;
    const CONFLICTING_DELETE_ARGS: &str = r#"{"memoryId":"fixture-id","query":"fixture-private-value"}"#;
    const PRIVATE_MARKER: &str = "fixture-private-remote-value";
    let (calls, mut captured) = mpsc::unbounded_channel();
    let (results, mut captured_results) = mpsc::unbounded_channel();
    let rejection = Arc::new(Mutex::new(None::<(&str, StatusCode)>));
    let server_rejection = rejection.clone();
    let router = Router::new()
        .route(
            "/v1/chat/completions",
            post(move |Json(request): Json<Value>| {
                let last = request["messages"].as_array().unwrap().last().unwrap();
                let response = if last["role"] == "user" {
                    let prompt = last["content"].to_string();
                    let (tool_name, arguments) = if prompt.contains("Reclassify") {
                        ("memory_update", UPDATE_ARGS)
                    } else if prompt.contains("Invalid search") {
                        ("memory_search", INVALID_SEARCH_ARGS)
                    } else if prompt.contains("Conflicting deletion") {
                        ("memory_delete", CONFLICTING_DELETE_ARGS)
                    } else if prompt.contains("Continue listing") {
                        ("memory_list", LIST_ARGS)
                    } else {
                        ("memory_search", SEARCH_ARGS)
                    };
                    let tool = request["tools"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .find(|tool| tool["function"]["name"] == tool_name)
                        .unwrap();
                    let parameters = &tool["function"]["parameters"];
                    assert!(parameters["properties"].get("agentId").is_none());
                    if tool_name == "memory_update" {
                        assert_eq!(parameters["properties"]["category"]["type"], "string");
                        assert_eq!(parameters["required"], json!(["memoryId"]));
                    } else if tool_name == "memory_search" {
                        assert_eq!(parameters["properties"]["includeExpired"]["type"], "boolean");
                        assert_eq!(parameters["properties"]["since"]["type"], "string");
                        assert_eq!(parameters["properties"]["until"]["type"], "string");
                        assert_eq!(
                            parameters["properties"]["timeField"]["enum"],
                            json!(["created_at", "updated_at"])
                        );
                    } else if tool_name == "memory_list" {
                        assert!(parameters["properties"].get("query").is_none());
                        assert_eq!(parameters["properties"]["categories"]["type"], "array");
                        assert_eq!(parameters["properties"]["filters"]["type"], "object");
                        assert_eq!(parameters["properties"]["cursor"]["type"], "string");
                        assert_eq!(parameters["properties"]["limit"]["maximum"], 1000);
                    }
                    model_stream(
                        json!({"tool_calls":[{
                            "index":0,"id":"call_memory_fixture","type":"function",
                            "function":{"name":tool_name,"arguments":arguments}
                        }]}),
                        "tool_calls",
                    )
                } else {
                    assert_eq!(last["role"], "tool");
                    results.send(last["content"].as_str().unwrap().to_owned()).unwrap();
                    model_stream(json!({"content":"Fixture reply"}), "stop")
                };
                std::future::ready(response)
            }),
        )
        .route(
            "/mcp",
            post(move |headers: HeaderMap, Json(request): Json<Value>| {
                let calls = calls.clone();
                let failure = *server_rejection.lock().unwrap();
                async move {
                    assert_eq!(
                        headers[header::AUTHORIZATION].to_str().unwrap(),
                        format!("Bearer {KEY}")
                    );
                    let user = headers["x-mem0-user-id"].to_str().unwrap();
                    let agent = headers["x-mem0-agent-id"].to_str().unwrap();
                    if let Some(("initialize", status)) = failure {
                        assert_eq!(request["method"], "initialize");
                        calls
                            .send((user.to_owned(), agent.to_owned(), "initialize".to_owned()))
                            .unwrap();
                        return (
                            status,
                            [(header::WWW_AUTHENTICATE, PRIVATE_MARKER)],
                            Json(json!({"error": PRIVATE_MARKER})),
                        )
                            .into_response();
                    }
                    let result = match request["method"].as_str().unwrap() {
                        "initialize" => json!({
                            "protocolVersion":"2025-03-26", "capabilities":{"tools":{}},
                            "serverInfo":{"name":"mem0-fixture","version":"1"}
                        }),
                        "notifications/initialized" => return StatusCode::ACCEPTED.into_response(),
                        "tools/list" => {
                            let tools = [
                                "memory_search",
                                "memory_list",
                                "memory_get",
                                "memory_add",
                                "memory_update",
                                "memory_delete",
                            ]
                            .map(|name| json!({"name":name,"inputSchema":{"type":"object"}}));
                            json!({"tools":tools})
                        }
                        "tools/call" => {
                            let action = request["params"]["name"].as_str().unwrap();
                            let expected = match action {
                                "memory_search" if failure == Some(("tools/call", StatusCode::OK)) => {
                                    INVALID_SEARCH_ARGS
                                }
                                "memory_search" => SEARCH_ARGS,
                                "memory_list" => LIST_ARGS,
                                "memory_update" => UPDATE_ARGS,
                                other => panic!("unexpected memory tool {other}"),
                            };
                            assert_eq!(
                                request["params"]["arguments"],
                                serde_json::from_str::<Value>(expected).unwrap()
                            );
                            calls
                                .send((user.to_owned(), agent.to_owned(), action.to_owned()))
                                .unwrap();
                            if let Some(("tools/call", status)) = failure {
                                if !status.is_success() {
                                    return (status, Json(json!({"error": PRIVATE_MARKER}))).into_response();
                                }
                                let diagnostic = json!({
                                    "code":"invalid_arguments",
                                    "message":"Arguments do not match the tool schema.",
                                    "issues":[{"path":["limit"],"reason":"invalid_type"}]
                                });
                                json!({"isError":true,"content":[{"type":"text","text":diagnostic.to_string()}]})
                            } else if action == "memory_list" {
                                let page =
                                    json!({"memories":[],"count":0,"truncated":true,"nextCursor":"fixture-next"});
                                json!({"content":[{"type":"text","text":page.to_string()}]})
                            } else {
                                json!({"content":[{"type":"text","text":"{\"memory\":\"fixture memory result\"}"}]})
                            }
                        }
                        method => panic!("unexpected MCP method {method}"),
                    };
                    Json(json!({"jsonrpc":"2.0","id":request["id"],"result":result})).into_response()
                }
            }),
        );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    let temp = tempfile::tempdir().unwrap();
    let mut private_config = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(temp.path().join("mem0-mcp.json"))
        .unwrap();
    write!(
        private_config,
        "{}",
        json!({"url":format!("{base}/mcp"),"service_bearer":KEY})
    )
    .unwrap();
    let config = AppConfig {
        data_dir: temp.path().to_path_buf(),
        work_dir: temp.path().join("workspace"),
        ..Default::default()
    };
    let db = aionui_db::init_database_memory().await.unwrap();
    let services = AppServices::from_config(db, &config).await.unwrap();
    let mut app = create_router(&services).await.unwrap();
    let (token, csrf) = setup_and_login(&mut app, &services, "admin", "StrongP@ss1").await;
    let auth = (token.as_str(), csrf.as_str());
    let user = services.user_repo.find_by_username("admin").await.unwrap().unwrap();
    let definitions = SqliteAssistantDefinitionRepository::new(services.database.pool().clone());
    let provider = send_json(
        &app,
        "POST",
        "/api/providers",
        json!({
            "platform":"openai", "name":"Mem0 fixture model", "base_url":format!("{base}/v1"),
            "api_key":"local-test", "models":["stub"], "model_protocols":{"stub":"openai"}
        }),
        auth,
        StatusCode::CREATED,
    )
    .await;

    for (index, (failure, action, expected_code)) in [
        (None, "memory_search", None),
        (None, "memory_update", None),
        (None, "memory_list", None),
        (
            Some(("initialize", StatusCode::BAD_REQUEST)),
            "memory_search",
            Some("memory_handshake_rejected"),
        ),
        (
            Some(("initialize", StatusCode::UNAUTHORIZED)),
            "memory_update",
            Some("memory_auth_failed"),
        ),
        (
            Some(("initialize", StatusCode::FORBIDDEN)),
            "memory_search",
            Some("memory_access_denied"),
        ),
        (
            Some(("initialize", StatusCode::SERVICE_UNAVAILABLE)),
            "memory_update",
            Some("memory_unavailable"),
        ),
        (
            Some(("tools/call", StatusCode::SERVICE_UNAVAILABLE)),
            "memory_update",
            Some("write_unconfirmed"),
        ),
        (
            Some(("tools/call", StatusCode::OK)),
            "memory_search",
            Some("invalid_arguments"),
        ),
        (
            Some(("initialize", StatusCode::BAD_REQUEST)),
            "memory_delete",
            Some("invalid_memory_arguments"),
        ),
    ]
    .into_iter()
    .enumerate()
    {
        *rejection.lock().unwrap() = failure;
        let prompt = match (action, expected_code) {
            ("memory_search", Some("invalid_arguments")) => "Invalid search limit",
            ("memory_search", _) => "Recall a fixture preference",
            ("memory_list", _) => "Continue listing fixture tasks",
            ("memory_delete", _) => "Conflicting deletion arguments",
            _ => "Reclassify a fixture preference",
        };
        let assistant = send_json(
            &app,
            "POST",
            "/api/assistants",
            json!({
                "name":format!("Memory fixture {index}"),"agent_id":"632f31d2"
            }),
            auth,
            StatusCode::CREATED,
        )
        .await;
        let assistant_id = assistant["id"].as_str().unwrap();
        let definition = definitions
            .get_by_assistant_id_for_user(&user.id, assistant_id)
            .await
            .unwrap()
            .unwrap();
        let assistant_path = format!("/api/assistants/{assistant_id}");
        let detail = send_json(&app, "GET", &assistant_path, Value::Null, auth, StatusCode::OK).await;
        assert_eq!(detail["defaults"]["mem0_memory_configured"], true);
        assert_eq!(detail["defaults"]["mem0_memory_enabled"], false);
        send_json(
            &app,
            "PUT",
            &assistant_path,
            json!({
                "defaults":{"mem0_memory_enabled":true}
            }),
            auth,
            StatusCode::OK,
        )
        .await;
        let detail = send_json(&app, "GET", &assistant_path, Value::Null, auth, StatusCode::OK).await;
        assert_eq!(detail["defaults"]["mem0_memory_enabled"], true);
        let conversation = send_json(
            &app,
            "POST",
            "/api/conversations",
            json!({
                "type":"aionrs", "assistant":{"id":assistant_id,"locale":"en-US"},
                "model":{"provider_id":provider["id"],"model":"stub"}, "extra":{"session_mode":"default"}
            }),
            auth,
            StatusCode::CREATED,
        )
        .await;
        let path = format!("/api/conversations/{}", conversation["id"].as_str().unwrap());
        send_json(
            &app,
            "POST",
            &format!("{path}/messages"),
            json!({"content":prompt}),
            auth,
            StatusCode::ACCEPTED,
        )
        .await;
        let confirmations = format!("{path}/confirmations");
        let pending = wait_for_confirmation(&app, &confirmations, &token).await;
        send_json(
            &app,
            "POST",
            &format!("{confirmations}/{pending}/confirm"),
            json!({"msg_id":"", "data":{"value":"proceed_once"}}),
            auth,
            StatusCode::OK,
        )
        .await;
        let user_scope = format!("aion-user-{}", user.id);
        let agent_scope = format!("aion-assistant-{}", definition.id);
        if action != "memory_delete" {
            let scope = tokio::time::timeout(Duration::from_secs(12), captured.recv())
                .await
                .unwrap()
                .unwrap();
            let method = if matches!(failure, Some(("initialize", _))) {
                "initialize"
            } else {
                action
            };
            assert_eq!(scope, (user_scope.clone(), agent_scope.clone(), method.to_owned()));
        }
        let result = tokio::time::timeout(Duration::from_secs(12), captured_results.recv())
            .await
            .unwrap()
            .unwrap();
        let result_json: Value = serde_json::from_str(&result).unwrap();
        if let Some(code) = expected_code {
            assert_eq!(result_json["code"], code);
            assert!(!result.contains(PRIVATE_MARKER));
            assert!(!result.contains(KEY));
            assert!(!result.contains(&user_scope));
            assert!(!result.contains(&agent_scope));
            assert!(!result.contains("fixture-private-value"));
            if code == "invalid_arguments" {
                assert_eq!(
                    result_json["issues"],
                    json!([{"path":["limit"],"reason":"invalid_type"}])
                );
            }
            if code == "invalid_memory_arguments" {
                assert_eq!(
                    result_json["issues"],
                    json!([{"path":[],"reason":"conflicting_fields","fields":["memoryId","query"]}])
                );
                assert!(matches!(captured.try_recv(), Err(mpsc::error::TryRecvError::Empty)));
            }
        } else if action == "memory_list" {
            assert_eq!(result_json["nextCursor"], "fixture-next");
            assert_eq!(result_json["truncated"], true);
        } else {
            assert_eq!(result_json["memory"], "fixture memory result");
        }
        wait_for_finished_turn(&app, &path, &token).await;
    }
    server.abort();
}
