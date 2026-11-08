use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};

use aionui_db::{
    Database, IConversationRepository, SqliteAsyncImageJobRepository, SqliteConversationRepository,
    SqliteMcpServerRepository, init_database, init_database_memory,
};
use aionui_realtime::BroadcastEventBus;
use axum::extract::{Json, State as AxumState};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::{Router, routing::post};

use super::protocol::fingerprint;
use super::*;

const USER: &str = "image-user";
const OTHER_USER: &str = "other-user";
const ORIGINAL: &str = "original-conversation";
const SWITCHED: &str = "switched-conversation";
const SERVER: &str = "image-backend";
const PNG: &str = "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAAAAAA6fptVAAAACklEQVR4nGOoBwAAgQCA05RTSgAAAABJRU5ErkJggg==";
// A synthetic 1×1 PNG with correct chunk CRCs and invalid zlib data in IDAT.
const INVALID_PIXELS_PNG: &str =
    "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAIAAACQd1PeAAAAEUlEQVRub3QgYSB6bGliIHN0cmVhbUUA3FEAAAAASUVORK5CYII=";

#[derive(Default)]
struct MockBackend {
    lost_reply: AtomicBool,
    complete: AtomicBool,
    invalid_pixels: AtomicBool,
    requests: Mutex<Vec<Value>>,
}

async fn serve_backend() -> (String, Arc<MockBackend>, tokio::task::JoinHandle<()>) {
    let state = Arc::new(MockBackend::default());
    let router = Router::new().route("/mcp", post(mcp)).with_state(state.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/mcp", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    (url, state, server)
}

async fn mcp(AxumState(state): AxumState<Arc<MockBackend>>, Json(body): Json<Value>) -> Response {
    let id = body.get("id").cloned();
    let method = body["method"].as_str().unwrap();
    let result = match method {
        "initialize" => json!({
            "protocolVersion": "2025-03-26",
            "capabilities": { "tools": {} },
            "serverInfo": { "name": "aggregate-mock", "version": "1" }
        }),
        "notifications/initialized" => return StatusCode::ACCEPTED.into_response(),
        "tools/list" => json!({
            "tools": [
                { "name": "image_submit", "inputSchema": { "type": "object", "required": ["request_id", "prompt"], "properties": {
                    "request_id": { "type": "string" }, "prompt": { "type": "string" },
                    "images": { "type": "array" }, "size": { "type": "string" }, "preset": { "type": "string" }
                } } },
                { "name": "image_status", "inputSchema": { "type": "object", "required": ["task_id"], "properties": { "task_id": { "type": "string" } } } },
                { "name": "image_result", "inputSchema": { "type": "object", "required": ["task_id"], "properties": { "task_id": { "type": "string" } } } }
            ]
        }),
        "tools/call" => {
            let args = &body["params"]["arguments"];
            match body["params"]["name"].as_str().unwrap() {
                "image_submit" => {
                    assert!(args.get("provider").is_none() && args.get("model").is_none());
                    state.requests.lock().unwrap().push(args.clone());
                    if state.lost_reply.swap(false, Ordering::SeqCst) {
                        return StatusCode::BAD_GATEWAY.into_response();
                    }
                    structured(json!({ "task_id": args["request_id"], "state": "pending" }), false)
                }
                "image_status" => {
                    let received = state
                        .requests
                        .lock()
                        .unwrap()
                        .iter()
                        .any(|request| request["request_id"] == args["task_id"]);
                    if received {
                        let status = if state.complete.load(Ordering::SeqCst) {
                            "completed"
                        } else {
                            "running"
                        };
                        structured(json!({ "task_id": args["task_id"], "state": status }), false)
                    } else {
                        structured(json!({ "error_code": "task_not_found" }), true)
                    }
                }
                "image_result" => {
                    let png = if state.invalid_pixels.load(Ordering::SeqCst) {
                        INVALID_PIXELS_PNG
                    } else {
                        PNG
                    };
                    json!({
                        "structuredContent": {
                            "task_id": args["task_id"], "width": 1, "height": 1,
                            "sha256": fingerprint(&STANDARD.decode(png).unwrap())
                        },
                        "content": [{ "type": "image", "mimeType": "image/png", "data": png }],
                        "isError": false
                    })
                }
                _ => panic!("Unexpected mock tool"),
            }
        }
        _ => panic!("Unexpected mock method"),
    };
    Json(json!({ "jsonrpc": "2.0", "id": id, "result": result })).into_response()
}

fn structured(value: Value, is_error: bool) -> Value {
    json!({ "structuredContent": value, "content": [], "isError": is_error })
}

async fn database(url: &str) -> Database {
    let db = init_database_memory().await.unwrap();
    seed_database(&db, url).await;
    db
}

async fn seed_database(db: &Database, url: &str) {
    for (user, conversation) in [
        (USER, ORIGINAL),
        (USER, SWITCHED),
        (OTHER_USER, "other-user-conversation"),
    ] {
        sqlx::query(
            "INSERT OR IGNORE INTO users (id, user_type, username, password_hash, status, session_generation, created_at, updated_at)
             VALUES (?, 'local', ?, '', 'active', 1, 0, 0)",
        )
        .bind(user)
        .bind(user)
        .execute(db.pool())
        .await
        .unwrap();
        sqlx::query("INSERT INTO conversations (id, user_id, name, type, created_at, updated_at) VALUES (?, ?, 'test', 'aionrs', 0, 0)")
            .bind(conversation)
            .bind(user)
            .execute(db.pool())
            .await
            .unwrap();
    }
    sqlx::query(
        "INSERT INTO mcp_servers (id, user_id, name, transport_type, transport_config, purpose, is_default, enabled, created_at, updated_at)
         VALUES (?, ?, 'Aggregate', 'http', ?, 'async_image', 1, 1, 0, 0)",
    )
    .bind(SERVER)
    .bind(USER)
    .bind(json!({ "url": url }).to_string())
    .execute(db.pool())
    .await
    .unwrap();
}

fn jobs(db: &Database, events: Arc<BroadcastEventBus>) -> AsyncImageJobs {
    AsyncImageJobs::new(
        Arc::new(SqliteAsyncImageJobRepository::new(db.pool().clone())),
        Arc::new(SqliteMcpServerRepository::new(db.pool().clone())),
        Arc::new(SqliteConversationRepository::new(db.pool().clone())),
        events,
    )
}

fn request() -> AsyncImageRequest {
    AsyncImageRequest {
        user_id: USER.into(),
        conversation_id: ORIGINAL.into(),
        turn_id: "turn".into(),
        tool_call_id: "call".into(),
        prompt: "synthetic geometric shapes".into(),
        requested_server_id: None,
        size: None,
        preset: None,
        references: Vec::new(),
    }
}

async fn tick(jobs: &AsyncImageJobs, db: &Database) {
    sqlx::query("UPDATE async_image_jobs SET next_check_at = 0")
        .execute(db.pool())
        .await
        .unwrap();
    let mut claim = jobs
        .repo
        .claim(now_ms(), now_ms() + CLAIM_WINDOW, "test-lease")
        .await
        .unwrap()
        .unwrap();
    jobs.process_claim(&mut claim).await.unwrap();
}

#[tokio::test]
async fn lost_reply_restarts_and_delivers_one_card_only_to_original_conversation() {
    let (url, backend, server) = serve_backend().await;
    let directory = tempfile::tempdir().unwrap();
    let database_path = directory.path().join("aion.db");
    let db = init_database(&database_path).await.unwrap();
    seed_database(&db, &url).await;
    let events = Arc::new(BroadcastEventBus::new(16));
    let mut messages = events.subscribe();
    let service = jobs(&db, events.clone());
    let receipt = service.enqueue(request()).await.unwrap();
    assert_eq!(service.enqueue(request()).await.unwrap().task_id, receipt.task_id);
    assert_eq!(receipt.attachment_id, format!("async-image:{}", receipt.task_id));
    assert_eq!(messages.try_recv().unwrap().data["conversation_id"], ORIGINAL);
    backend.lost_reply.store(true, Ordering::SeqCst);
    tick(&service, &db).await;
    let state: String = sqlx::query_scalar("SELECT state FROM async_image_jobs")
        .fetch_one(db.pool())
        .await
        .unwrap();
    assert_eq!(state, "submitting");

    // The active view can change, and a new service reopens the persisted UUID.
    drop(service);
    db.close().await;
    let db = init_database(&database_path).await.unwrap();
    let restarted = jobs(&db, events.clone());
    assert_eq!(
        restarted
            .read_image(USER, SWITCHED, &receipt.task_id)
            .await
            .err()
            .unwrap()
            .code,
        "attachment_not_found"
    );
    backend.complete.store(true, Ordering::SeqCst);
    tick(&restarted, &db).await;
    let event = messages.try_recv().unwrap();
    assert_eq!(event.data["user_id"], USER);
    assert_eq!(event.data["conversation_id"], ORIGINAL);
    assert_eq!(
        event.data["data"]["update"]["raw_output"]["async_image"]["attachment_id"],
        receipt.task_id
    );
    assert!(messages.try_recv().is_err());

    for _ in 0..2 {
        let output = restarted.read_image(USER, ORIGINAL, &receipt.task_id).await.unwrap();
        assert_eq!(output.png, STANDARD.decode(PNG).unwrap());
    }
    assert_eq!(
        restarted
            .read_image(OTHER_USER, ORIGINAL, &receipt.task_id)
            .await
            .err()
            .unwrap()
            .code,
        "attachment_not_found"
    );
    assert_eq!(restarted.enqueue(request()).await.unwrap().state, "completed");
    assert_eq!(backend.requests.lock().unwrap().len(), 1);
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM messages WHERE type = 'acp_tool_call'")
        .fetch_one(db.pool())
        .await
        .unwrap();
    assert_eq!(count, 1);
    assert!(
        restarted
            .repo
            .claim(now_ms(), now_ms() + CLAIM_WINDOW, "duplicate")
            .await
            .unwrap()
            .is_none()
    );

    sqlx::query("DELETE FROM conversations WHERE id = ?")
        .bind(ORIGINAL)
        .execute(db.pool())
        .await
        .unwrap();
    assert_eq!(
        restarted
            .read_image(USER, ORIGINAL, &receipt.task_id)
            .await
            .err()
            .unwrap()
            .code,
        "attachment_not_found"
    );
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM async_image_jobs")
        .fetch_one(db.pool())
        .await
        .unwrap();
    assert_eq!(count, 0);
    server.abort();
}

#[tokio::test]
async fn endpoint_change_stops_the_original_job_and_foreign_scope_cannot_enqueue() {
    let (url, backend, server) = serve_backend().await;
    let db = database(&url).await;
    let events = Arc::new(BroadcastEventBus::new(16));
    let mut messages = events.subscribe();
    let service = jobs(&db, events);
    let mut foreign = request();
    foreign.user_id = OTHER_USER.into();
    assert_eq!(
        service.enqueue(foreign).await.err().unwrap().code,
        "conversation_not_found"
    );
    service.enqueue(request()).await.unwrap();
    messages.try_recv().unwrap();
    sqlx::query("UPDATE mcp_servers SET transport_config = ?")
        .bind(json!({ "url": url, "headers": { "X-Synthetic-Backend": "switched" } }).to_string())
        .execute(db.pool())
        .await
        .unwrap();
    tick(&service, &db).await;
    let event = messages.try_recv().unwrap();
    assert_eq!(
        event.data["data"]["update"]["raw_output"]["async_image"]["error_code"],
        "backend_changed"
    );
    assert!(backend.requests.lock().unwrap().is_empty());
    server.abort();
}

#[tokio::test]
async fn resetting_the_conversation_removes_completed_and_claimed_jobs() {
    let (url, backend, server) = serve_backend().await;
    let db = database(&url).await;
    let events = Arc::new(BroadcastEventBus::new(16));
    let mut messages = events.subscribe();
    let service = jobs(&db, events);
    let completed = service.enqueue(request()).await.unwrap();
    backend.complete.store(true, Ordering::SeqCst);
    tick(&service, &db).await;
    tick(&service, &db).await;
    assert!(service.read_image(USER, ORIGINAL, &completed.task_id).await.is_ok());

    let mut next = request();
    next.tool_call_id = "other-call".into();
    service.enqueue(next).await.unwrap();
    let mut claim = service
        .repo
        .claim(now_ms(), now_ms() + CLAIM_WINDOW, "held-lease")
        .await
        .unwrap()
        .unwrap();
    while messages.try_recv().is_ok() {}
    SqliteConversationRepository::new(db.pool().clone())
        .delete_messages_by_conversation(USER, ORIGINAL)
        .await
        .unwrap();
    service.process_claim(&mut claim).await.unwrap();
    assert_eq!(backend.requests.lock().unwrap().len(), 1);
    assert!(messages.try_recv().is_err());
    assert!(service.read_image(USER, ORIGINAL, &completed.task_id).await.is_err());
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM async_image_jobs")
        .fetch_one(db.pool())
        .await
        .unwrap();
    assert_eq!(count, 0);
    server.abort();
}

#[test]
fn validates_png_chunk_checksums() {
    let mut png = STANDARD.decode(PNG).unwrap();
    assert_eq!(png_dimensions(&png), Some((1, 1)));
    png[43] ^= 1;
    assert_eq!(png_dimensions(&png), None);
}

#[test]
fn decodes_an_interlaced_sixteen_bit_png() {
    // A synthetic 2×2 RGBA image with 16-bit samples and Adam7 interlacing.
    let png = STANDARD
        .decode("iVBORw0KGgoAAAANSUhEUgAAAAIAAAACEAYAAAFVIeHxAAAAGElEQVR4nGP4/58BCMAkiIBwgRSUAWQBAPgqDfPtoYDoAAAAAElFTkSuQmCC")
        .unwrap();
    assert_eq!(png_dimensions(&png), Some((2, 2)));
}

#[tokio::test]
async fn matching_metadata_with_invalid_pixels_fails_without_an_attachment() {
    let (url, backend, server) = serve_backend().await;
    let db = database(&url).await;
    let events = Arc::new(BroadcastEventBus::new(16));
    let mut messages = events.subscribe();
    let service = jobs(&db, events);
    let receipt = service.enqueue(request()).await.unwrap();
    backend.complete.store(true, Ordering::SeqCst);
    backend.invalid_pixels.store(true, Ordering::SeqCst);
    tick(&service, &db).await;
    tick(&service, &db).await;

    let state: String = sqlx::query_scalar("SELECT state FROM async_image_jobs WHERE task_id = ?")
        .bind(&receipt.task_id)
        .fetch_one(db.pool())
        .await
        .unwrap();
    assert_eq!(state, "failed");
    let mut last = messages.try_recv().unwrap();
    while let Ok(message) = messages.try_recv() {
        last = message;
    }
    assert_eq!(
        last.data["data"]["update"]["raw_output"]["async_image"]["error_code"],
        "invalid_backend_result"
    );
    assert_eq!(
        service
            .read_image(USER, ORIGINAL, &receipt.task_id)
            .await
            .err()
            .unwrap()
            .code,
        "attachment_not_found"
    );
    server.abort();
}

#[tokio::test]
async fn routes_new_jobs_by_explicit_choice_then_live_global_default() {
    let (url, backend, server) = serve_backend().await;
    let db = database(&url).await;
    let service = jobs(&db, Arc::new(BroadcastEventBus::new(16)));
    let original = service.enqueue(request()).await.unwrap();
    backend.complete.store(true, Ordering::SeqCst);
    tick(&service, &db).await;
    tick(&service, &db).await;
    sqlx::query(
        "INSERT INTO mcp_servers (id, user_id, name, transport_type, transport_config, purpose, enabled, created_at, updated_at)
         VALUES ('second', ?, 'Second', 'http', ?, 'async_image', 1, 1, 1),
                ('foreign', ?, 'Foreign', 'http', ?, 'async_image', 1, 1, 1)",
    )
    .bind(USER)
    .bind(json!({"url": url}).to_string())
    .bind(OTHER_USER)
    .bind(json!({"url": url}).to_string())
    .execute(db.pool())
    .await
    .unwrap();
    assert_eq!(service.backends(USER).await.unwrap().len(), 2);

    let enqueue = |call: &'static str, explicit: Option<&'static str>| {
        let mut input = request();
        input.tool_call_id = call.into();
        input.requested_server_id = explicit.map(str::to_owned);
        service.enqueue(input)
    };
    let selected_backend = |task_id: String| {
        let pool = db.pool().clone();
        async move {
            sqlx::query_scalar::<_, String>("SELECT server_id FROM async_image_jobs WHERE task_id = ?")
                .bind(task_id)
                .fetch_one(&pool)
                .await
                .unwrap()
        }
    };
    sqlx::query("UPDATE conversations SET extra = '{\"image_backend_id\":\"second\"}' WHERE id = ?")
        .bind(ORIGINAL)
        .execute(db.pool())
        .await
        .unwrap();
    assert_eq!(
        selected_backend(enqueue("saved", None).await.unwrap().task_id).await,
        "second"
    );
    assert_eq!(
        selected_backend(enqueue("once", Some(SERVER)).await.unwrap().task_id).await,
        SERVER
    );
    assert_eq!(
        enqueue("foreign", Some("foreign")).await.err().unwrap().code,
        "backend_unavailable"
    );

    sqlx::query("UPDATE mcp_servers SET enabled = 0 WHERE id = 'second'")
        .execute(db.pool())
        .await
        .unwrap();
    assert_eq!(
        enqueue("disabled", None).await.err().unwrap().code,
        "backend_unavailable"
    );
    assert_eq!(service.backends(USER).await.unwrap().len(), 1);

    sqlx::query("UPDATE conversations SET extra = '{}' WHERE id = ?")
        .bind(ORIGINAL)
        .execute(db.pool())
        .await
        .unwrap();
    sqlx::query("UPDATE mcp_servers SET is_default = 0 WHERE id = ?")
        .bind(SERVER)
        .execute(db.pool())
        .await
        .unwrap();
    assert_eq!(enqueue("singleton", None).await.err().unwrap().code, "backend_required");

    sqlx::query("UPDATE mcp_servers SET enabled = 1, is_default = 1 WHERE id = 'second'")
        .execute(db.pool())
        .await
        .unwrap();
    let mut edit = request();
    edit.tool_call_id = "edit".into();
    edit.references.push(AsyncImageReference::Generated(original.task_id));
    assert_eq!(
        selected_backend(service.enqueue(edit).await.unwrap().task_id).await,
        "second"
    );
    server.abort();
}
