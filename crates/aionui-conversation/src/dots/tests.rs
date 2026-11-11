use std::sync::Mutex;

use aionui_db::{SqliteConversationRepository, SqliteDotsRepository, init_database_memory};
use aionui_realtime::BroadcastEventBus;
use axum::{
    Json, Router,
    body::Bytes,
    extract::{Path as Param, Query, State},
    http::{HeaderMap, StatusCode},
    routing::{get, post, put},
};

use super::*;

const OWNER: &str = "system_default_user";
const ARTIFACT: &str = "art_00000000000000000000000000000001";
const REQUEST_A: &str = "req_0000000000000000000000000000000a";
const REQUEST_B: &str = "req_0000000000000000000000000000000b";

#[derive(Default)]
struct Fixture {
    cursor: i64,
    submissions: Vec<Value>,
    received: Vec<Value>,
    uploaded: Option<Vec<u8>>,
    deleted: bool,
}

#[derive(Clone)]
struct Mock {
    base: Url,
    data: Arc<Mutex<Fixture>>,
}

async fn cursor(State(mock): State<Mock>) -> Json<Value> {
    Json(json!({"cursor": mock.data.lock().unwrap().cursor}))
}
async fn ack(State(mock): State<Mock>, Json(value): Json<Value>) -> Json<Value> {
    let mut data = mock.data.lock().unwrap();
    data.cursor = data.cursor.max(value["sequence"].as_i64().unwrap());
    Json(json!({"cursor": data.cursor}))
}
async fn received(
    State(mock): State<Mock>,
    Query(query): Query<std::collections::HashMap<String, String>>,
) -> Json<Value> {
    let after: i64 = query["after"].parse().unwrap();
    let data = mock.data.lock().unwrap();
    let messages: Vec<_> = data
        .received
        .iter()
        .filter(|value| value["sequence"].as_i64().unwrap() > after)
        .collect();
    Json(json!({"messages": messages}))
}
async fn submit(State(mock): State<Mock>, headers: HeaderMap, Json(value): Json<Value>) -> (StatusCode, Json<Value>) {
    assert_eq!(headers["authorization"], "Bearer test-token");
    let id = if value["text"] == "first" { REQUEST_A } else { REQUEST_B };
    mock.data.lock().unwrap().submissions.push(value);
    (
        StatusCode::ACCEPTED,
        Json(json!({"request_id": id, "delivery_status": "queued", "reply_status": "pending"})),
    )
}
async fn upload(State(mock): State<Mock>, Json(value): Json<Value>) -> Json<Value> {
    assert_eq!(value["filename"], "arbitrary.dataformat");
    assert_eq!(value["content_md5"], "kAFQmDzST7DWlj99KOF/cg==");
    Json(
        json!({"status": "pending", "artifact_id": ARTIFACT, "upload_url": mock.base.join("storage").unwrap(),
        "required_headers": {"Content-Length": "3", "Content-MD5": "kAFQmDzST7DWlj99KOF/cg==", "If-None-Match": "*", "Content-Disposition": "attachment", "Cache-Control": "private"}}),
    )
}
async fn put_file(State(mock): State<Mock>, headers: HeaderMap, bytes: Bytes) -> StatusCode {
    assert!(headers.get("authorization").is_none());
    mock.data.lock().unwrap().uploaded = Some(bytes.to_vec());
    StatusCode::OK
}
async fn grant(State(mock): State<Mock>, Param(id): Param<String>) -> (StatusCode, Json<Value>) {
    assert_eq!(id, ARTIFACT);
    if mock.data.lock().unwrap().deleted {
        return (StatusCode::NOT_FOUND, Json(json!({})));
    }
    (
        StatusCode::OK,
        Json(
            json!({"download_url": mock.base.join("storage").unwrap(), "filename": "arbitrary.dataformat", "content_type": "application/octet-stream", "size_bytes": 3}),
        ),
    )
}

async fn mock_bridge() -> (Mock, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = Url::parse(&format!("http://{}/", listener.local_addr().unwrap())).unwrap();
    let mock = Mock {
        base,
        data: Arc::default(),
    };
    let app = Router::new()
        .route("/api/received", get(received))
        .route("/api/consumers/{consumer}", get(cursor))
        .route("/api/consumers/{consumer}/ack", post(ack))
        .route(
            "/api/subscriptions",
            get(|| async { Json(json!({"subscriptions": [{"channel": "default", "id": "subscription"}]})) }),
        )
        .route("/api/messages", post(submit))
        .route(
            "/api/messages/{id}",
            get(|Param(id): Param<String>| async move {
                Json(json!({"request_id": id, "delivery_status": "accepted", "reply_status": "pending"}))
            }),
        )
        .route("/api/artifacts/uploads", post(upload))
        .route("/api/artifacts/{id}/download", get(grant))
        .route(
            "/storage",
            put(put_file).get(|headers: HeaderMap| async move {
                assert!(headers.get("authorization").is_none());
                "abc"
            }),
        )
        .with_state(mock.clone());
    (
        mock,
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() }),
    )
}

fn config(mock: &Mock) -> DotsConfig {
    DotsConfig {
        base: mock.base.clone(),
        token: "test-token".into(),
        channel: "default".into(),
        subscription: None,
        owner_id: None,
    }
}

fn receipt(seq: i64, kind: &str, request: Option<&str>, text: &str, attachments: &[&str]) -> Value {
    json!({"sequence": seq, "kind": kind, "request_id": request, "channel": "default", "text": text,
        "artifact_ids": attachments, "metadata": {}, "id": uuid::Uuid::new_v4()})
}

async fn seed(repo: &dyn IConversationRepository, root: &Path, id: &str, is_default: bool) -> ConversationRow {
    let workspace = root.join(id);
    tokio::fs::create_dir(&workspace).await.unwrap();
    let row = ConversationRow {
        id: id.into(),
        user_id: OWNER.into(),
        name: id.into(),
        r#type: "acp".into(),
        extra: json!({"backend":"dots", "workspace":workspace, "dots_default":is_default}).to_string(),
        model: None,
        status: Some("pending".into()),
        source: Some("aionui".into()),
        channel_chat_id: None,
        pinned: false,
        pinned_at: None,
        created_at: now_ms(),
        updated_at: now_ms(),
        project_id: None,
        folder_id: None,
        name_source: None,
    };
    repo.create(&row).await.unwrap();
    row
}

fn user_message(id: &str, conversation: &str, text: &str) -> MessageRow {
    let mut row = DotsService::receipt_message(id.into(), conversation.into(), text, "user", &[], None);
    row.position = Some("right".into());
    row
}

#[tokio::test]
async fn restart_routes_out_of_order_replies_and_keeps_pushes_in_the_default_conversation() {
    let db = init_database_memory().await.unwrap();
    let repo = Arc::new(SqliteDotsRepository::new(db.pool().clone()));
    let conversations = Arc::new(SqliteConversationRepository::new(db.pool().clone()));
    let temp = tempfile::tempdir().unwrap();
    let a = seed(conversations.as_ref(), temp.path(), "chat-a", false).await;
    let b = seed(conversations.as_ref(), temp.path(), "chat-b", false).await;
    let default = seed(conversations.as_ref(), temp.path(), "default", true).await;
    let (mock, server) = mock_bridge().await;
    mock.data
        .lock()
        .unwrap()
        .received
        .push(receipt(1, "push", None, "old push", &[]));
    let new_service = || {
        let service = DotsService::new(
            config(&mock),
            OWNER.into(),
            repo.clone(),
            conversations.clone(),
            Arc::new(BroadcastEventBus::new(16)),
        )
        .unwrap();
        service.set_default(default.id.clone());
        service
    };
    let service = new_service();
    let source = temp.path().join("arbitrary.dataformat");
    tokio::fs::write(&source, b"abc").await.unwrap();
    service
        .enqueue(
            &workspace(&a).unwrap(),
            &user_message("msg-a", &a.id, "first"),
            "first",
            &[source.to_string_lossy().into_owned()],
        )
        .await
        .unwrap();
    service
        .enqueue(
            &workspace(&b).unwrap(),
            &user_message("msg-b", &b.id, "second"),
            "second",
            &[],
        )
        .await
        .unwrap();
    service.poll(&mut None).await.unwrap();
    {
        let data = mock.data.lock().unwrap();
        assert_eq!(data.cursor, 1);
        assert_eq!(data.uploaded.as_deref(), Some(b"abc".as_slice()));
        assert_eq!(data.submissions.len(), 2);
        assert!(
            data.submissions
                .iter()
                .any(|item| item["artifact_ids"] == json!([ARTIFACT]))
        );
    }
    // Simulate losing the successful HTTP response for B: Bridge still has
    // its reply with the original request metadata after the restart.
    sqlx::query("UPDATE dots_bridge_requests SET request_id = NULL WHERE message_id = 'msg-b'")
        .execute(db.pool())
        .await
        .unwrap();
    let mut reply_b = receipt(2, "reply", Some(REQUEST_B), "second reply", &[]);
    reply_b["metadata"] = json!({"aion_binding": service.binding, "aion_message_id": "msg-b"});
    let replies = vec![
        reply_b,
        receipt(3, "push", None, "", &[ARTIFACT]),
        receipt(4, "reply", Some(REQUEST_A), "first reply", &[ARTIFACT]),
        receipt(
            5,
            "reply",
            Some("req_ffffffffffffffffffffffffffffffff"),
            "unmatched",
            &[],
        ),
    ];
    drop(service);
    mock.data.lock().unwrap().received.extend(replies.clone());
    let restarted = new_service();
    let mut pos = None;
    restarted.poll(&mut pos).await.unwrap();
    for (event, destination) in replies.iter().zip([&b.id, &default.id, &a.id]) {
        let id = format!("dots-{}-{}", &restarted.binding[..12], event["id"].as_str().unwrap());
        let row = conversations
            .get_message(OWNER, destination, &id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(row.conversation_id, *destination);
        assert!(
            conversations
                .get_message("other-user", destination, &id)
                .await
                .unwrap()
                .is_none()
        );
    }
    assert_eq!(pos, Some(5));
    assert!(repo.pending(&restarted.binding, OWNER).await.unwrap().is_empty());
    restarted.poll(&mut Some(1)).await.unwrap();
    assert_eq!(
        repo.artifact_binding("other-user", &a.id, ARTIFACT).await.unwrap(),
        None
    );
    assert_eq!(repo.artifact_binding(OWNER, &b.id, ARTIFACT).await.unwrap(), None);

    let cache = workspace(&a).unwrap().join(".dots/inbox").join(ARTIFACT);
    assert_eq!(tokio::fs::read(&cache).await.unwrap(), b"abc");
    tokio::fs::remove_file(&cache).await.unwrap();
    assert_eq!(
        restarted
            .fetch_attachment(&workspace(&a).unwrap(), ARTIFACT)
            .await
            .unwrap()
            .bytes,
        b"abc"
    );
    tokio::fs::remove_file(&cache).await.unwrap();
    mock.data.lock().unwrap().deleted = true;
    assert!(matches!(
        restarted.fetch_attachment(&workspace(&a).unwrap(), ARTIFACT).await,
        Err(ConversationError::NotFound { .. })
    ));
    server.abort();
}

#[test]
fn input_limits_allow_only_attachments_and_reject_empty_or_oversized_messages() {
    assert!(DotsService::validate("", 1).is_ok());
    assert!(matches!(
        DotsService::validate(" ", 0),
        Err(ConversationError::BadRequest { .. })
    ));
    assert!(matches!(
        DotsService::validate("test", 5),
        Err(ConversationError::BadRequest { .. })
    ));
    assert!(matches!(
        DotsService::validate(&"字".repeat(16_001), 0),
        Err(ConversationError::BadRequest { .. })
    ));
}
