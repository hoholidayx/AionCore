//! Integration tests for McpConfigService with real SQLite.
//!
//! Tests from test-plan §1 (CRUD) at the service layer.

use std::collections::HashMap;
use std::sync::Arc;

const TEST_USER_ID: &str = "system_default_user";
const OTHER_USERNAME: &str = "user-2";

use aionui_api_types::{
    BatchImportMcpServersRequest, CreateMcpServerRequest, ImportMcpServerRequest, McpServerPurpose, McpTransport,
    UpdateMcpServerRequest,
};
use aionui_db::{IUserRepository, SqliteMcpServerRepository, SqliteUserRepository};
use aionui_mcp::{McpConfigService, McpError};

async fn make_service() -> McpConfigService {
    make_service_with_other_user().await.0
}

async fn make_service_with_other_user() -> (McpConfigService, String) {
    let db = aionui_db::init_database_memory().await.unwrap();
    let user_repo = SqliteUserRepository::new(db.pool().clone());
    let other_user = user_repo.create_user(OTHER_USERNAME, "hash").await.unwrap();
    let repo = Arc::new(SqliteMcpServerRepository::new(db.pool().clone()));
    (McpConfigService::new(repo), other_user.id)
}

fn stdio_req(name: &str) -> CreateMcpServerRequest {
    CreateMcpServerRequest {
        purpose: None,
        name: name.to_owned(),
        backend_alias: None,
        description: Some("test".to_owned()),
        transport: McpTransport::Stdio {
            command: "npx".into(),
            args: vec!["-y".into(), "@test/server".into()],
            env: HashMap::new(),
        },
        original_json: None,
        builtin: false,
    }
}

fn http_req(name: &str) -> CreateMcpServerRequest {
    CreateMcpServerRequest {
        purpose: None,
        name: name.to_owned(),
        backend_alias: None,
        description: None,
        transport: McpTransport::Http {
            url: "https://example.com/mcp".into(),
            headers: HashMap::from([("Auth".into(), "Bearer tok".into())]),
        },
        original_json: None,
        builtin: false,
    }
}

fn stdio_import_req(name: &str) -> ImportMcpServerRequest {
    ImportMcpServerRequest {
        purpose: None,
        name: name.to_owned(),
        backend_alias: None,
        description: Some("test".to_owned()),
        transport: McpTransport::Stdio {
            command: "npx".into(),
            args: vec!["-y".into(), "@test/server".into()],
            env: HashMap::new(),
        },
        original_json: None,
        builtin: false,
        enabled: None,
    }
}

fn http_import_req(name: &str) -> ImportMcpServerRequest {
    ImportMcpServerRequest {
        purpose: None,
        name: name.to_owned(),
        backend_alias: None,
        description: None,
        transport: McpTransport::Http {
            url: "https://example.com/mcp".into(),
            headers: HashMap::from([("Auth".into(), "Bearer tok".into())]),
        },
        original_json: None,
        builtin: false,
        enabled: None,
    }
}

#[tokio::test]
async fn image_backend_alias_survives_omission_and_clears_with_null() {
    let svc = make_service().await;
    let name = "Example Image Backend";
    let mut server = http_import_req(name);
    server.purpose = Some(McpServerPurpose::AsyncImage);
    server.enabled = Some(true);
    let imported = svc
        .batch_import(TEST_USER_ID, BatchImportMcpServersRequest { servers: vec![server] })
        .await
        .unwrap();
    let legacy = &imported[0];
    assert!(legacy.backend_alias.is_none());
    svc.set_image_default(TEST_USER_ID, Some(&legacy.id)).await.unwrap();

    let edit = serde_json::from_value(serde_json::json!({ "backend_alias": "  Example-Fast-2  " })).unwrap();
    let updated = svc.edit_server(TEST_USER_ID, &legacy.id, edit).await.unwrap();
    assert_eq!(updated.backend_alias.as_deref(), Some("example-fast-2"));

    let omitted = BatchImportMcpServersRequest {
        servers: vec![http_import_req(name)],
    };
    let updated = svc.batch_import(TEST_USER_ID, omitted).await.unwrap();
    assert_eq!(updated[0].id, legacy.id);
    assert_eq!(updated[0].backend_alias.as_deref(), Some("example-fast-2"));
    assert!(updated[0].is_default);

    let cleared = serde_json::from_value(serde_json::json!({
        "servers": [{
            "name": name,
            "backend_alias": null,
            "transport": { "type": "http", "url": "https://example.com/mcp" }
        }]
    }))
    .unwrap();
    let updated = svc.batch_import(TEST_USER_ID, cleared).await.unwrap();
    assert_eq!(updated[0].name, name);
    assert!(updated[0].backend_alias.is_none());
    assert!(updated[0].is_default);
    let stored = svc.get_server(TEST_USER_ID, &legacy.id).await.unwrap();
    assert!(stored.backend_alias.is_none());
}

#[tokio::test]
async fn image_backend_aliases_cannot_collide_with_account_selectors() {
    let (svc, other_user) = make_service_with_other_user().await;
    let mut server = http_req("legacy-image");
    server.purpose = Some(McpServerPurpose::AsyncImage);
    let legacy = svc.add_server(TEST_USER_ID, server).await.unwrap();

    let mut conflicting = http_import_req("Provider Image");
    conflicting.purpose = Some(McpServerPurpose::AsyncImage);
    conflicting.backend_alias = Some(Some(" Legacy-Image ".into()));
    let error = svc
        .batch_import(
            TEST_USER_ID,
            BatchImportMcpServersRequest {
                servers: vec![conflicting],
            },
        )
        .await
        .unwrap_err();
    assert!(
        matches!(error, McpError::Conflict(message) if message == "Image backend selector 'legacy-image' already exists")
    );

    for user in [TEST_USER_ID, &other_user] {
        let mut server = http_req("Provider Image");
        server.purpose = Some(McpServerPurpose::AsyncImage);
        server.backend_alias = Some(Some("provider-image".into()));
        let created = svc.add_server(user, server).await.unwrap();
        assert_eq!(created.backend_alias.as_deref(), Some("provider-image"));
    }

    let edit = serde_json::from_value(serde_json::json!({ "backend_alias": "provider-image" })).unwrap();
    let error = svc.edit_server(TEST_USER_ID, &legacy.id, edit).await.unwrap_err();
    assert!(
        matches!(error, McpError::Conflict(message) if message == "Image backend selector 'provider-image' already exists")
    );
    assert!(
        svc.get_server(TEST_USER_ID, &legacy.id)
            .await
            .unwrap()
            .backend_alias
            .is_none()
    );
}

#[tokio::test]
async fn image_backend_alias_validation_applies_on_create() {
    let svc = make_service().await;
    for alias in [
        "",
        "extra--hyphen",
        "space here",
        "图",
        "123456789012345678901234567890123",
    ] {
        let mut server = http_req("Invalid Image Alias");
        server.purpose = Some(McpServerPurpose::AsyncImage);
        server.backend_alias = Some(Some(alias.into()));
        let error = svc.add_server(TEST_USER_ID, server).await.unwrap_err();
        assert!(
            matches!(error, McpError::InvalidEdit(message) if message.starts_with("Image backend aliases must contain"))
        );
    }

    let mut server = http_req("General MCP");
    server.backend_alias = Some(Some("general-alias".into()));
    let error = svc.add_server(TEST_USER_ID, server).await.unwrap_err();
    assert!(
        matches!(error, McpError::InvalidEdit(message) if message == "Backend aliases are only available for asynchronous image MCP servers")
    );
}

// ---------------------------------------------------------------------------
// Create
// ---------------------------------------------------------------------------

#[tokio::test]
async fn create_and_get_stdio_server() {
    let svc = make_service().await;
    let resp = svc.add_server(TEST_USER_ID, stdio_req("test-stdio")).await.unwrap();

    assert!(resp.id.starts_with("mcp_"));
    assert_eq!(resp.name, "test-stdio");
    assert!(!resp.enabled);
    assert_eq!(resp.description.as_deref(), Some("test"));

    let found = svc.get_server(TEST_USER_ID, &resp.id).await.unwrap();
    assert_eq!(found.id, resp.id);
}

#[tokio::test]
async fn create_http_with_headers() {
    let svc = make_service().await;
    let resp = svc.add_server(TEST_USER_ID, http_req("test-http")).await.unwrap();

    match resp.transport {
        McpTransport::Http { ref url, ref headers } => {
            assert_eq!(url, "https://example.com/mcp");
            assert_eq!(headers.get("Auth").unwrap(), "Bearer tok");
        }
        _ => panic!("expected Http"),
    }
}

#[tokio::test]
async fn create_same_name_upserts() {
    let svc = make_service().await;
    let first = svc.add_server(TEST_USER_ID, stdio_req("dup")).await.unwrap();
    let second = svc.add_server(TEST_USER_ID, http_req("dup")).await.unwrap();

    assert_eq!(first.id, second.id);
    match second.transport {
        McpTransport::Http { .. } => {}
        _ => panic!("expected Http after upsert"),
    }
}

// ---------------------------------------------------------------------------
// Read
// ---------------------------------------------------------------------------

#[tokio::test]
async fn list_empty() {
    let svc = make_service().await;
    assert!(svc.list_servers(TEST_USER_ID).await.unwrap().is_empty());
}

#[tokio::test]
async fn list_returns_all() {
    let svc = make_service().await;
    svc.add_server(TEST_USER_ID, stdio_req("a")).await.unwrap();
    svc.add_server(TEST_USER_ID, http_req("b")).await.unwrap();
    assert_eq!(svc.list_servers(TEST_USER_ID).await.unwrap().len(), 2);
}

#[tokio::test]
async fn get_not_found() {
    let svc = make_service().await;
    let err = svc.get_server(TEST_USER_ID, "nonexistent").await.unwrap_err();
    assert!(matches!(err, McpError::NotFound(_)));
}

#[tokio::test]
async fn servers_are_scoped_by_current_user() {
    let (svc, other_user_id) = make_service_with_other_user().await;
    let owner_server = svc.add_server(TEST_USER_ID, stdio_req("shared")).await.unwrap();
    let other_server = svc.add_server(&other_user_id, http_req("shared")).await.unwrap();

    assert_ne!(owner_server.id, other_server.id);
    assert_eq!(svc.list_servers(TEST_USER_ID).await.unwrap().len(), 1);
    assert_eq!(svc.list_servers(&other_user_id).await.unwrap().len(), 1);

    assert!(matches!(
        svc.get_server(TEST_USER_ID, &other_server.id).await.unwrap_err(),
        McpError::NotFound(_)
    ));
    assert!(matches!(
        svc.edit_server(
            TEST_USER_ID,
            &other_server.id,
            UpdateMcpServerRequest {
                purpose: None,
                name: None,
                backend_alias: None,
                description: Some(Some("forged".into())),
                transport: None,
                original_json: None,
                builtin: None,
            },
        )
        .await
        .unwrap_err(),
        McpError::NotFound(_)
    ));
    assert!(matches!(
        svc.delete_server(TEST_USER_ID, &other_server.id).await.unwrap_err(),
        McpError::NotFound(_)
    ));

    svc.delete_server(&other_user_id, &other_server.id).await.unwrap();
    assert_eq!(
        svc.get_server(TEST_USER_ID, &owner_server.id).await.unwrap().id,
        owner_server.id
    );
}

// ---------------------------------------------------------------------------
// Update
// ---------------------------------------------------------------------------

#[tokio::test]
async fn edit_name_is_rejected() {
    let svc = make_service().await;
    let created = svc.add_server(TEST_USER_ID, stdio_req("old")).await.unwrap();

    let err = svc
        .edit_server(
            TEST_USER_ID,
            &created.id,
            UpdateMcpServerRequest {
                purpose: None,
                name: Some("new".into()),
                backend_alias: None,
                description: None,
                transport: None,
                original_json: None,
                builtin: None,
            },
        )
        .await
        .unwrap_err();
    assert!(matches!(err, McpError::InvalidEdit(_)));
}

#[tokio::test]
async fn edit_transport() {
    let svc = make_service().await;
    let created = svc.add_server(TEST_USER_ID, stdio_req("test")).await.unwrap();

    let updated = svc
        .edit_server(
            TEST_USER_ID,
            &created.id,
            UpdateMcpServerRequest {
                purpose: None,
                name: None,
                backend_alias: None,
                description: None,
                transport: Some(McpTransport::Sse {
                    url: "https://new.url".into(),
                    headers: HashMap::new(),
                }),
                original_json: None,
                builtin: None,
            },
        )
        .await
        .unwrap();
    match updated.transport {
        McpTransport::Sse { ref url, .. } => assert_eq!(url, "https://new.url"),
        _ => panic!("expected Sse"),
    }
}

#[tokio::test]
async fn edit_clears_description() {
    let svc = make_service().await;
    let created = svc.add_server(TEST_USER_ID, stdio_req("test")).await.unwrap();
    assert!(created.description.is_some());

    let updated = svc
        .edit_server(
            TEST_USER_ID,
            &created.id,
            UpdateMcpServerRequest {
                purpose: None,
                name: None,
                backend_alias: None,
                description: Some(None),
                transport: None,
                original_json: None,
                builtin: None,
            },
        )
        .await
        .unwrap();
    assert!(updated.description.is_none());
}

#[tokio::test]
async fn edit_not_found() {
    let svc = make_service().await;
    let err = svc
        .edit_server(
            TEST_USER_ID,
            "nonexistent",
            UpdateMcpServerRequest {
                purpose: None,
                name: Some("x".into()),
                backend_alias: None,
                description: None,
                transport: None,
                original_json: None,
                builtin: None,
            },
        )
        .await
        .unwrap_err();
    assert!(matches!(err, McpError::NotFound(_)));
}

#[tokio::test]
async fn edit_name_conflict() {
    let svc = make_service().await;
    svc.add_server(TEST_USER_ID, stdio_req("a")).await.unwrap();
    let b = svc.add_server(TEST_USER_ID, stdio_req("b")).await.unwrap();

    let err = svc
        .edit_server(
            TEST_USER_ID,
            &b.id,
            UpdateMcpServerRequest {
                purpose: None,
                name: Some("a".into()),
                backend_alias: None,
                description: None,
                transport: None,
                original_json: None,
                builtin: None,
            },
        )
        .await
        .unwrap_err();
    assert!(matches!(err, McpError::InvalidEdit(_)));
}

// ---------------------------------------------------------------------------
// Delete
// ---------------------------------------------------------------------------

#[tokio::test]
async fn delete_removes_server() {
    let svc = make_service().await;
    let created = svc.add_server(TEST_USER_ID, stdio_req("del")).await.unwrap();
    let was_enabled = svc.delete_server(TEST_USER_ID, &created.id).await.unwrap();
    assert!(!was_enabled);

    let err = svc.get_server(TEST_USER_ID, &created.id).await.unwrap_err();
    assert!(matches!(err, McpError::NotFound(_)));
}

#[tokio::test]
async fn delete_enabled_returns_true() {
    let svc = make_service().await;
    let created = svc.add_server(TEST_USER_ID, stdio_req("del-en")).await.unwrap();
    svc.toggle_server(TEST_USER_ID, &created.id).await.unwrap();

    let was_enabled = svc.delete_server(TEST_USER_ID, &created.id).await.unwrap();
    assert!(was_enabled);
}

#[tokio::test]
async fn delete_not_found() {
    let svc = make_service().await;
    let err = svc.delete_server(TEST_USER_ID, "nonexistent").await.unwrap_err();
    assert!(matches!(err, McpError::NotFound(_)));
}

// ---------------------------------------------------------------------------
// Toggle
// ---------------------------------------------------------------------------

#[tokio::test]
async fn toggle_enables_then_disables() {
    let svc = make_service().await;
    let created = svc.add_server(TEST_USER_ID, stdio_req("tog")).await.unwrap();
    assert!(!created.enabled);

    let toggled = svc.toggle_server(TEST_USER_ID, &created.id).await.unwrap();
    assert!(toggled.enabled);

    let toggled_back = svc.toggle_server(TEST_USER_ID, &created.id).await.unwrap();
    assert!(!toggled_back.enabled);
}

// ---------------------------------------------------------------------------
// Batch import
// ---------------------------------------------------------------------------

#[tokio::test]
async fn batch_import_creates_and_upserts() {
    let svc = make_service().await;
    svc.add_server(TEST_USER_ID, stdio_req("existing")).await.unwrap();

    let req = BatchImportMcpServersRequest {
        servers: vec![
            http_import_req("existing"), // upsert
            stdio_import_req("new"),     // create
        ],
    };
    let results = svc.batch_import(TEST_USER_ID, req).await.unwrap();
    assert_eq!(results.len(), 2);

    let all = svc.list_servers(TEST_USER_ID).await.unwrap();
    assert_eq!(all.len(), 2);
}

#[tokio::test]
async fn batch_import_preserves_enabled_in_database() {
    let svc = make_service().await;
    let mut req = stdio_import_req("enabled-db-mcp");
    req.enabled = Some(true);

    let result = svc
        .batch_import(TEST_USER_ID, BatchImportMcpServersRequest { servers: vec![req] })
        .await
        .unwrap();

    assert_eq!(result.len(), 1);
    assert_eq!(result[0].name, "enabled-db-mcp");
    assert!(result[0].enabled);

    let listed = svc.list_servers(TEST_USER_ID).await.unwrap();
    assert_eq!(listed.len(), 1);
    assert!(listed[0].enabled);
}
