use super::*;

#[test]
fn memory_argument_issues_only_report_trusted_fields() {
    assert_eq!(
        Action::Search.argument_issue(&json!({"qurey":"private fixture"})),
        Some(json!({"path":[],"reason":"unknown_field","suggestion":"query"}))
    );
    assert_eq!(
        Action::Search.argument_issue(&json!({("fixture-private-field".repeat(8)):"private fixture"})),
        Some(json!({"path":[],"reason":"unknown_field"}))
    );
    assert_eq!(
        Action::Search.argument_issue(&json!({"scope":"session","query":"fixture"})),
        Some(json!({"path":["scope"],"reason":"invalid_value"}))
    );
    assert_eq!(
        Action::Delete.argument_issue(&json!({"memoryId":"fixture","query":"private fixture"})),
        Some(json!({"path":[],"reason":"conflicting_fields","fields":["memoryId","query"]}))
    );
}

#[test]
fn model_cannot_override_identity_or_request_session_or_bulk_memory() {
    assert!(
        Action::Search
            .argument_issue(&json!({"query":"test","agentId":"another"}))
            .is_some()
    );
    assert!(
        Action::Search
            .argument_issue(&json!({"query":"test","scope":"session"}))
            .is_some()
    );
    assert!(
        Action::Add
            .argument_issue(&json!({"text":"test","longTerm":false}))
            .is_some()
    );
    assert!(
        Action::Delete
            .argument_issue(&json!({"all":true,"confirm":true}))
            .is_some()
    );
    assert!(
        Action::Delete
            .argument_issue(&json!({"memoryId":"id-a","confirm":true}))
            .is_some()
    );
    assert!(
        Action::Delete
            .argument_issue(&json!({"memoryId":"id-a","query":"test"}))
            .is_some()
    );
    assert!(
        Action::Delete
            .argument_issue(&json!({"memoryId":"invalid/path"}))
            .is_some()
    );
    assert!(Action::Delete.argument_issue(&json!({"memoryId":"id-a"})).is_none());
    assert!(
        Action::Delete
            .argument_issue(&json!({"query":"duplicate preference"}))
            .is_none()
    );
}

#[test]
fn memory_delete_filters_require_a_long_term_preview() {
    assert!(
        Action::Delete
            .argument_issue(&json!({"query":"fixture","categories":["work.todo"],"scope":"long-term"}))
            .is_none()
    );
    assert_eq!(
        Action::Delete.argument_issue(&json!({"query":"fixture","scope":"session"})),
        Some(json!({"path":["scope"],"reason":"invalid_value"}))
    );
    assert_eq!(
        Action::Delete.argument_issue(&json!({"memoryId":"fixture","scope":"all"})),
        Some(json!({"path":["scope"],"reason":"preview_only"}))
    );
    assert_eq!(
        Action::Delete.argument_issue(&json!({"memoryId":"fixture","categories":null})),
        Some(json!({"path":["categories"],"reason":"preview_only"}))
    );
}

#[test]
fn memory_update_accepts_metadata_patches_for_a_valid_id() {
    assert!(
        Action::Update
            .argument_issue(&json!({"memoryId":"id-a","text":"Corrected fact"}))
            .is_none()
    );
    assert!(
        Action::Update
            .argument_issue(&json!({"memoryId":"id-a","category":"rp.habit","importance":0}))
            .is_none()
    );
    assert!(
        Action::Update
            .argument_issue(&json!({"memoryId":"id-a","metadata":{"topic":"reading"}}))
            .is_none()
    );
    assert!(Action::Update.argument_issue(&json!({"memoryId":"id-a"})).is_some());
    assert!(
        Action::Update
            .argument_issue(&json!({"memoryId":"invalid/id","category":"rp.habit"}))
            .is_some()
    );
    assert!(
        Action::Update
            .argument_issue(&json!({"memoryId":"id-a","category":"rp.habit","agentId":"another"}))
            .is_some()
    );
}

#[test]
fn memory_expiry_arguments_preserve_the_tool_and_scope_boundaries() {
    assert!(
        Action::Add
            .argument_issue(&json!({"text":"Fixture","expirationDate":"2030-02-01"}))
            .is_none()
    );
    assert!(
        Action::Search
            .argument_issue(&json!({"query":"Fixture","includeExpired":true}))
            .is_none()
    );
    assert!(
        Action::List
            .argument_issue(&json!({"scope":"long-term","includeExpired":true}))
            .is_none()
    );
    assert!(
        Action::List
            .argument_issue(&json!({"scope":"session","includeExpired":true}))
            .is_some()
    );
    assert!(
        Action::Get
            .argument_issue(&json!({"memoryId":"id-a","includeExpired":true}))
            .is_some()
    );
    assert!(
        Action::Update
            .argument_issue(&json!({"memoryId":"id-a","expirationDate":"2030-02-01"}))
            .is_some()
    );
}
