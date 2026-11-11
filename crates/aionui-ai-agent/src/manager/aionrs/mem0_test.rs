use super::*;

#[test]
fn model_cannot_override_identity_or_request_session_or_bulk_memory() {
    assert!(!Action::Search.accepts(&json!({"query":"test","agentId":"another"})));
    assert!(!Action::Search.accepts(&json!({"query":"test","scope":"session"})));
    assert!(!Action::Add.accepts(&json!({"text":"test","longTerm":false})));
    assert!(!Action::Delete.accepts(&json!({"all":true,"confirm":true})));
    assert!(!Action::Delete.accepts(&json!({"memoryId":"id-a","confirm":true})));
    assert!(!Action::Delete.accepts(&json!({"memoryId":"id-a","query":"test"})));
    assert!(!Action::Delete.accepts(&json!({"memoryId":"invalid/path"})));
    assert!(Action::Delete.accepts(&json!({"memoryId":"id-a"})));
    assert!(Action::Delete.accepts(&json!({"query":"duplicate preference"})));
}

#[test]
fn memory_update_accepts_metadata_patches_for_a_valid_id() {
    assert!(Action::Update.accepts(&json!({"memoryId":"id-a","text":"Corrected fact"})));
    assert!(Action::Update.accepts(&json!({"memoryId":"id-a","category":"rp.habit","importance":0})));
    assert!(Action::Update.accepts(&json!({"memoryId":"id-a","metadata":{"topic":"reading"}})));
    assert!(!Action::Update.accepts(&json!({"memoryId":"id-a"})));
    assert!(!Action::Update.accepts(&json!({"memoryId":"invalid/id","category":"rp.habit"})));
    assert!(!Action::Update.accepts(&json!({"memoryId":"id-a","category":"rp.habit","agentId":"another"})));
}

#[test]
fn memory_expiry_arguments_preserve_the_tool_and_scope_boundaries() {
    assert!(Action::Add.accepts(&json!({"text":"Fixture","expirationDate":"2030-02-01"})));
    assert!(Action::Search.accepts(&json!({"query":"Fixture","includeExpired":true})));
    assert!(Action::List.accepts(&json!({"scope":"long-term","includeExpired":true})));
    assert!(!Action::List.accepts(&json!({"scope":"session","includeExpired":true})));
    assert!(!Action::Get.accepts(&json!({"memoryId":"id-a","includeExpired":true})));
    assert!(!Action::Update.accepts(&json!({"memoryId":"id-a","expirationDate":"2030-02-01"})));
}
