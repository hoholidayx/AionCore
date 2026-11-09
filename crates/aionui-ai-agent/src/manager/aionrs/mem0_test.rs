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
