use super::*;
use aionui_realtime::BroadcastEventBus;

fn approvals() -> (
    Arc<Mem0DeleteApprovals>,
    broadcast::Receiver<AgentStreamEvent>,
    Arc<BroadcastEventBus>,
) {
    let (tx, events) = broadcast::channel(4);
    let bus = Arc::new(BroadcastEventBus::new(4));
    let approvals = Mem0DeleteApprovals::new(
        Arc::new(RwLock::new(Vec::new())),
        tx,
        bus.clone(),
        "user-a".into(),
        "conversation-a".into(),
    );
    (Arc::new(approvals), events, bus)
}

async fn card(events: &mut broadcast::Receiver<AgentStreamEvent>) -> Confirmation {
    let AgentStreamEvent::AcpPermission(AcpPermissionEventData::Confirmation(card)) = events.recv().await.unwrap()
    else {
        panic!("expected a memory confirmation card");
    };
    card
}

#[tokio::test]
async fn approval_accepts_only_one_explicit_choice() {
    let (approvals, mut events, _) = approvals();
    let owner = approvals.clone();
    let waiting = tokio::spawn(async move { owner.request("id-a", "Test memory").await });
    let card = card(&mut events).await;
    assert!(card.description.contains("id-a"));
    assert_eq!(card.options[0].value, "cancel");
    assert_eq!(card.options[1].value, "proceed_once");
    assert!(
        approvals
            .resolve(&card.call_id, &json!({"value":"unexpected"}), false)
            .is_err()
    );
    assert!(
        approvals
            .resolve(&card.call_id, &json!({"value":"proceed_once"}), true)
            .is_err()
    );
    approvals
        .resolve(&card.call_id, &json!({"value":"proceed_once"}), false)
        .unwrap();
    assert!(
        approvals
            .resolve(&card.call_id, &json!({"value":"proceed_once"}), false)
            .is_err()
    );
    assert!(waiting.await.unwrap());
    assert!(approvals.confirmations.read().unwrap().is_empty());
}

#[tokio::test]
async fn denial_and_turn_cancellation_keep_the_memory() {
    let (approvals, mut events, bus) = approvals();
    let mut removals = bus.subscribe();
    let owner = approvals.clone();
    let waiting = tokio::spawn(async move { owner.request("id-a", "Test memory").await });
    let denied = card(&mut events).await;
    approvals
        .resolve(&denied.call_id, &json!({"value":"cancel"}), false)
        .unwrap();
    assert!(!waiting.await.unwrap());

    let owner = approvals.clone();
    let waiting = tokio::spawn(async move { owner.request("id-b", "Test memory").await });
    let cancelled = card(&mut events).await;
    waiting.abort();
    assert!(waiting.await.unwrap_err().is_cancelled());
    let removed = removals.recv().await.unwrap();
    assert_eq!(removed.name, "confirmation.remove");
    assert_eq!(removed.data["id"], cancelled.id);
    assert!(
        approvals
            .resolve(&cancelled.call_id, &json!({"value":"proceed_once"}), false)
            .is_err()
    );
    assert!(approvals.confirmations.read().unwrap().is_empty());
}

#[tokio::test]
async fn expired_approval_cannot_be_replayed() {
    let (approvals, mut events, _) = approvals();
    let owner = approvals.clone();
    let waiting = tokio::spawn(async move { owner.request("id-a", "Test memory").await });
    let expired = card(&mut events).await;
    approvals
        .pending
        .lock()
        .unwrap()
        .get_mut(&expired.call_id)
        .unwrap()
        .deadline = Instant::now();
    assert!(
        approvals
            .resolve(&expired.call_id, &json!({"value":"proceed_once"}), false)
            .is_err()
    );
    assert!(!waiting.await.unwrap());
    assert!(approvals.confirmations.read().unwrap().is_empty());
}
