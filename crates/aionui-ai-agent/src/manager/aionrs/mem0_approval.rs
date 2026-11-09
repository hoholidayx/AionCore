use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

use aionui_api_types::{MEM0_DELETE_CONFIRMATION_PREFIX, WebSocketMessage};
use aionui_common::{Confirmation, ConfirmationOption};
use aionui_realtime::EventBroadcaster;
use serde_json::{Value, json};
use tokio::sync::{broadcast, oneshot};
use tokio::time::Instant;
use uuid::Uuid;

use crate::error::AgentError;
use crate::protocol::events::{AcpPermissionEventData, AgentStreamEvent};

const CONFIRMATION_TIMEOUT: Duration = Duration::from_secs(120);

/// One-shot approvals owned by the conversation, independent of generic tool permissions.
pub(super) struct Mem0DeleteApprovals {
    pending: Mutex<HashMap<String, PendingDelete>>,
    confirmations: Arc<RwLock<Vec<Confirmation>>>,
    event_tx: broadcast::Sender<AgentStreamEvent>,
    broadcaster: Arc<dyn EventBroadcaster>,
    user_id: String,
    conversation_id: String,
}

struct PendingDelete {
    sender: oneshot::Sender<bool>,
    deadline: Instant,
}

impl Mem0DeleteApprovals {
    pub(super) fn new(
        confirmations: Arc<RwLock<Vec<Confirmation>>>,
        event_tx: broadcast::Sender<AgentStreamEvent>,
        broadcaster: Arc<dyn EventBroadcaster>,
        user_id: String,
        conversation_id: String,
    ) -> Self {
        Self {
            pending: Mutex::new(HashMap::new()),
            confirmations,
            event_tx,
            broadcaster,
            user_id,
            conversation_id,
        }
    }

    pub(super) async fn request(self: &Arc<Self>, memory_id: &str, memory: &str) -> bool {
        let id = format!("{MEM0_DELETE_CONFIRMATION_PREFIX}{}", Uuid::now_v7());
        let (sender, receiver) = oneshot::channel();
        let deadline = Instant::now() + CONFIRMATION_TIMEOUT;
        let snippet: String = memory
            .chars()
            .filter(|c| !c.is_control() || *c == '\n')
            .take(600)
            .collect();
        let confirmation = Confirmation {
            id: id.clone(),
            call_id: id.clone(),
            title: Some("messages.confirmation.memoryDeleteTitle".into()),
            action: Some("memory_delete".into()),
            command_type: Some("memory_delete".into()),
            description: format!("{memory_id}\n\n{snippet}"),
            options: vec![
                ConfirmationOption {
                    label: "messages.confirmation.keepMemory".into(),
                    value: json!("cancel"),
                    params: None,
                },
                ConfirmationOption {
                    label: "messages.confirmation.deleteMemoryOnce".into(),
                    value: json!("proceed_once"),
                    params: None,
                },
            ],
            questions: None,
        };
        let published = {
            let (Ok(mut pending), Ok(mut cards)) = (self.pending.lock(), self.confirmations.write()) else {
                return false;
            };
            pending.insert(id.clone(), PendingDelete { sender, deadline });
            cards.push(confirmation.clone());
            self.event_tx
                .send(AgentStreamEvent::AcpPermission(AcpPermissionEventData::Confirmation(
                    confirmation,
                )))
                .is_ok()
        };
        let _guard = PendingConfirmation { approvals: self, id };
        if !published {
            return false;
        }
        matches!(tokio::time::timeout_at(deadline, receiver).await, Ok(Ok(true)))
    }

    pub(super) fn resolve(&self, id: &str, data: &Value, always_allow: bool) -> Result<(), AgentError> {
        let approved = match (data.get("value").and_then(Value::as_str), always_allow) {
            (Some("proceed_once"), false) => true,
            (Some("cancel"), false) => false,
            _ => return Err(AgentError::bad_request("Invalid memory deletion confirmation choice")),
        };
        let pending = self
            .remove(id)
            .ok_or_else(|| AgentError::conflict("Memory deletion confirmation is no longer pending"))?;
        if Instant::now() >= pending.deadline {
            self.emit_removal(id);
            return Err(AgentError::conflict("Memory deletion confirmation expired"));
        }
        pending
            .sender
            .send(approved)
            .map_err(|_| AgentError::conflict("Memory deletion confirmation is no longer pending"))
    }

    pub(super) fn cancel_all(&self) {
        let Ok(mut pending) = self.pending.lock() else { return };
        let cancelled = std::mem::take(&mut *pending);
        drop(pending);
        if let Ok(mut cards) = self.confirmations.write() {
            cards.retain(|card| !cancelled.contains_key(&card.call_id));
        }
        for id in cancelled.keys() {
            self.emit_removal(id);
        }
    }

    fn remove(&self, id: &str) -> Option<PendingDelete> {
        let pending = self.pending.lock().ok()?.remove(id)?;
        if let Ok(mut cards) = self.confirmations.write() {
            cards.retain(|card| card.call_id != id);
        }
        Some(pending)
    }

    fn cancel(&self, id: &str) {
        if self.remove(id).is_some() {
            self.emit_removal(id);
        }
    }

    fn emit_removal(&self, id: &str) {
        self.broadcaster.broadcast(WebSocketMessage::new(
            "confirmation.remove",
            json!({"user_id": self.user_id, "conversation_id": self.conversation_id, "id": id}),
        ));
    }
}

struct PendingConfirmation<'a> {
    approvals: &'a Mem0DeleteApprovals,
    id: String,
}

impl Drop for PendingConfirmation<'_> {
    fn drop(&mut self) {
        self.approvals.cancel(&self.id);
    }
}

#[cfg(test)]
#[path = "mem0_approval_test.rs"]
mod tests;
