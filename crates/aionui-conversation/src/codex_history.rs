use std::path::Path;
use std::sync::Arc;

use aionui_api_types::ConversationDeleteOptions;
use aionui_common::AgentKillReason;
use aionui_db::models::ConversationRow;
use serde_json::Value;

use super::{ConversationService, is_ordinary_assistant_extra};
use crate::ConversationError;

const LOCAL_OWNER: &str = "system_default_user";

#[async_trait::async_trait]
pub trait CodexHistoryDeletion: Send + Sync {
    async fn delete(&self, thread_id: &str, codex_home: &Path) -> Result<(), String>;
}

impl ConversationService {
    pub fn with_codex_history_deletion(mut self, deletion: Option<Arc<dyn CodexHistoryDeletion>>) -> Self {
        self.codex_history_deletion = deletion;
        self
    }

    pub fn codex_history_deletion(&self) -> Option<Arc<dyn CodexHistoryDeletion>> {
        self.codex_history_deletion.clone()
    }

    pub(super) async fn enable_codex_history_deletion_for_new_conversation(&self, row: &ConversationRow) {
        if self.codex_history_deletion.is_some()
            && row.user_id == LOCAL_OWNER
            && eligible_row(row)
            && let Err(error) = self
                .acp_session_repo
                .enable_codex_history_deletion(&row.user_id, &row.id)
                .await
        {
            tracing::warn!(conversation_id = %row.id, %error, "Could not enable Codex history deletion");
        }
    }

    pub(crate) async fn delete_options(
        &self,
        user_id: &str,
        id: &str,
        interactive: bool,
    ) -> Result<ConversationDeleteOptions, ConversationError> {
        let row = self.owned_delete_row(user_id, id).await?;
        if !interactive || user_id != LOCAL_OWNER || self.codex_history_deletion.is_none() || !eligible_row(&row) {
            return Ok(ConversationDeleteOptions::Unavailable);
        }
        if self
            .acp_session_repo
            .is_codex_history_deletion_pending(user_id, id)
            .await?
        {
            return Ok(ConversationDeleteOptions::RetryRequired);
        }
        Ok(
            if self.acp_session_repo.codex_delete_target(user_id, id).await?.is_some() {
                ConversationDeleteOptions::Available
            } else {
                ConversationDeleteOptions::Unavailable
            },
        )
    }

    pub(crate) async fn delete_with_codex_history(
        &self,
        user_id: &str,
        id: &str,
        interactive: bool,
    ) -> Result<(), ConversationError> {
        if !interactive || user_id != LOCAL_OWNER {
            return Err(ConversationError::Forbidden {
                reason: "Codex history deletion requires the interactive local owner".into(),
            });
        }
        let _lifecycle = self.runtime_state.lock_lifecycle(id).await?;
        let row = self.owned_delete_row(user_id, id).await?;
        let deletion = self
            .codex_history_deletion
            .as_ref()
            .filter(|_| eligible_row(&row))
            .ok_or_else(unavailable)?;
        let _runtime = self.runtime_state.begin_idle_deletion(id)?;
        let target = self
            .acp_session_repo
            .begin_codex_history_deletion(user_id, id)
            .await?
            .ok_or_else(unavailable)?;
        if target.phase != "codex_deleted" {
            self.task_manager
                .kill_and_wait(id, Some(AgentKillReason::RuntimeRestart))
                .await;
            if let Err(error) = deletion.delete(&target.thread_id, Path::new(&target.codex_home)).await {
                tracing::warn!(conversation_id = id, %error, "Codex history deletion needs a retry");
                return Err(ConversationError::BadGateway { reason: error });
            }
            if !self.acp_session_repo.mark_codex_history_deleted(user_id, id).await? {
                return Err(ConversationError::internal("Codex history deletion state disappeared"));
            }
            tracing::info!(conversation_id = id, "Codex history deletion confirmed");
        }
        self.delete_local(user_id, id).await
    }

    pub(super) async fn reject_pending_codex_delete(&self, user_id: &str, id: &str) -> Result<(), ConversationError> {
        if user_id == LOCAL_OWNER
            && self
                .acp_session_repo
                .is_codex_history_deletion_pending(user_id, id)
                .await?
        {
            return Err(ConversationError::Busy {
                reason: "Codex history deletion is pending".into(),
            });
        }
        Ok(())
    }

    async fn owned_delete_row(&self, user_id: &str, id: &str) -> Result<ConversationRow, ConversationError> {
        self.conversation_repo
            .get(user_id, id)
            .await?
            .ok_or_else(|| ConversationError::NotFound { id: id.to_owned() })
    }
}

fn unavailable() -> ConversationError {
    ConversationError::Busy {
        reason: "No exclusive verified Codex thread is available for deletion".into(),
    }
}

fn eligible_row(row: &ConversationRow) -> bool {
    row.r#type == "acp"
        && row.source.as_deref() == Some("aionui")
        && row.channel_chat_id.is_none()
        && serde_json::from_str::<Value>(&row.extra).is_ok_and(|extra| standalone(&extra))
}

fn standalone(extra: &Value) -> bool {
    is_ordinary_assistant_extra(extra) && extra.get("fork").is_none_or(Value::is_null)
}
