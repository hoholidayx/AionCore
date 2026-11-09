use aionui_api_types::{AssistantRuntimeOverridesRequest, TeamSessionBinding};
use aionui_common::AgentType;
use aionui_db::models::ConversationRow;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::error::ConversationError;

pub(crate) const ASSISTANT_RUNTIME_PROVENANCE_KEY: &str = "assistant_runtime_provenance";

/// Server-owned provenance for a conversation's next-turn assistant settings.
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(default)]
pub(crate) struct AssistantRuntimeProvenance {
    pub(crate) locale: Option<String>,
    pub(crate) overrides: AssistantRuntimeOverridesRequest,
    pub(crate) skills_revision: i64,
    pub(crate) mcps_revision: i64,
    pub(crate) rules_revision: i64,
}

impl AssistantRuntimeProvenance {
    pub(crate) fn from_extra(extra: &Value) -> Result<Self, ConversationError> {
        let Some(value) = extra.get(ASSISTANT_RUNTIME_PROVENANCE_KEY) else {
            return Ok(Self::default());
        };
        serde_json::from_value(value.clone())
            .map_err(|e| ConversationError::internal(format!("Invalid assistant runtime provenance: {e}")))
    }
}

pub(crate) fn is_ordinary_aionrs_conversation(row: &ConversationRow, extra: &Value) -> bool {
    row.r#type == AgentType::Aionrs.serde_name()
        && row.source.as_deref() == Some("aionui")
        && row.channel_chat_id.is_none()
        && is_ordinary_assistant_extra(extra)
}

pub(crate) fn is_ordinary_assistant_extra(extra: &Value) -> bool {
    extra.is_object()
        && TeamSessionBinding::from_extra_value(extra).is_ok_and(|binding| binding.is_none())
        && ["cron_job_id", "cronJobId"]
            .iter()
            .all(|key| extra.get(*key).is_none_or(Value::is_null))
}
