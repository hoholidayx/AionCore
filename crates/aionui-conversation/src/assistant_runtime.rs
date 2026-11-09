//! Apply published assistant settings at the boundary of an ordinary user turn.

use aionui_ai_agent::session_context::{AgentSessionContext, AgentSessionKind};
use aionui_common::AgentType;
use aionui_db::{
    ApplyAssistantRuntimeConfigParams, AssistantRuntimeMcpSnapshotUpdate, AssistantRuntimeSkillSnapshotUpdate,
    models::ConversationRow,
};
use serde_json::{Map, Value, json};
use sha2::{Digest, Sha256};

use crate::ConversationError;
use crate::assistant_runtime_provenance::{
    ASSISTANT_RUNTIME_PROVENANCE_KEY, AssistantRuntimeProvenance, is_ordinary_aionrs_conversation,
};
use crate::skill_snapshot::compute_initial_skills;

use super::ConversationService;

impl ConversationService {
    pub(super) async fn refresh_published_assistant_runtime(
        &self,
        row: ConversationRow,
    ) -> Result<ConversationRow, ConversationError> {
        let Some(config_repo) = self.assistant_runtime_config_repository() else {
            return Ok(row);
        };
        if row.r#type != AgentType::Aionrs.serde_name() {
            return Ok(row);
        }
        let extra = runtime_extra(&row)?;
        if !is_ordinary_aionrs_conversation(&row, &extra) {
            return Ok(row);
        }
        // The send path loads its first row before attachment work and the
        // turn claim. Read the current row here, under that claim.
        let Some(row) = self
            .conversation_repo
            .get_unarchived_for_user(&row.user_id, &row.id)
            .await?
        else {
            return Err(ConversationError::NotFound { id: row.id });
        };
        let extra = runtime_extra(&row)?;
        if !is_ordinary_aionrs_conversation(&row, &extra) {
            return Ok(row);
        }
        let Some(snapshot) = self
            .conversation_repo
            .get_assistant_snapshot(&row.user_id, &row.id)
            .await?
        else {
            return Ok(row);
        };
        let Some(config) = config_repo.get_for_user(&row.user_id, &snapshot.assistant_id).await? else {
            return Ok(row);
        };
        let mut provenance = AssistantRuntimeProvenance::from_extra(&extra)?;
        let skills_changed = config.skills_revision > provenance.skills_revision;
        let mcps_changed = config.mcps_revision > provenance.mcps_revision;
        let rules_changed = config.rules_revision > provenance.rules_revision;
        if !skills_changed && !mcps_changed && !rules_changed {
            return Ok(row);
        }
        let Some(definitions) = self.assistant_definition_repo() else {
            return Err(ConversationError::internal(
                "Assistant definition repository is unavailable",
            ));
        };
        if definitions
            .get_by_assistant_id_for_user(&row.user_id, &snapshot.assistant_id)
            .await?
            .is_none_or(|definition| definition.id != snapshot.assistant_definition_id)
        {
            // A deleted or replaced assistant cannot update the original binding.
            return Ok(row);
        }

        let mut patch = Map::new();
        let skill_snapshot = if skills_changed {
            let skill_ids = provenance.overrides.skill_ids.as_deref().unwrap_or(&config.skill_ids);
            let disabled_builtin_skill_ids = provenance
                .overrides
                .disabled_builtin_skill_ids
                .as_deref()
                .unwrap_or(&config.disabled_builtin_skill_ids);
            let auto_inject = self.skill_resolver.auto_inject_names().await;
            patch.insert(
                "skills".to_owned(),
                json!(compute_initial_skills(
                    &auto_inject,
                    skill_ids,
                    disabled_builtin_skill_ids
                )),
            );
            provenance.skills_revision = config.skills_revision;
            Some((
                json!(skill_ids).to_string(),
                json!(disabled_builtin_skill_ids).to_string(),
            ))
        } else {
            None
        };
        let mcp_snapshot = if mcps_changed {
            let mcp_ids = provenance.overrides.mcp_ids.as_deref().unwrap_or(&config.mcp_ids);
            let resolved = self
                .build_selected_assistant_mcp_snapshot(&row.user_id, mcp_ids, &AgentType::Aionrs, &extra)
                .await?;
            patch.insert("mcp_server_ids".to_owned(), json!(resolved.mcp_server_ids));
            patch.insert("mcp_servers".to_owned(), json!(resolved.mcp_servers));
            patch.insert("mcp_statuses".to_owned(), json!(resolved.mcp_statuses));
            patch.insert("session_mcp_servers".to_owned(), json!(resolved.session_mcp_servers));
            provenance.mcps_revision = config.mcps_revision;
            Some(json!(mcp_ids).to_string())
        } else {
            None
        };
        let rules = if rules_changed {
            let rules = config.resolve_rule(provenance.locale.as_deref());
            patch.insert("preset_context".to_owned(), Value::Null);
            patch.insert(
                "preset_rules".to_owned(),
                if rules.trim().is_empty() {
                    Value::Null
                } else {
                    json!(rules)
                },
            );
            provenance.rules_revision = config.rules_revision;
            Some(rules)
        } else {
            None
        };
        patch.insert(ASSISTANT_RUNTIME_PROVENANCE_KEY.to_owned(), json!(provenance));
        let extra_patch = Value::Object(patch);

        self.conversation_repo
            .apply_assistant_runtime_config(
                &row.user_id,
                &row.id,
                &ApplyAssistantRuntimeConfigParams {
                    expected_extra: &row.extra,
                    extra_patch: &extra_patch,
                    skills: skill_snapshot
                        .as_ref()
                        .map(|(selected, disabled)| AssistantRuntimeSkillSnapshotUpdate {
                            mode: &config.skills_mode,
                            ids_json: selected,
                            disabled_ids_json: disabled,
                        }),
                    mcps: mcp_snapshot.as_ref().map(|selected| AssistantRuntimeMcpSnapshotUpdate {
                        mode: &config.mcps_mode,
                        ids_json: selected,
                    }),
                    rules_content: rules,
                },
            )
            .await
            .map_err(Into::into)
    }

    pub(super) fn assistant_runtime_fingerprint(
        row: &ConversationRow,
        context: &AgentSessionContext,
    ) -> Result<Option<[u8; 32]>, ConversationError> {
        let AgentSessionKind::Aionrs(aionrs) = &context.kind else {
            return Ok(None);
        };
        if !is_ordinary_aionrs_conversation(row, &runtime_extra(row)?) {
            return Ok(None);
        }
        let config = &aionrs.config;
        let mut effective = json!([
            config.system_prompt,
            config.preset_rules,
            config.skills,
            config.mcp_server_ids,
            config.session_mcp_servers
        ]);
        effective.sort_all_objects();
        Ok(Some(Sha256::digest(effective.to_string().as_bytes()).into()))
    }
}

fn runtime_extra(row: &ConversationRow) -> Result<Value, ConversationError> {
    serde_json::from_str(&row.extra)
        .map_err(|error| ConversationError::internal(format!("Invalid assistant runtime conversation extra: {error}")))
}
