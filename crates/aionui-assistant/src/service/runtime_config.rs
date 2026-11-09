//! Commits the assistant settings that existing ordinary conversations may follow.

use std::collections::{BTreeMap, HashMap};
use std::fs;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, Weak};

use aionui_api_types::{AssistantRuleMutation, AssistantSource, UpdateAssistantRequest};
use aionui_db::{AssistantDefinitionRow, AssistantRuntimeConfig};
use tokio::sync::{Mutex as AsyncMutex, OwnedMutexGuard};

use super::{
    AssistantError, AssistantService, DEFAULT_USER_ID, decode_str_list, encode_filename_component,
    legacy_filename_component_is_safe,
};

type AssistantMutationKey = (String, String);
type AssistantMutationLock = AsyncMutex<()>;

#[derive(Default)]
pub(super) struct AssistantMutationLocks(Mutex<HashMap<AssistantMutationKey, Weak<AssistantMutationLock>>>);

impl AssistantMutationLocks {
    pub(super) async fn lock(&self, user_id: &str, assistant_id: &str) -> Result<OwnedMutexGuard<()>, AssistantError> {
        let lock = {
            let key = (user_id.to_owned(), assistant_id.to_owned());
            let mut locks = self
                .0
                .lock()
                .map_err(|_| AssistantError::Internal("assistant mutation lock failed".into()))?;
            if let Some(lock) = locks.get(&key).and_then(Weak::upgrade) {
                lock
            } else {
                locks.retain(|_, lock| lock.strong_count() > 0);
                let lock = Arc::new(AsyncMutex::new(()));
                locks.insert(key, Arc::downgrade(&lock));
                lock
            }
        };
        Ok(lock.lock_owned().await)
    }
}

#[derive(Default)]
pub(super) struct RuntimeConfigChanges {
    skills: bool,
    mcps: bool,
    rules: bool,
}

impl RuntimeConfigChanges {
    pub(super) fn from_request(request: &UpdateAssistantRequest) -> Self {
        let defaults = request.defaults.as_ref();
        Self {
            skills: defaults.is_some_and(|defaults| defaults.skills.is_some())
                || request.enabled_skills.is_some()
                || request.disabled_builtin_skills.is_some(),
            mcps: defaults.is_some_and(|defaults| defaults.mcps.is_some()),
            rules: request.rules.is_some(),
        }
    }

    pub(super) fn any(&self) -> bool {
        self.skills || self.mcps || self.rules
    }
}

impl AssistantService {
    /// Capture the old complete settings before any source write can expose a partial save.
    pub(super) async fn runtime_config_baseline(
        &self,
        user_id: &str,
        assistant_id: &str,
    ) -> Result<Option<AssistantRuntimeConfig>, AssistantError> {
        if let Some(config) = self.runtime_config_repo.get_for_user(user_id, assistant_id).await? {
            return Ok(Some(config));
        }
        let Some(definition) = self
            .definition_repo
            .get_by_assistant_id_for_user(user_id, assistant_id)
            .await?
        else {
            // Older rule-only clients may create files before the assistant exists.
            return Ok(None);
        };
        if definition.source == "builtin" {
            return Ok(None);
        }

        let mut config = AssistantRuntimeConfig::default();
        self.read_runtime_defaults(user_id, &definition, &mut config, true, true)
            .await?;
        config.rules_by_locale = read_rule_files(&self.user_rules_dir_for_user(user_id), assistant_id)?;
        if user_id == DEFAULT_USER_ID {
            config.legacy_rules_by_locale = read_rule_files(&self.user_rules_root_dir(), assistant_id)?;
        }
        self.runtime_config_repo
            .put_for_user(user_id, assistant_id, &config)
            .await?;
        Ok(Some(config))
    }

    async fn read_runtime_defaults(
        &self,
        user_id: &str,
        definition: &AssistantDefinitionRow,
        config: &mut AssistantRuntimeConfig,
        skills: bool,
        mcps: bool,
    ) -> Result<(), AssistantError> {
        let preference = self.preference_repo.get_for_user(user_id, &definition.id).await?;
        if skills {
            config.skills_mode = definition.default_skills_mode.clone();
            let (skill_ids, disabled) = if config.skills_mode == "fixed" {
                (
                    Some(definition.default_skill_ids.as_str()),
                    Some(definition.default_disabled_builtin_skill_ids.as_str()),
                )
            } else {
                (
                    preference.as_ref().map(|value| value.last_skill_ids.as_str()),
                    preference
                        .as_ref()
                        .map(|value| value.last_disabled_builtin_skill_ids.as_str()),
                )
            };
            config.skill_ids = decode_str_list(skill_ids)?;
            config.disabled_builtin_skill_ids = decode_str_list(disabled)?;
        }
        if mcps {
            config.mcps_mode = definition.default_mcps_mode.clone();
            let ids = if config.mcps_mode == "fixed" {
                Some(definition.default_mcp_ids.as_str())
            } else {
                preference.as_ref().map(|value| value.last_mcp_ids.as_str())
            };
            config.mcp_ids = decode_str_list(ids)?;
        }
        Ok(())
    }

    pub(super) async fn publish_runtime_config(
        &self,
        user_id: &str,
        assistant_id: &str,
        prior: AssistantRuntimeConfig,
        changes: RuntimeConfigChanges,
        rules: Option<&AssistantRuleMutation>,
    ) -> Result<(), AssistantError> {
        let mut next = prior.clone();
        if changes.skills || changes.mcps {
            let definition = self
                .definition_repo
                .get_by_assistant_id_for_user(user_id, assistant_id)
                .await?
                .ok_or_else(|| AssistantError::NotFound(format!("assistant '{assistant_id}' not found")))?;
            self.read_runtime_defaults(user_id, &definition, &mut next, changes.skills, changes.mcps)
                .await?;
        }
        if changes.skills
            && (prior.skills_revision == 0
                || next.skills_mode == "auto"
                || next.skills_mode != prior.skills_mode
                || next.skill_ids != prior.skill_ids
                || next.disabled_builtin_skill_ids != prior.disabled_builtin_skill_ids)
        {
            next.skills_revision += 1;
        }
        if changes.mcps
            && (prior.mcps_revision == 0
                || next.mcps_mode == "auto"
                || next.mcps_mode != prior.mcps_mode
                || next.mcp_ids != prior.mcp_ids)
        {
            next.mcps_revision += 1;
        }
        if let Some(rules) = rules {
            match rules {
                AssistantRuleMutation::Write { locale, content } => {
                    let locale = locale.as_deref().unwrap_or_default();
                    if content.is_empty() {
                        next.rules_by_locale.remove(locale);
                    } else {
                        next.rules_by_locale.insert(locale.to_owned(), content.clone());
                    }
                }
                AssistantRuleMutation::Clear => {
                    next.rules_by_locale.clear();
                    next.legacy_rules_by_locale.clear();
                }
            }
            if prior.rules_revision == 0
                || next.rules_by_locale != prior.rules_by_locale
                || next.legacy_rules_by_locale != prior.legacy_rules_by_locale
            {
                next.rules_revision += 1;
            }
        }
        if next != prior {
            self.runtime_config_repo
                .put_for_user(user_id, assistant_id, &next)
                .await?;
        }
        Ok(())
    }

    pub(super) async fn mutate_rule_for_user(
        &self,
        user_id: &str,
        assistant_id: &str,
        rules: AssistantRuleMutation,
    ) -> Result<bool, AssistantError> {
        let _guard = self.mutation_locks.lock(user_id, assistant_id).await?;
        if self.classify_source_for_user(user_id, assistant_id).await == AssistantSource::Builtin {
            return Err(AssistantError::BadRequest(
                "Cannot edit rules for built-in assistant".into(),
            ));
        }
        let prior = self.runtime_config_baseline(user_id, assistant_id).await?;
        let changed = self.apply_rule_source_mutation(user_id, assistant_id, &rules)?;
        if let Some(prior) = prior {
            self.publish_runtime_config(
                user_id,
                assistant_id,
                prior,
                RuntimeConfigChanges::default(),
                Some(&rules),
            )
            .await?;
        }
        Ok(changed)
    }

    pub(super) fn apply_rule_source_mutation(
        &self,
        user_id: &str,
        assistant_id: &str,
        rules: &AssistantRuleMutation,
    ) -> Result<bool, AssistantError> {
        match rules {
            AssistantRuleMutation::Write { locale, content } => {
                let path = self.user_rule_path_for_user(user_id, assistant_id, locale.as_deref());
                let parent = self.user_rules_dir_for_user(user_id);
                fs::create_dir_all(&parent).map_err(rule_file_error)?;
                let mut draft = tempfile::NamedTempFile::new_in(parent).map_err(rule_file_error)?;
                draft.write_all(content.as_bytes()).map_err(rule_file_error)?;
                draft.persist(path).map_err(|error| rule_file_error(error.error))?;
                Ok(true)
            }
            AssistantRuleMutation::Clear => {
                let removed = clear_rule_files(&self.user_rules_dir_for_user(user_id), assistant_id)?;
                if user_id == DEFAULT_USER_ID {
                    Ok(clear_rule_files(&self.user_rules_root_dir(), assistant_id)? || removed)
                } else {
                    Ok(removed)
                }
            }
        }
    }
}

fn rule_file_error(error: io::Error) -> AssistantError {
    AssistantError::Internal(format!("assistant rule file operation failed: {error}"))
}

fn clear_rule_files(directory: &Path, assistant_id: &str) -> Result<bool, AssistantError> {
    let mut removed = false;
    for (_, _, path) in rule_files(directory, assistant_id)? {
        match fs::remove_file(path) {
            Ok(()) => removed = true,
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(rule_file_error(error)),
        }
    }
    Ok(removed)
}

fn read_rule_files(directory: &Path, assistant_id: &str) -> Result<BTreeMap<String, String>, AssistantError> {
    let mut rules = BTreeMap::new();
    for (_, locale, path) in rule_files(directory, assistant_id)? {
        if !fs::symlink_metadata(&path).map_err(rule_file_error)?.is_file() {
            continue;
        }
        let content = fs::read_to_string(path).map_err(rule_file_error)?;
        if !content.is_empty() {
            rules.entry(locale).or_insert(content);
        }
    }
    Ok(rules)
}

fn rule_files(directory: &Path, assistant_id: &str) -> Result<Vec<(bool, String, PathBuf)>, AssistantError> {
    let entries = match fs::read_dir(directory) {
        Ok(entries) => entries,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(rule_file_error(error)),
    };
    let encoded_id = encode_filename_component(assistant_id);
    let mut files = Vec::new();
    for entry in entries {
        let entry = entry.map_err(rule_file_error)?;
        let name = entry.file_name();
        let Some(stem) = name.to_str().and_then(|name| name.strip_suffix(".md")) else {
            continue;
        };
        if let Some(locale) = rule_locale_suffix(stem, &encoded_id)
            && let Some(decoded) = decode_rule_locale(locale)
        {
            files.push((false, decoded, entry.path()));
        } else if legacy_filename_component_is_safe(assistant_id)
            && let Some(locale) = rule_locale_suffix(stem, assistant_id)
        {
            files.push((true, locale.to_owned(), entry.path()));
        }
    }
    files.sort_unstable();
    Ok(files)
}

fn rule_locale_suffix<'a>(stem: &'a str, prefix: &str) -> Option<&'a str> {
    match stem.strip_prefix(prefix)? {
        "" => Some(""),
        suffix => suffix.strip_prefix('.').filter(|suffix| !suffix.is_empty()),
    }
}

fn decode_rule_locale(locale: &str) -> Option<String> {
    let mut bytes = locale.bytes();
    let mut decoded = Vec::with_capacity(locale.len());
    while let Some(byte) = bytes.next() {
        if byte == b'%' {
            let hi = char::from(bytes.next()?).to_digit(16)?;
            let lo = char::from(bytes.next()?).to_digit(16)?;
            decoded.push((hi * 16 + lo) as u8);
        } else {
            decoded.push(byte);
        }
    }
    let decoded = String::from_utf8(decoded).ok()?;
    (encode_filename_component(&decoded) == locale).then_some(decoded)
}
