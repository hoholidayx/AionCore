use std::collections::BTreeMap;
use std::fmt;

use serde::{Deserialize, Serialize};

/// The committed settings that an ordinary assistant conversation may follow.
/// A revision of zero is the baseline captured before that component's first edit.
#[derive(Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AssistantRuntimeConfig {
    pub skills_revision: i64,
    pub mcps_revision: i64,
    pub rules_revision: i64,
    pub skills_mode: String,
    pub mcps_mode: String,
    pub skill_ids: Vec<String>,
    pub disabled_builtin_skill_ids: Vec<String>,
    pub mcp_ids: Vec<String>,
    /// The empty locale key represents the locale-neutral rule.
    pub rules_by_locale: BTreeMap<String, String>,
    /// Rule files predating user-scoped storage, available only for the local default user.
    #[serde(default)]
    pub legacy_rules_by_locale: BTreeMap<String, String>,
}

impl fmt::Debug for AssistantRuntimeConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AssistantRuntimeConfig")
            .field("skills_revision", &self.skills_revision)
            .field("mcps_revision", &self.mcps_revision)
            .field("rules_revision", &self.rules_revision)
            .finish_non_exhaustive()
    }
}

impl AssistantRuntimeConfig {
    pub fn resolve_rule(&self, locale: Option<&str>) -> &str {
        let rules = if self.rules_by_locale.is_empty() {
            &self.legacy_rules_by_locale
        } else {
            &self.rules_by_locale
        };
        locale
            .and_then(|locale| rules.get(locale))
            .or_else(|| rules.get(""))
            .or_else(|| rules.values().next())
            .map(String::as_str)
            .unwrap_or_default()
    }
}
