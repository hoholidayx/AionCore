//! Host-private Mem0 endpoint, credentials, and assistant namespaces.

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::fs::{File, OpenOptions};
use std::io::{self, Read};
use std::net::IpAddr;
use std::path::Path;
use std::sync::Arc;

use aionui_api_types::TeamSessionBinding;
use aionui_common::AgentType;
use aionui_db::models::ConversationRow;
use aionui_db::{
    ConversationAssistantSnapshotRow, IAgentMetadataRepository, IAssistantDefinitionRepository,
    IAssistantOverlayRepository, IConversationRepository, IUserRepository, resolve_agent_binding_from_rows,
};
use serde::Deserialize;
use url::{Host, Url};

const MAX_CONFIG_BYTES: u64 = 128 * 1024;
const USER_NAMESPACE_PREFIX: &str = "aion-user-";
const ASSISTANT_NAMESPACE_PREFIX: &str = "aion-assistant-";
const MAX_NAMESPACE_ID_BYTES: usize = 128;

type AssistantKey = (String, String);

#[derive(Clone, PartialEq, Eq, Hash)]
struct Mem0Namespace {
    user_id: String,
    agent_id: String,
}

enum Credentials {
    Legacy(HashMap<AssistantKey, Arc<str>>),
    Service {
        bearer: Arc<str>,
        aliases: HashMap<AssistantKey, Mem0Namespace>,
    },
}

pub struct Mem0Config {
    url: Arc<Url>,
    credentials: Credentials,
}

pub struct Mem0Repositories {
    pub users: Arc<dyn IUserRepository>,
    pub conversations: Arc<dyn IConversationRepository>,
    pub definitions: Arc<dyn IAssistantDefinitionRepository>,
    pub overlays: Arc<dyn IAssistantOverlayRepository>,
    pub agents: Arc<dyn IAgentMetadataRepository>,
}

#[derive(Debug, thiserror::Error)]
pub enum Mem0ConfigError {
    #[error("cannot read mem0-mcp.json")]
    Read,
    #[error("mem0-mcp.json must be a regular file owned by the service user with mode 0600")]
    Permissions,
    #[error("invalid mem0-mcp.json schema")]
    Schema,
    #[error("Mem0 URL must be a direct HTTP(S) URL to a private or loopback IP with path /mcp")]
    Endpoint,
    #[error("invalid or duplicate Mem0 profile")]
    Profile,
    #[error("invalid Mem0 service bearer")]
    ServiceBearer,
    #[error("invalid or conflicting Mem0 namespace alias")]
    NamespaceAlias,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ConfigFile {
    url: String,
    profiles: Option<Vec<ProfileFile>>,
    service_bearer: Option<String>,
    namespace_aliases: Option<Vec<NamespaceAliasFile>>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ProfileFile {
    user_id: String,
    assistant_definition_id: String,
    bearer: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct NamespaceAliasFile {
    user_id: String,
    assistant_definition_id: String,
    mem0_user_id: String,
    mem0_agent_id: String,
}

impl Mem0Config {
    pub fn load(data_dir: &Path) -> Result<Option<Self>, Mem0ConfigError> {
        let path = data_dir.join("mem0-mcp.json");
        let file = match open_config(&path) {
            Ok(file) => file,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(_) => return Err(Mem0ConfigError::Read),
        };
        let metadata = file.metadata().map_err(|_| Mem0ConfigError::Read)?;
        if !metadata.is_file() || !private_permissions(&metadata) {
            return Err(Mem0ConfigError::Permissions);
        }

        let mut bytes = Vec::new();
        file.take(MAX_CONFIG_BYTES + 1)
            .read_to_end(&mut bytes)
            .map_err(|_| Mem0ConfigError::Read)?;
        if bytes.len() as u64 > MAX_CONFIG_BYTES {
            return Err(Mem0ConfigError::Schema);
        }
        let file: ConfigFile = serde_json::from_slice(&bytes).map_err(|_| Mem0ConfigError::Schema)?;
        Self::from_file(file).map(Some)
    }

    fn from_file(file: ConfigFile) -> Result<Self, Mem0ConfigError> {
        let url = parse_endpoint(&file.url).ok_or(Mem0ConfigError::Endpoint)?;
        let credentials = match (file.profiles, file.service_bearer, file.namespace_aliases) {
            (Some(profiles), None, None) => Credentials::Legacy(load_profiles(profiles)?),
            (None, Some(bearer), aliases) if valid_bearer(&bearer) => Credentials::Service {
                bearer: Arc::from(bearer),
                aliases: load_namespace_aliases(aliases.unwrap_or_default())?,
            },
            (None, Some(_), _) => return Err(Mem0ConfigError::ServiceBearer),
            _ => return Err(Mem0ConfigError::Schema),
        };
        Ok(Self {
            url: Arc::new(url),
            credentials,
        })
    }

    pub fn has_profile(&self, user_id: &str, assistant_definition_id: &str) -> bool {
        match &self.credentials {
            Credentials::Legacy(_) => self.has_explicit_namespace(user_id, assistant_definition_id),
            Credentials::Service { .. } => valid_assistant_scope(user_id, assistant_definition_id),
        }
    }

    pub fn has_explicit_namespace(&self, user_id: &str, assistant_definition_id: &str) -> bool {
        let key = (user_id.to_owned(), assistant_definition_id.to_owned());
        match &self.credentials {
            Credentials::Legacy(profiles) => profiles.contains_key(&key),
            Credentials::Service { aliases, .. } => aliases.contains_key(&key),
        }
    }

    pub async fn binding_for(
        &self,
        row: &ConversationRow,
        snapshot: &ConversationAssistantSnapshotRow,
        repos: &Arc<Mem0Repositories>,
    ) -> Option<Mem0Binding> {
        if !eligible_conversation(row) || row.id != snapshot.conversation_id {
            return None;
        }
        let key = (row.user_id.clone(), snapshot.assistant_definition_id.clone());
        let (bearer, namespace) = match &self.credentials {
            Credentials::Legacy(profiles) => (profiles.get(&key)?.clone(), None),
            Credentials::Service { bearer, aliases } => {
                if !valid_assistant_scope(&key.0, &key.1) {
                    return None;
                }
                let namespace = aliases.get(&key).cloned().unwrap_or_else(|| Mem0Namespace {
                    user_id: format!("{USER_NAMESPACE_PREFIX}{}", key.0),
                    agent_id: format!("{ASSISTANT_NAMESPACE_PREFIX}{}", key.1),
                });
                (bearer.clone(), Some(namespace))
            }
        };
        let binding = Mem0Binding {
            url: self.url.clone(),
            bearer,
            namespace,
            user_id: row.user_id.clone(),
            assistant_definition_id: snapshot.assistant_definition_id.clone(),
            conversation_id: row.id.clone(),
            repos: repos.clone(),
        };
        binding.is_entitled().await.then_some(binding)
    }
}

fn load_profiles(entries: Vec<ProfileFile>) -> Result<HashMap<AssistantKey, Arc<str>>, Mem0ConfigError> {
    let mut profiles = HashMap::new();
    let mut bearers = HashSet::new();
    for profile in entries {
        if !valid_id(&profile.user_id) || !valid_id(&profile.assistant_definition_id) || !valid_bearer(&profile.bearer)
        {
            return Err(Mem0ConfigError::Profile);
        }
        let bearer: Arc<str> = Arc::from(profile.bearer);
        if !bearers.insert(bearer.clone())
            || profiles
                .insert((profile.user_id, profile.assistant_definition_id), bearer)
                .is_some()
        {
            return Err(Mem0ConfigError::Profile);
        }
    }
    Ok(profiles)
}

fn load_namespace_aliases(
    entries: Vec<NamespaceAliasFile>,
) -> Result<HashMap<AssistantKey, Mem0Namespace>, Mem0ConfigError> {
    let mut aliases = HashMap::new();
    let mut targets = HashSet::new();
    for entry in entries {
        if !valid_assistant_scope(&entry.user_id, &entry.assistant_definition_id)
            || !valid_namespace_id(&entry.mem0_user_id, MAX_NAMESPACE_ID_BYTES)
            || !valid_namespace_id(&entry.mem0_agent_id, MAX_NAMESPACE_ID_BYTES)
            || (entry.mem0_user_id.starts_with(USER_NAMESPACE_PREFIX)
                && entry.mem0_agent_id.starts_with(ASSISTANT_NAMESPACE_PREFIX))
        {
            return Err(Mem0ConfigError::NamespaceAlias);
        }
        let namespace = Mem0Namespace {
            user_id: entry.mem0_user_id,
            agent_id: entry.mem0_agent_id,
        };
        if !targets.insert(namespace.clone())
            || aliases
                .insert((entry.user_id, entry.assistant_definition_id), namespace)
                .is_some()
        {
            return Err(Mem0ConfigError::NamespaceAlias);
        }
    }
    Ok(aliases)
}

#[derive(Clone)]
pub struct Mem0Binding {
    url: Arc<Url>,
    bearer: Arc<str>,
    namespace: Option<Mem0Namespace>,
    user_id: String,
    assistant_definition_id: String,
    conversation_id: String,
    repos: Arc<Mem0Repositories>,
}

impl fmt::Debug for Mem0Binding {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_struct("Mem0Binding").finish_non_exhaustive()
    }
}

impl PartialEq for Mem0Binding {
    fn eq(&self, other: &Self) -> bool {
        self.url == other.url
            && self.bearer == other.bearer
            && self.namespace == other.namespace
            && self.user_id == other.user_id
            && self.assistant_definition_id == other.assistant_definition_id
            && self.conversation_id == other.conversation_id
    }
}

impl Eq for Mem0Binding {}

impl Mem0Binding {
    pub(crate) fn url(&self) -> &Url {
        &self.url
    }

    pub(crate) fn bearer(&self) -> &str {
        &self.bearer
    }

    pub(crate) fn namespace(&self) -> Option<(&str, &str)> {
        self.namespace
            .as_ref()
            .map(|namespace| (namespace.user_id.as_str(), namespace.agent_id.as_str()))
    }

    /// Recheck the live owner, conversation scope, assistant binding, and opt-in before tool calls.
    pub(crate) async fn is_entitled(&self) -> bool {
        if !matches!(self.repos.users.find_active_by_id(&self.user_id).await, Ok(Some(_))) {
            return false;
        }
        let repo = &self.repos.conversations;
        let Ok(Some(row)) = repo.get_unarchived_for_user(&self.user_id, &self.conversation_id).await else {
            return false;
        };
        if !eligible_conversation(&row) {
            return false;
        }
        let Ok(Some(snapshot)) = repo.get_assistant_snapshot(&self.user_id, &self.conversation_id).await else {
            return false;
        };
        self.snapshot_is_entitled(&snapshot).await
    }

    async fn snapshot_is_entitled(&self, snapshot: &ConversationAssistantSnapshotRow) -> bool {
        if snapshot.assistant_definition_id != self.assistant_definition_id {
            return false;
        }
        let Ok(Some(overlay)) = self
            .repos
            .overlays
            .get_for_user(&self.user_id, &self.assistant_definition_id)
            .await
        else {
            return false;
        };
        if !overlay.enabled || !overlay.mem0_memory_enabled {
            return false;
        }
        let Ok(Some(definition)) = self
            .repos
            .definitions
            .get_by_assistant_id_for_user(&self.user_id, &snapshot.assistant_id)
            .await
        else {
            return false;
        };
        if definition.id != self.assistant_definition_id {
            return false;
        }
        let agent_id = overlay.agent_id_override.as_deref().unwrap_or(&definition.agent_id);
        self.repos
            .agents
            .list_all_for_user(&self.user_id)
            .await
            .is_ok_and(|agents| {
                resolve_agent_binding_from_rows(&agents, agent_id).is_some_and(|binding| {
                    binding.agent_type == AgentType::Aionrs.serde_name()
                        && binding.runtime_backend == AgentType::Aionrs.serde_name()
                })
            })
    }
}

fn eligible_conversation(row: &ConversationRow) -> bool {
    row.r#type == AgentType::Aionrs.serde_name()
        && row.source.as_deref() == Some("aionui")
        && row.channel_chat_id.is_none()
        && TeamSessionBinding::team_id_marker_from_extra_str(&row.extra).is_none()
        && serde_json::from_str::<serde_json::Value>(&row.extra).is_ok_and(|extra| {
            extra.as_object().is_some_and(|extra| {
                ["cron_job_id", "cronJobId"]
                    .iter()
                    .all(|key| extra.get(*key).is_none_or(serde_json::Value::is_null))
            })
        })
}

fn valid_id(value: &str) -> bool {
    !value.is_empty() && value.len() <= 255 && !value.chars().any(char::is_whitespace)
}

fn valid_bearer(value: &str) -> bool {
    (24..=256).contains(&value.len()) && value.bytes().all(|byte| byte.is_ascii_graphic())
}

fn valid_namespace_id(value: &str, max_bytes: usize) -> bool {
    !value.is_empty()
        && value.len() <= max_bytes
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
}

fn valid_assistant_scope(user_id: &str, assistant_definition_id: &str) -> bool {
    valid_namespace_id(user_id, MAX_NAMESPACE_ID_BYTES - USER_NAMESPACE_PREFIX.len())
        && valid_namespace_id(
            assistant_definition_id,
            MAX_NAMESPACE_ID_BYTES - ASSISTANT_NAMESPACE_PREFIX.len(),
        )
}

fn parse_endpoint(value: &str) -> Option<Url> {
    let (scheme, rest) = value.split_once("://")?;
    let (authority, path) = rest.split_once('/')?;
    if !matches!(scheme, "http" | "https") || path != "mcp" {
        return None;
    }
    let ip = if let Some(host) = authority.strip_prefix('[') {
        IpAddr::V6(host.split_once(']')?.0.parse().ok()?)
    } else {
        IpAddr::V4(authority.split(':').next()?.parse().ok()?)
    };
    let allowed = match ip {
        IpAddr::V4(ip) => ip.is_loopback() || ip.is_private(),
        IpAddr::V6(ip) => ip.is_loopback() || ip.is_unique_local(),
    };
    let url = Url::parse(value).ok()?;
    if allowed
        && url.host()
            == Some(match ip {
                IpAddr::V4(ip) => Host::Ipv4(ip),
                IpAddr::V6(ip) => Host::Ipv6(ip),
            })
        && url.username().is_empty()
        && url.password().is_none()
        && !authority.contains('@')
        && url.port() != Some(0)
        && url.path() == "/mcp"
        && url.query().is_none()
        && url.fragment().is_none()
    {
        Some(url)
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn parse_config(value: serde_json::Value) -> Result<Mem0Config, Mem0ConfigError> {
        let file = serde_json::from_value(value).map_err(|_| Mem0ConfigError::Schema)?;
        Mem0Config::from_file(file)
    }

    #[test]
    fn accepts_only_literal_private_or_loopback_mcp_endpoints() {
        for value in [
            "http://192.168.50.7:13001/mcp",
            "https://10.0.0.7/mcp",
            "http://127.0.0.1:8080/mcp",
            "http://[::1]:8080/mcp",
            "https://[fd00::7]/mcp",
        ] {
            assert!(parse_endpoint(value).is_some(), "rejected {value}");
        }
        for value in [
            "http://mem0.local/mcp",
            "http://127.1/mcp",
            "http://8.8.8.8/mcp",
            "http://169.254.169.254/mcp",
            "http://[::ffff:127.0.0.1]/mcp",
            "http://127.0.0.1:0/mcp",
            "http://user:pass@127.0.0.1/mcp",
            "http://127.0.0.1/mcp/",
            "http://127.0.0.1/mcp?query=1",
            "http://127.0.0.1/mcp#fragment",
            "http://127.0.0.1/a/../mcp",
        ] {
            assert!(parse_endpoint(value).is_none(), "accepted {value}");
        }
    }

    fn direct_conversation() -> ConversationRow {
        ConversationRow {
            id: "conversation-a".into(),
            user_id: "user-a".into(),
            name: "Ordinary chat".into(),
            r#type: "aionrs".into(),
            extra: "{}".into(),
            model: None,
            status: Some("pending".into()),
            source: Some("aionui".into()),
            channel_chat_id: None,
            pinned: false,
            pinned_at: None,
            created_at: 0,
            updated_at: 0,
            project_id: None,
            folder_id: None,
            name_source: None,
        }
    }

    #[test]
    fn only_ordinary_aionrs_conversations_are_eligible() {
        let direct = direct_conversation();
        assert!(eligible_conversation(&direct));
        for extra in [
            r#"{"cron_job_id":"job-a"}"#,
            r#"{"cronJobId":"job-a"}"#,
            r#"{"teamId":"team-a"}"#,
            "[]",
            "invalid",
        ] {
            assert!(!eligible_conversation(&ConversationRow {
                extra: extra.into(),
                ..direct.clone()
            }));
        }
        assert!(!eligible_conversation(&ConversationRow {
            source: Some("telegram".into()),
            ..direct.clone()
        }));
        assert!(!eligible_conversation(&ConversationRow {
            channel_chat_id: Some("channel-a".into()),
            ..direct.clone()
        }));
        assert!(!eligible_conversation(&ConversationRow {
            r#type: "acp".into(),
            ..direct
        }));
    }

    #[tokio::test]
    async fn existing_binding_loses_access_when_settings_or_conversation_change() {
        use aionui_db::{
            ISidebarStore, SqliteAgentMetadataRepository, SqliteAssistantDefinitionRepository,
            SqliteAssistantOverlayRepository, SqliteConversationRepository, SqliteSidebarStore, SqliteUserRepository,
            UpsertAssistantDefinitionParams, UpsertConversationAssistantSnapshotParams, UserStatus,
            init_database_memory,
        };

        let database = init_database_memory().await.unwrap();
        let pool = database.pool();
        let repos = Arc::new(Mem0Repositories {
            users: Arc::new(SqliteUserRepository::new(pool.clone())),
            conversations: Arc::new(SqliteConversationRepository::new(pool.clone())),
            definitions: Arc::new(SqliteAssistantDefinitionRepository::new(pool.clone())),
            overlays: Arc::new(SqliteAssistantOverlayRepository::new(pool.clone())),
            agents: Arc::new(SqliteAgentMetadataRepository::new(pool.clone())),
        });
        let user = repos
            .users
            .create_user("memory-owner", "test-password-hash")
            .await
            .unwrap();
        let agents = repos.agents.list_all_for_user(&user.id).await.unwrap();
        let engine_id = resolve_agent_binding_from_rows(&agents, AgentType::Aionrs.serde_name())
            .unwrap()
            .agent_id;
        let definition = repos
            .definitions
            .upsert_for_user(
                &user.id,
                &UpsertAssistantDefinitionParams {
                    id: "def-memory",
                    assistant_id: "memory-assistant",
                    source: "user",
                    owner_type: "user",
                    source_ref: None,
                    name: "Memory Assistant",
                    name_i18n: "{}",
                    description: None,
                    description_i18n: "{}",
                    avatar_type: "none",
                    avatar_value: None,
                    agent_id: &engine_id,
                    rule_resource_type: "user_file",
                    rule_resource_ref: None,
                    recommended_prompts: "[]",
                    recommended_prompts_i18n: "{}",
                    default_model_mode: "auto",
                    default_model_value: None,
                    default_permission_mode: "auto",
                    default_permission_value: None,
                    default_thought_level_mode: "auto",
                    default_thought_level_value: None,
                    default_skills_mode: "auto",
                    default_skill_ids: "[]",
                    custom_skill_names: "[]",
                    default_disabled_builtin_skill_ids: "[]",
                    default_mcps_mode: "auto",
                    default_mcp_ids: "[]",
                    async_image_enabled: None,
                    async_image_backend_ids: None,
                },
            )
            .await
            .unwrap();
        let row = ConversationRow {
            user_id: user.id.clone(),
            ..direct_conversation()
        };
        repos.conversations.create(&row).await.unwrap();
        let snapshot = repos
            .conversations
            .upsert_assistant_snapshot(
                &user.id,
                &UpsertConversationAssistantSnapshotParams {
                    conversation_id: &row.id,
                    assistant_definition_id: &definition.id,
                    assistant_id: &definition.assistant_id,
                    assistant_source: &definition.source,
                    agent_id: &engine_id,
                    rules_content: "",
                    default_model_mode: "auto",
                    resolved_model_id: None,
                    default_permission_mode: "auto",
                    resolved_permission_value: None,
                    default_thought_level_mode: "auto",
                    resolved_thought_level_value: None,
                    default_skills_mode: "auto",
                    resolved_skill_ids: "[]",
                    resolved_disabled_builtin_skill_ids: "[]",
                    default_mcps_mode: "auto",
                    resolved_mcp_ids: "[]",
                },
            )
            .await
            .unwrap()
            .unwrap();
        let legacy_bearer = "x".repeat(24);
        let service_bearer = "y".repeat(24);
        let legacy = parse_config(json!({
            "url": "http://127.0.0.1/mcp",
            "profiles": [{"user_id": user.id, "assistant_definition_id": definition.id, "bearer": legacy_bearer}]
        }))
        .unwrap();
        let shared = parse_config(json!({
            "url": "http://127.0.0.1/mcp",
            "service_bearer": service_bearer,
        }))
        .unwrap();
        let aliased = parse_config(json!({
            "url": "http://127.0.0.1/mcp",
            "service_bearer": service_bearer,
            "namespace_aliases": [{
                "user_id": user.id, "assistant_definition_id": definition.id,
                "mem0_user_id": "previous-owner", "mem0_agent_id": "previous-assistant"
            }]
        }))
        .unwrap();
        assert!(legacy.binding_for(&row, &snapshot, &repos).await.is_none());
        assert!(shared.binding_for(&row, &snapshot, &repos).await.is_none());
        repos
            .overlays
            .set_mem0_memory_enabled_for_user(&user.id, &definition.id, true, (0, true))
            .await
            .unwrap();
        let legacy_binding = legacy.binding_for(&row, &snapshot, &repos).await.unwrap();
        assert_eq!(legacy_binding.bearer(), legacy_bearer);
        assert_eq!(legacy_binding.namespace(), None);
        let binding = shared.binding_for(&row, &snapshot, &repos).await.unwrap();
        assert_eq!(binding.bearer(), service_bearer);
        let expected_user = format!("aion-user-{}", user.id);
        assert_eq!(
            binding.namespace(),
            Some((expected_user.as_str(), "aion-assistant-def-memory"))
        );
        let aliased_binding = aliased.binding_for(&row, &snapshot, &repos).await.unwrap();
        assert_eq!(aliased_binding.bearer(), service_bearer);
        assert_eq!(
            aliased_binding.namespace(),
            Some(("previous-owner", "previous-assistant"))
        );
        assert_ne!(binding, aliased_binding);
        assert!(binding.is_entitled().await);
        assert!(legacy_binding.is_entitled().await);
        let debug = format!("{binding:?}");
        assert!(!debug.contains(&service_bearer));
        assert!(!debug.contains(&expected_user));
        let other_user = ConversationRow {
            user_id: "other-user".into(),
            ..row.clone()
        };
        assert!(shared.binding_for(&other_user, &snapshot, &repos).await.is_none());
        repos
            .overlays
            .set_mem0_memory_enabled_for_user(&user.id, &definition.id, false, (0, true))
            .await
            .unwrap();
        assert!(!binding.is_entitled().await);
        assert!(!legacy_binding.is_entitled().await);
        repos
            .overlays
            .set_mem0_memory_enabled_for_user(&user.id, &definition.id, true, (0, true))
            .await
            .unwrap();
        let sidebar = SqliteSidebarStore::new(pool.clone());
        sidebar
            .set_conversation_archived(&user.id, &row.id, Some(1))
            .await
            .unwrap();
        assert!(!binding.is_entitled().await);
        assert!(!legacy_binding.is_entitled().await);
        sidebar
            .set_conversation_archived(&user.id, &row.id, None)
            .await
            .unwrap();
        assert!(binding.is_entitled().await);
        assert!(legacy_binding.is_entitled().await);
        repos.users.set_status(&user.id, UserStatus::Disabled).await.unwrap();
        assert!(!binding.is_entitled().await);
        assert!(!legacy_binding.is_entitled().await);
    }

    #[test]
    fn shared_config_requires_one_valid_bearer_and_disjoint_namespaces() {
        let alias = json!({
            "user_id": "user-a", "assistant_definition_id": "definition-a",
            "mem0_user_id": "old-user", "mem0_agent_id": "old-assistant"
        });
        let mut payload = json!({
            "url": "http://127.0.0.1/mcp", "service_bearer": "x".repeat(24),
            "namespace_aliases": [alias]
        });
        let config = parse_config(payload.clone()).unwrap();
        assert!(config.has_profile("user-a", "definition-a"));
        assert!(config.has_profile("user-b", "definition-b"));
        assert!(!config.has_profile("user\n", "definition-a"));
        assert!(config.has_explicit_namespace("user-a", "definition-a"));
        assert!(!config.has_explicit_namespace("user-b", "definition-b"));

        let mut mixed = payload.clone();
        mixed["profiles"] = json!([]);
        assert!(matches!(parse_config(mixed), Err(Mem0ConfigError::Schema)));
        let mut short_bearer = payload.clone();
        short_bearer["service_bearer"] = json!("too-short");
        assert!(matches!(
            parse_config(short_bearer),
            Err(Mem0ConfigError::ServiceBearer)
        ));

        let mut conflicting = payload.clone();
        conflicting["namespace_aliases"][0]["mem0_user_id"] = json!("aion-user-user-b");
        conflicting["namespace_aliases"][0]["mem0_agent_id"] = json!("aion-assistant-definition-b");
        assert!(matches!(
            parse_config(conflicting),
            Err(Mem0ConfigError::NamespaceAlias)
        ));
        let mut duplicate_target = alias;
        duplicate_target["assistant_definition_id"] = json!("definition-b");
        payload["namespace_aliases"]
            .as_array_mut()
            .unwrap()
            .push(duplicate_target);
        assert!(matches!(parse_config(payload), Err(Mem0ConfigError::NamespaceAlias)));
    }

    #[cfg(unix)]
    #[test]
    fn loads_only_private_owned_files_and_matches_the_exact_profile_pair() {
        use std::os::unix::fs::{PermissionsExt, symlink};

        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("mem0-mcp.json");
        let load = || Mem0Config::load(directory.path());
        assert!(load().unwrap().is_none());

        let payload = serde_json::json!({
            "url": "http://127.0.0.1/mcp",
            "profiles": [{"user_id":"user-a", "assistant_definition_id":"definition-a", "bearer":"x".repeat(24)}]
        });
        std::fs::write(&path, payload.to_string()).unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).unwrap();
        assert!(matches!(load(), Err(Mem0ConfigError::Permissions)));

        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
        let config = load().unwrap().unwrap();
        assert!(config.has_profile("user-a", "definition-a"));
        assert!(!config.has_profile("user-b", "definition-a"));
        assert!(!config.has_profile("user-a", "definition-b"));

        let mut duplicate = payload;
        duplicate["profiles"].as_array_mut().unwrap().push(serde_json::json!({
            "user_id":"user-b", "assistant_definition_id":"definition-b", "bearer":"x".repeat(24)
        }));
        std::fs::write(&path, duplicate.to_string()).unwrap();
        assert!(matches!(load(), Err(Mem0ConfigError::Profile)));

        let target = directory.path().join("other.json");
        std::fs::rename(&path, &target).unwrap();
        symlink(&target, &path).unwrap();
        assert!(matches!(load(), Err(Mem0ConfigError::Read)));
    }
}

fn open_config(path: &Path) -> io::Result<File> {
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK);
    }
    options.open(path)
}

fn private_permissions(metadata: &std::fs::Metadata) -> bool {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        // SAFETY: geteuid has no arguments or memory effects.
        metadata.mode() & 0o7777 == 0o600 && metadata.uid() == unsafe { libc::geteuid() }
    }
    #[cfg(not(unix))]
    {
        let _ = metadata;
        false
    }
}
