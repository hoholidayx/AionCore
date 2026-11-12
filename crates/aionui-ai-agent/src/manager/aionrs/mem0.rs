use std::collections::HashMap;
use std::sync::Arc;

use aion_config::config::{McpServerConfig, TransportType};
use aion_mcp::manager::McpManager;
use aion_mcp::protocol::McpContent;
use aion_mcp::transport::McpError;
use aion_protocol::events::ToolCategory;
use aion_tools::Tool;
use aion_types::tool::{JsonSchema, ToolResult};
use aionui_realtime::EventBroadcaster;
use serde_json::{Value, json};
use tokio::sync::OnceCell;

use crate::mem0_config::Mem0Binding;

use super::mem0_approval::Mem0DeleteApprovals;

const SERVER: &str = "aion-memory";

pub(crate) struct Mem0ToolContext {
    pub(crate) binding: Mem0Binding,
    pub(crate) user_id: String,
    pub(crate) broadcaster: Arc<dyn EventBroadcaster>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Action {
    Search,
    List,
    Get,
    Add,
    Update,
    Delete,
}

impl Action {
    const ALL: [Self; 6] = [
        Self::Search,
        Self::List,
        Self::Get,
        Self::Add,
        Self::Update,
        Self::Delete,
    ];

    fn name(self) -> &'static str {
        match self {
            Self::Search => "memory_search",
            Self::List => "memory_list",
            Self::Get => "memory_get",
            Self::Add => "memory_add",
            Self::Update => "memory_update",
            Self::Delete => "memory_delete",
        }
    }

    fn description(self) -> &'static str {
        match self {
            Self::Search => {
                "Search this assistant's long-term memories by meaning. Filter by relevance with minScore, or narrow by categories, flat metadata, or record time. Set includeExpired to view expired memories."
            }
            Self::List => {
                "List this assistant's long-term memories by category, metadata, or record time. Pass nextCursor as cursor with the same filters to continue."
            }
            Self::Get => "Read a long-term memory by its exact ID.",
            Self::Add => {
                "Save durable facts. Follow the memory-common skill when enabled and search first. Pass text, facts, or items. Use items for different categories or importance values and inspect each indexed result. Optionally set expirationDate (YYYY-MM-DD, UTC). Omit secrets."
            }
            Self::Update => {
                "Update a long-term memory in place by ID. Provide text, category, importance, or metadata. Omit secrets."
            }
            Self::Delete => {
                "Preview long-term candidates with query and optional categories or scope. Use memoryId on its own to propose a deletion. Wait for the user's confirmation and the tool result."
            }
        }
    }

    fn schema(self) -> JsonSchema {
        let text = || json!({"type":"string", "minLength":1, "maxLength":8000});
        let id = || json!({"type":"string", "pattern":"^[A-Za-z0-9_-]{1,128}$"});
        let (mut properties, required) = match self {
            Self::Search => (
                json!({
                    "query":text(), "limit":{"type":"integer","minimum":1,"maximum":50},
                    "minScore":{"type":"number","minimum":0,"maximum":1,"description":"Inclusive minimum returned relevance score."}
                }),
                vec!["query"],
            ),
            Self::List => (
                json!({
                    "limit":{"type":"integer","minimum":1,"maximum":1000,"default":1000},
                    "cursor":{"type":"string","minLength":1,"maxLength":128,"description":"nextCursor from the same list query."}
                }),
                vec![],
            ),
            Self::Get => (json!({"memoryId":id()}), vec!["memoryId"]),
            Self::Add => (
                json!({
                    "facts":{"type":"array","items":text(),"minItems":1,"maxItems":25},
                    "text":text(), "category":{"type":"string"},
                    "items":{
                        "type":"array","minItems":1,"maxItems":25,
                        "description":"Use instead of text or facts. Put category and importance on each item and omit top-level category, importance and metadata.",
                        "items":{
                            "type":"object","required":["text"],"additionalProperties":false,
                            "properties":{
                                "text":text(),
                                "category":{"type":"string","minLength":1,"maxLength":80},
                                "importance":{"type":"number","minimum":0,"maximum":1}
                            }
                        }
                    },
                    "importance":{
                        "type":"number","minimum":0,"maximum":1,
                        "description":"Overrides metadata.importance, including an explicit 0. If both are omitted and a category is set, use its built-in default or 0.5 for a custom category."
                    },
                    "metadata":{"type":"object"}, "longTerm":{"type":"boolean","enum":[true]},
                    "expirationDate":{"type":"string","pattern":"^[0-9]{4}-[0-9]{2}-[0-9]{2}$","description":"Last visible date in UTC (YYYY-MM-DD)."}
                }),
                vec![],
            ),
            Self::Update => (
                json!({
                    "memoryId":id(), "text":text(), "category":{"type":"string","minLength":1,"maxLength":80},
                    "importance":{
                        "type":"number","minimum":0,"maximum":1,
                        "description":"Overrides metadata.importance, including an explicit 0. Omit both to keep the existing importance, even when changing category."
                    },
                    "metadata":{"type":"object","maxProperties":40,"description":"Top-level metadata keys to merge."}
                }),
                vec!["memoryId"],
            ),
            Self::Delete => (json!({"memoryId":id(),"query":text()}), vec![]),
        };
        if matches!(self, Self::Search | Self::List | Self::Delete) {
            properties["categories"] = json!({"type":"array","items":{"type":"string"},"minItems":1,"maxItems":20});
            properties["scope"] = json!({"type":"string","enum":["all","long-term"]});
        }
        if matches!(self, Self::Search | Self::List) {
            properties["filters"] =
                json!({"type":"object","description":"Flat metadata fields and comparison operators."});
            properties["includeExpired"] = json!({"type":"boolean","default":false});
            properties["since"] =
                json!({"type":"string","description":"Inclusive record time (UTC date or timestamp with an offset)."});
            properties["until"] =
                json!({"type":"string","description":"Exclusive record time (UTC date or timestamp with an offset)."});
            properties["timeField"] =
                json!({"type":"string","enum":["created_at","updated_at"],"default":"created_at"});
        }
        json!({"type":"object","properties":properties,"required":required,"additionalProperties":false})
    }

    fn argument_issue(self, input: &Value) -> Option<Value> {
        let Some(args) = input.as_object() else {
            return Some(json!({"path":[],"reason":"invalid_type"}));
        };
        let schema = self.schema();
        let properties = schema["properties"].as_object().expect("tool schema properties");
        if let Some(unknown) = args.keys().find(|key| !properties.contains_key(*key)) {
            let mut issue = json!({"path":[],"reason":"unknown_field"});
            if unknown.len() <= 32
                && unknown.as_bytes().first().is_some_and(u8::is_ascii_alphabetic)
                && unknown.bytes().all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
            {
                let mut matches = properties.keys().filter(|field| one_edit_apart(unknown, field));
                if let (Some(suggestion), None) = (matches.next(), matches.next()) {
                    issue["suggestion"] = json!(suggestion);
                }
            }
            return Some(issue);
        }
        if let Some(scope) = args.get("scope")
            && !matches!(scope.as_str(), Some("all" | "long-term"))
        {
            return Some(field_issue("scope", "invalid_value"));
        }
        match self {
            Self::Add => args
                .get("longTerm")
                .filter(|value| value.as_bool() != Some(true))
                .map(|_| field_issue("longTerm", "invalid_value")),
            Self::Update => match args.get("memoryId") {
                None => Some(field_issue("memoryId", "missing")),
                Some(id) => memory_id_issue(id).or_else(|| {
                    (args.len() == 1).then(|| {
                        json!({"path":[],"reason":"empty_patch","fields":["text","category","importance","metadata"]})
                    })
                }),
            },
            Self::Delete => match (args.get("query"), args.get("memoryId")) {
                (Some(query), None) => match query.as_str() {
                    Some(text) if !text.trim().is_empty() => None,
                    Some(_) => Some(field_issue("query", "invalid_value")),
                    None => Some(field_issue("query", "invalid_type")),
                },
                (None, Some(id)) => ["categories", "scope"]
                    .into_iter()
                    .find(|field| args.contains_key(*field))
                    .map(|field| field_issue(field, "preview_only"))
                    .or_else(|| memory_id_issue(id)),
                (query, _) => {
                    let reason = if query.is_some() {
                        "conflicting_fields"
                    } else {
                        "missing_choice"
                    };
                    Some(json!({"path":[],"reason":reason,"fields":["memoryId","query"]}))
                }
            },
            _ => None,
        }
    }
}

fn field_issue(field: &str, reason: &str) -> Value {
    json!({"path":[field],"reason":reason})
}

fn memory_id_issue(id: &Value) -> Option<Value> {
    match id.as_str() {
        Some(id) if valid_id(id) => None,
        Some(_) => Some(field_issue("memoryId", "invalid_value")),
        None => Some(field_issue("memoryId", "invalid_type")),
    }
}

fn one_edit_apart(name: &str, field: &str) -> bool {
    let (short, long) = if name.len() <= field.len() {
        (name.as_bytes(), field.as_bytes())
    } else {
        (field.as_bytes(), name.as_bytes())
    };
    if long.len() - short.len() > 1 {
        return false;
    }
    let first = short.iter().zip(long).position(|(a, b)| a != b).unwrap_or(short.len());
    if short.len() != long.len() {
        return short[first..] == long[first + 1..];
    }
    first < short.len()
        && (short[first + 1..] == long[first + 1..]
            || (first + 1 < short.len()
                && short[first] == long[first + 1]
                && short[first + 1] == long[first]
                && short[first + 2..] == long[first + 2..]))
}

fn valid_id(id: &str) -> bool {
    !id.is_empty()
        && id.len() <= 128
        && id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-'))
}

struct Mem0Client {
    binding: Mem0Binding,
    manager: OnceCell<McpManager>,
}

impl Mem0Client {
    async fn request(&self, action: Action, input: Value, write: bool) -> Result<Value, ToolResult> {
        let manager = self.manager.get_or_try_init(|| self.connect()).await?;
        let reply = manager
            .call_tool_structured(SERVER, action.name(), input)
            .await
            .map_err(|_| {
                if write {
                    failure("write_unconfirmed", "Check the current memory before retrying.")
                } else {
                    failure("memory_unavailable", "The assistant's memory request failed.")
                }
            })?;
        let result = reply.structured_content.or_else(|| {
            reply.content.into_iter().find_map(|block| match block {
                McpContent::Text { text } => serde_json::from_str::<Value>(&text).ok(),
                _ => None,
            })
        });
        let Some(result) = result.filter(Value::is_object) else {
            return Err(if write {
                failure(
                    "write_unconfirmed",
                    "The memory service returned an invalid write result. Verify the current state.",
                )
            } else {
                failure(
                    "invalid_memory_response",
                    "The memory service returned an invalid result.",
                )
            });
        };
        if reply.is_error {
            return Err(error_result(result));
        }
        Ok(result)
    }

    async fn connect(&self) -> Result<McpManager, ToolResult> {
        let mut headers = HashMap::from([("Authorization".into(), format!("Bearer {}", self.binding.bearer()))]);
        if let Some((user_id, agent_id)) = self.binding.namespace() {
            headers.insert("X-Mem0-User-Id".into(), user_id.into());
            headers.insert("X-Mem0-Agent-Id".into(), agent_id.into());
        }
        let config = McpServerConfig {
            transport: TransportType::StreamableHttp,
            url: Some(self.binding.url().to_string()),
            headers: Some(headers),
            command: None,
            args: None,
            env: None,
            deferred: None,
            startup_timeout_ms: Some(6_000),
        };
        let manager = McpManager::connect_pinned_http_direct(SERVER, &config)
            .await
            .map_err(|error| match error {
                McpError::HttpStatus { status: 400, .. } => failure(
                    "memory_handshake_rejected",
                    "The memory service rejected the MCP handshake (HTTP 400). Check the MCP response diagnostics.",
                ),
                McpError::HttpStatus { status: 401, .. } => failure(
                    "memory_auth_failed",
                    "The memory service rejected authentication (HTTP 401). Check the configured credential.",
                ),
                McpError::HttpStatus { status: 403, .. } => failure(
                    "memory_access_denied",
                    "The memory service denied access (HTTP 403). Check the MCP access configuration.",
                ),
                _ => failure("memory_unavailable", "The assistant's memory service is unavailable."),
            })?;
        let tools = manager.all_tools();
        if !Action::ALL.iter().all(|action| {
            tools
                .iter()
                .any(|(_, tool)| tool.name == action.name() && tool.input_schema["type"] == "object")
        }) {
            manager.shutdown().await;
            return Err(failure(
                "memory_unavailable",
                "The assistant's memory service is unavailable.",
            ));
        }
        Ok(manager)
    }

    async fn get_record(&self, id: &str) -> Result<Value, ToolResult> {
        let result = self.request(Action::Get, json!({"memoryId": id}), false).await?;
        let record = &result["memory"];
        if record["id"].as_str() != Some(id) || !record["memory"].is_string() {
            return Err(failure(
                "invalid_memory_response",
                "The memory service did not return the requested record.",
            ));
        }
        Ok(record.clone())
    }
}

struct Mem0Tool {
    action: Action,
    client: Arc<Mem0Client>,
    approvals: Arc<Mem0DeleteApprovals>,
}

pub(super) fn tools(binding: Mem0Binding, approvals: Arc<Mem0DeleteApprovals>) -> impl Iterator<Item = Box<dyn Tool>> {
    let client = Arc::new(Mem0Client {
        binding,
        manager: OnceCell::new(),
    });
    Action::ALL.into_iter().map(move |action| {
        Box::new(Mem0Tool {
            action,
            client: client.clone(),
            approvals: approvals.clone(),
        }) as Box<dyn Tool>
    })
}

fn failure(code: &str, message: &str) -> ToolResult {
    error_result(json!({"code": code, "message": message}))
}

fn error_result(data: Value) -> ToolResult {
    ToolResult {
        content: data.to_string(),
        is_error: true,
    }
}

impl Mem0Tool {
    async fn run(&self, mut input: Value) -> Result<Value, ToolResult> {
        if let Some(issue) = self.action.argument_issue(&input) {
            return Err(error_result(json!({
                "code":"invalid_memory_arguments",
                "message":"Use valid arguments within the assistant's long-term memory scope.",
                "issues":[issue]
            })));
        }
        if !self.client.binding.is_entitled().await {
            return Err(failure(
                "memory_disabled",
                "Long-term memory is no longer enabled for this assistant.",
            ));
        }
        if self.action == Action::Delete
            && let Some(id) = input["memoryId"].as_str()
        {
            let before = self.client.get_record(id).await?;
            let text = before["memory"].as_str().unwrap_or_default();
            if !self.approvals.request(id, text).await {
                return Err(failure("memory_deletion_not_approved", "The memory was kept."));
            }
            let current = self.client.get_record(id).await;
            if !self.client.binding.is_entitled().await {
                return Err(failure(
                    "memory_disabled",
                    "Long-term memory is no longer enabled for this assistant.",
                ));
            }
            if current? != before {
                return Err(failure(
                    "memory_changed",
                    "The memory changed. Review it before proposing deletion again.",
                ));
            }
            return self.client.request(Action::Delete, json!({"memoryId":id}), true).await;
        }
        if self.action == Action::Delete {
            // A role credential may also have a session binding.
            input["scope"] = json!("long-term");
        }
        self.client
            .request(self.action, input, matches!(self.action, Action::Add | Action::Update))
            .await
    }
}

#[async_trait::async_trait]
impl Tool for Mem0Tool {
    fn name(&self) -> &str {
        self.action.name()
    }
    fn description(&self) -> &str {
        self.action.description()
    }
    fn input_schema(&self) -> JsonSchema {
        self.action.schema()
    }
    fn category(&self) -> ToolCategory {
        ToolCategory::Mcp
    }
    fn is_concurrency_safe(&self, _input: &Value) -> bool {
        false
    }
    fn uses_host_approval(&self, input: &Value) -> bool {
        self.action == Action::Delete && input.get("query").is_none()
    }
    fn describe(&self, _input: &Value) -> String {
        self.description().to_owned()
    }

    async fn execute(&self, input: Value) -> ToolResult {
        match self.run(input).await {
            Ok(result) => ToolResult {
                content: result.to_string(),
                is_error: false,
            },
            Err(error) => error,
        }
    }
}

#[cfg(test)]
#[path = "mem0_test.rs"]
mod tests;
