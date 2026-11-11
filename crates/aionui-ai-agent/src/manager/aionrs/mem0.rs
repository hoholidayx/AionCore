use std::collections::HashMap;
use std::sync::Arc;

use aion_config::config::{McpServerConfig, TransportType};
use aion_mcp::manager::McpManager;
use aion_mcp::protocol::McpContent;
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
                "Search this assistant's long-term memories by meaning, optionally filtered by categories, flat metadata, or record time. Set includeExpired to view expired memories."
            }
            Self::List => {
                "List this assistant's long-term memories, optionally filtered by record time. Set includeExpired to view expired memories."
            }
            Self::Get => "Read a long-term memory by its exact ID.",
            Self::Add => {
                "Save durable facts. Follow the Memory Triage Skill, search first, and pass either facts or text. Optionally set expirationDate (YYYY-MM-DD, UTC). Omit secrets."
            }
            Self::Update => {
                "Update a long-term memory in place by ID. Provide text, category, importance, or metadata. Omit secrets."
            }
            Self::Delete => {
                "Preview candidates with query, or propose deleting one memory with memoryId. Wait for the user's confirmation and the tool result."
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
                    "categories":{"type":"array","items":{"type":"string"}},
                    "filters":{"type":"object","description":"Flat metadata fields and comparison operators."}
                }),
                vec!["query"],
            ),
            Self::List => (json!({}), vec![]),
            Self::Get => (json!({"memoryId":id()}), vec!["memoryId"]),
            Self::Add => (
                json!({
                    "facts":{"type":"array","items":text(),"minItems":1,"maxItems":25},
                    "text":text(), "category":{"type":"string"}, "importance":{"type":"number","minimum":0,"maximum":1},
                    "metadata":{"type":"object"}, "longTerm":{"type":"boolean","enum":[true]},
                    "expirationDate":{"type":"string","pattern":"^[0-9]{4}-[0-9]{2}-[0-9]{2}$","description":"Last visible date in UTC (YYYY-MM-DD)."}
                }),
                vec![],
            ),
            Self::Update => (
                json!({
                    "memoryId":id(), "text":text(), "category":{"type":"string","minLength":1,"maxLength":80},
                    "importance":{"type":"number","minimum":0,"maximum":1},
                    "metadata":{"type":"object","maxProperties":40,"description":"Top-level metadata keys to merge."}
                }),
                vec!["memoryId"],
            ),
            Self::Delete => (json!({"memoryId":id(),"query":text()}), vec![]),
        };
        if matches!(self, Self::Search | Self::List) {
            properties["scope"] = json!({"type":"string","enum":["all","long-term"]});
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

    fn accepts(self, input: &Value) -> bool {
        let Some(args) = input.as_object() else { return false };
        let schema = self.schema();
        if args.keys().any(|key| schema["properties"].get(key).is_none()) {
            return false;
        }
        if let Some(scope) = args.get("scope")
            && !matches!(scope.as_str(), Some("all" | "long-term"))
        {
            return false;
        }
        match self {
            Self::Add => args.get("longTerm").is_none_or(|value| value.as_bool() == Some(true)),
            Self::Update => args.len() > 1 && args.get("memoryId").and_then(Value::as_str).is_some_and(valid_id),
            Self::Delete => match (args.get("query"), args.get("memoryId")) {
                (Some(query), None) => query.as_str().is_some_and(|text| !text.trim().is_empty()),
                (None, Some(id)) => id.as_str().is_some_and(valid_id),
                _ => false,
            },
            _ => true,
        }
    }
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
        let manager = self
            .manager
            .get_or_try_init(|| self.connect())
            .await
            .map_err(|()| failure("memory_unavailable", "The assistant's memory service is unavailable."))?;
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
            return Err(ToolResult {
                content: result.to_string(),
                is_error: true,
            });
        }
        Ok(result)
    }

    async fn connect(&self) -> Result<McpManager, ()> {
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
            .map_err(|_| ())?;
        let tools = manager.all_tools();
        if !Action::ALL.iter().all(|action| {
            tools
                .iter()
                .any(|(_, tool)| tool.name == action.name() && tool.input_schema["type"] == "object")
        }) {
            manager.shutdown().await;
            return Err(());
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
    ToolResult {
        content: json!({"code": code, "message": message}).to_string(),
        is_error: true,
    }
}

impl Mem0Tool {
    async fn run(&self, input: Value) -> Result<Value, ToolResult> {
        if !self.action.accepts(&input) {
            return Err(failure(
                "invalid_memory_arguments",
                "Use valid arguments within the assistant's long-term memory scope.",
            ));
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
