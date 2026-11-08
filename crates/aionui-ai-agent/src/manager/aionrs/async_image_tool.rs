use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use aion_protocol::events::ToolCategory;
use aion_tools::{Tool, ToolExecutionOutput};
use aion_types::message::ContentBlock;
use aion_types::tool::{JsonSchema, ToolResult};
use serde::Deserialize;
use serde_json::{Value, json};

use crate::async_image::{
    AsyncImageEndpoint, AsyncImageError, AsyncImageQueue, AsyncImageReference, AsyncImageRequest, TOOL_NAME,
};

#[derive(Clone)]
pub(crate) struct AsyncImageTool {
    queue: Arc<dyn AsyncImageQueue>,
    user_id: String,
    conversation_id: String,
    endpoints: Vec<AsyncImageEndpoint>,
    turn: Arc<Mutex<Option<AttachmentTurn>>>,
}

struct AttachmentTurn {
    id: String,
    files: Vec<String>,
}

pub(super) struct AttachmentTurnGuard(Arc<Mutex<Option<AttachmentTurn>>>);

impl Drop for AttachmentTurnGuard {
    fn drop(&mut self) {
        if let Ok(mut turn) = self.0.lock() {
            *turn = None;
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct GenerateArgs {
    prompt: String,
    size: Option<String>,
    preset: Option<String>,
    backend: Option<String>,
    #[serde(default)]
    references: Vec<String>,
}

impl AsyncImageTool {
    pub(crate) fn new(
        queue: Arc<dyn AsyncImageQueue>,
        user_id: String,
        conversation_id: String,
        endpoints: Vec<AsyncImageEndpoint>,
    ) -> Self {
        Self {
            queue,
            user_id,
            conversation_id,
            endpoints,
            turn: Arc::new(Mutex::new(None)),
        }
    }

    pub(super) fn begin_turn(&self, id: &str, files: &[String], blocks: &mut Vec<ContentBlock>) -> AttachmentTurnGuard {
        if let Ok(mut turn) = self.turn.lock() {
            *turn = Some(AttachmentTurn {
                id: id.to_owned(),
                files: files.to_vec(),
            });
        }
        if !files.is_empty() {
            let labels = files
                .iter()
                .enumerate()
                .map(|(index, file)| {
                    let name = std::path::Path::new(file)
                        .file_name()
                        .unwrap_or_default()
                        .to_string_lossy();
                    format!("attachment:{id}:{} — {name}", index + 1)
                })
                .collect::<Vec<_>>()
                .join("\n");
            blocks.push(ContentBlock::Text {
                text: format!("[Image generation attachment references]\n{labels}"),
            });
        }
        AttachmentTurnGuard(Arc::clone(&self.turn))
    }

    fn request(&self, input: Value, call_id: &str) -> Result<AsyncImageRequest, AsyncImageError> {
        let args: GenerateArgs = serde_json::from_value(input)
            .map_err(|_| AsyncImageError::new("invalid_request", "Invalid image generation arguments"))?;
        let invalid_ref = || {
            AsyncImageError::new(
                "attachment_unavailable",
                "Image reference is unavailable in this conversation",
            )
        };
        if args.references.len() > 10 {
            return Err(AsyncImageError::new(
                "invalid_request",
                "Use at most ten reference images",
            ));
        }
        let turn = self.turn.lock().map_err(|_| invalid_ref())?;
        let turn = turn.as_ref().ok_or_else(invalid_ref)?;
        let references = args
            .references
            .iter()
            .map(|reference| {
                if let Some(id) = reference.strip_prefix("async-image:") {
                    return Ok(AsyncImageReference::Generated(id.to_owned()));
                }
                let index = reference
                    .strip_prefix("attachment:")
                    .and_then(|value| value.split_once(':'))
                    .filter(|(id, _)| *id == turn.id)
                    .and_then(|(_, index)| index.parse::<usize>().ok())
                    .and_then(|index| index.checked_sub(1))
                    .ok_or_else(invalid_ref)?;
                let path = turn.files.get(index).ok_or_else(invalid_ref)?;
                Ok(AsyncImageReference::AuthorizedPath(PathBuf::from(path)))
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(AsyncImageRequest {
            user_id: self.user_id.clone(),
            conversation_id: self.conversation_id.clone(),
            turn_id: turn.id.clone(),
            tool_call_id: call_id.to_owned(),
            prompt: args.prompt,
            requested_server_id: args.backend,
            size: args.size,
            preset: args.preset,
            available: self.endpoints.clone(),
            references,
        })
    }
}

#[async_trait::async_trait]
impl Tool for AsyncImageTool {
    fn name(&self) -> &str {
        TOOL_NAME
    }

    fn description(&self) -> &str {
        "Generate or edit one image asynchronously. Use attachment references in their intended order. The image is delivered to the original conversation in the background. The receipt contains a reference for a later edit."
    }

    fn input_schema(&self) -> JsonSchema {
        let endpoints = self
            .endpoints
            .iter()
            .map(|entry| format!("{}: {}", entry.id, entry.name))
            .collect::<Vec<_>>();
        json!({
            "type": "object",
            "required": ["prompt"],
            "additionalProperties": false,
            "properties": {
                "prompt": {"type": "string"},
                "size": {"type": "string", "description": "Optional output size or aspect ratio."},
                "preset": {"type": "string", "enum": ["fast", "quality"], "description": "Optional speed or quality preference."},
                "references": {"type": "array", "items": {"type": "string"}, "maxItems": 10,
                               "description": "Ordered attachment references from this turn or a previous image receipt."},
                "backend": {"type": "string", "description": format!("Optional image endpoint ID. Available endpoints: {}", endpoints.join(", "))}
            }
        })
    }

    fn is_concurrency_safe(&self, _input: &Value) -> bool {
        false
    }

    fn category(&self) -> ToolCategory {
        ToolCategory::Mcp
    }

    async fn execute(&self, _input: Value) -> ToolResult {
        error_result(AsyncImageError::new(
            "invalid_request",
            "Image generation requires a conversation tool call",
        ))
    }

    async fn execute_with_call_id(&self, input: Value, call_id: &str) -> ToolExecutionOutput {
        let result = match self.request(input, call_id) {
            Ok(request) => self.queue.enqueue(request).await,
            Err(error) => Err(error),
        };
        match result {
            Ok(receipt) => ToolResult {
                content: json!(receipt).to_string(),
                is_error: false,
            },
            Err(error) => error_result(error),
        }
        .into()
    }
}

fn error_result(error: AsyncImageError) -> ToolResult {
    ToolResult {
        content: json!({ "error_code": error.code, "message": error.message }).to_string(),
        is_error: true,
    }
}
