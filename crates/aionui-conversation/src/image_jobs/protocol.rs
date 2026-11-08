use std::collections::{BTreeMap, HashMap};
use std::time::Duration;

use aion_config::config::{McpServerConfig, TransportType};
use aion_mcp::manager::McpManager;
use aion_mcp::protocol::{McpContent, McpToolResult};
use aionui_api_types::{ASYNC_IMAGE_TOOLS, async_image_schema_matches};
use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use serde::Deserialize;
use serde_json::Value;
use sha2::{Digest, Sha256};

pub(super) const CONTRACT: &str = "image_jobs_v1";
pub(super) const MAX_REFERENCE: usize = 20 * 1024 * 1024;
pub(super) const MAX_TOTAL: usize = 32 * 1024 * 1024;
pub(super) const MAX_REFERENCES: usize = 10;
pub(super) const MAX_REQUEST: usize = 48 * 1024 * 1024;
pub(super) const MAX_RESULT: usize = 32 * 1024 * 1024;
const SERVER: &str = "async_image";

#[derive(Deserialize)]
pub(super) struct EndpointConfig {
    pub url: String,
    #[serde(default)]
    pub headers: HashMap<String, String>,
}

pub(super) struct ImageJobClient(McpManager);

#[derive(Debug)]
pub(super) enum BackendError {
    Transient,
    TaskNotFound,
    ResultNotReady,
    InvalidContract,
    InvalidResult,
    Rejected(&'static str),
}

impl ImageJobClient {
    pub async fn connect(endpoint: EndpointConfig) -> Result<Self, BackendError> {
        let config = McpServerConfig {
            transport: TransportType::StreamableHttp,
            command: None,
            args: None,
            env: None,
            url: Some(endpoint.url),
            headers: Some(endpoint.headers),
            deferred: None,
            startup_timeout_ms: Some(Duration::from_secs(6).as_millis() as u64),
        };
        let manager = McpManager::connect_all(&HashMap::from([(SERVER.to_owned(), config)]))
            .await
            .map_err(|_| BackendError::Transient)?;
        let tools = manager.all_tools();
        if manager.server_names().is_empty() {
            return Err(BackendError::Transient);
        }
        for (name, _) in ASYNC_IMAGE_TOOLS {
            let compatible = tools
                .iter()
                .any(|(_, tool)| tool.name == name && async_image_schema_matches(name, &tool.input_schema));
            if !compatible {
                return Err(BackendError::InvalidContract);
            }
        }
        Ok(Self(manager))
    }

    pub async fn submit(&self, args: Value, task_id: &str) -> Result<RemoteState, BackendError> {
        let tools = self.0.all_tools();
        let schema = tools
            .iter()
            .find(|(_, tool)| tool.name == "image_submit")
            .and_then(|(_, tool)| tool.input_schema.get("properties"))
            .ok_or(BackendError::InvalidContract)?;
        if args
            .as_object()
            .is_none_or(|args| args.keys().any(|key| schema.get(key).is_none()))
        {
            return Err(BackendError::Rejected("unsupported_input"));
        }
        let result = self.call("image_submit", args).await?;
        read_state(result, task_id)
    }

    pub async fn status(&self, task_id: &str) -> Result<RemoteState, BackendError> {
        let result = self
            .call("image_status", serde_json::json!({ "task_id": task_id }))
            .await?;
        read_state(result, task_id)
    }

    pub async fn result(&self, task_id: &str) -> Result<Vec<u8>, BackendError> {
        let result = self
            .call("image_result", serde_json::json!({ "task_id": task_id }))
            .await?;
        verified_image(result, task_id)
    }

    async fn call(&self, name: &str, args: Value) -> Result<McpToolResult, BackendError> {
        let result = self
            .0
            .call_tool_structured(SERVER, name, args)
            .await
            .map_err(|_| BackendError::Transient)?;
        if result.is_error {
            let code = result
                .structured_content
                .as_ref()
                .and_then(|value| value.get("error_code"))
                .and_then(Value::as_str);
            return Err(match code {
                Some("task_not_found") => BackendError::TaskNotFound,
                Some("result_not_ready") => BackendError::ResultNotReady,
                Some("result_expired") => BackendError::Rejected("result_expired"),
                Some("request_conflict") => BackendError::Rejected("request_conflict"),
                Some("invalid_request") => BackendError::Rejected("invalid_request"),
                Some("queue_full") => BackendError::Rejected("queue_full"),
                Some("backend_unavailable") => BackendError::Rejected("backend_unavailable"),
                Some("worker_unavailable") => BackendError::Transient,
                _ => BackendError::Rejected("backend_rejected"),
            });
        }
        Ok(result)
    }
}

#[derive(PartialEq, Eq, Debug)]
pub(super) enum RemoteState {
    Active,
    Uncertain,
    Completed,
    Failed(&'static str),
}

fn read_state(result: McpToolResult, expected_id: &str) -> Result<RemoteState, BackendError> {
    let fields = result.structured_content.ok_or(BackendError::InvalidResult)?;
    if fields.get("task_id").and_then(Value::as_str) != Some(expected_id) {
        return Err(BackendError::InvalidResult);
    }
    match fields.get("state").and_then(Value::as_str) {
        Some("pending" | "submitting" | "queued" | "running") => Ok(RemoteState::Active),
        Some("submission_uncertain") => Ok(RemoteState::Uncertain),
        Some("completed") => Ok(RemoteState::Completed),
        Some("expired") => Ok(RemoteState::Failed("result_expired")),
        Some("failed") => Ok(RemoteState::Failed(
            match fields.get("error_code").and_then(Value::as_str) {
                Some("backend_unavailable") => "backend_unavailable",
                Some("output_unavailable") => "output_unavailable",
                Some("output_invalid") => "output_invalid",
                Some("interrupted_by_restart") => "interrupted_by_restart",
                _ => "generation_failed",
            },
        )),
        _ => Err(BackendError::InvalidResult),
    }
}

fn verified_image(result: McpToolResult, expected_id: &str) -> Result<Vec<u8>, BackendError> {
    let fields = result.structured_content.ok_or(BackendError::InvalidResult)?;
    let mut images = result.content.iter().filter_map(|block| match block {
        McpContent::Image { data, mime_type } => Some((data, mime_type)),
        _ => None,
    });
    let (encoded, mime_type) = images.next().ok_or(BackendError::InvalidResult)?;
    if images.next().is_some()
        || mime_type != "image/png"
        || encoded.len() > MAX_RESULT.div_ceil(3) * 4
        || fields.get("task_id").and_then(Value::as_str) != Some(expected_id)
    {
        return Err(BackendError::InvalidResult);
    }
    let png = STANDARD.decode(encoded).map_err(|_| BackendError::InvalidResult)?;
    let (width, height) = png_dimensions(&png).ok_or(BackendError::InvalidResult)?;
    let sha256 = fingerprint(&png);
    if png.len() > MAX_RESULT
        || fields.get("sha256").and_then(Value::as_str) != Some(sha256.as_str())
        || fields.get("width").and_then(Value::as_u64) != Some(width.into())
        || fields.get("height").and_then(Value::as_u64) != Some(height.into())
    {
        return Err(BackendError::InvalidResult);
    }
    Ok(png)
}

pub(super) fn fingerprint(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

pub(super) fn endpoint_fingerprint(url: &str, headers: &HashMap<String, String>) -> String {
    let canonical: BTreeMap<_, _> = headers.iter().collect();
    fingerprint(serde_json::json!([url, canonical]).to_string().as_bytes())
}

pub(super) fn png_dimensions(png: &[u8]) -> Option<(u32, u32)> {
    const HEADER: &[u8] = b"\x89PNG\r\n\x1a\n\x00\x00\x00\x0dIHDR";
    if !png.starts_with(HEADER) {
        return None;
    }
    let width = u32::from_be_bytes(png.get(16..20)?.try_into().ok()?);
    let height = u32::from_be_bytes(png.get(20..24)?.try_into().ok()?);
    if width == 0 || width > 8192 || height == 0 || height > 8192 || width * height > 32_000_000 {
        return None;
    }
    let mut chunks = png.get(8..)?;
    let mut has_data = false;
    while !chunks.is_empty() {
        let length = u32::from_be_bytes(chunks.get(..4)?.try_into().ok()?) as usize;
        let end = 8usize.checked_add(length)?;
        let crc_end = end.checked_add(4)?;
        let crc = u32::from_be_bytes(chunks.get(end..crc_end)?.try_into().ok()?);
        if crc32fast::hash(chunks.get(4..end)?) != crc {
            return None;
        }
        match chunks.get(4..8)? {
            b"IDAT" => has_data |= length > 0,
            b"IEND" => return (has_data && length == 0 && chunks.len() == crc_end).then_some((width, height)),
            b"acTL" | b"fdAT" | b"fcTL" => return None,
            _ => (),
        }
        chunks = chunks.get(crc_end..)?;
    }
    None
}
