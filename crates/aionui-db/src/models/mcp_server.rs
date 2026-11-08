use aionui_common::TimestampMs;
use serde::{Deserialize, Serialize};

/// Row mapping for the `mcp_servers` table.
///
/// Enum-like fields (`transport_type`, `status`) are stored as TEXT.
/// The service layer converts them to/from domain enums.
///
/// JSON fields (`transport_config`, `tools`) are stored as TEXT in SQLite
/// and deserialized by the service layer.
#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct McpServerRow {
    pub id: String,
    pub user_id: String,
    /// Unique server name (used as identifier when syncing to Agent CLIs).
    pub name: String,
    pub backend_alias: Option<String>,
    pub description: Option<String>,
    /// Whether this server is synced to Agent CLIs.
    pub enabled: bool,
    /// One of: "stdio", "sse", "http".
    pub transport_type: String,
    /// JSON object: command/args/env (stdio) or url/headers (sse/http).
    pub transport_config: String,
    /// JSON array of tool descriptions (populated after connection test).
    pub tools: Option<String>,
    /// One of: "connected", "disconnected", "error", "testing".
    /// Represents the latest test result, not a live runtime state.
    pub last_test_status: String,
    pub last_connected: Option<TimestampMs>,
    /// Original JSON text for editing restoration.
    pub original_json: Option<String>,
    /// Whether this is a built-in server (hidden from edit/delete in UI).
    pub builtin: bool,
    pub purpose: String,
    pub is_default: bool,
    /// Soft-delete timestamp. `NULL` means active.
    pub deleted_at: Option<TimestampMs>,
    pub created_at: TimestampMs,
    pub updated_at: TimestampMs,
}

impl McpServerRow {
    pub fn image_backend_selector(&self) -> &str {
        self.backend_alias.as_deref().unwrap_or(&self.name)
    }

    pub fn is_enabled_image_backend(&self) -> bool {
        self.enabled
            && !self.builtin
            && self.deleted_at.is_none()
            && self.purpose == "async_image"
            && matches!(self.transport_type.as_str(), "http" | "streamable_http")
    }
}
