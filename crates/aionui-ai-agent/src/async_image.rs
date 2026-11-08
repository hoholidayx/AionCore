//! Host port for background image generation. The agent does not own task storage.

use std::collections::HashMap;
use std::path::PathBuf;

use async_trait::async_trait;
use serde::Serialize;

pub(crate) const TOOL_NAME: &str = "image_generate_async";
pub const MAX_IMAGE_REFERENCES: usize = 10;
pub const IMAGE_REFERENCE_PREFIX: &str = "async-image:";

#[derive(Clone)]
pub struct AsyncImageEndpoint {
    pub id: String,
    pub name: String,
    pub url: String,
    pub headers: HashMap<String, String>,
    pub is_default: bool,
}

pub enum AsyncImageReference {
    AuthorizedPath(PathBuf),
    Generated(String),
}

pub struct AsyncImageRequest {
    pub user_id: String,
    pub conversation_id: String,
    pub turn_id: String,
    pub tool_call_id: String,
    pub prompt: String,
    pub requested_server_id: Option<String>,
    pub size: Option<String>,
    pub preset: Option<String>,
    pub available: Vec<AsyncImageEndpoint>,
    pub references: Vec<AsyncImageReference>,
}

#[derive(Serialize)]
pub struct AsyncImageReceipt {
    pub task_id: String,
    pub attachment_id: String,
    pub state: String,
}

#[derive(Debug, thiserror::Error)]
#[error("{message}")]
pub struct AsyncImageError {
    pub code: &'static str,
    pub message: String,
}

impl AsyncImageError {
    pub fn new(code: &'static str, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }
}

#[async_trait]
pub trait AsyncImageQueue: Send + Sync {
    async fn enqueue(&self, request: AsyncImageRequest) -> Result<AsyncImageReceipt, AsyncImageError>;
}
