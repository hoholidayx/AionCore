use std::path::{Path, PathBuf};

use aionui_api_types::DotsMessageAttachment;
use serde::{Deserialize, Serialize};
use tokio::io::AsyncReadExt as _;

use super::{
    DotsService,
    client::{BridgeError, FILE_LIMIT},
};
use crate::ConversationError;

#[derive(Deserialize, Serialize)]
pub(super) struct StagedFile {
    pub name: String,
    path: PathBuf,
}

pub struct DotsDownload {
    pub bytes: Vec<u8>,
    pub content_type: &'static str,
}

pub(super) fn outbox(workspace: &Path, id: &str) -> PathBuf {
    workspace.join(".dots").join("outbox").join(id)
}

async fn directory(root: &Path, relative: &str) -> Result<PathBuf, std::io::Error> {
    let root = tokio::fs::canonicalize(root).await?;
    let private = root.join(".dots");
    let path = private.join(relative);
    // A concurrent conversation deletion may remove the workspace. Creating
    // one level at a time never recreates the workspace after its deletion.
    for directory in [&private, &path] {
        match tokio::fs::create_dir(directory).await {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(error) => return Err(error),
        }
        if tokio::fs::canonicalize(directory).await? != *directory {
            return Err(std::io::Error::other("Unexpected Dots storage directory"));
        }
    }
    Ok(path)
}

pub(super) async fn stage(workspace: &Path, id: &str, paths: &[String]) -> Result<Vec<StagedFile>, ConversationError> {
    if paths.is_empty() {
        return Ok(Vec::new());
    }
    let parent = directory(workspace, "outbox")
        .await
        .map_err(|_| ConversationError::internal("Dots attachment directory unavailable"))?;
    let dir = parent.join(id);
    tokio::fs::create_dir(&dir)
        .await
        .map_err(|_| ConversationError::internal("Dots staging directory unavailable"))?;
    let result = stage_files(&dir, paths).await;
    if result.is_err() {
        let _ = tokio::fs::remove_dir_all(&dir).await;
    }
    result
}

async fn stage_files(dir: &Path, paths: &[String]) -> Result<Vec<StagedFile>, ConversationError> {
    let invalid = || ConversationError::BadRequest {
        reason: "Dots attachments must be readable regular files of at most 20 MiB".into(),
    };
    let mut staged = Vec::with_capacity(paths.len());
    for (index, source) in paths.iter().enumerate() {
        let source = Path::new(source);
        let name = source
            .file_name()
            .and_then(|s| s.to_str())
            .filter(|s| {
                !s.trim().is_empty()
                    && s.chars().count() <= 255
                    && !s.chars().any(|ch| ch.is_ascii_control() || matches!(ch, '/' | '\\'))
            })
            .ok_or_else(invalid)?
            .to_owned();
        if !tokio::fs::metadata(source).await.map_err(|_| invalid())?.is_file() {
            return Err(invalid());
        }
        let path = dir.join(index.to_string());
        let bytes = read_limited(source).await.map_err(|_| invalid())?;
        tokio::fs::write(&path, bytes)
            .await
            .map_err(|_| ConversationError::internal("Failed to stage Dots attachment"))?;
        staged.push(StagedFile { name, path });
    }
    Ok(staged)
}

async fn read_limited(path: &Path) -> Result<Vec<u8>, std::io::Error> {
    let file = tokio::fs::File::open(path).await?;
    let metadata = file.metadata().await?;
    if !metadata.is_file() || metadata.len() > FILE_LIMIT as u64 {
        return Err(std::io::Error::other("Invalid Dots attachment size"));
    }
    let mut bytes = Vec::with_capacity(metadata.len() as usize);
    file.take((FILE_LIMIT + 1) as u64).read_to_end(&mut bytes).await?;
    if bytes.len() > FILE_LIMIT {
        return Err(std::io::Error::other("Invalid Dots attachment size"));
    }
    Ok(bytes)
}

pub(super) async fn read_staged(file: &StagedFile) -> Result<Vec<u8>, BridgeError> {
    read_limited(&file.path).await.map_err(|_| BridgeError::Protocol)
}

impl DotsService {
    pub(super) async fn describe_attachment(&self, id: &str) -> DotsMessageAttachment {
        let mut file = DotsMessageAttachment {
            artifact_id: id.into(),
            filename: id.into(),
            preview: false,
        };
        if let Ok(grant) = self.bridge.download_grant(id).await {
            file.filename = grant.filename;
            file.preview = matches!(
                grant.content_type.as_str(),
                "image/png" | "image/jpeg" | "image/gif" | "image/webp"
            );
        }
        file
    }

    pub(super) async fn read_cached(&self, workspace: &Path, id: &str) -> Result<DotsDownload, BridgeError> {
        let dir = directory(workspace, "inbox").await.map_err(|_| BridgeError::Protocol)?;
        let path = dir.join(id);
        let bytes = match read_limited(&path).await {
            Ok(bytes) => bytes,
            Err(_) => {
                let grant = self.bridge.download_grant(id).await?;
                let bytes = match self.bridge.download(&grant).await {
                    Err(BridgeError::Http(reqwest::StatusCode::FORBIDDEN)) => {
                        let grant = self.bridge.download_grant(id).await?;
                        self.bridge.download(&grant).await?
                    }
                    result => result?,
                };
                let temp = dir.join(format!(".{}.part", uuid::Uuid::new_v4()));
                if tokio::fs::write(&temp, &bytes).await.is_ok() {
                    if tokio::fs::rename(&temp, &path).await.is_err() {
                        let _ = tokio::fs::remove_file(&temp).await;
                    }
                } else {
                    let _ = tokio::fs::remove_file(&temp).await;
                }
                bytes
            }
        };
        Ok(DotsDownload {
            content_type: image_type(&bytes),
            bytes,
        })
    }

    pub(crate) async fn fetch_attachment(&self, workspace: &Path, id: &str) -> Result<DotsDownload, ConversationError> {
        self.read_cached(workspace, id).await.map_err(|e| match e {
            BridgeError::Http(reqwest::StatusCode::NOT_FOUND) => ConversationError::NotFound { id: id.into() },
            _ => ConversationError::internal("Dots attachment is temporarily unavailable"),
        })
    }
}

pub(crate) async fn local_attachment(workspace: &Path, id: &str) -> Option<DotsDownload> {
    let path = workspace.join(".dots/inbox").join(id);
    if tokio::fs::canonicalize(&path).await.ok()? != path {
        return None;
    }
    let bytes = read_limited(&path).await.ok()?;
    Some(DotsDownload {
        content_type: image_type(&bytes),
        bytes,
    })
}

fn image_type(bytes: &[u8]) -> &'static str {
    if bytes.starts_with(b"\x89PNG\r\n\x1a\n") {
        "image/png"
    } else if bytes.starts_with(b"\xff\xd8\xff") {
        "image/jpeg"
    } else if bytes.starts_with(b"GIF87a") || bytes.starts_with(b"GIF89a") {
        "image/gif"
    } else if bytes.starts_with(b"RIFF") && bytes.get(8..12) == Some(b"WEBP") {
        "image/webp"
    } else {
        "application/octet-stream"
    }
}
