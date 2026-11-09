use std::fs::{self, OpenOptions};
use std::io::Read;
use std::path::{Component, Path};

const MAX_TOOL_IMAGE_BYTES: u64 = 32 * 1024 * 1024;

pub(crate) struct ToolImage {
    pub(crate) mime_type: &'static str,
    pub(crate) bytes: Vec<u8>,
}

/// Read a backend-recorded image path. The caller authorizes the message before
/// calling this helper, so a Codex home outside Aion's file roots is supported.
pub(crate) fn read_tool_image_file(path: &Path, mime_type: &str) -> Option<ToolImage> {
    if !path.is_absolute() || path.components().any(|component| component == Component::ParentDir) {
        return None;
    }
    if !fs::symlink_metadata(path).ok()?.is_file() {
        return None;
    }
    let mut options = OpenOptions::new();
    options.read(true);
    // Refuse a symlink or blocking FIFO even if the entry is replaced after the
    // pre-open metadata check. Validate the opened handle again before reading.
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK);
    }
    let file = options.open(path).ok()?;
    let metadata = file.metadata().ok()?;
    if !metadata.is_file() || metadata.len() == 0 || metadata.len() > MAX_TOOL_IMAGE_BYTES {
        return None;
    }

    let mut bytes = Vec::new();
    file.take(MAX_TOOL_IMAGE_BYTES + 1).read_to_end(&mut bytes).ok()?;
    if bytes.len() as u64 > MAX_TOOL_IMAGE_BYTES {
        return None;
    }
    let mime_type = match mime_type {
        "image/png" if bytes.starts_with(b"\x89PNG\r\n\x1a\n") => "image/png",
        "image/jpeg" if bytes.starts_with(b"\xff\xd8\xff") => "image/jpeg",
        "image/gif" if bytes.starts_with(b"GIF87a") || bytes.starts_with(b"GIF89a") => "image/gif",
        "image/webp" if bytes.starts_with(b"RIFF") && bytes.get(8..12) == Some(b"WEBP") => "image/webp",
        _ => return None,
    };
    Some(ToolImage { mime_type, bytes })
}
