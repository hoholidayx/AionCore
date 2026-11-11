mod client;
mod files;
pub use files::DotsDownload;
pub(crate) use files::local_attachment;

use std::{
    path::{Path, PathBuf},
    sync::{
        Arc, OnceLock,
        atomic::{AtomicU8, Ordering},
    },
    time::Duration,
};

use aionui_api_types::DotsMessageAttachment;
use aionui_common::now_ms;
use aionui_db::{
    DbError, DotsReceiptWrite, DotsRequest, IConversationRepository, IDotsRepository,
    models::{ConversationRow, MessageRow},
};
use aionui_realtime::EventBroadcaster;
use client::{Bridge, BridgeError, Receipt, valid_id};
use futures_util::{StreamExt as _, stream};
use reqwest::{StatusCode, Url};
use serde_json::{Value, json};
use sha2::{Digest as _, Sha256};
use tokio::sync::{Notify, watch};

use crate::{ConversationError, message_persistence::persisted_message_stream};

pub const BACKEND: &str = aionui_ai_agent::dots::BACKEND;
pub(crate) fn is_artifact_id(id: &str) -> bool {
    valid_id(id, "art_")
}

pub const DEFAULT_MARKER: &str = "dots_default";

pub struct DotsConfig {
    base: Url,
    token: String,
    channel: String,
    subscription: Option<String>,
    pub owner_id: Option<String>,
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct SettingsFile {
    url: String,
    token_file: PathBuf,
    owner_id: Option<String>,
    channel: Option<String>,
    subscription_id: Option<String>,
}

impl DotsConfig {
    /// Missing configuration disables Dots. A configured but invalid file
    /// never reconciles away the existing conversation.
    pub fn load(data_dir: &Path) -> Result<Option<Self>, String> {
        let path = data_dir.join("dots-bridge.json");
        let raw = match std::fs::read(&path) {
            Ok(raw) => raw,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(_) => return Err("Cannot read dots-bridge.json".into()),
        };
        let settings: SettingsFile = serde_json::from_slice(&raw).map_err(|_| "Invalid dots-bridge.json")?;
        let base = Url::parse(&settings.url)
            .ok()
            .filter(client::valid_storage_url)
            .filter(|url| url.path() == "/" && url.query().is_none() && url.fragment().is_none())
            .ok_or("Dots Bridge URL must be a loopback HTTP or HTTPS origin")?;
        let token = std::fs::read_to_string(data_dir.join(settings.token_file))
            .map_err(|_| "Cannot read the Dots Bridge token file")?
            .trim()
            .to_owned();
        if token.is_empty() || token.bytes().any(|b| b.is_ascii_whitespace() || b.is_ascii_control()) {
            return Err("Invalid Dots Bridge token".into());
        }
        let channel = settings.channel.unwrap_or_else(|| "default".into());
        if channel.is_empty()
            || channel.len() > 128
            || !channel
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'.' | b'-'))
        {
            return Err("Invalid Dots Bridge channel".into());
        }
        Ok(Some(Self {
            base,
            token,
            channel,
            subscription: settings.subscription_id.filter(|value| !value.is_empty()),
            owner_id: settings.owner_id,
        }))
    }
}

pub struct DotsService {
    owner: String,
    binding: String,
    consumer: String,
    channel: String,
    subscription: Option<String>,
    default: OnceLock<String>,
    health: AtomicU8,
    bridge: Bridge,
    repo: Arc<dyn IDotsRepository>,
    conversations: Arc<dyn IConversationRepository>,
    broadcaster: Arc<dyn EventBroadcaster>,
    wake: Notify,
    updated: watch::Sender<u64>,
}

#[derive(Debug, thiserror::Error)]
enum PollError {
    #[error(transparent)]
    Bridge(#[from] BridgeError),
    #[error(transparent)]
    Db(#[from] DbError),
}

impl DotsService {
    pub fn new(
        config: DotsConfig,
        owner: String,
        instance_id: &str,
        repo: Arc<dyn IDotsRepository>,
        conversations: Arc<dyn IConversationRepository>,
        broadcaster: Arc<dyn EventBroadcaster>,
    ) -> Result<Self, String> {
        let binding = format!(
            "{:x}",
            Sha256::digest(format!("{instance_id}\n{owner}\n{}\n{}", config.base, config.channel))
        );
        let consumer = format!("aion-{binding}");
        Ok(Self {
            owner,
            binding,
            consumer,
            channel: config.channel,
            subscription: config.subscription,
            default: OnceLock::new(),
            health: AtomicU8::new(0),
            bridge: Bridge::new(config.base, config.token).map_err(|e| e.to_string())?,
            repo,
            conversations,
            broadcaster,
            wake: Notify::new(),
            updated: watch::channel(0).0,
        })
    }

    pub(crate) fn is_attachment_source(&self, user: &str, binding: &str) -> bool {
        self.owns(user) && self.binding == binding
    }

    pub fn owns(&self, user: &str) -> bool {
        self.owner == user
    }
    pub fn owner(&self) -> &str {
        &self.owner
    }
    pub fn default_id(&self) -> Option<&str> {
        self.default.get().map(String::as_str)
    }
    pub(crate) fn set_default(&self, id: String) {
        let _ = self.default.set(id);
    }

    pub(crate) fn health(&self) -> &'static str {
        match self.health.load(Ordering::Relaxed) {
            1 => "online",
            2 => "offline",
            3 => "no_subscription",
            _ => "checking",
        }
    }

    pub(crate) fn validate(text: &str, file_count: usize) -> Result<(), ConversationError> {
        if text.chars().count() > client::TEXT_LIMIT
            || file_count > client::FILE_COUNT
            || (text.trim().is_empty() && file_count == 0)
        {
            return Err(ConversationError::BadRequest {
                reason: "Dots requires text (up to 16,000 characters) or up to four attachments".into(),
            });
        }
        Ok(())
    }

    pub(crate) async fn enqueue(
        &self,
        workspace: &Path,
        message: &MessageRow,
        text: &str,
        files: &[String],
    ) -> Result<(), ConversationError> {
        let staged = files::stage(workspace, &message.id, files).await?;
        let json_files = serde_json::to_string(&staged)
            .map_err(|_| ConversationError::internal("Invalid Dots attachment manifest"))?;
        if let Err(error) = self
            .repo
            .enqueue(
                &self.owner,
                &self.binding,
                message,
                if text.trim().is_empty() { "" } else { text },
                &json_files,
            )
            .await
        {
            if !staged.is_empty() {
                let _ = tokio::fs::remove_dir_all(files::outbox(workspace, &message.id)).await;
            }
            return Err(if error.is_unique_violation() {
                ConversationError::Busy {
                    reason: "This Dots conversation is waiting for a reply".into(),
                }
            } else {
                error.into()
            });
        }
        self.wake.notify_one();
        Ok(())
    }

    pub(crate) async fn pending(&self) -> Result<Vec<DotsRequest>, DbError> {
        self.repo.pending(&self.binding, &self.owner).await
    }

    pub async fn run(&self, mut shutdown: watch::Receiver<bool>) {
        let mut interval = tokio::time::interval(Duration::from_secs(3));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut cursor = None;
        loop {
            tokio::select! {
                _ = interval.tick() => {},
                _ = self.wake.notified() => {},
                _ = shutdown.changed() => break,
            }
            tokio::select! {
                _ = shutdown.changed() => break,
                result = self.poll(&mut cursor) => {
                    if let Err(error) = result
                        && self.health.swap(2, Ordering::Relaxed) != 2
                    {
                        tracing::warn!(%error, "Dots Bridge polling unavailable");
                    }
                }
            }
        }
    }

    async fn poll(&self, cursor: &mut Option<i64>) -> Result<(), PollError> {
        let current = match *cursor {
            Some(cursor) => cursor,
            None => self.initialize_cursor().await?,
        };
        *cursor = Some(current);
        let mut previous = current;
        for receipt in self.bridge.received(&self.channel, current).await? {
            if receipt.sequence <= previous || receipt.channel != self.channel {
                return Err(BridgeError::Protocol.into());
            }
            previous = receipt.sequence;
            if receipt.sequence <= cursor.unwrap_or_default() {
                continue;
            }
            self.receive(&receipt).await?;
            *cursor = Some(self.bridge.ack(&self.consumer, &self.channel, receipt.sequence).await?);
        }
        let active = self
            .bridge
            .subscription_active(&self.channel, self.subscription.as_deref())
            .await?;
        self.health.store(if active { 1 } else { 3 }, Ordering::Relaxed);
        let errors: Vec<_> = stream::iter(self.repo.pending(&self.binding, &self.owner).await?)
            .map(|request| async move { self.advance(request, active).await })
            .buffer_unordered(4)
            .filter_map(|r| async { r.err() })
            .collect()
            .await;
        if let Some(error) = errors.into_iter().next() {
            return Err(error);
        }
        Ok(())
    }

    async fn initialize_cursor(&self) -> Result<i64, PollError> {
        let current = self.bridge.cursor(&self.consumer, &self.channel).await?;
        let start = match self.repo.start_sequence(&self.binding).await? {
            Some(start) => start,
            None => {
                let mut end = current;
                if current == 0 {
                    while let Some(last) = self.bridge.received(&self.channel, end).await?.last() {
                        if last.sequence <= end {
                            return Err(BridgeError::Protocol.into());
                        }
                        end = last.sequence;
                    }
                }
                self.repo.initialize_consumer(&self.binding, end).await?;
                end
            }
        };
        Ok(if start > current {
            self.bridge.ack(&self.consumer, &self.channel, start).await?
        } else {
            current
        })
    }

    async fn advance(&self, request: DotsRequest, active: bool) -> Result<(), PollError> {
        if let Some(id) = request.request_id.as_deref() {
            match self.bridge.status(id).await {
                Ok(status) => {
                    let failure = match (status.reply_status.as_str(), status.delivery_status.as_str()) {
                        ("replied", _) => None, // The durable received feed owns the reply.
                        ("expired", _) => Some("reply_expired"),
                        ("cancelled", _) if status.superseded_by.is_some() => None, // Wait for its receipt.
                        ("cancelled", _) => Some("cancelled"),
                        (_, "failed" | "cancelled") => Some("delivery_failed"),
                        _ => None,
                    };
                    if let Some(failure) = failure {
                        self.fail(&request, failure).await?;
                    }
                }
                Err(BridgeError::Http(StatusCode::NOT_FOUND)) => self.fail(&request, "request_missing").await?,
                Err(error) => return Err(error.into()),
            }
            return Ok(());
        }
        if !active {
            self.fail(&request, "no_subscription").await?;
            return Ok(());
        }
        match self.submit(&request).await {
            Ok(Some(id)) => self.repo.bind_request(&self.binding, &request.message_id, &id).await?,
            Ok(None) => {}
            Err(error) if !error.retryable() => self.fail(&request, "delivery_failed").await?,
            Err(error) => return Err(error.into()),
        }
        Ok(())
    }

    async fn submit(&self, request: &DotsRequest) -> Result<Option<String>, BridgeError> {
        let key = self.message_key(&request.message_id);
        let ids: Vec<String> = match request.artifact_ids.as_deref() {
            Some(ids) => serde_json::from_str(ids).map_err(|_| BridgeError::Protocol)?,
            None => {
                let staged: Vec<files::StagedFile> =
                    serde_json::from_str(&request.files).map_err(|_| BridgeError::Protocol)?;
                let mut ids = Vec::new();
                for (index, file) in staged.iter().enumerate() {
                    let bytes = files::read_staged(file).await?;
                    ids.push(self.bridge.upload(bytes, &file.name, &format!("{key}-{index}")).await?);
                }
                let json = serde_json::to_string(&ids).map_err(|_| BridgeError::Protocol)?;
                self.repo
                    .record_artifacts(&self.binding, &request.message_id, &json)
                    .await
                    .map_err(|_| BridgeError::Network)?;
                ids
            }
        };
        // File uploads can outlast a local deletion. Only publish while the
        // originating conversation still owns this pending request.
        if self
            .repo
            .request(&self.binding, &self.owner, &request.message_id)
            .await
            .map_err(|_| BridgeError::Network)?
            .is_none_or(|row| row.state != "pending")
        {
            return Ok(None);
        }
        let mut body = json!({
            "text": request.text, "artifact_ids": ids, "channel": self.channel, "idempotency_key": key,
            "metadata": {"aion_message_id": request.message_id, "aion_binding": self.binding},
        });
        if let Some(subscription) = &self.subscription {
            body["subscription_id"] = json!(subscription);
        }
        let status = self.bridge.submit(body).await?;
        if !valid_id(&status.request_id, "req_") {
            return Err(BridgeError::Protocol);
        }
        Ok(Some(status.request_id))
    }

    fn message_key(&self, message_id: &str) -> String {
        format!("aion-{:x}", Sha256::digest(format!("{}:{message_id}", self.binding)))
    }

    async fn find_request(&self, receipt: &Receipt, id: &str) -> Result<Option<DotsRequest>, DbError> {
        if let Some(request) = self.repo.by_request(&self.binding, id).await? {
            return Ok(Some(request));
        }
        // Bridge persists these metadata fields from the original request.
        // This closes the crash window between HTTP 202 and local binding.
        if receipt.metadata.get("aion_binding").and_then(Value::as_str) == Some(&self.binding)
            && let Some(message_id) = receipt.metadata.get("aion_message_id").and_then(Value::as_str)
            && let Some(request) = self.repo.request(&self.binding, &self.owner, message_id).await?
            && request.request_id.is_none()
            && request.artifact_ids.is_some()
        {
            self.repo.bind_request(&self.binding, message_id, id).await?;
            return Ok(Some(request));
        }
        Ok(None)
    }

    async fn receive(&self, receipt: &Receipt) -> Result<(), PollError> {
        let (conversation_id, request) = match (receipt.kind.as_str(), receipt.request_id.as_deref()) {
            ("push", None) => (self.default.get().ok_or(BridgeError::Protocol)?.clone(), None),
            ("reply" | "progress" | "cancel", Some(id)) if valid_id(id, "req_") => {
                let Some(request) = self.find_request(receipt, id).await? else {
                    tracing::debug!(request_id = %id, "Dots receipt has no local request");
                    return Ok(());
                };
                (request.conversation_id.clone(), Some(request))
            }
            _ => {
                tracing::warn!(sequence = receipt.sequence, "Skipped invalid Dots receipt");
                return Ok(());
            }
        };
        let Ok(receipt_id) = uuid::Uuid::parse_str(&receipt.id) else {
            return Err(BridgeError::Protocol.into());
        };
        let Some(row) = self.conversations.get(&self.owner, &conversation_id).await? else {
            return Ok(());
        };
        let mut attachments = Vec::new();
        for id in receipt
            .artifact_ids
            .iter()
            .filter(|id| valid_id(id, "art_"))
            .take(client::FILE_COUNT)
        {
            attachments.push(self.describe_attachment(id).await);
        }
        let ids = attachments
            .iter()
            .map(|file| file.artifact_id.clone())
            .collect::<Vec<_>>();
        let completion = match (receipt.kind.as_str(), request.as_ref()) {
            ("reply", Some(request)) => Some((request.message_id.as_str(), "replied")),
            ("cancel", Some(request)) => Some((request.message_id.as_str(), "cancelled")),
            _ => None,
        };
        let message = Self::receipt_message(
            format!("dots-{}-{receipt_id}", &self.binding[..12]),
            conversation_id,
            &receipt.text,
            &receipt.kind,
            &attachments,
            request.as_ref().map(|r| r.message_id.as_str()),
        );
        match self
            .repo
            .persist_receipt(DotsReceiptWrite {
                binding: &self.binding,
                owner: &self.owner,
                message: &message,
                artifact_ids: &ids,
                completion,
            })
            .await
        {
            Ok(true) => {
                self.broadcaster
                    .broadcast(persisted_message_stream(&self.owner, &message, None));
                self.updated.send_modify(|version| *version = version.wrapping_add(1));
                if let Some(workspace) = workspace(&row) {
                    for id in &ids {
                        let _ = tokio::time::timeout(Duration::from_secs(3), self.read_cached(&workspace, id)).await;
                    }
                }
            }
            Ok(false) | Err(DbError::NotFound(_)) => {}
            Err(error) => return Err(error.into()),
        }
        Ok(())
    }

    fn receipt_message(
        id: String,
        conversation_id: String,
        text: &str,
        kind: &str,
        attachments: &[DotsMessageAttachment],
        in_reply_to: Option<&str>,
    ) -> MessageRow {
        MessageRow {
            msg_id: Some(id.clone()), id, conversation_id, r#type: "text".into(),
            content: json!({ "content": text, "dots_kind": kind, "dots_attachments": attachments, "in_reply_to": in_reply_to }).to_string(),
            position: Some("left".into()), status: Some("finish".into()), hidden: false,
            created_at: now_ms(), backend_turn_id: None,
        }
    }

    async fn fail(&self, request: &DotsRequest, failure: &str) -> Result<(), DbError> {
        let message = Self::receipt_message(
            format!("dots-failure-{}", request.message_id),
            request.conversation_id.clone(),
            "",
            failure,
            &[],
            Some(&request.message_id),
        );
        match self
            .repo
            .persist_receipt(DotsReceiptWrite {
                binding: &self.binding,
                owner: &self.owner,
                message: &message,
                artifact_ids: &[],
                completion: Some((&request.message_id, "failed")),
            })
            .await
        {
            Ok(true) => {
                self.broadcaster
                    .broadcast(persisted_message_stream(&self.owner, &message, None));
                self.updated.send_modify(|version| *version = version.wrapping_add(1));
                tracing::info!(conversation_id = %request.conversation_id, message_id = %request.message_id, failure, "Dots request closed");
            }
            Ok(false) | Err(DbError::NotFound(_)) => {}
            Err(error) => return Err(error),
        }
        Ok(())
    }
}

pub(crate) fn is_dots(row: &ConversationRow) -> bool {
    row.r#type == "acp"
        && serde_json::from_str::<Value>(&row.extra)
            .ok()
            .and_then(|v| {
                v.get("backend")
                    .and_then(Value::as_str)
                    .map(|backend| backend == BACKEND)
            })
            .unwrap_or(false)
}

pub(crate) fn is_default(row: &ConversationRow) -> bool {
    serde_json::from_str::<Value>(&row.extra)
        .ok()
        .and_then(|v| v.get(DEFAULT_MARKER).and_then(Value::as_bool))
        .unwrap_or(false)
}

fn workspace(row: &ConversationRow) -> Option<PathBuf> {
    let extra: Value = serde_json::from_str(&row.extra).ok()?;
    Some(PathBuf::from(extra.get("workspace")?.as_str()?))
}

#[async_trait::async_trait]
impl aionui_ai_agent::dots::DotsTurns for DotsService {
    fn allowed(&self, user: &str) -> bool {
        self.owns(user)
    }

    async fn wait(&self, conversation: &str, message: &str) -> Result<(), ()> {
        let mut updates = self.updated.subscribe();
        loop {
            let row = self
                .repo
                .request(&self.binding, &self.owner, message)
                .await
                .map_err(|_| ())?
                .ok_or(())?;
            if row.conversation_id != conversation {
                return Err(());
            }
            match row.state.as_str() {
                "replied" | "cancelled" => return Ok(()),
                "failed" => return Err(()),
                _ => {}
            }
            tokio::select! {
                _ = updates.changed() => {},
                _ = tokio::time::sleep(Duration::from_secs(5)) => {},
            }
        }
    }
}

#[cfg(test)]
mod tests;
