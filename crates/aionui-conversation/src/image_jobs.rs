//! Server-owned asynchronous image jobs and their durable conversation cards.
mod clients;
mod protocol;

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use aionui_ai_agent::async_image::{
    AsyncImageBackend, AsyncImageError, AsyncImageQueue, AsyncImageReceipt, AsyncImageReference, AsyncImageRequest,
    IMAGE_BACKEND_OVERRIDE_KEY, IMAGE_REFERENCE_PREFIX, MAX_IMAGE_REFERENCES,
};
use aionui_api_types::McpToolResponse;
use aionui_common::{TimestampMs, now_ms};
use aionui_db::models::{
    AsyncImageJobKey, AsyncImageJobReceiptRow, AsyncImageJobState as State, ClaimedAsyncImageJob, ConversationRow,
    McpServerRow, MessageRow, NewAsyncImageJob,
};
use aionui_db::{
    AsyncImageJobUpdate, DbError, IAssistantDefinitionRepository, IAsyncImageJobRepository, IConversationRepository,
    IMcpServerRepository,
};
use aionui_realtime::EventBroadcaster;
use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use serde_json::{Value, json};
use tokio::io::AsyncReadExt;
use tokio::sync::{Notify, watch};
use uuid::Uuid;

use crate::message_persistence::persisted_message_stream;
use crate::session_context::conversation_assistant_id;

use self::clients::{EndpointKey, ImageJobClients};
use self::protocol::{
    BackendError, CONTRACT, EndpointConfig, ImageJobClient, MAX_REFERENCE, MAX_REQUEST, MAX_TOTAL, RemoteState,
    endpoint_fingerprint, png_dimensions,
};

const CHECK_DELAY: TimestampMs = 2_000;
const RETRY_DELAY: TimestampMs = 5_000;
const RETRY_WINDOW: TimestampMs = 20 * 60 * 1_000;
// One claim can run discovery and up to three 30-second MCP calls.
const CLAIM_WINDOW: TimestampMs = 120_000;

pub struct AsyncImageAttachment {
    pub png: Vec<u8>,
}

pub struct AsyncImageJobs {
    repo: Arc<dyn IAsyncImageJobRepository>,
    mcp_repo: Arc<dyn IMcpServerRepository>,
    conversation_repo: Arc<dyn IConversationRepository>,
    assistant_repo: Arc<dyn IAssistantDefinitionRepository>,
    broadcaster: Arc<dyn EventBroadcaster>,
    wake: Notify,
    clients: ImageJobClients,
}

enum Step {
    Progress,
    Retry(&'static str),
    Completed(Vec<u8>),
    Failed(&'static str),
    Gone,
}

impl AsyncImageJobs {
    pub fn new(
        repo: Arc<dyn IAsyncImageJobRepository>,
        mcp_repo: Arc<dyn IMcpServerRepository>,
        conversation_repo: Arc<dyn IConversationRepository>,
        assistant_repo: Arc<dyn IAssistantDefinitionRepository>,
        broadcaster: Arc<dyn EventBroadcaster>,
    ) -> Self {
        Self {
            repo,
            mcp_repo,
            conversation_repo,
            assistant_repo,
            broadcaster,
            wake: Notify::new(),
            clients: ImageJobClients::default(),
        }
    }

    async fn allowed_backends(
        &self,
        conversation: &ConversationRow,
        extra: &Value,
    ) -> Result<Vec<McpServerRow>, AsyncImageError> {
        let snapshot = self
            .conversation_repo
            .get_assistant_snapshot(&conversation.user_id, &conversation.id)
            .await
            .map_err(database_error)?;
        let allowed = image_backend_ids_for_assistant(
            Some(self.assistant_repo.as_ref()),
            &conversation.user_id,
            conversation_assistant_id(snapshot.as_ref(), extra),
        )
        .await
        .map_err(database_error)?;
        let rows = self
            .mcp_repo
            .list(&conversation.user_id)
            .await
            .map_err(database_error)?;
        Ok(rows
            .into_iter()
            .filter(|row| image_backend_allowed(row, allowed.as_deref()))
            .collect())
    }

    pub async fn read_image(
        &self,
        user_id: &str,
        conversation_id: &str,
        task_id: &str,
    ) -> Result<AsyncImageAttachment, AsyncImageError> {
        let row = self
            .repo
            .get_attachment(user_id, conversation_id, task_id)
            .await
            .map_err(database_error)?
            .ok_or_else(|| AsyncImageError::new("attachment_not_found", "Image attachment not found"))?;
        Ok(AsyncImageAttachment { png: row.png })
    }

    pub async fn run(&self, shutdown: watch::Receiver<bool>) {
        tokio::join!(self.run_worker(shutdown.clone()), self.run_worker(shutdown));
        self.clients.close().await;
    }

    async fn run_worker(&self, mut shutdown: watch::Receiver<bool>) {
        while !*shutdown.borrow() {
            let now = now_ms();
            let claim_token = Uuid::now_v7().to_string();
            match self.repo.claim(now, now + CLAIM_WINDOW, &claim_token).await {
                Ok(Some(mut job)) => {
                    let task_id = job.task_id.clone();
                    tokio::select! {
                        _ = shutdown.changed() => {
                            if self.repo.release(&job).await.is_err() {
                                tracing::warn!(%task_id, "Asynchronous image job lease release failed");
                            }
                            return;
                        }
                        outcome = self.process_claim(&mut job) => {
                            if outcome.is_err() {
                                tracing::warn!(%task_id, "Asynchronous image job persistence failed");
                            }
                        }
                    }
                }
                Ok(None) => {
                    self.clients.prune().await;
                    tokio::select! {
                        _ = shutdown.changed() => return,
                        _ = self.wake.notified() => (),
                        _ = tokio::time::sleep(Duration::from_millis(500)) => (),
                    }
                }
                Err(_) => {
                    tracing::warn!("Asynchronous image job claim failed");
                    tokio::select! {
                        _ = shutdown.changed() => return,
                        _ = tokio::time::sleep(Duration::from_secs(2)) => (),
                    }
                }
            }
        }
    }

    async fn process_claim(&self, job: &mut ClaimedAsyncImageJob) -> Result<(), DbError> {
        let prior = job.state;
        let step = self.advance(job).await?;
        let now = now_ms();
        let (state, retry_started_at, delay, png, error) = match step {
            Step::Gone => return Ok(()),
            Step::Progress => (State::Polling, None, CHECK_DELAY, None, None),
            Step::Completed(png) => (State::Completed, None, 0, Some(png), None),
            Step::Failed(code) => (State::Failed, None, 0, None, Some(code)),
            Step::Retry(code) => {
                let start = job.retry_started_at.unwrap_or(now);
                if now.saturating_sub(start) >= RETRY_WINDOW {
                    (State::Failed, None, 0, None, Some(code))
                } else {
                    (job.state, Some(start), RETRY_DELAY, None, None)
                }
            }
        };
        let card = (state != prior && state != State::Submitting).then(|| {
            build_card(
                &job.conversation_id,
                &job.turn_id,
                &job.task_id,
                state,
                error,
                job.created_at,
            )
        });
        let committed = self
            .repo
            .finish_claim(AsyncImageJobUpdate {
                claim: job,
                state,
                next_check_at: now + delay,
                retry_started_at,
                result_png: png.as_deref(),
                card: card.as_ref(),
                now,
            })
            .await?;
        if committed && let Some(card) = card {
            self.broadcaster
                .broadcast(persisted_message_stream(&job.user_id, &card, None));
            if let Some(code) = error {
                tracing::warn!(task_id = %job.task_id, code, "Asynchronous image job failed");
            } else if state == State::Completed {
                tracing::info!(task_id = %job.task_id, "Asynchronous image job completed");
            }
        }
        Ok(())
    }

    async fn advance(&self, job: &mut ClaimedAsyncImageJob) -> Result<Step, DbError> {
        if job.contract_version != CONTRACT {
            return Ok(Step::Failed("unsupported_contract"));
        }
        let Some(row) = self.mcp_repo.find_by_id(&job.user_id, &job.server_id).await? else {
            return Ok(Step::Failed("backend_changed"));
        };
        if row.purpose != "async_image" || !matches!(row.transport_type.as_str(), "http" | "streamable_http") {
            return Ok(Step::Failed("backend_changed"));
        }
        let Ok(endpoint) = serde_json::from_str::<EndpointConfig>(&row.transport_config) else {
            return Ok(Step::Failed("backend_changed"));
        };
        if endpoint_fingerprint(&endpoint.url, &endpoint.headers) != job.endpoint_fingerprint {
            return Ok(Step::Failed("backend_changed"));
        }
        let key = EndpointKey {
            user_id: job.user_id.clone(),
            server_id: job.server_id.clone(),
            fingerprint: job.endpoint_fingerprint.clone(),
        };
        let slot = self.clients.slot(&key).await;
        let client = match ImageJobClients::connect(&slot, endpoint).await {
            Ok(client) => client,
            Err(error) => return Ok(backend_failure(error)),
        };
        let step = self.advance_connected(job, client).await;
        if matches!(step, Ok(Step::Retry("backend_status_unavailable"))) {
            self.clients.invalidate(&key, &slot).await;
        }
        step
    }

    async fn advance_connected(
        &self,
        job: &mut ClaimedAsyncImageJob,
        client: &ImageJobClient,
    ) -> Result<Step, DbError> {
        let status = match job.state {
            State::Pending => self.submit(job, client).await?,
            State::Submitting => match client.status(&job.task_id).await {
                Err(BackendError::TaskNotFound) => self.submit(job, client).await?,
                result => Some(result),
            },
            State::Polling => Some(client.status(&job.task_id).await),
            State::Completed | State::Failed => None,
        };
        let step = match status {
            None => Step::Gone,
            Some(Err(error)) => backend_failure(error),
            Some(Ok(RemoteState::Active)) => Step::Progress,
            Some(Ok(RemoteState::Uncertain)) => {
                job.state = State::Polling;
                Step::Retry("submission_uncertain")
            }
            Some(Ok(RemoteState::Failed(code))) => Step::Failed(code),
            Some(Ok(RemoteState::Completed)) => {
                job.state = State::Polling;
                match client.result(&job.task_id).await {
                    Ok(png) => Step::Completed(png),
                    Err(error) => backend_failure(error),
                }
            }
        };
        Ok(step)
    }

    async fn submit(
        &self,
        job: &mut ClaimedAsyncImageJob,
        client: &ImageJobClient,
    ) -> Result<Option<Result<RemoteState, BackendError>>, DbError> {
        let Some(request) = job
            .request_body
            .as_deref()
            .and_then(|body| serde_json::from_str::<Value>(body).ok())
        else {
            return Ok(Some(Err(BackendError::InvalidContract)));
        };
        if !self.repo.mark_submitting(job).await? {
            return Ok(None);
        }
        job.state = State::Submitting;
        Ok(Some(client.submit(request, &job.task_id).await))
    }
}

#[async_trait::async_trait]
impl AsyncImageQueue for AsyncImageJobs {
    async fn backends(&self, user_id: &str, conversation_id: &str) -> Result<Vec<AsyncImageBackend>, AsyncImageError> {
        let conversation = self
            .conversation_repo
            .get(user_id, conversation_id)
            .await
            .map_err(database_error)?
            .ok_or_else(|| AsyncImageError::new("conversation_not_found", "Image conversation not found"))?;
        let extra = serde_json::from_str(&conversation.extra).unwrap_or(Value::Null);
        let rows = self.allowed_backends(&conversation, &extra).await?;
        Ok(rows
            .into_iter()
            .map(|row| {
                let supports_edit = row
                    .tools
                    .as_deref()
                    .and_then(|tools| serde_json::from_str::<Vec<McpToolResponse>>(tools).ok())
                    .and_then(|tools| tools.into_iter().find(|tool| tool.name == "image_submit"))
                    .and_then(|tool| tool.input_schema)
                    .and_then(|schema| {
                        schema
                            .get("properties")
                            .map(|properties| properties.get("images").is_some())
                    });
                AsyncImageBackend {
                    id: row.id,
                    name: row.name,
                    supports_edit,
                }
            })
            .collect())
    }

    async fn enqueue(&self, request: AsyncImageRequest) -> Result<AsyncImageReceipt, AsyncImageError> {
        let key = AsyncImageJobKey {
            user_id: request.user_id,
            conversation_id: request.conversation_id,
            turn_id: request.turn_id,
            tool_call_id: request.tool_call_id,
        };
        if let Some(receipt) = self.repo.find_by_call(&key).await.map_err(database_error)? {
            return Ok(to_receipt(receipt));
        }
        if request.prompt.trim().is_empty() || request.prompt.chars().count() > 8000 {
            return Err(AsyncImageError::new(
                "invalid_prompt",
                "Enter a prompt of up to 8000 characters",
            ));
        }
        if request.references.len() > MAX_IMAGE_REFERENCES {
            return Err(AsyncImageError::new(
                "invalid_reference",
                format!("Use up to {MAX_IMAGE_REFERENCES} PNG references"),
            ));
        }
        let conversation = self
            .conversation_repo
            .get(&key.user_id, &key.conversation_id)
            .await
            .map_err(database_error)?
            .ok_or_else(|| AsyncImageError::new("conversation_not_found", "Image conversation not found"))?;
        let extra = serde_json::from_str::<Value>(&conversation.extra).unwrap_or(Value::Null);
        let saved_backend = extra.get(IMAGE_BACKEND_OVERRIDE_KEY).and_then(Value::as_str);
        let available = self.allowed_backends(&conversation, &extra).await?;
        let backend = select_backend(&available, request.requested_server_id.as_deref().or(saved_backend))?;
        let endpoint = serde_json::from_str::<EndpointConfig>(&backend.transport_config)
            .map_err(|_| AsyncImageError::new("backend_unavailable", "The selected image backend is unavailable"))?;
        let mut images = Vec::with_capacity(request.references.len());
        let mut total = 0usize;
        let invalid_reference =
            || AsyncImageError::new("invalid_reference", "Use valid PNG references within the size limits");
        for reference in request.references {
            let png = match reference {
                AsyncImageReference::AuthorizedPath(path) => read_reference(&path).await?,
                AsyncImageReference::Generated(task_id) => {
                    self.read_image(&key.user_id, &key.conversation_id, &task_id).await?.png
                }
            };
            total += png.len();
            if total > MAX_TOTAL || png.len() > MAX_REFERENCE {
                return Err(invalid_reference());
            }
            let encoded = tokio::task::spawn_blocking(move || png_dimensions(&png).map(|_| STANDARD.encode(png)))
                .await
                .ok()
                .flatten()
                .ok_or_else(invalid_reference)?;
            images.push(encoded);
        }
        let task_id = Uuid::now_v7().to_string();
        let mut args = json!({ "request_id": task_id, "prompt": request.prompt });
        if let Some(size) = request.size {
            args["size"] = json!(size);
        }
        if let Some(preset) = request.preset {
            args["preset"] = json!(preset);
        }
        if !images.is_empty() {
            args["images"] = json!(images);
        }
        let request_body = args.to_string();
        if request_body.len() > MAX_REQUEST {
            return Err(AsyncImageError::new(
                "invalid_reference",
                "Image request exceeds the size limit",
            ));
        }
        let now = now_ms();
        let card = build_card(&key.conversation_id, &key.turn_id, &task_id, State::Pending, None, now);
        let job = NewAsyncImageJob {
            key,
            task_id,
            server_id: backend.id.clone(),
            endpoint_fingerprint: endpoint_fingerprint(&endpoint.url, &endpoint.headers),
            contract_version: CONTRACT.into(),
            request_body,
            now,
        };
        let (receipt, created) = self.repo.enqueue(&job, &card).await.map_err(database_error)?;
        if created {
            self.broadcaster
                .broadcast(persisted_message_stream(&job.key.user_id, &card, None));
            self.wake.notify_one();
        }
        Ok(to_receipt(receipt))
    }
}

/// Absent IDs inherit the account catalog. An empty list permits no image backend.
pub(crate) async fn image_backend_ids_for_assistant(
    repo: Option<&dyn IAssistantDefinitionRepository>,
    user_id: &str,
    assistant_id: Option<&str>,
) -> Result<Option<Vec<String>>, DbError> {
    let (Some(repo), Some(assistant_id)) = (repo, assistant_id) else {
        return Ok(None);
    };
    let Some(definition) = repo.get_by_assistant_id_for_user(user_id, assistant_id).await? else {
        // A conversation bound to a removed assistant must not gain access to
        // backends that the assistant may have previously excluded.
        return Ok(Some(Vec::new()));
    };
    if !definition.async_image_enabled {
        return Ok(Some(Vec::new()));
    }
    definition
        .async_image_backend_ids
        .as_deref()
        .map(serde_json::from_str)
        .transpose()
        .map_err(|error| DbError::Init(format!("Invalid assistant image backend IDs: {error}")))
}

pub(crate) fn image_backend_allowed(row: &McpServerRow, allowed: Option<&[String]>) -> bool {
    row.is_enabled_image_backend() && allowed.is_none_or(|ids| ids.contains(&row.id))
}

fn select_backend<'a>(
    allowed: &'a [McpServerRow],
    explicit: Option<&str>,
) -> Result<&'a McpServerRow, AsyncImageError> {
    if let Some(explicit) = explicit {
        return allowed.iter().find(|server| server.id == explicit).ok_or_else(|| {
            AsyncImageError::new(
                "backend_unavailable",
                "The selected image backend is unavailable. Choose another backend.",
            )
        });
    }
    allowed.iter().find(|server| server.is_default).ok_or_else(|| {
        if allowed.is_empty() {
            AsyncImageError::new("backend_unavailable", "No asynchronous image backend is available")
        } else {
            AsyncImageError::new(
                "backend_required",
                "Select an image backend or configure a global default.",
            )
        }
    })
}

async fn read_reference(path: &Path) -> Result<Vec<u8>, AsyncImageError> {
    let invalid = || AsyncImageError::new("invalid_reference", "The selected image attachment could not be read");
    if !tokio::fs::metadata(path).await.map_err(|_| invalid())?.is_file() {
        return Err(invalid());
    }
    let file = tokio::fs::File::open(path).await.map_err(|_| invalid())?;
    if !file.metadata().await.map_err(|_| invalid())?.is_file() {
        return Err(invalid());
    }
    let mut png = Vec::new();
    file.take(MAX_REFERENCE as u64 + 1)
        .read_to_end(&mut png)
        .await
        .map_err(|_| invalid())?;
    Ok(png)
}

fn backend_failure(error: BackendError) -> Step {
    match error {
        BackendError::Transient => Step::Retry("backend_status_unavailable"),
        BackendError::ResultNotReady => Step::Retry("result_not_ready"),
        BackendError::TaskNotFound => Step::Failed("task_not_found"),
        BackendError::InvalidContract => Step::Failed("unsupported_contract"),
        BackendError::InvalidResult => Step::Failed("invalid_backend_result"),
        BackendError::Rejected(code) => Step::Failed(code),
    }
}

fn database_error(error: DbError) -> AsyncImageError {
    if matches!(error, DbError::NotFound(_)) {
        AsyncImageError::new("conversation_not_found", "Conversation not found")
    } else {
        AsyncImageError::new("storage_unavailable", "Asynchronous image job storage is unavailable")
    }
}

fn to_receipt(row: AsyncImageJobReceiptRow) -> AsyncImageReceipt {
    AsyncImageReceipt {
        attachment_id: format!("{IMAGE_REFERENCE_PREFIX}{}", row.task_id),
        task_id: row.task_id,
        state: ui_status(row.state).0.into(),
    }
}

fn ui_status(state: State) -> (&'static str, &'static str) {
    match state {
        State::Pending | State::Submitting => ("pending", "work"),
        State::Polling => ("in_progress", "work"),
        State::Completed => ("completed", "finish"),
        State::Failed => ("failed", "error"),
    }
}

fn build_card(
    conversation_id: &str,
    turn_id: &str,
    task_id: &str,
    state: State,
    error: Option<&str>,
    created_at: TimestampMs,
) -> MessageRow {
    let id = format!("{IMAGE_REFERENCE_PREFIX}{task_id}");
    let (tool_status, message_status) = ui_status(state);
    let mut image = json!({});
    if state == State::Completed {
        image["attachment_id"] = json!(task_id);
    }
    if let Some(code) = error {
        image["error_code"] = json!(code);
    }
    let content = json!({
        "session_id": conversation_id,
        "update": {
            "session_update": "tool_call",
            "tool_call_id": id,
            "title": "Asynchronous image generation",
            "kind": "execute",
            "status": tool_status,
            "raw_output": { "async_image": image },
        },
    });
    MessageRow {
        id: id.clone(),
        conversation_id: conversation_id.into(),
        msg_id: Some(id),
        r#type: "acp_tool_call".into(),
        content: content.to_string(),
        position: Some("left".into()),
        status: Some(message_status.into()),
        hidden: false,
        created_at,
        backend_turn_id: Some(turn_id.into()),
    }
}

#[cfg(test)]
#[path = "image_jobs/tests.rs"]
mod tests;
