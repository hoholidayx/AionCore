use aionui_common::TimestampMs;

#[derive(Clone)]
pub struct AsyncImageJobKey {
    pub user_id: String,
    pub conversation_id: String,
    pub turn_id: String,
    pub tool_call_id: String,
}

pub struct NewAsyncImageJob {
    pub key: AsyncImageJobKey,
    pub task_id: String,
    pub server_id: String,
    pub endpoint_fingerprint: String,
    pub contract_version: String,
    pub request_body: String,
    pub now: TimestampMs,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug, sqlx::Type)]
#[sqlx(rename_all = "snake_case")]
pub enum AsyncImageJobState {
    Pending,
    Submitting,
    Polling,
    Completed,
    Failed,
}

impl AsyncImageJobState {
    pub fn retains_request(self) -> bool {
        matches!(self, Self::Pending | Self::Submitting)
    }
}

#[derive(sqlx::FromRow)]
pub struct AsyncImageJobReceiptRow {
    pub task_id: String,
    pub state: AsyncImageJobState,
}

#[derive(sqlx::FromRow)]
pub struct ClaimedAsyncImageJob {
    pub task_id: String,
    pub user_id: String,
    pub conversation_id: String,
    pub turn_id: String,
    pub server_id: String,
    pub endpoint_fingerprint: String,
    pub contract_version: String,
    pub request_body: Option<String>,
    pub state: AsyncImageJobState,
    pub retry_started_at: Option<TimestampMs>,
    pub claim_token: String,
    pub created_at: TimestampMs,
}

#[derive(sqlx::FromRow)]
pub struct AsyncImageAttachmentRow {
    pub server_id: String,
    pub png: Vec<u8>,
}
