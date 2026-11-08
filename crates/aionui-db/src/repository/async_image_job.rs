use aionui_common::TimestampMs;

use crate::error::DbError;
use crate::models::{
    AsyncImageAttachmentRow, AsyncImageJobKey, AsyncImageJobReceiptRow, AsyncImageJobState, ClaimedAsyncImageJob,
    MessageRow, NewAsyncImageJob,
};

pub struct AsyncImageJobUpdate<'a> {
    pub claim: &'a ClaimedAsyncImageJob,
    pub state: AsyncImageJobState,
    pub next_check_at: TimestampMs,
    pub retry_started_at: Option<TimestampMs>,
    pub result_png: Option<&'a [u8]>,
    pub card: Option<&'a MessageRow>,
    pub now: TimestampMs,
}

#[async_trait::async_trait]
pub trait IAsyncImageJobRepository: Send + Sync {
    async fn find_by_call(&self, key: &AsyncImageJobKey) -> Result<Option<AsyncImageJobReceiptRow>, DbError>;

    /// Returns the existing receipt on tool-call replay. The boolean indicates a new card was inserted.
    async fn enqueue(
        &self,
        job: &NewAsyncImageJob,
        card: &MessageRow,
    ) -> Result<(AsyncImageJobReceiptRow, bool), DbError>;

    async fn claim(
        &self,
        now: TimestampMs,
        claim_until: TimestampMs,
        claim_token: &str,
    ) -> Result<Option<ClaimedAsyncImageJob>, DbError>;

    async fn mark_submitting(&self, claim: &ClaimedAsyncImageJob) -> Result<bool, DbError>;

    async fn finish_claim(&self, update: AsyncImageJobUpdate<'_>) -> Result<bool, DbError>;

    async fn release(&self, claim: &ClaimedAsyncImageJob) -> Result<(), DbError>;

    async fn get_attachment(
        &self,
        user_id: &str,
        conversation_id: &str,
        task_id: &str,
    ) -> Result<Option<AsyncImageAttachmentRow>, DbError>;
}
