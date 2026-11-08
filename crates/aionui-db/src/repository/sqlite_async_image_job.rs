use aionui_common::TimestampMs;
use sqlx::{Sqlite, SqlitePool, Transaction};

use crate::error::DbError;
use crate::models::{
    AsyncImageAttachmentRow, AsyncImageJobKey, AsyncImageJobReceiptRow, AsyncImageJobState, ClaimedAsyncImageJob,
    MessageRow, NewAsyncImageJob,
};
use crate::repository::async_image_job::{AsyncImageJobUpdate, IAsyncImageJobRepository};

const FIND_BY_CALL: &str = "SELECT j.task_id, j.state FROM async_image_jobs j
    JOIN conversations c ON c.id = j.conversation_id AND c.user_id = j.user_id
    WHERE j.user_id = ? AND j.conversation_id = ? AND j.turn_id = ? AND j.tool_call_id = ?";

pub struct SqliteAsyncImageJobRepository {
    pool: SqlitePool,
}

impl SqliteAsyncImageJobRepository {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }
}

#[async_trait::async_trait]
impl IAsyncImageJobRepository for SqliteAsyncImageJobRepository {
    async fn find_by_call(&self, key: &AsyncImageJobKey) -> Result<Option<AsyncImageJobReceiptRow>, DbError> {
        Ok(sqlx::query_as(FIND_BY_CALL)
            .bind(&key.user_id)
            .bind(&key.conversation_id)
            .bind(&key.turn_id)
            .bind(&key.tool_call_id)
            .fetch_optional(&self.pool)
            .await?)
    }

    async fn enqueue(
        &self,
        job: &NewAsyncImageJob,
        card: &MessageRow,
    ) -> Result<(AsyncImageJobReceiptRow, bool), DbError> {
        let mut tx = self.pool.begin().await?;
        let created = sqlx::query(
            "INSERT INTO async_image_jobs (
                task_id, user_id, conversation_id, card_id, turn_id, tool_call_id, server_id,
                endpoint_fingerprint, contract_version, request_body, state, next_check_at, created_at
             )
             SELECT ?, c.user_id, c.id, ?, ?, ?, ?, ?, ?, ?, 'pending', ?, ?
             FROM conversations c WHERE c.id = ? AND c.user_id = ?
             ON CONFLICT(conversation_id, turn_id, tool_call_id) DO NOTHING",
        )
        .bind(&job.task_id)
        .bind(&card.id)
        .bind(&job.key.turn_id)
        .bind(&job.key.tool_call_id)
        .bind(&job.server_id)
        .bind(&job.endpoint_fingerprint)
        .bind(&job.contract_version)
        .bind(&job.request_body)
        .bind(job.now)
        .bind(job.now)
        .bind(&job.key.conversation_id)
        .bind(&job.key.user_id)
        .execute(&mut *tx)
        .await?
        .rows_affected()
            == 1;
        let receipt = if created {
            persist_card(&mut tx, &job.key.user_id, &job.key.conversation_id, card, job.now).await?;
            AsyncImageJobReceiptRow {
                task_id: job.task_id.clone(),
                state: AsyncImageJobState::Pending,
            }
        } else {
            sqlx::query_as(FIND_BY_CALL)
                .bind(&job.key.user_id)
                .bind(&job.key.conversation_id)
                .bind(&job.key.turn_id)
                .bind(&job.key.tool_call_id)
                .fetch_optional(&mut *tx)
                .await?
                .ok_or_else(|| DbError::NotFound("Conversation not found".into()))?
        };
        tx.commit().await?;
        Ok((receipt, created))
    }

    async fn claim(
        &self,
        now: TimestampMs,
        claim_until: TimestampMs,
        claim_token: &str,
    ) -> Result<Option<ClaimedAsyncImageJob>, DbError> {
        Ok(sqlx::query_as(
            "UPDATE async_image_jobs SET claim_token = ?, claim_until = ?
             WHERE task_id = (
                 SELECT j.task_id FROM async_image_jobs j
                 JOIN conversations c ON c.id = j.conversation_id AND c.user_id = j.user_id
                 WHERE j.state IN ('pending', 'submitting', 'polling') AND j.next_check_at <= ?
                     AND (j.claim_until IS NULL OR j.claim_until <= ?)
                 ORDER BY j.next_check_at, j.created_at, j.task_id LIMIT 1
             )
             RETURNING task_id, user_id, conversation_id, turn_id, server_id, endpoint_fingerprint,
                 contract_version, request_body, state, retry_started_at, claim_token, created_at",
        )
        .bind(claim_token)
        .bind(claim_until)
        .bind(now)
        .bind(now)
        .fetch_optional(&self.pool)
        .await?)
    }

    async fn mark_submitting(&self, claim: &ClaimedAsyncImageJob) -> Result<bool, DbError> {
        Ok(sqlx::query(
            "UPDATE async_image_jobs SET state = 'submitting'
             WHERE task_id = ? AND claim_token = ? AND state IN ('pending', 'submitting')
                AND EXISTS (
                    SELECT 1 FROM conversations c WHERE c.id = async_image_jobs.conversation_id
                        AND c.user_id = async_image_jobs.user_id
                )",
        )
        .bind(&claim.task_id)
        .bind(&claim.claim_token)
        .execute(&self.pool)
        .await?
        .rows_affected()
            == 1)
    }

    async fn finish_claim(&self, update: AsyncImageJobUpdate<'_>) -> Result<bool, DbError> {
        let claim = update.claim;
        let mut tx = self.pool.begin().await?;
        let updated = sqlx::query(
            "UPDATE async_image_jobs SET state = ?, next_check_at = ?, retry_started_at = ?,
                request_body = CASE WHEN ? THEN request_body ELSE NULL END,
                result_png = ?, claim_token = NULL, claim_until = NULL
             WHERE task_id = ? AND claim_token = ?
                AND EXISTS (
                    SELECT 1 FROM conversations c WHERE c.id = async_image_jobs.conversation_id
                        AND c.user_id = async_image_jobs.user_id
                )",
        )
        .bind(update.state)
        .bind(update.next_check_at)
        .bind(update.retry_started_at)
        .bind(update.state.retains_request())
        .bind(update.result_png)
        .bind(&claim.task_id)
        .bind(&claim.claim_token)
        .execute(&mut *tx)
        .await?
        .rows_affected()
            == 1;
        if updated && let Some(card) = update.card {
            persist_card(&mut tx, &claim.user_id, &claim.conversation_id, card, update.now).await?;
        }
        tx.commit().await?;
        Ok(updated)
    }

    async fn release(&self, claim: &ClaimedAsyncImageJob) -> Result<(), DbError> {
        sqlx::query(
            "UPDATE async_image_jobs SET claim_token = NULL, claim_until = NULL WHERE task_id = ? AND claim_token = ?",
        )
        .bind(&claim.task_id)
        .bind(&claim.claim_token)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn get_attachment(
        &self,
        user_id: &str,
        conversation_id: &str,
        task_id: &str,
    ) -> Result<Option<AsyncImageAttachmentRow>, DbError> {
        Ok(sqlx::query_as(
            "SELECT j.server_id, j.result_png AS png FROM async_image_jobs j
             JOIN conversations c ON c.id = j.conversation_id AND c.user_id = j.user_id
             WHERE j.user_id = ? AND j.conversation_id = ? AND j.task_id = ?
                AND j.state = 'completed' AND j.result_png IS NOT NULL",
        )
        .bind(user_id)
        .bind(conversation_id)
        .bind(task_id)
        .fetch_optional(&self.pool)
        .await?)
    }
}

async fn persist_card(
    tx: &mut Transaction<'_, Sqlite>,
    user_id: &str,
    conversation_id: &str,
    card: &MessageRow,
    now: TimestampMs,
) -> Result<(), DbError> {
    let changed = sqlx::query(
        "INSERT INTO messages (id, conversation_id, msg_id, type, content, position, status, hidden, created_at, backend_turn_id)
         SELECT ?, c.id, ?, ?, ?, ?, ?, ?, ?, ? FROM conversations c WHERE c.id = ? AND c.user_id = ?
         ON CONFLICT(id) DO UPDATE SET content = excluded.content, status = excluded.status
         WHERE messages.conversation_id = excluded.conversation_id AND messages.type = excluded.type",
    )
    .bind(&card.id)
    .bind(&card.msg_id)
    .bind(&card.r#type)
    .bind(&card.content)
    .bind(&card.position)
    .bind(&card.status)
    .bind(card.hidden)
    .bind(card.created_at)
    .bind(&card.backend_turn_id)
    .bind(conversation_id)
    .bind(user_id)
    .execute(&mut **tx)
    .await?;
    if changed.rows_affected() != 1 {
        return Err(DbError::NotFound("Conversation card not found".into()));
    }
    sqlx::query("UPDATE conversations SET updated_at = MAX(updated_at, ?) WHERE id = ? AND user_id = ?")
        .bind(now)
        .bind(conversation_id)
        .bind(user_id)
        .execute(&mut **tx)
        .await?;
    Ok(())
}
