use sqlx::SqlitePool;

use super::sqlite_conversation::insert_scoped_message;
use crate::{DbError, models::MessageRow};

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct DotsRequest {
    pub message_id: String,
    pub conversation_id: String,
    pub text: String,
    pub files: String,
    pub artifact_ids: Option<String>,
    pub request_id: Option<String>,
    pub state: String,
}

pub struct DotsReceiptWrite<'a> {
    pub binding: &'a str,
    pub owner: &'a str,
    pub message: &'a MessageRow,
    pub artifact_ids: &'a [String],
    pub completion: Option<(&'a str, &'a str)>,
}

#[async_trait::async_trait]
pub trait IDotsRepository: Send + Sync {
    async fn remove_catalog_except(&self, agent_id: &str, owner: Option<&str>) -> Result<(), DbError>;
    async fn defaults(&self) -> Result<Vec<(String, String)>, DbError>;
    async fn start_sequence(&self, binding: &str) -> Result<Option<i64>, DbError>;
    async fn initialize_consumer(&self, binding: &str, sequence: i64) -> Result<(), DbError>;
    async fn enqueue(
        &self,
        owner: &str,
        binding: &str,
        message: &MessageRow,
        text: &str,
        files: &str,
    ) -> Result<(), DbError>;
    async fn request(&self, binding: &str, owner: &str, message_id: &str) -> Result<Option<DotsRequest>, DbError>;
    async fn by_request(&self, binding: &str, request_id: &str) -> Result<Option<DotsRequest>, DbError>;
    async fn pending(&self, binding: &str, owner: &str) -> Result<Vec<DotsRequest>, DbError>;
    async fn record_artifacts(&self, binding: &str, message_id: &str, artifact_ids: &str) -> Result<(), DbError>;
    async fn bind_request(&self, binding: &str, message_id: &str, request_id: &str) -> Result<(), DbError>;
    async fn persist_receipt(&self, receipt: DotsReceiptWrite<'_>) -> Result<bool, DbError>;
    async fn artifact_binding(
        &self,
        owner: &str,
        conversation_id: &str,
        artifact_id: &str,
    ) -> Result<Option<String>, DbError>;
}

pub struct SqliteDotsRepository(SqlitePool);

impl SqliteDotsRepository {
    pub fn new(pool: SqlitePool) -> Self {
        Self(pool)
    }
}

const REQUEST: &str =
    "SELECT message_id, conversation_id, text, files, artifact_ids, request_id, state FROM dots_bridge_requests";

const OWNED_REQUEST: &str =
    "EXISTS (SELECT 1 FROM conversations c WHERE c.id = dots_bridge_requests.conversation_id AND c.user_id = ?)";

#[async_trait::async_trait]
impl IDotsRepository for SqliteDotsRepository {
    async fn remove_catalog_except(&self, agent_id: &str, owner: Option<&str>) -> Result<(), DbError> {
        sqlx::query("DELETE FROM agent_metadata WHERE agent_id = ? AND user_id IS NOT ?")
            .bind(agent_id)
            .bind(owner)
            .execute(&self.0)
            .await?;
        Ok(())
    }

    async fn defaults(&self) -> Result<Vec<(String, String)>, DbError> {
        Ok(sqlx::query_as("SELECT user_id, id FROM conversations WHERE json_valid(extra) AND json_extract(extra, '$.dots_default') = 1")
            .fetch_all(&self.0).await?)
    }

    async fn start_sequence(&self, binding: &str) -> Result<Option<i64>, DbError> {
        Ok(
            sqlx::query_scalar("SELECT start_sequence FROM dots_bridge_consumers WHERE binding = ?")
                .bind(binding)
                .fetch_optional(&self.0)
                .await?,
        )
    }

    async fn initialize_consumer(&self, binding: &str, sequence: i64) -> Result<(), DbError> {
        sqlx::query("INSERT INTO dots_bridge_consumers (binding, start_sequence) VALUES (?, ?) ON CONFLICT DO NOTHING")
            .bind(binding)
            .bind(sequence)
            .execute(&self.0)
            .await?;
        Ok(())
    }

    async fn enqueue(
        &self,
        owner: &str,
        binding: &str,
        message: &MessageRow,
        text: &str,
        files: &str,
    ) -> Result<(), DbError> {
        let mut tx = self.0.begin_with("BEGIN IMMEDIATE").await?;
        insert_scoped_message(&mut tx, owner, message).await?;
        sqlx::query("INSERT INTO dots_bridge_requests (message_id, conversation_id, binding, text, files) VALUES (?, ?, ?, ?, ?)")
            .bind(&message.id).bind(&message.conversation_id).bind(binding).bind(text).bind(files).execute(&mut *tx).await?;
        tx.commit().await?;
        Ok(())
    }

    async fn request(&self, binding: &str, owner: &str, message_id: &str) -> Result<Option<DotsRequest>, DbError> {
        Ok(sqlx::query_as(&format!(
            "{REQUEST} WHERE binding = ? AND message_id = ? AND {OWNED_REQUEST}"
        ))
        .bind(binding)
        .bind(message_id)
        .bind(owner)
        .fetch_optional(&self.0)
        .await?)
    }

    async fn by_request(&self, binding: &str, request_id: &str) -> Result<Option<DotsRequest>, DbError> {
        Ok(
            sqlx::query_as(&format!("{REQUEST} WHERE binding = ? AND request_id = ?"))
                .bind(binding)
                .bind(request_id)
                .fetch_optional(&self.0)
                .await?,
        )
    }

    async fn pending(&self, binding: &str, owner: &str) -> Result<Vec<DotsRequest>, DbError> {
        Ok(sqlx::query_as(&format!(
            "{REQUEST} WHERE binding = ? AND state = 'pending' AND {OWNED_REQUEST} ORDER BY message_id"
        ))
        .bind(binding)
        .bind(owner)
        .fetch_all(&self.0)
        .await?)
    }

    async fn record_artifacts(&self, binding: &str, message_id: &str, artifact_ids: &str) -> Result<(), DbError> {
        sqlx::query("UPDATE dots_bridge_requests SET artifact_ids = ? WHERE binding = ? AND message_id = ? AND artifact_ids IS NULL")
            .bind(artifact_ids).bind(binding).bind(message_id).execute(&self.0).await?;
        Ok(())
    }

    async fn bind_request(&self, binding: &str, message_id: &str, request_id: &str) -> Result<(), DbError> {
        sqlx::query("UPDATE dots_bridge_requests SET request_id = ? WHERE binding = ? AND message_id = ? AND request_id IS NULL")
            .bind(request_id).bind(binding).bind(message_id).execute(&self.0).await?;
        Ok(())
    }

    async fn persist_receipt(&self, receipt: DotsReceiptWrite<'_>) -> Result<bool, DbError> {
        let DotsReceiptWrite {
            binding,
            owner,
            message,
            artifact_ids,
            completion,
        } = receipt;
        let mut tx = self.0.begin_with("BEGIN IMMEDIATE").await?;
        let duplicate: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM messages WHERE id = ?)")
            .bind(&message.id)
            .fetch_one(&mut *tx)
            .await?;
        if duplicate {
            return Ok(false);
        }
        insert_scoped_message(&mut tx, owner, message).await?;
        for id in artifact_ids {
            sqlx::query("INSERT INTO dots_bridge_attachments (conversation_id, binding, artifact_id) VALUES (?, ?, ?) ON CONFLICT DO NOTHING")
                .bind(&message.conversation_id).bind(binding).bind(id).execute(&mut *tx).await?;
        }
        if let Some((message_id, state)) = completion {
            sqlx::query(
                "UPDATE dots_bridge_requests SET state = ? WHERE binding = ? AND message_id = ? AND state = 'pending'",
            )
            .bind(state)
            .bind(binding)
            .bind(message_id)
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;
        Ok(true)
    }

    async fn artifact_binding(
        &self,
        owner: &str,
        conversation_id: &str,
        artifact_id: &str,
    ) -> Result<Option<String>, DbError> {
        Ok(sqlx::query_scalar(
            "SELECT a.binding FROM dots_bridge_attachments a JOIN conversations c ON a.conversation_id = c.id
            WHERE c.user_id = ? AND a.conversation_id = ? AND a.artifact_id = ?",
        )
        .bind(owner)
        .bind(conversation_id)
        .bind(artifact_id)
        .fetch_optional(&self.0)
        .await?)
    }
}
