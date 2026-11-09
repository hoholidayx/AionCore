use async_trait::async_trait;
use sqlx::SqlitePool;

use crate::{AssistantRuntimeConfig, DbError, IAssistantRuntimeConfigRepository};

pub struct SqliteAssistantRuntimeConfigRepository {
    pool: SqlitePool,
}

impl SqliteAssistantRuntimeConfigRepository {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl IAssistantRuntimeConfigRepository for SqliteAssistantRuntimeConfigRepository {
    async fn get_for_user(&self, user_id: &str, assistant_id: &str) -> Result<Option<AssistantRuntimeConfig>, DbError> {
        let json: Option<String> = sqlx::query_scalar(
            "SELECT config_json FROM assistant_runtime_configs WHERE user_id = ? AND assistant_id = ?",
        )
        .bind(user_id)
        .bind(assistant_id)
        .fetch_optional(&self.pool)
        .await?;
        json.as_deref()
            .map(|json| {
                serde_json::from_str(json)
                    .map_err(|error| DbError::Init(format!("decode assistant runtime config: {error}")))
            })
            .transpose()
    }

    async fn put_for_user(
        &self,
        user_id: &str,
        assistant_id: &str,
        config: &AssistantRuntimeConfig,
    ) -> Result<(), DbError> {
        let json = serde_json::to_string(config)
            .map_err(|error| DbError::Init(format!("encode assistant runtime config: {error}")))?;
        sqlx::query(
            "INSERT INTO assistant_runtime_configs (user_id, assistant_id, config_json) VALUES (?, ?, ?) \
             ON CONFLICT(user_id, assistant_id) DO UPDATE SET config_json = excluded.config_json",
        )
        .bind(user_id)
        .bind(assistant_id)
        .bind(json)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn delete_for_user(&self, user_id: &str, assistant_id: &str) -> Result<(), DbError> {
        sqlx::query("DELETE FROM assistant_runtime_configs WHERE user_id = ? AND assistant_id = ?")
            .bind(user_id)
            .bind(assistant_id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }
}
