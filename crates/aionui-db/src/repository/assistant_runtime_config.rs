use async_trait::async_trait;

use crate::{AssistantRuntimeConfig, DbError};

#[async_trait]
pub trait IAssistantRuntimeConfigRepository: Send + Sync {
    async fn get_for_user(&self, user_id: &str, assistant_id: &str) -> Result<Option<AssistantRuntimeConfig>, DbError>;

    async fn put_for_user(
        &self,
        user_id: &str,
        assistant_id: &str,
        config: &AssistantRuntimeConfig,
    ) -> Result<(), DbError>;

    async fn delete_for_user(&self, user_id: &str, assistant_id: &str) -> Result<(), DbError>;
}
