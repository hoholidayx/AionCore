use std::path::{Path, PathBuf};
use std::time::Duration;

use aionui_common::CommandSpec;
use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, BufReader};

use super::codex_conn::{CodexConnection, codex_app_server_args, initialize_params};
use super::codex_title::{read_rpc_frame, write_frame};
use super::types::BackendError;
use crate::adapter::{AgentIo, ManagedProcessIo};

impl CodexConnection {
    /// Permanently delete a verified thread using a separate management connection.
    pub async fn delete_history(
        &self,
        program: PathBuf,
        thread_id: &str,
        expected_home: &Path,
    ) -> Result<(), BackendError> {
        let command = CommandSpec {
            command: program,
            args: codex_app_server_args(&[]),
            env: Vec::new(),
            cwd: None,
        };
        let process = self
            .spawner
            .spawn(command, &[], "aionui-codex-history")
            .await
            .map_err(|_| history_error("Could not start Codex for history deletion"))?;
        let io = ManagedProcessIo::new(process);
        let result = tokio::time::timeout(Duration::from_secs(15), delete_thread(&io, thread_id, expected_home)).await;
        io.terminate().await;
        result.map_err(|_| history_error("Codex history deletion timed out"))?
    }
}

async fn delete_thread(io: &dyn AgentIo, thread_id: &str, expected_home: &Path) -> Result<(), BackendError> {
    let transport_error = || history_error("Codex history deletion did not receive a valid response");
    let (mut stdin, stdout) = io.take_stdio().await.ok_or_else(transport_error)?;
    let mut lines = BufReader::new(stdout).lines();
    write_frame(&mut stdin, &initialize_params().into_frame(1, "initialize"))
        .await
        .ok_or_else(transport_error)?;
    let reply = read_rpc_frame(&mut lines, 1).await.ok_or_else(transport_error)?;
    let actual_home = reply.pointer("/result/codexHome").and_then(Value::as_str);
    let actual_home = actual_home.map(Path::new).filter(|path| path.is_absolute());
    if let Some(path) = actual_home
        && tokio::fs::canonicalize(path).await.ok().as_deref() == Some(expected_home)
    {
        write_frame(&mut stdin, &json!({"method": "initialized", "params": {}}))
            .await
            .ok_or_else(transport_error)?;
    } else {
        return Err(history_error("Codex data directory does not match the verified thread"));
    }
    write_frame(
        &mut stdin,
        &json!({"id": 2, "method": "thread/delete", "params": {"threadId": thread_id}}),
    )
    .await
    .ok_or_else(transport_error)?;
    let reply = read_rpc_frame(&mut lines, 2).await.ok_or_else(transport_error)?;
    if reply.get("result").is_some_and(Value::is_object) || already_deleted(&reply, thread_id) {
        return Ok(());
    }
    if reply.pointer("/error/code").and_then(Value::as_i64) == Some(-32601) {
        return Err(history_error(
            "This Codex version does not support permanent thread deletion",
        ));
    }
    Err(history_error("Codex rejected permanent thread deletion"))
}

fn already_deleted(reply: &Value, thread_id: &str) -> bool {
    // Verified with codex 0.167.0-alpha.3.2 via repeated thread/delete in an isolated home.
    reply.pointer("/error/code").and_then(Value::as_i64) == Some(-32600)
        && reply.pointer("/error/message").and_then(Value::as_str)
            == Some(format!("no rollout found for thread id {thread_id}").as_str())
}

fn history_error(message: &str) -> BackendError {
    BackendError::Transport(message.to_owned())
}

#[cfg(test)]
#[path = "codex_history_test.rs"]
mod tests;
