//! Session adapter for conversation-owned durable Dots turns.

use std::sync::{
    Arc, Mutex,
    atomic::{AtomicU64, Ordering},
};

use aionui_session::{
    Admission, BackendError, BlockSet, Capabilities, Command, CommandReceipt, SessionBackend, SessionEnvelope,
    SessionEvent, TurnOutcome,
};
use futures_util::{stream, stream::BoxStream};
use tokio::{sync::broadcast, task::AbortHandle};

pub const BACKEND: &str = "dots";
pub const AGENT_ID: &str = "builtin-dots-bridge";

#[async_trait::async_trait]
pub trait DotsTurns: Send + Sync {
    fn allowed(&self, user: &str) -> bool;
    async fn wait(&self, conversation: &str, message: &str) -> Result<(), ()>;
}

pub(crate) struct DotsSession {
    conversation: String,
    turns: Arc<dyn DotsTurns>,
    events: broadcast::Sender<SessionEnvelope>,
    epoch: AtomicU64,
    monitor: Mutex<Option<AbortHandle>>,
}

impl DotsSession {
    pub(crate) fn new(conversation: String, turns: Arc<dyn DotsTurns>) -> Self {
        Self {
            conversation,
            turns,
            events: broadcast::channel(32).0,
            epoch: AtomicU64::new(0),
            monitor: Mutex::new(None),
        }
    }
}

impl Drop for DotsSession {
    fn drop(&mut self) {
        if let Ok(Some(task)) = self.monitor.get_mut().map(Option::take) {
            task.abort();
        }
    }
}

#[async_trait::async_trait]
impl SessionBackend for DotsSession {
    async fn dispatch(&self, command: Command) -> Result<CommandReceipt, BackendError> {
        let Command::Send { metadata, .. } = command else {
            return Err(BackendError::CommandNotSupported { command: "dots" });
        };
        let message = metadata
            .client_msg_id
            .ok_or_else(|| BackendError::Transport("Missing Dots request identity".into()))?;
        let mut monitor = self
            .monitor
            .lock()
            .map_err(|_| BackendError::Transport("Dots session unavailable".into()))?;
        if let Some(previous) = monitor.take() {
            previous.abort();
        }
        let epoch = self.epoch.fetch_add(1, Ordering::Relaxed) + 1;
        let events = self.events.clone();
        let conversation = self.conversation.clone();
        let turns = self.turns.clone();
        let task = tokio::spawn(async move {
            let emit = |event| {
                let _ = events.send(SessionEnvelope {
                    session_id: conversation.clone(),
                    turn_gen: epoch,
                    event,
                });
            };
            emit(SessionEvent::TurnStarted { epoch });
            let failed = turns.wait(&conversation, &message).await.is_err();
            emit(SessionEvent::TurnResult {
                is_error: failed,
                api_error_status: None,
                result_text: String::new(),
                epoch,
                outcome: if failed {
                    TurnOutcome::Failed
                } else {
                    TurnOutcome::EndTurn
                },
            });
        });
        *monitor = Some(task.abort_handle());
        Ok(CommandReceipt {
            accepted: true,
            admission: Admission::Started,
            turn_gen: epoch,
        })
    }

    fn events(&self) -> BoxStream<'static, SessionEnvelope> {
        Box::pin(stream::unfold(self.events.subscribe(), |mut events| async move {
            loop {
                match events.recv().await {
                    Ok(event) => return Some((event, events)),
                    Err(broadcast::error::RecvError::Closed) => return None,
                    Err(broadcast::error::RecvError::Lagged(_)) => continue,
                }
            }
        }))
    }

    fn capabilities(&self) -> Capabilities {
        Capabilities {
            prompt_blocks: BlockSet {
                text: true,
                resource: true,
                ..Default::default()
            },
            ..Default::default()
        }
    }

    fn output_persisted_externally(&self) -> bool {
        true
    }
}
