use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::{Mutex, OnceCell};

use super::protocol::{BackendError, EndpointConfig, ImageJobClient};

const MAX_SESSIONS: usize = 16;
const SESSION_IDLE: Duration = Duration::from_secs(120);

#[derive(Clone, Eq, Hash, PartialEq)]
pub(super) struct EndpointKey {
    pub user_id: String,
    pub server_id: String,
    pub fingerprint: String,
}

type ClientSlot = Arc<OnceCell<ImageJobClient>>;

struct CachedClient {
    slot: ClientSlot,
    last_used: Instant,
}

#[derive(Default)]
pub(super) struct ImageJobClients(Mutex<HashMap<EndpointKey, CachedClient>>);

impl ImageJobClients {
    pub async fn slot(&self, key: &EndpointKey) -> ClientSlot {
        let mut clients = self.0.lock().await;
        if let Some(client) = clients.get_mut(key) {
            client.last_used = Instant::now();
            return client.slot.clone();
        }
        if clients.len() >= MAX_SESSIONS
            && let Some(oldest) = clients
                .iter()
                .filter(|(_, client)| Arc::strong_count(&client.slot) == 1)
                .min_by_key(|(_, client)| client.last_used)
                .map(|(key, _)| key.clone())
        {
            clients.remove(&oldest);
        }
        let slot = Arc::new(OnceCell::new());
        clients.insert(
            key.clone(),
            CachedClient {
                slot: slot.clone(),
                last_used: Instant::now(),
            },
        );
        slot
    }

    pub async fn connect(slot: &ClientSlot, endpoint: EndpointConfig) -> Result<&ImageJobClient, BackendError> {
        slot.get_or_try_init(|| ImageJobClient::connect(endpoint)).await
    }

    pub async fn invalidate(&self, key: &EndpointKey, failed: &ClientSlot) {
        let mut clients = self.0.lock().await;
        if clients.get(key).is_some_and(|cached| Arc::ptr_eq(&cached.slot, failed)) {
            clients.remove(key);
        }
    }

    pub async fn prune(&self) {
        self.0
            .lock()
            .await
            .retain(|_, client| Arc::strong_count(&client.slot) > 1 || client.last_used.elapsed() < SESSION_IDLE);
    }

    pub async fn close(&self) {
        let clients = std::mem::take(&mut *self.0.lock().await);
        let close = clients
            .values()
            .filter_map(|client| client.slot.get())
            .map(ImageJobClient::close);
        futures_util::future::join_all(close).await;
    }
}
