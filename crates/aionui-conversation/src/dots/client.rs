use std::time::Duration;

use reqwest::{Client, Method, Response, StatusCode, Url};
use serde::{Deserialize, de::DeserializeOwned};

pub(super) const FILE_LIMIT: usize = 20 * 1024 * 1024;
pub(super) const FILE_COUNT: usize = 4;
pub(super) const TEXT_LIMIT: usize = 16_000;

pub(super) struct Bridge {
    api: Client,
    storage: Client,
    base: Url,
    token: String,
}

#[derive(Debug, thiserror::Error)]
pub(super) enum BridgeError {
    #[error("Bridge or storage is unreachable")]
    Network,
    #[error("Bridge or storage returned HTTP {0}")]
    Http(StatusCode),
    #[error("Bridge returned an invalid response")]
    Protocol,
}

impl BridgeError {
    pub(super) fn retryable(&self) -> bool {
        match self {
            Self::Network => true,
            Self::Http(status) => status.is_server_error() || matches!(status.as_u16(), 401 | 403 | 408 | 429),
            Self::Protocol => false,
        }
    }
}

#[derive(Deserialize)]
pub(super) struct MessageStatus {
    pub request_id: String,
    pub delivery_status: String,
    pub reply_status: String,
    pub superseded_by: Option<String>,
}

#[derive(Deserialize)]
pub(super) struct Receipt {
    pub sequence: i64,
    pub id: String,
    pub kind: String,
    pub request_id: Option<String>,
    pub channel: String,
    pub text: String,
    pub metadata: serde_json::Value,
    #[serde(default)]
    pub artifact_ids: Vec<String>,
}

#[derive(Deserialize)]
pub(super) struct DownloadGrant {
    pub download_url: Url,
    pub filename: String,
    pub content_type: String,
    pub size_bytes: usize,
}

#[derive(Deserialize)]
struct UploadGrant {
    artifact_id: String,
    status: String,
    upload_url: Option<Url>,
    required_headers: Option<std::collections::HashMap<String, String>>,
}

#[derive(Deserialize)]
struct Cursor {
    cursor: i64,
}

#[derive(Deserialize)]
struct Received {
    messages: Vec<Receipt>,
}

#[derive(Deserialize)]
struct Subscriptions {
    subscriptions: Vec<Subscription>,
}

#[derive(Deserialize)]
struct Subscription {
    id: String,
    channel: String,
}

impl Bridge {
    pub(super) fn new(base: Url, token: String) -> Result<Self, BridgeError> {
        let builder = || {
            Client::builder()
                .redirect(reqwest::redirect::Policy::none())
                .connect_timeout(Duration::from_secs(5))
        };
        let api = builder()
            .no_proxy()
            .timeout(Duration::from_secs(10))
            .build()
            .map_err(|_| BridgeError::Protocol)?;
        let storage = builder()
            .timeout(Duration::from_secs(60))
            .build()
            .map_err(|_| BridgeError::Protocol)?;
        Ok(Self {
            api,
            storage,
            base,
            token,
        })
    }

    fn request(&self, method: Method, path: &str) -> Result<reqwest::RequestBuilder, BridgeError> {
        let url = self.base.join(path).map_err(|_| BridgeError::Protocol)?;
        Ok(self.api.request(method, url).bearer_auth(&self.token))
    }

    async fn json<T: DeserializeOwned>(request: reqwest::RequestBuilder) -> Result<T, BridgeError> {
        let response = request.send().await.map_err(|_| BridgeError::Network)?;
        if !response.status().is_success() {
            return Err(BridgeError::Http(response.status()));
        }
        response.json().await.map_err(|_| BridgeError::Protocol)
    }

    pub(super) async fn subscription_active(
        &self,
        channel: &str,
        subscription: Option<&str>,
    ) -> Result<bool, BridgeError> {
        let result: Subscriptions = Self::json(self.request(Method::GET, "/api/subscriptions")?).await?;
        Ok(result
            .subscriptions
            .iter()
            .filter(|s| s.channel == channel && subscription.is_none_or(|id| id == s.id))
            .count()
            == 1)
    }

    pub(super) async fn cursor(&self, consumer: &str, channel: &str) -> Result<i64, BridgeError> {
        let result: Cursor = Self::json(
            self.request(Method::GET, &format!("/api/consumers/{consumer}"))?
                .query(&[("channel", channel)]),
        )
        .await?;
        Ok(result.cursor)
    }

    pub(super) async fn received(&self, channel: &str, after: i64) -> Result<Vec<Receipt>, BridgeError> {
        let result: Received = Self::json(
            self.request(Method::GET, "/api/received")?
                .query(&[("channel", channel), ("after", &after.to_string())]),
        )
        .await?;
        Ok(result.messages)
    }

    pub(super) async fn ack(&self, consumer: &str, channel: &str, sequence: i64) -> Result<i64, BridgeError> {
        let result: Cursor = Self::json(
            self.request(Method::POST, &format!("/api/consumers/{consumer}/ack"))?
                .json(&serde_json::json!({"channel": channel, "sequence": sequence})),
        )
        .await?;
        Ok(result.cursor)
    }

    pub(super) async fn submit(&self, request: serde_json::Value) -> Result<MessageStatus, BridgeError> {
        Self::json(self.request(Method::POST, "/api/messages")?.json(&request)).await
    }

    pub(super) async fn status(&self, request_id: &str) -> Result<MessageStatus, BridgeError> {
        if !valid_id(request_id, "req_") {
            return Err(BridgeError::Protocol);
        }
        Self::json(self.request(Method::GET, &format!("/api/messages/{request_id}"))?).await
    }

    pub(super) async fn upload(&self, bytes: Vec<u8>, filename: &str, key: &str) -> Result<String, BridgeError> {
        use base64::Engine as _;
        use md5::{Digest as _, Md5};
        let body = serde_json::json!({
            "filename": filename,
            "size_bytes": bytes.len(),
            "content_md5": base64::engine::general_purpose::STANDARD.encode(Md5::digest(&bytes)),
            "content_type": mime_guess::from_path(filename).first_or_octet_stream().essence_str(),
            "idempotency_key": key,
        });
        let grant: UploadGrant = Self::json(self.request(Method::POST, "/api/artifacts/uploads")?.json(&body)).await?;
        if !valid_id(&grant.artifact_id, "art_") {
            return Err(BridgeError::Protocol);
        }
        if grant.status == "ready" {
            return Ok(grant.artifact_id);
        }
        if grant.status != "pending" {
            return Err(BridgeError::Protocol);
        }
        let url = grant
            .upload_url
            .filter(valid_storage_url)
            .ok_or(BridgeError::Protocol)?;
        let mut req = self.storage.put(url).body(bytes);
        for (key, value) in grant.required_headers.ok_or(BridgeError::Protocol)? {
            if !matches!(
                key.to_ascii_lowercase().as_str(),
                "content-type"
                    | "content-length"
                    | "content-md5"
                    | "content-disposition"
                    | "cache-control"
                    | "if-none-match"
            ) {
                return Err(BridgeError::Protocol);
            }
            req = req.header(key, value);
        }
        let response = req.send().await.map_err(|_| BridgeError::Network)?;
        // An earlier PUT may have succeeded after the client lost its response.
        if response.status().is_success() || response.status() == StatusCode::PRECONDITION_FAILED {
            Ok(grant.artifact_id)
        } else {
            Err(BridgeError::Http(response.status()))
        }
    }

    pub(super) async fn download_grant(&self, id: &str) -> Result<DownloadGrant, BridgeError> {
        if !valid_id(id, "art_") {
            return Err(BridgeError::Protocol);
        }
        let grant: DownloadGrant =
            Self::json(self.request(Method::GET, &format!("/api/artifacts/{id}/download"))?).await?;
        if grant.size_bytes > FILE_LIMIT || !valid_storage_url(&grant.download_url) {
            return Err(BridgeError::Protocol);
        }
        Ok(grant)
    }

    pub(super) async fn download(&self, grant: &DownloadGrant) -> Result<Vec<u8>, BridgeError> {
        let mut response: Response = self
            .storage
            .get(grant.download_url.clone())
            .send()
            .await
            .map_err(|_| BridgeError::Network)?;
        if !response.status().is_success() {
            return Err(BridgeError::Http(response.status()));
        }
        let mut bytes = Vec::with_capacity(grant.size_bytes);
        while let Some(chunk) = response.chunk().await.map_err(|_| BridgeError::Network)? {
            if bytes.len() + chunk.len() > grant.size_bytes {
                return Err(BridgeError::Protocol);
            }
            bytes.extend_from_slice(&chunk);
        }
        if bytes.len() != grant.size_bytes {
            return Err(BridgeError::Protocol);
        }
        Ok(bytes)
    }
}

pub(super) fn valid_id(id: &str, prefix: &str) -> bool {
    id.strip_prefix(prefix)
        .is_some_and(|hex| hex.len() == 32 && hex.bytes().all(|ch| ch.is_ascii_hexdigit()))
}

pub(super) fn valid_storage_url(url: &Url) -> bool {
    url.has_host()
        && url.username().is_empty()
        && url.password().is_none()
        && (url.scheme() == "https"
            || (url.scheme() == "http" && matches!(url.host_str(), Some("127.0.0.1" | "[::1]" | "localhost"))))
}
