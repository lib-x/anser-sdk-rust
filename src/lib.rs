//! Async Rust client for Anser's versioned HTTP API.
//!
//! A configured API key is sent only to the configured origin. Publish
//! operations reuse their idempotency key when retried. Keep a caller-supplied
//! key if the operation also needs to be retried across process restarts.

use std::time::Duration;

use reqwest::header::{AUTHORIZATION, HeaderValue};
use reqwest::{Method, RequestBuilder, Url};
use serde::{Deserialize, Serialize};
pub use types::*;

mod transport;
mod types;

#[cfg(test)]
mod mock;

pub use transport::{ApiError, Error};

/// Options for delivery requests.
#[derive(Clone, Default)]
pub struct SendOptions {
    /// A stable key chosen by the caller. Omit it to generate one for this SDK call.
    pub idempotency_key: Option<String>,
    /// Ask Anser to wait for a terminal result up to its configured limit.
    pub wait: bool,
    /// How long the server may wait. Values are clamped to 1 ms–30 seconds.
    pub wait_timeout: Option<Duration>,
}

#[derive(Clone, Default)]
pub struct PageOptions {
    pub cursor: Option<String>,
    pub limit: Option<u32>,
}

#[derive(Clone, Copy)]
pub struct RetryConfig {
    /// Retries beyond the first HTTP attempt.
    pub max_retries: u8,
    pub base_delay: Duration,
    pub max_delay: Duration,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_retries: 2,
            base_delay: Duration::from_millis(200),
            max_delay: Duration::from_secs(2),
        }
    }
}

#[derive(Clone)]
pub struct Client {
    base_url: Url,
    key: Option<HeaderValue>,
    http: reqwest::Client,
    retry: RetryConfig,
    operation_timeout: Duration,
}

pub struct ClientBuilder {
    base_url: Url,
    key: Option<HeaderValue>,
    request_timeout: Duration,
    operation_timeout: Duration,
    retry: RetryConfig,
}

impl ClientBuilder {
    pub fn api_key(mut self, key: &str) -> Result<Self, Error> {
        self.key = Some(bearer(key)?);
        Ok(self)
    }

    /// Set the deadline for one HTTP request, including decoding its response.
    #[must_use]
    pub fn request_timeout(mut self, timeout: Duration) -> Self {
        self.request_timeout = timeout;
        self
    }

    /// Set the deadline for the operation across all attempts and delays.
    #[must_use]
    pub fn operation_timeout(mut self, timeout: Duration) -> Self {
        self.operation_timeout = timeout;
        self
    }

    #[must_use]
    pub fn retry(mut self, retry: RetryConfig) -> Self {
        self.retry = retry;
        self
    }

    pub fn build(self) -> Result<Client, Error> {
        if self.request_timeout.is_zero()
            || self.operation_timeout.is_zero()
            || self.operation_timeout > Duration::from_secs(86400)
            || self.retry.base_delay > self.retry.max_delay
        {
            return Err(Error::InvalidConfiguration);
        }
        let http = reqwest::Client::builder()
            .timeout(self.request_timeout)
            .redirect(reqwest::redirect::Policy::none())
            .retry(reqwest::retry::never())
            .no_proxy()
            .build()
            .map_err(transport::transport_error)?;
        Ok(Client {
            base_url: self.base_url,
            key: self.key,
            http,
            retry: self.retry,
            operation_timeout: self.operation_timeout,
        })
    }
}

impl Client {
    pub fn builder(base_url: &str) -> Result<ClientBuilder, Error> {
        let base_url = Url::parse(base_url).map_err(|_| Error::InvalidBaseUrl)?;
        if !matches!(base_url.scheme(), "http" | "https")
            || base_url.host().is_none()
            || !base_url.username().is_empty()
            || base_url.password().is_some()
            || base_url.query().is_some()
            || base_url.fragment().is_some()
        {
            return Err(Error::InvalidBaseUrl);
        }
        Ok(ClientBuilder {
            base_url,
            key: None,
            request_timeout: Duration::from_secs(32),
            operation_timeout: Duration::from_secs(45),
            retry: RetryConfig::default(),
        })
    }

    pub fn new(base_url: &str, api_key: &str) -> Result<Self, Error> {
        Self::builder(base_url)?.api_key(api_key)?.build()
    }

    /// Assigns a key to this client while keeping its transport configuration.
    pub fn with_api_key(mut self, api_key: &str) -> Result<Self, Error> {
        self.key = Some(bearer(api_key)?);
        Ok(self)
    }

    pub async fn send(&self, message: &Publish) -> Result<Receipt, Error> {
        self.send_with_options(message, &SendOptions::default())
            .await
    }

    pub async fn send_with_options(
        &self,
        message: &Publish,
        options: &SendOptions,
    ) -> Result<Receipt, Error> {
        self.enqueue(&["send"], &PublishWire::new(message), options)
            .await
    }

    pub async fn publish(&self, topic: &str, message: &Publish) -> Result<Receipt, Error> {
        self.publish_with_options(topic, message, &SendOptions::default())
            .await
    }

    pub async fn publish_with_options(
        &self,
        topic: &str,
        message: &Publish,
        options: &SendOptions,
    ) -> Result<Receipt, Error> {
        self.enqueue(
            &["topics", topic, "publish"],
            &PublishWire::new(message),
            options,
        )
        .await
    }

    /// Synthesize and deliver an MP3 through a global administrator credential.
    ///
    /// Repeat a caller-supplied idempotency key after an ambiguous response.
    pub async fn send_speech(
        &self,
        input: &SpeechSend,
        idempotency_key: Option<&str>,
    ) -> Result<Receipt, Error> {
        let mut request = self
            .request(Method::POST, &["speech", "send"], true)?
            .json(input);
        if let Some(key) = idempotency_key {
            request = request.header("Idempotency-Key", idempotency_header(key)?);
        }
        // A transient response can occur while the provider is still synthesizing.
        self.execute(request, false).await
    }

    /// Sends a rendered template through explicit targets or the default route.
    pub async fn send_template(
        &self,
        message: &TemplatePublish,
        options: &SendOptions,
    ) -> Result<Receipt, Error> {
        self.enqueue(&["send"], message, options).await
    }

    /// Publishes a rendered template to a topic.
    pub async fn publish_template(
        &self,
        topic: &str,
        message: &TemplatePublish,
        options: &SendOptions,
    ) -> Result<Receipt, Error> {
        self.enqueue(&["topics", topic, "publish"], message, options)
            .await
    }

    pub async fn templates(&self, options: &PageOptions) -> Result<Page<Template>, Error> {
        let request = self.request(Method::GET, &["templates"], true)?;
        self.execute(with_page(request, options), true).await
    }

    pub async fn template(&self, id: &str) -> Result<Template, Error> {
        let request = self.request(Method::GET, &["templates", id], true)?;
        self.execute(request, true).await
    }

    pub async fn create_template(&self, input: &TemplateInput) -> Result<Template, Error> {
        let request = self
            .request(Method::POST, &["templates"], true)?
            .json(input);
        self.execute(request, false).await
    }

    pub async fn update_template(
        &self,
        id: &str,
        input: &TemplateInput,
    ) -> Result<Template, Error> {
        let request = self
            .request(Method::PUT, &["templates", id], true)?
            .json(input);
        self.execute(request, false).await
    }

    pub async fn delete_template(&self, id: &str) -> Result<(), Error> {
        let request = self.request(Method::DELETE, &["templates", id], true)?;
        self.execute(request, false).await
    }

    async fn enqueue(
        &self,
        path: &[&str],
        message: &impl Serialize,
        options: &SendOptions,
    ) -> Result<Receipt, Error> {
        let mut request = self.request(Method::POST, path, true)?;
        if options.wait {
            request = request.query(&[("wait", "true")]);
            if let Some(timeout) = options.wait_timeout {
                let timeout_ms =
                    u64::try_from(timeout.as_millis().clamp(1, 30_000)).expect("clamped to u64");
                request = request.query(&[("timeout_ms", timeout_ms)]);
            }
        }
        let generated;
        let key = if let Some(key) = &options.idempotency_key {
            key
        } else {
            generated = uuid::Uuid::new_v4().to_string();
            &generated
        };
        let key = idempotency_header(key)?;
        let request = request.header("Idempotency-Key", key).json(message);
        self.execute(request, true).await
    }

    pub async fn subscribe(
        &self,
        topic: &str,
        subscription: &SubscriptionInput,
    ) -> Result<Subscription, Error> {
        let request = self
            .request(Method::POST, &["topics", topic, "subscriptions"], true)?
            .json(&SubscriptionWire {
                request: subscription,
                password: subscription.password.as_deref(),
            });
        // Do not retry creation if its response may have been lost.
        self.execute(request, false).await
    }

    pub async fn unsubscribe(&self, topic: &str, id: &str) -> Result<(), Error> {
        let request = self.request(
            Method::DELETE,
            &["topics", topic, "subscriptions", id],
            true,
        )?;
        self.execute(request, false).await
    }

    pub async fn message(&self, id: &str) -> Result<MessageDetail, Error> {
        let request = self.request(Method::GET, &["messages", id], true)?;
        self.execute(request, true).await
    }

    /// Lists notification targets through an authenticated LazyCat ingress for a global admin.
    pub async fn lazycat_devices(&self) -> Result<LazycatDevices, Error> {
        self.execute(
            self.request(Method::GET, &["channels", "lazycat", "devices"], true)?,
            true,
        )
        .await
    }

    pub async fn messages(&self, options: &PageOptions) -> Result<Page<MessageDetail>, Error> {
        let request = self.request(Method::GET, &["messages"], true)?;
        self.execute(with_page(request, options), true).await
    }

    pub async fn inbox(&self, options: &PageOptions) -> Result<Page<InboxItem>, Error> {
        let request = self.request(Method::GET, &["inbox"], true)?;
        self.execute(with_page(request, options), true).await
    }

    /// Waits for new inbox items. With no cursor the server returns immediately.
    pub async fn wait_inbox(&self, options: &PageOptions) -> Result<Page<InboxItem>, Error> {
        let request = self
            .request(Method::GET, &["inbox"], true)?
            .query(&[("wait", "true")]);
        self.execute(with_page(request, options), true).await
    }

    pub async fn send_inbox(
        &self,
        recipient: &str,
        message: &Message,
    ) -> Result<InboxReceipt, Error> {
        let request = self
            .request(Method::POST, &["inbox", "send"], true)?
            .json(&InboxWire { recipient, message });
        self.execute(request, false).await
    }

    /// Retries with a caller-supplied stable key. Requires a server with native inbox
    /// idempotency support. For older servers use `send_inbox` for a single HTTP attempt.
    pub async fn send_inbox_idempotent(
        &self,
        recipient: &str,
        message: &Message,
        key: &str,
    ) -> Result<InboxReceipt, Error> {
        let request = self
            .request(Method::POST, &["inbox", "send"], true)?
            .header("Idempotency-Key", idempotency_header(key)?)
            .json(&InboxWire { recipient, message });
        self.execute(request, true).await
    }

    pub async fn mark_inbox_read(&self, id: i64) -> Result<(), Error> {
        let request = self.request(Method::POST, &["inbox", &id.to_string(), "read"], true)?;
        self.execute(request, false).await
    }

    pub async fn current_user(&self) -> Result<User, Error> {
        let request = self.request(Method::GET, &["auth", "me"], true)?;
        self.execute(request, true).await
    }

    pub async fn logout(&self) -> Result<(), Error> {
        let request = self.request(Method::POST, &["auth", "logout"], true)?;
        self.execute(request, false).await
    }

    pub async fn login(&self, username: &str, password: &str) -> Result<Session, Error> {
        self.login_credentials(username, password, None).await
    }

    /// Signs in using an authenticator code or an unused recovery code.
    pub async fn login_with_code(
        &self,
        username: &str,
        password: &str,
        code: &str,
    ) -> Result<Session, Error> {
        self.login_credentials(username, password, Some(code)).await
    }

    async fn login_credentials(
        &self,
        username: &str,
        password: &str,
        code: Option<&str>,
    ) -> Result<Session, Error> {
        let request = self
            .request(Method::POST, &["auth", "login"], false)?
            .json(&LoginWire {
                username,
                password,
                code,
            });
        self.execute(request, false).await
    }

    /// Returns the current session user's two-factor authentication status.
    pub async fn totp_status(&self) -> Result<TotpStatus, Error> {
        let request = self.request(Method::GET, &["auth", "totp"], true)?;
        self.execute(request, true).await
    }

    /// Starts a new TOTP setup bound to the current session.
    pub async fn setup_totp(&self, password: &str) -> Result<TotpSetup, Error> {
        let request = self
            .request(Method::POST, &["auth", "totp", "setup"], true)?
            .json(&PasswordWire { password });
        self.execute(request, false).await
    }

    /// Enables TOTP and returns the only copy of the new recovery codes.
    pub async fn confirm_totp(&self, code: &str) -> Result<TotpConfirmation, Error> {
        let request = self
            .request(Method::POST, &["auth", "totp", "confirm"], true)?
            .json(&CodeWire { code });
        self.execute(request, false).await
    }

    /// Disables TOTP with the current password and an authenticator or recovery code.
    pub async fn disable_totp(&self, password: &str, code: &str) -> Result<TotpStatus, Error> {
        let request = self
            .request(Method::POST, &["auth", "totp", "disable"], true)?
            .json(&TotpDisableWire { password, code });
        self.execute(request, false).await
    }

    pub async fn exchange_pair_code(&self, code: &str) -> Result<Session, Error> {
        let request = self
            .request(Method::POST, &["auth", "pair", "exchange"], false)?
            .json(&CodeWire { code });
        // A one-time code may have been consumed by a request whose response was lost.
        self.execute(request, false).await
    }

    /// Generates an expiring link for pairing another client.
    pub async fn create_pair_link(&self, server: Option<&str>) -> Result<PairLink, Error> {
        let request = self
            .request(Method::POST, &["auth", "pair"], true)?
            .json(&PairOptionsWire { server });
        self.execute(request, false).await
    }

    fn request(
        &self,
        method: Method,
        path: &[&str],
        authenticated: bool,
    ) -> Result<RequestBuilder, Error> {
        let mut endpoint = self.base_url.clone();
        let mut segments = endpoint
            .path_segments_mut()
            .map_err(|_| Error::InvalidBaseUrl)?;
        segments.pop_if_empty().push("v1");
        for segment in path {
            if matches!(*segment, "" | "." | "..") {
                return Err(Error::InvalidPath);
            }
            segments.push(segment);
        }
        drop(segments);
        let mut request = self.http.request(method, endpoint);
        if authenticated && let Some(key) = &self.key {
            request = request.header(AUTHORIZATION, key.clone());
        }
        Ok(request)
    }
}

fn with_page(mut request: RequestBuilder, options: &PageOptions) -> RequestBuilder {
    if let Some(cursor) = &options.cursor {
        request = request.query(&[("cursor", cursor)]);
    }
    if let Some(limit) = options.limit {
        request = request.query(&[("limit", limit)]);
    }
    request
}

#[derive(Clone, Deserialize)]
pub struct InboxItem {
    pub id: i64,
    pub audio_path: Option<String>,
    pub from_username: Option<String>,
    pub topic: Option<String>,
    pub title: String,
    pub body: String,
    pub format: String,
    pub created_at: i64,
    pub read_at: Option<i64>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct InboxReceipt {
    pub id: i64,
    /// Present on servers which record direct notifications in sending history.
    pub message_id: Option<String>,
}

/// Sensitive session response. Omit Debug so tokens do not end up in logs.
#[derive(Clone, Deserialize)]
pub struct Session {
    pub token: String,
    pub user: User,
}

#[derive(Clone, Copy, Debug, Deserialize)]
pub struct TotpStatus {
    pub enabled: bool,
}

/// A short-lived setup containing the authenticator seed and provisioning URI.
#[derive(Deserialize)]
pub struct TotpSetup {
    pub secret: String,
    pub uri: String,
    /// Setup expiration as Unix milliseconds.
    pub expires_at: i64,
}

/// Store the recovery codes securely. The server returns them once.
#[derive(Deserialize)]
pub struct TotpConfirmation {
    pub enabled: bool,
    pub recovery_codes: Vec<String>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct User {
    pub id: String,
    pub username: String,
    pub admin: bool,
    pub enabled: bool,
    pub created_at: i64,
}

/// Pairing links contain a one-time code. Share them only with the receiving client.
#[derive(Clone, Deserialize)]
pub struct PairLink {
    pub url: String,
    pub expires_at: i64,
}

#[derive(Serialize)]
struct PublishWire<'a> {
    #[serde(flatten)]
    request: &'a Publish,
    #[serde(skip_serializing_if = "Option::is_none")]
    topic_password: Option<&'a str>,
}

impl<'a> PublishWire<'a> {
    fn new(request: &'a Publish) -> Self {
        Self {
            request,
            topic_password: request.topic_password.as_deref(),
        }
    }
}

#[derive(Serialize)]
struct SubscriptionWire<'a> {
    #[serde(flatten)]
    request: &'a SubscriptionInput,
    #[serde(skip_serializing_if = "Option::is_none")]
    password: Option<&'a str>,
}

#[derive(Serialize)]
struct LoginWire<'a> {
    username: &'a str,
    password: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    code: Option<&'a str>,
}

#[derive(Serialize)]
struct CodeWire<'a> {
    code: &'a str,
}

#[derive(Serialize)]
struct PasswordWire<'a> {
    password: &'a str,
}

#[derive(Serialize)]
struct TotpDisableWire<'a> {
    password: &'a str,
    code: &'a str,
}

#[derive(Serialize)]
struct PairOptionsWire<'a> {
    server: Option<&'a str>,
}

#[derive(Serialize)]
struct InboxWire<'a> {
    recipient: &'a str,
    #[serde(flatten)]
    message: &'a Message,
}

fn idempotency_header(key: &str) -> Result<HeaderValue, Error> {
    if key.is_empty() || key.len() > 128 || !key.bytes().all(|byte| byte.is_ascii_graphic()) {
        return Err(Error::InvalidIdempotencyKey);
    }
    let mut key = HeaderValue::from_str(key).map_err(|_| Error::InvalidIdempotencyKey)?;
    key.set_sensitive(true);
    Ok(key)
}

fn bearer(key: &str) -> Result<HeaderValue, Error> {
    if key.is_empty() || !key.bytes().all(|byte| byte.is_ascii_graphic()) {
        return Err(Error::InvalidApiKey);
    }
    let mut value =
        HeaderValue::from_str(&format!("Bearer {key}")).map_err(|_| Error::InvalidApiKey)?;
    value.set_sensitive(true);
    Ok(value)
}

#[cfg(test)]
mod tests;
