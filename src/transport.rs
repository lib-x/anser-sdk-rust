use std::time::{Duration, SystemTime};

use reqwest::header::{HeaderMap, RETRY_AFTER};
use reqwest::{Request, RequestBuilder, Response, StatusCode};
use serde::Deserialize;
use serde::de::DeserializeOwned;
use serde_json::Value;
use tokio::time::{Instant, sleep, timeout_at};

use crate::Client;

const RESPONSE_LIMIT: usize = 32 * 1024 * 1024;

#[derive(Debug, Deserialize, thiserror::Error)]
#[error("Anser API returned HTTP {}", .status.as_u16())]
pub struct ApiError {
    #[serde(skip, default = "unknown_status")]
    pub status: StatusCode,
    pub code: String,
    pub message: String,
    #[serde(default)]
    pub details: Value,
    #[serde(skip)]
    pub retry_after: Option<Duration>,
}

fn unknown_status() -> StatusCode {
    StatusCode::INTERNAL_SERVER_ERROR
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("configure an HTTP(S) server URL without credentials, query or fragment")]
    InvalidBaseUrl,
    #[error("an API key must contain visible ASCII characters")]
    InvalidApiKey,
    #[error("an idempotency key must contain 1–128 visible ASCII characters")]
    InvalidIdempotencyKey,
    #[error("path component must not be empty, . or ..")]
    InvalidPath,
    #[error("client timeouts or retry delays are invalid")]
    InvalidConfiguration,
    #[error("could not replay the request")]
    RequestReplay,
    #[error("could not complete the API request")]
    Transport(#[source] reqwest::Error),
    #[error("API operation timed out")]
    Timeout,
    #[error(transparent)]
    Api(#[from] ApiError),
    #[error("Anser returned an unexpected successful response")]
    InvalidResponse,
    #[error("Anser response exceeded the client limit")]
    ResponseTooLarge,
}

pub(crate) fn transport_error(error: reqwest::Error) -> Error {
    Error::Transport(error.without_url())
}

impl Error {
    fn retryable(&self) -> bool {
        match self {
            Self::Transport(error) => {
                error.is_timeout() || error.is_connect() || error.is_body() || error.is_request()
            }
            Self::Api(error) => {
                error.status.is_server_error()
                    || matches!(
                        error.status,
                        StatusCode::TOO_MANY_REQUESTS | StatusCode::REQUEST_TIMEOUT
                    )
            }
            _ => false,
        }
    }

    fn retry_after(&self) -> Option<Duration> {
        if let Self::Api(error) = self {
            return error.retry_after;
        }
        None
    }
}

impl Client {
    pub(crate) async fn execute<T: DeserializeOwned>(
        &self,
        request: RequestBuilder,
        safe_to_retry: bool,
    ) -> Result<T, Error> {
        let request = request.build().map_err(transport_error)?;
        let deadline = Instant::now() + self.operation_timeout;
        let max_retries = if safe_to_retry {
            self.retry.max_retries
        } else {
            0
        };
        let mut attempt_count = 0;
        let mut delay = self.retry.base_delay;
        loop {
            let attempt = request.try_clone().ok_or(Error::RequestReplay)?;
            let outcome = timeout_at(deadline, self.send_once(attempt))
                .await
                .map_err(|_| Error::Timeout)?;
            let Err(error) = outcome else { return outcome };
            if attempt_count >= max_retries || !error.retryable() {
                return Err(error);
            }

            let wait = delay.max(error.retry_after().unwrap_or_default());
            if wait >= deadline.saturating_duration_since(Instant::now()) {
                return Err(error);
            }
            sleep(wait).await;
            attempt_count += 1;
            delay = delay.saturating_mul(2).min(self.retry.max_delay);
        }
    }

    async fn send_once<T: DeserializeOwned>(&self, request: Request) -> Result<T, Error> {
        let response = self.http.execute(request).await.map_err(transport_error)?;
        let status = response.status();
        if !status.is_success() {
            let retry_after = retry_after(response.headers());
            let body = bounded_body(response).await;
            let mut error = body
                .ok()
                .and_then(|bytes| serde_json::from_slice::<ApiError>(&bytes).ok())
                .unwrap_or_else(|| ApiError {
                    status,
                    code: "unexpected_http_status".into(),
                    message: "Anser returned an unexpected HTTP status".into(),
                    details: Value::Null,
                    retry_after,
                });
            error.status = status;
            error.retry_after = retry_after;
            return Err(error.into());
        }
        let bytes = bounded_body(response).await?;
        let body: &[u8] = if bytes.is_empty() { b"null" } else { &bytes };
        serde_json::from_slice(body).map_err(|_| Error::InvalidResponse)
    }
}

async fn bounded_body(mut response: Response) -> Result<Vec<u8>, Error> {
    if response
        .content_length()
        .is_some_and(|length| length > RESPONSE_LIMIT as u64)
    {
        return Err(Error::ResponseTooLarge);
    }
    let mut bytes = Vec::new();
    while let Some(chunk) = response.chunk().await.map_err(transport_error)? {
        if chunk.len() > RESPONSE_LIMIT - bytes.len() {
            return Err(Error::ResponseTooLarge);
        }
        bytes.extend_from_slice(&chunk);
    }
    Ok(bytes)
}

fn retry_after(headers: &HeaderMap) -> Option<Duration> {
    let header = headers.get(RETRY_AFTER)?.to_str().ok()?;
    if let Ok(seconds) = header.parse::<u64>() {
        return Some(Duration::from_secs(seconds));
    }
    Some(
        httpdate::parse_http_date(header)
            .ok()?
            .duration_since(SystemTime::now())
            .unwrap_or_default(),
    )
}
