//! Types used by the Anser HTTP API.

use std::num::NonZeroU32;

use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MessageFormat {
    #[default]
    Text,
    Markdown,
    Html,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct Message {
    #[serde(default)]
    pub title: String,
    #[serde(default)]
    pub body: String,
    #[serde(default)]
    pub format: MessageFormat,
    #[serde(default)]
    pub tags: Vec<String>,
    pub url: Option<String>,
    pub level: Option<String>,
    #[serde(default)]
    pub attachments: Vec<Attachment>,
    #[serde(default)]
    pub extras: serde_json::Value,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Attachment {
    pub url: String,
    pub name: Option<String>,
    pub content_type: Option<String>,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Mode {
    Broadcast,
    Failover,
    Weighted,
    WeightedFailover,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SuccessCriteria {
    Any,
    All,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Backoff {
    Fixed,
    Exponential,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Target {
    pub channel: Option<String>,
    pub account_id: Option<String>,
    #[serde(default)]
    pub tag_selector: Vec<String>,
    pub weight: Option<u32>,
    pub priority: Option<i32>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct Retry {
    pub max_attempts: NonZeroU32,
    pub backoff: Backoff,
    pub base_delay: u64,
    pub max_delay: u64,
    pub jitter: bool,
    pub until_success: bool,
    pub ttl: u64,
}

impl Default for Retry {
    fn default() -> Self {
        Self {
            max_attempts: NonZeroU32::new(3).unwrap(),
            backoff: Backoff::Exponential,
            base_delay: 1_000,
            max_delay: 30_000,
            jitter: true,
            until_success: false,
            ttl: 3_600_000,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct Policy {
    pub mode: Mode,
    pub targets: Vec<Target>,
    pub retry: Retry,
    pub success_criteria: SuccessCriteria,
}

impl Default for Policy {
    fn default() -> Self {
        Self {
            mode: Mode::Broadcast,
            targets: Vec::new(),
            retry: Retry::default(),
            success_criteria: SuccessCriteria::All,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(untagged)]
pub enum Selector {
    Name(String),
    Target(Target),
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct Strategy {
    pub mode: Option<Mode>,
    pub retry: Option<Retry>,
    pub success_criteria: Option<SuccessCriteria>,
}

#[derive(Clone, Default, Deserialize, Serialize)]
pub struct Publish {
    #[serde(flatten)]
    pub message: Message,
    pub channels: Option<Vec<Selector>>,
    pub strategy: Option<Strategy>,
    /// UTC milliseconds since the Unix epoch.
    pub expire_at: Option<i64>,
    pub callback_url: Option<String>,
    #[serde(default, skip_serializing)]
    pub topic_password: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Receipt {
    pub message_id: String,
    pub status: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Page<T> {
    pub items: Vec<T>,
    pub next_cursor: Option<String>,
}

/// Registered notification targets for the authenticated LazyCat platform user.
#[derive(Debug, Deserialize, Serialize)]
pub struct LazycatDevices {
    pub user_id: String,
    pub devices: Vec<LazycatDevice>,
    pub max_selection: Option<usize>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub configured_device_ids: Vec<String>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct LazycatDevice {
    pub device_id: String,
    pub name: String,
    pub online: bool,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Topic {
    pub name: String,
    #[serde(default)]
    pub description: String,
    #[serde(default = "enabled")]
    pub enabled: bool,
    #[serde(default)]
    pub default_tags: Vec<String>,
    pub retention: Option<u32>,
    #[serde(default)]
    pub policy: Policy,
    #[serde(default)]
    pub protected: bool,
}

fn enabled() -> bool {
    true
}

#[derive(Clone, Deserialize, Serialize)]
pub struct SubscriptionInput {
    pub subscriber_ref: String,
    pub account_id: Option<String>,
    pub target: Option<Value>,
    #[serde(default, skip_serializing)]
    pub password: Option<String>,
    #[serde(default)]
    pub filter: Vec<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Subscription {
    pub user_id: Option<String>,
    pub id: String,
    pub subscriber_ref: String,
    pub account_id: Option<String>,
    pub filter: Vec<String>,
    pub created_at: i64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Attempt {
    pub n: i64,
    pub started_at: i64,
    pub finished_at: Option<i64>,
    pub error_code: Option<String>,
    pub error_msg: Option<String>,
    pub latency: Option<i64>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Delivery {
    pub id: String,
    pub channel: String,
    pub account_id: String,
    /// Current account name for administrators with global account access.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub account_name: Option<String>,
    pub subscription_id: Option<String>,
    pub status: String,
    pub next_retry_at: Option<i64>,
    pub attempts: Vec<Attempt>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct MessageDetail {
    pub id: String,
    pub topic: Option<String>,
    pub status: String,
    pub message: Message,
    pub created_at: i64,
    /// Number of native inboxes the server wrote to at send time.
    #[serde(default)]
    pub inbox_deliveries: i64,
    /// Destination username supplied by the sender of a direct inbox notification.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub inbox_recipient: Option<String>,
    pub deliveries: Vec<Delivery>,
}
