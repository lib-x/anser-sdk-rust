# Anser SDK for Rust

Official Rust client for the [Anser](https://github.com/ca-x/anser) v1 notification API.

## Install

Requires Rust 1.94 or newer and a running Anser server.

```sh
cargo add anser-sdk@0.2.0
cargo add tokio --features macros,rt-multi-thread
```

## Authenticate

In the Anser admin console, open **API Keys → Create key**. Grant `send` for sending and reading delivery status, or `subscribe` for subscription management. Save the generated key to a secret manager when Anser displays it.

Set `ANSER_URL` to your installation URL, including any reverse-proxy path prefix and without `/v1`. Set `ANSER_API_KEY` from your secret manager. SDK requests use the `Authorization: Bearer` header. Use HTTPS for remote servers.

A key with a topic prefix can only act on matching topics. Direct sends require a key without a prefix. For the example, create and configure an `alerts` topic in Anser. If it is password-protected, also set `ANSER_TOPIC_PASSWORD`.

## Publish and look up delivery

Copy the example to `src/main.rs` and run `cargo run`.

```rust
use anser_sdk::{Client, Message, Publish};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = Client::new(
        &std::env::var("ANSER_URL")?,
        &std::env::var("ANSER_API_KEY")?,
    )?;
    let receipt = client
        .publish(
            "alerts",
            &Publish {
                message: Message {
                    title: "Build complete".into(),
                    body: "The release is ready.".into(),
                    ..Message::default()
                },
                topic_password: std::env::var("ANSER_TOPIC_PASSWORD").ok(),
                ..Publish::default()
            },
        )
        .await?;
    let detail = client.message(&receipt.message_id).await?;
    println!("{}: {}", receipt.message_id, detail.status);
    Ok(())
}
```

An HTTP `202` confirms that Anser queued the message. The message can still be pending when you first read it. Save the message ID and check the message detail and its delivery attempts as needed. Use the same key to query a message created with a non-admin key.

## Send with a template

Template methods require Anser 0.6.0 or later. Use `TemplateRef::TemplateId` or `TemplateRef::TemplateName` with `send_template` or `publish_template`. The server supplies the final title, body, and format. The [complete example](examples/template.rs) uses `builtin.deployment` and omits the template source. Use the same `ANSER_IDEMPOTENCY_KEY` if you retry across processes.

The SDK also lists, reads, creates, updates, and deletes templates. API keys with `send` can use built-in and shared templates. Private templates require the owner’s session, and creating a shared template requires a global administrator. See [the template guide](https://github.com/ca-x/anser/blob/main/docs/notification-templates.en.md).

## Send a voice notification

Use `send_speech` with a global administrator credential and an OpenAI-compatible speech configuration. The server renders an MP3 and sends it to an inbox, topic, or default route. Topic and channel voice delivery requires a reachable HTTPS `ANSER_PUBLIC_URL`. See [speech plugin options](https://github.com/ca-x/anser/tree/main/anser-plugins/openai-tts).

## API coverage

`send`, `publish`, `message`, `messages`, `subscribe`, `unsubscribe`. Direct send uses explicit selectors or the server's default route. Messages are paginated by cursor. A protected topic uses `topic_password` on publish and `password` on subscribe. Subscription targets tied to channel accounts require a global administrator.

The Rust client retries eligible operations within a bounded deadline. Each logical send reuses one idempotency key across attempts. Use `SendOptions::idempotency_key` with `send_with_options` or `publish_with_options` to persist the key across process restarts. It also exposes native inbox, session, pairing, TOTP, and LazyCat device APIs. Those endpoints can require a signed-in user or administrator. Server idempotency defaults to a 24-hour window per key.

[Anser documentation](https://ca-x.github.io/anser/?lang=en#sdk) · [简体中文使用说明](https://ca-x.github.io/anser/?lang=zh-CN#sdk)
