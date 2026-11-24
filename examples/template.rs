use anser_sdk::{Client, SendOptions, TemplatePublish, TemplateRef};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = Client::new(
        &std::env::var("ANSER_URL")?,
        &std::env::var("ANSER_API_KEY")?,
    )?;
    let mut message = TemplatePublish::new(
        TemplateRef::TemplateId("builtin.deployment".into()),
        [
            ("service".into(), "api".into()),
            ("version".into(), "1.2.3".into()),
            ("environment".into(), "production".into()),
        ]
        .into(),
    );
    message.topic_password = std::env::var("ANSER_TOPIC_PASSWORD").ok();
    let options = SendOptions {
        idempotency_key: std::env::var("ANSER_IDEMPOTENCY_KEY").ok(),
        ..SendOptions::default()
    };
    let receipt = client
        .publish_template("alerts", &message, &options)
        .await?;
    let detail = client.message(&receipt.message_id).await?;
    println!("{}: {}", receipt.message_id, detail.status);
    Ok(())
}
