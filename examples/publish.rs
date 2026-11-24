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
