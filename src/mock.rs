//! Loopback HTTP fixture for adapter integration tests.

use std::time::Duration;

use reqwest::Url;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::task::JoinHandle;

pub struct Request {
    pub headers: String,
    pub body: Vec<u8>,
}

/// Listens for a single request and returns it to the test while the transport
/// sees a real HTTP response. `status` includes the reason phrase.
pub async fn serve(
    status: &'static str,
    extra_headers: &'static str,
    body: &'static str,
) -> (Url, JoinHandle<Request>) {
    let (listener, url) = listen().await;
    let task = tokio::spawn(async move {
        tokio::time::timeout(
            Duration::from_secs(5),
            receive(&listener, (status, extra_headers, body)),
        )
        .await
        .unwrap()
    });
    (url, task)
}

/// Listens for a series of independent requests on one loopback endpoint.
pub async fn serve_sequence(
    responses: &'static [(&'static str, &'static str, &'static str)],
) -> (Url, JoinHandle<Vec<Request>>) {
    let (listener, url) = listen().await;
    let task = tokio::spawn(async move {
        tokio::time::timeout(Duration::from_secs(5), async {
            let mut requests = Vec::with_capacity(responses.len());
            for &response in responses {
                requests.push(receive(&listener, response).await);
            }
            requests
        })
        .await
        .unwrap()
    });
    (url, task)
}

async fn listen() -> (TcpListener, Url) {
    let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let url = Url::parse(&format!("http://{}/", listener.local_addr().unwrap())).unwrap();
    (listener, url)
}

async fn receive(
    listener: &TcpListener,
    (status, extra_headers, body): (&str, &str, &str),
) -> Request {
    let (mut stream, _) = listener.accept().await.unwrap();
    let mut received = Vec::new();
    let mut chunk = [0u8; 2048];
    let header_end = loop {
        let length = stream.read(&mut chunk).await.unwrap();
        assert_ne!(length, 0, "request ended before headers");
        received.extend_from_slice(&chunk[..length]);
        if let Some(index) = received.windows(4).position(|item| item == b"\r\n\r\n") {
            break index + 4;
        }
        assert!(received.len() <= 64 * 1024);
    };
    let headers = String::from_utf8(received[..header_end].to_vec()).unwrap();
    let content_length = headers
        .lines()
        .find_map(|line| {
            let (name, value) = line.split_once(':')?;
            if name.eq_ignore_ascii_case("content-length") {
                return Some(value.trim().parse::<usize>().unwrap());
            }
            None
        })
        .unwrap_or_default();
    let mut request_body = received[header_end..].to_vec();
    let consumed = request_body.len();
    if consumed < content_length {
        request_body.resize(content_length, 0);
        stream
            .read_exact(&mut request_body[consumed..])
            .await
            .unwrap();
    }
    let response = format!(
        "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n{extra_headers}\r\n{body}",
        body.len(),
    );
    stream.write_all(response.as_bytes()).await.unwrap();
    Request {
        headers,
        body: request_body,
    }
}
