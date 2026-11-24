use crate::mock;
use serde_json::{Value, json};

use super::*;

const RATE_LIMIT: &str =
    r#"{"code":"slow_down","message":"Try again later","details":{"limit":10}}"#;
const RECEIPT: &str = r#"{"message_id":"fixture-id","status":"pending"}"#;
const SESSION: &str = r#"{"token":"session-fixture","user":{"id":"u1","username":"alice","admin":false,"enabled":true,"created_at":1000}}"#;
const SUBSCRIPTION: &str =
    r#"{"id":"sub1","subscriber_ref":"alice","account_id":null,"filter":[],"created_at":1000}"#;

fn fixture_client(base: &str) -> Client {
    Client::builder(base)
        .unwrap()
        .api_key("fixture-api-key")
        .unwrap()
        .retry(RetryConfig {
            max_retries: 2,
            base_delay: Duration::ZERO,
            max_delay: Duration::ZERO,
        })
        .build()
        .unwrap()
}

fn header<'a>(request: &'a mock::Request, name: &str) -> Option<&'a str> {
    request.headers.lines().find_map(|line| {
        let (key, value) = line.split_once(':')?;
        key.eq_ignore_ascii_case(name).then(|| value.trim())
    })
}

#[tokio::test]
async fn retries_a_protected_publish_with_one_idempotency_key() {
    let (mut base, requests) = mock::serve_sequence(&[
        ("429 Too Many Requests", "Retry-After: 0\r\n", RATE_LIMIT),
        ("202 Accepted", "", RECEIPT),
    ])
    .await;
    base.set_path("/proxy/anser/");
    let client = fixture_client(base.as_str());
    let publish = Publish {
        message: Message {
            title: "雁字".into(),
            body: "通知".into(),
            ..Message::default()
        },
        topic_password: Some("topic-pass-fixture".into()),
        ..Publish::default()
    };
    let result = client
        .publish_with_options(
            "status",
            &publish,
            &SendOptions {
                idempotency_key: None,
                wait: true,
                wait_timeout: Some(Duration::from_secs(4)),
            },
        )
        .await
        .unwrap();
    assert_eq!(result.message_id, "fixture-id");
    let requests = requests.await.unwrap();
    assert_eq!(requests.len(), 2);
    let key = header(&requests[0], "idempotency-key").unwrap();
    assert!(uuid::Uuid::parse_str(key).is_ok());
    for request in &requests {
        assert!(
            request.headers.starts_with(
                "POST /proxy/anser/v1/topics/status/publish?wait=true&timeout_ms=4000 "
            )
        );
        assert_eq!(
            header(request, "authorization"),
            Some("Bearer fixture-api-key")
        );
        assert_eq!(header(request, "idempotency-key"), Some(key));
        let body: Value = serde_json::from_slice(&request.body).unwrap();
        assert_eq!(body["topic_password"], "topic-pass-fixture");
        assert_eq!(body["title"], "雁字");
    }
    assert_eq!(requests[0].body, requests[1].body);
}

#[tokio::test]
async fn native_send_supports_legacy_receipts_and_opt_in_idempotent_retries() {
    let (base, requests) = mock::serve_sequence(&[
        ("201 Created", "", r#"{"id":1}"#),
        ("429 Too Many Requests", "Retry-After: 0\r\n", RATE_LIMIT),
        ("201 Created", "", r#"{"id":2,"message_id":"trace-id"}"#),
    ])
    .await;
    let client = fixture_client(base.as_str());
    let message = Message {
        title: "Arrived".into(),
        body: "Train 24 is here".into(),
        ..Message::default()
    };
    let legacy = client.send_inbox("bob", &message).await.unwrap();
    assert_eq!(legacy.id, 1);
    assert!(legacy.message_id.is_none());
    let receipt = client
        .send_inbox_idempotent("bob", &message, "train-24")
        .await
        .unwrap();
    assert_eq!(receipt.message_id.as_deref(), Some("trace-id"));
    let requests = requests.await.unwrap();
    assert_eq!(header(&requests[0], "idempotency-key"), None);
    for request in &requests[1..] {
        assert!(request.headers.starts_with("POST /v1/inbox/send "));
        assert_eq!(header(request, "idempotency-key"), Some("train-24"));
        let input: Value = serde_json::from_slice(&request.body).unwrap();
        assert_eq!(input["recipient"], "bob");
    }
}

#[tokio::test]
async fn keeps_path_segments_and_api_errors_typed() {
    let (base, request) = mock::serve(
        "404 Not Found",
        "",
        r#"{"code":"missing_message","message":"Unknown ID","details":{"reason":"message"}}"#,
    )
    .await;
    let error = fixture_client(base.as_str())
        .message("odd/path?value")
        .await
        .unwrap_err();
    let Error::Api(error) = error else {
        panic!("expected an API error")
    };
    assert_eq!(error.status, reqwest::StatusCode::NOT_FOUND);
    assert_eq!(error.code, "missing_message");
    assert_eq!(error.message, "Unknown ID");
    assert_eq!(error.details["reason"], "message");
    assert!(
        request
            .await
            .unwrap()
            .headers
            .starts_with("GET /v1/messages/odd%2Fpath%3Fvalue ")
    );
}

#[tokio::test]
async fn serializes_subscription_password_and_does_not_send_it_when_unsubscribing() {
    let (base, requests) = mock::serve_sequence(&[
        ("201 Created", "", SUBSCRIPTION),
        ("204 No Content", "", ""),
    ])
    .await;
    let client = fixture_client(base.as_str());
    let subscription = client
        .subscribe(
            "protected",
            &SubscriptionInput {
                subscriber_ref: "alice".into(),
                account_id: None,
                target: None,
                password: Some("password-fixture".into()),
                filter: Vec::new(),
            },
        )
        .await
        .unwrap();
    client
        .unsubscribe("protected", &subscription.id)
        .await
        .unwrap();
    let requests = requests.await.unwrap();
    let input: Value = serde_json::from_slice(&requests[0].body).unwrap();
    assert_eq!(input["password"], "password-fixture");
    assert!(
        requests[1]
            .headers
            .starts_with("DELETE /v1/topics/protected/subscriptions/sub1 ")
    );
    assert!(requests[1].body.is_empty());
}

#[tokio::test]
async fn login_and_pair_exchange_use_their_public_endpoints_without_forwarding_a_key() {
    let (base, requests) =
        mock::serve_sequence(&[("200 OK", "", SESSION), ("200 OK", "", SESSION)]).await;
    let client = fixture_client(base.as_str());
    let login = client.login("alice", "account-pass-fixture").await.unwrap();
    let pair = client.exchange_pair_code("pair-fixture").await.unwrap();
    assert_eq!(login.token, "session-fixture");
    assert_eq!(pair.user.username, "alice");
    let requests = requests.await.unwrap();
    assert!(requests[0].headers.starts_with("POST /v1/auth/login "));
    assert!(
        requests[1]
            .headers
            .starts_with("POST /v1/auth/pair/exchange ")
    );
    assert!(
        requests
            .iter()
            .all(|request| header(request, "authorization").is_none())
    );
    let login: Value = serde_json::from_slice(&requests[0].body).unwrap();
    let exchange: Value = serde_json::from_slice(&requests[1].body).unwrap();
    assert_eq!(
        login,
        json!({"username":"alice","password":"account-pass-fixture"})
    );
    assert_eq!(exchange, json!({"code":"pair-fixture"}));
}

#[tokio::test]
async fn never_retries_a_one_time_pair_exchange() {
    let (base, request) = mock::serve(
        "503 Service Unavailable",
        "",
        r#"{"code":"later","message":"Try again later"}"#,
    )
    .await;
    let result = fixture_client(base.as_str())
        .exchange_pair_code("pair-fixture")
        .await;
    assert!(
        matches!(result, Err(Error::Api(error)) if error.status == reqwest::StatusCode::SERVICE_UNAVAILABLE)
    );
    request.await.unwrap();
}

#[tokio::test]
async fn does_not_follow_cross_origin_redirects() {
    let (base, request) =
        mock::serve("302 Found", "Location: http://127.0.0.1:9/other\r\n", "").await;
    let result = fixture_client(base.as_str()).message("fixture-id").await;
    assert!(matches!(result, Err(Error::Api(error)) if error.status == reqwest::StatusCode::FOUND));
    assert_eq!(
        header(&request.await.unwrap(), "authorization"),
        Some("Bearer fixture-api-key")
    );
}

#[tokio::test]
async fn discovers_devices_without_supplying_a_platform_identity() {
    let (base, request) = mock::serve(
        "200 OK",
        "",
        r#"{"user_id":"alice","devices":[{"device_id":"d1","name":"Laptop","online":true}]}"#,
    )
    .await;
    let devices = fixture_client(base.as_str())
        .lazycat_devices()
        .await
        .unwrap();
    assert_eq!(devices.user_id, "alice");
    assert_eq!(devices.devices[0].device_id, "d1");
    assert!(devices.devices[0].online);
    let request = request.await.unwrap();
    assert!(
        request
            .headers
            .starts_with("GET /v1/channels/lazycat/devices ")
    );
    assert!(header(&request, "x-anser-lazycat-uid").is_none());
    assert!(header(&request, "x-anser-lazycat-proof").is_none());
}

#[tokio::test]
async fn reads_inbox_items_and_encodes_cursor() {
    let (base, request) = mock::serve("200 OK", "", r#"{"items":[{"id":42,"from_username":"bob","topic":"alerts","title":"T","body":"B","format":"text","created_at":1000,"read_at":null}],"next_cursor":"43"}"#).await;
    let page = fixture_client(base.as_str())
        .inbox(&PageOptions {
            cursor: Some("opaque/?".into()),
            limit: Some(4),
        })
        .await
        .unwrap();
    assert_eq!(page.items[0].id, 42);
    assert_eq!(page.items[0].from_username.as_deref(), Some("bob"));
    assert_eq!(page.next_cursor.as_deref(), Some("43"));
    let request = request.await.unwrap();
    assert!(
        request
            .headers
            .starts_with("GET /v1/inbox?cursor=opaque%2F%3F&limit=4 ")
    );
}

#[tokio::test]
async fn totp_methods_send_codes_only_to_their_intended_endpoints() {
    let (base, requests) = mock::serve_sequence(&[
        ("200 OK", "", SESSION),
        ("200 OK", "", r#"{"enabled":false}"#),
        (
            "200 OK",
            "",
            r#"{"secret":"fixture","uri":"otpauth://totp/Anser:alice","expires_at":123}"#,
        ),
        (
            "200 OK",
            "",
            r#"{"enabled":true,"recovery_codes":["recovery-fixture"]}"#,
        ),
        ("200 OK", "", r#"{"enabled":false}"#),
    ])
    .await;
    let client = fixture_client(base.as_str());
    assert_eq!(
        client
            .login_with_code("alice", "fixture-pass", "123456")
            .await
            .unwrap()
            .token,
        "session-fixture"
    );
    assert!(!client.totp_status().await.unwrap().enabled);
    assert_eq!(
        client.setup_totp("fixture-pass").await.unwrap().expires_at,
        123
    );
    assert_eq!(
        client.confirm_totp("123456").await.unwrap().recovery_codes,
        ["recovery-fixture"]
    );
    assert!(
        !client
            .disable_totp("fixture-pass", "recovery-fixture")
            .await
            .unwrap()
            .enabled
    );

    let requests = requests.await.unwrap();
    assert!(requests[0].headers.starts_with("POST /v1/auth/login "));
    assert_eq!(header(&requests[0], "authorization"), None);
    let login: Value = serde_json::from_slice(&requests[0].body).unwrap();
    assert_eq!(
        login,
        json!({"username":"alice", "password":"fixture-pass", "code":"123456"})
    );
    for (request, endpoint) in requests[1..].iter().zip([
        "GET /v1/auth/totp ",
        "POST /v1/auth/totp/setup ",
        "POST /v1/auth/totp/confirm ",
        "POST /v1/auth/totp/disable ",
    ]) {
        assert!(request.headers.starts_with(endpoint));
        assert_eq!(
            header(request, "authorization"),
            Some("Bearer fixture-api-key")
        );
    }
    let disable: Value = serde_json::from_slice(&requests[4].body).unwrap();
    assert_eq!(
        disable,
        json!({"password":"fixture-pass", "code":"recovery-fixture"})
    );
}

#[tokio::test]
async fn publishes_a_template_with_no_source_and_reuses_the_key() {
    let (base, requests) = mock::serve_sequence(&[
        ("429 Too Many Requests", "Retry-After: 0\r\n", RATE_LIMIT),
        ("202 Accepted", "", RECEIPT),
    ])
    .await;
    let client = fixture_client(base.as_str());
    let mut publish = TemplatePublish::new(
        TemplateRef::TemplateName("builtin.deployment".into()),
        [("service".into(), "api".into())].into(),
    );
    publish.topic_password = Some("fixture-password".into());
    client
        .publish_template("alerts", &publish, &SendOptions::default())
        .await
        .unwrap();
    let requests = requests.await.unwrap();
    assert_eq!(
        header(&requests[0], "idempotency-key"),
        header(&requests[1], "idempotency-key")
    );
    assert_eq!(requests[0].body, requests[1].body);
    let body: Value = serde_json::from_slice(&requests[0].body).unwrap();
    assert_eq!(
        body,
        json!({
            "template_name":"builtin.deployment","variables":{"service":"api"},"topic_password":"fixture-password"
        })
    );
}

#[tokio::test]
async fn manages_templates_over_the_versioned_paths() {
    const TEMPLATE: &str = r#"{"id":"a/b","name":"deploy","title":"Update","body":"{{service}}","format":"text","variables":["service"],"shared":false,"builtin":false,"can_edit":true,"created_at":1000,"updated_at":1000}"#;
    let (base, requests) = mock::serve_sequence(&[
        ("200 OK", "", r#"{"items":[],"next_cursor":null}"#),
        ("201 Created", "", TEMPLATE),
        ("200 OK", "", TEMPLATE),
        ("200 OK", "", TEMPLATE),
        ("204 No Content", "", ""),
    ])
    .await;
    let client = fixture_client(base.as_str());
    let input = TemplateInput {
        name: "deploy".into(),
        title: "Update".into(),
        body: "{{service}}".into(),
        format: TemplateFormat::Text,
        shared: None,
    };
    assert!(
        client
            .templates(&PageOptions::default())
            .await
            .unwrap()
            .items
            .is_empty()
    );
    let template = client.create_template(&input).await.unwrap();
    assert_eq!(
        client.template(&template.id).await.unwrap().variables,
        ["service"]
    );
    assert_eq!(
        client
            .update_template(&template.id, &input)
            .await
            .unwrap()
            .name,
        "deploy"
    );
    client.delete_template(&template.id).await.unwrap();
    let requests = requests.await.unwrap();
    for (request, method) in requests[2..].iter().zip(["GET", "PUT", "DELETE"]) {
        assert!(
            request
                .headers
                .starts_with(&format!("{method} /v1/templates/a%2Fb "))
        );
    }
}

#[tokio::test]
async fn sends_speech_with_an_explicit_idempotency_key() {
    let (base, request) = mock::serve("201 Created", "", RECEIPT).await;
    let client = fixture_client(base.as_str());
    let input = SpeechSend {
        config: SpeechConfig {
            server_url: "https://speech.example".into(),
            api_key: "fixture-speech".into(),
            model: "tts-1".into(),
            voice: "alloy".into(),
        },
        message: Message {
            body: "Build ready".into(),
            ..Message::default()
        },
        destination: SpeechDestination::Inbox {
            username: "alice".into(),
        },
    };
    let receipt = client.send_speech(&input, Some("speech-17")).await.unwrap();
    assert_eq!(receipt.message_id, "fixture-id");
    let request = request.await.unwrap();
    assert!(request.headers.starts_with("POST /v1/speech/send "));
    assert_eq!(header(&request, "Idempotency-Key"), Some("speech-17"));
    let body: Value = serde_json::from_slice(&request.body).unwrap();
    assert_eq!(
        body["destination"],
        json!({"kind":"inbox","username":"alice"})
    );
    assert_eq!(body["config"]["api_key"], "fixture-speech");
}
