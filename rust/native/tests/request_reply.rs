use dynamic_asyncapi_client::{
    Action, Code, Document, ExchangeOptions, ExchangePlan, Json, PlanOptions,
};
use dynamic_asyncapi_native::{Receipt, RuntimeCode, SessionOptions, request_reply};
use dynamic_asyncapi_session::SessionPlan;
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use std::time::Duration;
use tokio::{net::TcpListener, sync::oneshot, time::timeout};
use tokio_tungstenite::tungstenite::Message;

fn source(port: u16, edition: &str) -> Value {
    json!({"asyncapi":edition,"info":{"title":"request reply","version":"1"},
        "servers":{"s":{"host":format!("127.0.0.1:{port}"),"protocol":"ws"}},
        "channels":{
            "requests":{"address":"/rpc","messages":{"request":{"contentType":"application/json","correlationId":{"location":"$message.payload#/requestId"}}}},
            "replies":{"address":"/rpc","messages":{"reply":{"contentType":"application/json","correlationId":{"location":"$message.payload#/responseId"}}}}
        },
        "operations":{"ask":{"action":"send","channel":{"$ref":"#/channels/requests"},"reply":{"channel":{"$ref":"#/channels/replies"},"messages":[{"$ref":"#/channels/replies/messages/reply"}]}}}
    })
}
fn options(ms: u32) -> ExchangeOptions {
    ExchangeOptions::first(PlanOptions::application(), PlanOptions::application(), ms)
}
fn prepare(source: &Value, ms: u32) -> Result<ExchangePlan, dynamic_asyncapi_client::Diagnostic> {
    Document::parse(&source.to_string())?
        .operation_id("ask")?
        .compile()?
        .prepare_exchange(&options(ms))
}
fn value(text: &str) -> Json {
    Json::parse(text, Default::default()).unwrap()
}

#[test]
fn declared_reply_keeps_native_identity_and_exact_correlation_policy() {
    for edition in ["3.0.0", "3.1.0"] {
        let doc = Document::parse(&source(80, edition).to_string()).unwrap();
        let operation = doc.operation_id("ask").unwrap().compile().unwrap();
        assert!(operation.prepare(&PlanOptions::application()).is_err());
        let plan = operation.prepare_exchange(&options(1000)).unwrap();
        assert_eq!(
            plan.request().describe().identity,
            plan.reply().describe().identity
        );
        assert_eq!(plan.reply().describe().application_action, Action::Receive);
        assert_eq!(
            plan.describe()
                .declaration
                .channel
                .as_ref()
                .unwrap()
                .pointer,
            "/channels/replies"
        );
        assert!(SessionPlan::new(&[plan.request().clone(), plan.reply().clone()]).is_err());
        assert!(SessionPlan::exchange(&plan).is_ok());
        let key = plan
            .request_key(&value(r#"{"requestId":"\u0061"}"#))
            .unwrap();
        assert!(plan.matches_reply(&key, &value(r#"{"responseId":"a"}"#)));
        for bad in [
            r#"{}"#,
            r#"{"requestId":null}"#,
            r#"{"requestId":1}"#,
            r#"{"requestId":""}"#,
        ] {
            assert!(plan.request_key(&value(bad)).is_err());
        }
        for mismatch in [
            r#"{}"#,
            r#"{"responseId":null}"#,
            r#"{"responseId":1}"#,
            r#"{"responseId":"b"}"#,
        ] {
            assert!(!plan.matches_reply(&key, &value(mismatch)));
        }
        drop(operation);
        drop(doc);
        assert!(plan.matches_reply(&key, &value(r#"{"responseId":"a"}"#)));
    }
}

#[test]
fn unsupported_or_invalid_reply_semantics_refuse_during_preparation() {
    let original = source(80, "3.1.0");
    type Mutation = Box<dyn Fn(&mut Value)>;
    let cases: Vec<Mutation> = vec![
        Box::new(|s| s["operations"]["ask"]["reply"]["channel"] = json!({"address":"/rpc"})),
        Box::new(|s| {
            s["operations"]["ask"]["reply"]["messages"] =
                json!([{"$ref":"#/channels/requests/messages/request"}])
        }),
        Box::new(|s| s["operations"]["ask"]["reply"]["messages"] = json!([])),
        Box::new(|s| {
            s["operations"]["ask"]["reply"]["address"] =
                json!({"location":"$message.payload#/replyTo"})
        }),
        Box::new(|s| s["channels"]["replies"]["address"] = json!("/other")),
        Box::new(|s| {
            s["channels"]["replies"]["messages"]["reply"]["correlationId"]["location"] =
                json!("$message.header#/id")
        }),
        Box::new(|s| {
            s["channels"]["replies"]["messages"]["reply"]
                .as_object_mut()
                .unwrap()
                .remove("correlationId");
        }),
        Box::new(|s| {
            s["channels"]["replies"]["messages"]["reply"]["payload"] = json!({"type":"object"})
        }),
        Box::new(|s| s["operations"]["ask"]["action"] = json!("receive")),
        Box::new(|s| s["servers"]["s"]["protocol"] = json!("mqtt")),
    ];
    for (i, change) in cases.into_iter().enumerate() {
        let mut s = original.clone();
        change(&mut s);
        assert!(prepare(&s, 1000).is_err(), "case {i} unexpectedly admitted");
    }
    for ms in [0, 300001] {
        assert_eq!(
            prepare(&original, ms).unwrap_err().code,
            Code::InvalidConfiguration
        );
    }
}

#[test]
fn reply_and_message_traits_preserve_declared_locations() {
    let mut s = source(80, "3.1.0");
    s["components"] = json!({
        "replies":{"result":s["operations"]["ask"]["reply"].clone()},
        "messageTraits":{"correlated":{"correlationId":{"location":"$message.payload#/responseId"}}}
    });
    s["operations"]["ask"]["reply"] = json!({"$ref":"#/components/replies/result"});
    let message = &mut s["channels"]["replies"]["messages"]["reply"];
    message.as_object_mut().unwrap().remove("correlationId");
    message["traits"] = json!([{"$ref":"#/components/messageTraits/correlated"}]);
    let doc = Document::parse(&s.to_string()).unwrap();
    let op = doc.operation_id("ask").unwrap().compile().unwrap();
    let reply = op.reply().unwrap().unwrap();
    assert_eq!(
        reply.describe().definition.pointer,
        "/components/replies/result"
    );
    assert_eq!(
        reply
            .correlation("reply")
            .unwrap()
            .unwrap()
            .location
            .pointer,
        "/components/messageTraits/correlated/correlationId/location"
    );
    assert!(op.prepare_exchange(&options(1000)).is_ok());
}

#[tokio::test]
async fn request_reply_round_trip_filters_unrelated_messages_and_preserves_exact_result() {
    for edition in ["3.0.0", "3.1.0"] {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let plan = prepare(
            &source(listener.local_addr().unwrap().port(), edition),
            2000,
        )
        .unwrap();
        let peer = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let mut ws = tokio_tungstenite::accept_async(socket).await.unwrap();
            let request = ws.next().await.unwrap().unwrap();
            assert_eq!(
                request.into_text().unwrap(),
                r#"{"requestId":"r1","input":900719925474099312345}"#
            );
            for reply in [
                r#"{"responseId":1}"#,
                r#"{"responseId":"other"}"#,
                r#"{"responseId":"r1","result":900719925474099312345}"#,
            ] {
                ws.send(Message::Text(reply.into())).await.unwrap();
            }
            assert!(matches!(
                ws.next().await.unwrap().unwrap(),
                Message::Close(_)
            ));
            ws.flush().await.unwrap();
        });
        let request = value(r#"{"requestId":"r1","input":900719925474099312345}"#);
        let response = request_reply(&plan, &request, SessionOptions::default())
            .await
            .unwrap();
        assert_eq!(response.request_receipt, Receipt::WebSocketFlushed);
        assert_eq!(response.unmatched_replies, 2);
        assert!(response.close.is_ok());
        assert_eq!(
            response
                .reply
                .payload
                .as_json()
                .unwrap()
                .get("result")
                .unwrap()
                .number_text(),
            Some("900719925474099312345")
        );
        peer.await.unwrap();
    }
}

#[tokio::test]
async fn invalid_request_is_rejected_before_connecting() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let plan = prepare(
        &source(listener.local_addr().unwrap().port(), "3.1.0"),
        1000,
    )
    .unwrap();
    let error = request_reply(
        &plan,
        &value(r#"{"requestId":null}"#),
        SessionOptions::default(),
    )
    .await
    .unwrap_err();
    assert_eq!(error.error.code, RuntimeCode::InvalidPayload);
    assert!(!error.error.delivery_unknown);
    assert!(error.request_receipt.is_none());
    assert!(
        timeout(Duration::from_millis(30), listener.accept())
            .await
            .is_err()
    );
}

#[tokio::test]
async fn timeout_preserves_send_evidence_and_releases_connection() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let plan = prepare(&source(listener.local_addr().unwrap().port(), "3.1.0"), 250).unwrap();
    let peer = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let mut ws = tokio_tungstenite::accept_async(socket).await.unwrap();
        assert!(ws.next().await.unwrap().unwrap().is_text());
        // The timeout must close the socket, even though this peer never replies.
        let ended = timeout(Duration::from_secs(2), ws.next()).await.unwrap();
        assert!(ended.is_none() || ended.unwrap().is_err());
    });
    let error = request_reply(
        &plan,
        &value(r#"{"requestId":"timeout"}"#),
        SessionOptions::default(),
    )
    .await
    .unwrap_err();
    assert_eq!(error.error.code, RuntimeCode::Deadline);
    assert_eq!(error.request_receipt, Some(Receipt::WebSocketFlushed));
    peer.await.unwrap();
}

#[tokio::test]
async fn cancellation_after_send_releases_connection() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let plan = prepare(
        &source(listener.local_addr().unwrap().port(), "3.1.0"),
        5000,
    )
    .unwrap();
    let (sent, seen) = oneshot::channel();
    let peer = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let mut ws = tokio_tungstenite::accept_async(socket).await.unwrap();
        assert!(ws.next().await.unwrap().unwrap().is_text());
        sent.send(()).unwrap();
        let ended = timeout(Duration::from_secs(2), ws.next()).await.unwrap();
        assert!(ended.is_none() || ended.unwrap().is_err());
    });
    let call = tokio::spawn(async move {
        request_reply(
            &plan,
            &value(r#"{"requestId":"cancel"}"#),
            SessionOptions::default(),
        )
        .await
    });
    seen.await.unwrap();
    call.abort();
    assert!(call.await.unwrap_err().is_cancelled());
    peer.await.unwrap();
}

#[tokio::test]
async fn malformed_reply_retains_send_evidence() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let plan = prepare(
        &source(listener.local_addr().unwrap().port(), "3.1.0"),
        2000,
    )
    .unwrap();
    let peer = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let mut ws = tokio_tungstenite::accept_async(socket).await.unwrap();
        ws.next().await.unwrap().unwrap();
        ws.send(Message::Text("{".into())).await.unwrap();
        let _ = ws.next().await;
    });
    let error = request_reply(
        &plan,
        &value(r#"{"requestId":"invalid"}"#),
        SessionOptions::default(),
    )
    .await
    .unwrap_err();
    assert_eq!(error.error.code, RuntimeCode::InvalidPayload);
    assert_eq!(error.request_receipt, Some(Receipt::WebSocketFlushed));
    peer.await.unwrap();
}

#[tokio::test]
async fn concurrent_calls_with_the_same_id_are_isolated_by_connection() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let plan = prepare(
        &source(listener.local_addr().unwrap().port(), "3.1.0"),
        3000,
    )
    .unwrap();
    let peer = tokio::spawn(async move {
        let mut peers = Vec::new();
        for _ in 0..2 {
            let (socket, _) = listener.accept().await.unwrap();
            peers.push(tokio::spawn(async move {
                let mut ws = tokio_tungstenite::accept_async(socket).await.unwrap();
                let request = ws.next().await.unwrap().unwrap().into_text().unwrap();
                let request: Value = serde_json::from_str(&request).unwrap();
                ws.send(Message::Text(
                    json!({"responseId":"same","result":request["input"]})
                        .to_string()
                        .into(),
                ))
                .await
                .unwrap();
                assert!(matches!(
                    ws.next().await.unwrap().unwrap(),
                    Message::Close(_)
                ));
                ws.flush().await.unwrap();
            }));
        }
        for peer in peers {
            peer.await.unwrap();
        }
    });
    let a = value(r#"{"requestId":"same","input":"A"}"#);
    let b = value(r#"{"requestId":"same","input":"B"}"#);
    let (a, b) = tokio::join!(
        request_reply(&plan, &a, SessionOptions::default()),
        request_reply(&plan, &b, SessionOptions::default())
    );
    for (response, expected) in [(a, "A"), (b, "B")] {
        let response = response.unwrap();
        assert_eq!(
            response
                .reply
                .payload
                .as_json()
                .unwrap()
                .get("result")
                .unwrap()
                .as_str(),
            Some(expected)
        );
        assert!(response.close.is_ok());
    }
    peer.await.unwrap();
}

#[tokio::test]
async fn unmatched_reply_work_is_bounded() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let plan = prepare(
        &source(listener.local_addr().unwrap().port(), "3.1.0"),
        2000,
    )
    .unwrap();
    let peer = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let mut ws = tokio_tungstenite::accept_async(socket).await.unwrap();
        ws.next().await.unwrap().unwrap();
        for _ in 0..65 {
            ws.send(Message::Text(r#"{"responseId":"other"}"#.into()))
                .await
                .unwrap();
            // Pace this peer to exercise the total-work limit rather than the
            // independent instantaneous receive-queue capacity.
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
        let _ = ws.next().await;
    });
    let options = SessionOptions::default();
    let error = request_reply(&plan, &value(r#"{"requestId":"bounded"}"#), options)
        .await
        .unwrap_err();
    assert_eq!(error.error.code, RuntimeCode::Backpressure);
    assert_eq!(error.error.detail, "exchange exceeded 64 unmatched replies");
    assert_eq!(error.request_receipt, Some(Receipt::WebSocketFlushed));
    peer.await.unwrap();
}

#[tokio::test]
async fn valid_reply_survives_graceful_close_deadline() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let plan = prepare(&source(listener.local_addr().unwrap().port(), "3.1.0"), 250).unwrap();
    let peer = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let mut ws = tokio_tungstenite::accept_async(socket).await.unwrap();
        ws.next().await.unwrap().unwrap();
        ws.send(Message::Text(r#"{"responseId":"close"}"#.into()))
            .await
            .unwrap();
        // Do not read/acknowledge the client's close until after its deadline.
        tokio::time::sleep(Duration::from_millis(400)).await;
    });
    let response = request_reply(
        &plan,
        &value(r#"{"requestId":"close"}"#),
        SessionOptions::default(),
    )
    .await
    .unwrap();
    assert_eq!(response.close.unwrap_err().code, RuntimeCode::Deadline);
    assert_eq!(
        response
            .reply
            .payload
            .as_json()
            .unwrap()
            .get("responseId")
            .unwrap()
            .as_str(),
        Some("close")
    );
    peer.await.unwrap();
}
