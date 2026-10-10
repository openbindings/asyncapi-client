//! Development lifecycle tests with disposable loopback peers. These cases are
//! implementation-authored and are not independent conformance qualification.
use dynamic_asyncapi_client::{Document, Plan, PlanOptions};
use dynamic_asyncapi_native::{
    CloseReceipt, Credentials, Delivery, Incoming, Receipt, RuntimeCode, Session, SessionOptions,
    SessionState,
};
use futures_util::{SinkExt, StreamExt};
use serde_json::json;
use std::time::Duration;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::oneshot,
    time::{sleep, timeout},
};
use tokio_tungstenite::{accept_async, tungstenite::Message};

fn plans(protocol: &str, port: u16, receive: bool) -> Vec<Plan> {
    plans_at_qos(protocol, port, receive, 1, 1)
}
fn plans_at_qos(
    protocol: &str,
    port: u16,
    receive: bool,
    send_qos: u8,
    receive_qos: u8,
) -> Vec<Plan> {
    let mut value = json!({"asyncapi":"3.1.0","info":{"title":"test","version":"1"},
        "servers":{"local":{"host":format!("127.0.0.1:{port}"),"protocol":protocol}},
        "channels":{"events":{"address":if protocol=="mqtt" {"fixture/events"}else{"/events"},"messages":{"event":{"contentType":"application/octet-stream"}}}},
        "operations":{"emit":{"action":"send","channel":{"$ref":"#/channels/events"}}}});
    if receive {
        value["operations"]["listen"] =
            json!({"action":"receive","channel":{"$ref":"#/channels/events"}});
    }
    if protocol == "mqtt" {
        value["servers"]["local"]["protocolVersion"] = json!("3.1.1");
        value["servers"]["local"]["bindings"] =
            json!({"mqtt":{"clientId":"test-client","keepAlive":1,"cleanSession":true}});
        for (id, operation) in value["operations"].as_object_mut().unwrap() {
            operation["bindings"] =
                json!({"mqtt":{"qos":if id == "emit" { send_qos } else { receive_qos }}});
        }
    }
    let document = Document::parse(&value.to_string()).unwrap();
    [Some("emit"), receive.then_some("listen")]
        .into_iter()
        .flatten()
        .map(|id| {
            document
                .operation_id(id)
                .unwrap()
                .compile()
                .unwrap()
                .prepare(&PlanOptions::application())
                .unwrap()
        })
        .collect()
}
async fn listener() -> (TcpListener, u16) {
    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = l.local_addr().unwrap().port();
    (l, port)
}
async fn packet(stream: &mut TcpStream) -> (u8, Vec<u8>) {
    let kind = stream.read_u8().await.unwrap();
    let mut count = 0usize;
    let mut multiplier = 1;
    for _ in 0..4 {
        let b = stream.read_u8().await.unwrap();
        count += usize::from(b & 127) * multiplier;
        if b & 128 == 0 {
            break;
        }
        multiplier *= 128;
    }
    assert!(count <= 1024 * 1024 + 4096);
    let mut bytes = vec![0; count];
    stream.read_exact(&mut bytes).await.unwrap();
    (kind, bytes)
}
fn publish(bytes: &[u8]) -> (u16, &[u8]) {
    let n = usize::from(u16::from_be_bytes([bytes[0], bytes[1]]));
    (
        u16::from_be_bytes([bytes[n + 2], bytes[n + 3]]),
        &bytes[n + 4..],
    )
}
async fn ack(stream: &mut TcpStream, id: u16) {
    stream
        .write_all(&[0x40, 2, (id >> 8) as u8, id as u8])
        .await
        .unwrap();
}
async fn mqtt_ready(listener: TcpListener) -> TcpStream {
    let (mut stream, _) = listener.accept().await.unwrap();
    assert_eq!(packet(&mut stream).await.0, 0x10);
    stream.write_all(&[0x20, 2, 0, 0]).await.unwrap();
    stream
}
async fn socket_closed(stream: &mut TcpStream) {
    let mut byte = [0];
    match timeout(Duration::from_secs(2), stream.read(&mut byte))
        .await
        .unwrap()
    {
        Ok(0) => {}
        Err(e)
            if matches!(
                e.kind(),
                std::io::ErrorKind::ConnectionReset | std::io::ErrorKind::ConnectionAborted
            ) => {}
        result => panic!("socket was not released: {result:?}"),
    }
}
async fn ws_close(mut socket: tokio_tungstenite::WebSocketStream<TcpStream>) {
    assert!(matches!(
        socket.next().await.unwrap().unwrap(),
        Message::Close(_)
    ));
    let _ = socket.flush().await;
}
async fn failed(session: &Session) -> RuntimeCode {
    timeout(Duration::from_secs(2), async {
        loop {
            if let SessionState::Failed(error) = session.state() {
                break error.code;
            }
            sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap()
}

#[test]
fn credentials_debug_does_not_expose_values() {
    let value = format!(
        "{:?}",
        Credentials::new("sensitive-user", "sensitive-password")
    );
    assert!(!value.contains("sensitive"));
    assert!(value.contains("redacted"));
}

#[tokio::test]
async fn incompatible_plans_and_unsupported_hosts_refuse_before_connecting() {
    let mut p = plans("ws", 1, false);
    p.push(plans("ws", 2, true).remove(1));
    assert!(
        matches!(Session::open(&p,SessionOptions::default()).await,Err(e) if e.code==RuntimeCode::InvalidConfiguration)
    );
    assert!(
        matches!(Session::open(&[],SessionOptions::default()).await,Err(e) if e.code==RuntimeCode::InvalidConfiguration)
    );
    let options = SessionOptions {
        max_messages: 0,
        ..Default::default()
    };
    assert!(
        matches!(Session::open(&plans("mqtt",1,false),options).await,Err(e) if e.code==RuntimeCode::InvalidConfiguration)
    );
}

#[tokio::test]
async fn readiness_timeout_releases_an_unacknowledged_mqtt_connection() {
    let (listener, port) = listener().await;
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        assert_eq!(packet(&mut stream).await.0, 0x10);
        socket_closed(&mut stream).await;
    });
    let options = SessionOptions {
        connect_timeout: Duration::from_millis(50),
        ..Default::default()
    };
    assert!(
        matches!(Session::open(&plans("mqtt",port,false),options).await,Err(e) if e.code==RuntimeCode::Deadline && !e.delivery_unknown)
    );
    server.await.unwrap();
}

#[tokio::test]
async fn subscription_readiness_waits_for_matching_suback() {
    let (listener, port) = listener().await;
    let (seen, observed) = oneshot::channel();
    let (release, wait) = oneshot::channel();
    let server = tokio::spawn(async move {
        let mut stream = mqtt_ready(listener).await;
        let (kind, body) = packet(&mut stream).await;
        assert_eq!(kind, 0x82);
        seen.send(()).unwrap();
        wait.await.unwrap();
        stream
            .write_all(&[0x90, 3, body[0], body[1], 1])
            .await
            .unwrap();
        assert_eq!(packet(&mut stream).await.0, 0xe0);
        socket_closed(&mut stream).await;
    });
    let p = plans("mqtt", port, true);
    let mut opening = Box::pin(Session::open(&p, SessionOptions::default()));
    tokio::select! { result=&mut opening=>panic!("became ready before SubAck: {}",result.is_ok()), _=observed=>{} }
    assert!(
        timeout(Duration::from_millis(20), &mut opening)
            .await
            .is_err()
    );
    release.send(()).unwrap();
    let session = opening.await.unwrap();
    assert_eq!(
        session.close().await.unwrap(),
        CloseReceipt::MqttDisconnectFlushed
    );
    server.await.unwrap();
}

#[tokio::test]
async fn mismatched_puback_never_becomes_a_send_receipt() {
    let (listener, port) = listener().await;
    let server = tokio::spawn(async move {
        let mut stream = mqtt_ready(listener).await;
        let (kind, body) = packet(&mut stream).await;
        assert_eq!(kind, 0x32);
        let (id, _) = publish(&body);
        ack(&mut stream, id + 1).await;
        socket_closed(&mut stream).await;
    });
    let mut session = Session::open(&plans("mqtt", port, false), SessionOptions::default())
        .await
        .unwrap();
    let error = session.send(0, vec![1, 2, 3]).await.unwrap_err();
    // Driver state errors remain protocol errors even before our own matcher.
    assert_eq!(error.code, RuntimeCode::Protocol);
    assert!(error.delivery_unknown);
    assert!(session.next().await.is_err());
    drop(session);
    server.await.unwrap();
}

#[tokio::test]
async fn cancelled_queued_send_is_skipped_and_late_ack_cannot_complete_another_send() {
    let (listener, port) = listener().await;
    let (seen, observed) = oneshot::channel();
    let (release, wait) = oneshot::channel();
    let server = tokio::spawn(async move {
        let mut stream = mqtt_ready(listener).await;
        let (_, body) = packet(&mut stream).await;
        let (id, payload) = publish(&body);
        assert_eq!(payload, [1]);
        seen.send(()).unwrap();
        wait.await.unwrap();
        ack(&mut stream, id).await;
        let (_, body) = packet(&mut stream).await;
        let (id, payload) = publish(&body);
        assert_eq!(payload, [3]);
        ack(&mut stream, id).await;
        assert_eq!(packet(&mut stream).await.0, 0xe0);
        socket_closed(&mut stream).await;
    });
    let session = Session::open(&plans("mqtt", port, false), SessionOptions::default())
        .await
        .unwrap();
    let sender = session.sender();
    let first = tokio::spawn(async move { sender.send(0, vec![1]).await });
    observed.await.unwrap();
    first.abort();
    let _ = first.await;
    let sender = session.sender();
    let second = tokio::spawn(async move { sender.send(0, vec![2]).await });
    tokio::task::yield_now().await;
    second.abort();
    let _ = second.await;
    let sender = session.sender();
    let third = tokio::spawn(async move { sender.send(0, vec![3]).await });
    release.send(()).unwrap();
    assert!(matches!(
        third.await.unwrap().unwrap(),
        Receipt::MqttPubAck { .. }
    ));
    session.close().await.unwrap();
    server.await.unwrap();
}

#[tokio::test]
async fn missing_puback_expires_with_uncertain_delivery_and_releases_socket() {
    let (listener, port) = listener().await;
    let server = tokio::spawn(async move {
        let mut stream = mqtt_ready(listener).await;
        assert_eq!(packet(&mut stream).await.0, 0x32);
        socket_closed(&mut stream).await;
    });
    let session = Session::open(
        &plans("mqtt", port, false),
        SessionOptions {
            operation_timeout: Duration::from_millis(50),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let error = session.send(0, vec![1]).await.unwrap_err();
    assert_eq!(error.code, RuntimeCode::Deadline);
    assert!(error.delivery_unknown);
    assert_eq!(failed(&session).await, RuntimeCode::Deadline);
    let rejected = session.send(0, vec![2]).await.unwrap_err();
    assert_eq!(rejected.code, RuntimeCode::Deadline);
    assert!(!rejected.delivery_unknown);
    server.await.unwrap();
}

#[tokio::test]
async fn idle_mqtt_session_drives_keepalive_without_application_calls() {
    let (listener, port) = listener().await;
    let (ping, observed) = oneshot::channel();
    let server = tokio::spawn(async move {
        let mut stream = mqtt_ready(listener).await;
        assert_eq!(
            timeout(Duration::from_secs(2), packet(&mut stream))
                .await
                .unwrap()
                .0,
            0xc0
        );
        stream.write_all(&[0xd0, 0]).await.unwrap();
        ping.send(()).unwrap();
        assert_eq!(packet(&mut stream).await.0, 0xe0);
        socket_closed(&mut stream).await;
    });
    let session = Session::open(&plans("mqtt", port, false), SessionOptions::default())
        .await
        .unwrap();
    observed.await.unwrap();
    session.close().await.unwrap();
    server.await.unwrap();
}

#[tokio::test]
async fn dropping_websocket_owner_closes_socket_and_retained_senders() {
    let (listener, port) = listener().await;
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut socket = accept_async(stream).await.unwrap();
        socket_closed(socket.get_mut()).await;
    });
    let session = Session::open(&plans("ws", port, true), SessionOptions::default())
        .await
        .unwrap();
    let sender = session.sender();
    drop(session);
    assert_eq!(
        sender.send(0, vec![1]).await.unwrap_err().code,
        RuntimeCode::Closed
    );
    server.await.unwrap();
}

#[tokio::test]
async fn cancelling_close_aborts_the_task_instead_of_detaching_it() {
    let (listener, port) = listener().await;
    let (seen, observed) = oneshot::channel();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut socket = accept_async(stream).await.unwrap();
        let stream = socket.get_mut();
        let mut close = [0; 6];
        stream.read_exact(&mut close).await.unwrap();
        assert_eq!(close[0], 0x88);
        seen.send(()).unwrap();
        socket_closed(stream).await;
    });
    let session = Session::open(&plans("ws", port, false), SessionOptions::default())
        .await
        .unwrap();
    let mut close = Box::pin(session.close());
    tokio::select! { _=&mut close=>panic!("close completed without peer handshake"),_=observed=>{} }
    assert!(
        timeout(Duration::from_millis(20), &mut close)
            .await
            .is_err()
    );
    drop(close);
    server.await.unwrap();
}

#[tokio::test]
async fn full_message_queue_preserves_terminal_backpressure_after_draining() {
    let (listener, port) = listener().await;
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut socket = accept_async(stream).await.unwrap();
        for _ in 0..3 {
            socket.send(Message::Binary(vec![1].into())).await.unwrap();
        }
        let _ = socket.next().await;
    });
    let mut session = Session::open(
        &plans("ws", port, true),
        SessionOptions {
            max_messages: 2,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert_eq!(failed(&session).await, RuntimeCode::Backpressure);
    for _ in 0..2 {
        assert!(matches!(
            session.next().await.unwrap(),
            Some(Incoming::Message(_))
        ));
    }
    assert_eq!(
        session.next().await.unwrap_err().code,
        RuntimeCode::Backpressure
    );
    server.await.unwrap();
}

#[tokio::test]
async fn byte_budget_is_enforced_independently_of_message_count() {
    let (listener, port) = listener().await;
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut socket = accept_async(stream).await.unwrap();
        socket
            .send(Message::Binary(vec![0; 8].into()))
            .await
            .unwrap();
        socket.send(Message::Binary(vec![1].into())).await.unwrap();
        let _ = socket.next().await;
    });
    let mut session = Session::open(
        &plans("ws", port, true),
        SessionOptions {
            max_messages: 16,
            max_buffered_bytes: 8,
            max_message_bytes: 8,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert_eq!(failed(&session).await, RuntimeCode::Backpressure);
    assert!(
        matches!(session.next().await.unwrap(),Some(Incoming::Message(m)) if m.payload.len()==8)
    );
    assert_eq!(
        session.next().await.unwrap_err().code,
        RuntimeCode::Backpressure
    );
    server.await.unwrap();
}

#[tokio::test]
async fn idle_websocket_session_replies_to_ping_without_application_reads() {
    let (listener, port) = listener().await;
    let (seen, observed) = oneshot::channel();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut socket = accept_async(stream).await.unwrap();
        socket.send(Message::Ping(vec![9].into())).await.unwrap();
        assert!(
            matches!(socket.next().await.unwrap().unwrap(),Message::Pong(bytes) if bytes.as_ref()==[9])
        );
        seen.send(()).unwrap();
        ws_close(socket).await;
    });
    let session = Session::open(&plans("ws", port, true), SessionOptions::default())
        .await
        .unwrap();
    observed.await.unwrap();
    assert_eq!(
        session.close().await.unwrap(),
        CloseReceipt::WebSocketHandshake
    );
    server.await.unwrap();
}

#[tokio::test]
async fn one_hundred_explicit_close_cycles_join_every_connection() {
    let (listener, port) = listener().await;
    let server = tokio::spawn(async move {
        let mut closed = 0;
        for _ in 0..100 {
            let (stream, _) = listener.accept().await.unwrap();
            ws_close(accept_async(stream).await.unwrap()).await;
            closed += 1;
        }
        closed
    });
    let p = plans("ws", port, true);
    for _ in 0..100 {
        let session = Session::open(&p, SessionOptions::default()).await.unwrap();
        assert_eq!(
            session.close().await.unwrap(),
            CloseReceipt::WebSocketHandshake
        );
    }
    assert_eq!(server.await.unwrap(), 100);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn send_receipt_releases_capacity_before_waking_its_recipient() {
    let (listener, port) = listener().await;
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut socket = accept_async(stream).await.unwrap();
        for _ in 0..1000 {
            assert!(matches!(
                socket.next().await.unwrap().unwrap(),
                Message::Binary(_)
            ));
        }
        ws_close(socket).await;
    });
    let session = Session::open(
        &plans("ws", port, false),
        SessionOptions {
            max_messages: 1,
            max_buffered_bytes: 1,
            max_message_bytes: 1,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    for _ in 0..1000 {
        assert_eq!(
            session.send(0, vec![7]).await.unwrap(),
            Receipt::WebSocketFlushed
        );
    }
    session.close().await.unwrap();
    server.await.unwrap();
}

#[tokio::test]
async fn control_frames_are_not_limited_by_a_smaller_binary_message_budget() {
    let (listener, port) = listener().await;
    let (seen, observed) = oneshot::channel();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut socket = accept_async(stream).await.unwrap();
        socket
            .send(Message::Ping(vec![9; 125].into()))
            .await
            .unwrap();
        assert!(
            matches!(socket.next().await.unwrap().unwrap(),Message::Pong(bytes) if bytes.len()==125)
        );
        seen.send(()).unwrap();
        ws_close(socket).await;
    });
    let session = Session::open(
        &plans("ws", port, true),
        SessionOptions {
            max_buffered_bytes: 1,
            max_message_bytes: 1,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    observed.await.unwrap();
    session.close().await.unwrap();
    server.await.unwrap();
}

/// A deliberately small independent packet writer for the receive contracts.
async fn incoming_publish(stream: &mut TcpStream, qos: u8) {
    incoming_publication(stream, qos, 7, false, b"payload").await;
}
async fn incoming_publication(
    stream: &mut TcpStream,
    qos: u8,
    id: u16,
    duplicate: bool,
    payload: &[u8],
) {
    let topic = b"fixture/events";
    let mut body = Vec::from((topic.len() as u16).to_be_bytes());
    body.extend_from_slice(topic);
    if qos > 0 {
        body.extend_from_slice(&id.to_be_bytes());
    }
    body.extend_from_slice(payload);
    assert!(body.len() < 128);
    stream
        .write_all(&[
            0x30 | (qos << 1) | (u8::from(duplicate) << 3),
            body.len() as u8,
        ])
        .await
        .unwrap();
    stream.write_all(&body).await.unwrap();
}

#[tokio::test]
async fn qos_zero_flushes_without_puback_and_releases_capacity() {
    let (listener, port) = listener().await;
    let server = tokio::spawn(async move {
        let mut stream = mqtt_ready(listener).await;
        for sequence in 0..1000_u16 {
            let (kind, body) = packet(&mut stream).await;
            assert_eq!(kind, 0x30);
            let topic_len = usize::from(u16::from_be_bytes([body[0], body[1]]));
            assert_eq!(&body[2..2 + topic_len], b"fixture/events");
            // QoS 0 has no packet identifier between topic and payload.
            assert_eq!(&body[2 + topic_len..], &sequence.to_be_bytes());
        }
        assert_eq!(packet(&mut stream).await.0, 0xe0);
        socket_closed(&mut stream).await;
    });
    let session = Session::open(
        &plans_at_qos("mqtt", port, false, 0, 0),
        SessionOptions {
            max_messages: 1,
            max_buffered_bytes: 2,
            max_message_bytes: 2,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert!(session.mqtt_subscriptions().is_empty());
    for sequence in 0..1000_u16 {
        assert_eq!(
            session
                .send(0, sequence.to_be_bytes().to_vec())
                .await
                .unwrap(),
            Receipt::MqttPublishFlushed
        );
    }
    session.close().await.unwrap();
    server.await.unwrap();
}

#[tokio::test]
async fn subscription_grants_are_maxima_and_delivery_reports_actual_qos() {
    for (requested, granted, actual) in [
        (0, 0, 0),
        (1, 0, 0),
        (1, 1, 0),
        (1, 1, 1),
        (2, 0, 0),
        (2, 1, 0),
        (2, 1, 1),
        (2, 2, 0),
        (2, 2, 1),
        (2, 2, 2),
    ] {
        let (listener, port) = listener().await;
        let (done, observed) = oneshot::channel();
        let server = tokio::spawn(async move {
            let mut stream = mqtt_ready(listener).await;
            let (kind, body) = packet(&mut stream).await;
            assert_eq!(kind, 0x82);
            assert_eq!(*body.last().unwrap(), requested);
            stream
                .write_all(&[0x90, 3, body[0], body[1], granted])
                .await
                .unwrap();
            incoming_publish(&mut stream, actual).await;
            if actual == 1 {
                assert_eq!(packet(&mut stream).await, (0x40, vec![0, 7]));
            } else if actual == 2 {
                assert_eq!(packet(&mut stream).await, (0x50, vec![0, 7]));
                stream.write_all(&[0x62, 2, 0, 7]).await.unwrap();
                assert_eq!(packet(&mut stream).await, (0x70, vec![0, 7]));
            }
            done.send(()).unwrap();
            // A QoS 0 delivery has no acknowledgment on the wire.
            assert_eq!(packet(&mut stream).await.0, 0xe0);
            socket_closed(&mut stream).await;
        });
        let mut session = Session::open(
            &plans_at_qos("mqtt", port, true, 0, requested),
            SessionOptions::default(),
        )
        .await
        .unwrap();
        let subscriptions = session.mqtt_subscriptions();
        assert_eq!(subscriptions.len(), 1);
        assert_eq!(subscriptions[0].operation, 1);
        assert_eq!(subscriptions[0].topic, "fixture/events");
        assert_eq!(subscriptions[0].requested_qos, requested);
        assert_eq!(subscriptions[0].granted_qos, granted);
        let Some(Incoming::Message(message)) = session.next().await.unwrap() else {
            panic!("expected classified publication")
        };
        assert_eq!(message.operation, 1);
        assert_eq!(message.payload.as_ref(), b"payload");
        assert!(
            matches!(message.delivery, Delivery::Mqtt {qos,packet_id,..} if qos == actual && packet_id == if actual == 0 { 0 } else { 7 })
        );
        observed.await.unwrap();
        session.close().await.unwrap();
        server.await.unwrap();
    }
}

#[tokio::test]
async fn publication_before_suback_is_retained_until_session_readiness() {
    let (listener, port) = listener().await;
    let server = tokio::spawn(async move {
        let mut stream = mqtt_ready(listener).await;
        let (_, body) = packet(&mut stream).await;
        incoming_publish(&mut stream, 0).await;
        stream
            .write_all(&[0x90, 3, body[0], body[1], 0])
            .await
            .unwrap();
        assert_eq!(packet(&mut stream).await.0, 0xe0);
        socket_closed(&mut stream).await;
    });
    let mut session = Session::open(&plans("mqtt", port, true), SessionOptions::default())
        .await
        .unwrap();
    assert_eq!(session.mqtt_subscriptions()[0].granted_qos, 0);
    assert!(
        matches!(session.next().await.unwrap(),Some(Incoming::Message(message)) if message.payload.as_ref() == b"payload")
    );
    session.close().await.unwrap();
    server.await.unwrap();
}

#[tokio::test]
async fn invalid_or_refused_grants_never_make_a_session_ready() {
    for (requested, granted, expected) in [
        (0, 1, RuntimeCode::Protocol),
        (1, 2, RuntimeCode::Protocol),
        (1, 0x80, RuntimeCode::Connection),
    ] {
        let (listener, port) = listener().await;
        let server = tokio::spawn(async move {
            let mut stream = mqtt_ready(listener).await;
            let (_, body) = packet(&mut stream).await;
            stream
                .write_all(&[0x90, 3, body[0], body[1], granted])
                .await
                .unwrap();
            socket_closed(&mut stream).await;
        });
        assert!(
            matches!(Session::open(&plans_at_qos("mqtt",port,true,0,requested), SessionOptions::default()).await, Err(error) if error.code == expected)
        );
        server.await.unwrap();
    }
}

#[tokio::test]
async fn publication_above_negotiated_grant_is_a_protocol_failure() {
    let (listener, port) = listener().await;
    let server = tokio::spawn(async move {
        let mut stream = mqtt_ready(listener).await;
        let (_, body) = packet(&mut stream).await;
        stream
            .write_all(&[0x90, 3, body[0], body[1], 0])
            .await
            .unwrap();
        incoming_publish(&mut stream, 1).await;
        // Automatic acknowledgment happens in the backend before delivery policy.
        assert_eq!(packet(&mut stream).await, (0x40, vec![0, 7]));
        socket_closed(&mut stream).await;
    });
    let mut session = Session::open(&plans("mqtt", port, true), SessionOptions::default())
        .await
        .unwrap();
    assert_eq!(
        session.next().await.unwrap_err().code,
        RuntimeCode::Protocol
    );
    server.await.unwrap();
}

#[tokio::test]
async fn mixed_send_qos_preserves_each_receipts_meaning() {
    let (listener, port) = listener().await;
    let source = json!({"asyncapi":"3.1.0","info":{"title":"mixed","version":"1"},
        "servers":{"local":{"host":format!("127.0.0.1:{port}"),"protocol":"mqtt","protocolVersion":"3.1.1",
            "bindings":{"mqtt":{"clientId":"mixed","cleanSession":true,"keepAlive":1}}}},
        "channels":{"c":{"address":"fixture/events","messages":{"m":{"contentType":"application/octet-stream"}}}},
        "operations":{
            "q0":{"action":"send","channel":{"$ref":"#/channels/c"},"bindings":{"mqtt":{"qos":0}}},
            "q1":{"action":"send","channel":{"$ref":"#/channels/c"},"bindings":{"mqtt":{"qos":1}}}}});
    let document = Document::parse(&source.to_string()).unwrap();
    let p: Vec<_> = ["q0", "q1"]
        .into_iter()
        .map(|id| {
            document
                .operation_id(id)
                .unwrap()
                .compile()
                .unwrap()
                .prepare(&PlanOptions::application())
                .unwrap()
        })
        .collect();
    let server = tokio::spawn(async move {
        let mut stream = mqtt_ready(listener).await;
        for qos in [0, 1, 0, 1] {
            let (kind, body) = packet(&mut stream).await;
            assert_eq!(kind, 0x30 | (qos << 1));
            if qos == 1 {
                let (id, payload) = publish(&body);
                assert_eq!(payload, [1]);
                ack(&mut stream, id).await;
            }
        }
        assert_eq!(packet(&mut stream).await.0, 0xe0);
        socket_closed(&mut stream).await;
    });
    let session = Session::open(&p, SessionOptions::default()).await.unwrap();
    let mut ids = Vec::new();
    for qos in [0, 1, 0, 1] {
        match session.send(qos, vec![qos as u8]).await.unwrap() {
            Receipt::MqttPublishFlushed => assert_eq!(qos, 0),
            Receipt::MqttPubAck { packet_id } => {
                assert_eq!(qos, 1);
                assert!(packet_id > 0);
                ids.push(packet_id);
            }
            other => panic!("unexpected receipt: {other:?}"),
        }
    }
    assert_eq!(ids.len(), 2);
    assert_ne!(ids[0], ids[1]);
    session.close().await.unwrap();
    server.await.unwrap();
}

#[tokio::test]
async fn suback_must_match_subscription_identity_and_count() {
    for wrong_id in [false, true] {
        let (listener, port) = listener().await;
        let server = tokio::spawn(async move {
            let mut stream = mqtt_ready(listener).await;
            let (_, body) = packet(&mut stream).await;
            let id = u16::from_be_bytes([body[0], body[1]]) + u16::from(wrong_id);
            let mut ack = vec![
                0x90,
                if wrong_id { 3 } else { 4 },
                (id >> 8) as u8,
                id as u8,
                1,
            ];
            if !wrong_id {
                ack.push(1);
            }
            stream.write_all(&ack).await.unwrap();
            socket_closed(&mut stream).await;
        });
        assert!(
            matches!(Session::open(&plans("mqtt",port,true),SessionOptions::default()).await,Err(error) if error.code == RuntimeCode::Protocol)
        );
        server.await.unwrap();
    }
}

async fn control_packet(stream: &mut TcpStream, kind: u8, id: u16) {
    stream
        .write_all(&[kind, 2, (id >> 8) as u8, id as u8])
        .await
        .unwrap();
}

#[tokio::test]
async fn qos_two_receipt_waits_for_pubcomp_after_repeated_pubrec() {
    let (listener, port) = listener().await;
    let (seen, observed) = oneshot::channel();
    let (release, wait) = oneshot::channel();
    let server = tokio::spawn(async move {
        let mut stream = mqtt_ready(listener).await;
        let (kind, body) = packet(&mut stream).await;
        assert_eq!(kind, 0x34);
        let (id, payload) = publish(&body);
        assert_eq!(payload, b"payload");
        for _ in 0..2 {
            control_packet(&mut stream, 0x50, id).await;
            assert_eq!(packet(&mut stream).await, (0x62, id.to_be_bytes().to_vec()));
        }
        seen.send(id).unwrap();
        wait.await.unwrap();
        control_packet(&mut stream, 0x70, id).await;
        assert_eq!(packet(&mut stream).await.0, 0xe0);
        socket_closed(&mut stream).await;
    });
    let session = Session::open(
        &plans_at_qos("mqtt", port, false, 2, 0),
        SessionOptions::default(),
    )
    .await
    .unwrap();
    let sender = session.sender();
    let mut sending = Box::pin(sender.send(0, Vec::from(b"payload".as_slice())));
    let id = tokio::select! { result=&mut sending=>panic!("send completed before PUBCOMP: {result:?}"), id=observed=>id.unwrap() };
    assert!(
        timeout(Duration::from_millis(20), &mut sending)
            .await
            .is_err()
    );
    release.send(()).unwrap();
    assert_eq!(
        sending.await.unwrap(),
        Receipt::MqttPubComp { packet_id: id }
    );
    session.close().await.unwrap();
    server.await.unwrap();
}

#[tokio::test]
async fn qos_two_receive_suppresses_duplicates_and_allows_completed_identifier_reuse() {
    for id in [7, u16::MAX] {
        let (listener, port) = listener().await;
        let (done, observed) = oneshot::channel();
        let server = tokio::spawn(async move {
            let mut stream = mqtt_ready(listener).await;
            let (_, body) = packet(&mut stream).await;
            stream
                .write_all(&[0x90, 3, body[0], body[1], 2])
                .await
                .unwrap();
            for payload in [b"first".as_slice(), b"second".as_slice()] {
                for duplicate in [false, true] {
                    incoming_publication(&mut stream, 2, id, duplicate, payload).await;
                    assert_eq!(packet(&mut stream).await, (0x50, id.to_be_bytes().to_vec()));
                }
                for _ in 0..2 {
                    control_packet(&mut stream, 0x62, id).await;
                    assert_eq!(packet(&mut stream).await, (0x70, id.to_be_bytes().to_vec()));
                }
            }
            done.send(()).unwrap();
            assert_eq!(packet(&mut stream).await.0, 0xe0);
            socket_closed(&mut stream).await;
        });
        let mut session = Session::open(
            &plans_at_qos("mqtt", port, true, 0, 2),
            SessionOptions::default(),
        )
        .await
        .unwrap();
        for payload in [b"first".as_slice(), b"second".as_slice()] {
            let Some(Incoming::Message(message)) = session.next().await.unwrap() else {
                panic!("missing publication")
            };
            assert_eq!(message.payload.as_ref(), payload);
            assert!(
                matches!(message.delivery,Delivery::Mqtt{qos:2,packet_id,duplicate:false,..} if packet_id==id)
            );
        }
        observed.await.unwrap();
        assert!(
            timeout(Duration::from_millis(20), session.next())
                .await
                .is_err()
        );
        session.close().await.unwrap();
        server.await.unwrap();
    }
}

#[tokio::test]
async fn qos_two_rejects_acknowledgment_type_phase_and_identity_mismatches() {
    for mode in [
        "puback",
        "premature-pubcomp",
        "wrong-pubrec",
        "wrong-pubcomp",
    ] {
        let (listener, port) = listener().await;
        let server = tokio::spawn(async move {
            let mut stream = mqtt_ready(listener).await;
            let (_, body) = packet(&mut stream).await;
            let (id, _) = publish(&body);
            match mode {
                "puback" => control_packet(&mut stream, 0x40, id).await,
                "premature-pubcomp" => control_packet(&mut stream, 0x70, id).await,
                "wrong-pubrec" => control_packet(&mut stream, 0x50, id + 1).await,
                "wrong-pubcomp" => {
                    control_packet(&mut stream, 0x50, id).await;
                    assert_eq!(packet(&mut stream).await, (0x62, id.to_be_bytes().to_vec()));
                    control_packet(&mut stream, 0x70, id + 1).await;
                }
                _ => unreachable!(),
            }
            socket_closed(&mut stream).await;
        });
        let session = Session::open(
            &plans_at_qos("mqtt", port, false, 2, 0),
            SessionOptions::default(),
        )
        .await
        .unwrap();
        let error = session.send(0, vec![1]).await.unwrap_err();
        assert_eq!(error.code, RuntimeCode::Protocol, "{mode}");
        assert!(error.delivery_unknown);
        server.await.unwrap();
    }
}

#[tokio::test]
async fn qos_one_rejects_a_pubrec_instead_of_switching_to_qos_two() {
    let (listener, port) = listener().await;
    let server = tokio::spawn(async move {
        let mut stream = mqtt_ready(listener).await;
        let (_, body) = packet(&mut stream).await;
        let (id, _) = publish(&body);
        control_packet(&mut stream, 0x50, id).await;
        socket_closed(&mut stream).await;
    });
    let session = Session::open(&plans("mqtt", port, false), SessionOptions::default())
        .await
        .unwrap();
    let error = session.send(0, vec![1]).await.unwrap_err();
    assert_eq!(error.code, RuntimeCode::Protocol);
    assert!(error.delivery_unknown);
    server.await.unwrap();
}

#[tokio::test]
async fn missing_qos_two_acknowledgments_expire_with_uncertain_delivery() {
    for after_pubrec in [false, true] {
        let (listener, port) = listener().await;
        let server = tokio::spawn(async move {
            let mut stream = mqtt_ready(listener).await;
            let (_, body) = packet(&mut stream).await;
            let (id, _) = publish(&body);
            if after_pubrec {
                control_packet(&mut stream, 0x50, id).await;
                assert_eq!(packet(&mut stream).await, (0x62, id.to_be_bytes().to_vec()));
            }
            socket_closed(&mut stream).await;
        });
        let session = Session::open(
            &plans_at_qos("mqtt", port, false, 2, 0),
            SessionOptions {
                operation_timeout: Duration::from_millis(60),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        let error = session.send(0, vec![1]).await.unwrap_err();
        assert_eq!(error.code, RuntimeCode::Deadline);
        assert!(error.delivery_unknown);
        server.await.unwrap();
    }
}

#[tokio::test]
async fn qos_two_protocol_state_and_capacity_are_reusable_across_many_sends() {
    let (listener, port) = listener().await;
    let server = tokio::spawn(async move {
        let mut stream = mqtt_ready(listener).await;
        for sequence in 0..1000_u16 {
            let (kind, body) = packet(&mut stream).await;
            assert_eq!(kind, 0x34);
            let (id, payload) = publish(&body);
            assert_eq!(payload, sequence.to_be_bytes());
            control_packet(&mut stream, 0x50, id).await;
            assert_eq!(packet(&mut stream).await, (0x62, id.to_be_bytes().to_vec()));
            control_packet(&mut stream, 0x70, id).await;
        }
        assert_eq!(packet(&mut stream).await.0, 0xe0);
        socket_closed(&mut stream).await;
    });
    let session = Session::open(
        &plans_at_qos("mqtt", port, false, 2, 0),
        SessionOptions {
            max_messages: 1,
            max_message_bytes: 2,
            max_buffered_bytes: 2,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    for sequence in 0..1000_u16 {
        assert!(
            matches!(session.send(0,sequence.to_be_bytes().to_vec()).await.unwrap(),Receipt::MqttPubComp{packet_id} if packet_id>0)
        );
    }
    session.close().await.unwrap();
    server.await.unwrap();
}

#[tokio::test]
async fn cancelling_a_qos_two_waiter_does_not_cross_complete_the_next_send() {
    let (listener, port) = listener().await;
    let (seen, observed) = oneshot::channel();
    let (release, wait) = oneshot::channel();
    let server = tokio::spawn(async move {
        let mut stream = mqtt_ready(listener).await;
        let (_, body) = packet(&mut stream).await;
        let (first, payload) = publish(&body);
        assert_eq!(payload, [1]);
        control_packet(&mut stream, 0x50, first).await;
        assert_eq!(
            packet(&mut stream).await,
            (0x62, first.to_be_bytes().to_vec())
        );
        seen.send(first).unwrap();
        wait.await.unwrap();
        // Repeated PUBREC after the application cancelled its waiter still
        // belongs to the first protocol exchange.
        control_packet(&mut stream, 0x50, first).await;
        assert_eq!(
            packet(&mut stream).await,
            (0x62, first.to_be_bytes().to_vec())
        );
        control_packet(&mut stream, 0x70, first).await;
        let (_, body) = packet(&mut stream).await;
        let (second, payload) = publish(&body);
        assert_ne!(first, second);
        assert_eq!(payload, [2]);
        control_packet(&mut stream, 0x50, second).await;
        assert_eq!(
            packet(&mut stream).await,
            (0x62, second.to_be_bytes().to_vec())
        );
        control_packet(&mut stream, 0x70, second).await;
        assert_eq!(packet(&mut stream).await.0, 0xe0);
        socket_closed(&mut stream).await;
        second
    });
    let session = Session::open(
        &plans_at_qos("mqtt", port, false, 2, 0),
        SessionOptions::default(),
    )
    .await
    .unwrap();
    let sender = session.sender();
    let first = tokio::spawn(async move { sender.send(0, vec![1]).await });
    let first_id = observed.await.unwrap();
    first.abort();
    assert!(first.await.unwrap_err().is_cancelled());
    let sender = session.sender();
    let second = tokio::spawn(async move { sender.send(0, vec![2]).await });
    release.send(()).unwrap();
    let Receipt::MqttPubComp { packet_id } = second.await.unwrap().unwrap() else {
        panic!("wrong receipt")
    };
    assert_ne!(packet_id, first_id);
    session.close().await.unwrap();
    assert_eq!(server.await.unwrap(), packet_id);
}

#[tokio::test]
async fn dropping_qos_two_owner_releases_an_unfinished_exchange() {
    let (listener, port) = listener().await;
    let (seen, observed) = oneshot::channel();
    let server = tokio::spawn(async move {
        let mut stream = mqtt_ready(listener).await;
        let (_, body) = packet(&mut stream).await;
        let (id, _) = publish(&body);
        control_packet(&mut stream, 0x50, id).await;
        assert_eq!(packet(&mut stream).await, (0x62, id.to_be_bytes().to_vec()));
        seen.send(()).unwrap();
        socket_closed(&mut stream).await;
    });
    let session = Session::open(
        &plans_at_qos("mqtt", port, false, 2, 0),
        SessionOptions::default(),
    )
    .await
    .unwrap();
    let sender = session.sender();
    let sending = tokio::spawn(async move { sender.send(0, vec![1]).await });
    observed.await.unwrap();
    drop(session);
    let error = sending.await.unwrap().unwrap_err();
    assert_eq!(error.code, RuntimeCode::Closed);
    assert!(error.delivery_unknown);
    server.await.unwrap();
}
