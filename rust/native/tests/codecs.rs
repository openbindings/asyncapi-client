use bytes::Bytes;
use dynamic_asyncapi_client::{Document, Json, PlanOptions, WebSocketFrame};
use dynamic_asyncapi_native::{Incoming, RuntimeCode, Session, SessionOptions};
use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpListener;
use tokio_tungstenite::tungstenite::Message;

#[tokio::test]
async fn native_websocket_codec_rejections_preserve_stream_progress_and_exact_values() {
    for binary in [false, true] {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let peer = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let mut socket = tokio_tungstenite::accept_async(socket).await.unwrap();
            let malformed = if binary {
                Message::Binary(Bytes::from_static(b"{"))
            } else {
                Message::Text("{".into())
            };
            let mismatched = if binary {
                Message::Text("{}".into())
            } else {
                Message::Binary(Bytes::from_static(b"{}"))
            };
            socket.send(malformed).await.unwrap();
            socket.send(mismatched).await.unwrap();
            let message = socket.next().await.unwrap().unwrap();
            assert_eq!(message.is_binary(), binary);
            assert_eq!(
                message.clone().into_data().as_ref(),
                b"{\"id\":900719925474099312345}"
            );
            socket.send(message).await.unwrap();
            assert!(matches!(
                socket.next().await.unwrap().unwrap(),
                Message::Close(_)
            ));
            socket.flush().await.unwrap();
        });
        let doc=Document::parse(&serde_json::json!({"asyncapi":"3.1.0","info":{"title":"codecs","version":"1"},"servers":{"s":{"host":format!("127.0.0.1:{port}"),"protocol":"ws"}},"channels":{"c":{"address":"/events","messages":{"m":{"contentType":"application/json"}}}},"operations":{"emit":{"action":"send","channel":{"$ref":"#/channels/c"}},"listen":{"action":"receive","channel":{"$ref":"#/channels/c"}}}}).to_string()).unwrap();
        let mut options = PlanOptions::application();
        if binary {
            options.websocket_frame = Some(WebSocketFrame::Binary);
        }
        let plans = ["emit", "listen"].map(|id| {
            doc.operation_id(id)
                .unwrap()
                .compile()
                .unwrap()
                .prepare(&options)
                .unwrap()
        });
        let mut session = Session::open(&plans, SessionOptions::default())
            .await
            .unwrap();
        let sender = session.sender();
        let error = sender.send(0, Bytes::from_static(b"{")).await.unwrap_err();
        assert_eq!(error.code, RuntimeCode::InvalidPayload);
        assert!(!error.delivery_unknown);
        sender
            .send_json(
                0,
                &Json::parse("{\"id\":900719925474099312345}", Default::default()).unwrap(),
            )
            .await
            .unwrap();
        assert!(matches!(
            session.next().await.unwrap(),
            Some(Incoming::InvalidPayload {
                payload_bytes: 1,
                ..
            })
        ));
        assert!(matches!(
            session.next().await.unwrap(),
            Some(Incoming::Rejected {
                payload_bytes: 2,
                ..
            })
        ));
        let Some(Incoming::Message(message)) = session.next().await.unwrap() else {
            panic!("expected JSON message")
        };
        let id = message.payload.as_json().unwrap().get("id").unwrap();
        drop(message);
        session.close().await.unwrap();
        peer.await.unwrap();
        assert_eq!(id.number_text(), Some("900719925474099312345"));
    }
}
