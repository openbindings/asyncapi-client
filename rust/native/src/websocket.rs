use crate::{
    CloseReceipt, Context, Delivery, Incoming, Receipt, RuntimeCode, RuntimeError, SendCommand,
};
use dynamic_asyncapi_session::Route;
use futures_util::{SinkExt, StreamExt};
use std::time::Duration;
use tokio::{
    net::TcpStream,
    sync::{mpsc, watch},
    time::{Instant, sleep_until, timeout, timeout_at},
};
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream,
    tungstenite::{Message, protocol::WebSocketConfig},
};
pub(crate) type Driver = WebSocketStream<MaybeTlsStream<TcpStream>>;
pub(crate) async fn connect(endpoint: &str, context: &Context) -> Result<Driver, RuntimeError> {
    let config = WebSocketConfig::default()
        .write_buffer_size(0)
        .max_write_buffer_size(context.options.max_message_bytes + 4096)
        .max_message_size(Some(context.options.max_message_bytes))
        .max_frame_size(Some(context.options.max_message_bytes.max(125)));
    tokio_tungstenite::connect_async_with_config(endpoint, Some(config), true)
        .await
        .map(|(socket, _)| socket)
        .map_err(driver_error)
}
pub(crate) async fn run<S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send>(
    socket: WebSocketStream<S>,
    context: Context,
    mut commands: mpsc::Receiver<SendCommand>,
    mut shutdown: watch::Receiver<bool>,
) -> Result<CloseReceipt, RuntimeError> {
    let (mut writer, mut reader) = socket.split();
    let mut closing: Option<Instant> = None;
    loop {
        let deadline = closing.unwrap_or_else(|| Instant::now() + Duration::from_secs(300));
        tokio::select! {
            _=sleep_until(deadline),if closing.is_some()=>return Err(RuntimeError::new(RuntimeCode::Deadline,"WebSocket close handshake deadline expired")),
            _=shutdown.changed(),if closing.is_none()=> {
                closing=Some(Instant::now()+context.options.operation_timeout);
                timeout_at(closing.unwrap(),writer.send(Message::Close(None))).await.map_err(|_|RuntimeError::new(RuntimeCode::Deadline,"WebSocket close frame deadline expired"))?.map_err(|_|RuntimeError::new(RuntimeCode::Connection,"WebSocket close frame failed"))?;
            },
            command=commands.recv(),if closing.is_none()=> {
                let Some(command)=command else { return Err(RuntimeError::new(RuntimeCode::Closed,"session command owner ended")); };
                if command.response.is_closed() { continue; }
                if Instant::now()>=command.deadline { command.complete(Err(RuntimeError::new(RuntimeCode::Deadline,"send expired before driver submission")));continue; }
                let mut peer_closed=false;
                {
                    // Keep both halves progressing under backpressure. A peer
                    // may itself be waiting for us to drain its outgoing frame.
                    let outgoing=if context.plans.plans()[command.operation].websocket_frame()==Some(dynamic_asyncapi_client::WebSocketFrame::Text) {
                        Message::Text(command.payload.clone().try_into().map_err(|_|RuntimeError::new(RuntimeCode::InvalidPayload,"text frame requires UTF-8"))?)
                    } else { Message::Binary(command.payload.clone()) };
                    let writing=timeout_at(command.deadline,writer.send(outgoing));
                    tokio::pin!(writing);
                    loop {
                        tokio::select! {
                            result=&mut writing=> { result.map_err(|_|RuntimeError::new(RuntimeCode::Deadline,"WebSocket flush deadline expired").uncertain())?.map_err(|error|driver_error(error).uncertain())?;break; },
                            incoming=reader.next()=> {
                                let message=incoming.ok_or_else(||RuntimeError::new(RuntimeCode::Connection,"WebSocket ended while flushing").uncertain())?.map_err(|error|driver_error(error).uncertain())?;
                                if matches!(observe(&context,message)?,Observed::Close) { peer_closed=true; }
                            }
                        }
                    }
                }
                command.complete(Ok(Receipt::WebSocketFlushed));
                if peer_closed { return Ok(CloseReceipt::WebSocketHandshake); }
            },
            incoming=reader.next()=> {
                let message=incoming.ok_or_else(||RuntimeError::new(RuntimeCode::Connection,"WebSocket ended without a close handshake"))?.map_err(driver_error)?;
                match observe(&context,message)? {
                    Observed::Data=>{},
                    Observed::Control=> {
                        timeout(context.options.operation_timeout,writer.flush()).await.map_err(|_|RuntimeError::new(RuntimeCode::Deadline,"WebSocket control flush deadline expired"))?.map_err(driver_error)?;
                    },
                    Observed::Close=> {
                        let result=timeout(context.options.operation_timeout,writer.flush()).await.map_err(|_|RuntimeError::new(RuntimeCode::Deadline,"WebSocket close response deadline expired"))?;
                        match result { Ok(())|Err(tokio_tungstenite::tungstenite::Error::ConnectionClosed)=>return Ok(CloseReceipt::WebSocketHandshake),Err(error)=>return Err(driver_error(error)) }
                    },
                }
            }
        }
    }
}

enum Observed {
    Data,
    Control,
    Close,
}
fn observe(context: &Context, message: Message) -> Result<Observed, RuntimeError> {
    match message {
        Message::Binary(payload) => observe_payload(context, payload, true)?,
        Message::Text(text) => observe_payload(context, text.into(), false)?,
        Message::Ping(_) | Message::Pong(_) => return Ok(Observed::Control),
        Message::Close(_) => return Ok(Observed::Close),
        Message::Frame(_) => {
            return Err(RuntimeError::new(
                RuntimeCode::Protocol,
                "unexpected raw WebSocket frame",
            ));
        }
    }
    Ok(Observed::Data)
}

fn observe_payload(
    context: &Context,
    payload: bytes::Bytes,
    binary: bool,
) -> Result<(), RuntimeError> {
    match context.plans.websocket_route(binary) {
        Route::Operation(operation) => {
            context.deliver_payload(operation, payload, Delivery::WebSocket)
        }
        Route::Rejected(reason) => context.deliver(Incoming::Rejected {
            reason,
            payload_bytes: payload.len(),
        }),
    }
}
fn driver_error(error: tokio_tungstenite::tungstenite::Error) -> RuntimeError {
    use tokio_tungstenite::tungstenite::Error;
    match error {
        Error::Capacity(_) | Error::WriteBufferFull(_) => RuntimeError::new(
            RuntimeCode::Backpressure,
            "WebSocket frame or message buffer limit exceeded",
        ),
        Error::Protocol(_) | Error::Utf8(_) | Error::AttackAttempt => RuntimeError::new(
            RuntimeCode::Protocol,
            "WebSocket peer or driver violated protocol expectations",
        ),
        Error::ConnectionClosed | Error::AlreadyClosed => {
            RuntimeError::new(RuntimeCode::Closed, "WebSocket connection is closed")
        }
        _ => RuntimeError::new(
            RuntimeCode::Connection,
            "WebSocket connection or upgrade failed",
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Budget, Limits, SessionOptions, SessionPlan};
    use dynamic_asyncapi_client::{Document, PlanOptions};
    use tokio::sync::oneshot;

    #[tokio::test]
    async fn simultaneous_writes_progress_with_only_sixty_four_bytes_of_transport_capacity() {
        let source = r##"{"asyncapi":"3.1.0","info":{"title":"duplex","version":"1"},"servers":{"s":{"host":"localhost","protocol":"ws"}},"channels":{"c":{"address":"/","messages":{"m":{"contentType":"application/octet-stream"}}}},"operations":{"send":{"action":"send","channel":{"$ref":"#/channels/c"}},"receive":{"action":"receive","channel":{"$ref":"#/channels/c"}}}}"##;
        let document = Document::parse(source).unwrap();
        let plans = SessionPlan::new(
            &vec!["send", "receive"]
                .into_iter()
                .map(|name| {
                    document
                        .operation_id(name)
                        .unwrap()
                        .compile()
                        .unwrap()
                        .prepare(&PlanOptions::application())
                        .unwrap()
                })
                .collect::<Vec<_>>(),
        )
        .unwrap();
        let (client, server) = tokio::io::duplex(64);
        let (client, server) = tokio::join!(
            tokio_tungstenite::client_async("ws://localhost/", client),
            tokio_tungstenite::accept_async(server)
        );
        let client = client.unwrap().0;
        let mut server = server.unwrap();
        let (events, mut received) = mpsc::channel(4);
        let budget = Budget::new(Limits {
            max_messages: 4,
            max_buffered_bytes: 2048,
            max_message_bytes: 1024,
        })
        .unwrap();
        let context = Context {
            plans,
            events,
            budget: budget.clone(),
            options: SessionOptions::default(),
        };
        let (commands, requests) = mpsc::channel(4);
        let (shutdown, closing) = watch::channel(false);
        let actor = tokio::spawn(run(client, context, requests, closing));
        let peer = tokio::spawn(async move {
            server
                .send(Message::Binary(vec![2; 512].into()))
                .await
                .unwrap();
            assert!(
                matches!(server.next().await.unwrap().unwrap(),Message::Binary(bytes) if bytes.as_ref()==vec![1;512])
            );
            assert!(matches!(
                server.next().await.unwrap().unwrap(),
                Message::Close(_)
            ));
            let _ = server.flush().await;
        });
        let (response, receipt) = oneshot::channel();
        commands
            .send(SendCommand {
                operation: 0,
                payload: vec![1; 512].into(),
                response,
                deadline: Instant::now() + Duration::from_secs(2),
                _lease: budget.reserve(512).unwrap(),
            })
            .await
            .unwrap();
        assert_eq!(
            timeout(Duration::from_secs(1), receipt)
                .await
                .unwrap()
                .unwrap()
                .unwrap(),
            Receipt::WebSocketFlushed
        );
        let queued = received.recv().await.unwrap();
        assert!(
            matches!(queued.incoming,Incoming::Message(message) if message.payload.as_ref()==vec![2;512])
        );
        shutdown.send_replace(true);
        assert_eq!(
            actor.await.unwrap().unwrap(),
            CloseReceipt::WebSocketHandshake
        );
        peer.await.unwrap();
    }
}
