use crate::{
    CloseReceipt, Context, Delivery, Incoming, Receipt, Received, RuntimeCode, RuntimeError,
    SendCommand, wire::MqttSettings,
};
use dynamic_asyncapi_client::{Action, TransportPlan};
use rumqttc::{
    AsyncClient, ConnectReturnCode, Event, EventLoop, Incoming as Packet, MqttOptions, Outgoing,
    QoS, SubscribeFilter, SubscribeReasonCode,
};
use std::time::Duration;
use tokio::{
    sync::{mpsc, watch},
    time::{Instant, sleep_until},
};

pub(crate) struct Driver {
    client: AsyncClient,
    events: EventLoop,
}
pub(crate) async fn connect(
    settings: MqttSettings,
    context: &Context,
) -> Result<Driver, RuntimeError> {
    let mut options = MqttOptions::new(settings.client_id, settings.host, settings.port);
    options.set_clean_session(settings.clean_session);
    options.set_keep_alive(Duration::from_secs(settings.keep_alive_seconds.into()));
    options.set_max_packet_size(
        context.options.max_message_bytes + 4096,
        context.options.max_message_bytes + 4096,
    );
    if let Some(credentials) = &context.options.credentials {
        options.set_credentials(&credentials.username, &credentials.password);
    }
    let (client, mut events) = AsyncClient::new(options, 16);
    match events.poll().await.map_err(connection_error)? {
        Event::Incoming(Packet::ConnAck(ack)) if ack.code == ConnectReturnCode::Success => {}
        Event::Incoming(Packet::ConnAck(_)) => {
            return Err(RuntimeError::new(
                RuntimeCode::Connection,
                "broker rejected the MQTT connection",
            ));
        }
        _ => {
            return Err(RuntimeError::new(
                RuntimeCode::Protocol,
                "MQTT connection did not begin with ConnAck",
            ));
        }
    }
    let topics: Vec<_> = context
        .plans
        .iter()
        .filter(|p| p.describe().wire_action == Action::Receive)
        .map(|p| match &p.describe().transport {
            TransportPlan::Mqtt311 { topic, .. } => {
                SubscribeFilter::new(topic.clone(), QoS::AtLeastOnce)
            }
            _ => unreachable!(),
        })
        .collect();
    if !topics.is_empty() {
        let count = topics.len();
        client.try_subscribe_many(topics).map_err(|_| {
            RuntimeError::new(
                RuntimeCode::DriverFailed,
                "driver rejected a prepared subscription",
            )
        })?;
        let mut expected = None;
        loop {
            match events.poll().await.map_err(connection_error)? {
                Event::Outgoing(Outgoing::Subscribe(id)) => expected = Some(id),
                Event::Incoming(Packet::SubAck(ack)) => {
                    if expected != Some(ack.pkid)
                        || ack.return_codes.len() != count
                        || ack
                            .return_codes
                            .iter()
                            .any(|code| *code != SubscribeReasonCode::Success(QoS::AtLeastOnce))
                    {
                        return Err(RuntimeError::new(
                            RuntimeCode::Protocol,
                            "broker did not acknowledge the requested QoS 1 subscriptions",
                        ));
                    }
                    break;
                }
                Event::Incoming(Packet::Publish(message)) => deliver(context, message)?,
                _ => {}
            }
        }
    }
    Ok(Driver { client, events })
}
fn connection_error(error: rumqttc::ConnectionError) -> RuntimeError {
    use rumqttc::{ConnectionError as C, StateError as S};
    match error {
        C::MqttState(
            S::Unsolicited(_) | S::WrongPacket | S::InvalidState | S::Deserialization(_),
        )
        | C::NotConnAck(_) => RuntimeError::new(
            RuntimeCode::Protocol,
            "MQTT peer or driver violated packet-state expectations",
        ),
        C::NetworkTimeout
        | C::FlushTimeout
        | C::MqttState(S::AwaitPingResp | S::CollisionTimeout) => RuntimeError::new(
            RuntimeCode::Deadline,
            "MQTT protocol progress deadline expired",
        ),
        C::RequestsDone => RuntimeError::new(
            RuntimeCode::DriverFailed,
            "MQTT driver request channel ended",
        ),
        _ => RuntimeError::new(
            RuntimeCode::Connection,
            "MQTT connection failed; automatic reconnect is disabled",
        ),
    }
}
fn deliver(context: &Context, message: rumqttc::Publish) -> Result<(), RuntimeError> {
    let operation=context.plans.iter().position(|p|p.describe().wire_action==Action::Receive && matches!(&p.describe().transport,TransportPlan::Mqtt311 { topic,.. } if topic==&message.topic));
    match operation {
        Some(operation) if message.qos == QoS::AtLeastOnce => {
            context.deliver(Incoming::Message(Received {
                operation,
                payload: message.payload,
                delivery: Delivery::Mqtt {
                    topic: message.topic,
                    qos: 1,
                    retain: message.retain,
                    duplicate: message.dup,
                    packet_id: message.pkid,
                },
            }))
        }
        _ => context.deliver(Incoming::Rejected {
            reason: "MQTT message has no matching receive operation at the configured QoS",
            payload_bytes: message.payload.len(),
        }),
    }
}
struct Pending {
    command: SendCommand,
    packet_id: Option<u16>,
}
pub(crate) async fn run(
    mut driver: Driver,
    context: Context,
    mut commands: mpsc::Receiver<SendCommand>,
    mut shutdown: watch::Receiver<bool>,
) -> Result<CloseReceipt, RuntimeError> {
    let mut pending: Option<Pending> = None;
    let mut closing = false;
    loop {
        // Do not drop/recreate an in-progress EventLoop::poll when a command
        // arrives: it may be partway through a write/flush. Drive this same
        // future to completion while commands use the driver's separate queue.
        let polling = driver.events.poll();
        tokio::pin!(polling);
        let event = loop {
            let deadline = pending
                .as_ref()
                .map(|p| p.command.deadline)
                .unwrap_or_else(|| Instant::now() + Duration::from_secs(300));
            tokio::select! {
                event=&mut polling=>break event.map_err(|error|if pending.is_some() { connection_error(error).uncertain() } else { connection_error(error) })?,
                _=sleep_until(deadline),if pending.is_some()=>return Err(RuntimeError::new(RuntimeCode::Deadline,"MQTT publish acknowledgment deadline expired").uncertain()),
                _=shutdown.changed(),if !closing=> {
                    closing=true;
                    driver.client.try_disconnect().map_err(|_|RuntimeError::new(RuntimeCode::DriverFailed,"driver rejected shutdown"))?;
                },
                command=commands.recv(),if pending.is_none() && !closing=> {
                    let Some(command)=command else { return Err(RuntimeError::new(RuntimeCode::Closed,"session command owner ended")); };
                    if command.response.is_closed() { continue; }
                    if Instant::now()>=command.deadline { command.complete(Err(RuntimeError::new(RuntimeCode::Deadline,"send expired before driver submission")));continue; }
                    let TransportPlan::Mqtt311 { topic,retain,.. }=&context.plans[command.operation].describe().transport else { unreachable!() };
                    driver.client.try_publish(topic,QoS::AtLeastOnce,*retain,command.payload.clone()).map_err(|_|RuntimeError::new(RuntimeCode::DriverFailed,"driver rejected prepared publish"))?;
                    pending=Some(Pending { command,packet_id:None });
                },
            }
        };
        match event {
            Event::Outgoing(Outgoing::Publish(id)) => {
                let Some(pending) = pending.as_mut() else {
                    return Err(RuntimeError::new(
                        RuntimeCode::Protocol,
                        "outgoing MQTT publish has no command",
                    ));
                };
                pending.packet_id = Some(id);
            }
            Event::Incoming(Packet::PubAck(ack)) => {
                if pending.as_ref().and_then(|p| p.packet_id) != Some(ack.pkid) {
                    return Err(RuntimeError::new(
                        RuntimeCode::Protocol,
                        "MQTT acknowledgement identity did not match the pending send",
                    )
                    .uncertain());
                }
                let pending = pending.take().unwrap();
                pending.command.complete(Ok(Receipt::MqttPubAck {
                    packet_id: ack.pkid,
                }));
            }
            Event::Incoming(Packet::Publish(message)) => deliver(&context, message)?,
            Event::Outgoing(Outgoing::Disconnect) if closing => {
                return Ok(CloseReceipt::MqttDisconnectFlushed);
            }
            Event::Incoming(Packet::Disconnect) => {
                return Err(RuntimeError::new(
                    RuntimeCode::Connection,
                    "broker disconnected the session",
                )
                .uncertain());
            }
            _ => {}
        }
    }
}
