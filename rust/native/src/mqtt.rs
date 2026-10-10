use crate::{
    CloseReceipt, Context, Delivery, Incoming, MqttSubscription, Receipt, RuntimeCode,
    RuntimeError, SendCommand,
};
use dynamic_asyncapi_client::{Action, TransportPlan};
use dynamic_asyncapi_session::{MqttSettings, Route};
use rumqttc::{
    AsyncClient, Broker, ConnectReturnCode, Event, EventLoop, Incoming as Packet, MqttOptions,
    Outgoing, PublishOptions, QoS, SubscribeFilter, SubscribeReasonCode,
};
use std::{collections::HashMap, time::Duration};
use tokio::{
    sync::{mpsc, watch},
    time::{Instant, sleep_until},
};

pub(crate) struct Driver {
    client: AsyncClient,
    events: EventLoop,
    pub(crate) subscriptions: Vec<MqttSubscription>,
    grants: HashMap<String, u8>,
}
pub(crate) async fn connect(
    settings: MqttSettings,
    context: &Context,
) -> Result<Driver, RuntimeError> {
    let mut options = MqttOptions::new(
        settings.client_id,
        Broker::tcp(settings.host, settings.port),
    );
    options.set_clean_session(settings.clean_session);
    if let Some(tls) = &context.options.tls {
        options.set_transport(rumqttc::Transport::tls_with_config(
            rumqttc::TlsConfiguration::Rustls(tls.config()),
        ));
    }
    options.set_keep_alive(settings.keep_alive_seconds);
    options.set_max_packet_size(
        context.options.max_message_bytes + 4096,
        context.options.max_message_bytes + 4096,
    );
    if let Some(credentials) = &context.options.credentials {
        options.set_credentials(credentials.username.clone(), credentials.password.clone());
    }
    let (client, mut events) = AsyncClient::builder(options)
        .capacity(16)
        .try_build()
        .map_err(|_| {
            RuntimeError::new(
                RuntimeCode::InvalidConfiguration,
                "MQTT driver rejected connection options",
            )
        })?;
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
    let mut subscriptions: Vec<_> = context
        .plans
        .plans()
        .iter()
        .enumerate()
        .filter(|(_, p)| p.describe().wire_action == Action::Receive)
        .map(|(operation, p)| match &p.describe().transport {
            TransportPlan::Mqtt311 { topic, qos, .. } => MqttSubscription {
                operation,
                topic: topic.clone(),
                requested_qos: *qos,
                granted_qos: *qos,
            },
            _ => unreachable!(),
        })
        .collect();
    let mut grants = HashMap::new();
    if !subscriptions.is_empty() {
        let topics = subscriptions.iter().map(|subscription| {
            SubscribeFilter::new(
                subscription.topic.clone(),
                wire_qos(subscription.requested_qos),
            )
        });
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
                    if expected != Some(ack.pkid) || ack.return_codes.len() != subscriptions.len() {
                        return Err(RuntimeError::new(
                            RuntimeCode::Protocol,
                            "MQTT subscription acknowledgment identity or count did not match",
                        ));
                    }
                    for (subscription, code) in subscriptions.iter_mut().zip(ack.return_codes) {
                        let SubscribeReasonCode::Success(qos) = code else {
                            return Err(RuntimeError::new(
                                RuntimeCode::Connection,
                                "broker refused a subscription",
                            ));
                        };
                        if qos as u8 > subscription.requested_qos {
                            return Err(RuntimeError::new(
                                RuntimeCode::Protocol,
                                "broker granted QoS above the requested maximum",
                            ));
                        }
                        subscription.granted_qos = qos as u8;
                        grants.insert(subscription.topic.clone(), qos as u8);
                    }
                    break;
                }
                // MQTT permits matching publications before SUBACK. Until the
                // grant is known, admission uses the requested upper bound.
                Event::Incoming(Packet::Publish(message)) => deliver(context, &grants, message)?,
                _ => {}
            }
        }
    }
    Ok(Driver {
        client,
        events,
        subscriptions,
        grants,
    })
}
fn wire_qos(qos: u8) -> QoS {
    match qos {
        0 => QoS::AtMostOnce,
        1 => QoS::AtLeastOnce,
        2 => QoS::ExactlyOnce,
        _ => unreachable!("preparation checks MQTT QoS range"),
    }
}
fn connection_error(error: rumqttc::ConnectionError) -> RuntimeError {
    use rumqttc::{ConnectionError as C, StateError as S};
    match error {
        // The backend's framed codec wraps transport I/O (including a late TLS
        // alert) inside Deserialization. It is not malformed MQTT input.
        C::MqttState(S::Deserialization(rumqttc::Error::Io(_))) => RuntimeError::new(
            RuntimeCode::Connection,
            "MQTT transport failed before protocol progress could continue",
        ),
        C::MqttState(
            S::Unsolicited(_) | S::InvalidState | S::Deserialization(_) | S::ProtocolViolation(_),
        )
        | C::NotConnAck(_) => RuntimeError::new(
            RuntimeCode::Protocol,
            "MQTT peer or driver violated packet-state expectations",
        ),
        C::NetworkTimeout
        | C::FlushTimeout
        | C::DisconnectTimeout
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
fn deliver(
    context: &Context,
    grants: &HashMap<String, u8>,
    message: rumqttc::Publish,
) -> Result<(), RuntimeError> {
    let topic = std::str::from_utf8(&message.topic)
        .map_err(|_| RuntimeError::new(RuntimeCode::Protocol, "MQTT topic is not UTF-8"))?;
    if grants
        .get(topic)
        .is_some_and(|grant| message.qos as u8 > *grant)
    {
        return Err(RuntimeError::new(
            RuntimeCode::Protocol,
            "MQTT publication exceeded the granted subscription QoS",
        ));
    }
    match context.plans.mqtt_route(topic, message.qos as u8) {
        Route::Operation(operation) => context.deliver_payload(
            operation,
            message.payload,
            Delivery::Mqtt {
                topic: topic.to_owned(),
                qos: message.qos as u8,
                retain: message.retain,
                duplicate: message.dup,
                packet_id: message.pkid,
            },
        ),
        Route::Rejected(reason) => context.deliver(Incoming::Rejected {
            reason,
            payload_bytes: message.payload.len(),
        }),
    }
}
struct Pending {
    command: SendCommand,
    packet_id: Option<u16>,
    qos: u8,
    pubrec_seen: bool,
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
                _=sleep_until(deadline),if pending.is_some()=>return Err(RuntimeError::new(RuntimeCode::Deadline,"MQTT publish completion deadline expired").uncertain()),
                _=shutdown.changed(),if !closing=> {
                    closing=true;
                    driver.client.try_disconnect().map_err(|_|RuntimeError::new(RuntimeCode::DriverFailed,"driver rejected shutdown"))?;
                },
                command=commands.recv(),if pending.is_none() && !closing=> {
                    let Some(command)=command else { return Err(RuntimeError::new(RuntimeCode::Closed,"session command owner ended")); };
                    if command.response.is_closed() { continue; }
                    if Instant::now()>=command.deadline { command.complete(Err(RuntimeError::new(RuntimeCode::Deadline,"send expired before driver submission")));continue; }
                    let TransportPlan::Mqtt311 { topic,retain,qos,.. }=&context.plans.plans()[command.operation].describe().transport else { unreachable!() };
                    driver.client.try_publish(topic.as_str(),command.payload.clone(),PublishOptions::new(wire_qos(*qos)).retain(*retain)).map_err(|_|RuntimeError::new(RuntimeCode::DriverFailed,"driver rejected prepared publish"))?;
                    pending=Some(Pending { command,packet_id:None,qos:*qos,pubrec_seen:false });
                },
            }
        };
        match event {
            Event::Outgoing(Outgoing::Publish(id)) => {
                let Some(current) = pending.as_mut() else {
                    return Err(RuntimeError::new(
                        RuntimeCode::Protocol,
                        "outgoing MQTT publish has no command",
                    ));
                };
                if current.qos == 0 {
                    if id != 0 {
                        return Err(RuntimeError::new(
                            RuntimeCode::Protocol,
                            "QoS 0 publish unexpectedly has a packet identifier",
                        )
                        .uncertain());
                    }
                    // EventLoop yields the outgoing event after network flush,
                    // including when it first yields buffered incoming events.
                    pending
                        .take()
                        .unwrap()
                        .command
                        .complete(Ok(Receipt::MqttPublishFlushed));
                } else {
                    current.packet_id = Some(id);
                }
            }
            Event::Incoming(Packet::PubAck(ack)) => {
                if !pending
                    .as_ref()
                    .is_some_and(|p| p.qos == 1 && p.packet_id == Some(ack.pkid))
                {
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
            Event::Incoming(Packet::PubRec(ack)) => {
                let Some(current) = pending
                    .as_mut()
                    .filter(|p| p.qos == 2 && p.packet_id == Some(ack.pkid))
                else {
                    return Err(RuntimeError::new(
                        RuntimeCode::Protocol,
                        "MQTT PUBREC did not match the pending QoS 2 send",
                    )
                    .uncertain());
                };
                // The backend writes PUBREL, including for repeated PUBREC.
                // Ownership transfer at PUBREC does not complete this API's receipt.
                current.pubrec_seen = true;
            }
            Event::Incoming(Packet::PubComp(ack)) => {
                if !pending
                    .as_ref()
                    .is_some_and(|p| p.qos == 2 && p.pubrec_seen && p.packet_id == Some(ack.pkid))
                {
                    return Err(RuntimeError::new(
                        RuntimeCode::Protocol,
                        "MQTT PUBCOMP did not match the pending QoS 2 exchange",
                    )
                    .uncertain());
                }
                pending
                    .take()
                    .unwrap()
                    .command
                    .complete(Ok(Receipt::MqttPubComp {
                        packet_id: ack.pkid,
                    }));
            }
            Event::Incoming(Packet::Publish(message)) => {
                deliver(&context, &driver.grants, message)?
            }
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
