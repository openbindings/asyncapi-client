use crate::{RuntimeCode, RuntimeError};
use dynamic_asyncapi_client::{Action, Plan, TransportPlan};
use std::{collections::HashMap, sync::Arc};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MqttSettings {
    pub host: String,
    pub port: u16,
    pub tls: bool,
    pub client_id: String,
    pub clean_session: bool,
    pub keep_alive_seconds: u16,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ConnectionPlan {
    Mqtt(MqttSettings),
    WebSocket(String),
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Route {
    Operation(usize),
    Rejected(&'static str),
}
struct Inner {
    plans: Arc<[Plan]>,
    connection: ConnectionPlan,
    mqtt_receivers: HashMap<String, (usize, u8)>,
    websocket_receiver: Option<usize>,
}
/// Owns the immutable plans and compiles connection compatibility and receive
/// routing once. Driver capability checks still happen before host I/O.
#[derive(Clone)]
pub struct SessionPlan(Arc<Inner>);
impl SessionPlan {
    pub fn new(plans: &[Plan]) -> Result<Self, RuntimeError> {
        if plans.is_empty() || plans.len() > 16 {
            return Err(RuntimeError::new(
                RuntimeCode::InvalidConfiguration,
                "a session requires between one and sixteen plans",
            ));
        }
        let mut connection = None;
        let mut mqtt_receivers = HashMap::new();
        let mut websocket_receiver = None;
        for (index, prepared) in plans.iter().enumerate() {
            let plan = prepared.describe();
            if plan.role != plans[0].describe().role
                || plans[..index]
                    .iter()
                    .any(|p| p.describe().identity == plan.identity)
            {
                return Err(RuntimeError::new(
                    RuntimeCode::InvalidConfiguration,
                    "session plans need one role and distinct native operations",
                ));
            }
            let candidate = match &plan.transport {
                TransportPlan::Mqtt311 {
                    endpoint,
                    client_id,
                    clean_session,
                    keep_alive_seconds,
                    topic,
                    qos,
                    ..
                } => {
                    if topic.len() > 2048 {
                        return Err(RuntimeError::new(
                            RuntimeCode::Unsupported,
                            "initial MQTT execution topic limit is 2048 bytes",
                        ));
                    }
                    if plan.wire_action == Action::Receive
                        && (topic.contains(['#', '+'])
                            || mqtt_receivers
                                .insert(topic.clone(), (index, *qos))
                                .is_some())
                    {
                        return Err(RuntimeError::new(
                            RuntimeCode::Unsupported,
                            "receive topics must be distinct exact topics in this execution slice",
                        ));
                    }
                    let uri = fluent_uri::Uri::parse(endpoint.as_str()).map_err(|_| {
                        RuntimeError::new(
                            RuntimeCode::InvalidConfiguration,
                            "invalid prepared endpoint",
                        )
                    })?;
                    let tls = match uri.scheme().as_str() {
                        "mqtt" => false,
                        "mqtts" => true,
                        _ => {
                            return Err(RuntimeError::new(
                                RuntimeCode::Unsupported,
                                "unsupported MQTT endpoint scheme",
                            ));
                        }
                    };
                    let authority = uri.authority().ok_or_else(|| {
                        RuntimeError::new(
                            RuntimeCode::InvalidConfiguration,
                            "prepared endpoint needs an authority",
                        )
                    })?;
                    let host = authority
                        .host()
                        .trim_start_matches('[')
                        .trim_end_matches(']')
                        .to_owned();
                    let port = authority
                        .port()
                        .map(|p| p.as_str().parse())
                        .transpose()
                        .map_err(|_| {
                            RuntimeError::new(
                                RuntimeCode::InvalidConfiguration,
                                "invalid prepared port",
                            )
                        })?
                        .unwrap_or(if tls { 8883 } else { 1883 });
                    ConnectionPlan::Mqtt(MqttSettings {
                        host,
                        port,
                        tls,
                        client_id: client_id.clone(),
                        clean_session: *clean_session,
                        keep_alive_seconds: *keep_alive_seconds,
                    })
                }
                TransportPlan::WebSocket6455 {
                    endpoint, method, ..
                } => {
                    if method != "GET"
                        || !(endpoint.starts_with("ws://") || endpoint.starts_with("wss://"))
                    {
                        return Err(RuntimeError::new(
                            RuntimeCode::Unsupported,
                            "WebSocket execution requires ws/wss and GET",
                        ));
                    }
                    if plan.wire_action == Action::Receive
                        && websocket_receiver.replace(index).is_some()
                    {
                        return Err(RuntimeError::new(
                            RuntimeCode::Unsupported,
                            "binary WebSocket receive plans are ambiguous",
                        ));
                    }
                    ConnectionPlan::WebSocket(endpoint.clone())
                }
            };
            if connection.as_ref().is_some_and(|c| c != &candidate) {
                return Err(RuntimeError::new(
                    RuntimeCode::InvalidConfiguration,
                    "plans have incompatible connection settings",
                ));
            }
            connection.get_or_insert(candidate);
        }
        Ok(Self(Arc::new(Inner {
            plans: plans.into(),
            connection: connection.unwrap(),
            mqtt_receivers,
            websocket_receiver,
        })))
    }
    pub fn connection(&self) -> &ConnectionPlan {
        &self.0.connection
    }
    pub fn plans(&self) -> &[Plan] {
        &self.0.plans
    }
    pub fn send_plan(&self, operation: usize) -> Result<&Plan, RuntimeError> {
        let plan = self.0.plans.get(operation).ok_or_else(|| {
            RuntimeError::new(
                RuntimeCode::InvalidConfiguration,
                "operation index is not attached to this session",
            )
        })?;
        if plan.describe().wire_action != Action::Send {
            return Err(RuntimeError::new(
                RuntimeCode::InvalidConfiguration,
                "operation is a receive operation",
            ));
        }
        Ok(plan)
    }
    pub fn websocket_route(&self, binary: bool) -> Route {
        let Some(operation) = self.0.websocket_receiver else {
            return Route::Rejected("WebSocket frame has no attached receive operation");
        };
        let expected = self.0.plans[operation].websocket_frame();
        if binary != (expected == Some(dynamic_asyncapi_client::WebSocketFrame::Binary)) {
            return Route::Rejected(if !binary {
                "text WebSocket frame does not match the binary codec"
            } else {
                "binary WebSocket frame does not match the text frame policy"
            });
        }
        Route::Operation(operation)
    }

    pub fn mqtt_route(&self, topic: &str, qos: u8) -> Route {
        if let Some((operation, requested)) = self.0.mqtt_receivers.get(topic)
            && qos <= *requested
        {
            return Route::Operation(*operation);
        }
        Route::Rejected("MQTT message has no matching receive operation at the configured QoS")
    }
}
