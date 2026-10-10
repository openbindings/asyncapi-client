use crate::{RuntimeCode, RuntimeError, SessionOptions};
use dynamic_asyncapi_client::{Action, Plan, TransportPlan};

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct MqttSettings {
    pub host: String,
    pub port: u16,
    pub client_id: String,
    pub clean_session: bool,
    pub keep_alive_seconds: u16,
}
pub(crate) enum Connection {
    Mqtt(MqttSettings),
    WebSocket(String),
}
impl Connection {
    pub fn from_plans(plans: &[Plan], options: &SessionOptions) -> Result<Self, RuntimeError> {
        if plans.is_empty() || plans.len() > 16 {
            return Err(RuntimeError::new(
                RuntimeCode::InvalidConfiguration,
                "a session requires between one and sixteen plans",
            ));
        }
        let first = plans[0].describe();
        let mut connection = None;
        let mut receive_topics = std::collections::HashSet::new();
        let mut websocket_receiver = false;
        for (index, plan) in plans.iter().enumerate() {
            let plan = plan.describe();
            if plan.role != first.role
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
                    if *qos != 1 {
                        return Err(RuntimeError::new(
                            RuntimeCode::Unsupported,
                            "initial native MQTT execution requires QoS 1",
                        ));
                    }
                    if topic.len() > 2048 {
                        return Err(RuntimeError::new(
                            RuntimeCode::Unsupported,
                            "initial native MQTT topic limit is 2048 bytes",
                        ));
                    }
                    if plan.wire_action == Action::Receive
                        && (topic.contains(['#', '+']) || !receive_topics.insert(topic.clone()))
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
                    if uri.scheme().as_str() != "mqtt" {
                        return Err(RuntimeError::new(
                            RuntimeCode::Unsupported,
                            "native TLS support is not enabled in this execution slice",
                        ));
                    }
                    let authority = uri.authority().unwrap();
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
                        .unwrap_or(1883);
                    Self::Mqtt(MqttSettings {
                        host,
                        port,
                        client_id: client_id.clone(),
                        clean_session: *clean_session,
                        keep_alive_seconds: *keep_alive_seconds,
                    })
                }
                TransportPlan::WebSocket6455 { endpoint, method } => {
                    if !endpoint.starts_with("ws://") || method != "GET" {
                        return Err(RuntimeError::new(
                            RuntimeCode::Unsupported,
                            "initial native WebSocket execution requires ws and GET",
                        ));
                    }
                    if options.credentials.is_some() {
                        return Err(RuntimeError::new(
                            RuntimeCode::Unsupported,
                            "MQTT credentials cannot configure a WebSocket handshake",
                        ));
                    }
                    if plan.wire_action == Action::Receive {
                        if websocket_receiver {
                            return Err(RuntimeError::new(
                                RuntimeCode::Unsupported,
                                "binary WebSocket receive plans are ambiguous",
                            ));
                        }
                        websocket_receiver = true;
                    }
                    Self::WebSocket(endpoint.clone())
                }
            };
            if let Some(previous) = &connection {
                let compatible = match (previous, &candidate) {
                    (Self::Mqtt(a), Self::Mqtt(b)) => a == b,
                    (Self::WebSocket(a), Self::WebSocket(b)) => a == b,
                    _ => false,
                };
                if !compatible {
                    return Err(RuntimeError::new(
                        RuntimeCode::InvalidConfiguration,
                        "plans have incompatible connection settings",
                    ));
                }
            } else {
                connection = Some(candidate);
            }
        }
        Ok(connection.unwrap())
    }
}
