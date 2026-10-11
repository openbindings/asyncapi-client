//! Reusable, pure preparation. No transport is opened by this module.
mod exchange;
pub use exchange::{
    CompiledReply, ExchangeDescription, ExchangeOptions, ExchangePlan, ReplyAddressDescription,
    ReplyCompletion, ReplyDescription,
};
use crate::effective::{Effective, with_traits};
use crate::{
    Action, Code, Codec, Diagnostic, Document, Edition, Json, Location, Operation,
    OperationDescription, OperationIdentity, Payload, Requirement, WebSocketFrame,
};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, HashSet},
    sync::Arc,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum Role {
    Application,
    Peer,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum ProtocolProfile {
    Mqtt311,
    WebSocket6455,
}

/// Choices that bind an immutable compilation to a concrete deployment.
/// Credentials belong to runtime session configuration, never these options.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct PlanOptions {
    pub role: Role,
    #[serde(default)]
    pub server: Option<String>,
    #[serde(default)]
    pub message: Option<String>,
    #[serde(default)]
    pub profile: Option<ProtocolProfile>,
    #[serde(default)]
    pub variables: BTreeMap<String, String>,
    #[serde(default)]
    pub parameters: BTreeMap<String, String>,
    #[serde(default)]
    pub address: Option<String>,
    #[serde(default)]
    pub client_id: Option<String>,
    #[serde(default)]
    pub websocket_frame: Option<WebSocketFrame>,
    #[serde(default)]
    pub security: crate::SecuritySelection,
}
impl PlanOptions {
    pub fn application() -> Self {
        Self {
            role: Role::Application,
            server: None,
            message: None,
            profile: None,
            variables: BTreeMap::new(),
            parameters: BTreeMap::new(),
            address: None,
            client_id: None,
            websocket_frame: None,
            security: Default::default(),
        }
    }
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MessageDescription {
    /// Native channel message key in 3.x; native single/oneOf coordinate in 2.x.
    pub key: String,
    pub selection: Location,
    pub definition: Location,
    pub name: Option<String>,
    pub content_type: Option<String>,
    pub payload: Option<Location>,
    pub headers: Option<Location>,
    pub correlation_id: Option<Location>,
}
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ServerDescription {
    pub key: String,
    pub selection: Location,
    pub definition: Location,
    pub protocol: String,
    pub protocol_version: Option<String>,
}
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CompiledDescription {
    pub identity: OperationIdentity,
    pub operation: OperationDescription,
    pub messages: Vec<MessageDescription>,
    pub servers: Vec<ServerDescription>,
    pub reply: Option<Location>,
    pub operation_security: Option<Location>,
}
#[derive(Clone, Debug)]
struct Message {
    description: MessageDescription,
    effective: Effective,
}
#[derive(Clone, Debug)]
struct Server {
    description: ServerDescription,
    object: Json,
}
#[derive(Debug)]
struct Compilation {
    document: Document,
    description: CompiledDescription,
    operation: Effective,
    channel: Json,
    messages: Vec<Message>,
    servers: Vec<Server>,
    channel_message_count: usize,
}
/// A view of one side of an operation, without manufacturing an authored
/// operation or changing its native identity/description.
struct PreparationContext<'a> {
    compilation: &'a Compilation,
    channel: &'a Json,
    messages: &'a [Message],
    servers: &'a [Server],
    channel_message_count: usize,
    address: Option<&'a str>,
    action: Action,
}
impl<'a> PreparationContext<'a> {
    fn operation(compilation: &'a Compilation) -> Self {
        Self {
            compilation,
            channel: &compilation.channel,
            messages: &compilation.messages,
            servers: &compilation.servers,
            channel_message_count: compilation.channel_message_count,
            address: compilation.description.operation.address.as_deref(),
            action: compilation.description.operation.action,
        }
    }
}
impl std::ops::Deref for PreparationContext<'_> {
    type Target = Compilation;
    fn deref(&self) -> &Self::Target { self.compilation }
}
/// Owning topology and trait compilation. Profile binding resolution occurs
/// during preparation; a resulting Plan performs neither on the message path.
#[derive(Clone, Debug)]
pub struct CompiledOperation(Arc<Compilation>);

impl Operation {
    pub fn compile(&self) -> Result<CompiledOperation, Diagnostic> {
        let document = self.document();
        let (object, operation) = self.effective()?;
        let (channel, address) = self.channel(&object)?;
        let description = self.describe_resolved(&object, &operation, &channel, address)?;
        let (messages, channel_message_count) = messages(&document, &object, &channel)?;
        let servers = servers(&document, &channel)?;
        Ok(CompiledOperation(Arc::new(Compilation {
            description: CompiledDescription {
                identity: self.identity().clone(),
                operation: description,
                messages: messages.iter().map(|m| m.description.clone()).collect(),
                servers: servers.iter().map(|s| s.description.clone()).collect(),
                reply: operation.get("reply").map(|v| v.source.location()),
                operation_security: operation.get("security").map(|v| v.source.location()),
            },
            document,
            operation,
            channel,
            messages,
            servers,
            channel_message_count,
        })))
    }
}

fn invalid(node: &Json, detail: &str) -> Diagnostic {
    Diagnostic::new(Code::InvalidOperation, detail).at(node.location())
}
fn unsupported(node: &Json, detail: &str, requirement: Option<Requirement>) -> Diagnostic {
    let mut error = Diagnostic::new(Code::UnsupportedFeature, detail).at(node.location());
    error.requirement = requirement;
    error
}
fn map(node: &Json) -> Result<Vec<(String, Json)>, Diagnostic> {
    node.members()
        .ok_or_else(|| invalid(node, "expected an object map"))
}
fn array(node: &Json) -> Result<Vec<Json>, Diagnostic> {
    node.elements()
        .ok_or_else(|| invalid(node, "expected an array"))
}
fn same(a: &Json, b: &Json) -> bool {
    Arc::ptr_eq(&a.source, &b.source) && a.pointer == b.pointer
}
fn required_string(node: &Json, key: &str) -> Result<String, Diagnostic> {
    let value = node
        .get(key)
        .ok_or_else(|| invalid(node, "required string field is absent"))?;
    value
        .as_str()
        .map(str::to_owned)
        .ok_or_else(|| invalid(&value, "expected a string"))
}
fn messages(
    document: &Document,
    operation: &Json,
    channel: &Json,
) -> Result<(Vec<Message>, usize), Diagnostic> {
    let (entries, count) = if document.edition() == Edition::V2_6 {
        let declaration = operation
            .get("message")
            .ok_or_else(|| invalid(operation, "2.x operation message is required"))?;
        let resolved = document.resolve(declaration.clone())?;
        if let Some(alternatives) = resolved.get("oneOf") {
            let items = array(&alternatives)?;
            if items.is_empty() {
                return Err(invalid(&alternatives, "message oneOf must not be empty"));
            }
            let count = items.len();
            (
                items
                    .into_iter()
                    .map(|item| (item.location().pointer.clone(), item))
                    .collect(),
                count,
            )
        } else {
            (
                vec![(declaration.location().pointer.clone(), declaration)],
                1,
            )
        }
    } else {
        three_x_entries(document, operation.get("messages"), channel)?
    };
    Ok((resolve_messages(document, entries)?, count))
}
fn three_x_entries(document: &Document, selected: Option<Json>, channel: &Json) -> Result<(Vec<(String, Json)>, usize), Diagnostic> {
    Ok({
        let channel_entries = match channel.get("messages") {
            Some(m) => map(&m)?,
            None => vec![],
        };
        let count = channel_entries.len();
        if let Some(selected) = selected {
            let mut entries = Vec::new();
            let mut seen = HashSet::new();
            for item in array(&selected)? {
                let target = document.reference_target(&item)?;
                let (key, definition) = channel_entries
                    .iter()
                    .find(|(_, candidate)| same(candidate, &target))
                    .ok_or_else(|| {
                        invalid(
                            &item,
                            "operation messages must reference entries of its channel messages map",
                        )
                    })?;
                if !seen.insert(key.clone()) {
                    return Err(invalid(&item, "operation message reference is duplicated"));
                }
                entries.push((key.clone(), definition.clone()));
            }
            (entries, count)
        } else {
            (channel_entries, count)
        }

    })
}
fn resolve_messages(document: &Document, entries: Vec<(String, Json)>) -> Result<Vec<Message>, Diagnostic> {
    let mut result = Vec::with_capacity(entries.len());
    for (key, selection) in entries {
        let definition = document.resolve(selection.clone())?;
        let effective = with_traits(document, definition.clone(), &["payload", "traits"])?;
        let content_type = effective
            .optional_string("contentType")?
            .or(Effective::from(document.root()).optional_string("defaultContentType")?);
        let description = MessageDescription {
            key,
            selection: selection.location(),
            definition: definition.location(),
            name: effective.optional_string("name")?,
            content_type,
            payload: effective.get("payload").map(|v| v.source.location()),
            headers: effective.get("headers").map(|v| v.source.location()),
            correlation_id: effective.get("correlationId").map(|v| v.source.location()),
        };
        result.push(Message {
            description,
            effective,
        });
    }
    Ok(result)
}
fn servers(document: &Document, channel: &Json) -> Result<Vec<Server>, Diagnostic> {
    let root = document.root();
    let root_entries = match root.get("servers") {
        Some(s) => map(&s)?,
        None => vec![],
    };
    let explicit = channel
        .get("servers")
        .map(|v| array(&v))
        .transpose()?
        .unwrap_or_default();
    let entries = if explicit.is_empty() {
        root_entries.clone()
    } else {
        let mut entries = Vec::new();
        for item in explicit {
            if document.edition() == Edition::V2_6 {
                let key = item
                    .as_str()
                    .ok_or_else(|| invalid(&item, "2.x channel servers must be names"))?;
                let found = root_entries
                    .iter()
                    .find(|(name, _)| key == name)
                    .ok_or_else(|| invalid(&item, "channel names an unknown root server"))?;
                entries.push(found.clone());
            } else {
                let target = document.reference_target(&item)?;
                if let Some(found) = root_entries
                    .iter()
                    .find(|(_, source)| same(source, &target))
                {
                    entries.push(found.clone());
                } else if channel
                    .pointer
                    .strip_prefix("/components/channels/")
                    .is_some_and(|s| !s.contains('/'))
                {
                    entries.push((
                        format!(
                            "{}#{}",
                            target.location().uri.as_deref().unwrap_or(""),
                            target.pointer
                        ),
                        target,
                    ));
                } else {
                    return Err(invalid(
                        &item,
                        "root channel must reference root server entries",
                    ));
                }
            }
        }
        entries
    };
    let mut seen = HashSet::new();
    let mut result = Vec::new();
    for (key, selection) in entries {
        if !seen.insert(key.clone()) {
            return Err(invalid(&selection, "channel server is duplicated"));
        }
        let object = document.resolve(selection.clone())?;
        let protocol = required_string(&object, "protocol")?;
        let protocol_version =
            Effective::from(object.clone()).optional_string("protocolVersion")?;
        result.push(Server {
            description: ServerDescription {
                key,
                selection: selection.location(),
                definition: object.location(),
                protocol,
                protocol_version,
            },
            object,
        });
    }
    Ok(result)
}

/// Document-derived transport settings. Defaults are this profile's explicit
/// choices and will be sent as shown; they are not new AsyncAPI requirements.
#[derive(Clone, Debug, Serialize)]
#[serde(
    tag = "kind",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
pub enum TransportPlan {
    Mqtt311 {
        endpoint: String,
        client_id: String,
        clean_session: bool,
        keep_alive_seconds: u16,
        topic: String,
        qos: u8,
        retain: bool,
    },
    WebSocket6455 {
        endpoint: String,
        method: String,
        frame: WebSocketFrame,
    },
}
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PlanDescription {
    pub identity: OperationIdentity,
    pub role: Role,
    pub application_action: Action,
    pub wire_action: Action,
    pub server: String,
    pub message: String,
    pub content_type: String,
    pub codec: Codec,
    pub transport: TransportPlan,
    pub authentication: crate::AuthenticationPlan,
}
/// A reusable immutable message plan; preparation performs no I/O.
#[derive(Clone, Debug)]
pub struct Plan {
    compiled: CompiledOperation,
    description: Arc<PlanDescription>,
}
impl Plan {
    pub fn describe(&self) -> &PlanDescription {
        &self.description
    }
    pub fn operation(&self) -> &CompiledOperation {
        &self.compiled
    }
    /// Validate encoded bytes without changing them. Binary/UTF-8 need no body
    /// allocation; JSON is parsed for admission. Use decode_payload to retain
    /// that exact view instead of repeating the parse later.
    pub fn prepare_bytes<'a>(&self, bytes: &'a [u8]) -> Result<&'a [u8], Diagnostic> {
        match self.description.codec {
            Codec::Binary => {}
            Codec::Utf8 | Codec::Json => {
                let text = std::str::from_utf8(bytes).map_err(|_| {
                    Diagnostic::new(Code::InvalidValue, "message is not valid UTF-8")
                })?;
                if self.description.codec == Codec::Json {
                    Json::parse(text, Default::default())?;
                }
            }
        }
        Ok(bytes)
    }
    pub fn decode_payload(&self, bytes: impl Into<bytes::Bytes>) -> Result<Payload, Diagnostic> {
        Payload::decode(self.description.codec, bytes.into(), Default::default())
    }
    /// Check a typed payload against the prepared codec. Schema-bearing messages
    /// still require an evaluator at preparation; this does not waive that rule.
    pub fn prepare_payload(&self, payload: &Payload) -> Result<(), Diagnostic> {
        if payload.codec() != self.description.codec {
            return Err(Diagnostic::new(
                Code::InvalidValue,
                "payload representation does not match the prepared codec",
            ));
        }
        Ok(())
    }
    pub fn websocket_frame(&self) -> Option<WebSocketFrame> {
        match self.description.transport {
            TransportPlan::WebSocket6455 { frame, .. } => Some(frame),
            _ => None,
        }
    }
}
impl CompiledOperation {
    pub fn describe(&self) -> &CompiledDescription {
        &self.0.description
    }
    /// Resolve this message's effective correlation declaration on demand.
    /// Absence is distinct from an unknown message key or a broken declaration.
    /// Inspection does not assert transport or request/reply execution support.
    pub fn correlation(
        &self,
        message: &str,
    ) -> Result<Option<crate::CorrelationDescription>, Diagnostic> {
        let message = self
            .0
            .messages
            .iter()
            .find(|m| m.description.key == message)
            .ok_or_else(|| {
                Diagnostic::new(
                    Code::InvalidConfiguration,
                    "selected message is not available",
                )
            })?;
        message
            .effective
            .get("correlationId")
            .map(|declaration| crate::expression::correlation(&self.0.document, declaration))
            .transpose()
    }
    /// Inspect security alternatives for a server, including schemes outside
    /// current execution profiles. Resolves every alternative; preparation only
    /// resolves the selected alternatives. No credentials are acquired.
    pub fn authentication(
        &self,
        server: &str,
    ) -> Result<crate::AuthenticationDescription, Diagnostic> {
        let server = self
            .0
            .servers
            .iter()
            .find(|s| s.description.key == server)
            .ok_or_else(|| {
                Diagnostic::new(
                    Code::InvalidConfiguration,
                    "selected server is not available",
                )
            })?;
        crate::authentication::inspect(
            &self.0.document,
            server.object.get("security").as_ref(),
            self.0
                .operation
                .get("security")
                .map(|v| v.source.clone())
                .as_ref(),
        )
    }
    pub fn message_source(&self, key: &str) -> Option<Json> {
        self.0
            .messages
            .iter()
            .find(|m| m.description.key == key)
            .map(|m| m.effective.source.clone())
    }
    pub fn server_source(&self, key: &str) -> Option<Json> {
        self.0
            .servers
            .iter()
            .find(|s| s.description.key == key)
            .map(|s| s.object.clone())
    }
    pub fn prepare(&self, options: &PlanOptions) -> Result<Plan, Diagnostic> {
        self.prepare_message(options, PreparationContext::operation(&self.0), false, false)
    }
    fn prepare_message(
        &self,
        options: &PlanOptions,
        c: PreparationContext<'_>,
        allow_reply: bool,
        allow_correlation: bool,
    ) -> Result<Plan, Diagnostic> {
        let supplied_bytes = options
            .variables
            .iter()
            .chain(options.parameters.iter())
            .flat_map(|(k, v)| [k.len(), v.len()])
            .chain([
                options.address.as_ref().map_or(0, String::len),
                options.client_id.as_ref().map_or(0, String::len),
            ])
            .fold(0usize, usize::saturating_add);
        if supplied_bytes > c.document.limits().source_bytes {
            return Err(Diagnostic::new(
                Code::Limit,
                "preparation input byte limit exceeded",
            ));
        }
        if c.messages.is_empty() {
            return Err(Diagnostic::new(
                Code::NoMessages,
                "operation declares no messages to exchange",
            ));
        }
        let server_keys: Vec<_> = c
            .servers
            .iter()
            .map(|s| s.description.key.clone())
            .collect();
        let server_index = choose(
            &server_keys,
            options.server.as_deref(),
            Requirement::Server {
                choices: server_keys.clone(),
            },
        )?;
        let server = &c.servers[server_index];
        let message_keys: Vec<_> = c
            .messages
            .iter()
            .map(|s| s.description.key.clone())
            .collect();
        let message_index = choose(
            &message_keys,
            options.message.as_deref(),
            Requirement::Message {
                choices: message_keys.clone(),
            },
        )?;
        let message = &c.messages[message_index];
        if c.channel_message_count != 1 {
            return Err(unsupported(
                &c.channel,
                "multiple-message channel classification needs an evaluator",
                Some(Requirement::Evaluator),
            ));
        }
        if let Some(reply) = c.operation.get("reply") && !allow_reply {
            return Err(unsupported(
                &reply.source,
                "declared reply needs a reply-capable plan",
                Some(Requirement::Reply),
            ));
        }
        for field in ["payload", "headers", "correlationId"] {
            if field == "correlationId" && allow_correlation { continue; }
            if let Some(value) = message.effective.get(field) {
                return Err(unsupported(
                    &value.source,
                    "message declaration requires schema or correlation support",
                    Some(Requirement::Evaluator),
                ));
            }
        }
        let content_type = message.description.content_type.as_deref();
        let codec = content_type.and_then(crate::codec::select).ok_or_else(|| {
            unsupported(
                &message.effective.source,
                "content type needs a supported payload codec",
                Some(Requirement::Codec {
                    content_type: message.description.content_type.clone(),
                }),
            )
        })?;
        let action = c.action;
        let wire_action = match (options.role, action) {
            (Role::Application, a) => a,
            (Role::Peer, Action::Send) => Action::Receive,
            (Role::Peer, Action::Receive) => Action::Send,
        };
        let protocol = server.description.protocol.as_str();
        let profile = match protocol {
            "mqtt" | "mqtts" => match (&server.description.protocol_version, options.profile) {
                (Some(v), _) if v != "3.1.1" => {
                    return Err(Diagnostic::new(
                        Code::UnsupportedProtocol,
                        "MQTT version is outside the initial 3.1.1 profile",
                    )
                    .at(server.object.location()));
                }
                (_, Some(ProtocolProfile::WebSocket6455)) => {
                    return Err(Diagnostic::new(
                        Code::InvalidConfiguration,
                        "profile contradicts server protocol",
                    ));
                }
                (None, None) => {
                    return Err(Diagnostic::new(
                        Code::MissingConfiguration,
                        "server needs a protocol profile",
                    )
                    .needs(Requirement::ProtocolProfile));
                }
                _ => ProtocolProfile::Mqtt311,
            },
            "ws" | "wss" => {
                if options.profile == Some(ProtocolProfile::Mqtt311) {
                    return Err(Diagnostic::new(
                        Code::InvalidConfiguration,
                        "profile contradicts server protocol",
                    ));
                }
                if server
                    .description
                    .protocol_version
                    .as_deref()
                    .is_some_and(|v| v != "13")
                {
                    return Err(Diagnostic::new(
                        Code::UnsupportedProtocol,
                        "WebSocket version is outside the RFC 6455 profile",
                    )
                    .at(server.object.location()));
                }
                ProtocolProfile::WebSocket6455
            }
            _ => {
                return Err(Diagnostic::new(
                    Code::UnsupportedProtocol,
                    "server protocol has no preparation profile yet",
                )
                .at(server.object.location()));
            }
        };
        let authentication = crate::authentication::prepare(
            &c.document,
            server.object.get("security").as_ref(),
            c.operation
                .get("security")
                .map(|v| v.source.clone())
                .as_ref(),
            &options.security,
            protocol,
        )?;
        let endpoint = endpoint(&c.document, &server.object, protocol, &options.variables)?;
        let address = address(
            &c.document,
            &c.channel,
            c.address,
            options,
        )?;
        if profile == ProtocolProfile::Mqtt311 && options.websocket_frame.is_some() {
            return Err(Diagnostic::new(
                Code::InvalidConfiguration,
                "WebSocket framing does not apply to MQTT",
            ));
        }
        let mut transport = match profile {
            ProtocolProfile::Mqtt311 => {
                mqtt(&c, server, message, options, wire_action, endpoint, address)?
            }
            ProtocolProfile::WebSocket6455 => {
                websocket(&c, server, message, options, endpoint, address)?
            }
        };
        if let TransportPlan::WebSocket6455 { frame, .. } = &mut transport {
            *frame = options
                .websocket_frame
                .unwrap_or(if codec == Codec::Binary {
                    WebSocketFrame::Binary
                } else {
                    WebSocketFrame::Text
                });
            if codec == Codec::Binary && *frame == WebSocketFrame::Text {
                return Err(Diagnostic::new(
                    Code::InvalidConfiguration,
                    "binary content cannot use the UTF-8 text frame profile",
                ));
            }
        }
        Ok(Plan {
            compiled: self.clone(),
            description: Arc::new(PlanDescription {
                identity: c.description.identity.clone(),
                role: options.role,
                application_action: action,
                wire_action,
                server: server.description.key.clone(),
                message: message.description.key.clone(),
                content_type: content_type.unwrap().into(),
                codec,
                transport,
                authentication,
            }),
        })
    }
}
fn choose(
    keys: &[String],
    explicit: Option<&str>,
    requirement: Requirement,
) -> Result<usize, Diagnostic> {
    if let Some(key) = explicit {
        return keys.iter().position(|k| k == key).ok_or_else(|| {
            Diagnostic::new(
                Code::InvalidConfiguration,
                "selected choice is not available on this operation",
            )
        });
    }
    if keys.len() == 1 {
        Ok(0)
    } else {
        Err(Diagnostic::new(
            Code::MissingConfiguration,
            "operation requires an explicit available choice",
        )
        .needs(requirement))
    }
}

fn binding(
    document: &Document,
    parent: &Effective,
    name: &str,
    revision: Option<&str>,
    allowed: &[&str],
) -> Result<Option<Effective>, Diagnostic> {
    let Some(bindings) = parent.get("bindings") else {
        return Ok(None);
    };
    let bindings = bindings.resolve(document)?;
    if !bindings.is_object() {
        return Err(invalid(&bindings.source, "bindings must be an object"));
    }
    let Some(binding) = bindings.get(name) else {
        return Ok(None);
    };
    let binding = binding.resolve(document)?;
    let members = binding
        .members()
        .ok_or_else(|| invalid(&binding.source, "protocol binding must be an object"))?;
    for (key, value) in members {
        if key == "bindingVersion" && revision.is_some() {
            if value.string() != revision {
                return Err(Diagnostic::new(
                    Code::UnsupportedBinding,
                    "binding revision is not supported by this profile",
                )
                .at(value.source.location()));
            }
        } else if !allowed.contains(&key.as_str()) {
            return Err(Diagnostic::new(
                Code::UnsupportedBinding,
                "binding field is not defined at this scope or implemented by this profile",
            )
            .at(value.source.location()));
        }
    }
    Ok(Some(binding))
}
fn bool_field(binding: Option<&Effective>, name: &str, default: bool) -> Result<bool, Diagnostic> {
    match binding.and_then(|b| b.get(name)) {
        None => Ok(default),
        Some(v) => v
            .source
            .as_bool()
            .ok_or_else(|| invalid(&v.source, "binding field must be boolean")),
    }
}
fn int_field(
    binding: Option<&Effective>,
    name: &str,
    default: u64,
    max: u64,
) -> Result<u64, Diagnostic> {
    match binding.and_then(|b| b.get(name)) {
        None => Ok(default),
        Some(v) => v
            .source
            .number_text()
            .and_then(exact_nonnegative_integer)
            .filter(|n| *n <= max)
            .ok_or_else(|| invalid(&v.source, "binding integer is outside the protocol range")),
    }
}
fn mqtt(
    c: &PreparationContext<'_>,
    server: &Server,
    message: &Message,
    options: &PlanOptions,
    action: Action,
    endpoint: String,
    topic: String,
) -> Result<TransportPlan, Diagnostic> {
    let sb = binding(
        &c.document,
        &Effective::from(server.object.clone()),
        "mqtt",
        Some("0.2.0"),
        &[
            "clientId",
            "cleanSession",
            "keepAlive",
            "lastWill",
            "sessionExpiryInterval",
            "maximumPacketSize",
        ],
    )?;
    let ob = binding(
        &c.document,
        &c.operation,
        "mqtt",
        Some("0.2.0"),
        &["qos", "retain", "messageExpiryInterval"],
    )?;
    binding(
        &c.document,
        &Effective::from(c.channel.clone()),
        "mqtt",
        None,
        &[],
    )?;
    let mb = binding(
        &c.document,
        &message.effective,
        "mqtt",
        Some("0.2.0"),
        &[
            "payloadFormatIndicator",
            "correlationData",
            "contentType",
            "responseTopic",
        ],
    )?;
    for (binding, fields) in [
        (&sb, &["sessionExpiryInterval", "maximumPacketSize"][..]),
        (&ob, &["messageExpiryInterval"][..]),
        (
            &mb,
            &[
                "payloadFormatIndicator",
                "correlationData",
                "contentType",
                "responseTopic",
            ][..],
        ),
    ] {
        for field in fields {
            if let Some(v) = binding.as_ref().and_then(|b| b.get(field)) {
                return Err(Diagnostic::new(
                    Code::UnsupportedProtocol,
                    "MQTT 5 field cannot be used in the 3.1.1 profile",
                )
                .at(v.source.location()));
            }
        }
    }
    if let Some(v) = sb.as_ref().and_then(|b| b.get("lastWill")) {
        return Err(unsupported(
            &v.source,
            "last-will preparation is not implemented yet",
            None,
        ));
    }
    let authored_id = sb
        .as_ref()
        .map(|b| b.optional_string("clientId"))
        .transpose()?
        .flatten();
    let client_id = match (&options.client_id, options.role) {
        (Some(id), Role::Peer) if Some(id) == authored_id.as_ref() => {
            return Err(Diagnostic::new(
                Code::InvalidConfiguration,
                "peer must not reuse the described application's MQTT client identity",
            ));
        }
        (Some(id), _) => id.clone(),
        (None, Role::Application) if authored_id.is_some() => authored_id.unwrap(),
        _ => {
            return Err(Diagnostic::new(
                Code::MissingConfiguration,
                "MQTT requires a caller client identity",
            )
            .needs(Requirement::ClientIdentity));
        }
    };
    if client_id.is_empty() || client_id.len() > 65535 || client_id.contains('\0') {
        return Err(Diagnostic::new(
            Code::InvalidConfiguration,
            "MQTT client identity is invalid",
        ));
    }
    let qos = int_field(ob.as_ref(), "qos", 0, 2)? as u8;
    let retain = bool_field(ob.as_ref(), "retain", false)?;
    if action == Action::Receive && ob.as_ref().and_then(|b| b.get("retain")).is_some() {
        return Err(Diagnostic::new(
            Code::InvalidConfiguration,
            "publish-only retain binding cannot configure a subscription",
        ));
    }
    if topic.is_empty() || topic.len() > 65535 || topic.contains('\0') {
        return Err(Diagnostic::new(
            Code::InvalidConfiguration,
            "MQTT topic or filter is invalid",
        ));
    }
    if action == Action::Send && (topic.contains('#') || topic.contains('+')) {
        return Err(Diagnostic::new(
            Code::InvalidConfiguration,
            "MQTT publish topic cannot contain wildcards",
        ));
    }
    if action == Action::Receive {
        let levels: Vec<_> = topic.split('/').collect();
        for (i, level) in levels.iter().enumerate() {
            if (level.contains('+') && *level != "+")
                || (level.contains('#') && (*level != "#" || i + 1 != levels.len()))
            {
                return Err(Diagnostic::new(
                    Code::InvalidConfiguration,
                    "MQTT subscription wildcard placement is invalid",
                ));
            }
        }
    }
    Ok(TransportPlan::Mqtt311 {
        endpoint,
        topic,
        qos,
        retain,
        client_id,
        clean_session: bool_field(sb.as_ref(), "cleanSession", true)?,
        keep_alive_seconds: int_field(sb.as_ref(), "keepAlive", 60, 65535)? as u16,
    })
}
fn websocket(
    c: &PreparationContext<'_>,
    server: &Server,
    message: &Message,
    options: &PlanOptions,
    mut endpoint: String,
    address: String,
) -> Result<TransportPlan, Diagnostic> {
    if options.role == Role::Peer {
        return Err(Diagnostic::new(
            Code::MissingConfiguration,
            "WebSocket peer role requires an established connection or hosting route",
        )
        .needs(Requirement::PeerRoute));
    }
    if options.client_id.is_some() {
        return Err(Diagnostic::new(
            Code::InvalidConfiguration,
            "MQTT client identity does not apply to WebSocket",
        ));
    }
    binding(
        &c.document,
        &Effective::from(server.object.clone()),
        "ws",
        None,
        &[],
    )?;
    binding(&c.document, &c.operation, "ws", None, &[])?;
    binding(&c.document, &message.effective, "ws", None, &[])?;
    let cb = binding(
        &c.document,
        &Effective::from(c.channel.clone()),
        "ws",
        Some("0.1.0"),
        &["method", "query", "headers"],
    )?;
    let method = cb
        .as_ref()
        .map(|b| b.optional_string("method"))
        .transpose()?
        .flatten()
        .unwrap_or("GET".into());
    if method != "GET" {
        return Err(Diagnostic::new(
            Code::UnsupportedProtocol,
            "RFC 6455 profile requires the GET opening handshake",
        )
        .at(c.channel.location()));
    }
    for field in ["query", "headers"] {
        if let Some(v) = cb.as_ref().and_then(|b| b.get(field)) {
            return Err(unsupported(
                &v.source,
                "handshake values require schema-backed preparation",
                Some(Requirement::Evaluator),
            ));
        }
    }
    // An authored channel path is appended to the server's base path, never URL
    // resolved: a channel cannot replace the server authority or inject a query.
    if !address.is_empty() {
        if !address.starts_with('/')
            || address.starts_with("//")
            || address.contains(['?', '#', '\\'])
        {
            return Err(Diagnostic::new(
                Code::InvalidConfiguration,
                "WebSocket channel address must be an absolute path without query or fragment",
            ));
        }
        endpoint = format!("{}{}", endpoint.trim_end_matches('/'), address);
    }
    validate_endpoint(&endpoint, "ws")?;
    Ok(TransportPlan::WebSocket6455 {
        endpoint,
        method,
        frame: WebSocketFrame::Binary,
    })
}

fn endpoint(
    document: &Document,
    server: &Json,
    protocol: &str,
    supplied: &BTreeMap<String, String>,
) -> Result<String, Diagnostic> {
    let templates = if document.edition() == Edition::V2_6 {
        vec![required_string(server, "url")?]
    } else {
        vec![
            required_string(server, "host")?,
            Effective::from(server.clone())
                .optional_string("pathname")?
                .unwrap_or_default(),
        ]
    };
    let values = substitutions(
        document,
        server.get("variables"),
        &templates,
        supplied,
        false,
    )?;
    let mut endpoint = if templates.len() == 1 {
        let s = substitute(&templates[0], &values, document.limits().source_bytes)?;
        if s.contains("://") {
            s
        } else {
            format!("{protocol}://{s}")
        }
    } else {
        let host = substitute(&templates[0], &values, document.limits().source_bytes)?;
        let path = substitute(&templates[1], &values, document.limits().source_bytes)?;
        if host.contains(['/', '?', '#', '@', '\\'])
            || (!path.is_empty() && !path.starts_with('/'))
            || path.contains(['?', '#', '\\'])
        {
            return Err(invalid(
                server,
                "host and pathname must remain separate URI components",
            ));
        }
        format!("{protocol}://{host}{path}")
    };
    let expected = if protocol.starts_with("mqtt") {
        "mqtt"
    } else {
        "ws"
    };
    validate_endpoint(&endpoint, expected)?;
    let uri = fluent_uri::Uri::parse(endpoint.as_str()).unwrap();
    if uri.scheme().as_str() != protocol {
        return Err(invalid(
            server,
            "server URL scheme contradicts the declared protocol",
        ));
    }
    if expected == "mqtt" && !uri.path().as_str().is_empty() && uri.path().as_str() != "/" {
        return Err(invalid(
            server,
            "MQTT TCP endpoint cannot contain an application path",
        ));
    }
    if expected == "mqtt" {
        endpoint = endpoint.trim_end_matches('/').to_owned();
    }
    Ok(endpoint)
}
fn validate_endpoint(endpoint: &str, protocol: &str) -> Result<(), Diagnostic> {
    let uri = fluent_uri::Uri::parse(endpoint).map_err(|_| {
        Diagnostic::new(
            Code::InvalidConfiguration,
            "endpoint is not a valid absolute URI",
        )
    })?;
    let authority = uri.authority().ok_or_else(|| {
        Diagnostic::new(Code::InvalidConfiguration, "endpoint requires an authority")
    })?;
    if authority.host().is_empty()
        || authority.userinfo().is_some()
        || uri.has_fragment()
        || uri.query().is_some()
        || !(uri.scheme().as_str() == protocol || uri.scheme().as_str() == format!("{protocol}s"))
    {
        return Err(Diagnostic::new(
            Code::InvalidConfiguration,
            "endpoint has unsupported authority, scheme, query or fragment",
        ));
    }
    if let Some(port) = authority.port()
        && port.as_str().parse::<u16>().is_err()
    {
        return Err(Diagnostic::new(
            Code::InvalidConfiguration,
            "endpoint port is invalid",
        ));
    }
    Ok(())
}
fn address(
    document: &Document,
    channel: &Json,
    authored: Option<&str>,
    options: &PlanOptions,
) -> Result<String, Diagnostic> {
    if authored.is_some() && options.address.is_some() {
        return Err(Diagnostic::new(
            Code::InvalidConfiguration,
            "a concrete authored address cannot be replaced; supply its parameters",
        ));
    }
    let template = authored.or(options.address.as_deref()).ok_or_else(|| {
        Diagnostic::new(
            Code::MissingConfiguration,
            "channel needs a concrete address",
        )
        .needs(Requirement::Address)
    })?;
    let values = substitutions(
        document,
        channel.get("parameters"),
        &[template.to_owned()],
        &options.parameters,
        true,
    )?;
    substitute(template, &values, document.limits().source_bytes)
}
fn template_names(template: &str) -> Result<HashSet<String>, Diagnostic> {
    let mut names = HashSet::new();
    let mut rest = template;
    while let Some((before, after)) = rest.split_once('{') {
        if before.contains('}') {
            return Err(Diagnostic::new(
                Code::InvalidOperation,
                "unbalanced address expression",
            ));
        }
        let (name, next) = after.split_once('}').ok_or_else(|| {
            Diagnostic::new(Code::InvalidOperation, "unbalanced address expression")
        })?;
        if name.is_empty() || name.contains('{') {
            return Err(Diagnostic::new(
                Code::InvalidOperation,
                "invalid address expression",
            ));
        }
        names.insert(name.into());
        rest = next;
    }
    if rest.contains('}') {
        return Err(Diagnostic::new(
            Code::InvalidOperation,
            "unbalanced address expression",
        ));
    }
    Ok(names)
}
fn substitutions(
    document: &Document,
    declarations: Option<Json>,
    templates: &[String],
    supplied: &BTreeMap<String, String>,
    parameters: bool,
) -> Result<BTreeMap<String, String>, Diagnostic> {
    let names: HashSet<_> = templates
        .iter()
        .map(|s| template_names(s))
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .flatten()
        .collect();
    let declarations: BTreeMap<_, _> = declarations
        .map(|v| map(&v))
        .transpose()?
        .unwrap_or_default()
        .into_iter()
        .collect();
    if supplied.keys().any(|k| !names.contains(k))
        || declarations.keys().any(|k| !names.contains(k))
    {
        return Err(Diagnostic::new(
            Code::InvalidConfiguration,
            "supplied or declared parameter is not used by its template",
        ));
    }
    let mut values = BTreeMap::new();
    // Ordered traversal makes the first actionable requirement deterministic.
    let names: std::collections::BTreeSet<_> = names.into_iter().collect();
    for name in names {
        let declaration = declarations.get(&name).ok_or_else(|| {
            Diagnostic::new(
                Code::InvalidOperation,
                "template expression has no declaration",
            )
        })?;
        let declaration = document.resolve(declaration.clone())?;
        if declaration.kind() != "object" {
            return Err(invalid(
                &declaration,
                "variable declaration must be an object",
            ));
        }
        if parameters && document.edition() == Edition::V2_6 && declaration.get("schema").is_some()
        {
            return Err(unsupported(
                &declaration,
                "2.x parameter schema requires an evaluator",
                Some(Requirement::Evaluator),
            ));
        }
        let value = supplied
            .get(&name)
            .cloned()
            .or(Effective::from(declaration.clone()).optional_string("default")?)
            .ok_or_else(|| {
                Diagnostic::new(Code::MissingConfiguration, "template needs a value")
                    .at(declaration.location())
                    .needs(if parameters {
                        Requirement::Parameter { name: name.clone() }
                    } else {
                        Requirement::Variable { name: name.clone() }
                    })
            })?;
        if value.contains(['{', '}', '\0'])
            || (!parameters && value.contains(['/', '?', '#', '@', '\\']))
        {
            return Err(Diagnostic::new(
                Code::InvalidConfiguration,
                "template value would alter URI structure or add an expression",
            ));
        }
        if let Some(allowed) = declaration.get("enum") {
            let allowed = array(&allowed)?;
            if allowed.iter().any(|v| v.as_str().is_none()) {
                return Err(invalid(&declaration, "variable enum must contain strings"));
            }
            if !allowed.iter().any(|v| v.as_str() == Some(&value)) {
                return Err(Diagnostic::new(
                    Code::InvalidConfiguration,
                    "value is outside its declared enumeration",
                )
                .at(declaration.location()));
            }
        }
        values.insert(name, value);
    }
    Ok(values)
}
fn substitute(
    template: &str,
    values: &BTreeMap<String, String>,
    limit: usize,
) -> Result<String, Diagnostic> {
    let mut output = String::with_capacity(template.len());
    let mut rest = template;
    while let Some((before, after)) = rest.split_once('{') {
        let (name, next) = after
            .split_once('}')
            .ok_or_else(|| Diagnostic::new(Code::InvalidOperation, "unbalanced template"))?;
        let value = values
            .get(name)
            .ok_or_else(|| Diagnostic::new(Code::MissingConfiguration, "template value absent"))?;
        if output
            .len()
            .saturating_add(before.len())
            .saturating_add(value.len())
            > limit
        {
            return Err(Diagnostic::new(
                Code::Limit,
                "expanded address byte limit exceeded",
            ));
        }
        output.push_str(before);
        output.push_str(value);
        rest = next;
    }
    if output.len().saturating_add(rest.len()) > limit {
        return Err(Diagnostic::new(
            Code::Limit,
            "expanded address byte limit exceeded",
        ));
    }
    output.push_str(rest);
    Ok(output)
}

// Integer-valued JSON numbers include 1.0 and 1e0. Never round through f64.
fn exact_nonnegative_integer(text: &str) -> Option<u64> {
    let negative = text.starts_with('-');
    let text = text.strip_prefix('-').unwrap_or(text);
    let (mantissa, exponent) = text.split_once(['e', 'E']).unwrap_or((text, "0"));
    let fractional = mantissa.split_once('.').map_or(0, |(_, f)| f.len());
    let digits = mantissa.replace('.', "");
    let digits = digits.trim_start_matches('0');
    if digits.is_empty() {
        return Some(0);
    }
    if negative {
        return None;
    }
    let scale = exponent
        .parse::<i64>()
        .ok()?
        .checked_sub(i64::try_from(fractional).ok()?)?;
    let mut result;
    if scale < 0 {
        let removed = usize::try_from(scale.unsigned_abs()).ok()?;
        let end = digits.len().checked_sub(removed)?;
        if !digits[end..].bytes().all(|b| b == b'0') {
            return None;
        }
        result = digits[..end].parse::<u64>().ok()?;
    } else {
        if digits.len().saturating_add(usize::try_from(scale).ok()?) > 20 {
            return None;
        }
        result = digits.parse::<u64>().ok()?;
        for _ in 0..scale {
            result = result.checked_mul(10)?;
        }
    }
    Some(result)
}
