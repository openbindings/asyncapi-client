//! Pure reply topology and exchange preparation. No sessions or hidden I/O.
use super::*;
use crate::{CorrelationDescription, ExpressionSource, RuntimeExpression};

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ReplyAddressDescription {
    pub selection: Location,
    pub definition: Location,
    pub location: Location,
    pub expression: RuntimeExpression,
}
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ReplyDescription {
    pub selection: Location,
    pub definition: Location,
    pub application_action: Action,
    pub channel_selection: Option<Location>,
    pub channel: Option<Location>,
    pub address: Option<String>,
    pub address_expression: Option<ReplyAddressDescription>,
    /// None means unknown channel; an empty list admits no messages.
    pub messages: Option<Vec<MessageDescription>>,
    pub servers: Vec<ServerDescription>,
}
#[derive(Clone, Debug)]
struct ReplyCompilation {
    operation: CompiledOperation,
    description: ReplyDescription,
    channel: Option<Json>,
    messages: Vec<Message>,
    servers: Vec<Server>,
    channel_message_count: usize,
}
/// Owning native 3.x reply topology, retaining its original operation identity.
#[derive(Clone, Debug)]
pub struct CompiledReply(Arc<ReplyCompilation>);
impl CompiledReply {
    pub fn describe(&self) -> &ReplyDescription {
        &self.0.description
    }
    pub fn operation(&self) -> &CompiledOperation {
        &self.0.operation
    }
    pub fn correlation(&self, message: &str) -> Result<Option<CorrelationDescription>, Diagnostic> {
        let selected = self
            .0
            .messages
            .iter()
            .find(|m| m.description.key == message)
            .ok_or_else(|| {
                Diagnostic::new(
                    Code::InvalidConfiguration,
                    "selected reply message is not available",
                )
            })?;
        selected
            .effective
            .get("correlationId")
            .map(|v| crate::expression::correlation(&self.0.operation.0.document, v))
            .transpose()
    }
}
fn reverse(action: Action) -> Action {
    match action {
        Action::Send => Action::Receive,
        Action::Receive => Action::Send,
    }
}
fn component(node: &Json, collection: &str) -> bool {
    node.pointer
        .strip_prefix(&format!("/components/{collection}/"))
        .is_some_and(|key| !key.contains('/'))
}
fn reference_object(value: &Effective) -> Result<Json, Diagnostic> {
    let reference = value
        .get("$ref")
        .ok_or_else(|| invalid(&value.source, "reply channel must be a reference"))?;
    let (parent, _) = reference.source.pointer.rsplit_once('/').unwrap();
    Ok(reference.source.source.root().pointer(parent).unwrap())
}
impl CompiledOperation {
    /// Resolve replies lazily; omission stays unknown. Native 2.x request/reply
    /// requires an explicitly configured operation pair.
    pub fn reply(&self) -> Result<Option<CompiledReply>, Diagnostic> {
        let Some(selection) = self.0.operation.get("reply") else {
            return Ok(None);
        };
        if self.0.document.edition() == Edition::V2_6 {
            return Err(unsupported(
                &selection.source,
                "operation reply is not a native 2.x field",
                Some(Requirement::Reply),
            ));
        }
        let definition = selection.resolve(&self.0.document)?;
        if !definition.is_object() {
            return Err(invalid(&definition.source, "reply must be an object"));
        }
        let address_expression = definition
            .get("address")
            .map(|selection| {
                let address = selection.resolve(&self.0.document)?;
                if !address.is_object() {
                    return Err(invalid(&address.source, "reply address must be an object"));
                }
                let location = address.get("location").ok_or_else(|| {
                    invalid(&address.source, "reply address location is required")
                })?;
                let text = location.string().ok_or_else(|| {
                    invalid(&location.source, "reply address location must be a string")
                })?;
                let expression = RuntimeExpression::parse(text).map_err(|mut e| {
                    if e.code == Code::InvalidValue {
                        e.code = Code::InvalidOperation;
                    }
                    e.at(location.source.location())
                })?;
                Ok(ReplyAddressDescription {
                    selection: selection.source.location(),
                    definition: address.source.location(),
                    location: location.source.location(),
                    expression,
                })
            })
            .transpose()?;
        let mut channel_selection = None;
        let mut channel = None;
        let mut address = None;
        let mut reply_messages = Vec::new();
        let mut reply_servers = Vec::new();
        let mut count = 0;
        if let Some(declaration) = definition.get("channel") {
            let reference = reference_object(&declaration)?;
            let target = self.0.document.reference_target(&reference)?;
            let root = self.0.document.root();
            let relaxed = component(&definition.source, "replies")
                || component(&self.0.operation.source, "operations");
            if !relaxed
                && (!Arc::ptr_eq(&target.source, &root.source)
                    || !target
                        .pointer
                        .strip_prefix("/channels/")
                        .is_some_and(|key| !key.contains('/')))
            {
                return Err(invalid(
                    &reference,
                    "root operation reply must reference a root channel entry",
                ));
            }
            let resolved = self.0.document.resolve(reference.clone())?;
            if resolved.kind() != "object" {
                return Err(invalid(&resolved, "reply channel must be an object"));
            }
            address = match resolved.get("address") {
                Some(value) if value.is_null() => None,
                _ => Effective::from(resolved.clone()).optional_string("address")?,
            };
            if address_expression.is_some() && address.is_some() {
                return Err(invalid(
                    &resolved,
                    "a dynamic reply address requires an unknown channel address",
                ));
            }
            let (entries, message_count) = three_x_entries(
                &self.0.document,
                definition.get("messages").map(|v| v.source),
                &resolved,
            )?;
            reply_messages = resolve_messages(&self.0.document, entries)?;
            reply_servers = servers(&self.0.document, &resolved)?;
            count = message_count;
            channel_selection = Some(reference.location());
            channel = Some(resolved);
        } else if let Some(messages) = definition.get("messages") {
            array(&messages.source)?;
            return Err(Diagnostic::new(
                Code::MissingConfiguration,
                "reply message selection needs a reply channel",
            )
            .at(messages.source.location())
            .needs(Requirement::ReplyChannel));
        }
        let description = ReplyDescription {
            selection: selection.source.location(),
            definition: definition.source.location(),
            application_action: reverse(self.0.description.operation.action),
            channel_selection,
            channel: channel.as_ref().map(Json::location),
            address,
            address_expression,
            messages: channel.as_ref().map(|_| {
                reply_messages
                    .iter()
                    .map(|m| m.description.clone())
                    .collect()
            }),
            servers: reply_servers
                .iter()
                .map(|s| s.description.clone())
                .collect(),
        };
        Ok(Some(CompiledReply(Arc::new(ReplyCompilation {
            operation: self.clone(),
            description,
            channel,
            messages: reply_messages,
            servers: reply_servers,
            channel_message_count: count,
        }))))
    }
}

/// One request and its declared reply. Completion is the first matching reply.
/// This policy is supplied by the library, not inferred from AsyncAPI.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ExchangeOptions {
    pub request: PlanOptions,
    pub reply: PlanOptions,
    pub timeout_ms: u32,
}
impl ExchangeOptions {
    pub fn first(request: PlanOptions, reply: PlanOptions, timeout_ms: u32) -> Self {
        Self {
            request,
            reply,
            timeout_ms,
        }
    }
}
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ExchangeDescription {
    pub request: PlanDescription,
    pub reply: PlanDescription,
    pub declaration: ReplyDescription,
    pub request_correlation: RuntimeExpression,
    pub reply_correlation: RuntimeExpression,
    pub timeout_ms: u32,
}
/// Pure, owning plan for a native 3.x, single-response WebSocket exchange.
/// Both sides retain the same authored operation identity.
#[derive(Clone, Debug)]
pub struct ExchangePlan {
    request: Plan,
    reply: Plan,
    description: Arc<ExchangeDescription>,
}
impl ExchangePlan {
    pub fn describe(&self) -> &ExchangeDescription {
        &self.description
    }
    pub fn request(&self) -> &Plan {
        &self.request
    }
    pub fn reply(&self) -> &Plan {
        &self.reply
    }
    /// String keys compare decoded contents exactly. The initial policy requires
    /// a nonempty string of at most 1024 UTF-8 bytes. It never injects an ID.
    pub fn request_key(&self, payload: &Json) -> Result<String, Diagnostic> {
        let key = self
            .description
            .request_correlation
            .evaluate(None, Some(payload));
        key.as_ref()
            .and_then(Json::as_str)
            .filter(|s| !s.is_empty() && s.len() <= 1024)
            .map(str::to_owned)
            .ok_or_else(|| {
                Diagnostic::new(
                    Code::InvalidValue,
                    "request correlation must be a nonempty string of at most 1024 bytes",
                )
            })
    }
    /// Missing, null, numeric and different string IDs do not match. No coercion.
    pub fn matches_reply(&self, key: &str, payload: &Json) -> bool {
        self.description
            .reply_correlation
            .evaluate(None, Some(payload))
            .as_ref()
            .and_then(Json::as_str)
            == Some(key)
    }
}
fn correlation_expression(
    declared: Option<CorrelationDescription>,
    side: &str,
    codec: Codec,
) -> Result<RuntimeExpression, Diagnostic> {
    let declared = declared.ok_or_else(|| {
        Diagnostic::new(
            Code::MissingConfiguration,
            "exchange requires a declared correlation location on each message",
        )
        .needs(Requirement::Correlation { side: side.into() })
    })?;
    if codec != Codec::Json || declared.expression.source() != ExpressionSource::Payload {
        return Err(Diagnostic::new(
            Code::UnsupportedFeature,
            "initial exchange preparation requires JSON payload correlation",
        )
        .at(declared.location)
        .needs(Requirement::Correlation { side: side.into() }));
    }
    Ok(declared.expression)
}
impl CompiledOperation {
    /// Prepare a declared 3.x reply on the same WebSocket connection. Static or
    /// explicitly configured addresses are supported; dynamic routes refuse.
    pub fn prepare_exchange(&self, options: &ExchangeOptions) -> Result<ExchangePlan, Diagnostic> {
        if options.request.role != options.reply.role
            || !(1..=300_000).contains(&options.timeout_ms)
        {
            return Err(Diagnostic::new(
                Code::InvalidConfiguration,
                "exchange requires one role and a deadline between 1 and 300000 milliseconds",
            ));
        }
        let reply = self.reply()?.ok_or_else(|| {
            Diagnostic::new(
                Code::MissingConfiguration,
                "exchange requires a declared 3.x reply",
            )
            .needs(Requirement::Reply)
        })?;
        let r = &reply.0;
        if let Some(address) = &r.description.address_expression {
            return Err(Diagnostic::new(
                Code::UnsupportedFeature,
                "dynamic reply addresses require per-request routing support",
            )
            .at(address.location.clone())
            .needs(Requirement::Reply));
        }
        let channel = r.channel.as_ref().ok_or_else(|| {
            Diagnostic::new(Code::MissingConfiguration, "declared reply needs a channel")
                .needs(Requirement::ReplyChannel)
        })?;
        let request = self.prepare_message(
            &options.request,
            PreparationContext::operation(&self.0),
            true,
            true,
        )?;
        if request.describe().wire_action != Action::Send
            || !matches!(
                request.describe().transport,
                TransportPlan::WebSocket6455 { .. }
            )
        {
            return Err(Diagnostic::new(
                Code::UnsupportedFeature,
                "initial exchanges require an outgoing WebSocket request",
            ));
        }
        let reply_plan = self.prepare_message(
            &options.reply,
            PreparationContext {
                compilation: &self.0,
                channel,
                messages: &r.messages,
                servers: &r.servers,
                channel_message_count: r.channel_message_count,
                address: r.description.address.as_deref(),
                action: r.description.application_action,
            },
            true,
            true,
        )?;
        match (
            &request.describe().transport,
            &reply_plan.describe().transport,
        ) {
            (
                TransportPlan::WebSocket6455 { endpoint: a, .. },
                TransportPlan::WebSocket6455 { endpoint: b, .. },
            ) if a == b => {}
            _ => {
                return Err(Diagnostic::new(
                    Code::UnsupportedFeature,
                    "initial exchange request and reply must share a WebSocket endpoint",
                ));
            }
        }
        let request_correlation = correlation_expression(
            self.correlation(&request.describe().message)?,
            "request",
            request.describe().codec,
        )?;
        let reply_correlation = correlation_expression(
            reply.correlation(&reply_plan.describe().message)?,
            "reply",
            reply_plan.describe().codec,
        )?;
        let description = Arc::new(ExchangeDescription {
            request: request.describe().clone(),
            reply: reply_plan.describe().clone(),
            declaration: r.description.clone(),
            request_correlation,
            reply_correlation,
            timeout_ms: options.timeout_ms,
        });
        Ok(ExchangePlan {
            request,
            reply: reply_plan,
            description,
        })
    }
}
