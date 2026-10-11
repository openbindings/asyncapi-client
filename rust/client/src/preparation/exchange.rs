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
    pub fn describe(&self) -> &ReplyDescription { &self.0.description }
    pub fn operation(&self) -> &CompiledOperation { &self.0.operation }
    pub fn correlation(&self, message: &str) -> Result<Option<CorrelationDescription>, Diagnostic> {
        let selected = self.0.messages.iter().find(|m| m.description.key == message)
            .ok_or_else(|| Diagnostic::new(Code::InvalidConfiguration, "selected reply message is not available"))?;
        selected.effective.get("correlationId").map(|v| crate::expression::correlation(&self.0.operation.0.document, v)).transpose()
    }
}
fn reverse(action: Action) -> Action {
    match action { Action::Send => Action::Receive, Action::Receive => Action::Send }
}
fn component(node: &Json, collection: &str) -> bool {
    node.pointer.strip_prefix(&format!("/components/{collection}/")).is_some_and(|key| !key.contains('/'))
}
fn reference_object(value: &Effective) -> Result<Json, Diagnostic> {
    let reference = value.get("$ref").ok_or_else(|| invalid(&value.source, "reply channel must be a reference"))?;
    let (parent, _) = reference.source.pointer.rsplit_once('/').unwrap();
    Ok(reference.source.source.root().pointer(parent).unwrap())
}
impl CompiledOperation {
    /// Resolve replies lazily; omission stays unknown. Native 2.x request/reply
    /// requires an explicitly configured operation pair.
    pub fn reply(&self) -> Result<Option<CompiledReply>, Diagnostic> {
        let Some(selection) = self.0.operation.get("reply") else { return Ok(None); };
        if self.0.document.edition() == Edition::V2_6 {
            return Err(unsupported(&selection.source, "operation reply is not a native 2.x field", Some(Requirement::Reply)));
        }
        let definition = selection.resolve(&self.0.document)?;
        if !definition.is_object() { return Err(invalid(&definition.source, "reply must be an object")); }
        let address_expression = definition.get("address").map(|selection| {
            let address = selection.resolve(&self.0.document)?;
            if !address.is_object() { return Err(invalid(&address.source, "reply address must be an object")); }
            let location = address.get("location").ok_or_else(|| invalid(&address.source, "reply address location is required"))?;
            let text = location.string().ok_or_else(|| invalid(&location.source, "reply address location must be a string"))?;
            let expression = RuntimeExpression::parse(text).map_err(|mut e| {
                if e.code == Code::InvalidValue { e.code = Code::InvalidOperation; }
                e.at(location.source.location())
            })?;
            Ok(ReplyAddressDescription { selection: selection.source.location(), definition: address.source.location(), location: location.source.location(), expression })
        }).transpose()?;
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
            let relaxed = component(&definition.source, "replies") || component(&self.0.operation.source, "operations");
            if !relaxed && (!Arc::ptr_eq(&target.source, &root.source) || !target.pointer.strip_prefix("/channels/").is_some_and(|key| !key.contains('/'))) {
                return Err(invalid(&reference, "root operation reply must reference a root channel entry"));
            }
            let resolved = self.0.document.resolve(reference.clone())?;
            if resolved.kind() != "object" { return Err(invalid(&resolved, "reply channel must be an object")); }
            address = match resolved.get("address") {
                Some(value) if value.is_null() => None,
                _ => Effective::from(resolved.clone()).optional_string("address")?,
            };
            if address_expression.is_some() && address.is_some() {
                return Err(invalid(&resolved, "a dynamic reply address requires an unknown channel address"));
            }
            let (entries, message_count) = three_x_entries(&self.0.document, definition.get("messages").map(|v| v.source), &resolved)?;
            reply_messages = resolve_messages(&self.0.document, entries)?;
            reply_servers = servers(&self.0.document, &resolved)?;
            count = message_count;
            channel_selection = Some(reference.location());
            channel = Some(resolved);
        } else if let Some(messages) = definition.get("messages") {
            array(&messages.source)?;
            return Err(Diagnostic::new(Code::MissingConfiguration, "reply message selection needs a reply channel").at(messages.source.location()).needs(Requirement::ReplyChannel));
        }
        let description = ReplyDescription {
            selection: selection.source.location(), definition: definition.source.location(), application_action: reverse(self.0.description.operation.action),
            channel_selection, channel: channel.as_ref().map(Json::location), address, address_expression,
            messages: channel.as_ref().map(|_| reply_messages.iter().map(|m| m.description.clone()).collect()), servers: reply_servers.iter().map(|s| s.description.clone()).collect(),
        };
        Ok(Some(CompiledReply(Arc::new(ReplyCompilation { operation: self.clone(), description, channel, messages: reply_messages, servers: reply_servers, channel_message_count: count }))))
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "camelCase", deny_unknown_fields)]
pub enum ReplyCompletion {
    First,
    Count { replies: u32 },
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ExchangeOptions {
    pub request: PlanOptions,
    pub reply: PlanOptions,
    #[serde(default)]
    pub request_correlation: Option<String>,
    #[serde(default)]
    pub reply_correlation: Option<String>,
    #[serde(default)]
    pub completion: Option<ReplyCompletion>,
    pub timeout_ms: u32,
}
impl ExchangeOptions {
    pub fn first(request: PlanOptions, reply: PlanOptions, timeout_ms: u32) -> Self {
        Self { request, reply, request_correlation: None, reply_correlation: None, completion: Some(ReplyCompletion::First), timeout_ms }
    }
    fn validate(&self) -> Result<ReplyCompletion, Diagnostic> {
        if self.request.role != self.reply.role { return Err(Diagnostic::new(Code::InvalidConfiguration, "exchange sides must use the same role")); }
        if !(1..=300_000).contains(&self.timeout_ms) { return Err(Diagnostic::new(Code::InvalidConfiguration, "exchange timeout must be between 1 and 300000 milliseconds")); }
        let completion = self.completion.ok_or_else(|| Diagnostic::new(Code::MissingConfiguration, "exchange needs an explicit reply completion policy").needs(Requirement::ReplyCompletion))?;
        if matches!(completion, ReplyCompletion::Count { replies } if !(1..=64).contains(&replies)) { return Err(Diagnostic::new(Code::InvalidConfiguration, "reply count must be between one and sixty-four")); }
        Ok(completion)
    }
}
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ExchangeDescription {
    pub request: PlanDescription,
    pub reply: PlanDescription,
    pub declaration: Option<ReplyDescription>,
    pub request_correlation: RuntimeExpression,
    pub reply_correlation: RuntimeExpression,
    pub completion: ReplyCompletion,
    pub timeout_ms: u32,
}
/// Owning pure plan, distinct from ordinary session inputs. Runtime exchange
/// readiness and matching support must be implemented before execution.
#[derive(Clone, Debug)]
pub struct ExchangePlan {
    request: Plan,
    reply: Plan,
    description: Arc<ExchangeDescription>,
}
impl ExchangePlan {
    pub fn describe(&self) -> &ExchangeDescription { &self.description }
    pub fn request_operation(&self) -> &CompiledOperation { self.request.operation() }
    /// A declared reply retains the original operation, never a fabricated one.
    pub fn reply_operation(&self) -> &CompiledOperation { self.reply.operation() }
}
fn correlation_expression(declared: Option<CorrelationDescription>, configured: Option<&str>, side: &str, codec: Codec) -> Result<RuntimeExpression, Diagnostic> {
    let configured = configured.map(RuntimeExpression::parse).transpose()?;
    if let (Some(declared), Some(configured)) = (&declared, &configured) {
        if declared.expression.source() != configured.source() || declared.expression.pointer() != configured.pointer() {
            return Err(Diagnostic::new(Code::InvalidConfiguration, "configured correlation contradicts the message declaration").at(declared.location.clone()));
        }
    }
    let expression = declared.map(|v| v.expression).or(configured).ok_or_else(|| Diagnostic::new(Code::MissingConfiguration, "exchange needs a correlation location for each side").needs(Requirement::Correlation { side: side.into() }))?;
    if codec != Codec::Json || expression.source() != ExpressionSource::Payload {
        return Err(Diagnostic::new(Code::UnsupportedFeature, "initial exchange preparation requires JSON payload correlation").needs(Requirement::Correlation { side: side.into() }));
    }
    Ok(expression)
}
fn finish(request: Plan, reply: Plan, declaration: Option<ReplyDescription>, request_correlation: Option<CorrelationDescription>, reply_correlation: Option<CorrelationDescription>, options: &ExchangeOptions) -> Result<ExchangePlan, Diagnostic> {
    let completion = options.validate()?;
    if request.describe().wire_action == reply.describe().wire_action {
        return Err(Diagnostic::new(Code::InvalidConfiguration, "exchange request and reply must have opposite wire directions"));
    }
    let request_expression = correlation_expression(request_correlation, options.request_correlation.as_deref(), "request", request.describe().codec)?;
    let reply_expression = correlation_expression(reply_correlation, options.reply_correlation.as_deref(), "reply", reply.describe().codec)?;
    let description = Arc::new(ExchangeDescription { request: request.describe().clone(), reply: reply.describe().clone(), declaration, request_correlation: request_expression, reply_correlation: reply_expression, completion, timeout_ms: options.timeout_ms });
    Ok(ExchangePlan { request, reply, description })
}
impl CompiledOperation {
    /// Prepare an explicit pair, including native 2.x operations. A declared
    /// reply cannot be bypassed by choosing a different operation.
    pub fn prepare_configured_exchange(&self, reply: &CompiledOperation, options: &ExchangeOptions) -> Result<ExchangePlan, Diagnostic> {
        options.validate()?;
        let request_plan = self.prepare_message(&options.request, PreparationContext::operation(&self.0), false, true)?;
        let reply_plan = reply.prepare_message(&options.reply, PreparationContext::operation(&reply.0), false, true)?;
        let request_correlation = self.correlation(&request_plan.describe().message)?;
        let reply_correlation = reply.correlation(&reply_plan.describe().message)?;
        finish(request_plan, reply_plan, None, request_correlation, reply_correlation, options)
    }
    /// Prepare a native 3.x reply with a static/configured channel address.
    /// Dynamic reply-address expressions cannot be silently replaced.
    pub fn prepare_exchange(&self, options: &ExchangeOptions) -> Result<ExchangePlan, Diagnostic> {
        options.validate()?;
        let reply = self.reply()?.ok_or_else(|| Diagnostic::new(Code::MissingConfiguration, "operation has no declared reply; configure an explicit pair").needs(Requirement::Reply))?;
        let r = &reply.0;
        if let Some(address) = &r.description.address_expression {
            return Err(Diagnostic::new(Code::UnsupportedFeature, "dynamic reply addresses need per-request routing support").at(address.location.clone()).needs(Requirement::Reply));
        }
        let channel = r.channel.as_ref().ok_or_else(|| Diagnostic::new(Code::MissingConfiguration, "declared reply needs a channel").needs(Requirement::ReplyChannel))?;
        let request = self.prepare_message(&options.request, PreparationContext::operation(&self.0), true, true)?;
        if matches!(request.describe().transport, TransportPlan::Mqtt311 { .. }) && self.0.operation.get("bindings").is_some() {
            return Err(unsupported(&self.0.operation.source, "declared MQTT reply delivery bindings need an explicit reply profile", Some(Requirement::Reply)));
        }
        let reply_plan = self.prepare_message(&options.reply, PreparationContext { compilation: &self.0, channel, messages: &r.messages, servers: &r.servers, channel_message_count: r.channel_message_count, address: r.description.address.as_deref(), action: r.description.application_action }, true, true)?;
        let request_correlation = self.correlation(&request.describe().message)?;
        let reply_correlation = reply.correlation(&reply_plan.describe().message)?;
        finish(request, reply_plan, Some(r.description.clone()), request_correlation, reply_correlation, options)
    }
}
