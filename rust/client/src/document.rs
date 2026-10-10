use crate::source::{Source, escape, valid_pointer};
use crate::{Code, Diagnostic, Json, Limits, Location, Requirement};
use serde_json::Value;
use std::{
    collections::{BTreeMap, HashSet},
    sync::Arc,
};

/// Reader families implemented by this foundation. Earlier 2.x remains open work.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize)]
pub enum Edition {
    V2_6,
    V3_0,
    V3_1,
}

/// The action of the application described by the document.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub enum Action {
    Send,
    Receive,
}

#[derive(Debug)]
struct Snapshot {
    root: Arc<Source>,
    resources: BTreeMap<String, Arc<Source>>,
    edition: Edition,
    version: String,
    limits: Limits,
}

/// Immutable, owning document snapshot. No method performs source acquisition.
#[derive(Clone, Debug)]
pub struct Document(Arc<Snapshot>);

impl Document {
    pub fn parse(text: &str) -> Result<Self, Diagnostic> {
        Self::parse_with(text, None, Limits::default())
    }
    pub fn parse_at(text: &str, uri: &str) -> Result<Self, Diagnostic> {
        Self::parse_with(text, Some(uri), Limits::default())
    }
    pub fn parse_with(text: &str, uri: Option<&str>, limits: Limits) -> Result<Self, Diagnostic> {
        if limits.resources == 0 || text.len() > limits.total_source_bytes {
            return Err(Diagnostic::new(
                Code::Limit,
                "document resource limit exceeded",
            ));
        }
        let uri = uri.map(resource_uri).transpose()?;
        let root = Source::parse(text, uri, limits)?;
        let json = root.root();
        if !json.value().is_object() {
            return Err(
                Diagnostic::new(Code::InvalidDocument, "document root must be an object")
                    .at(json.location()),
            );
        }
        let version_node = json.get("asyncapi").ok_or_else(|| {
            Diagnostic::new(Code::InvalidDocument, "asyncapi version is required")
                .at(json.location())
        })?;
        let version = version_node
            .as_str()
            .ok_or_else(|| {
                Diagnostic::new(Code::InvalidDocument, "asyncapi version must be a string")
                    .at(version_node.location())
            })?
            .to_owned();
        let edition = edition(&version).map_err(|e| e.at(version_node.location()))?;
        let info = json.get("info").ok_or_else(|| {
            Diagnostic::new(Code::InvalidDocument, "info is required").at(json.location())
        })?;
        for key in ["title", "version"] {
            if info.get(key).is_none_or(|x| x.as_str().is_none()) {
                return Err(Diagnostic::new(
                    Code::InvalidDocument,
                    "info requires string title and version",
                )
                .at(info.location()));
            }
        }
        if edition == Edition::V2_6 && json.get("channels").is_none() {
            return Err(
                Diagnostic::new(Code::InvalidDocument, "2.6 requires channels").at(json.location()),
            );
        }
        for key in ["channels", "operations", "servers", "components"] {
            if let Some(node) = json.get(key)
                && !node.value().is_object()
            {
                return Err(Diagnostic::new(
                    Code::InvalidDocument,
                    "document map must be an object",
                )
                .at(node.location()));
            }
        }
        let mut resources = BTreeMap::new();
        if let Some(uri) = &root.uri {
            resources.insert(uri.clone(), root.clone());
        }
        Ok(Self(Arc::new(Snapshot {
            root,
            resources,
            edition,
            version,
            limits,
        })))
    }
    pub fn edition(&self) -> Edition {
        self.0.edition
    }
    pub fn version(&self) -> &str {
        &self.0.version
    }
    pub fn source(&self) -> &str {
        &self.0.root.text
    }
    pub fn root(&self) -> Json {
        self.0.root.root()
    }

    /// Complete dependencies into a new snapshot; existing handles keep their graph.
    pub fn with_resource(&self, uri: &str, text: &str) -> Result<Self, Diagnostic> {
        let uri = resource_uri(uri)?;
        if let Some(existing) = self.0.resources.get(&uri) {
            if existing.text.as_ref() == text {
                return Ok(self.clone());
            }
            return Err(Diagnostic::new(
                Code::InvalidReference,
                "resource URI already identifies different source",
            ));
        }
        let anonymous_root = usize::from(self.0.root.uri.is_none());
        let current_bytes = self
            .0
            .resources
            .values()
            .map(|s| s.text.len())
            .sum::<usize>()
            + if anonymous_root == 1 {
                self.0.root.text.len()
            } else {
                0
            };
        if self.0.resources.len() + anonymous_root >= self.0.limits.resources
            || current_bytes.saturating_add(text.len()) > self.0.limits.total_source_bytes
        {
            return Err(Diagnostic::new(
                Code::Limit,
                "document resource limit exceeded",
            ));
        }
        let source = Source::parse(text, Some(uri.clone()), self.0.limits)?;
        let mut resources = self.0.resources.clone();
        resources.insert(uri, source);
        Ok(Self(Arc::new(Snapshot {
            root: self.0.root.clone(),
            resources,
            edition: self.0.edition,
            version: self.0.version.clone(),
            limits: self.0.limits,
        })))
    }

    /// Each inventory item can fail independently. Selection does not fetch resources.
    pub fn operations(&self) -> std::vec::IntoIter<Result<Operation, Diagnostic>> {
        let mut entries = Vec::new();
        if self.edition() != Edition::V2_6 {
            if let Some(operations) = self.root().get("operations") {
                for (_, node) in operations.members().unwrap() {
                    entries.push(Ok(Operation {
                        document: self.clone(),
                        selection: OperationIdentity {
                            uri: self.0.root.uri.clone(),
                            pointer: node.location().pointer,
                        },
                        authored: node,
                        channel: None,
                        legacy_action: None,
                    }));
                }
            }
        } else if let Some(channels) = self.root().get("channels") {
            for (key, channel) in channels.members().unwrap() {
                match self.resolve(channel) {
                    Err(error) => entries.push(Err(error)),
                    Ok(channel) => {
                        if !channel.value().is_object() {
                            entries.push(Err(Diagnostic::new(
                                Code::InvalidDocument,
                                "channel must be an object",
                            )
                            .at(channel.location())));
                            continue;
                        }
                        for (field, action) in
                            [("publish", Action::Receive), ("subscribe", Action::Send)]
                        {
                            if let Some(node) = channel.get(field) {
                                let selection = OperationIdentity {
                                    uri: self.0.root.uri.clone(),
                                    pointer: format!("/channels/{}/{field}", escape(&key)),
                                };
                                entries.push(Ok(Operation {
                                    document: self.clone(),
                                    selection,
                                    authored: node,
                                    channel: Some((key.clone(), channel.clone())),
                                    legacy_action: Some(action),
                                }));
                            }
                        }
                    }
                }
            }
        }
        entries.into_iter()
    }

    pub fn operation_at(&self, pointer: &str) -> Result<Operation, Diagnostic> {
        if !valid_pointer(pointer) {
            return Err(Diagnostic::new(
                Code::InvalidReference,
                "operation selection requires a JSON Pointer",
            ));
        }
        let selection = OperationIdentity {
            uri: self.0.root.uri.clone(),
            pointer: pointer.into(),
        };
        let parts = pointer.split('/').collect::<Vec<_>>();
        if self.edition() != Edition::V2_6 {
            if parts.len() == 3
                && parts[1] == "operations"
                && let Some(authored) = self.root().pointer(pointer)
            {
                return Ok(Operation {
                    document: self.clone(),
                    selection,
                    authored,
                    channel: None,
                    legacy_action: None,
                });
            }
        } else if parts.len() == 4 && parts[1] == "channels" {
            let action = match parts[3] {
                "publish" => Some(Action::Receive),
                "subscribe" => Some(Action::Send),
                _ => None,
            };
            if let Some(action) = action
                && let Some(channel) = self.root().pointer(&format!("/channels/{}", parts[2]))
            {
                let channel = self.resolve(channel)?;
                if !channel.value().is_object() {
                    return Err(Diagnostic::new(
                        Code::InvalidDocument,
                        "channel must be an object",
                    )
                    .at(channel.location()));
                }
                if let Some(authored) = channel.get(parts[3]) {
                    let key = parts[2].replace("~1", "/").replace("~0", "~");
                    return Ok(Operation {
                        document: self.clone(),
                        selection,
                        authored,
                        channel: Some((key, channel)),
                        legacy_action: Some(action),
                    });
                }
            }
        }
        Err(Diagnostic::new(
            Code::MissingOperation,
            "no operation at this location",
        ))
    }

    pub fn operation_id(&self, id: &str) -> Result<Operation, Diagnostic> {
        if self.edition() != Edition::V2_6 {
            return self.operation_at(&format!("/operations/{}", escape(id)));
        }
        let mut found = None;
        let mut unresolved = None;
        for entry in self.operations() {
            let result =
                entry.and_then(|operation| operation.authored_id().map(|key| (operation, key)));
            match result {
                Ok((operation, Some(key))) if key == id => {
                    if found.is_some() {
                        return Err(Diagnostic::new(
                            Code::AmbiguousOperation,
                            "authored operation ID is ambiguous",
                        ));
                    }
                    found = Some(operation);
                }
                Err(error) => {
                    unresolved.get_or_insert(error);
                }
                _ => {}
            }
        }
        // A missing dependency may hide an additional occurrence of a 2.x ID.
        if let Some(error) = unresolved {
            return Err(error);
        }
        found.ok_or_else(|| {
            Diagnostic::new(Code::MissingOperation, "no operation with this authored ID")
        })
    }

    pub(crate) fn resolve(&self, mut node: Json) -> Result<Json, Diagnostic> {
        let mut seen = HashSet::new();
        let mut steps = 0;
        while let Some(reference_node) = node.get("$ref") {
            if !seen.insert((Arc::as_ptr(&node.source) as usize, node.pointer.clone())) {
                return Err(
                    Diagnostic::new(Code::ReferenceCycle, "cyclic protocol reference")
                        .at(reference_node.location()),
                );
            }
            if steps >= self.0.limits.reference_steps {
                return Err(
                    Diagnostic::new(Code::Limit, "reference traversal limit exceeded")
                        .at(reference_node.location()),
                );
            }
            steps += 1;
            node = self.reference_target(&node)?;
        }
        Ok(node)
    }

    fn reference_target(&self, node: &Json) -> Result<Json, Diagnostic> {
        if self.0.limits.reference_steps == 0 {
            return Err(
                Diagnostic::new(Code::Limit, "reference traversal limit exceeded")
                    .at(node.location()),
            );
        }
        let reference_node = node.get("$ref").ok_or_else(|| {
            Diagnostic::new(Code::InvalidReference, "expected a reference").at(node.location())
        })?;
        let reference = reference_node.as_str().ok_or_else(|| {
            Diagnostic::new(Code::InvalidReference, "$ref must be a string")
                .at(reference_node.location())
        })?;
        let parsed = fluent_uri::UriRef::parse(reference).map_err(|_| {
            Diagnostic::new(Code::InvalidReference, "invalid reference URI")
                .at(reference_node.location())
        })?;
        let absolute = if reference.starts_with('#') || reference.is_empty() {
            format!("{}{reference}", node.source.uri.as_deref().unwrap_or(""))
        } else if parsed.scheme().is_some() {
            parsed.normalize().to_string()
        } else {
            let base = node.source.uri.as_ref().ok_or_else(|| {
                Diagnostic::new(
                    Code::MissingResource,
                    "relative reference needs a source URI",
                )
                .at(reference_node.location())
                .needs(Requirement::SourceUri)
            })?;
            let base = fluent_uri::Uri::parse(base.as_str()).unwrap();
            parsed
                .resolve_against(&base)
                .map_err(|_| {
                    Diagnostic::new(
                        Code::InvalidReference,
                        "reference cannot resolve against source URI",
                    )
                    .at(reference_node.location())
                })?
                .normalize()
                .to_string()
        };
        let (uri, fragment) = absolute.split_once('#').unwrap_or((&absolute, ""));
        let resource = if uri.is_empty() {
            node.source.clone()
        } else {
            self.0.resources.get(uri).cloned().ok_or_else(|| {
                Diagnostic::new(
                    Code::MissingResource,
                    "referenced source has not been supplied",
                )
                .at(reference_node.location())
                .needs(Requirement::Resource { uri: uri.into() })
            })?
        };
        let pointer = decode_fragment(fragment).map_err(|e| e.at(reference_node.location()))?;
        resource.root().pointer(&pointer).ok_or_else(|| {
            Diagnostic::new(
                Code::MissingReferenceTarget,
                "referenced JSON Pointer is absent",
            )
            .at(reference_node.location())
        })
    }
}

/// Native selector in the assembled document. This is not an authored byte span:
/// a 2.x channel may be defined in another resource.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
pub struct OperationIdentity {
    pub uri: Option<String>,
    pub pointer: String,
}

/// An owning operation selected through its native document coordinate.
#[derive(Clone, Debug)]
pub struct Operation {
    document: Document,
    selection: OperationIdentity,
    authored: Json,
    channel: Option<(String, Json)>,
    legacy_action: Option<Action>,
}

/// Effective operation facts; these do not imply an executable peer route.
#[derive(Clone, Debug, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct OperationDescription {
    pub action: Action,
    pub operation_id: Option<String>,
    pub summary: Option<String>,
    pub description: Option<String>,
    pub channel: Location,
    pub address: Option<String>,
}

impl Operation {
    pub fn identity(&self) -> &OperationIdentity {
        &self.selection
    }
    pub fn location(&self) -> Location {
        self.authored.location()
    }
    pub fn authored(&self) -> Json {
        self.authored.clone()
    }
    pub fn document(&self) -> Document {
        self.document.clone()
    }
    pub fn authored_id(&self) -> Result<Option<String>, Diagnostic> {
        if self.legacy_action.is_none() {
            return Ok(Some(
                self.selection
                    .pointer
                    .strip_prefix("/operations/")
                    .unwrap()
                    .replace("~1", "/")
                    .replace("~0", "~"),
            ));
        }
        let (object, effective) = self.effective()?;
        optional_string(&effective, "operationId", &object.location())
    }

    fn effective(&self) -> Result<(Json, Value), Diagnostic> {
        let object = self.document.resolve(self.authored.clone())?;
        if !object.value().is_object() {
            return Err(
                Diagnostic::new(Code::InvalidOperation, "operation must be an object")
                    .at(object.location()),
            );
        }
        let mut effective = Value::Object(Default::default());
        let mut remaining = self.document.0.limits.merge_nodes;
        if let Some(traits) = object.get("traits") {
            let items = traits.elements().ok_or_else(|| {
                Diagnostic::new(Code::InvalidOperation, "traits must be an array")
                    .at(traits.location())
            })?;
            if items.len() > self.document.0.limits.trait_count {
                return Err(
                    Diagnostic::new(Code::Limit, "operation trait limit exceeded")
                        .at(traits.location()),
                );
            }
            for item in items {
                let item = self.document.resolve(item)?;
                let forbidden: &[&str] = if self.legacy_action.is_some() {
                    &["message", "traits"]
                } else {
                    &["action", "channel", "messages", "traits"]
                };
                if !item.value().is_object() || forbidden.iter().any(|key| item.get(key).is_some())
                {
                    return Err(Diagnostic::new(
                        Code::InvalidOperation,
                        "invalid operation trait field or shape",
                    )
                    .at(item.location()));
                }
                merge_patch(&mut effective, item.value(), &mut remaining)
                    .map_err(|e| e.at(item.location()))?;
            }
        }
        merge_patch(&mut effective, object.value(), &mut remaining)
            .map_err(|e| e.at(object.location()))?;
        Ok((object, effective))
    }

    pub fn describe(&self) -> Result<OperationDescription, Diagnostic> {
        let (object, effective) = self.effective()?;
        let action = match self.legacy_action {
            Some(action) => action,
            None => match object.get("action").as_ref().and_then(Json::as_str) {
                Some("send") => Action::Send,
                Some("receive") => Action::Receive,
                _ => {
                    return Err(Diagnostic::new(
                        Code::InvalidOperation,
                        "action must be send or receive",
                    )
                    .at(object.location()));
                }
            },
        };
        let (channel, address) = if let Some((address, channel)) = &self.channel {
            (channel.clone(), Some(address.clone()))
        } else {
            let channel_ref = object.get("channel").ok_or_else(|| {
                Diagnostic::new(Code::InvalidOperation, "operation channel is required")
                    .at(object.location())
            })?;
            if channel_ref.get("$ref").is_none() {
                return Err(Diagnostic::new(
                    Code::InvalidOperation,
                    "operation channel must be a reference",
                )
                .at(channel_ref.location()));
            }
            let channel_target = self.document.reference_target(&channel_ref)?;
            if !Arc::ptr_eq(&channel_target.source, &self.document.0.root)
                || !channel_target
                    .pointer
                    .strip_prefix("/channels/")
                    .is_some_and(|key| !key.contains('/'))
            {
                return Err(Diagnostic::new(
                    Code::InvalidOperation,
                    "root operation must reference a root channel",
                )
                .at(channel_ref.location()));
            }
            let channel = self.document.resolve(channel_ref)?;
            if !channel.value().is_object() {
                return Err(
                    Diagnostic::new(Code::InvalidOperation, "channel must be an object")
                        .at(channel.location()),
                );
            }
            let address = match channel.get("address") {
                Some(value) if value.is_null() => None,
                _ => optional_string(channel.value(), "address", &channel.location())?,
            };
            (channel, address)
        };
        let operation_id = if self.legacy_action.is_some() {
            optional_string(&effective, "operationId", &object.location())?
        } else {
            self.authored_id()?
        };
        Ok(OperationDescription {
            action,
            operation_id,
            summary: optional_string(&effective, "summary", &object.location())?,
            description: optional_string(&effective, "description", &object.location())?,
            channel: channel.location(),
            address,
        })
    }
}

fn optional_string(
    value: &Value,
    key: &str,
    location: &Location,
) -> Result<Option<String>, Diagnostic> {
    match value.get(key) {
        None => Ok(None),
        Some(Value::String(s)) => Ok(Some(s.clone())),
        _ => Err(
            Diagnostic::new(Code::InvalidOperation, "declared field must be a string")
                .at(location.clone()),
        ),
    }
}
fn spend(remaining: &mut usize) -> Result<(), Diagnostic> {
    *remaining = remaining.checked_sub(1).ok_or_else(|| {
        Diagnostic::new(Code::Limit, "effective declaration merge limit exceeded")
    })?;
    Ok(())
}
fn count_clone(value: &Value, remaining: &mut usize) -> Result<(), Diagnostic> {
    spend(remaining)?;
    match value {
        Value::Array(items) => {
            for item in items {
                count_clone(item, remaining)?;
            }
        }
        Value::Object(fields) => {
            for item in fields.values() {
                count_clone(item, remaining)?;
            }
        }
        _ => {}
    }
    Ok(())
}
fn merge_patch(target: &mut Value, patch: &Value, remaining: &mut usize) -> Result<(), Diagnostic> {
    if let Value::Object(fields) = patch {
        spend(remaining)?;
        if !target.is_object() {
            *target = Value::Object(Default::default());
        }
        let target = target.as_object_mut().unwrap();
        for (key, value) in fields {
            if value.is_null() {
                spend(remaining)?;
                target.remove(key);
            } else {
                merge_patch(
                    target.entry(key.clone()).or_insert(Value::Null),
                    value,
                    remaining,
                )?;
            }
        }
    } else {
        count_clone(patch, remaining)?;
        *target = patch.clone();
    }
    Ok(())
}
fn resource_uri(uri: &str) -> Result<String, Diagnostic> {
    let parsed = fluent_uri::Uri::parse(uri)
        .map_err(|_| Diagnostic::new(Code::InvalidReference, "source URI must be absolute"))?;
    if parsed.has_fragment() || parsed.authority().is_some_and(|a| a.userinfo().is_some()) {
        return Err(Diagnostic::new(
            Code::InvalidReference,
            "source URI cannot contain a fragment or user information",
        ));
    }
    Ok(parsed.normalize().to_string())
}
fn decode_fragment(fragment: &str) -> Result<String, Diagnostic> {
    let mut bytes = Vec::with_capacity(fragment.len());
    let raw = fragment.as_bytes();
    let mut i = 0;
    while i < raw.len() {
        if raw[i] == b'%' {
            let hex = fragment.get(i + 1..i + 3).ok_or_else(|| {
                Diagnostic::new(Code::InvalidReference, "invalid escaped fragment")
            })?;
            bytes.push(u8::from_str_radix(hex, 16).map_err(|_| {
                Diagnostic::new(Code::InvalidReference, "invalid escaped fragment")
            })?);
            i += 3;
        } else {
            bytes.push(raw[i]);
            i += 1;
        }
    }
    let pointer = String::from_utf8(bytes)
        .map_err(|_| Diagnostic::new(Code::InvalidReference, "fragment must be UTF-8"))?;
    if !valid_pointer(&pointer) {
        return Err(Diagnostic::new(
            Code::InvalidReference,
            "fragment must be a JSON Pointer",
        ));
    }
    Ok(pointer)
}
fn edition(version: &str) -> Result<Edition, Diagnostic> {
    let (numeric, suffix) = version
        .split_once('-')
        .map_or((version, None), |(a, b)| (a, Some(b)));
    if suffix.is_some_and(|s| s.is_empty() || !s.chars().all(|c| c.is_ascii_alphanumeric())) {
        return Err(Diagnostic::new(
            Code::UnsupportedVersion,
            "invalid AsyncAPI version suffix",
        ));
    }
    let parts: Vec<_> = numeric.split('.').collect();
    if parts.len() != 3
        || parts
            .iter()
            .any(|p| p.is_empty() || !p.bytes().all(|b| b.is_ascii_digit()))
    {
        return Err(Diagnostic::new(
            Code::UnsupportedVersion,
            "expected an AsyncAPI major.minor.patch version",
        ));
    }
    match (parts[0], parts[1]) {
        ("2", "6") => Ok(Edition::V2_6),
        ("3", "0") => Ok(Edition::V3_0),
        ("3", "1") => Ok(Edition::V3_1),
        _ => Err(Diagnostic::new(
            Code::UnsupportedVersion,
            "this reader family is not implemented",
        )),
    }
}
