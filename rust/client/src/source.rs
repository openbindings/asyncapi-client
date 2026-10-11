use crate::{Code, Diagnostic, Limits};
use serde_json::{Value, value::RawValue};
use std::{
    collections::{BTreeMap, HashSet},
    ops::Range,
    sync::Arc,
};

/// Original UTF-8 byte coordinates plus a logical JSON Pointer. Alias expansion
/// identifies the defining value and its use sites, never normalized byte ranges.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
pub struct Location {
    pub uri: Option<String>,
    pub pointer: String,
    pub bytes: Range<usize>,
    /// Alias-use ranges leading to this defining value, outermost first.
    pub aliases: Vec<Range<usize>>,
}

#[derive(Debug)]
pub(crate) struct Source {
    pub uri: Option<String>,
    pub text: Arc<str>,
    pub value: Value,
    spans: BTreeMap<String, Range<usize>>,
    aliases: BTreeMap<String, Vec<Range<usize>>>,
    numbers: BTreeMap<String, String>,
    yaml: bool,
}

impl Source {
    pub fn parse(text: &str, uri: Option<String>, limits: Limits) -> Result<Arc<Self>, Diagnostic> {
        if text.len() > limits.source_bytes {
            return Err(Diagnostic::new(Code::Limit, "source byte limit exceeded"));
        }
        // Validate syntax without first materializing an unbounded Value tree.
        if serde_json::from_str::<&RawValue>(text).is_err() {
            let parsed = crate::yaml::parse(text, uri.as_deref(), limits)?;
            return Ok(Arc::new(Self {
                uri,
                text: Arc::from(text),
                value: parsed.value,
                spans: parsed.spans,
                aliases: parsed.aliases,
                numbers: parsed.numbers,
                yaml: true,
            }));
        }
        Self::from_validated_json(text, uri, limits)
    }
    fn parse_json(text: &str, limits: Limits) -> Result<Arc<Self>, Diagnostic> {
        if text.len() > limits.source_bytes {
            return Err(Diagnostic::new(Code::Limit, "JSON byte limit exceeded"));
        }
        serde_json::from_str::<&RawValue>(text)
            .map_err(|_| Diagnostic::new(Code::InvalidJson, "expected one strict JSON value"))?;
        Self::from_validated_json(text, None, limits)
    }
    fn from_validated_json(
        text: &str,
        uri: Option<String>,
        limits: Limits,
    ) -> Result<Arc<Self>, Diagnostic> {
        let mut scanner = Scanner {
            text,
            uri: uri.clone(),
            at: 0,
            nodes: 0,
            limits,
            spans: BTreeMap::new(),
        };
        let value = scanner.value(String::new(), 0)?;
        Ok(Arc::new(Self {
            uri,
            text: Arc::from(text),
            value,
            spans: scanner.spans,
            aliases: BTreeMap::new(),
            numbers: BTreeMap::new(),
            yaml: false,
        }))
    }
    pub fn root(self: &Arc<Self>) -> Json {
        Json {
            source: self.clone(),
            pointer: String::new(),
        }
    }
}

/// An owning view into exact JSON. Child handles remain valid independently.
#[derive(Clone, Debug)]
pub struct Json {
    pub(crate) source: Arc<Source>,
    pub(crate) pointer: String,
}
impl Json {
    /// Admit an independent, owning JSON value. No YAML fallback, I/O, schema
    /// evaluation or floating-point conversion. Child views own their source.
    pub fn parse(text: &str, limits: Limits) -> Result<Self, Diagnostic> {
        Source::parse_json(text, limits).map(|source| source.root())
    }
    /// Project into an application type using standard Serde JSON conversion.
    /// Integer overflow refuses; choosing float types explicitly permits rounding.
    /// YAML views use their exact JSON representation. This leaves the owner
    /// intact and is not schema validation. Custom Deserialize work is unbounded
    /// caller code. Potentially sensitive error text requires explicit detail().
    pub fn deserialize<T: serde::de::DeserializeOwned>(&self) -> Result<T, DeserializationError> {
        if self.source.yaml {
            serde_json::from_str(&self.to_json()).map_err(DeserializationError)
        } else {
            serde_json::from_str(self.raw()).map_err(DeserializationError)
        }
    }
    pub fn kind(&self) -> &'static str {
        match self.value() {
            Value::Null => "null",
            Value::Bool(_) => "boolean",
            Value::Number(_) => "number",
            Value::String(_) => "string",
            Value::Array(_) => "array",
            Value::Object(_) => "object",
        }
    }
    pub(crate) fn value(&self) -> &Value {
        self.source
            .value
            .pointer(&self.pointer)
            .expect("validated owning JSON location")
    }
    pub fn location(&self) -> Location {
        Location {
            uri: self.source.uri.clone(),
            pointer: self.pointer.clone(),
            bytes: self.source.spans[&self.pointer].clone(),
            aliases: self
                .source
                .aliases
                .get(&self.pointer)
                .cloned()
                .unwrap_or_default(),
        }
    }
    pub fn source_text(&self) -> &str {
        &self.source.text
    }
    pub fn raw(&self) -> &str {
        &self.source.text[self.source.spans[&self.pointer].clone()]
    }
    pub fn as_str(&self) -> Option<&str> {
        self.value().as_str()
    }
    pub fn as_bool(&self) -> Option<bool> {
        self.value().as_bool()
    }
    pub fn is_null(&self) -> bool {
        self.value().is_null()
    }
    /// Exact JSON numeric token. JSON keeps its authored spelling; YAML radix and
    /// non-JSON decimal spellings normalize without floating-point conversion.
    pub fn number_text(&self) -> Option<&str> {
        self.value().is_number().then(|| {
            self.source
                .numbers
                .get(&self.pointer)
                .map_or_else(|| self.raw(), String::as_str)
        })
    }
    /// JSON for this logical value. YAML values preserve exact numbers, while
    /// formatting/escapes use JSON. `raw()` separately returns authored source.
    pub fn to_json(&self) -> String {
        if !self.source.yaml {
            return self.raw().to_owned();
        }
        let mut output = String::new();
        self.write_json(&mut output);
        output
    }
    fn write_json(&self, output: &mut String) {
        match self.value() {
            Value::Number(_) => output.push_str(self.number_text().unwrap()),
            Value::Array(values) => {
                output.push('[');
                for index in 0..values.len() {
                    if index > 0 {
                        output.push(',');
                    }
                    self.at(index).unwrap().write_json(output);
                }
                output.push(']');
            }
            Value::Object(fields) => {
                output.push('{');
                for (index, key) in fields.keys().enumerate() {
                    if index > 0 {
                        output.push(',');
                    }
                    output.push_str(&serde_json::to_string(key).unwrap());
                    output.push(':');
                    self.get(key).unwrap().write_json(output);
                }
                output.push('}');
            }
            value => output.push_str(&value.to_string()),
        }
    }
    pub fn get(&self, key: &str) -> Option<Self> {
        self.value().as_object()?.get(key)?;
        self.pointer(&format!("/{}", escape(key)))
    }
    pub fn at(&self, index: usize) -> Option<Self> {
        self.value().as_array()?.get(index)?;
        self.pointer(&format!("/{index}"))
    }
    /// Resolve a JSON Pointer relative to this value. The empty pointer is self.
    pub fn pointer(&self, pointer: &str) -> Option<Self> {
        if !valid_pointer(pointer) {
            return None;
        }
        let pointer = format!("{}{pointer}", self.pointer);
        self.source.spans.contains_key(&pointer).then(|| Self {
            source: self.source.clone(),
            pointer,
        })
    }
    pub fn members(&self) -> Option<Vec<(String, Self)>> {
        Some(
            self.value()
                .as_object()?
                .keys()
                .map(|key| (key.clone(), self.get(key).unwrap()))
                .collect(),
        )
    }
    pub fn elements(&self) -> Option<Vec<Self>> {
        Some(
            (0..self.value().as_array()?.len())
                .map(|i| self.at(i).unwrap())
                .collect(),
        )
    }
}

/// Safe default formatting for a failed application-type projection.
pub struct DeserializationError(serde_json::Error);
impl DeserializationError {
    pub fn line(&self) -> usize {
        self.0.line()
    }
    pub fn column(&self) -> usize {
        self.0.column()
    }
    /// Explicit access to Serde's error, which may contain message data.
    pub fn detail(&self) -> &serde_json::Error {
        &self.0
    }
}
impl std::fmt::Display for DeserializationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "JSON does not match the requested Rust type at line {}, column {}",
            self.line(),
            self.column()
        )
    }
}
impl std::fmt::Debug for DeserializationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(self, f)
    }
}
impl std::error::Error for DeserializationError {}

pub(crate) fn escape(key: &str) -> String {
    key.replace('~', "~0").replace('/', "~1")
}
pub(crate) fn valid_pointer(pointer: &str) -> bool {
    if !pointer.is_empty() && !pointer.starts_with('/') {
        return false;
    }
    let mut chars = pointer.chars();
    while let Some(c) = chars.next() {
        if c == '~' && !matches!(chars.next(), Some('0' | '1')) {
            return false;
        }
    }
    true
}

// Input syntax has already been checked by serde_json's borrowed RawValue parser.
// This pass records spans and constructs objects directly. Deserializing an entire
// arbitrary_precision Value would interpret serde's private Number map key as a
// tagged number, which is not a reserved key in an AsyncAPI document.
struct Scanner<'a> {
    text: &'a str,
    uri: Option<String>,
    at: usize,
    nodes: usize,
    limits: Limits,
    spans: BTreeMap<String, Range<usize>>,
}
impl Scanner<'_> {
    fn skip(&mut self) {
        while self
            .text
            .as_bytes()
            .get(self.at)
            .is_some_and(u8::is_ascii_whitespace)
        {
            self.at += 1;
        }
    }
    fn string(&mut self) -> Range<usize> {
        let start = self.at;
        self.at += 1;
        loop {
            let c = self.text.as_bytes()[self.at];
            self.at += 1;
            match c {
                b'\\' => self.at += 1,
                b'"' => return start..self.at,
                _ => {}
            }
        }
    }
    fn error(&self, code: Code, detail: &str, pointer: &str, start: usize) -> Diagnostic {
        Diagnostic::new(code, detail).at(Location {
            uri: self.uri.clone(),
            pointer: pointer.into(),
            bytes: start..self.at,
            aliases: Vec::new(),
        })
    }
    fn value(&mut self, pointer: String, depth: usize) -> Result<Value, Diagnostic> {
        self.skip();
        let start = self.at;
        // Keep the recursive scanner and downstream Value operations within a
        // fixed implementation ceiling even if a caller supplies a larger bound.
        if depth > self.limits.depth.min(96) || self.nodes >= self.limits.nodes {
            return Err(self.error(
                Code::Limit,
                "JSON depth or node limit exceeded",
                &pointer,
                start,
            ));
        }
        self.nodes += 1;
        let value = match self.text.as_bytes()[self.at] {
            b'{' => {
                let mut fields = serde_json::Map::new();
                self.at += 1;
                self.skip();
                let mut keys = HashSet::new();
                if self.text.as_bytes()[self.at] != b'}' {
                    loop {
                        self.skip();
                        let key_span = self.string();
                        let key: String = serde_json::from_str(&self.text[key_span.clone()])
                            .map_err(|_| {
                                self.error(
                                    Code::InvalidJson,
                                    "object key is not valid Unicode",
                                    &pointer,
                                    key_span.start,
                                )
                            })?;
                        let child = format!("{pointer}/{}", escape(&key));
                        if !keys.insert(key.clone()) {
                            return Err(self.error(
                                Code::DuplicateMember,
                                "duplicate object member",
                                &child,
                                key_span.start,
                            ));
                        }
                        self.skip();
                        self.at += 1;
                        fields.insert(key, self.value(child, depth + 1)?);
                        self.skip();
                        if self.text.as_bytes()[self.at] == b'}' {
                            break;
                        }
                        self.at += 1;
                    }
                }
                self.at += 1;
                Value::Object(fields)
            }
            b'[' => {
                let mut items = Vec::new();
                self.at += 1;
                self.skip();
                let mut index = 0;
                if self.text.as_bytes()[self.at] != b']' {
                    loop {
                        items.push(self.value(format!("{pointer}/{index}"), depth + 1)?);
                        index += 1;
                        self.skip();
                        if self.text.as_bytes()[self.at] == b']' {
                            break;
                        }
                        self.at += 1;
                    }
                }
                self.at += 1;
                Value::Array(items)
            }
            b'"' => {
                let span = self.string();
                Value::String(serde_json::from_str(&self.text[span]).map_err(|_| {
                    self.error(
                        Code::InvalidJson,
                        "string is not valid Unicode",
                        &pointer,
                        start,
                    )
                })?)
            }
            _ => {
                while self
                    .text
                    .as_bytes()
                    .get(self.at)
                    .is_some_and(|c| !c.is_ascii_whitespace() && !b",]}".contains(c))
                {
                    self.at += 1;
                }
                serde_json::from_str(&self.text[start..self.at]).map_err(|_| {
                    self.error(Code::InvalidJson, "invalid JSON scalar", &pointer, start)
                })?
            }
        };
        self.spans.insert(pointer, start..self.at);
        Ok(value)
    }
}
