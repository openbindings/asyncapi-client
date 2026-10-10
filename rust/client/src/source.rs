use crate::{Code, Diagnostic, Limits};
use serde_json::{Value, value::RawValue};
use std::{
    collections::{BTreeMap, HashSet},
    ops::Range,
    sync::Arc,
};

/// Coordinates in an original UTF-8 source, not a reserialized document.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
pub struct Location {
    pub uri: Option<String>,
    pub pointer: String,
    pub bytes: Range<usize>,
}

#[derive(Debug)]
pub(crate) struct Source {
    pub uri: Option<String>,
    pub text: Arc<str>,
    pub value: Value,
    spans: BTreeMap<String, Range<usize>>,
}

impl Source {
    pub fn parse(text: &str, uri: Option<String>, limits: Limits) -> Result<Arc<Self>, Diagnostic> {
        if text.len() > limits.source_bytes {
            return Err(Diagnostic::new(Code::Limit, "source byte limit exceeded"));
        }
        // Validate syntax without first materializing an unbounded Value tree.
        let _: &RawValue = serde_json::from_str(text)
            .map_err(|_| Diagnostic::new(Code::InvalidJson, "invalid JSON source"))?;
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
    /// The authored numeric token, including negative zero and exponent spelling.
    pub fn number_text(&self) -> Option<&str> {
        self.value().is_number().then(|| self.raw())
    }
    /// Original JSON for this value, preserving all numeric tokens and members.
    pub fn to_json(&self) -> String {
        self.raw().to_owned()
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
                        let key: String =
                            serde_json::from_str(&self.text[key_span.clone()]).unwrap();
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
                Value::String(serde_json::from_str(&self.text[span]).expect("validated string"))
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
                serde_json::from_str(&self.text[start..self.at]).expect("validated scalar")
            }
        };
        self.spans.insert(pointer, start..self.at);
        Ok(value)
    }
}
