//! Payload representation is separate from message headers and transport framing.
use crate::{Code, Diagnostic, Json, Limits};
use bytes::Bytes;
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum Codec {
    Binary,
    Utf8,
    Json,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum WebSocketFrame {
    Binary,
    Text,
}

/// Owned encoded bytes and, for JSON, an exact decoded view. Parsing happens
/// once on admission. Binary Bytes retain shared ownership without a body copy.
/// The JSON view has its own bounded source/index allocation; wire-byte budgets
/// are not a measurement of its full heap use.
#[derive(Clone, Debug)]
pub struct Payload {
    bytes: Bytes,
    codec: Codec,
    json: Option<Json>,
}
impl Payload {
    pub fn binary(bytes: impl Into<Bytes>) -> Self {
        Self {
            bytes: bytes.into(),
            codec: Codec::Binary,
            json: None,
        }
    }
    pub fn text(text: impl Into<String>) -> Self {
        Self {
            bytes: Bytes::from(text.into()),
            codec: Codec::Utf8,
            json: None,
        }
    }
    /// Existing exact values need no second parse; YAML views encode as JSON.
    /// Only the selected value is emitted, never its containing document.
    pub fn from_json(json: Json) -> Self {
        Self {
            bytes: Bytes::from(json.to_json()),
            codec: Codec::Json,
            json: Some(json),
        }
    }
    pub fn codec(&self) -> Codec {
        self.codec
    }
    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }
    pub fn len(&self) -> usize {
        self.bytes.len()
    }
    pub fn is_empty(&self) -> bool {
        self.bytes.is_empty()
    }
    pub fn as_text(&self) -> Option<&str> {
        (self.codec == Codec::Utf8)
            .then(|| std::str::from_utf8(&self.bytes).expect("admitted UTF-8"))
    }
    pub fn as_json(&self) -> Option<&Json> {
        self.json.as_ref()
    }
    pub fn into_bytes(self) -> Bytes {
        self.bytes
    }
    pub(crate) fn decode(codec: Codec, bytes: Bytes, limits: Limits) -> Result<Self, Diagnostic> {
        if bytes.len() > limits.source_bytes {
            return Err(Diagnostic::new(Code::Limit, "message byte limit exceeded"));
        }
        let json = match codec {
            Codec::Binary => None,
            Codec::Utf8 | Codec::Json => {
                let text = std::str::from_utf8(&bytes).map_err(|_| {
                    Diagnostic::new(Code::InvalidValue, "message is not valid UTF-8")
                })?;
                if codec == Codec::Json {
                    Some(Json::parse(text, limits)?)
                } else {
                    None
                }
            }
        };
        Ok(Self { bytes, codec, json })
    }
}
impl AsRef<[u8]> for Payload {
    fn as_ref(&self) -> &[u8] {
        self.as_bytes()
    }
}

pub(crate) fn select(content_type: &str) -> Option<Codec> {
    fn ows(value: &str) -> &str {
        value.trim_matches([' ', '\t'])
    }
    let mut parts = content_type.split(';');
    let media = ows(parts.next()?);
    let (kind, subtype) = media.split_once('/')?;
    if kind.is_empty()
        || subtype.is_empty()
        || !kind
            .bytes()
            .chain(subtype.bytes())
            .all(|c| c.is_ascii_alphanumeric() || b"!#$%&'+-.^_`|~".contains(&c))
    {
        return None;
    }
    let codec = if kind.eq_ignore_ascii_case("application")
        && subtype.eq_ignore_ascii_case("octet-stream")
    {
        Codec::Binary
    } else if kind.eq_ignore_ascii_case("application")
        && (subtype.eq_ignore_ascii_case("json")
            || (subtype.len() > 5 && subtype.to_ascii_lowercase().ends_with("+json")))
    {
        Codec::Json
    } else if kind.eq_ignore_ascii_case("text") && subtype.eq_ignore_ascii_case("plain") {
        Codec::Utf8
    } else {
        return None;
    };
    let mut charset = false;
    for part in parts {
        let (key, value) = ows(part).split_once('=')?;
        if codec == Codec::Binary || charset || !ows(key).eq_ignore_ascii_case("charset") {
            return None;
        }
        let value = ows(value);
        let value = value
            .strip_prefix('"')
            .and_then(|v| v.strip_suffix('"'))
            .unwrap_or(value);
        if !value.eq_ignore_ascii_case("utf-8") {
            return None;
        }
        charset = true;
    }
    Some(codec)
}
