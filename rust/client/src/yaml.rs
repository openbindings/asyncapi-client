//! YAML grammar/events are delegated to granit-parser; source ownership, scalar
//! policy and bounded graph expansion belong to the AsyncAPI source layer.
use crate::source::escape;
use crate::{Code, Diagnostic, Limits, Location};
use granit_parser::{Event, Parser, ScalarStyle, Tag};
use serde_json::Value;
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    ops::Range,
};

pub(crate) struct Parsed {
    pub value: Value,
    pub spans: BTreeMap<String, Range<usize>>,
    pub aliases: BTreeMap<String, Vec<Range<usize>>>,
    pub numbers: BTreeMap<String, String>,
}
enum Kind {
    Scalar(Value, Option<String>),
    Sequence(Vec<usize>),
    Mapping(Vec<(String, usize)>),
    Alias(usize),
}
struct Node {
    kind: Kind,
    span: Range<usize>,
}
struct Frame {
    id: usize,
    pointer: String,
    key: Option<String>,
    keys: HashSet<String>,
}
fn failure(
    code: Code,
    detail: &str,
    uri: Option<&str>,
    pointer: &str,
    span: Range<usize>,
) -> Diagnostic {
    Diagnostic::new(code, detail).at(Location {
        uri: uri.map(str::to_owned),
        pointer: pointer.into(),
        bytes: span,
        aliases: Vec::new(),
    })
}
fn check_tag(tag: Option<&Tag>, expected: &str) -> bool {
    tag.is_none_or(|tag| tag.is_yaml_core_schema_tag(expected))
}

pub(crate) fn parse(source: &str, uri: Option<&str>, limits: Limits) -> Result<Parsed, Diagnostic> {
    let depth = limits.depth.min(96);
    let options = granit_parser::options! {emit_comments:false,flow_nesting_limit:depth,block_nesting_limit:depth};
    let mut nodes: Vec<Node> = Vec::new();
    let mut stack: Vec<Frame> = Vec::new();
    let mut anchors = HashMap::new();
    let mut root = None;
    let mut documents = 0;
    let mut number_work = limits.number_conversion_work;
    for entry in Parser::new_from_str_with_options(source, options) {
        let (event, span) = entry.map_err(|e| {
            let position = e
                .marker()
                .byte_offset()
                .unwrap_or(source.len())
                .min(source.len());
            let code = if matches!(e.kind(), granit_parser::ErrorKind::RecursionLimitExceeded) {
                Code::Limit
            } else {
                Code::InvalidYaml
            };
            failure(
                code,
                "YAML source could not be admitted",
                uri,
                "",
                position..position,
            )
        })?;
        let range = span
            .byte_range()
            .expect("UTF-8 parser provides byte coordinates");
        let diagnostic_range = span
            .tag_start()
            .and_then(|marker| marker.byte_offset())
            .map_or_else(|| range.clone(), |start| start..range.end);
        match event {
            Event::StreamStart | Event::StreamEnd | Event::DocumentEnd | Event::Comment(..) => {
                continue;
            }
            Event::DocumentStart(_, version) => {
                documents += 1;
                if documents > 1 || version.is_some_and(|v| v.major != 1 || v.minor != 2) {
                    return Err(failure(
                        Code::UnsupportedYaml,
                        "one YAML 1.2 document is required",
                        uri,
                        "",
                        range,
                    ));
                }
                continue;
            }
            Event::SequenceEnd | Event::MappingEnd => {
                let frame = stack.pop().expect("balanced YAML collection events");
                nodes[frame.id].span.end = range.end;
                continue;
            }
            _ => {}
        }
        let key_position = stack.last().is_some_and(|frame| {
            matches!(nodes[frame.id].kind, Kind::Mapping(_)) && frame.key.is_none()
        });
        let pointer = stack
            .last()
            .map_or_else(String::new, |frame| match &nodes[frame.id].kind {
                Kind::Sequence(items) => format!("{}/{}", frame.pointer, items.len()),
                Kind::Mapping(_) => frame.key.as_ref().map_or_else(
                    || frame.pointer.clone(),
                    |key| format!("{}/{}", frame.pointer, escape(key)),
                ),
                _ => unreachable!(),
            });
        if nodes.len() >= limits.nodes {
            return Err(failure(
                Code::Limit,
                "YAML node limit exceeded",
                uri,
                &pointer,
                range,
            ));
        }
        let (kind, anchor, container) = match event {
            Event::Scalar(value, style, anchor, tag) => {
                let scalar = if key_position {
                    if !check_tag(tag.as_deref(), "str") {
                        return Err(failure(
                            Code::UnsupportedYaml,
                            "YAML map key must be a string",
                            uri,
                            &pointer,
                            diagnostic_range,
                        ));
                    }
                    (Value::String(value.to_string()), None)
                } else {
                    scalar(&value, style, tag.as_deref(), &mut number_work).map_err(
                        |(code, detail)| {
                            failure(code, detail, uri, &pointer, diagnostic_range.clone())
                        },
                    )?
                };
                (Kind::Scalar(scalar.0, scalar.1), anchor, false)
            }
            Event::SequenceStart(_, anchor, tag) => {
                if key_position || !check_tag(tag.as_deref(), "seq") {
                    return Err(failure(
                        Code::UnsupportedYaml,
                        "unsupported YAML collection tag or key",
                        uri,
                        &pointer,
                        diagnostic_range,
                    ));
                }
                (Kind::Sequence(Vec::new()), anchor, true)
            }
            Event::MappingStart(_, anchor, tag) => {
                if key_position || !check_tag(tag.as_deref(), "map") {
                    return Err(failure(
                        Code::UnsupportedYaml,
                        "unsupported YAML collection tag or key",
                        uri,
                        &pointer,
                        diagnostic_range,
                    ));
                }
                (Kind::Mapping(Vec::new()), anchor, true)
            }
            Event::Alias(anchor) => {
                let target = *anchors.get(&anchor).ok_or_else(|| {
                    failure(
                        Code::InvalidYaml,
                        "unknown YAML alias",
                        uri,
                        &pointer,
                        range.clone(),
                    )
                })?;
                (Kind::Alias(target), 0, false)
            }
            _ => {
                return Err(failure(
                    Code::InvalidYaml,
                    "unexpected YAML event",
                    uri,
                    &pointer,
                    range,
                ));
            }
        };
        let id = nodes.len();
        nodes.push(Node {
            kind,
            span: range.clone(),
        });
        if anchor != 0 {
            anchors.insert(anchor, id);
        }
        if key_position {
            let mut key_id = id;
            let mut visited = HashSet::new();
            while let Kind::Alias(target) = nodes[key_id].kind {
                if !visited.insert(key_id) {
                    return Err(failure(
                        Code::UnsupportedYaml,
                        "recursive YAML key alias",
                        uri,
                        &pointer,
                        range,
                    ));
                }
                key_id = target;
            }
            let Kind::Scalar(Value::String(key), _) = &nodes[key_id].kind else {
                return Err(failure(
                    Code::UnsupportedYaml,
                    "YAML map key must resolve to a string",
                    uri,
                    &pointer,
                    range,
                ));
            };
            let frame = stack.last_mut().unwrap();
            if !frame.keys.insert(key.clone()) {
                return Err(failure(
                    Code::DuplicateMember,
                    "duplicate YAML map key",
                    uri,
                    &format!("{}/{}", frame.pointer, escape(key)),
                    range,
                ));
            }
            frame.key = Some(key.clone());
        } else if let Some(frame) = stack.last_mut() {
            match &mut nodes[frame.id].kind {
                Kind::Sequence(items) => items.push(id),
                Kind::Mapping(fields) => fields.push((frame.key.take().unwrap(), id)),
                _ => unreachable!(),
            }
        } else if root.replace(id).is_some() {
            return Err(failure(
                Code::InvalidYaml,
                "multiple YAML roots",
                uri,
                &pointer,
                range,
            ));
        }
        if container {
            if stack.len() >= depth {
                return Err(failure(
                    Code::Limit,
                    "YAML depth limit exceeded",
                    uri,
                    &pointer,
                    range,
                ));
            }
            stack.push(Frame {
                id,
                pointer,
                key: None,
                keys: HashSet::new(),
            });
        }
    }
    let root = root.ok_or_else(|| {
        failure(
            Code::InvalidYaml,
            "empty YAML document",
            uri,
            "",
            0..source.len(),
        )
    })?;
    let mut expander = Expander {
        nodes: &nodes,
        uri,
        limits,
        used_nodes: 0,
        bytes: 0,
        active: HashSet::new(),
        parsed: Parsed {
            value: Value::Null,
            spans: BTreeMap::new(),
            aliases: BTreeMap::new(),
            numbers: BTreeMap::new(),
        },
    };
    expander.parsed.value = expander.expand(root, String::new(), 0, Vec::new())?;
    Ok(expander.parsed)
}

type ScalarError = (Code, &'static str);
fn scalar(
    value: &str,
    style: ScalarStyle,
    tag: Option<&Tag>,
    work: &mut usize,
) -> Result<(Value, Option<String>), ScalarError> {
    let explicit = tag
        .map(|tag| {
            tag.core_suffix()
                .ok_or((Code::UnsupportedYaml, "unsupported YAML tag"))
        })
        .transpose()?;
    if explicit == Some("str") || (explicit.is_none() && style != ScalarStyle::Plain) {
        return Ok((Value::String(value.into()), None));
    }
    if explicit.is_some_and(|s| !matches!(s, "null" | "bool" | "int" | "float")) {
        return Err((Code::UnsupportedYaml, "unsupported YAML scalar tag"));
    }
    if explicit.is_none_or(|s| s == "null") && matches!(value, "" | "~" | "null" | "Null" | "NULL")
    {
        return Ok((Value::Null, None));
    }
    if explicit.is_none_or(|s| s == "bool") {
        match value {
            "true" | "True" | "TRUE" => return Ok((Value::Bool(true), None)),
            "false" | "False" | "FALSE" => return Ok((Value::Bool(false), None)),
            _ => {}
        }
    }
    if matches!(
        value.to_ascii_lowercase().as_str(),
        ".inf" | "+.inf" | "-.inf" | ".nan"
    ) {
        return Err((
            Code::UnsupportedYaml,
            "non-finite YAML numbers cannot become JSON",
        ));
    }
    let mut number = None;
    if explicit.is_none_or(|s| s == "int" || s == "float") {
        if explicit != Some("float")
            && let Some((digits, base)) = value
                .strip_prefix("0x")
                .map(|s| (s, 16))
                .or_else(|| value.strip_prefix("0o").map(|s| (s, 8)))
            && !digits.is_empty()
            && digits.bytes().all(|b| (b as char).is_digit(base))
        {
            let cost = digits
                .len()
                .checked_mul(digits.len())
                .ok_or((Code::Limit, "numeric conversion bound exceeded"))?;
            *work = work
                .checked_sub(cost)
                .ok_or((Code::Limit, "numeric conversion work limit exceeded"))?;
            number = Some(radix_decimal(digits, base));
        } else {
            number = decimal(value, explicit == Some("int"));
        }
    }
    if let Some(number) = number {
        let parsed = number
            .parse::<serde_json::Number>()
            .map_err(|_| (Code::InvalidYaml, "numeric scalar could not be represented"))?;
        return Ok((Value::Number(parsed), Some(number)));
    }
    if explicit.is_some() {
        return Err((Code::UnsupportedYaml, "invalid explicitly tagged scalar"));
    }
    Ok((Value::String(value.into()), None))
}
fn decimal(value: &str, integer_only: bool) -> Option<String> {
    let (sign, value) = if let Some(v) = value.strip_prefix('-') {
        ("-", v)
    } else {
        ("", value.strip_prefix('+').unwrap_or(value))
    };
    let pieces: Vec<_> = value.split(['e', 'E']).collect();
    if pieces.len() > 2 || (integer_only && pieces.len() > 1) {
        return None;
    }
    let exponent = pieces.get(1).copied();
    if let Some(e) = exponent {
        let digits = e.strip_prefix(['+', '-']).unwrap_or(e);
        if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
            return None;
        }
    }
    let mantissa: Vec<_> = pieces[0].split('.').collect();
    if mantissa.len() > 2 || (integer_only && mantissa.len() > 1) {
        return None;
    }
    if !mantissa
        .iter()
        .all(|p| p.bytes().all(|b| b.is_ascii_digit()))
        || mantissa.iter().all(|p| p.is_empty())
    {
        return None;
    }
    let integer = mantissa[0].trim_start_matches('0');
    let mut out = format!("{sign}{}", if integer.is_empty() { "0" } else { integer });
    if let Some(fraction) = mantissa.get(1) {
        out.push('.');
        out.push_str(if fraction.is_empty() { "0" } else { fraction });
    }
    if let Some(e) = exponent {
        out.push('e');
        out.push_str(e);
    }
    Some(out)
}
fn radix_decimal(digits: &str, base: u32) -> String {
    let mut decimal = vec![0u32];
    for c in digits.chars() {
        let mut carry = c.to_digit(base).unwrap();
        for digit in &mut decimal {
            let n = *digit * base + carry;
            *digit = n % 10;
            carry = n / 10;
        }
        while carry > 0 {
            decimal.push(carry % 10);
            carry /= 10;
        }
    }
    decimal
        .into_iter()
        .rev()
        .map(|n| char::from_digit(n, 10).unwrap())
        .collect()
}
struct Expander<'a> {
    nodes: &'a [Node],
    uri: Option<&'a str>,
    limits: Limits,
    used_nodes: usize,
    bytes: usize,
    active: HashSet<usize>,
    parsed: Parsed,
}
impl Expander<'_> {
    fn spend(&mut self, bytes: usize, node: &Node, pointer: &str) -> Result<(), Diagnostic> {
        self.bytes = self.bytes.saturating_add(bytes);
        if self.bytes > self.limits.expanded_bytes {
            return Err(failure(
                Code::Limit,
                "expanded YAML byte limit exceeded",
                self.uri,
                pointer,
                node.span.clone(),
            ));
        }
        Ok(())
    }
    fn expand(
        &mut self,
        id: usize,
        pointer: String,
        depth: usize,
        mut aliases: Vec<Range<usize>>,
    ) -> Result<Value, Diagnostic> {
        let node = &self.nodes[id];
        if depth > self.limits.depth.min(96) || self.used_nodes >= self.limits.nodes {
            return Err(failure(
                Code::Limit,
                "expanded YAML node/depth limit exceeded",
                self.uri,
                &pointer,
                node.span.clone(),
            ));
        }
        self.used_nodes += 1;
        if !self.active.insert(id) {
            return Err(failure(
                Code::UnsupportedYaml,
                "recursive YAML alias cannot become a JSON tree",
                self.uri,
                &pointer,
                node.span.clone(),
            ));
        }
        let result = match &node.kind {
            Kind::Alias(target) => {
                aliases.push(node.span.clone());
                self.expand(*target, pointer, depth + 1, aliases)?
            }
            kind => {
                self.parsed.spans.insert(pointer.clone(), node.span.clone());
                if !aliases.is_empty() {
                    self.parsed.aliases.insert(pointer.clone(), aliases.clone());
                }
                match kind {
                    Kind::Scalar(value, number) => {
                        let bytes = if let Some(n) = number {
                            n.len()
                        } else if let Value::String(s) = value {
                            json_string_len(s)
                        } else if matches!(value, Value::Bool(false)) {
                            5
                        } else {
                            4
                        };
                        self.spend(bytes, node, &pointer)?;
                        if let Some(number) = number {
                            self.parsed.numbers.insert(pointer, number.clone());
                        }
                        value.clone()
                    }
                    Kind::Sequence(items) => {
                        self.spend(2 + items.len().saturating_sub(1), node, &pointer)?;
                        let mut values = Vec::with_capacity(items.len().min(self.limits.nodes));
                        for (index, id) in items.iter().enumerate() {
                            values.push(self.expand(
                                *id,
                                format!("{pointer}/{index}"),
                                depth + 1,
                                aliases.clone(),
                            )?);
                        }
                        Value::Array(values)
                    }
                    Kind::Mapping(fields) => {
                        self.spend(2 + fields.len().saturating_sub(1), node, &pointer)?;
                        let mut values = serde_json::Map::new();
                        for (key, id) in fields {
                            self.spend(json_string_len(key) + 1, node, &pointer)?;
                            values.insert(
                                key.clone(),
                                self.expand(
                                    *id,
                                    format!("{pointer}/{}", escape(key)),
                                    depth + 1,
                                    aliases.clone(),
                                )?,
                            );
                        }
                        Value::Object(values)
                    }
                    Kind::Alias(_) => unreachable!(),
                }
            }
        };
        self.active.remove(&id);
        Ok(result)
    }
}
fn json_string_len(value: &str) -> usize {
    2 + value
        .chars()
        .map(|c| match c {
            '"' | '\\' | '\n' | '\r' | '\t' | '\u{8}' | '\u{c}' => 2,
            c if c < '\u{20}' => 6,
            c => c.len_utf8(),
        })
        .sum::<usize>()
}
