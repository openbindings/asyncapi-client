//! Message runtime expressions, independent of transport and correlation policy.
use crate::{Code, Diagnostic, Document, Json, Location, effective::Effective};
use serde::Serialize;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum ExpressionSource {
    Header,
    Payload,
}

/// An admitted AsyncAPI message expression. The pointer follows the JSON
/// Pointer string grammar in AsyncAPI's ABNF: percent signs are literal, not
/// URI decoding instructions. Parsing performs no message lookup or I/O.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct RuntimeExpression {
    expression: String,
    source: ExpressionSource,
    pointer: String,
}
impl RuntimeExpression {
    /// The initial profile admits at most 16 KiB and 256 pointer segments.
    pub fn parse(expression: &str) -> Result<Self, Diagnostic> {
        if expression.len() > 16 * 1024 {
            return Err(Diagnostic::new(
                Code::Limit,
                "runtime expression byte limit exceeded",
            ));
        }
        let (head, pointer) = expression.split_once('#').unwrap_or((expression, ""));
        let source = match head {
            "$message.header" => ExpressionSource::Header,
            "$message.payload" => ExpressionSource::Payload,
            _ => {
                return Err(Diagnostic::new(
                    Code::InvalidValue,
                    "invalid message runtime expression source",
                ));
            }
        };
        if !crate::source::valid_pointer(pointer) {
            return Err(Diagnostic::new(
                Code::InvalidValue,
                "invalid runtime expression JSON Pointer",
            ));
        }
        if pointer.bytes().filter(|b| *b == b'/').count() > 256 {
            return Err(Diagnostic::new(
                Code::Limit,
                "runtime expression segment limit exceeded",
            ));
        }
        Ok(Self {
            expression: expression.into(),
            source,
            pointer: pointer.into(),
        })
    }
    pub fn expression(&self) -> &str {
        &self.expression
    }
    pub fn source(&self) -> ExpressionSource {
        self.source
    }
    pub fn pointer(&self) -> &str {
        &self.pointer
    }
    /// Missing roots/paths return None; a present JSON null remains Some(Json).
    /// The result owns its source, preserves its type and exact numeric token,
    /// and outlives both the input view and this expression. No value is coerced.
    pub fn evaluate(&self, header: Option<&Json>, payload: Option<&Json>) -> Option<Json> {
        match self.source {
            ExpressionSource::Header => header,
            ExpressionSource::Payload => payload,
        }?
        .pointer(&self.pointer)
    }
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CorrelationDescription {
    pub selection: Location,
    pub definition: Location,
    /// Authored location field, including the actual trait/resource origin.
    pub location: Location,
    pub description: Option<String>,
    pub expression: RuntimeExpression,
}

pub(crate) fn correlation(
    document: &Document,
    declaration: Effective,
) -> Result<CorrelationDescription, Diagnostic> {
    let definition = declaration.resolve(document)?;
    if !definition.is_object() {
        return Err(
            Diagnostic::new(Code::InvalidOperation, "correlation ID must be an object")
                .at(definition.source.location()),
        );
    }
    let location = definition.get("location").ok_or_else(|| {
        Diagnostic::new(
            Code::InvalidOperation,
            "correlation ID location is required",
        )
        .at(definition.source.location())
    })?;
    let text = location.string().ok_or_else(|| {
        Diagnostic::new(
            Code::InvalidOperation,
            "correlation ID location must be a string",
        )
        .at(location.source.location())
    })?;
    let expression = RuntimeExpression::parse(text).map_err(|mut error| {
        if error.code == Code::InvalidValue {
            error.code = Code::InvalidOperation;
        }
        error.at(location.source.location())
    })?;
    Ok(CorrelationDescription {
        selection: declaration.source.location(),
        definition: definition.source.location(),
        location: location.source.location(),
        description: definition.optional_string("description")?,
        expression,
    })
}
