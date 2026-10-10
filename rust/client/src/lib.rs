//! A standalone AsyncAPI semantic engine with explicit source ownership.
//!
//! Source admission and inspection are currently implemented for JSON documents.
//! Admission is not whole-document or payload validation. No method performs I/O.
#![forbid(unsafe_code)]

mod document;
mod source;

pub use document::{Action, Document, Edition, Operation, OperationDescription, OperationIdentity};
pub use source::{Json, Location};

/// Stable coarse diagnostic classification. Individual reasons may be extended.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize)]
#[non_exhaustive]
pub enum Code {
    InvalidJson,
    DuplicateMember,
    InvalidDocument,
    UnsupportedVersion,
    InvalidReference,
    MissingResource,
    MissingReferenceTarget,
    ReferenceCycle,
    InvalidOperation,
    MissingOperation,
    AmbiguousOperation,
    Limit,
}

/// A requirement the caller can satisfy without changing the original source.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
#[non_exhaustive]
#[serde(tag = "kind", rename_all = "camelCase")]
pub enum Requirement {
    Resource { uri: String },
    SourceUri,
}

/// An error tied to the original resource and JSON Pointer when available.
/// Default details describe the failure without copying payloads or credentials.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
pub struct Diagnostic {
    pub code: Code,
    pub location: Option<Location>,
    pub requirement: Option<Requirement>,
    pub detail: String,
}

impl Diagnostic {
    pub(crate) fn new(code: Code, detail: impl Into<String>) -> Self {
        Self {
            code,
            location: None,
            requirement: None,
            detail: detail.into(),
        }
    }
    pub(crate) fn at(mut self, location: Location) -> Self {
        self.location = Some(location);
        self
    }
    pub(crate) fn needs(mut self, requirement: Requirement) -> Self {
        self.requirement = Some(requirement);
        self
    }
}

impl std::fmt::Display for Diagnostic {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}: {}", self.code, self.detail)
    }
}
impl std::error::Error for Diagnostic {}

/// Admission and traversal bounds. These defaults are development policies.
#[derive(Clone, Copy, Debug)]
pub struct Limits {
    pub source_bytes: usize,
    pub total_source_bytes: usize,
    /// Maximum nesting depth; the current implementation also caps this at 96.
    pub depth: usize,
    pub nodes: usize,
    pub resources: usize,
    pub reference_steps: usize,
    pub trait_count: usize,
    pub merge_nodes: usize,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            source_bytes: 16 * 1024 * 1024,
            total_source_bytes: 64 * 1024 * 1024,
            depth: 96,
            nodes: 500_000,
            resources: 128,
            reference_steps: 64,
            trait_count: 64,
            merge_nodes: 500_000,
        }
    }
}
