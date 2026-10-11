use serde::Serialize;
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[non_exhaustive]
pub enum RuntimeCode {
    InvalidConfiguration,
    InvalidPayload,
    Unsupported,
    Closed,
    Cancelled,
    Backpressure,
    Deadline,
    Connection,
    Protocol,
    DriverFailed,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct RuntimeError {
    pub code: RuntimeCode,
    pub detail: &'static str,
    /// On a failed send, bytes may have left the process without a final receipt.
    pub delivery_unknown: bool,
    pub diagnostic: Option<dynamic_asyncapi_client::Diagnostic>,
}
impl RuntimeError {
    pub fn new(code: RuntimeCode, detail: &'static str) -> Self {
        Self {
            code,
            detail,
            delivery_unknown: false,
            diagnostic: None,
        }
    }
    pub fn payload(diagnostic: dynamic_asyncapi_client::Diagnostic) -> Self {
        Self {
            code: RuntimeCode::InvalidPayload,
            detail: "payload does not satisfy the prepared codec",
            delivery_unknown: false,
            diagnostic: Some(diagnostic),
        }
    }
    pub fn uncertain(mut self) -> Self {
        self.delivery_unknown = true;
        self
    }
}
impl std::fmt::Display for RuntimeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}: {}", self.code, self.detail)
    }
}
impl std::error::Error for RuntimeError {}
