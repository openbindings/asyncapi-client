//! Portable connection admission, routing and ownership limits. No network I/O.
#![forbid(unsafe_code)]
mod authentication;
mod budget;
mod error;
mod plan;
pub use authentication::{AuthenticatedEndpoint, QueryCredentials};
pub use budget::{Budget, Lease, Limits, Usage};
pub use error::{RuntimeCode, RuntimeError};
pub use plan::{ConnectionPlan, MqttSettings, Route, SessionPlan};
