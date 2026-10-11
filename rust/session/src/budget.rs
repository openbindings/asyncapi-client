use crate::{RuntimeCode, RuntimeError};
use serde::{Deserialize, Serialize};
use std::sync::{Arc, Mutex};

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields, rename_all = "camelCase")]
pub struct Limits {
    pub max_messages: usize,
    pub max_buffered_bytes: usize,
    pub max_message_bytes: usize,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            max_messages: 64,
            max_buffered_bytes: 1024 * 1024,
            max_message_bytes: 1024 * 1024,
        }
    }
}
impl Limits {
    pub fn validate(&self) -> Result<(), RuntimeError> {
        if self.max_messages == 0
            || self.max_messages > 64
            || self.max_buffered_bytes == 0
            || self.max_buffered_bytes > 1024 * 1024
            || self.max_message_bytes == 0
            || self.max_message_bytes > self.max_buffered_bytes
        {
            return Err(RuntimeError::new(
                RuntimeCode::InvalidConfiguration,
                "session limits are outside the initial profile",
            ));
        }
        Ok(())
    }
}
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize)]
pub struct Usage {
    pub messages: usize,
    pub bytes: usize,
}
struct Inner {
    limits: Limits,
    usage: Mutex<Usage>,
}
/// Clones share one admission budget. Reservations account for count and bytes
/// atomically; rejected reservations do not retain either kind of capacity.
#[derive(Clone)]
pub struct Budget(Arc<Inner>);
pub struct Lease {
    inner: Arc<Inner>,
    bytes: usize,
}
impl Budget {
    pub fn new(limits: Limits) -> Result<Self, RuntimeError> {
        limits.validate()?;
        Ok(Self(Arc::new(Inner {
            limits,
            usage: Mutex::new(Usage::default()),
        })))
    }
    pub fn reserve(&self, bytes: usize) -> Result<Lease, RuntimeError> {
        let mut usage = self.0.usage.lock().map_err(|_| {
            RuntimeError::new(RuntimeCode::DriverFailed, "session budget lock failed")
        })?;
        if bytes > self.0.limits.max_message_bytes
            || usage.messages >= self.0.limits.max_messages
            || bytes > self.0.limits.max_buffered_bytes - usage.bytes
        {
            return Err(RuntimeError::new(
                RuntimeCode::Backpressure,
                "session message or byte capacity exhausted",
            ));
        }
        usage.messages += 1;
        usage.bytes += bytes;
        Ok(Lease {
            inner: self.0.clone(),
            bytes,
        })
    }
    pub fn usage(&self) -> Usage {
        *self
            .0
            .usage
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
    }
}
impl Drop for Lease {
    fn drop(&mut self) {
        let mut usage = self
            .inner
            .usage
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        usage.messages -= 1;
        usage.bytes -= self.bytes;
    }
}
