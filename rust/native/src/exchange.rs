//! An exchange owns its connection. Dropping its future drops the session.
use crate::{
    CloseReceipt, Incoming, Receipt, Received, RuntimeCode, RuntimeError, Session, SessionOptions,
};
use dynamic_asyncapi_client::{ExchangePlan, Json};
use dynamic_asyncapi_session::SessionPlan;
use std::time::Duration;
use tokio::time::{Instant, timeout_at};

#[derive(Debug)]
pub struct ExchangeResponse {
    pub request_receipt: Receipt,
    pub reply: Received,
    pub unmatched_replies: usize,
    /// A valid reply is retained even when graceful shutdown fails.
    pub close: Result<CloseReceipt, RuntimeError>,
}
#[derive(Debug)]
pub struct ExchangeError {
    pub error: RuntimeError,
    /// Preserve send evidence when a later receive/deadline fails. This receipt
    /// does not mean the peer processed the request; retries may duplicate work.
    pub request_receipt: Option<Receipt>,
}
impl std::fmt::Display for ExchangeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.error.fmt(f)
    }
}
impl std::error::Error for ExchangeError {}

/// Send once and return the first correlated reply on a dedicated connection.
/// The plan deadline covers connection, send, reply and graceful shutdown.
/// A dropped future closes its owned session; cancellation cannot retract a
/// sent request. There are no automatic retries or generated correlation IDs.
/// Up to 64 well-formed nonmatching replies are discarded, then execution fails.
pub async fn request_reply(
    plan: &ExchangePlan,
    request: &Json,
    options: SessionOptions,
) -> Result<ExchangeResponse, ExchangeError> {
    let deadline = Instant::now() + Duration::from_millis(plan.describe().timeout_ms.into());
    let mut receipt = None;
    let mut send_started = false;
    let result = timeout_at(deadline, async {
        let key = plan.request_key(request).map_err(RuntimeError::payload)?;
        let mut session = Session::open_plan(SessionPlan::exchange(plan)?, options).await?;
        send_started = true;
        receipt = Some(session.sender().send_json(0, request).await?);
        let mut unmatched = 0;
        loop {
            match session.next().await? {
                Some(Incoming::Message(reply)) => {
                    let payload = reply
                        .payload
                        .as_json()
                        .expect("exchange plans require JSON");
                    if plan.matches_reply(&key, payload) {
                        // Preserve the response even if closing consumes the
                        // remaining deadline or the peer refuses a clean close.
                        return Ok((reply, unmatched, session));
                    }
                    unmatched += 1;
                    if unmatched > 64 {
                        return Err(RuntimeError::new(
                            RuntimeCode::Backpressure,
                            "exchange exceeded 64 unmatched replies",
                        ));
                    }
                }
                Some(Incoming::InvalidPayload { diagnostic, .. }) => {
                    return Err(RuntimeError::payload(diagnostic));
                }
                Some(Incoming::Rejected { .. }) => {
                    return Err(RuntimeError::new(
                        RuntimeCode::Protocol,
                        "exchange received an unexpected frame",
                    ));
                }
                None => {
                    return Err(RuntimeError::new(
                        RuntimeCode::Closed,
                        "connection closed before a matching reply",
                    ));
                }
            }
        }
    })
    .await;
    let (reply, unmatched_replies, session) = match result {
        Ok(Ok(value)) => value,
        Ok(Err(error)) => {
            return Err(ExchangeError {
                error,
                request_receipt: receipt,
            });
        }
        Err(_) => {
            let mut error = RuntimeError::new(RuntimeCode::Deadline, "exchange deadline expired");
            error.delivery_unknown = send_started && receipt.is_none();
            return Err(ExchangeError {
                error,
                request_receipt: receipt,
            });
        }
    };
    let close = timeout_at(deadline, session.close())
        .await
        .unwrap_or_else(|_| {
            Err(RuntimeError::new(
                RuntimeCode::Deadline,
                "exchange shutdown deadline expired",
            ))
        });
    Ok(ExchangeResponse {
        request_receipt: receipt.unwrap(),
        reply,
        unmatched_replies,
        close,
    })
}
