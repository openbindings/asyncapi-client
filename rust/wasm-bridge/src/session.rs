use crate::{PlanHandle, encoded};
use dynamic_asyncapi_host::{
    Cancellation, Incoming, RuntimeCode, RuntimeError, Sender, Session, SessionOptions,
};
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::future_to_promise;
fn error(error: RuntimeError) -> JsValue {
    JsValue::from_str(&encoded(&error))
}
fn closed() -> JsValue {
    error(RuntimeError::new(
        RuntimeCode::Closed,
        "session handle is closed",
    ))
}
#[wasm_bindgen]
#[derive(Default)]
pub struct CancellationHandle(Cancellation);
#[wasm_bindgen]
impl CancellationHandle {
    #[wasm_bindgen(constructor)]
    pub fn new() -> Self {
        Self::default()
    }
    pub fn cancel(&self) {
        self.0.cancel();
    }
}
#[wasm_bindgen]
#[derive(Default)]
pub struct HostSessionBuilder {
    plans: Vec<dynamic_asyncapi_client::Plan>,
}
#[wasm_bindgen]
impl HostSessionBuilder {
    #[wasm_bindgen(constructor)]
    pub fn new() -> Self {
        Self::default()
    }
    pub fn add(&mut self, plan: &PlanHandle) -> Result<(), JsValue> {
        if self.plans.len() >= 16 {
            return Err(error(RuntimeError::new(
                RuntimeCode::InvalidConfiguration,
                "a session supports at most sixteen plans",
            )));
        }
        self.plans.push(plan.0.clone());
        Ok(())
    }
    pub fn open(
        &self,
        options: &str,
        cancel: &CancellationHandle,
    ) -> Result<js_sys::Promise, JsValue> {
        let options: SessionOptions = serde_json::from_str(options).map_err(|_| {
            error(RuntimeError::new(
                RuntimeCode::InvalidConfiguration,
                "host session options have an invalid field or type",
            ))
        })?;
        let plans = self.plans.clone();
        let cancel = cancel.0.clone();
        Ok(future_to_promise(async move {
            Session::open(&plans, options, Some(cancel))
                .await
                .map(|session| JsValue::from(HostSessionHandle(Some(session))))
                .map_err(error)
        }))
    }
}
#[wasm_bindgen]
pub struct HostSessionHandle(Option<Session>);
#[wasm_bindgen]
impl HostSessionHandle {
    pub fn sender(&self) -> Result<HostSenderHandle, JsValue> {
        Ok(HostSenderHandle(
            self.0.as_ref().ok_or_else(closed)?.sender(),
        ))
    }
    pub fn next(&self, cancel: &CancellationHandle) -> Result<js_sys::Promise, JsValue> {
        // Clone only the waiter state before returning the Promise. No Wasm
        // handle borrow is held across await, so disposal remains available.
        let next = self
            .0
            .as_ref()
            .ok_or_else(closed)?
            .next(Some(cancel.0.clone()));
        Ok(future_to_promise(async move {
            next.await
                .map(|value| {
                    value
                        .map(|value| JsValue::from(IncomingHandle(Some(value))))
                        .unwrap_or(JsValue::UNDEFINED)
                })
                .map_err(error)
        }))
    }
    pub fn close(&mut self, cancel: &CancellationHandle) -> Result<js_sys::Promise, JsValue> {
        let session = self.0.take().ok_or_else(closed)?;
        let cancel = cancel.0.clone();
        Ok(future_to_promise(async move {
            session
                .close(Some(cancel))
                .await
                .map(|value| JsValue::from_str(&encoded(&value)))
                .map_err(error)
        }))
    }
    pub fn usage_json(&self) -> Result<String, JsValue> {
        Ok(encoded(&self.0.as_ref().ok_or_else(closed)?.usage()))
    }
}
#[wasm_bindgen]
pub struct HostSenderHandle(Sender);
#[wasm_bindgen]
impl HostSenderHandle {
    pub fn send(&self, operation: usize, payload: &js_sys::Uint8Array) -> Result<String, JsValue> {
        self.0
            .send_js(operation, payload)
            .map(|receipt| encoded(&receipt))
            .map_err(error)
    }
}
#[wasm_bindgen]
pub struct IncomingHandle(Option<Incoming>);
#[wasm_bindgen]
impl IncomingHandle {
    pub fn metadata_json(&self) -> String {
        match self.0.as_ref() {
            Some(Incoming::Message { operation, payload }) => encoded(
                &serde_json::json!({"kind":"message","operation":operation,"payloadBytes":payload.len()}),
            ),
            Some(Incoming::Rejected {
                reason,
                payload_bytes,
            }) => encoded(
                &serde_json::json!({"kind":"rejected","reason":reason,"payloadBytes":payload_bytes}),
            ),
            None => "null".into(),
        }
    }
    pub fn take_payload(&mut self) -> Option<Vec<u8>> {
        match self.0.take() {
            Some(Incoming::Message { payload, .. }) => Some(payload),
            _ => None,
        }
    }
}
