//! Rust-owned WebSocket sessions over the browser/Worker host API.
//! A send receipt means host-buffer acceptance, never socket flush or delivery.
#![forbid(unsafe_code)]
mod wait;
use dynamic_asyncapi_client::{Codec, Payload, Plan, WebSocketFrame};
use dynamic_asyncapi_session::{Budget, ConnectionPlan, Lease, Route, SessionPlan, Usage};
pub use dynamic_asyncapi_session::{Limits, RuntimeCode, RuntimeError};
use serde::{Deserialize, Serialize};
use std::{
    cell::{Cell, RefCell},
    collections::VecDeque,
    future::{Future, poll_fn},
    rc::{Rc, Weak},
    task::Poll,
};
pub use wait::Cancellation;
use wait::{Signal, Timer};
use wasm_bindgen::{JsCast, prelude::*};
use web_sys::{BinaryType, CloseEvent, Event, MessageEvent, WebSocket};

#[derive(Clone, Debug, Deserialize)]
#[serde(default, deny_unknown_fields, rename_all = "camelCase")]
pub struct SessionOptions {
    pub limits: Limits,
    pub connect_timeout_ms: u32,
    pub close_timeout_ms: u32,
}
impl Default for SessionOptions {
    fn default() -> Self {
        Self {
            limits: Limits::default(),
            connect_timeout_ms: 5000,
            close_timeout_ms: 5000,
        }
    }
}
impl SessionOptions {
    fn validate(&self) -> Result<(), RuntimeError> {
        self.limits.validate()?;
        if self.connect_timeout_ms == 0
            || self.connect_timeout_ms > 300_000
            || self.close_timeout_ms == 0
            || self.close_timeout_ms > 300_000
        {
            return Err(RuntimeError::new(
                RuntimeCode::InvalidConfiguration,
                "host session deadlines must be between 1 and 300000 milliseconds",
            ));
        }
        Ok(())
    }
}
#[derive(Clone, Copy, Debug, Serialize)]
#[serde(tag = "kind", rename_all = "camelCase")]
pub enum Receipt {
    WebSocketHostAccepted,
}
#[derive(Clone, Copy, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CloseReceipt {
    pub code: u16,
    pub was_clean: bool,
}
#[derive(Clone, Debug)]
pub enum SessionState {
    Connecting,
    Open,
    Closing,
    Closed(CloseReceipt),
    Failed(RuntimeError),
}
#[derive(Debug)]
pub enum Incoming {
    InvalidPayload {
        diagnostic: dynamic_asyncapi_client::Diagnostic,
        payload_bytes: usize,
    },
    Message {
        operation: usize,
        payload: Payload,
    },
    Rejected {
        reason: &'static str,
        payload_bytes: usize,
    },
}
struct Queued {
    incoming: Incoming,
    _lease: Lease,
}
struct Shared {
    plan: SessionPlan,
    options: SessionOptions,
    budget: Budget,
    state: RefCell<SessionState>,
    queue: RefCell<VecDeque<Queued>>,
    signal: Signal,
    receiving: Cell<bool>,
}
impl Shared {
    fn fail(&self, error: RuntimeError) {
        if !matches!(
            *self.state.borrow(),
            SessionState::Closed(_) | SessionState::Failed(_)
        ) {
            self.state.replace(SessionState::Failed(error));
        }
        self.signal.wake();
    }
    fn active(&self) -> Result<(), RuntimeError> {
        match &*self.state.borrow() {
            SessionState::Open => Ok(()),
            SessionState::Failed(error) => {
                let mut error = error.clone();
                error.delivery_unknown = false;
                Err(error)
            }
            _ => Err(RuntimeError::new(
                RuntimeCode::Closed,
                "host session is not open",
            )),
        }
    }
    fn enqueue(&self, event: MessageEvent) -> Result<(), RuntimeError> {
        if matches!(
            *self.state.borrow(),
            SessionState::Closed(_) | SessionState::Failed(_)
        ) {
            return Ok(());
        }
        let data = event.data();
        let binary = data.is_instance_of::<js_sys::ArrayBuffer>();
        if !binary && !data.is_string() {
            return Err(RuntimeError::new(
                RuntimeCode::Protocol,
                "host WebSocket delivered an unexpected data type",
            ));
        }
        let (incoming, lease) = match self.plan.websocket_route(binary) {
            Route::Operation(operation) => {
                let size = if binary {
                    js_sys::Uint8Array::new(&data).length() as usize
                } else {
                    js_string_utf8_length(&data, self.options.limits.max_message_bytes as u32)
                };
                let lease = self.budget.reserve(size)?;
                let bytes = if binary {
                    js_sys::Uint8Array::new(&data).to_vec()
                } else {
                    data.as_string()
                        .ok_or_else(|| {
                            RuntimeError::new(RuntimeCode::Protocol, "host text conversion failed")
                        })?
                        .into_bytes()
                };
                let incoming = match self.plan.plans()[operation].decode_payload(bytes) {
                    Ok(payload) => Incoming::Message { operation, payload },
                    Err(diagnostic) => Incoming::InvalidPayload {
                        diagnostic,
                        payload_bytes: size,
                    },
                };
                (incoming, lease)
            }
            Route::Rejected(reason) => {
                // Text byte length is counted by the host without retaining or
                // copying the rejected body into Wasm. Only bounded metadata stays.
                let size = if binary {
                    js_sys::Uint8Array::new(&data).length() as usize
                } else {
                    js_string_utf8_length(&data, self.options.limits.max_message_bytes as u32)
                };
                if size > self.options.limits.max_message_bytes {
                    return Err(RuntimeError::new(
                        RuntimeCode::Backpressure,
                        "host message exceeds the configured byte limit",
                    ));
                }
                (
                    Incoming::Rejected {
                        reason,
                        payload_bytes: size,
                    },
                    self.budget.reserve(0)?,
                )
            }
        };
        self.queue.borrow_mut().push_back(Queued {
            incoming,
            _lease: lease,
        });
        self.signal.wake();
        Ok(())
    }
}
// Host-side UTF-16 length traversal avoids one Wasm crossing per character and
// never allocates a payload-sized Rust string for a rejected text frame.
#[wasm_bindgen(
    inline_js = "export function asyncapiUtf8Length(value, limit) { let n=0; for(let i=0;i<value.length;i++) { const u=value.charCodeAt(i); if(u<128)n++; else if(u<2048)n+=2; else if(u>=0xd800 && u<=0xdbff && i+1<value.length && value.charCodeAt(i+1)>=0xdc00 && value.charCodeAt(i+1)<=0xdfff) { n+=4;i++; } else n+=3; if(n>limit)return limit+1; } return n; } export function asyncapiWellFormed(value) { for(let i=0;i<value.length;i++) { const n=value.charCodeAt(i); if(n>=0xd800 && n<=0xdbff) { const next=value.charCodeAt(++i); if(!(next>=0xdc00 && next<=0xdfff)) return false; } else if(n>=0xdc00 && n<=0xdfff) return false; } return true; }"
)]
extern "C" {
    #[wasm_bindgen(js_name = asyncapiUtf8Length)]
    fn js_string_utf8_length(value: &JsValue, limit: u32) -> usize;
    #[wasm_bindgen(js_name = asyncapiWellFormed)]
    fn js_string_well_formed(value: &JsValue) -> bool;
}
struct Driver {
    socket: WebSocket,
    shared: Rc<Shared>,
    disposed: Cell<bool>,
    _open: Closure<dyn FnMut(Event)>,
    _message: Closure<dyn FnMut(MessageEvent)>,
    _error: Closure<dyn FnMut(Event)>,
    _close: Closure<dyn FnMut(CloseEvent)>,
}
impl Driver {
    fn dispose(&self) {
        if self.disposed.replace(true) {
            return;
        }
        self.socket.set_onopen(None);
        self.socket.set_onmessage(None);
        self.socket.set_onerror(None);
        self.socket.set_onclose(None);
        let _ = self.socket.close();
        self.shared.fail(RuntimeError::new(
            RuntimeCode::Closed,
            "host session owner was disposed",
        ));
        self.shared.queue.borrow_mut().clear();
    }
}
impl Drop for Driver {
    fn drop(&mut self) {
        self.dispose();
    }
}
/// Cloneable weak capability; retaining it never retains a socket or session.
#[derive(Clone)]
pub struct Sender(Weak<Driver>);
impl Sender {
    fn driver(&self, operation: usize, bytes: usize) -> Result<(Rc<Driver>, Lease), RuntimeError> {
        let driver = self
            .0
            .upgrade()
            .ok_or_else(|| RuntimeError::new(RuntimeCode::Closed, "host session owner ended"))?;
        if driver.disposed.get() {
            return Err(RuntimeError::new(
                RuntimeCode::Closed,
                "host session was disposed",
            ));
        }
        driver.shared.active()?;
        driver.shared.plan.send_plan(operation)?;
        if driver.socket.ready_state() != WebSocket::OPEN {
            return Err(RuntimeError::new(
                RuntimeCode::Closed,
                "host socket is not open",
            ));
        }
        if bytes
            > driver
                .shared
                .options
                .limits
                .max_buffered_bytes
                .saturating_sub(driver.socket.buffered_amount() as usize)
        {
            return Err(RuntimeError::new(
                RuntimeCode::Backpressure,
                "host send buffer capacity exhausted",
            ));
        }
        let lease = driver.shared.budget.reserve(bytes)?;
        Ok((driver, lease))
    }
    fn write(driver: &Driver, operation: usize, payload: &[u8]) -> Result<Receipt, RuntimeError> {
        let result = if driver.shared.plan.plans()[operation].websocket_frame()
            == Some(WebSocketFrame::Text)
        {
            driver
                .socket
                .send_with_str(std::str::from_utf8(payload).map_err(|_| {
                    RuntimeError::new(RuntimeCode::InvalidPayload, "text frame requires UTF-8")
                })?)
        } else {
            driver.socket.send_with_u8_array(payload)
        };
        result.map_err(|_| {
            RuntimeError::new(RuntimeCode::Connection, "host rejected WebSocket send").uncertain()
        })?;
        Ok(Receipt::WebSocketHostAccepted)
    }
    pub fn send(&self, operation: usize, payload: &[u8]) -> Result<Receipt, RuntimeError> {
        let (driver, _lease) = self.driver(operation, payload.len())?;
        driver.shared.plan.plans()[operation]
            .prepare_bytes(payload)
            .map_err(RuntimeError::payload)?;
        Self::write(&driver, operation, payload)
    }
    pub fn send_payload(
        &self,
        operation: usize,
        payload: &Payload,
    ) -> Result<Receipt, RuntimeError> {
        let (driver, _lease) = self.driver(operation, payload.len())?;
        driver.shared.plan.plans()[operation]
            .prepare_payload(payload)
            .map_err(RuntimeError::payload)?;
        Self::write(&driver, operation, payload.as_bytes())
    }
    pub fn send_text(&self, operation: usize, payload: &str) -> Result<Receipt, RuntimeError> {
        self.send_payload(operation, &Payload::text(payload))
    }
    pub fn send_json(
        &self,
        operation: usize,
        payload: &dynamic_asyncapi_client::Json,
    ) -> Result<Receipt, RuntimeError> {
        self.send_payload(operation, &Payload::from_json(payload.clone()))
    }
    /// The binary identity codec uses the original JS view. Other codecs admit
    /// bytes in Rust before the host sees a send. Quota precedes the body copy.
    pub fn send_js(
        &self,
        operation: usize,
        payload: &js_sys::Uint8Array,
    ) -> Result<Receipt, RuntimeError> {
        let (driver, _lease) = self.driver(operation, payload.length() as usize)?;
        let plan = &driver.shared.plan.plans()[operation];
        if plan.describe().codec == Codec::Binary {
            driver.socket.send_with_js_u8_array(payload).map_err(|_| {
                RuntimeError::new(RuntimeCode::Connection, "host rejected WebSocket send")
                    .uncertain()
            })?;
            Ok(Receipt::WebSocketHostAccepted)
        } else {
            let bytes = payload.to_vec();
            plan.prepare_bytes(&bytes).map_err(RuntimeError::payload)?;
            Self::write(&driver, operation, &bytes)
        }
    }
    pub fn send_js_text(
        &self,
        operation: usize,
        payload: &JsValue,
    ) -> Result<Receipt, RuntimeError> {
        if !payload.is_string() {
            return Err(RuntimeError::new(
                RuntimeCode::InvalidPayload,
                "text input is not well-formed Unicode",
            ));
        }
        let size = js_string_utf8_length(payload, Limits::default().max_message_bytes as u32);
        let (driver, _lease) = self.driver(operation, size)?;
        // Oversized input stops at bounded UTF-8 counting, before a complete
        // Unicode traversal or any Rust body allocation.
        if !js_string_well_formed(payload) {
            return Err(RuntimeError::new(
                RuntimeCode::InvalidPayload,
                "text input is not well-formed Unicode",
            ));
        }
        let text = payload.as_string().ok_or_else(|| {
            RuntimeError::new(RuntimeCode::InvalidPayload, "text conversion failed")
        })?;
        let payload = Payload::text(text);
        driver.shared.plan.plans()[operation]
            .prepare_payload(&payload)
            .map_err(RuntimeError::payload)?;
        Self::write(&driver, operation, payload.as_bytes())
    }
}
/// Owns callback registrations and a host socket. Drop removes handlers and
/// initiates close. Await `close` to observe a clean close event.
pub struct Session(Rc<Driver>);
impl Session {
    pub async fn open(
        plans: &[Plan],
        options: SessionOptions,
        cancel: Option<Cancellation>,
    ) -> Result<Self, RuntimeError> {
        options.validate()?;
        if cancel.as_ref().is_some_and(Cancellation::is_cancelled) {
            return Err(cancelled());
        }
        let plan = SessionPlan::new(plans)?;
        let ConnectionPlan::WebSocket(endpoint) = plan.connection() else {
            return Err(RuntimeError::new(
                RuntimeCode::Unsupported,
                "this host driver supports only WebSocket plans",
            ));
        };
        let socket = WebSocket::new(endpoint).map_err(|_| {
            RuntimeError::new(
                RuntimeCode::Connection,
                "host could not construct the WebSocket",
            )
        })?;
        socket.set_binary_type(BinaryType::Arraybuffer);
        let shared = Rc::new(Shared {
            plan,
            budget: Budget::new(options.limits)?,
            options,
            state: RefCell::new(SessionState::Connecting),
            queue: RefCell::new(VecDeque::new()),
            signal: Signal::default(),
            receiving: Cell::new(false),
        });
        let weak = Rc::downgrade(&shared);
        let open = Closure::new(move |_: Event| {
            if let Some(shared) = weak.upgrade() {
                if matches!(*shared.state.borrow(), SessionState::Connecting) {
                    shared.state.replace(SessionState::Open);
                }
                shared.signal.wake();
            }
        });
        let weak = Rc::downgrade(&shared);
        let failing_socket = socket.clone();
        let message = Closure::new(move |event: MessageEvent| {
            if let Some(shared) = weak.upgrade()
                && let Err(error) = shared.enqueue(event)
            {
                shared.fail(error);
                let _ = failing_socket.close();
            }
        });
        let weak = Rc::downgrade(&shared);
        let failing_socket = socket.clone();
        let error = Closure::new(move |_: Event| {
            if let Some(shared) = weak.upgrade() {
                shared.fail(RuntimeError::new(
                    RuntimeCode::Connection,
                    "host WebSocket reported an error",
                ));
                let _ = failing_socket.close();
            }
        });
        let weak = Rc::downgrade(&shared);
        let close = Closure::new(move |event: CloseEvent| {
            if let Some(shared) = weak.upgrade() {
                if event.was_clean() && !matches!(*shared.state.borrow(), SessionState::Failed(_)) {
                    shared.state.replace(SessionState::Closed(CloseReceipt {
                        code: event.code(),
                        was_clean: true,
                    }));
                    shared.signal.wake();
                } else {
                    shared.fail(RuntimeError::new(
                        RuntimeCode::Connection,
                        "host WebSocket closed without a clean handshake",
                    ));
                }
            }
        });
        socket.set_onopen(Some(open.as_ref().unchecked_ref()));
        socket.set_onmessage(Some(message.as_ref().unchecked_ref()));
        socket.set_onerror(Some(error.as_ref().unchecked_ref()));
        socket.set_onclose(Some(close.as_ref().unchecked_ref()));
        let session = Self(Rc::new(Driver {
            socket,
            shared,
            disposed: Cell::new(false),
            _open: open,
            _message: message,
            _error: error,
            _close: close,
        }));
        wait_state(
            session.0.shared.clone(),
            Some(session.0.shared.options.connect_timeout_ms),
            cancel,
            |shared| match &*shared.state.borrow() {
                SessionState::Connecting => Poll::Pending,
                SessionState::Open => Poll::Ready(Ok(())),
                SessionState::Failed(error) => Poll::Ready(Err(error.clone())),
                _ => Poll::Ready(Err(RuntimeError::new(
                    RuntimeCode::Closed,
                    "host socket closed before readiness",
                ))),
            },
        )
        .await?;
        Ok(session)
    }
    pub fn sender(&self) -> Sender {
        Sender(Rc::downgrade(&self.0))
    }
    pub fn state(&self) -> SessionState {
        self.0.shared.state.borrow().clone()
    }
    pub fn usage(&self) -> Usage {
        self.0.shared.budget.usage()
    }
    pub fn dispose(&self) {
        self.0.dispose();
    }
    /// The returned future owns no socket. Cancellation or dropping the future
    /// releases its waiter; dropping the session wakes it with terminal evidence.
    pub fn next(
        &self,
        cancel: Option<Cancellation>,
    ) -> impl Future<Output = Result<Option<Incoming>, RuntimeError>> + 'static {
        let shared = self.0.shared.clone();
        async move {
            if shared.receiving.replace(true) {
                return Err(RuntimeError::new(
                    RuntimeCode::InvalidConfiguration,
                    "only one receive wait may be pending per session",
                ));
            }
            struct Receiving(Rc<Shared>);
            impl Drop for Receiving {
                fn drop(&mut self) {
                    self.0.receiving.set(false);
                }
            }
            let _guard = Receiving(shared.clone());
            wait_state(shared, None, cancel, |shared| {
                if let Some(Queued { incoming, _lease }) = shared.queue.borrow_mut().pop_front() {
                    drop(_lease);
                    return Poll::Ready(Ok(Some(incoming)));
                }
                match &*shared.state.borrow() {
                    SessionState::Closed(_) => Poll::Ready(Ok(None)),
                    SessionState::Failed(error) => Poll::Ready(Err(error.clone())),
                    _ => Poll::Pending,
                }
            })
            .await
        }
    }
    pub async fn close(self, cancel: Option<Cancellation>) -> Result<CloseReceipt, RuntimeError> {
        if cancel.as_ref().is_some_and(Cancellation::is_cancelled) {
            return Err(cancelled());
        }
        if matches!(self.state(), SessionState::Open) {
            self.0.shared.state.replace(SessionState::Closing);
            self.0.socket.close_with_code(1000).map_err(|_| {
                RuntimeError::new(RuntimeCode::Connection, "host rejected WebSocket close")
            })?;
        }
        wait_state(
            self.0.shared.clone(),
            Some(self.0.shared.options.close_timeout_ms),
            cancel,
            |shared| match &*shared.state.borrow() {
                SessionState::Closed(receipt) => Poll::Ready(Ok(*receipt)),
                SessionState::Failed(error) => Poll::Ready(Err(error.clone())),
                _ => Poll::Pending,
            },
        )
        .await
    }
}
fn cancelled() -> RuntimeError {
    RuntimeError::new(RuntimeCode::Cancelled, "host operation was cancelled")
}
async fn wait_state<T>(
    shared: Rc<Shared>,
    deadline: Option<u32>,
    cancel: Option<Cancellation>,
    mut poll: impl FnMut(&Shared) -> Poll<Result<T, RuntimeError>>,
) -> Result<T, RuntimeError> {
    let state_waiter = shared.signal.register();
    let cancel_waiter = cancel.as_ref().map(Cancellation::register);
    let timer = deadline.map(Timer::new).transpose()?;
    let timer_waiter = timer.as_ref().map(Timer::register);
    poll_fn(|cx| {
        state_waiter.poll(cx);
        if let Some(waiter) = &cancel_waiter {
            waiter.poll(cx);
        }
        if let Some(waiter) = &timer_waiter {
            waiter.poll(cx);
        }
        if cancel.as_ref().is_some_and(Cancellation::is_cancelled) {
            return Poll::Ready(Err(cancelled()));
        }
        if timer.as_ref().is_some_and(Timer::elapsed) {
            return Poll::Ready(Err(RuntimeError::new(
                RuntimeCode::Deadline,
                "host session deadline expired",
            )));
        }
        poll(&shared)
    })
    .await
}
