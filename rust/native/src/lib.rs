//! Explicitly owned native protocol sessions for immutable AsyncAPI plans.
//! This initial execution slice supports MQTT 3.1.1 QoS 1 and binary WebSocket,
//! both over TCP. TLS, recovery and other protocol profiles remain open work.
#![forbid(unsafe_code)]
mod mqtt;
mod websocket;
mod wire;

pub use bytes::Bytes;
use dynamic_asyncapi_client::{Action, Plan};
use serde::Serialize;
use std::{sync::Arc, time::Duration};
use tokio::{
    sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot, watch},
    task::JoinHandle,
    time::{Instant, timeout},
};
use wire::Connection;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[non_exhaustive]
pub enum RuntimeCode {
    InvalidConfiguration,
    Unsupported,
    Closed,
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
}
impl RuntimeError {
    fn new(code: RuntimeCode, detail: &'static str) -> Self {
        Self {
            code,
            detail,
            delivery_unknown: false,
        }
    }
    fn uncertain(mut self) -> Self {
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

/// Per-session credentials. Debug output never prints either value.
#[derive(Clone)]
pub struct Credentials {
    username: String,
    password: String,
}
impl Credentials {
    pub fn new(username: impl Into<String>, password: impl Into<String>) -> Self {
        Self {
            username: username.into(),
            password: password.into(),
        }
    }
}
impl std::fmt::Debug for Credentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Credentials([redacted])")
    }
}
#[derive(Clone, Debug)]
pub struct SessionOptions {
    pub credentials: Option<Credentials>,
    pub connect_timeout: Duration,
    pub operation_timeout: Duration,
    pub max_messages: usize,
    pub max_buffered_bytes: usize,
    pub max_message_bytes: usize,
}
impl Default for SessionOptions {
    fn default() -> Self {
        Self {
            credentials: None,
            connect_timeout: Duration::from_secs(5),
            operation_timeout: Duration::from_secs(5),
            max_messages: 64,
            max_buffered_bytes: 1024 * 1024,
            max_message_bytes: 1024 * 1024,
        }
    }
}
impl SessionOptions {
    fn validate(&self) -> Result<(), RuntimeError> {
        if self.max_messages == 0
            || self.max_messages > 64
            || self.max_buffered_bytes == 0
            || self.max_buffered_bytes > 1024 * 1024
            || self.max_message_bytes == 0
            || self.max_message_bytes > self.max_buffered_bytes
            || self.connect_timeout.is_zero()
            || self.operation_timeout.is_zero()
            || self.connect_timeout > Duration::from_secs(300)
            || self.operation_timeout > Duration::from_secs(300)
        {
            return Err(RuntimeError::new(
                RuntimeCode::InvalidConfiguration,
                "session limits or deadlines are outside the initial profile",
            ));
        }
        if let Some(c) = &self.credentials
            && (c.username.len() > 65535 || c.password.len() > 65535 || c.username.contains('\0'))
        {
            return Err(RuntimeError::new(
                RuntimeCode::InvalidConfiguration,
                "MQTT credentials exceed protocol bounds",
            ));
        }
        Ok(())
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(
    tag = "kind",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
pub enum Receipt {
    MqttPubAck { packet_id: u16 },
    WebSocketFlushed,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum CloseReceipt {
    MqttDisconnectFlushed,
    WebSocketHandshake,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SessionState {
    Open,
    Closed(CloseReceipt),
    Failed(RuntimeError),
}

/// A binary message classified to exactly one attached receive operation.
/// `operation` is its index in the slice supplied to `Session::open`.
#[derive(Clone, Debug)]
pub struct Received {
    pub operation: usize,
    pub payload: Bytes,
    pub delivery: Delivery,
}
#[derive(Clone, Debug)]
pub enum Delivery {
    /// This initial driver auto-acknowledges MQTT delivery before application processing.
    Mqtt {
        topic: String,
        qos: u8,
        retain: bool,
        duplicate: bool,
        packet_id: u16,
    },
    WebSocket,
}
#[derive(Clone, Debug)]
pub enum Incoming {
    Message(Received),
    /// Preserves the observation without treating an unexpected frame as an
    /// operation's message. Payload content is not placed in error strings.
    Rejected {
        reason: &'static str,
        payload_bytes: usize,
    },
}
struct Lease {
    _messages: OwnedSemaphorePermit,
    _bytes: OwnedSemaphorePermit,
}
#[derive(Clone)]
struct Budget {
    messages: Arc<Semaphore>,
    bytes: Arc<Semaphore>,
    max_message: usize,
}
impl Budget {
    fn reserve(&self, bytes: usize) -> Result<Lease, RuntimeError> {
        if bytes > self.max_message {
            return Err(RuntimeError::new(
                RuntimeCode::Backpressure,
                "message exceeds the configured byte limit",
            ));
        }
        let messages = self.messages.clone().try_acquire_owned().map_err(|_| {
            RuntimeError::new(
                RuntimeCode::Backpressure,
                "session message capacity exhausted",
            )
        })?;
        let bytes = self
            .bytes
            .clone()
            .try_acquire_many_owned(bytes as u32)
            .map_err(|_| {
                RuntimeError::new(RuntimeCode::Backpressure, "session byte capacity exhausted")
            })?;
        Ok(Lease {
            _messages: messages,
            _bytes: bytes,
        })
    }
}
struct Queued {
    incoming: Incoming,
    _lease: Lease,
}
struct SendCommand {
    operation: usize,
    payload: Bytes,
    response: oneshot::Sender<Result<Receipt, RuntimeError>>,
    deadline: Instant,
    _lease: Lease,
}
impl SendCommand {
    fn complete(self, result: Result<Receipt, RuntimeError>) {
        let Self {
            response,
            payload,
            _lease,
            ..
        } = self;
        drop(payload);
        drop(_lease);
        // Publish the receipt only after capacity is reusable by its recipient.
        let _ = response.send(result);
    }
}
#[derive(Clone)]
struct Context {
    plans: Arc<[Plan]>,
    events: mpsc::Sender<Queued>,
    budget: Budget,
    options: SessionOptions,
}
impl Context {
    fn deliver(&self, incoming: Incoming) -> Result<(), RuntimeError> {
        let size = match &incoming {
            Incoming::Message(message) => message.payload.len(),
            Incoming::Rejected { .. } => 0,
        };
        let lease = self.budget.reserve(size)?;
        self.events
            .try_send(Queued {
                incoming,
                _lease: lease,
            })
            .map_err(|_| {
                RuntimeError::new(
                    RuntimeCode::Backpressure,
                    "incoming queue is full or has no owner",
                )
            })
    }
}
/// Cloneable send capability. It can be used concurrently with `Session::next`.
/// Dropping the session closes all retained sender capabilities.
#[derive(Clone)]
pub struct Sender {
    commands: mpsc::Sender<SendCommand>,
    plans: Arc<[Plan]>,
    budget: Budget,
    terminal: watch::Receiver<SessionState>,
    shutdown: watch::Receiver<bool>,
    deadline: Duration,
}
impl Sender {
    pub async fn send(
        &self,
        operation: usize,
        payload: impl Into<Bytes>,
    ) -> Result<Receipt, RuntimeError> {
        if *self.shutdown.borrow() {
            return Err(RuntimeError::new(RuntimeCode::Closed, "session is closing"));
        }
        if let Some(mut error) = state_error(&self.terminal.borrow()) {
            error.delivery_unknown = false;
            return Err(error);
        }
        let plan = self.plans.get(operation).ok_or_else(|| {
            RuntimeError::new(
                RuntimeCode::InvalidConfiguration,
                "operation index is not attached to this session",
            )
        })?;
        if plan.describe().wire_action != Action::Send {
            return Err(RuntimeError::new(
                RuntimeCode::InvalidConfiguration,
                "operation is a receive operation",
            ));
        }
        let payload = payload.into();
        let _ = plan.prepare_bytes(&payload);
        let lease = self.budget.reserve(payload.len())?;
        let (response, receipt) = oneshot::channel();
        self.commands
            .try_send(SendCommand {
                operation,
                payload,
                response,
                deadline: Instant::now() + self.deadline,
                _lease: lease,
            })
            .map_err(|_| {
                RuntimeError::new(RuntimeCode::Backpressure, "send queue is full or closed")
            })?;
        match receipt.await {
            Ok(result) => result,
            Err(_) => {
                let mut terminal = self.terminal.clone();
                while matches!(*terminal.borrow(), SessionState::Open) {
                    if terminal.changed().await.is_err() {
                        break;
                    }
                }
                Err(state_error(&terminal.borrow())
                    .unwrap_or_else(|| {
                        RuntimeError::new(RuntimeCode::Closed, "driver ended before a send receipt")
                    })
                    .uncertain())
            }
        }
    }
}
/// One explicitly shared connection, its receive queue, and its driver task.
/// Plans must agree on connection/role, and receive operations must be unambiguous.
/// Explicit `close` joins the task; Drop initiates task abortion and socket release.
pub struct Session {
    sender: Sender,
    events: mpsc::Receiver<Queued>,
    shutdown: watch::Sender<bool>,
    terminal: watch::Sender<SessionState>,
    task: Option<JoinHandle<()>>,
    timeout: Duration,
}
impl Session {
    pub async fn open(plans: &[Plan], options: SessionOptions) -> Result<Self, RuntimeError> {
        options.validate()?;
        let connection = Connection::from_plans(plans, &options)?;
        if tokio::runtime::Handle::try_current().is_err() {
            return Err(RuntimeError::new(
                RuntimeCode::InvalidConfiguration,
                "native sessions require a Tokio runtime",
            ));
        }
        let plans: Arc<[Plan]> = plans.into();
        let budget = Budget {
            messages: Arc::new(Semaphore::new(options.max_messages)),
            bytes: Arc::new(Semaphore::new(options.max_buffered_bytes)),
            max_message: options.max_message_bytes,
        };
        let (events, receiver) = mpsc::channel(options.max_messages);
        let context = Context {
            plans: plans.clone(),
            events,
            budget: budget.clone(),
            options: options.clone(),
        };
        let driver = timeout(options.connect_timeout, async {
            match connection {
                Connection::Mqtt(settings) => mqtt::connect(settings, &context)
                    .await
                    .map(|driver| Driver::Mqtt(Box::new(driver))),
                Connection::WebSocket(endpoint) => websocket::connect(&endpoint, &context)
                    .await
                    .map(|driver| Driver::WebSocket(Box::new(driver))),
            }
        })
        .await
        .map_err(|_| {
            RuntimeError::new(RuntimeCode::Deadline, "session readiness deadline expired")
        })??;
        let (commands, requests) = mpsc::channel(options.max_messages);
        let (shutdown, shutdown_rx) = watch::channel(false);
        let (terminal, state) = watch::channel(SessionState::Open);
        let guard = TerminalGuard(terminal.clone());
        let task = tokio::spawn(async move {
            let result = match driver {
                Driver::Mqtt(driver) => mqtt::run(*driver, context, requests, shutdown_rx).await,
                Driver::WebSocket(driver) => {
                    websocket::run(*driver, context, requests, shutdown_rx).await
                }
            };
            guard.0.send_replace(match result {
                Ok(receipt) => SessionState::Closed(receipt),
                Err(error) => SessionState::Failed(error),
            });
        });
        Ok(Self {
            sender: Sender {
                commands,
                plans,
                budget,
                terminal: state,
                shutdown: shutdown.subscribe(),
                deadline: options.operation_timeout,
            },
            events: receiver,
            shutdown,
            terminal,
            task: Some(task),
            timeout: options.operation_timeout,
        })
    }
    pub fn sender(&self) -> Sender {
        self.sender.clone()
    }
    pub fn state(&self) -> SessionState {
        self.terminal.borrow().clone()
    }
    pub async fn send(
        &self,
        operation: usize,
        payload: impl Into<Bytes>,
    ) -> Result<Receipt, RuntimeError> {
        self.sender.send(operation, payload).await
    }
    /// Cancellation-safe wait. Queued valid observations are drained before the
    /// terminal status; no terminal event needs space in the bounded data queue.
    pub async fn next(&mut self) -> Result<Option<Incoming>, RuntimeError> {
        if let Some(queued) = self.events.recv().await {
            return Ok(Some(queued.incoming));
        }
        let mut terminal = self.sender.terminal.clone();
        while matches!(*terminal.borrow(), SessionState::Open) {
            if terminal.changed().await.is_err() {
                break;
            }
        }
        match terminal.borrow().clone() {
            SessionState::Closed(_) => Ok(None),
            SessionState::Failed(error) => Err(error),
            SessionState::Open => Err(RuntimeError::new(
                RuntimeCode::DriverFailed,
                "driver ended without terminal evidence",
            )),
        }
    }
    pub async fn close(mut self) -> Result<CloseReceipt, RuntimeError> {
        self.shutdown.send_replace(true);
        match timeout(self.timeout, self.task.as_mut().unwrap()).await {
            Ok(Ok(())) => {}
            Ok(Err(_)) => {
                return Err(RuntimeError::new(
                    RuntimeCode::DriverFailed,
                    "driver task failed",
                ));
            }
            Err(_) => {
                self.terminal
                    .send_replace(SessionState::Failed(RuntimeError::new(
                        RuntimeCode::Deadline,
                        "session close deadline expired",
                    )));
                let task = self.task.take().unwrap();
                task.abort();
                let _ = task.await;
                return Err(RuntimeError::new(
                    RuntimeCode::Deadline,
                    "session close deadline expired",
                ));
            }
        }
        self.task.take();
        match self.state() {
            SessionState::Closed(receipt) => Ok(receipt),
            SessionState::Failed(error) => Err(error),
            SessionState::Open => Err(RuntimeError::new(
                RuntimeCode::DriverFailed,
                "driver ended without close evidence",
            )),
        }
    }
}
impl Drop for Session {
    fn drop(&mut self) {
        if let Some(task) = self.task.take() {
            if matches!(*self.terminal.borrow(), SessionState::Open) {
                self.terminal
                    .send_replace(SessionState::Failed(RuntimeError::new(
                        RuntimeCode::Closed,
                        "session owner was dropped",
                    )));
            }
            task.abort();
        }
    }
}
struct TerminalGuard(watch::Sender<SessionState>);
impl Drop for TerminalGuard {
    fn drop(&mut self) {
        if matches!(*self.0.borrow(), SessionState::Open) {
            self.0.send_replace(SessionState::Failed(RuntimeError::new(
                RuntimeCode::DriverFailed,
                "driver stopped without terminal evidence",
            )));
        }
    }
}
fn state_error(state: &SessionState) -> Option<RuntimeError> {
    match state {
        SessionState::Open => None,
        SessionState::Closed(_) => {
            Some(RuntimeError::new(RuntimeCode::Closed, "session is closed"))
        }
        SessionState::Failed(error) => Some(error.clone()),
    }
}
enum Driver {
    Mqtt(Box<mqtt::Driver>),
    WebSocket(Box<websocket::Driver>),
}
