# Native AsyncAPI sessions

This unpublished crate executes prepared plans from `dynamic-asyncapi-client`. It owns Tokio and the protocol libraries; the semantic engine and its Wasm composition remain independent of native dependencies.

The first execution slice supports schema-free binary messages over MQTT 3.1.1 QoS 1 and WebSocket RFC 6455, over TCP. TLS, other MQTT QoS, automatic recovery, wildcard receive dispatch, payload schemas, authentication-scheme planning and replies remain required expansion work. Explicit session credentials are currently available for MQTT; they are never copied into a document or plan.

```rust,ignore
let send = document.operation_id("emit")?.compile()?.prepare(&PlanOptions::application())?;
let receive = document.operation_id("listen")?.compile()?.prepare(&PlanOptions::application())?;
let mut session = Session::open(&[send, receive], SessionOptions::default()).await?;
let sender = session.sender();
let receipt = sender.send(0, Vec::from(b"payload".as_slice())).await?;
let observation = session.next().await?;
let close = session.close().await?;
```

The plan slice establishes explicit connection sharing. Plans must agree on connection settings and role; duplicate native operations and ambiguous receive routes refuse before I/O. Readiness requires a successful connection handshake and all MQTT subscription acknowledgments. A separate receive observation does not imply request/reply correlation.

A session owns one driver task. MQTT keepalive and WebSocket control frames progress while the application is idle. WebSocket reads continue while a write is pending. Sender handles can be retained or used concurrently with a receive wait, but the driver serializes application sends. Close rejects new sends, performs protocol shutdown, joins the task and releases its connection. Dropping an owner, including canceling its close future, aborts that task and initiates connection release.

MQTT send receipts identify the matching PubAck. WebSocket receipts mean the frame was flushed to the socket. Neither receipt establishes application processing by a peer. MQTT receive delivery is automatically acknowledged by this initial driver before application processing; manual settlement is not implemented yet. Unexpected WebSocket text is a rejected observation, never a binary operation result.

Failed sends distinguish pre-admission rejection from potentially transmitted data with `delivery_unknown`. Canceling a send future does not retract bytes already handed to a driver. Queued canceled sends are skipped; a later acknowledgement cannot complete a different pending command. Deadlines after driver submission end the session, with no hidden reconnect or retransmission loop.

Default and maximum admission limits in this slice are 16 attached plans, 64 queued/pending message slots and 1 MiB of their payload bytes per session, with a 1 MiB message limit. Options may lower the queue/byte limits. Those quotas are shared by outbound pending messages and queued receive observations, so simultaneous traffic consumes their combined capacity. Driver buffers and source snapshots have separate bounds. Queue overflow ends the session explicitly; queued observations can be drained before its terminal error, which is stored separately from the data queue. Control frames retain their protocol allowance even when the application message limit is smaller.

The development checks include actual loopback peers, wrong-route and serialized-I/O controls, readiness, cancellation, deadlines, idle control traffic, queue/byte limits and repeated closes. These are development evidence; the full independent protocol, ownership, host and performance gates remain open.
