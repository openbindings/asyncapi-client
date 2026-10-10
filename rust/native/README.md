# Native AsyncAPI sessions

This unpublished crate executes prepared plans from `dynamic-asyncapi-client`. It owns Tokio and the protocol libraries; the semantic engine and its Wasm composition remain independent of native dependencies.

The first execution slice supports schema-free binary, JSON and UTF-8 messages over MQTT 3.1.1 QoS 0/1/2 and WebSocket RFC 6455, over TCP or explicitly configured TLS. MQTT 5, automatic recovery, wildcard receive dispatch, payload schemas, authentication-scheme planning and replies remain required expansion work. Explicit session credentials are currently available for MQTT; they are never copied into a document or plan.

```rust,ignore
let send = document.operation_id("emit")?.compile()?.prepare(&PlanOptions::application())?;
let receive = document.operation_id("listen")?.compile()?.prepare(&PlanOptions::application())?;
let mut session = Session::open(&[send, receive], SessionOptions::default()).await?;
let sender = session.sender();
let receipt = sender.send(0, Vec::from(b"payload".as_slice())).await?;
let observation = session.next().await?;
let close = session.close().await?;
```

For `mqtts` or `wss`, supply a reusable trust configuration:

```rust,ignore
let tls = TlsConfig::system_roots()?; // blocking setup; includes SSL_CERT_FILE/DIR overrides
// Or use only a supplied CA bundle: TlsConfig::from_ca_pem(&ca_bytes)?
// Optional mutual TLS: tls.with_client_identity(&certificate_chain, &private_key)?
let options = SessionOptions { tls: Some(tls.clone()), ..Default::default() };
let session = Session::open(&plans, options).await?;
```

Secure endpoints without a trust configuration, and plaintext endpoints supplied
with one, refuse before connecting. Both drivers use rustls with an explicitly
selected ring provider, TLS 1.2/1.3, certificate validity/chain verification and
the endpoint hostname or IP identity. There is no verification-bypass option or
silent TLS downgrade. Custom CA bundles do not merge ambient roots; partial
system-root loads refuse. Root loading and parsing happen when constructing the
configuration, which is cheaply cloned across sessions. Handshakes remain within
the connection deadline. Client identities produce a new configuration, leaving
the original intact; debug output and runtime errors omit credential material.
PEM admission permits 4 MiB of certificates, 512 CA certificates, 16 client-chain
certificates and one unencrypted private key up to 64 KiB. Revocation policy,
broader TLS profiles and independent security qualification remain open.

The plan slice establishes explicit connection sharing. Plans must agree on connection settings and role; duplicate native operations and ambiguous receive routes refuse before I/O. Readiness requires a successful connection handshake and all MQTT subscription acknowledgments. Once open, `session.mqtt_subscriptions()` exposes each operation’s requested and granted QoS. A subscription QoS is a maximum: a broker may grant a lower value, and delivery metadata reports the actual publication QoS. Publications above a negotiated grant end the session with a protocol error. A separate receive observation does not imply request/reply correlation.

A session owns one driver task. MQTT keepalive and WebSocket control frames progress while the application is idle. WebSocket reads continue while a write is pending. Sender handles can be retained or used concurrently with a receive wait, but the driver serializes application sends. Close rejects new sends, performs protocol shutdown, joins the task and releases its connection. Dropping an owner, including canceling its close future, aborts that task and initiates connection release.

MQTT QoS 0 receipts mean PUBLISH was flushed locally; no broker acknowledgment exists. QoS 1 receipts identify the matching PUBACK. QoS 2 receipts require the matching PUBCOMP after PUBREC/PUBREL; PUBREC alone is not completion. WebSocket receipts mean the frame was flushed to the socket. These receipts do not establish application processing by a peer. MQTT receive delivery is automatically acknowledged by this initial driver before application processing; manual settlement is not implemented yet. Unexpected WebSocket text is a rejected observation, never a binary operation result.

Failed sends distinguish pre-admission rejection from potentially transmitted data with `delivery_unknown`. Canceling a send future does not retract bytes already handed to a driver. Queued canceled sends are skipped; a later acknowledgement cannot complete a different pending command. Deadlines after driver submission end the session, with no hidden reconnect or retransmission loop.

Default and maximum admission limits in this slice are 16 attached plans, 64 queued/pending message slots and 1 MiB of their payload bytes per session, with a 1 MiB message limit. Options may lower the queue/byte limits. Those quotas are shared by outbound pending messages and queued receive observations, so simultaneous traffic consumes their combined capacity. Driver buffers and source snapshots have separate bounds. Queue overflow ends the session explicitly; queued observations can be drained before its terminal error, which is stored separately from the data queue. Control frames retain their protocol allowance even when the application message limit is smaller.

The development checks include actual loopback peers, wrong-route and serialized-I/O controls, readiness, cancellation, deadlines, idle control traffic, queue/byte limits and repeated closes. These are development evidence; the full independent protocol, ownership, host and performance gates remain open.

MQTT 3.1.1 uses a [pinned repaired transport](../vendor/rumqttc-v4-next/LOCAL-CHANGES.md). Source provenance, the original license and the exact local patch are retained. It suppresses duplicate QoS 2 publication delivery, responds to repeated PUBREC/PUBREL, permits completed packet identifier reuse and validates acknowledgment type/phase/identity. Inbound QoS 2 tracking uses fixed packet-identifier bitsets; it does not retain already-delivered payloads. Live packet tests include 1,000 completed sends with one reusable queue slot. Recovery, manual settlement and independent conformance/performance qualification remain open; these tests do not imply an exactly-once application transaction.

JSON and UTF-8 text use the same prepared plans as binary messages. Clone a sender
and call `send_text`, `send_json`, or `send_payload`; `send` accepts encoded bytes
and validates the selected codec. Received `Payload` retains wire bytes and offers
`as_text`/`as_json` without reparsing. `Incoming::InvalidPayload` preserves a codec
diagnostic without terminating the stream. WebSocket frame defaults/overrides are
explicit in the plan; MQTT publishes the encoded bytes. Schema declarations
continue to require evaluator support. Queue byte limits describe wire payloads,
not the complete heap of the decoded exact JSON graph.

Declared `userPassword` requires `SessionOptions.credentials` before connecting;
`X509` requires a `TlsConfig` returned by `with_client_identity`. Server and
operation requirements both apply, including across all attached plans. Credentials
are connection-wide runtime material, separate from serializable plan options.
Configured material does not guarantee peer authorization. Other declared schemes
remain unsupported. The TLS peer suite includes document-declared requirements in
AsyncAPI 2.6, 3.0 and 3.1, missing-material refusal without TCP activity, and actual
broker rejection of a wrong password.
