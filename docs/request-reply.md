# Native request/reply

`CompiledOperation::prepare_exchange` prepares an AsyncAPI 3.0/3.1 operation's
**declared reply**. `dynamic_asyncapi_native::request_reply` then opens one owned
WebSocket connection, sends one JSON request, waits for the first matching JSON
reply, and closes the connection. No OpenBindings dependency is involved.

```rust,ignore
let plan = document.operation_id("ask")?.compile()?.prepare_exchange(
    &ExchangeOptions::first(
        PlanOptions::application(),
        PlanOptions::application(),
        5_000,
    ),
)?;
let response = request_reply(&plan, &request, SessionOptions::default()).await?;
let reply = response.reply.payload.as_json().unwrap();
```

The runnable `rust/native/examples/request_reply.rs` accepts a document filename,
operation key, and JSON request. The local peer tests contain a complete document
with different request/reply correlation locations and exact large-number data:

```sh
cargo test --manifest-path rust/Cargo.toml -p dynamic-asyncapi-native --test request_reply --locked
```

## Contract

- The request is an application `send` operation. Its declared reply receives on
  the same WebSocket endpoint. Both sides retain the authored operation identity;
  the engine does not manufacture a second operation or alter the document.
- Each side selects a single, schema-free `application/json` message with a
  declared payload correlation location. Reply channel references and message
  membership are checked. Correlation metadata inherited through traits retains
  its defining location.
- The caller supplies a nonempty string correlation ID, at most 1024 UTF-8 bytes.
  IDs compare decoded string contents exactly; numbers, null, missing values and
  different strings do not match. IDs are neither generated nor inserted. Use
  fresh IDs appropriate to the service; correlation alone does not authenticate
  a response. The request and response may use different payload locations.
- One call owns one connection. The receiver is attached before sending.
  Concurrent calls are isolated by connection, including when IDs coincide.
  Multiplexing/reusing a connection and server-side reply handling are future work.
- The first matching reply completes the call. This is explicit library policy;
  AsyncAPI does not prescribe a universal reply count or timeout. The configured
  1–300000 ms deadline covers readiness, send, receive and graceful close.
- Existing session byte/message limits apply. At most 64 well-formed nonmatching
  replies are discarded; the next one terminates with `Backpressure`. Malformed
  payloads and unexpected frames terminate with structured errors.
- `ExchangeError` retains any completed send receipt. A timeout after an uncertain
  send reports `delivery_unknown`; dropping the future cannot retract a request.
  There are no retries. A valid reply is retained even if graceful close fails:
  `ExchangeResponse.close` reports that failure separately.
- Dropping the future drops its owned session and aborts the driver. Explicit
  completion attempts bounded graceful shutdown. Reply values own their source.

## Boundaries

This slice covers native Rust WebSocket request/reply in 3.0/3.1, with text or
explicit binary JSON framing supplied by the existing plan options. It does not
implement AsyncAPI 2.x configured operation pairs, MQTT request/reply, dynamic
reply addresses, header correlation, schema evaluation, multiple-response
completion, connection multiplexing, or a TypeScript request/reply method.
Planning and matching live in the portable Rust core for later host composition.
Worker transport debugging and qualification remain separate, unresolved work.

The native reply structure follows the
[AsyncAPI 3.0 Operation Reply Object](https://www.asyncapi.com/docs/reference/specification/v3.0.0#operationReplyObject).
The string-ID, first-reply, connection ownership and resource-limit choices above
are this library's initial execution profile, not additional specification rules.

This work is a finite follow-up to the paused development loop. Its tests are
implementation regression evidence, not independent full-product qualification.
