# Rust host sessions

This unpublished crate owns ordinary browser/Worker WebSocket sessions. It
composes directly inside a downstream Rust/Wasm module and backs the standalone
TypeScript API. It uses `dynamic-asyncapi-session` for admission, immutable plans,
routing, errors and message/byte quotas. Neither a JavaScript document interpreter
nor the standalone TypeScript facade is required by Rust consumers.

`Session::open(&plans, options, cancellation).await` constructs and waits for a
host WebSocket. `Session::sender()` returns a weak, cloneable send capability;
`session.next(cancellation).await` returns a binary message or rejected frame.
`session.close(cancellation).await` consumes the owner and observes clean close.
Drop removes handlers and starts closing. Dropping a pending receive future
releases its waiter without consuming a queued message. Explicit cancellation
works the same way; canceling open or close also releases the session owner.

The send receipt is `WebSocketHostAccepted`. It is weaker than a native socket
flush and does not establish delivery. The outbound host's buffered byte amount
is checked before submission. Rust's bounded incoming queue and shared quotas
are separate from browser buffering before event dispatch. There is no receive
backpressure primitive in the classic host API. Overflow ends the session and
retains its error separately from the bounded queue. Text frames retain only
bounded rejection metadata; no text body is copied into Rust.

Default limits are 64 messages and 1 MiB, configurable downwards; readiness and
close deadlines default to five seconds. Timers and event closures are owned,
removed on drop, and never forgotten. There is no background task or global
socket. One receive wait may be pending per session. Source plans may be dropped
after opening; the session owns them.

Initial development covers local schema-free binary WebSocket with GET and no
handshake headers. Browser/Worker network policy applies. Local workerd loopback
configuration does not establish production network access. Worker close
qualification is open: its pinned runtime has intermittently reported a host
error followed by a clean close, including in a direct raw-host control. This
client preserves the error rather than silently declaring success. TLS,
authentication, further codecs/protocols and sustained performance/ownership
qualification remain required work.
