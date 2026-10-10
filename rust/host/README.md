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
qualification remains open. The current tested local profile pins workerd
1.20261010.1, compatibility date 2026-10-08, and the WebSocket constructor. Its
constructor path passed 800 raw control connections, but the later JSON codec
consumer and a matched direct-next control both failed with a host WebSocket
error. Current lifecycle/authentication checks pass without resolving that codec
shutdown failure.
The older 1.20261006.1 profile had intermittent error/clean-close sequences;
other raw connection methods still reproduce this on the newer runtime. These
observations are preserved, not treated as client success. Declared X509 is
refused before socket construction because this driver cannot configure a client
identity. Host-capable authentication, broader TLS/codecs/protocols and sustained
performance/ownership qualification remain required work.
