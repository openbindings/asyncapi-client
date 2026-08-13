# Protocol-driver qualification

An AsyncAPI protocol driver is the concrete end of this authority chain:

1. AsyncAPI Core resolves the artifact, application perspective, selected server, channel address, message declarations, security requirements, and payload codec.
2. The applicable AsyncAPI protocol-binding specification governs concrete binding objects at its defined locations.
3. A protocol driver maps those resolved facts into a mature protocol-native client and preserves the resulting exchange lifecycle.

The standalone engine therefore supplies drivers with separate resolved input
and output directions. Each direction carries its own server, protocol, server
URL, channel, governing messages, statically knowable address, and
artifact-governed encoder or decoder. This is load-bearing for AsyncAPI reply
operations: the operation direction and reply direction may not have the same
channel or even the same protocol. A runtime-expression address remains absent
until a driver has the corresponding message; it is never replaced with the
other direction's address.

A driver must not repeat AsyncAPI reference, trait, security-reference,
message-selection, or payload-codec logic. It may resolve protocol timing and
per-message routing that only the concrete driver can observe. It may place a
credential only through a security alternative declared by the artifact;
unrelated context must never be volunteered to the protocol.

## Admission

A driver profile is qualified one semantic cell at a time. Every authority-derived cell is exactly one of:

- `supported`: executable evidence demonstrates the authored behavior;
- `excluded`: the driver refuses it before connection or dispatch;
- `unqualified`: implementation may exist, but the project makes no fidelity claim until sufficient evidence exists.

Corpus frequency can prioritize cells but cannot define their meaning. A supported cell needs deterministic authority fixtures in both languages and live protocol evidence wherever behavior is observable only through a broker or server. Invalid, ambiguous, unsupported-version, unsupported-codec, and unsupported-security cases must fail before observable protocol work.

## Required lifecycle evidence

A streaming driver must demonstrate ordering, delivery boundaries, bounded backpressure at the driver session, cancellation, connection-loss behavior, output preservation before later failure, and connection identity semantics. If an authored protocol identity is shared by simultaneous operations, the driver must share or multiplex the concrete connection when opening independent connections would change protocol behavior.

## Abstraction gate

Driver registration is runtime capability. It must not alter synthesis, add a protocol allowlist to `openbindings.asyncapi`, add protocol fields to OBI operation values, or change Core/invoker frames. The standalone client may expose protocol diagnostics. The OpenBindings adapter keeps those diagnostics below the abstract invocation boundary rather than projecting them onto outputs, errors, or lifecycle frames.

Current matrices are
[websocket-reply.json](../conformance/websocket-reply.json),
[mqtt-3.1.1.json](../conformance/mqtt-3.1.1.json), and
[kafka.json](../conformance/kafka.json).
