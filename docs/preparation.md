# Pure preparation development slice

`Operation::compile()` produces an owning `CompiledOperation`. It resolves native operation/channel/message/server relationships and merges traits while retaining authored field origins. References inherited from an external trait resolve against that trait's source URI. Native selectors and resolved definitions remain distinct in inspection. Compilation does not convert a 2.x document into a 3.x document.

`CompiledOperation::prepare(&PlanOptions::application())` selects deployment choices and returns an owning `Plan`. Both APIs are also available through the handwritten TypeScript facade. A downstream Rust consumer can call them directly inside its own Wasm module. No preparation API acquires source files, opens connections or reads credentials.

## Current positive path

- AsyncAPI 2.6, 3.0 and 3.1, JSON or YAML; exactly one message in the selected channel/2.x message set.
- Explicit `application/octet-stream` on the message or as the document default, with no payload/header schema or correlation declaration.
- MQTT 3.1.1: application or peer action, topic/filter, QoS 0/1/2, retain for publishing, client identity, clean session and keepalive. The plan supports these settings; runtime execution is still separate work.
- WebSocket RFC 6455: application action, server base path plus channel path, GET handshake. Peer plans require an established-connection/hosting route, which this slice reports as a requirement.

An omitted 3.x operation message list means all channel messages; an empty list means no messages and cannot produce an exchange plan. Explicit message references must name channel message entries even when those entries reference shared component definitions. Empty or omitted channel server lists use the root servers; multiple available servers require a caller choice. Unknown supplied choices are errors.

Parameter substitution retains protocol syntax: an MQTT parameter value containing `/` remains topic text. The planner refuses wildcard publish topics, malformed subscription filters, undeclared parameters, unsupported URI component injection, user information in endpoints, and out-of-range binding values. Endpoint query strings and schema-backed handshake headers/query values are outside this first profile. Template expansion and option bytes have document admission bounds.

MQTT QoS defaults to 0, retain to false, clean session to true, keepalive to 60 seconds. These are this profile's explicit choices, not additional AsyncAPI rules. The document must state MQTT 3.1.1 or the caller must select that profile. MQTT 5-only binding fields refuse under 3.1.1. A peer must supply its own client identity and cannot reuse the described application's declared identity. Driver packet limits are distinct from MQTT 5 properties.

`Plan::prepare_bytes` currently borrows the same byte slice without copying or source traversal. It implements only the binary identity codec and does not claim schema validation. Runtime message-size admission, cancellation, subscriptions and delivery receipts are not implemented here yet.

## Explicit limits

Security requirements, payload/header schemas, correlation declarations, request/reply, last will, multiple-message classification, other protocols and earlier 2.x readers remain required work. They are not silently ignored to make an executable plan. Other protocol bindings may remain inspectable without being applied to the selected transport. Full document validation is not yet provided.

The component-operation location rule, MQTT publish/subscribe binding applicability to a peer role, and the WebSocket binding's POST option versus RFC 6455 GET still need independent review. Current behavior follows the defining component location, refuses a publish-only retain setting on a subscription, and refuses POST for the RFC 6455 profile. The development tests encode these declared policies; they are not independent conformance evidence.
