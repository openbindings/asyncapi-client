# Pure message preparation development slice

`Operation::compile()` produces an owning `CompiledOperation`. It resolves native operation/channel/message/server relationships and merges traits while retaining authored field origins. References inherited from an external trait resolve against that trait's source URI. Native selectors and resolved definitions remain distinct in inspection. Compilation does not convert a 2.x document into a 3.x document.

`CompiledOperation::prepare(&PlanOptions::application())` selects deployment choices and returns an owning `Plan`. Both APIs are also available through the handwritten TypeScript facade. A downstream Rust consumer can call them directly inside its own Wasm module. No preparation API acquires source files, opens connections or reads credentials.

## Current positive path

- AsyncAPI 2.6, 3.0 and 3.1, JSON or YAML; exactly one message in the selected channel/2.x message set.
- Explicit binary (`application/octet-stream`), JSON (`application/json` or application `+json` media types), or UTF-8 text (`text/plain`) on the message or as the document default, with no payload/header schema or correlation declaration. JSON/text accept a single optional `charset=utf-8` parameter, including a quoted value. Other parameters/charsets require another codec profile. The UTF-8 default for text/plain is an explicit client-profile choice.
- MQTT 3.1.1: application or peer action, topic/filter, QoS 0/1/2, retain for publishing, client identity, clean session and keepalive. The plan supports these settings; runtime execution is still separate work.
- WebSocket RFC 6455: application action, server base path plus channel path, GET handshake. Peer plans require an established-connection/hosting route, which this slice reports as a requirement.

An omitted 3.x operation message list means all channel messages; an empty list means no messages and cannot produce an exchange plan. Explicit message references must name channel message entries even when those entries reference shared component definitions. Empty or omitted channel server lists use the root servers; multiple available servers require a caller choice. Unknown supplied choices are errors.

Parameter substitution retains protocol syntax: an MQTT parameter value containing `/` remains topic text. The planner refuses wildcard publish topics, malformed subscription filters, undeclared parameters, unsupported URI component injection, user information in endpoints, and out-of-range binding values. Endpoint query strings and schema-backed handshake headers/query values are outside this first profile. Template expansion and option bytes have document admission bounds.

MQTT QoS defaults to 0, retain to false, clean session to true, keepalive to 60 seconds. These are this profile's explicit choices, not additional AsyncAPI rules. The document must state MQTT 3.1.1 or the caller must select that profile. MQTT 5-only binding fields refuse under 3.1.1. A peer must supply its own client identity and cannot reuse the described application's declared identity. Driver packet limits are distinct from MQTT 5 properties.

`Plan::prepare_bytes` validates encoded bytes and borrows the original slice. Binary identity and UTF-8 validation need no body allocation; JSON admission parses an exact value. `Plan::decode_payload` retains encoded bytes plus the exact JSON view so receive consumers do not need another parse. `Payload::text` and `Payload::from_json` provide typed inputs; `prepare_payload` checks the selected codec. These APIs do not claim schema validation. Runtime message-size admission, cancellation, subscriptions and delivery receipts live in the separate `dynamic-asyncapi-native` crate; they are not side effects of this preparation API.

WebSocket framing is explicit in the prepared transport. The default is binary for
binary content and text for JSON/UTF-8. `PlanOptions::websocket_frame` (TypeScript
`websocketFrame`) can select a binary frame for JSON or text. Binary content cannot
be placed in a text frame by this profile. These are client defaults, not AsyncAPI
requirements. MQTT carries the encoded bytes without WebSocket framing options.

Invalid incoming payloads are observations with a diagnostic, distinct from
wrong-frame/route rejection and terminal transport failures. Subsequent valid
messages can still progress. Queue limits count wire payload bytes and message
slots; decoded JSON source/index storage is separately bounded by value admission
and is not included in a claim about total heap use. JSON currently retains its
original Bytes alongside its exact source/index, with that additional storage
made explicit for later ownership/performance qualification.

## Explicit limits

Security requirements, payload/header schemas, correlation declarations, request/reply, last will, multiple-message classification, other protocols and earlier 2.x readers remain required work. They are not silently ignored to make an executable plan. Other protocol bindings may remain inspectable without being applied to the selected transport. Full document validation is not yet provided.

The component-operation location rule, MQTT publish/subscribe binding applicability to a peer role, and the WebSocket binding's POST option versus RFC 6455 GET still need independent review. Current behavior follows the defining component location, refuses a publish-only retain setting on a subscription, and refuses POST for the RFC 6455 profile. The development tests encode these declared policies; they are not independent conformance evidence.

## Document authentication requirements

`CompiledOperation::authentication(server_key)` (TypeScript `authentication(serverKey)`)
inspects server and effective operation security alternatives with authored and
resolved coordinates. Server and operation requirements both apply. In 2.6 each
alternative names components and requires every listed scheme; in 3.x each
alternative is a scheme or reference. The 2.6 names use the entry document's
`components.securitySchemes`; scheme references then follow normal resource
resolution. Inspection reports scheme types and required scopes, without claiming
complete validation of unsupported OAuth/HTTP/SASL scheme configuration.

A sole alternative is automatic. Multiple alternatives require explicit zero-based
`PlanOptions.security.server` and/or `.operation` choices, independently. An empty
2.6 requirement object is an empty conjunction and is never chosen automatically
among alternatives. Empty operation security does not remove server requirements.
Preparation resolves selected alternatives only; inspecting all alternatives can
therefore report a missing resource which an explicitly selected plan does not need.

Current execution plans support `userPassword` on MQTT 3.1.1 and `X509` on secure
MQTT/WebSocket endpoints, plus WebSocket `httpApiKey` with `in: query`. HTTP key
name and placement are visible in `httpApiKey` inspection metadata. Header and
cookie forms are inspectable but currently refuse execution. Other mechanisms
return an authentication requirement.
X509 never upgrades a plaintext endpoint implicitly. Plan descriptions expose
selected requirements, never acquired credentials. Native session configuration
supplies one connection-wide username/password pair and one optional TLS client
identity; all attached plans must have their declared mechanisms configured before
network activity. This is local configuration checking, not proof of authorization
by the broker or server. Browser/Worker WebSocket sessions support declared query keys; they refuse
username/password and client-certificate mechanisms before socket construction.
Ambient client certificates are not assumed to satisfy declared X509.

Current profile bounds are 64 alternatives per security array, 16 schemes in a
2.6 alternative, 256 scopes per scheme, and aggregate resolved scope/key-name text no larger
than the document's configured source-byte limit. Selection does not expand a
server/operation cross product. No credentials are fetched during inspection or
preparation. Additional mechanisms and host-capable authentication remain open.

Query API-key values come from `SessionOptions.query_credentials` in Rust
(`QueryCredentials::new([(name, value)])`) or `queryCredentials: { [name]: value }`
in TypeScript. Keys are the declared HTTP parameter names, not component aliases.
Missing and unused values refuse before I/O. Server/operation requirements across
attached plans are combined; repeated declarations of one parameter emit it once.
Two different definitions targeting the same parameter use the same explicitly
supplied value; peer authorization is still required.

A portable Rust routine builds a runtime-only endpoint using percent-encoded UTF-8
names and values. Plans and their descriptions retain the original endpoint. The
runtime endpoint and credential map redact Debug; session state does not retain
the credential map after setup. The transport/host necessarily receives the URL;
this is not a promise of zeroization or control over host logs. Native and Chromium redirect refusals are verified at peers. A traced Worker
consumer also exercised refusal, but the uninstrumented Worker run failed during
close of its first authenticated exchange; its reliability remains unqualified.
Full independent qualification is still pending.

Query credential limits are 32 names, 256 UTF-8 bytes per name, 16 KiB per value,
and 64 KiB total raw name/value bytes. Current preparation excludes authored URL
queries/fragments; the runtime refuses any collision rather than overwriting.
