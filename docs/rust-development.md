# Rust engine development

The replacement is a standalone dynamic AsyncAPI client: portable Rust semantics, optional native/host drivers, direct Rust composition, and a handwritten TypeScript facade over the same engine. The OpenBindings binding-kind adapter is a downstream consumer and is outside this repository's core.

| Location | Responsibility | Current status |
| --- | --- | --- |
| `rust/client` | Original source, immutable resource graph, native operation identity and semantics | JSON/YAML inspection, exact values, correlation expressions and pure MQTT/WebSocket preparation; reader families 2.6, 3.0, 3.1 |
| `rust/session` | Portable admission, plan ownership, routing and shared payload quotas | Used by native and host runtimes |
| `rust/host` | Rust-owned browser/Worker WebSocket callbacks and lifecycle | Binary/JSON/text execution, cancellation and bounded queues; Worker failure unresolved |
| `rust/native` | Explicitly owned Tokio sessions and protocol drivers | Binary/JSON/text MQTT 3.1.1 QoS 0/1/2 and WebSocket, with explicit TLS and selected authentication |
| `rust/wasm-bridge` | Private ABI for the TypeScript facade | Owning inspection/plan/session handles; delegates host execution to Rust |
| `packages/client` | Supported TypeScript API under development | Private preview; exact values, structured errors and deterministic disposal |
| `qualification/composition` | External Rust consumer with optional outer Wasm API | Direct inspection, preparation and host WebSocket exchange inside an outer Rust/Wasm module |

The target editions are 2.0–2.6, 3.0 and 3.1 in JSON and YAML. The native protocol target is MQTT 3.1.1/5, WebSocket, Kafka, AMQP 0-9-1, HTTP and Core NATS. Browser and Worker execution begins with WebSocket and HTTP clients. This table describes work in progress; neither accepting a version nor compiling Wasm establishes that target's support.

## Paused development checkpoint

Matthew approved adopting the Rust work as the main line and pausing the
continuous development loop on 2026-10-10. This is source adoption, not a release
or a maturity declaration. No packages, tags or deployments were requested.

The completed implementation checkpoint is `f761c77bbad2946a7f34e742c81027f62bb93c07`.
It passed 126 Rust tests, 24 TypeScript tests, four external Rust tests, native
and Wasm builds, formatting/clippy, a fresh npm archive consumer, and all four
component CI jobs. Chromium passed 22 combined observations. A separate Worker
consumer passed pure expression evaluation through both the TypeScript facade
and an outer Rust/Wasm module.

The combined Worker test failed after its first authenticated binary exchange
on workerd 1.20261010.1, compatibility date 2026-10-08. The peer verified the
credentials, echoed two bytes and observed close code 1000; the client received
a terminal WebSocket error. A diagnostic reported `Network connection lost`.
Unchanged passing runs and tracing controls are also preserved. None establishes
a repair. This is the unresolved D019 failure; Worker transport qualification
and deployed Cloudflare verification remain open. Host errors are not suppressed.

The unfinished reply-topology/exchange-planning fragment is preserved separately
on [codex/asyncapi-exchange-checkpoint-20261011](https://github.com/openbindings/asyncapi-client/tree/codex/asyncapi-exchange-checkpoint-20261011).
It was interrupted before formatting, compilation, tests or TypeScript exposure
and is excluded from this main-line implementation. Its checkpoint note records
the disposition; it must not be treated as an executable or verified feature.

Request/reply runtime matching, dynamic reply addresses, schema evaluation,
additional authentication/acquisition paths, AsyncAPI 2.0–2.5, MQTT 5, Kafka,
AMQP 0-9-1, HTTP and Core NATS remain incomplete. Recovery, manual settlement,
ownership/performance qualification, independent conformance and the broader
maturity gates also remain open. Current native MQTT acknowledges received
messages before application processing. Legacy retirement and downstream
consumer migration are separate, unfinished actions. Resumption requires a new
assignment; this checkpoint does not schedule continued work.

## Checks

Use Rust 1.99.0 with rustfmt/clippy and wasm32-unknown-unknown installed:

```sh
cargo fmt --manifest-path rust/Cargo.toml --all --check
cargo test --manifest-path rust/Cargo.toml --workspace --locked
cargo clippy --manifest-path rust/Cargo.toml --workspace --all-targets --locked -- -D warnings
cargo check --manifest-path rust/Cargo.toml --workspace --exclude dynamic-asyncapi-native --locked --target wasm32-unknown-unknown
cargo test --manifest-path qualification/composition/Cargo.toml --locked
```

For the TypeScript preview, install wasm-bindgen-cli 0.2.129, then:

```sh
cd packages/client
npm ci
npm run build:wasm
npm run build
npm test
```

The package currently lives outside the legacy pnpm workspace to avoid resolving two implementations of the same eventual package name. Its source/asset build is explicit, and it is private/unpublished. The legacy checks remain during migration; retirement, downstream consumer migration and publication are separate tracked actions.

## Current boundary

Source admission rejects duplicate decoded JSON keys, retains original byte ranges and exact numeric tokens, and bounds source size, depth, nodes and reference/trait work. Limits are development policy, not completed performance qualification. The implementation's depth ceiling is 96. Each document/resource completion is immutable; retained operations and source views own their data.

Protocol references use URI plus JSON Pointer resolution with explicit resource supply. Schema references require their own dialect rules and are not yet evaluated. Operation traits use ordered merge patch with authored-target precedence. Native 2.x publish/subscribe positions remain distinct from authored convenience IDs and from application direction. YAML aliases retain their defining ranges and use-site trace; expanded bytes, nodes and number-conversion work are bounded. Effective trait fields retain their defining source, including relative references inherited from external traits. Compilation preserves native message/server selection and explicit empty message sets. Pure plans support schema-free binary, JSON and UTF-8 messages over MQTT 3.1.1 or WebSocket RFC 6455, with explicit variables, parameters and no preparation I/O. Native sessions support MQTT QoS 0/1/2 and WebSocket, system/custom CA trust and optional client identities. Selected document authentication covers native MQTT username/password, native secure-transport X509 and portable WebSocket query API keys. Server and operation requirements both apply; unsupported mechanisms refuse. Correlation inspection and runtime-expression evaluation retain exact values and provenance, but do not enable request/reply transport execution. Schemas, additional profiles/editions and complete security qualification remain open. See [native sessions](../rust/native/README.md) for current ownership and receipt semantics. See [preparation](preparation.md) for the current contract and limits.

Development assertions are regression evidence. Independent wire observations, installed-package consumers, memory/performance budgets, long runs and fresh conformance challenges are required before mature-product claims. The prior Go/TypeScript behavior is not an oracle for new semantics.

## Native protocol fixtures

`cargo test` includes loopback lifecycle tests. The Node peers exercise document-driven routes across JSON/YAML and native 2.6/3.0/3.1 interpretations:

```sh
npm ci --prefix qualification/fixtures --ignore-scripts --no-audit --no-fund
cargo build --manifest-path rust/Cargo.toml -p dynamic-asyncapi-native --example exchange --locked
node qualification/fixtures/check-native.mjs /tmp/asyncapi-fixture-run
```

Choose a fresh output directory; the runner refuses to overwrite a previous receipt. It preserves source documents, binary/source hashes, stdout/stderr and peer records, including a wrong-route control. Native Linux/macOS CI runs these fixtures and uploads the records. Native transport execution does not establish browser/Worker transport support.

## Browser and Worker development

The `dynamic-asyncapi-host` crate composes directly in Rust/Wasm and backs the
TypeScript `openSession` API. It uses the ordinary WebSocket host interface,
without arbitrary handshake headers. The first positive fixture is binary
WebSocket over local TCP; TLS and broader profiles need their own evidence.
Host send receipts mean acceptance into the host buffer, not socket flush or
peer processing. See [host sessions](../rust/host/README.md).

Chrome and local workerd have exercised binary/JSON/text exchange, cancellation,
queue limits, iterators and callback release. Local workerd uses 1.20261010.1,
compatibility date 2026-10-08 and the WebSocket constructor. Both failed and
passed outcomes remain recorded, including failures after the runtime upgrade.
See the paused checkpoint above for the unresolved transport failure. A passing
local run does not establish deployed Cloudflare or universal shutdown behavior.
