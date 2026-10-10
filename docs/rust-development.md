# Rust engine development

The replacement is a standalone dynamic AsyncAPI client: portable Rust semantics, optional native/host drivers, direct Rust composition, and a handwritten TypeScript facade over the same engine. The OpenBindings binding-kind adapter is a downstream consumer and is outside this repository's core.

| Location | Responsibility | Current status |
| --- | --- | --- |
| `rust/client` | Original source, immutable resource graph, native operation identity and semantics | JSON/YAML inspection and pure MQTT/WebSocket preparation; reader families 2.6, 3.0, 3.1 |
| `rust/session` | Portable admission, plan ownership, routing and shared payload quotas | Used by native and host runtimes |
| `rust/host` | Rust-owned browser/Worker WebSocket callbacks and lifecycle | Binary execution, cancellation and bounded queues in development |
| `rust/native` | Explicitly owned Tokio sessions and protocol drivers | Initial binary MQTT 3.1.1 QoS 0/1 and WebSocket over TCP |
| `rust/wasm-bridge` | Private ABI for the TypeScript facade | Owning inspection/plan/session handles; delegates host execution to Rust |
| `packages/client` | Supported TypeScript API under development | Private preview; exact values, structured errors and deterministic disposal |
| `qualification/composition` | External Rust consumer with optional outer Wasm API | Direct inspection, preparation and host WebSocket exchange inside an outer Rust/Wasm module |

The target editions are 2.0–2.6, 3.0 and 3.1 in JSON and YAML. The native protocol target is MQTT 3.1.1/5, WebSocket, Kafka, AMQP 0-9-1, HTTP and Core NATS. Browser and Worker execution begins with WebSocket and HTTP clients. This table describes work in progress; neither accepting a version nor compiling Wasm establishes that target's support.

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

Protocol references use URI plus JSON Pointer resolution with explicit resource supply. Schema references require their own dialect rules and are not yet evaluated. Operation traits use ordered merge patch with authored-target precedence. Native 2.x publish/subscribe positions remain distinct from authored convenience IDs and from application direction. YAML aliases retain their defining ranges and use-site trace; expanded bytes, nodes and number-conversion work are bounded. Effective trait fields retain their defining source, including relative references inherited from external traits. Compilation preserves native message/server selection and explicit empty message sets. The first pure plan supports one schema-free binary message over MQTT 3.1.1 or WebSocket RFC 6455. It resolves declared variables and parameters without I/O. Native sessions execute the initial binary MQTT 3.1.1 QoS 0/1 and WebSocket paths over TCP. Security schemes, reply plans, schema evaluation, TLS, other driver profiles and complete edition conformance remain open. See [native sessions](../rust/native/README.md) for current ownership and receipt semantics. See [preparation](preparation.md) for the current contract and limits.

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

Chrome and local workerd have exercised binary exchange, cancellation, queue
limits and callback release. The current local profile pins workerd
1.20261010.1 with compatibility date 2026-10-08 and the WebSocket constructor.
The prior 1.20261006.1 runtime intermittently emitted an error followed by clean
close. The newer release has relevant ownership repairs, and the constructor
path passed 800 diagnostic control connections plus the actual-client suites.
Other raw Worker connection methods still reproduced errors. The older failure
receipts are preserved, and this is not a universal shutdown guarantee. The
client continues to keep every host error terminal.
