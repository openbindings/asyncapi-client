# Rust engine development

The replacement is a standalone dynamic AsyncAPI client: portable Rust semantics, optional native/host drivers, direct Rust composition, and a handwritten TypeScript facade over the same engine. The OpenBindings binding-kind adapter is a downstream consumer and is outside this repository's core.

| Location | Responsibility | Current status |
| --- | --- | --- |
| `rust/client` | Original source, immutable resource graph, native operation identity and semantics | JSON source/inspection prototype; reader families 2.6, 3.0, 3.1 |
| `rust/wasm-bridge` | Private ABI for the TypeScript facade | Inspection/owning handles; no transport logic |
| `packages/client` | Supported TypeScript API under development | Private preview; exact values, structured errors and deterministic disposal |
| `qualification/composition` | External Rust consumer with optional outer Wasm API | Direct inspection calls; preparation/message exchange still required |

The target editions are 2.0–2.6, 3.0 and 3.1 in JSON and YAML. The native protocol target is MQTT 3.1.1/5, WebSocket, Kafka, AMQP 0-9-1, HTTP and Core NATS. Browser and Worker execution begins with WebSocket and HTTP clients. This table describes work in progress; neither accepting a version nor compiling Wasm establishes that target's support.

## Checks

Use Rust 1.99.0 with rustfmt/clippy and wasm32-unknown-unknown installed:

```sh
cargo fmt --manifest-path rust/Cargo.toml --all --check
cargo test --manifest-path rust/Cargo.toml --workspace --locked
cargo clippy --manifest-path rust/Cargo.toml --workspace --all-targets --locked -- -D warnings
cargo check --manifest-path rust/Cargo.toml --workspace --locked --target wasm32-unknown-unknown
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

Protocol references use URI plus JSON Pointer resolution with explicit resource supply. Schema references require their own dialect rules and are not yet evaluated. Operation traits use ordered merge patch with authored-target precedence. Native 2.x publish/subscribe positions remain distinct from authored convenience IDs and from application direction. Effective-field source traces, complete message/server inspection, YAML, preparation, drivers and payload schemas remain open.

Development assertions are regression evidence. Independent wire observations, installed-package consumers, memory/performance budgets, long runs and fresh conformance challenges are required before mature-product claims. The prior Go/TypeScript behavior is not an oracle for new semantics.
