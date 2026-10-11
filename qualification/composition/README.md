# External Rust composition consumer

This separate Cargo workspace depends directly on the portable client. It exposes a narrow outer API natively and optionally through wasm-bindgen. Client calls and retained ownership remain within Rust; it does not call the standalone TypeScript facade or implement OpenBindings adaptation.

`cargo test --manifest-path qualification/composition/Cargo.toml` exercises the native consumer. Building the same manifest with `--target wasm32-unknown-unknown --features wasm` builds the outer Wasm interface. Initial inspection-only checks do not satisfy the milestone's preparation and message-exchange obligations.
