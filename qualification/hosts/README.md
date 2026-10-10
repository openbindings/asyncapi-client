# Actual host development checks

These checks launch a real Chromium executable and local workerd, use fresh
loopback peers and retain every report. They are implementation-authored
development evidence, not independent qualification. The exchange suite checks
TypeScript and direct outer Rust/Wasm execution, peer bytes, cancellation and
100 close cycles. The fault suite checks queue count/bytes, oversized frames,
UTF-8 text rejection, abnormal closure, deadlines and active cancellation.

Build the facade and outer composition module using Rust 1.99.0 and
wasm-bindgen-cli 0.2.129:

```sh
npm ci --prefix packages/client
npm run build:wasm --prefix packages/client
npm run build --prefix packages/client
cargo build --manifest-path qualification/composition/Cargo.toml --features wasm --target wasm32-unknown-unknown --release --locked
wasm-bindgen --target web --out-dir qualification/composition/wasm --out-name composition qualification/composition/target/wasm32-unknown-unknown/release/asyncapi_external_composition.wasm
npm ci --prefix qualification/fixtures
npm ci --prefix qualification/hosts
node qualification/hosts/run.mjs exchange /tmp/asyncapi-host-exchange
node qualification/hosts/run.mjs faults /tmp/asyncapi-host-faults
```

Output directories must be new. Set `ASYNCAPI_CHROME` to the Chromium executable
(default is macOS Chrome). Optional `ASYNCAPI_HOST_TOOLS` points at an existing
installation of the exact package.json tools; `ASYNCAPI_WORKERD` overrides its
binary and `ASYNCAPI_COMPOSITION_WASM` overrides the outer module directory.
Reported actual versions and artifact hashes remain the authority for each run.

The Worker is kept inside its request lifetime. Its network service permits
local fixture addresses; this is not a production Cloudflare capability claim.
The exchange suite may fail on the pinned Worker close anomaly documented in
`rust/host/README.md`; keep the failure receipt rather than retrying to claim
qualification. Callback assignment instrumentation observes library-owned
handlers. It does not prove a settled Wasm heap plateau or full host GC.

The raw shutdown controls contain no Rust or facade calls:

```sh
node qualification/hosts/probe-worker-close.mjs plain /tmp/asyncapi-raw-close
node qualification/hosts/probe-worker-close.mjs wasm-grow /tmp/asyncapi-raw-wasm-buffer
```

The second control sends a view of a WebAssembly memory and grows that memory.
Its observed failure is not enough to attribute causality to memory growth;
matched timing and alternative transport construction still need investigation.
