# Rust-backed AsyncAPI TypeScript preview

This private development package exposes the same Rust source and operation inspection as the native crate. JSON documents in the 2.6, 3.0 and 3.1 reader families are admitted; complete edition conformance, YAML, preparation and protocol execution remain open work. The historical TypeScript engine is not used.

Build with `npm run build:wasm` (Rust and wasm-bindgen 0.2.129 for package developers), then `npm run build`. A consumer of the built package needs the included Wasm assets, not Rust. Package-consumer and actual browser/Worker execution qualification is still pending.

```ts
import { createClient } from '@openbindings/asyncapi-client';
const client = await createClient();
using document = client.parse(source, { sourceUri: 'https://example.test/api.json' });
for (const entry of document.operations()) {
  if (!entry.ok) { console.error(entry.error.code); continue; }
  using operation = entry.value;
  console.log(operation.identity, operation.describe());
}
```

For Workers, pass a compiled Wasm module to `createClient({ wasm: module })`. For testing in Node, pass the asset bytes explicitly. The generated ABI is private. Initialization currently shares one module instance; independent Wasm heaps cannot exchange these handles.

Each document, operation and source view owns its backing source. Dispose each returned handle with `using`, `[Symbol.dispose]()` or `.dispose()`. Disposing a parent does not invalidate retained children. Source views expose raw JSON and exact number tokens without forcing values through JavaScript numbers. Parse original text when numeric fidelity matters; previously rounded JavaScript numbers cannot recover their original values.
