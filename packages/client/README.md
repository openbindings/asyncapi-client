# Rust-backed AsyncAPI TypeScript preview

This private development package exposes the same Rust source and operation inspection as the native crate. JSON/YAML documents in the 2.6, 3.0 and 3.1 reader families are admitted; complete edition conformance, preparation and protocol execution remain open work. The historical TypeScript engine is not used.

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

For Workers, pass a compiled Wasm module to `createClient({ wasm: module })`. For testing in Node, pass the asset bytes explicitly. The generated ABI is private. Initialization shares one module instance, including concurrent calls; independent Wasm heaps cannot exchange these handles.

Each document, operation and source view owns its backing source. Dispose each returned handle with `using`, `[Symbol.dispose]()` or `.dispose()`. Disposing a parent does not invalidate retained children. Source views expose raw JSON and exact number tokens without forcing values through JavaScript numbers. Parse original text when numeric fidelity matters; previously rounded JavaScript numbers cannot recover their original values.

For a YAML source view, `.raw` is the defining authored snippet and `.json` is its logical JSON representation. `location.bytes` and `location.aliases` use UTF-8 byte offsets, not JavaScript string indices. Alias-use ranges make expansion provenance explicit. Numeric and boolean-looking plain mapping keys stay strings; scalar values use the documented JSON-compatible YAML policy.
