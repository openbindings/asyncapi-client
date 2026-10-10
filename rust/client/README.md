# Dynamic AsyncAPI client

An unpublished Rust foundation for a standalone dynamic AsyncAPI client. The current slice is JSON source admission and native document inspection; protocol execution, YAML and the supported TypeScript facade are under development. A reader accepting a version does not establish complete support for that edition.

The engine has no OpenBindings, JavaScript, Tokio or network dependency. Downstream Rust consumers can compose it directly, including inside a Wasm build. Host drivers and the public TypeScript facade will be separate packages.

The current reader families are AsyncAPI 2.6, 3.0 and 3.1, retaining declared patch strings. The maturity target also includes 2.0 through 2.5 after their semantics are qualified. Parse admission is not full document or payload validation.

No package is published. The older Go and TypeScript implementations are historical/transitional code, not an oracle for this engine.
