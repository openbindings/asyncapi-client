# Dynamic AsyncAPI client

An unpublished Rust foundation for a standalone dynamic AsyncAPI client. The current slice is JSON/YAML source admission, native inspection, and pure binary MQTT/WebSocket preparation, with a Rust-backed TypeScript facade. Protocol execution and complete facade journeys are under development. A reader accepting a version does not establish complete support for that edition.

The engine has no OpenBindings, JavaScript, Tokio or network dependency. Downstream Rust consumers can compose it directly, including inside a Wasm build. Host drivers and the public TypeScript facade will be separate packages.

The current reader families are AsyncAPI 2.6, 3.0 and 3.1, retaining declared patch strings. The maturity target also includes 2.0 through 2.5 after their semantics are qualified. Parse admission is not full document or payload validation.

No package is published. The older Go and TypeScript implementations are historical/transitional code, not an oracle for this engine.

YAML uses granit-parser grammar/events and a bounded JSON-compatible value graph. Original value ranges and alias-use ranges are separate; `Json::raw` is authored source, while `Json::to_json` emits the logical JSON value with exact numbers. Mapping keys use scalar strings, including plain numeric/boolean-looking keys. Explicit non-string keys, non-finite numbers, unknown tags and recursive alias expansion are refused. This initial policy still requires broader independent YAML qualification.
