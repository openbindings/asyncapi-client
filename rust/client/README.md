# Dynamic AsyncAPI client

An unpublished Rust foundation for a standalone dynamic AsyncAPI client. The current slice is JSON/YAML source admission, native inspection, and pure binary/JSON/UTF-8 MQTT/WebSocket preparation, with a Rust-backed TypeScript facade. Protocol execution and complete facade journeys are under development. A reader accepting a version does not establish complete support for that edition.

The engine has no OpenBindings, JavaScript, Tokio or network dependency. Downstream Rust consumers can compose it directly, including inside a Wasm build. Host drivers and the public TypeScript facade will be separate packages.

The current reader families are AsyncAPI 2.6, 3.0 and 3.1, retaining declared patch strings. The maturity target also includes 2.0 through 2.5 after their semantics are qualified. Parse admission is not full document or payload validation.

No package is published. The older Go and TypeScript implementations are historical/transitional code, not an oracle for this engine.

## Correlation inspection and message expressions

`compiled.correlation(message_key)` resolves a message's effective correlation
declaration, including traits and supplied external resources. It returns source
coordinates and a `RuntimeExpression`, or `None` when absent. Unknown message keys
and invalid declarations return diagnostics. This does not enable request/reply
execution; preparation still refuses declarations needing that runtime support.

`RuntimeExpression::parse("$message.payload#/id")?.evaluate(header, payload)`
selects an owning `Json` view without coercion or number rounding. Missing roots
and paths return `None`; JSON null remains a present value. Expressions admit
16 KiB and 256 pointer segments. The suffix follows AsyncAPI's JSON Pointer
string grammar, including `~0`/`~1`; percent signs are literal. Root expressions
with or without a trailing `#` select the entire header or payload value.

## Independent message values

`Json::parse(text, limits)` admits strict JSON independently of a document. It
preserves number tokens, rejects duplicate members and malformed Unicode, and
has no YAML fallback. Children own their source and remain valid after dropping
the root. Byte, node and depth limits apply.

`Json::from_serializable(&value, limits)` supports ordinary Serde input, including
128-bit integers, enums and finite floats. Nonfinite floats fail instead of
becoming null. Standard Serde JSON map-key and byte-array conversions apply;
duplicate resulting names fail. The pinned arbitrary-precision Number protocol
is checked; RawValue and unknown private protocols fail. Literal private-looking
map keys remain data. Custom Serialize/Display code and transparent-wrapper
recursion remain caller work outside the library's resource bounds.

`value.deserialize::<T>()` explicitly projects a JSON or YAML view into an owned
Rust type. Integer overflow fails; selecting a floating-point destination permits
rounding. The exact owner remains unchanged. Default errors omit input and custom
error text; `error.detail()` deliberately exposes the underlying Serde error.

These value APIs feed the binary/JSON/UTF-8 transport codecs. They are not schema
evaluation; prepared execution still refuses payload schemas until a supported
evaluator is available. See `docs/preparation.md` for the codec/framing contract.

YAML uses granit-parser grammar/events and a bounded JSON-compatible value graph. Original value ranges and alias-use ranges are separate; `Json::raw` is authored source, while `Json::to_json` emits the logical JSON value with exact numbers. Mapping keys use scalar strings, including plain numeric/boolean-looking keys. Explicit non-string keys, non-finite numbers, unknown tags and recursive alias expansion are refused. This initial policy still requires broader independent YAML qualification.
