# Extraction ledger

The initial standalone implementation was extracted from the tested AsyncAPI
format packages in `openbindings-ts` and `openbindings-go`.

Moved below the boundary:

- AsyncAPI 3.0 parsing and internal/external reference closure;
- server, address, binding-field, and content resolution;
- security prerequisite derivation and credential application;
- HTTP publish and WebSocket publish/subscription execution;
- message/reply decoding, delivery-unit limits, pooling, backpressure, and cancellation;
- artifact-native operation inventory and authoring eligibility analysis.
- AsyncAPI-native document normalization, including ordered operation/message
  trait application and unresolved-trait refusal.

Retained in SDK adapters:

- binding revision constants and profile selection;
- OBI synthesis, schemas, and coverage records;
- Core invocation/context/hook/error translation;
- binding registration and selection.

The source extraction deliberately preserved protocol code before changing its
shape. Follow-up refactors are accepted only when standalone tests and the full
SDK adapter suites remain equivalent.

The TypeScript adapter now consumes the standalone analysis parser directly.
The Go adapter consumes `NormalizeDocument` before building its OBI-specific
authoring AST. Consequently the adapters do not independently reinterpret
traits or the accepted artifact envelope.

Non-HTTP protocol execution now follows the same extraction boundary. MQTT
and Kafka live in standalone optional drivers, while the OpenBindings adapters
only register them and translate the cardinality-neutral execution session.
SCRAM declarations are interpreted by the AsyncAPI layer as the existing
abstract username/password credential family; the raw declaration continues to
the Kafka driver, which selects the concrete SCRAM mechanism.
