# Architecture

The package has three deliberate layers:

1. The native `Client` loads an AsyncAPI artifact, inventories its operations, and offers `publish`, `subscribe`, and explicit `start` ergonomics.
2. The reusable `Engine` prepares one artifact operation and returns a cardinality-neutral `Execution` session.
3. Artifact code beneath the engine performs ordinary AsyncAPI document resolution and the concrete HTTP/WebSocket work.

No layer depends on an OpenBindings SDK or constructs an OBI. The OpenBindings
adapter lives in the language SDK repository and converts only at the outer
edge: source and operation coordinates in, then values, failures, metadata,
and lifecycle transitions out.

Artifact profiles version standalone execution behavior without placing
OpenBindings binding-specification identifiers in the public API. New protocol support
belongs below the engine when it is ordinary AsyncAPI behavior; OpenBindings-
specific selection, context negotiation carriage, and error translation belong
only in the adapter.

Protocol engines are optional packages/subpackages. The MQTT driver delegates
wire exchange to MQTT.js/Paho; the Kafka driver delegates it to Confluent's
librdkafka-backed JavaScript client/franz-go. AsyncAPI reference, operation,
message, address, security-alternative, and codec resolution remain in the
shared engine. Drivers receive those resolved facts, interpret only their
protocol-binding objects, and never synthesize protocol fields into operation
values.

The test-only WebSocket controls are explicit deterministic seams. They expose
counts and bounded actions, never pooled connections or credential-bearing
keys, and are not application configuration.
