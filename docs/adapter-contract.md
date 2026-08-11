# OpenBindings adapter contract

The OpenBindings AsyncAPI adapter is a thin bridge over this package. It owns:

- exact binding-revision to standalone-profile selection;
- conversion from an OBI source entry and binding ref to `PrepareOptions`;
- translation between the Core invocation handle and `Execution`;
- conversion of Core hook/context structures at the package boundary;
- conversion of standalone prerequisites, errors, metadata, and completion into Core vocabulary.

It does not own document parsing, reference resolution, target selection,
message encoding, security placement, HTTP/WebSocket dispatch, response
decoding, pooling, backpressure, or cancellation mechanics.

Synthesis remains an adapter responsibility because it creates an OBI. It may
reuse standalone analysis primitives, but the standalone client must neither
know about nor generate the document model above it.

Correct application behavior through OpenBindings must never require a caller
to inspect HTTP status, headers, WebSocket framing, or another protocol-specific
fact. Raw protocol evidence may be retained only through an explicit diagnostic
escape hatch.
