# Fidelity contract

For every admitted AsyncAPI operation/protocol cell, invocation through this
client must be as capable as bespoke code faithfully implementing the same
artifact. The client may hide syntax through ergonomics, but it must not invent,
drop, merge, reorder, or silently reinterpret artifact-authored behavior.

The qualification dimensions are:

- target fidelity: effective server, variables, address parameters, and protocol fields;
- message fidelity: governing alternatives, content type, encoding, reply declarations, and byte bounds;
- security fidelity: server plus operation requirements and concrete credential placement;
- lifecycle fidelity: ordering, input half-close, streaming, partial outputs, cancellation, backpressure, and terminal completion;
- failure fidelity: application-authored failures remain application data where the artifact says so; unsuccessful transport completion and local runtime failure remain structural failures with protocol evidence available diagnostically;
- authoring fidelity: operation inventory and exclusions account for artifact targets without silently manufacturing a callable cell.

Artifact normalization is part of that contract: reference resolution occurs
before AsyncAPI operation/message traits are applied, traits merge in authored
order, and the target object wins every conflict. An unresolved trait marks
the affected operation or message unresolvable; a partial merge is never
treated as a faithful operation.

“Supported” means these properties are demonstrated by conformance fixtures and
live protocol tests. A target outside the implemented profile is refused before
dispatch and recorded as a gap; it is never approximated.
