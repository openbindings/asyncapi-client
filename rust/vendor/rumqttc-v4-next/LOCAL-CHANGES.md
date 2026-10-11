# Pinned MQTT transport repair

This is the published `rumqttc-v4-next` 0.34.0 source, with the Apache-2.0
license and upstream attribution preserved. `UPSTREAM.json` records original
file hashes and upstream identity. The published archive omits the root license
file, so `LICENSE` is preserved from that exact upstream Git commit and recorded
as a supplemental source; `LOCAL.patch` is the complete local change.
This dependency is excluded from our workspace member list and is not published.

The MQTT 3.1.1 state repair resends PUBREL for a repeated PUBREC while retaining
the original pending completion, and rejects PUBACK/PUBREC whose packet type
does not match the pending publication's QoS. It does not add AsyncAPI semantics.
The upstream backend already suppresses duplicate QoS 2 receive delivery and
handles repeated PUBREL. Native raw-packet integration tests exercise these
behaviors through the actual client; implementation authorship is not independent
conformance qualification.

This pin is an explicit maintenance obligation. Any update must replay duplicate
PUBLISH/PUBREC/PUBREL, identifier reuse, phase/type mismatches, limits, deadlines
and cancellation against the replacement. Remove the local patch only when its
behavior is provided and verified by the selected upstream version. The source
repair has not been submitted to an upstream repository.
