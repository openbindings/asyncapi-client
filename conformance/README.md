# Conformance

Conformance is evaluated at three levels:

- native unit and live-protocol tests prove the standalone artifact runtime;
- SDK adapter suites prove that translation preserves the same behavior through OpenBindings;
- the varied corpus and held-out artifacts measure coverage without turning one corpus into the specification.

Fixtures should vary provenance as well as content. Generated examples from one
toolchain do not count as independent evidence. Any implementation rule added
for a corpus case must be justified by AsyncAPI or an incorporated protocol
binding, then tested with a minimal specification-derived fixture and at least
one held-out artifact when available.
