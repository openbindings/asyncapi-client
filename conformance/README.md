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

Protocol-driver matrices account separately for supported, excluded, and
unqualified authority cells. Current matrices cover
[`mqtt-3.1.1.json`](mqtt-3.1.1.json) and [`kafka.json`](kafka.json);
completing a matrix is not the same as supporting every cell.
