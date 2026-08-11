# Release qualification

`pnpm qualify:release` is the local release gate. It requires:

1. TypeScript typechecking, unit/integration tests, and package build;
2. Go race-enabled tests with workspace replacement disabled;
3. a boundary check proving neither language depends on an OpenBindings SDK and public TypeScript declarations do not leak Core vocabulary;
4. clean ESM and CommonJS consumers installed from the packed npm tarball;
5. a clean Go module consuming the standalone module through its public API.

Optional driver profiles add their own authority matrix, package boundary,
and live protocol gate. MQTT 3.1.1 runs TypeScript against an in-process Aedes
broker and Go against the same broker contract. Kafka runs both languages and
both SDK bridges against a pinned disposable Redpanda container, then enables
SCRAM-SHA-256 and repeats the security path through abstract username/password
context. Cells marked `unqualified` are not release claims even when
implementation code is present.

SDK adapter suites are a separate required integration gate until repositories
can consume published `0.1.x` packages. CI uses explicit local workspace
replacements only for the unreleased development dependency.

Release qualification does not imply complete AsyncAPI ecosystem coverage. The
corpus and holdout report in the OpenBindings conformance repository remains the
evidence for which protocol cells are admitted, excluded, or still unobserved.
Its authority-matrix evidence gate assigns every surveyed cell exactly once and
distinguishes executable support from deterministic exclusion; “fully
accounted for” must never be reported as “fully supported.”
