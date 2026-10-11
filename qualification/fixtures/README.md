# Local protocol peers

Pinned Aedes 1.2.0 and ws 8.22.0 provide disposable loopback peers for development. MQTT.js 5.16.0 is reserved for independent sender/receiver consumers as the client slice expands. Aedes supports MQTT 3.1.1; this fixture does not qualify MQTT 5.

`peers.mjs` starts fresh brokers/servers on allocated loopback ports and records a bounded list of observed payload hashes, lengths, MQTT route/flags and WebSocket frame types. MQTT uses explicit disposable credentials; the WebSocket peer sends an unsolicited notice before echoing binary messages. Records use fixed expectations, not values taken from a prepared client plan.

The adjacent `driver-controls` crate uses protocol libraries directly. Its successful traffic is a baseline/control, not dynamic AsyncAPI client execution. The wrong-topic case deliberately receives successful broker acknowledgements while failing the independent fixed-route checker. The dynamic native runner also checks QoS 0/1/2 echo traffic, receipt distinctions and subscription/delivery QoS. Raw packet lifecycle tests cover lower grants, invalid grants, pre-SUBACK delivery, cancellation and deadlines. TLS/auth scheme planning, MQTT 5, wildcard subscriptions, retained-delivery qualification, correlation, reconnect and broader independent failure cases remain required additions.

Install with `npm ci --ignore-scripts`. Build controls with `cargo build --release --locked --manifest-path qualification/driver-controls/Cargo.toml`. The development-loop runner captures raw process streams, peer records, timing samples and failures outside the product source tree. External review of fixture correctness is still pending.

`check-native.mjs` runs the actual Rust client against these peers. The fixture documents are implementation-authored development cases; the peer observes CONNECT/handshake settings, route, binary flag and payload bytes without deriving its expected route from the candidate plan. The supplied output directory must be new. A passing exchange is not evidence that the other protocol profiles, long-run ownership budgets or independent conformance gates are complete.

The native runner first reverses the declared local MQTT patch in a disposable directory and checks every restored/unchanged file against its recorded published-source hash. It also rejects unlisted vendored files. CI runs this through the ordinary native fixture job. This checks patch provenance and drift; it does not establish upstream trust or protocol conformance by itself.
