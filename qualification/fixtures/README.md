# Local protocol peers

Pinned Aedes 1.2.0 and ws 8.22.0 provide disposable loopback peers for development. MQTT.js 5.16.0 is reserved for independent sender/receiver consumers as the client slice expands. Aedes supports MQTT 3.1.1; this fixture does not qualify MQTT 5.

`peers.mjs` starts fresh brokers/servers on allocated loopback ports and records a bounded list of observed payload hashes, lengths, MQTT route/flags and WebSocket frame types. MQTT uses explicit disposable credentials; the WebSocket peer sends an unsolicited notice before echoing binary messages. Records use fixed expectations, not values taken from a prepared client plan.

The adjacent `driver-controls` crate uses protocol libraries directly. Its successful traffic is a baseline/control, not dynamic AsyncAPI client execution. The wrong-topic case deliberately receives successful broker acknowledgements while failing the independent fixed-route checker. Auth/TLS, QoS 0/2, subscriptions, retained delivery, cancellation, correlation, reconnect and other failure cases remain required additions.

Install with `npm ci --ignore-scripts`. Build controls with `cargo build --release --locked --manifest-path qualification/driver-controls/Cargo.toml`. The development-loop runner captures raw process streams, peer records, timing samples and failures outside the product source tree. External review of fixture correctness is still pending.
