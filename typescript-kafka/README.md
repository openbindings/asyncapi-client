# `@openbindings/asyncapi-kafka`

Kafka protocol driver for the standalone `@openbindings/asyncapi-client`.
It interprets AsyncAPI Kafka binding objects and delegates Kafka wire behavior
to Confluent's maintained, librdkafka-backed JavaScript client. It has no
dependency on an OpenBindings SDK and can be used directly by any AsyncAPI
consumer.

```ts
import { AsyncAPIClient } from "@openbindings/asyncapi-client";
import { createAsyncAPIKafkaDriver } from "@openbindings/asyncapi-kafka";

const client = await AsyncAPIClient.load(document, {
  drivers: [createAsyncAPIKafkaDriver()],
  context: {
    configuration: {
      kafka: { groupId: "orders-ui", fromBeginning: false },
    },
  },
});

await client.publish("placeOrder", { id: "order-17" });
```

The default profile owns plaintext `kafka` targets and AsyncAPI Kafka binding
versions 0.1.0 through 0.5.0. It never creates topics. TLS, Schema Registry
framing, dynamic record keys, record headers, tombstones, and reply operations
are refused until separately qualified. See `conformance/kafka.json` in the
repository for the exact executable support boundary.
