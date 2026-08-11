# `@openbindings/asyncapi-mqtt`

MQTT 3.1.1 protocol driver for `@openbindings/asyncapi-client`. It is a standalone AsyncAPI component and has no OBI or OpenBindings SDK dependency.

```ts
import { AsyncAPIClient } from "@openbindings/asyncapi-client";
import { createAsyncAPIMQTTDriver } from "@openbindings/asyncapi-mqtt";

const client = await AsyncAPIClient.load("https://example.com/asyncapi.yaml", {
  drivers: [createAsyncAPIMQTTDriver()],
});
await client.publish("sendCommand", { id: "c-17" });
```

The first profile targets MQTT 3.1.1 and AsyncAPI MQTT binding versions 0.1.0 and 0.2.0. It maps server `clientId`, clean-session and `keepAlive`, operation `qos` and `retain`, and declared `userPassword` security. It never volunteers undeclared credentials. Persistent sessions, Last Will, MQTT 5-only binding fields, replies, unsupported security placement, ambiguous protocol versions, and normalized AsyncAPI 2.x multi-scheme conjunctions are refused before connecting.

The exact release claims—including implemented fields that remain unqualified pending stronger live evidence—are recorded in the repository's [`conformance/mqtt-3.1.1.json`](https://github.com/openbindings/asyncapi-client/blob/main/conformance/mqtt-3.1.1.json).
`mqtts` is not registered by default until its TLS cells are live-qualified; explicitly adding it through the driver's `protocols` option is an opt-in outside the current release claim.
