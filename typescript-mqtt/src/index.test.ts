import { createServer, type Server } from "node:net";
import { Aedes, type AedesPublishPacket, type Client as AedesClient } from "aedes";
import { afterEach, describe, expect, it } from "vitest";
import { AsyncAPIClient } from "@openbindings/asyncapi-client";
import { createAsyncAPIMQTTDriver } from "./index.js";

describe("AsyncAPI MQTT 3.1.1 driver", () => {
  const cleanup: Array<() => Promise<void>> = [];

  afterEach(async () => {
    while (cleanup.length > 0) await cleanup.pop()?.();
  });

  it("publishes and subscribes through a live broker with binding-authored topic, QoS, retain, and userPassword", async () => {
    const { url, broker } = await liveBroker();
    const packets: AedesPublishPacket[] = [];
    broker.authenticate = (_client, username, password, done) => {
      done(null, username === "sensor" && password?.toString() === "secret");
    };
    broker.on("publish", (packet, client) => {
      if (client && packet.topic === "events/acme") packets.push(packet);
    });

    const client = await AsyncAPIClient.load(mqttDocument(url, true), {
      drivers: [createAsyncAPIMQTTDriver()],
      context: { basic: { username: "sensor", password: "secret" } },
    });
    cleanup.push(async () => client.close());

    const subscription = await client.subscribe<{ id: string }>("observe");
    cleanup.push(async () => {
      await subscription.cancel();
      await subscription.completed.catch(() => undefined);
    });
    await subscription.diagnostics.leading;
    await client.publish("publishQ0", { payload: { id: "evt-0" } });
    await client.publish("publish", { payload: { id: "evt-17" } });
    await client.publish("publishQ2", { payload: { id: "evt-2" } });

    const events = subscription.events[Symbol.asyncIterator]();
    await expect(events.next()).resolves.toEqual({ done: false, value: { value: { id: "evt-0" }, metadata: {} } });
    await expect(events.next()).resolves.toEqual({ done: false, value: { value: { id: "evt-17" }, metadata: {} } });
    await expect(events.next()).resolves.toEqual({ done: false, value: { value: { id: "evt-2" }, metadata: {} } });
    expect(packets).toHaveLength(3);
    expect(packets.map(({ qos, retain }) => ({ qos, retain }))).toEqual([
      { qos: 0, retain: false },
      { qos: 1, retain: true },
      { qos: 2, retain: false },
    ]);

    await subscription.cancel();
    await subscription.completed.catch(() => undefined);
  });

  it("refuses an unresolved MQTT protocol version before constructing a protocol client", async () => {
    let connected = false;
    const document = mqttDocument("mqtt://broker.example.test", false);
    Reflect.deleteProperty(document.servers.production, "protocolVersion");
    const client = await AsyncAPIClient.load(document, {
      drivers: [createAsyncAPIMQTTDriver({
        connector: () => {
          connected = true;
          throw new Error("must not connect");
        },
      })],
    });
    cleanup.push(async () => client.close());

    await expect(client.publish("publish", { payload: { id: "evt-17" } })).rejects.toThrow(/protocolVersion.*3\.1\.1/);
    expect(connected).toBe(false);
  });

  it("refuses MQTT 5 message-binding semantics in the MQTT 3.1.1 profile before connecting", async () => {
    let connected = false;
    const document = mqttDocument("mqtt://broker.example.test", false);
    (document.channels.events.messages.Event as unknown as Record<string, unknown>)["bindings"] = {
      mqtt: { bindingVersion: "0.2.0", responseTopic: "responses" },
    };
    const client = await AsyncAPIClient.load(document, {
      drivers: [createAsyncAPIMQTTDriver({
        connector: () => {
          connected = true;
          throw new Error("must not connect");
        },
      })],
    });
    cleanup.push(async () => client.close());

    await expect(client.publish("publish", { payload: { id: "evt-17" } })).rejects.toThrow(/MQTT 5 message-binding fields/);
    expect(connected).toBe(false);
  });

  it("refuses unqualified persistent-session and Last-Will cells before connecting", async () => {
    for (const binding of [
      { cleanSession: false },
      { cleanSession: true, lastWill: { topic: "offline", message: "gone", qos: 1, retain: true } },
    ]) {
      let connected = false;
      const document = mqttDocument("mqtt://broker.example.test", false);
      document.servers.production.bindings.mqtt = {
        ...document.servers.production.bindings.mqtt,
        ...binding,
      };
      const client = await AsyncAPIClient.load(document, {
        drivers: [createAsyncAPIMQTTDriver({
          connector: () => {
            connected = true;
            throw new Error("must not connect");
          },
        })],
      });
      cleanup.push(async () => client.close());
      await expect(client.publish("publish", { payload: { id: "evt-17" } })).rejects.toThrow(/persistent MQTT sessions|Last Will/);
      expect(connected).toBe(false);
    }
  });

  it("refuses undeclared binding fields and invalid MQTT topics before connecting", async () => {
    const cases = [
      {
        mutate(document: ReturnType<typeof mqttDocument>) {
          (document.operations.publish.bindings.mqtt as Record<string, unknown>)["future"] = true;
        },
        invoke: (client: AsyncAPIClient) => client.publish("publish", { payload: { id: "evt-17" } }),
      },
      {
        mutate(document: ReturnType<typeof mqttDocument>) { document.channels.events.address = "events/#"; },
        invoke: (client: AsyncAPIClient) => client.publish("publish", { payload: { id: "evt-17" } }),
      },
      {
        mutate(document: ReturnType<typeof mqttDocument>) { document.channels.events.address = "events/a+"; },
        invoke: async (client: AsyncAPIClient) => {
          const execution = await client.subscribe("observe");
          return execution.completed;
        },
      },
    ];
    for (const test of cases) {
      let connected = false;
      const document = mqttDocument("mqtt://broker.example.test", false);
      test.mutate(document);
      const client = await AsyncAPIClient.load(document, {
        drivers: [createAsyncAPIMQTTDriver({
          connector: () => {
            connected = true;
            throw new Error("must not connect");
          },
        })],
      });
      cleanup.push(async () => client.close());
      await expect(test.invoke(client)).rejects.toThrow(/undeclared fields|wildcard/);
      expect(connected).toBe(false);
    }
  });

  it("never volunteers stored basic credentials when the artifact declares no security", async () => {
    const { url, broker } = await liveBroker();
    let observed: { username?: string; password?: string } | undefined;
    broker.authenticate = (_client, username, password, done) => {
      observed = {
        ...(username === undefined ? {} : { username }),
        ...(password === undefined ? {} : { password: password.toString() }),
      };
      done(null, username === undefined && password === undefined);
    };
    const client = await AsyncAPIClient.load(mqttDocument(url, false), {
      drivers: [createAsyncAPIMQTTDriver()],
      context: { basic: { username: "must-not-leak", password: "must-not-leak" } },
    });
    cleanup.push(async () => client.close());

    await client.publish("publishQ0", { payload: { id: "no-credential-leak" } });
    expect(observed).toEqual({});
  });

  it("preserves delivered output before a later broker connection failure", async () => {
    const { url, broker } = await liveBroker();
    armPartialOutputFailure(broker);
    const document = mqttDocument(url, false);
    document.channels.events.address = "failure/{tenant}";
    const client = await AsyncAPIClient.load(document, {
      drivers: [createAsyncAPIMQTTDriver()],
    });
    cleanup.push(async () => client.close());

    const subscription = await client.subscribe<{ id: string }>("observe");
    const terminal = subscription.completed.catch((error: unknown) => error);
    const events = subscription.events[Symbol.asyncIterator]();
    await expect(events.next()).resolves.toEqual({
      done: false,
      value: { value: { id: "before-disconnect" }, metadata: {} },
    });
    await expect(events.next()).rejects.toThrow(/MQTT connection (?:closed|lost)/i);
    await expect(terminal).resolves.toMatchObject({ message: expect.stringMatching(/MQTT connection (?:closed|lost)/i) });
  });

  async function liveBroker(): Promise<{ url: string; broker: Aedes }> {
    const broker = await Aedes.createBroker();
    const server = createServer(broker.handle);
    await listen(server);
    const address = server.address();
    if (!address || typeof address === "string") throw new Error("broker has no TCP address");
    cleanup.push(() => closeBroker(server, broker));
    return { url: `mqtt://127.0.0.1:${address.port}`, broker };
  }
});

function mqttDocument(serverURL: string, secured: boolean) {
  const target = new URL(serverURL);
  const security = secured ? [{ type: "userPassword" }] : undefined;
  return {
    asyncapi: "3.0.0",
    info: { title: "MQTT conformance", version: "1" },
    defaultContentType: "application/json",
    servers: {
      production: {
        host: target.host,
        protocol: target.protocol.slice(0, -1),
        protocolVersion: "3.1.1",
        ...(security ? { security } : {}),
        bindings: {
          mqtt: {
            clientId: "ob-mqtt-test",
            cleanSession: true,
            keepAlive: 15,
            bindingVersion: "0.2.0",
          },
        },
      },
    },
    channels: {
      events: {
        address: "events/{tenant}",
        parameters: { tenant: { default: "acme" } },
        messages: {
          Event: {
            contentType: "application/json",
            payload: { type: "object", required: ["id"], properties: { id: { type: "string" } } },
          },
        },
      },
    },
    operations: {
      publish: {
        action: "receive" as const,
        channel: { $ref: "#/channels/events" },
        messages: [{ $ref: "#/channels/events/messages/Event" }],
        bindings: { mqtt: { qos: 1, retain: true, bindingVersion: "0.2.0" } },
      },
      publishQ0: {
        action: "receive" as const,
        channel: { $ref: "#/channels/events" },
        messages: [{ $ref: "#/channels/events/messages/Event" }],
        bindings: { mqtt: { qos: 0, retain: false, bindingVersion: "0.2.0" } },
      },
      publishQ2: {
        action: "receive" as const,
        channel: { $ref: "#/channels/events" },
        messages: [{ $ref: "#/channels/events/messages/Event" }],
        bindings: { mqtt: { qos: 2, retain: false, bindingVersion: "0.2.0" } },
      },
      observe: {
        action: "send" as const,
        channel: { $ref: "#/channels/events" },
        messages: [{ $ref: "#/channels/events/messages/Event" }],
        bindings: { mqtt: { qos: 1, bindingVersion: "0.2.0" } },
      },
    },
  };
}

function armPartialOutputFailure(broker: Aedes): void {
  let armed = true;
  broker.on("subscribe", (subscriptions, client) => {
    if (!armed || !subscriptions.some(({ topic }) => topic === "failure/acme")) return;
    armed = false;
    publishFromBroker(broker, "failure/acme", { id: "before-disconnect" })
      .then(() => destroyClientAfterDelivery(client))
      .catch((error: unknown) => client.emit("error", error));
  });
}

function publishFromBroker(broker: Aedes, topic: string, value: unknown): Promise<void> {
  return new Promise((resolve, reject) => {
    broker.publish({
      cmd: "publish",
      topic,
      payload: Buffer.from(JSON.stringify(value)),
      qos: 1,
      dup: false,
      retain: false,
    }, (error) => error ? reject(error) : resolve());
  });
}

function destroyClientAfterDelivery(client: AedesClient): void {
  // The broker publish callback completes after routing. Leave the client
  // enough time to acknowledge the QoS 1 delivery, then simulate an
  // ungraceful transport loss rather than an invocation cancellation.
  setTimeout(() => client.conn.destroy(), 50);
}

function listen(server: Server): Promise<void> {
  return new Promise((resolve, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", () => {
      server.off("error", reject);
      resolve();
    });
  });
}

async function closeBroker(server: Server, broker: Aedes): Promise<void> {
  await new Promise<void>((resolve) => server.close(() => resolve()));
  await new Promise<void>((resolve) => broker.close(resolve));
}
