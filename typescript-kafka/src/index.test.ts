import { afterEach, describe, expect, it } from "vitest";
import { AsyncAPIClient } from "@openbindings/asyncapi-client";
import {
  createAsyncAPIKafkaDriver,
  type KafkaClient,
  type KafkaClientFactory,
  type KafkaConnectionConfig,
  type KafkaConsumer,
  type KafkaConsumerMessage,
  type KafkaProducer,
} from "./index.js";

describe("AsyncAPI Kafka protocol driver", () => {
  const cleanup: Array<() => void | Promise<void>> = [];
  afterEach(async () => {
    while (cleanup.length > 0) await cleanup.pop()?.();
  });

  // Kafka record-header carriage (§9.2 per-cell capability; Go twin:
  // TestKafkaCarriesRecordHeadersBothDirections): the routed envelope's
  // application headers ride the unit seam as record headers on publish,
  // and received record headers project into the output envelope on
  // subscribe — declared properties only, undeclared pairs never leak.
  it("carries record headers both directions through the unit seam", async () => {
    const document = kafkaDocument();
    (document.channels.orders.messages.Event as Record<string, unknown>).headers = {
      type: "object",
      properties: { traceId: { type: "string" }, attempt: { type: "integer" } },
    };
    const factory = new FakeFactory();
    const client = await AsyncAPIClient.load(document, {
      drivers: [createAsyncAPIKafkaDriver({ clientFactory: factory })],
    });
    cleanup.push(() => client.close());
    await client.publish("publish", {
      payload: { id: "evt-1" },
      headers: { traceId: "t-7", attempt: 2 },
    });
    const carried = Object.fromEntries((factory.sent[0]?.headers ?? []).map(({ key, value }) => [key, text(value)]));
    expect(carried).toEqual({ traceId: "t-7", attempt: "2" });
    expect(text(factory.sent[0]?.value)).toBe(JSON.stringify({ id: "evt-1" }));
  });

  it("maps authored topic, client identity, group, key, codec, and ordering into the Kafka engine", async () => {
    const factory = new FakeFactory();
    const client = await AsyncAPIClient.load(kafkaDocument(), {
      drivers: [createAsyncAPIKafkaDriver({ clientFactory: factory })],
      context: { configuration: { kafka: { fromBeginning: true } } },
    });
    cleanup.push(() => client.close());

    await client.publish("publish", { payload: { id: "evt-1" } });
    await client.publish("publish", { payload: { id: "evt-2" } });
    expect(factory.configs).toEqual([
      { brokers: ["broker.example.test:9092"], clientId: "orders-client" },
      { brokers: ["broker.example.test:9092"], clientId: "orders-client" },
    ]);
    expect(factory.sent.map(({ topic, key, value }) => ({
      topic,
      key: text(key),
      value: JSON.parse(text(value)),
    }))).toEqual([
      { topic: "orders.v1", key: "tenant-a", value: { id: "evt-1" } },
      { topic: "orders.v1", key: "tenant-a", value: { id: "evt-2" } },
    ]);

    factory.consumerMessages = [
      { key: bytes("tenant-a"), value: bytes('{"id":"evt-3"}') },
      { key: bytes("tenant-a"), value: bytes('{"id":"evt-4"}') },
    ];
    const subscription = await client.subscribe<{ id: string }>("observe");
    const events = subscription.events[Symbol.asyncIterator]();
    await expect(events.next()).resolves.toMatchObject({ done: false, value: { value: { id: "evt-3" } } });
    await expect(events.next()).resolves.toMatchObject({ done: false, value: { value: { id: "evt-4" } } });
    subscription.cancel();
    await expect(subscription.completed).rejects.toThrow(/cancelled/);
    expect(factory.consumerOptions).toEqual([{ groupId: "orders-workers", fromBeginning: true }]);
    expect(factory.subscribed).toEqual(["orders.v1"]);
  });

  it("selects only artifact-declared SASL alternatives and never volunteers stored credentials", async () => {
    const factory = new FakeFactory();
    const unsecured = kafkaDocument();
    const client = await AsyncAPIClient.load(unsecured, {
      drivers: [createAsyncAPIKafkaDriver({ clientFactory: factory })],
      context: { basic: { username: "must-not-leak", password: "must-not-leak" } },
    });
    cleanup.push(() => client.close());
    await client.publish("publish", { payload: { id: "evt" } });
    expect(factory.configs[0]?.sasl).toBeUndefined();

    const secured = kafkaDocument();
    secured.servers.production.security = [{ type: "userPassword" }];
    const securedFactory = new FakeFactory();
    const securedClient = await AsyncAPIClient.load(secured, {
      drivers: [createAsyncAPIKafkaDriver({ clientFactory: securedFactory })],
      context: { basic: { username: "orders", password: "secret" } },
    });
    cleanup.push(() => securedClient.close());
    await securedClient.publish("publish", { payload: { id: "evt" } });
    expect(securedFactory.configs[0]?.sasl).toEqual({
      mechanism: "plain",
      username: "orders",
      password: "secret",
    });
  });

  it("completes non-singleton identity and key schemas only from explicit Kafka configuration", async () => {
    const document = kafkaDocument();
    document.operations.publish.bindings.kafka.clientId = { type: "string", pattern: "^cfg-" } as never;
    document.operations.observe.bindings.kafka.clientId = { type: "string", pattern: "^cfg-" } as never;
    document.operations.observe.bindings.kafka.groupId = { type: "string", pattern: "^group-" } as never;
    document.channels.orders.messages.Event.bindings.kafka.key = { type: "string", pattern: "^key-" } as never;
    const factory = new FakeFactory();
    const client = await AsyncAPIClient.load(document, {
      drivers: [createAsyncAPIKafkaDriver({ clientFactory: factory })],
      context: { configuration: { kafka: { clientId: "cfg-client", groupId: "group-workers", key: "key-tenant" } } },
    });
    cleanup.push(() => client.close());
    await client.publish("publish", { payload: { id: "configured" } });
    expect(factory.configs[0]?.clientId).toBe("cfg-client");
    expect(text(factory.sent[0]?.key)).toBe("key-tenant");
    factory.consumerMessages = [{ key: bytes("key-tenant"), value: bytes('{"id":"configured"}') }];
    const subscription = await client.subscribe("observe");
    await expect(subscription.events[Symbol.asyncIterator]().next()).resolves.toMatchObject({ done: false });
    subscription.cancel();
    await subscription.completed.catch(() => undefined);
    expect(factory.consumerOptions[0]?.groupId).toBe("group-workers");
  });

  it("refuses unsupported and ambiguous cells before constructing a Kafka client", async () => {
    const cases: Array<{ name: string; mutate(document: ReturnType<typeof kafkaDocument>): void; error: RegExp }> = [
      {
        name: "future binding revision",
        mutate(document) { document.operations.publish.bindings.kafka.bindingVersion = "9.0.0"; },
        error: /outside the 0\.1\.0-0\.5\.0/,
      },
      {
        name: "unknown channel field",
        mutate(document) { (document.channels.orders.bindings.kafka as Record<string, unknown>).future = true; },
        error: /undeclared fields/,
      },
      {
        name: "pre-0.4 topic configuration",
        mutate(document) { document.channels.orders.bindings.kafka.bindingVersion = "0.3.0"; },
        error: /predates topicConfiguration/,
      },
      {
        name: "Schema Registry framing",
        mutate(document) { (document.channels.orders.messages.Event.bindings.kafka as Record<string, unknown>).schemaIdLocation = "header"; },
        error: /Schema Registry/,
      },
      {
        name: "dynamic key without completion",
        mutate(document) { (document.channels.orders.messages.Event.bindings.kafka as Record<string, unknown>).key = { type: "string" }; },
        error: /does not select one value/,
      },
      {
        name: "invalid topic override",
        mutate(document) { document.channels.orders.bindings.kafka.topic = "orders/wild"; },
        error: /Kafka topic/,
      },
    ];
    for (const test of cases) {
      const document = kafkaDocument();
      test.mutate(document);
      const factory = new FakeFactory();
      const client = await AsyncAPIClient.load(document, {
        drivers: [createAsyncAPIKafkaDriver({ clientFactory: factory })],
      });
      cleanup.push(() => client.close());
      await expect(client.publish("publish", { payload: { id: test.name } })).rejects.toThrow(test.error);
      expect(factory.configs, test.name).toHaveLength(0);
    }
  });

  it("preserves output before a later consumer failure and validates authored keys", async () => {
    const factory = new FakeFactory();
    factory.consumerMessages = [{ key: bytes("tenant-a"), value: bytes('{"id":"before-failure"}') }];
    factory.consumerFailure = new Error("Kafka broker connection lost");
    const client = await AsyncAPIClient.load(kafkaDocument(), {
      drivers: [createAsyncAPIKafkaDriver({ clientFactory: factory })],
    });
    cleanup.push(() => client.close());

    const subscription = await client.subscribe<{ id: string }>("observe");
    const terminal = subscription.completed.catch((error: unknown) => error);
    const events = subscription.events[Symbol.asyncIterator]();
    await expect(events.next()).resolves.toMatchObject({ done: false, value: { value: { id: "before-failure" } } });
    await expect(events.next()).rejects.toThrow(/Kafka broker connection lost/);
    await expect(terminal).resolves.toMatchObject({ message: "Kafka broker connection lost" });

    const mismatchFactory = new FakeFactory();
    mismatchFactory.consumerMessages = [{ key: bytes("tenant-b"), value: bytes('{"id":"wrong-key"}') }];
    const mismatchClient = await AsyncAPIClient.load(kafkaDocument(), {
      drivers: [createAsyncAPIKafkaDriver({ clientFactory: mismatchFactory })],
    });
    cleanup.push(() => mismatchClient.close());
    const mismatch = await mismatchClient.subscribe("observe");
    await expect(mismatch.completed).rejects.toThrow(/does not match the authored message key/);
  });
});

class FakeFactory implements KafkaClientFactory {
  configs: KafkaConnectionConfig[] = [];
  sent: Array<{ topic: string; value: Uint8Array; key?: Uint8Array; headers?: readonly { key: string; value: Uint8Array }[] }> = [];
  subscribed: string[] = [];
  consumerOptions: Array<{ groupId: string; fromBeginning: boolean }> = [];
  consumerMessages: KafkaConsumerMessage[] = [];
  consumerFailure?: Error;

  create(config: KafkaConnectionConfig): KafkaClient {
    this.configs.push({ ...config, brokers: [...config.brokers] });
    return {
      producer: () => this.producer(),
      consumer: (options) => this.consumer(options),
    };
  }

  private producer(): KafkaProducer {
    return {
      connect: async () => undefined,
      send: async ({ topic, messages }) => {
        this.sent.push(...messages.map(({ value, key, headers }) => ({
          ...(headers ? { headers } : {}),
          topic,
          value: new Uint8Array(value),
          ...(key ? { key: new Uint8Array(key) } : {}),
        })));
      },
      disconnect: async () => undefined,
    };
  }

  private consumer(options: { groupId: string; fromBeginning: boolean }): KafkaConsumer {
    this.consumerOptions.push(options);
    return {
      connect: async () => undefined,
      subscribe: async ({ topic }) => { this.subscribed.push(topic); },
      run: async ({ eachMessage }) => {
        for (const message of this.consumerMessages) await eachMessage(message);
        if (this.consumerFailure) throw this.consumerFailure;
      },
      stop: async () => undefined,
      disconnect: async () => undefined,
    };
  }
}

function kafkaDocument() {
  return {
    asyncapi: "3.0.0",
    info: { title: "Kafka conformance", version: "1" },
    defaultContentType: "application/json",
    servers: {
      production: {
        host: "broker.example.test:9092",
        protocol: "kafka",
        bindings: { kafka: { bindingVersion: "0.5.0" } },
        security: undefined as undefined | Array<{ type: string }>,
      },
    },
    channels: {
      orders: {
        address: "orders/{tenant}",
        parameters: { tenant: { default: "acme" } },
        bindings: {
          kafka: {
            topic: "orders.v1",
            partitions: 3,
            replicas: 1,
            topicConfiguration: { "cleanup.policy": "delete", "retention.ms": 86_400_000 },
            bindingVersion: "0.5.0",
          },
        },
        messages: {
          Event: {
            contentType: "application/json",
            payload: { type: "object", required: ["id"], properties: { id: { type: "string" } } },
            bindings: { kafka: { key: { type: "string", const: "tenant-a" }, bindingVersion: "0.5.0" } },
          },
        },
      },
    },
    operations: {
      publish: {
        action: "receive" as const,
        channel: { $ref: "#/channels/orders" },
        messages: [{ $ref: "#/channels/orders/messages/Event" }],
        bindings: {
          kafka: {
            clientId: { type: "string", const: "orders-client" },
            bindingVersion: "0.5.0",
          },
        },
      },
      observe: {
        action: "send" as const,
        channel: { $ref: "#/channels/orders" },
        messages: [{ $ref: "#/channels/orders/messages/Event" }],
        bindings: {
          kafka: {
            clientId: { type: "string", const: "orders-client" },
            groupId: { type: "string", const: "orders-workers" },
            bindingVersion: "0.5.0",
          },
        },
      },
    },
  };
}

function bytes(value: string): Uint8Array { return new TextEncoder().encode(value); }
function text(value: Uint8Array | undefined): string { return value ? new TextDecoder().decode(value) : ""; }
