import { expect, it } from "vitest";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { AsyncAPIClient } from "@openbindings/asyncapi-client";
import { createAsyncAPIKafkaDriver } from "./index.js";

const brokerURL = process.env.ASYNCAPI_KAFKA_TEST_URL;
const topic = process.env.ASYNCAPI_KAFKA_TEST_TOPIC_TS;
const recoveryTopic = process.env.ASYNCAPI_KAFKA_TEST_TOPIC_TS_RECOVERY;
const securityTopic = process.env.ASYNCAPI_KAFKA_TEST_TOPIC_TS_SECURITY;
const container = process.env.ASYNCAPI_KAFKA_TEST_CONTAINER;
const exec = promisify(execFile);

it.skipIf(!brokerURL || !topic)("publishes and consumes ordered application values through a real Kafka broker", async () => {
  const target = new URL(brokerURL as string);
  const client = await AsyncAPIClient.load(kafkaLiveDocument(target.host, topic as string), {
    drivers: [createAsyncAPIKafkaDriver({ fromBeginning: true })],
  });
  try {
    for (const id of ["ts-1", "ts-2", "ts-3"]) {
      await client.publish("publish", { id });
    }
    const subscription = await client.subscribe<{ id: string }>("observe");
    const events = subscription.events[Symbol.asyncIterator]();
    for (const id of ["ts-1", "ts-2", "ts-3"]) {
      await expect(withTimeout(events.next())).resolves.toMatchObject({
        done: false,
        value: { value: { id } },
      });
    }
    subscription.cancel();
    await expect(subscription.completed).rejects.toThrow(/cancelled/);
  } finally {
    client.close();
  }
}, 20_000);

it.skipIf(!brokerURL || !recoveryTopic || !container)("preserves output ordering across a transient real broker loss", async () => {
  const target = new URL(brokerURL as string);
  const client = await AsyncAPIClient.load(kafkaLiveDocument(target.host, recoveryTopic as string), {
    drivers: [createAsyncAPIKafkaDriver({ fromBeginning: true })],
  });
  let paused = false;
  try {
    await client.publish("publish", { id: "before-loss" });
    const subscription = await client.subscribe<{ id: string }>("observe");
    const events = subscription.events[Symbol.asyncIterator]();
    await expect(withTimeout(events.next())).resolves.toMatchObject({
      done: false,
      value: { value: { id: "before-loss" } },
    });

    await exec("docker", ["pause", container as string]);
    paused = true;
    await new Promise((resolveWait) => setTimeout(resolveWait, 1_000));
    await exec("docker", ["unpause", container as string]);
    paused = false;

    await client.publish("publish", { id: "after-recovery" });
    await expect(withTimeout(events.next())).resolves.toMatchObject({
      done: false,
      value: { value: { id: "after-recovery" } },
    });
    subscription.cancel();
    await subscription.completed.catch(() => undefined);
  } finally {
    if (paused) await exec("docker", ["unpause", container as string]).catch(() => undefined);
    client.close();
  }
}, 30_000);

it.skipIf(!brokerURL || !securityTopic)("applies an authored SCRAM requirement through abstract username/password context", async () => {
  const target = new URL(brokerURL as string);
  const document = kafkaLiveDocument(target.host, securityTopic as string);
  document.servers.production.security = [{ type: "scramSha256" }];
  const client = await AsyncAPIClient.load(document, {
    drivers: [createAsyncAPIKafkaDriver({ fromBeginning: true })],
    context: { basic: { username: "orders", password: "secret-password" } },
  });
  try {
    await client.publish("publish", { id: "secured-ts" });
    const subscription = await client.subscribe<{ id: string }>("observe");
    const events = subscription.events[Symbol.asyncIterator]();
    await expect(withTimeout(events.next())).resolves.toMatchObject({
      done: false,
      value: { value: { id: "secured-ts" } },
    });
    subscription.cancel();
    await subscription.completed.catch(() => undefined);
  } finally {
    client.close();
  }
}, 20_000);

function kafkaLiveDocument(host: string, selectedTopic: string) {
  return {
    asyncapi: "3.0.0",
    info: { title: "Kafka TypeScript live qualification", version: "1" },
    defaultContentType: "application/json",
    servers: {
      production: {
        host,
        protocol: "kafka",
        bindings: { kafka: { bindingVersion: "0.5.0" } },
        security: undefined as undefined | Array<{ type: string }>,
      },
    },
    channels: {
      events: {
        address: "orders/{tenant}",
        parameters: { tenant: { default: "acme" } },
        bindings: {
          kafka: { topic: selectedTopic, partitions: 3, replicas: 1, bindingVersion: "0.5.0" },
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
        channel: { $ref: "#/channels/events" },
        messages: [{ $ref: "#/channels/events/messages/Event" }],
        bindings: { kafka: { clientId: { type: "string", const: "ob-kafka-ts-producer" }, bindingVersion: "0.5.0" } },
      },
      observe: {
        action: "send" as const,
        channel: { $ref: "#/channels/events" },
        messages: [{ $ref: "#/channels/events/messages/Event" }],
        bindings: {
          kafka: {
            clientId: { type: "string", const: "ob-kafka-ts-consumer" },
            groupId: { type: "string", const: `ob-kafka-ts-${process.pid}` },
            bindingVersion: "0.5.0",
          },
        },
      },
    },
  };
}

function withTimeout<T>(promise: Promise<T>): Promise<T> {
  let timer: ReturnType<typeof setTimeout>;
  return Promise.race([
    promise,
    new Promise<never>((_resolve, reject) => {
      timer = setTimeout(() => reject(new Error("Kafka live output timed out")), 10_000);
    }),
  ]).finally(() => clearTimeout(timer));
}
