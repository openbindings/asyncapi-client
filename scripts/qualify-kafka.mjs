import { spawn } from "node:child_process";
import { createServer } from "node:net";
import { randomUUID } from "node:crypto";
import { dirname, resolve } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import { isDeepStrictEqual } from "node:util";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const image = process.env.ASYNCAPI_KAFKA_TEST_IMAGE
  ?? "docker.redpanda.com/redpandadata/redpanda:v26.1.13";
const suffix = randomUUID().replaceAll("-", "").slice(0, 12);
const container = `ob-kafka-${suffix}`;
const port = await freePort();
const topics = {
  ts: `ob-kafka-ts-${suffix}`,
  go: `ob-kafka-go-${suffix}`,
  tsRecovery: `ob-kafka-ts-recovery-${suffix}`,
  goRecovery: `ob-kafka-go-recovery-${suffix}`,
  tsBridge: `ob-kafka-ts-bridge-${suffix}`,
  goBridge: `ob-kafka-go-bridge-${suffix}`,
  tsSecurity: `ob-kafka-ts-security-${suffix}`,
  goSecurity: `ob-kafka-go-security-${suffix}`,
  tsBridgeSecurity: `ob-kafka-ts-bridge-security-${suffix}`,
  goBridgeSecurity: `ob-kafka-go-bridge-security-${suffix}`,
};

let started = false;
try {
  await run("docker", [
    "run", "--rm", "-d",
    "--name", container,
    "--hostname", container,
    "-p", `127.0.0.1:${port}:19092`,
    image,
    "redpanda", "start",
    "--mode", "dev-container",
    "--smp", "1",
    "--memory", "512M",
    "--reserve-memory", "0M",
    "--node-id", "0",
    "--check=false",
    "--kafka-addr", "internal://0.0.0.0:9092,external://0.0.0.0:19092",
    "--advertise-kafka-addr", `internal://${container}:9092,external://127.0.0.1:${port}`,
  ], root, {});
  started = true;
  await waitForBroker(container);
  await run("docker", [
    "exec", container, "rpk", "topic", "create",
    topics.ts, topics.go, topics.tsRecovery, topics.goRecovery, topics.tsBridge, topics.goBridge,
    topics.tsSecurity, topics.goSecurity, topics.tsBridgeSecurity, topics.goBridgeSecurity,
    "--partitions", "3", "--replicas", "1", "-X", "brokers=localhost:9092",
  ], root, {});

  const environment = {
    ASYNCAPI_KAFKA_TEST_URL: `kafka://127.0.0.1:${port}`,
    ASYNCAPI_KAFKA_TEST_TOPIC_TS: topics.ts,
    ASYNCAPI_KAFKA_TEST_TOPIC_GO: topics.go,
    ASYNCAPI_KAFKA_TEST_TOPIC_TS_RECOVERY: topics.tsRecovery,
    ASYNCAPI_KAFKA_TEST_TOPIC_GO_RECOVERY: topics.goRecovery,
    ASYNCAPI_KAFKA_TEST_TOPIC_TS_BRIDGE: topics.tsBridge,
    ASYNCAPI_KAFKA_TEST_TOPIC_GO_BRIDGE: topics.goBridge,
    ASYNCAPI_KAFKA_TEST_CONTAINER: container,
  };
  await run("pnpm", [
    "--dir", "typescript-kafka", "exec", "vitest", "run", "src/driver_live.test.ts",
    "-t", "publishes and consumes|preserves output ordering",
  ], root, environment);
  await run("go", ["test", "./kafka", "-run", "TestLive(PublishSubscribe|Transient)", "-count=1"], resolve(root, "go"), {
    ...environment,
    GOWORK: "off",
    GOCACHE: "/tmp/openbindings-asyncapi-client-go-cache",
  });

  await run("pnpm", ["--dir", "typescript", "build"], root, {});
  await run("pnpm", ["--dir", "typescript-kafka", "build"], root, {});
  await run("pnpm", ["--filter", "@openbindings/asyncapi", "build"], resolve(root, "../openbindings-ts"), {});
  await qualifyTypeScriptBridge(environment.ASYNCAPI_KAFKA_TEST_URL, topics.tsBridge, false);
  await run("go", ["test", ".", "-run", "TestLiveOpenBindingsAdapterUsesKafkaDriver", "-count=1"], resolve(root, "../openbindings-go/formats/asyncapi"), {
    ...environment,
    GOWORK: resolve(root, "../go.work"),
    GOCACHE: "/tmp/openbindings-asyncapi-client-go-cache",
  });

  await run("docker", [
    "exec", container, "rpk", "security", "user", "create", "orders",
    "-p", "secret-password", "--mechanism", "SCRAM-SHA-256",
  ], root, {});
  await run("docker", [
    "exec", container, "rpk", "security", "acl", "create",
    "--allow-principal", "User:orders", "--operation", "all",
    "--topic", [topics.tsSecurity, topics.goSecurity, topics.tsBridgeSecurity, topics.goBridgeSecurity].join(","),
    "--group", "*", "--cluster",
    "-X", "brokers=localhost:9092",
  ], root, {});
  await run("docker", [
    "exec", container, "rpk", "cluster", "config", "set", "enable_sasl", "true",
  ], root, {});
  await waitForSecureBroker(container);
  const securityEnvironment = {
    ...environment,
    ASYNCAPI_KAFKA_TEST_TOPIC_TS_SECURITY: topics.tsSecurity,
    ASYNCAPI_KAFKA_TEST_TOPIC_GO_SECURITY: topics.goSecurity,
    ASYNCAPI_KAFKA_TEST_TOPIC_TS_BRIDGE_SECURITY: topics.tsBridgeSecurity,
    ASYNCAPI_KAFKA_TEST_TOPIC_GO_BRIDGE_SECURITY: topics.goBridgeSecurity,
  };
  await run("pnpm", [
    "--dir", "typescript-kafka", "exec", "vitest", "run", "src/driver_live.test.ts",
    "-t", "applies an authored SCRAM requirement",
  ], root, securityEnvironment);
  await run("go", ["test", "./kafka", "-run", "TestLiveSCRAMUsesAbstractBasicContext", "-count=1"], resolve(root, "go"), {
    ...securityEnvironment,
    GOWORK: "off",
    GOCACHE: "/tmp/openbindings-asyncapi-client-go-cache",
  });
  await qualifyTypeScriptBridge(environment.ASYNCAPI_KAFKA_TEST_URL, topics.tsBridgeSecurity, true);
  await run("go", ["test", ".", "-run", "TestLiveOpenBindingsAdapterUsesKafkaSCRAMWithoutProtocolFields", "-count=1"], resolve(root, "../openbindings-go/formats/asyncapi"), {
    ...securityEnvironment,
    GOWORK: resolve(root, "../go.work"),
    GOCACHE: "/tmp/openbindings-asyncapi-client-go-cache",
  });
  console.log("TypeScript and Go Kafka standalone/OpenBindings bridge qualification passed");
} finally {
  if (started) {
    await run("docker", ["rm", "-f", container], root, {}, true);
  }
}

async function qualifyTypeScriptBridge(brokerURL, topic, secured) {
  const [{ AsyncAPIEngine }, { createAsyncAPIKafkaDriver }, { AsyncAPIInvoker, BINDING_SPEC }] = await Promise.all([
    import(pathToFileURL(resolve(root, "typescript/dist/engine.js"))),
    import(pathToFileURL(resolve(root, "typescript-kafka/dist/index.js"))),
    import(pathToFileURL(resolve(root, "../openbindings-ts/packages/asyncapi/dist/index.js"))),
  ]);
  const target = new URL(brokerURL);
  const artifact = kafkaArtifact(target.host, topic, `ob-kafka-ts-bridge-${process.pid}`, secured);
  const invoker = new AsyncAPIInvoker(new AsyncAPIEngine({
    drivers: [createAsyncAPIKafkaDriver({ fromBeginning: true })],
  }));
  try {
    const publish = invoker.invokeBinding({
      source: { bindingSpec: BINDING_SPEC, content: artifact },
      ref: "#/operations/publish",
      ...(secured ? { context: { basic: { username: "orders", password: "secret-password" } } } : {}),
    });
    await publish.write({ id: "through-openbindings-kafka" });
    await publish.close();
    const publishedOutputs = [];
    for await (const output of publish.outputs) publishedOutputs.push(output);
    await publish.closed;
    if (publishedOutputs.length !== 0) {
      throw new Error(`Kafka publish unexpectedly produced OpenBindings values: ${JSON.stringify(publishedOutputs)}`);
    }

    const subscribe = invoker.invokeBinding({
      source: { bindingSpec: BINDING_SPEC, content: artifact },
      ref: "#/operations/observe",
      ...(secured ? { context: { basic: { username: "orders", password: "secret-password" } } } : {}),
    });
    const outputs = subscribe.outputs[Symbol.asyncIterator]();
    const first = await withTimeout(outputs.next(), "TypeScript Kafka bridge output");
    if (!isDeepStrictEqual(first, { done: false, value: { id: "through-openbindings-kafka" } })) {
      throw new Error(`TypeScript Kafka bridge changed the application value: ${JSON.stringify(first)}`);
    }
    await subscribe.cancel();
    await subscribe.closed.catch(() => undefined);
  } finally {
    invoker.close();
  }
}

function kafkaArtifact(host, topic, groupId, secured) {
  return {
    asyncapi: "3.0.0",
    info: { title: "Kafka TypeScript bridge qualification", version: "1" },
    defaultContentType: "application/json",
    servers: {
      production: {
        host,
        protocol: "kafka",
        ...(secured ? { security: [{ type: "scramSha256" }] } : {}),
        bindings: { kafka: { bindingVersion: "0.5.0" } },
      },
    },
    channels: {
      events: {
        address: "orders/{tenant}",
        parameters: { tenant: { default: "acme" } },
        bindings: { kafka: { topic, partitions: 3, replicas: 1, bindingVersion: "0.5.0" } },
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
        action: "receive",
        channel: { $ref: "#/channels/events" },
        messages: [{ $ref: "#/channels/events/messages/Event" }],
        bindings: { kafka: { clientId: { type: "string", const: "ob-kafka-ts-bridge-producer" }, bindingVersion: "0.5.0" } },
      },
      observe: {
        action: "send",
        channel: { $ref: "#/channels/events" },
        messages: [{ $ref: "#/channels/events/messages/Event" }],
        bindings: {
          kafka: {
            clientId: { type: "string", const: "ob-kafka-ts-bridge-consumer" },
            groupId: { type: "string", const: groupId },
            bindingVersion: "0.5.0",
          },
        },
      },
    },
  };
}

async function waitForBroker(name) {
  const deadline = Date.now() + 60_000;
  while (Date.now() < deadline) {
    const result = await run("docker", [
      "exec", name, "rpk", "cluster", "health", "--exit-when-healthy", "-X", "brokers=localhost:9092",
    ], root, {}, true);
    if (result === 0) return;
    await new Promise((resolveWait) => setTimeout(resolveWait, 500));
  }
  throw new Error("Kafka broker did not become healthy within 60 seconds");
}

async function waitForSecureBroker(name) {
  const deadline = Date.now() + 30_000;
  while (Date.now() < deadline) {
    const result = await run("docker", [
      "exec", name, "rpk", "cluster", "health", "--exit-when-healthy",
      "-X", "brokers=localhost:9092", "-X", "user=orders", "-X", "pass=secret-password",
      "-X", "sasl.mechanism=SCRAM-SHA-256",
    ], root, {}, true);
    if (result === 0) return;
    await new Promise((resolveWait) => setTimeout(resolveWait, 500));
  }
  throw new Error("SASL-enabled Kafka broker did not become healthy within 30 seconds");
}

function freePort() {
  return new Promise((resolvePort, reject) => {
    const server = createServer();
    server.once("error", reject);
    server.listen(0, "127.0.0.1", () => {
      const address = server.address();
      if (!address || typeof address === "string") {
        server.close();
        reject(new Error("failed to reserve a Kafka test port"));
        return;
      }
      server.close((error) => error ? reject(error) : resolvePort(address.port));
    });
  });
}

function withTimeout(promise, label) {
  let timer;
  return Promise.race([
    promise,
    new Promise((_, reject) => { timer = setTimeout(() => reject(new Error(`${label} timed out`)), 10_000); }),
  ]).finally(() => clearTimeout(timer));
}

function run(command, args, cwd, environment, allowFailure = false) {
  return new Promise((resolveRun, rejectRun) => {
    const child = spawn(command, args, { cwd, env: { ...process.env, ...environment }, stdio: allowFailure ? "ignore" : "inherit" });
    child.on("error", rejectRun);
    child.on("exit", (code, signal) => {
      if (code === 0 || allowFailure) resolveRun(code ?? 1);
      else rejectRun(new Error(`${command} ${args.join(" ")} exited with ${code ?? signal}`));
    });
  });
}
