import { spawn } from "node:child_process";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { once } from "node:events";
import { createInterface } from "node:readline";
import { pathToFileURL } from "node:url";
import { isDeepStrictEqual } from "node:util";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const broker = spawn(process.execPath, [resolve(root, "typescript-mqtt/scripts/broker.mjs")], {
  cwd: root,
  stdio: ["ignore", "pipe", "inherit"],
});
try {
  const lines = createInterface({ input: broker.stdout });
  const line = await Promise.race([
    once(lines, "line").then(([value]) => String(value)),
    once(broker, "exit").then(([code]) => { throw new Error(`MQTT broker exited before readiness (${code})`); }),
  ]);
  if (!line.startsWith("mqtt://")) throw new Error(`unexpected MQTT broker readiness line: ${line}`);
  await run("pnpm", ["--dir", "typescript", "build"], root, {});
  await run("pnpm", ["--dir", "typescript-mqtt", "build"], root, {});
  await run("pnpm", ["--filter", "@openbindings/asyncapi", "build"], resolve(root, "../openbindings-ts"), {});
  await qualifyTypeScriptBridge(line);
  await run("go", ["test", "./mqtt", "-run", "TestLive", "-count=1"], resolve(root, "go"), {
    ASYNCAPI_MQTT_TEST_URL: line,
    GOWORK: "off",
    GOCACHE: "/tmp/openbindings-asyncapi-client-go-cache",
  });
  await run("go", ["test", ".", "-run", "TestLiveOpenBindingsAdapter", "-count=1"], resolve(root, "../openbindings-go/formats/asyncapi"), {
    ASYNCAPI_MQTT_TEST_URL: line,
    GOWORK: resolve(root, "../go.work"),
    GOCACHE: "/tmp/openbindings-asyncapi-client-go-cache",
  });
  console.log("TypeScript and Go MQTT standalone/OpenBindings bridge qualification passed");
} finally {
  broker.kill("SIGTERM");
  await Promise.race([once(broker, "exit"), new Promise((resolveWait) => setTimeout(resolveWait, 5000))]);
}

async function qualifyTypeScriptBridge(brokerURL) {
  const [{ AsyncAPIEngine }, { createAsyncAPIMQTTDriver }, { AsyncAPIInvoker, BINDING_SPEC }] = await Promise.all([
    import(pathToFileURL(resolve(root, "typescript/dist/engine.js"))),
    import(pathToFileURL(resolve(root, "typescript-mqtt/dist/index.js"))),
    import(pathToFileURL(resolve(root, "../openbindings-ts/packages/asyncapi/dist/index.js"))),
  ]);
  const target = new URL(brokerURL);
  const invoker = new AsyncAPIInvoker(new AsyncAPIEngine({ drivers: [createAsyncAPIMQTTDriver()] }));
  try {
    const call = invoker.invokeBinding({
      source: { bindingSpec: BINDING_SPEC, content: mqttFailureArtifact(target.host) },
      ref: "#/operations/observe",
      context: { basic: { username: "sensor", password: "secret" } },
    });
    const terminal = call.closed.catch((error) => error);
    const outputs = call.outputs[Symbol.asyncIterator]();
    const first = await withTimeout(outputs.next(), "TypeScript bridge first MQTT output");
    if (!isDeepStrictEqual(first, { done: false, value: { id: "before-disconnect" } })) {
      throw new Error(`TypeScript bridge lost or changed pre-failure output: ${JSON.stringify(first)}`);
    }
    let outputFailure;
    try {
      await withTimeout(outputs.next(), "TypeScript bridge MQTT terminal");
    } catch (error) {
      outputFailure = error;
    }
    if (!/MQTT connection (?:closed|lost)/i.test(String(outputFailure?.message ?? outputFailure))) {
      throw new Error(`TypeScript bridge did not surface MQTT connection loss: ${String(outputFailure)}`);
    }
    const closedFailure = await withTimeout(terminal, "TypeScript bridge closed rejection");
    if (!/MQTT connection (?:closed|lost)/i.test(String(closedFailure?.message ?? closedFailure))) {
      throw new Error(`TypeScript bridge closed with the wrong failure: ${String(closedFailure)}`);
    }
  } finally {
    invoker.close();
  }
}

function mqttFailureArtifact(host) {
  return {
    asyncapi: "3.0.0",
    info: { title: "MQTT bridge failure qualification", version: "1" },
    defaultContentType: "application/json",
    servers: {
      production: {
        host,
        protocol: "mqtt",
        protocolVersion: "3.1.1",
        security: [{ type: "userPassword" }],
        bindings: { mqtt: { clientId: "ob-mqtt-ts-bridge-test", cleanSession: true, keepAlive: 15, bindingVersion: "0.2.0" } },
      },
    },
    channels: {
      events: {
        address: "failure/{tenant}",
        parameters: { tenant: { default: "acme" } },
        messages: { Event: { contentType: "application/json", payload: { type: "object" } } },
      },
    },
    operations: {
      observe: {
        action: "send",
        channel: { $ref: "#/channels/events" },
        messages: [{ $ref: "#/channels/events/messages/Event" }],
        bindings: { mqtt: { qos: 1, bindingVersion: "0.2.0" } },
      },
    },
  };
}

function withTimeout(promise, label) {
  let timer;
  return Promise.race([
    promise,
    new Promise((_, reject) => { timer = setTimeout(() => reject(new Error(`${label} timed out`)), 5000); }),
  ]).finally(() => clearTimeout(timer));
}

function run(command, args, cwd, environment) {
  return new Promise((resolveRun, rejectRun) => {
    const child = spawn(command, args, { cwd, env: { ...process.env, ...environment }, stdio: "inherit" });
    child.on("error", rejectRun);
    child.on("exit", (code, signal) => {
      if (code === 0) resolveRun();
      else rejectRun(new Error(`${command} ${args.join(" ")} exited with ${code ?? signal}`));
    });
  });
}
