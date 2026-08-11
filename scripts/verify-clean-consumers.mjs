import assert from "node:assert/strict";
import { mkdtemp, mkdir, readFile, readdir, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { spawn } from "node:child_process";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const temporaryRoot = await mkdtemp(join(tmpdir(), "openbindings-asyncapi-client-release-"));

const document = JSON.stringify({
  asyncapi: "3.0.0",
  info: { title: "clean consumer", version: "1" },
  servers: { production: { host: "api.example.test", protocol: "https" } },
  channels: { ping: { address: "/ping", messages: { Ping: { payload: { type: "object" } } } } },
  operations: {
    ping: {
      action: "receive",
      channel: { $ref: "#/channels/ping" },
      messages: [{ $ref: "#/channels/ping/messages/Ping" }],
      bindings: { http: { method: "POST" } },
    },
  },
});

try {
  const packageDirectory = join(temporaryRoot, "package");
  const typeScriptConsumer = join(temporaryRoot, "typescript-consumer");
  const goConsumer = join(temporaryRoot, "go-consumer");
  await Promise.all([mkdir(packageDirectory), mkdir(typeScriptConsumer), mkdir(goConsumer)]);
  const npmEnvironment = { npm_config_cache: join(temporaryRoot, "npm-cache") };

  await run("npm", ["pack", "--json", "--pack-destination", packageDirectory], join(root, "typescript"), npmEnvironment);
  const archives = (await readdir(packageDirectory)).filter((name) => name.endsWith(".tgz"));
  assert.equal(archives.length, 1, `expected one npm archive, got ${archives.join(", ")}`);
  const archive = join(packageDirectory, archives[0]);

  await writeFile(join(typeScriptConsumer, "package.json"), `${JSON.stringify({
    name: "asyncapi-client-release-consumer", private: true, type: "module",
  }, null, 2)}\n`);
  await run("npm", ["install", "--ignore-scripts", "--no-audit", "--no-fund", "--no-package-lock", archive], typeScriptConsumer, npmEnvironment);

  await writeFile(join(typeScriptConsumer, "esm.mjs"), `
import assert from "node:assert/strict";
import { AsyncAPIClient } from "@openbindings/asyncapi-client";
const client = await AsyncAPIClient.load(${JSON.stringify(document)});
assert.deepEqual(client.operations().map(({ key }) => key), ["ping"]);
client.close();
`);
  await writeFile(join(typeScriptConsumer, "cjs.cjs"), `
const assert = require("node:assert/strict");
const { AsyncAPIClient } = require("@openbindings/asyncapi-client");
(async () => {
  const client = await AsyncAPIClient.load(${JSON.stringify(document)});
  assert.deepEqual(client.operations().map(({ key }) => key), ["ping"]);
  client.close();
})().catch((error) => { console.error(error); process.exitCode = 1; });
`);
  await run(process.execPath, ["esm.mjs"], typeScriptConsumer);
  await run(process.execPath, ["cjs.cjs"], typeScriptConsumer);

  const goModule = resolve(root, "go").replaceAll("\\", "/");
  await writeFile(join(goConsumer, "go.mod"), `module releaseconsumer

go 1.25.12

require github.com/openbindings/asyncapi-client/go v0.0.0

replace github.com/openbindings/asyncapi-client/go => ${goModule}
`);
  await writeFile(join(goConsumer, "client_test.go"), `package releaseconsumer

import (
  "context"
  "testing"
  asyncapiclient "github.com/openbindings/asyncapi-client/go"
)

func TestCleanConsumer(t *testing.T) {
  client, err := asyncapiclient.Load(context.Background(), asyncapiclient.Source{Content: []byte(${JSON.stringify(document)})}, asyncapiclient.LoadOptions{})
  if err != nil { t.Fatal(err) }
  defer client.Close()
  operations := client.Operations()
  if len(operations) != 1 || operations[0].ID != "ping" { t.Fatalf("operations = %#v", operations) }
}
`);
  const goEnvironment = { GOWORK: "off", GOCACHE: join(temporaryRoot, "go-cache") };
  await run("go", ["mod", "tidy"], goConsumer, goEnvironment);
  await run("go", ["test", "./..."], goConsumer, goEnvironment);

  const manifest = JSON.parse(await readFile(join(typeScriptConsumer, "node_modules", "@openbindings", "asyncapi-client", "package.json"), "utf8"));
  assert.equal(manifest.name, "@openbindings/asyncapi-client");
  assert.deepEqual(Object.keys(manifest.exports), [".", "./engine", "./analysis", "./testing"]);
  console.log("clean AsyncAPI TypeScript ESM/CJS and Go consumers verified");
} finally {
  const expectedPrefix = join(tmpdir(), "openbindings-asyncapi-client-release-");
  if (!temporaryRoot.startsWith(expectedPrefix)) {
    throw new Error(`refusing to clean unexpected path ${temporaryRoot}`);
  }
  await rm(temporaryRoot, { recursive: true });
}

function run(command, args, cwd, environment = {}) {
  return new Promise((resolveRun, rejectRun) => {
    const child = spawn(command, args, { cwd, env: { ...process.env, ...environment }, stdio: "inherit" });
    child.on("error", rejectRun);
    child.on("exit", (code, signal) => {
      if (code === 0) resolveRun();
      else rejectRun(new Error(`${command} ${args.join(" ")} exited with ${code ?? signal}`));
    });
  });
}
