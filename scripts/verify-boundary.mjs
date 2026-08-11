import { readFile, readdir } from "node:fs/promises";

const packageJSON = JSON.parse(await readFile(new URL("../typescript/package.json", import.meta.url), "utf8"));
const dependencies = Object.keys(packageJSON.dependencies ?? {});
const forbiddenDependencies = dependencies.filter((name) => name.startsWith("@openbindings/"));
if (forbiddenDependencies.length > 0) {
  throw new Error(`standalone package has OpenBindings runtime dependencies: ${forbiddenDependencies.join(", ")}`);
}

const mqttPackageJSON = JSON.parse(await readFile(new URL("../typescript-mqtt/package.json", import.meta.url), "utf8"));
const mqttOpenBindingsDependencies = Object.keys(mqttPackageJSON.dependencies ?? {})
  .filter((name) => name.startsWith("@openbindings/") && name !== "@openbindings/asyncapi-client");
if (mqttOpenBindingsDependencies.length > 0) {
  throw new Error(`standalone MQTT package has SDK dependencies: ${mqttOpenBindingsDependencies.join(", ")}`);
}

const kafkaPackageJSON = JSON.parse(await readFile(new URL("../typescript-kafka/package.json", import.meta.url), "utf8"));
const kafkaOpenBindingsDependencies = Object.keys(kafkaPackageJSON.dependencies ?? {})
  .filter((name) => name.startsWith("@openbindings/") && name !== "@openbindings/asyncapi-client");
if (kafkaOpenBindingsDependencies.length > 0) {
  throw new Error(`standalone Kafka package has SDK dependencies: ${kafkaOpenBindingsDependencies.join(", ")}`);
}

const exportedPaths = Object.keys(packageJSON.exports ?? {});
if (exportedPaths.join(",") !== ".,./engine,./analysis,./testing") {
  throw new Error(`unexpected standalone exports: ${exportedPaths.join(", ")}`);
}

for (const declaration of ["index", "engine", "analysis"]) {
  const source = await readFile(new URL(`../typescript/dist/${declaration}.d.ts`, import.meta.url), "utf8");
  for (const forbidden of [
    "BindingInvocationArgs",
    "ContextRequiredDetails",
    "InvocationError",
    "bindingSpec",
    "@openbindings/",
    "OpenBindings Interface",
  ]) {
    if (source.includes(forbidden)) {
      throw new Error(`${declaration}.d.ts leaks OpenBindings concept ${forbidden}`);
    }
  }
}
const mqttDeclaration = await readFile(new URL("../typescript-mqtt/dist/index.d.ts", import.meta.url), "utf8");
for (const forbidden of ["BindingInvocationArgs", "openbindings.asyncapi@", "OpenBindings Interface"]) {
  if (mqttDeclaration.includes(forbidden)) throw new Error(`MQTT declaration leaks OpenBindings concept ${forbidden}`);
}
const kafkaDeclaration = await readFile(new URL("../typescript-kafka/dist/index.d.ts", import.meta.url), "utf8");
for (const forbidden of ["BindingInvocationArgs", "openbindings.asyncapi@", "OpenBindings Interface"]) {
  if (kafkaDeclaration.includes(forbidden)) throw new Error(`Kafka declaration leaks OpenBindings concept ${forbidden}`);
}

const goMod = await readFile(new URL("../go/go.mod", import.meta.url), "utf8");
if (goMod.includes("github.com/openbindings/openbindings-go")) {
  throw new Error("standalone Go module depends on the OpenBindings Go SDK");
}
const goFiles = (await readdir(new URL("../go/", import.meta.url), { recursive: true }))
  .filter((name) => name.endsWith(".go") && !name.endsWith("_test.go"));
for (const name of goFiles) {
  const source = await readFile(new URL(`../go/${name}`, import.meta.url), "utf8");
  for (const forbidden of [
    "github.com/openbindings/openbindings-go",
    "openbindings.asyncapi@",
    "type BindingInvocationArgs",
    "type InvocationError",
  ]) {
    if (source.includes(forbidden)) {
      throw new Error(`standalone Go source ${name} leaks SDK concept ${forbidden}`);
    }
  }
}

console.log("standalone AsyncAPI package boundary verified");
