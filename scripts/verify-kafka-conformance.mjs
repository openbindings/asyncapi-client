import assert from "node:assert/strict";
import { access, readFile } from "node:fs/promises";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const matrix = JSON.parse(await readFile(resolve(root, "conformance/kafka.json"), "utf8"));
assert.equal(matrix.format, "openbindings.asyncapi.protocol-driver-qualification@1");
assert.equal(matrix.driver, "kafka");
assert.ok(Array.isArray(matrix.cells) && matrix.cells.length > 0);
// Standalone CI checks this repository's evidence. The historical bridge
// fixture still requires the complete sibling checkout in the default mode.
const standalone = process.argv.includes("--standalone");
const legacyBridgeEvidence = "../openbindings-go/formats/asyncapi/driver_integration_test.go";
let externalBridgeReferences = 0;
const ids = new Set();
for (const cell of matrix.cells) {
  assert.equal(typeof cell.id, "string");
  assert.ok(!ids.has(cell.id), `duplicate Kafka qualification cell ${cell.id}`);
  ids.add(cell.id);
  assert.ok(["supported", "excluded", "unqualified"].includes(cell.disposition), `invalid disposition for ${cell.id}`);
  if (cell.disposition === "supported") assert.ok(cell.evidence?.length > 0, `supported cell ${cell.id} has no evidence`);
  if (cell.disposition !== "supported") assert.equal(typeof cell.reason, "string", `${cell.id} has no reason`);
  for (const file of cell.evidence ?? []) {
    if (standalone && file === legacyBridgeEvidence) {
      externalBridgeReferences++;
      continue;
    }
    await access(resolve(root, file));
  }
}
console.log(`Kafka authority matrix accounts for ${matrix.cells.length} semantic cells`);
if (standalone) console.log(`Standalone evidence check: ${externalBridgeReferences} historical SDK bridge references require separate replay`);
