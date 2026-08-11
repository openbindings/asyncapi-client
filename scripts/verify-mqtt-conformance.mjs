import assert from "node:assert/strict";
import { access, readFile } from "node:fs/promises";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const matrix = JSON.parse(await readFile(resolve(root, "conformance/mqtt-3.1.1.json"), "utf8"));
assert.equal(matrix.format, "openbindings.asyncapi.protocol-driver-qualification@1");
assert.equal(matrix.driver, "mqtt-3.1.1");
assert.ok(Array.isArray(matrix.cells) && matrix.cells.length > 0);
const ids = new Set();
for (const cell of matrix.cells) {
  assert.equal(typeof cell.id, "string");
  assert.ok(!ids.has(cell.id), `duplicate MQTT qualification cell ${cell.id}`);
  ids.add(cell.id);
  assert.ok(["supported", "excluded", "unqualified"].includes(cell.disposition), `invalid disposition for ${cell.id}`);
  if (cell.disposition === "supported") assert.ok(cell.evidence?.length > 0, `supported cell ${cell.id} has no evidence`);
  if (cell.disposition !== "supported") assert.equal(typeof cell.reason, "string", `${cell.id} has no reason`);
  for (const file of cell.evidence ?? []) await access(resolve(root, file));
}
console.log(`MQTT authority matrix accounts for ${matrix.cells.length} semantic cells`);
