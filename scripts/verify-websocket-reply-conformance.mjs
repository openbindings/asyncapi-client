import { readFile } from "node:fs/promises";

const matrix = JSON.parse(await readFile(new URL("../conformance/websocket-reply.json", import.meta.url), "utf8"));
if (matrix.format !== "openbindings.asyncapi.protocol-driver-qualification@1") {
  throw new Error("unexpected WebSocket reply conformance format");
}
const ids = new Set();
for (const cell of matrix.cells ?? []) {
  if (!cell.id || ids.has(cell.id)) throw new Error(`missing or duplicate cell id: ${cell.id}`);
  ids.add(cell.id);
  if (!["supported", "excluded", "unqualified"].includes(cell.disposition)) {
    throw new Error(`invalid disposition for ${cell.id}`);
  }
  if (cell.disposition === "supported" && (!Array.isArray(cell.evidence) || cell.evidence.length === 0)) {
    throw new Error(`supported cell ${cell.id} has no evidence`);
  }
  if (cell.disposition !== "supported" && !cell.reason) {
    throw new Error(`${cell.disposition} cell ${cell.id} has no reason`);
  }
}
for (const required of [
  "receive-operation-with-reply",
  "send-operation-with-reply",
  "isolated-concurrent-request-reply-sessions",
  "static-reply-channel-on-distinct-endpoint-same-server",
  "runtime-expression-reply-address-from-application-header",
  "cross-protocol-reply-channel",
]) {
  if (!ids.has(required)) throw new Error(`WebSocket reply matrix is missing ${required}`);
}
console.log(`verified ${ids.size} WebSocket reply conformance cells`);
