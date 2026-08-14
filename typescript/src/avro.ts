import type { AsyncAPIMessage } from "./asyncapi-types.js";

// The named Avro correspondence (openbindings.asyncapi@1 §9.2): a message
// whose payload declares an on-list Avro schema format crosses the boundary
// as logical application values. A JSON-family effective content type
// carries the Avro-JSON encoding directly — the ordinary JSON lane is that
// wire, so nothing extra is needed. Any other declared media carries the
// Avro BINARY encoding, a codec capability: this build has not qualified an
// Avro binary codec, so such an operation direction refuses before
// dispatch, exactly as an unqualified protocol driver does. It MUST NOT
// fall back to the byte boundary — the synthesized schema is the logical
// one, and base64 strings do not satisfy it. (Go twin: avro.go.)

/** Reports whether the message payload is a Multi Format Schema Object
 *  declaring an on-list Avro schema format. */
export function avroDeclaredMessage(message: AsyncAPIMessage): boolean {
  const payload = message.payload;
  if (payload === undefined || payload === null) return false;
  const format = (payload as Record<string, unknown>)["schemaFormat"];
  return typeof format === "string" && isAvroSchemaFormat(format);
}

/**
 * Mirrors the synthesis classifier's on-list rule:
 * application/vnd.apache.avro with an optional +json/+yaml suffix, version
 * parameter absent or 1.x. Anything else is off-list here.
 */
export function isAvroSchemaFormat(format: string): boolean {
  if (format.trim() === "") return false;
  const parts = format.split(";");
  const type = (parts.shift() ?? "").trim().toLowerCase();
  switch (type) {
    case "application/vnd.apache.avro":
    case "application/vnd.apache.avro+json":
    case "application/vnd.apache.avro+yaml":
      break;
    default:
      return false;
  }
  let version = "";
  for (const part of parts) {
    const index = part.indexOf("=");
    if (index < 1) return false;
    if (part.slice(0, index).trim().toLowerCase() === "version") {
      version = part.slice(index + 1).trim();
      if (version.startsWith('"') && version.endsWith('"')) version = version.slice(1, -1);
    }
  }
  return version === "" || version.startsWith("1.");
}

/**
 * Refuses the unqualified-codec case for one governing message: Avro
 * declared, effective media outside the JSON family (whose wire is the Avro
 * binary encoding). Callers with an artifact-silent content type apply the
 * guard to the configured lane instead.
 */
export function avroMediaGuard(message: AsyncAPIMessage, effectiveContentType: string): void {
  if (effectiveContentType === "" || !avroDeclaredMessage(message)) return;
  const normalized = (effectiveContentType.split(";")[0] ?? "").trim().toLowerCase();
  if (normalized === "application/json" || normalized.endsWith("+json")) return;
  throw new Error(
    `the governing message declares the Avro correspondence with media ${JSON.stringify(effectiveContentType)}, whose wire is the Avro binary encoding; this build has no qualified Avro binary codec`,
  );
}
