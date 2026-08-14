import avsc from "avsc";

import type { AsyncAPIMessage } from "./asyncapi-types.js";
import { contextConfiguration } from "./internal/index.js";

// The named Avro correspondence (openbindings.asyncapi@1 §9.2): a message
// whose payload declares an on-list Avro schema format crosses the boundary
// as logical application values. A JSON-family effective content type
// carries the Avro-JSON encoding directly — the ordinary JSON lane is that
// wire, so nothing extra is needed. Any other declared media carries the
// Avro BINARY encoding of the datum under the artifact's schema, through
// the qualified codec below (avsc). Wire framing around the binary encoding
// is the named `framing` configuration point: "bare" (the default — the
// binary encoding alone) or "confluent" (the Confluent wire prefix: magic
// byte 0x00 plus the big-endian 4-byte schema id supplied as
// configuration.schemaId). (Go twin: avro.go.)

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

const FRAMING_BARE = "bare";
const FRAMING_CONFLUENT = "confluent";

/**
 * The qualified Avro binary codec for one governing schema: logical value ⇄
 * Avro binary octets, via the Avro JSON Encoding as the value's textual
 * form (avsc's fromString/toString ARE that encoding).
 */
export class AvroBinaryCodec {
  private constructor(
    private readonly type: avsc.Type,
    private readonly framing: string,
    private readonly schemaId: number,
  ) {}

  /**
   * Builds the codec governing a non-JSON wire from the message set's Avro
   * declarations. undefined when no governing message declares an on-list
   * Avro format. All Avro-declared members must carry one identical schema:
   * bare Avro binary is not self-describing, so distinct candidate schemas
   * make the decode declaration ambiguous — refused loudly, never guessed
   * at.
   */
  static resolve(msgs: readonly AsyncAPIMessage[], context?: Record<string, unknown>): AvroBinaryCodec | undefined {
    let schemaJSON: string | undefined;
    for (const message of msgs) {
      if (!avroDeclaredMessage(message)) continue;
      const encoded = JSON.stringify((message.payload as Record<string, unknown>)["schema"] ?? null);
      if (schemaJSON === undefined) {
        schemaJSON = encoded;
        continue;
      }
      if (schemaJSON !== encoded) {
        throw new Error("the governing messages declare distinct Avro schemas; a bare binary wire cannot select among them");
      }
    }
    if (schemaJSON === undefined) return undefined;
    let type: avsc.Type;
    try {
      type = avsc.Type.forSchema(JSON.parse(schemaJSON) as avsc.Schema);
    } catch (e: unknown) {
      throw new Error(`the declared Avro schema is not a valid Avro schema: ${e instanceof Error ? e.message : String(e)}`);
    }
    const configuration = contextConfiguration(context);
    const framing = typeof configuration["framing"] === "string" ? configuration["framing"] : FRAMING_BARE;
    let schemaId = 0;
    if (framing === FRAMING_CONFLUENT) {
      const declared = configuration["schemaId"];
      if (typeof declared !== "number" || !Number.isInteger(declared) || declared < 0 || declared > 0xffffffff) {
        throw new Error(`configuration.framing ${JSON.stringify(framing)} requires configuration.schemaId (an unsigned 32-bit integer)`);
      }
      schemaId = declared;
    } else if (framing !== FRAMING_BARE) {
      throw new Error(`configuration.framing ${JSON.stringify(framing)} is not a named framing (bare, confluent)`);
    }
    return new AvroBinaryCodec(type, framing, schemaId);
  }

  /**
   * Renders one logical value as the wire's Avro binary octets: logical
   * JSON → Avro JSON Encoding text → native datum → binary encoding,
   * framed per the configuration point.
   */
  encode(value: unknown): Uint8Array {
    let native: unknown;
    try {
      native = this.type.fromString(JSON.stringify(value ?? null));
    } catch (e: unknown) {
      throw new Error(`the input value is not the Avro-JSON encoding of a datum under the declared schema: ${e instanceof Error ? e.message : String(e)}`);
    }
    let encoded: Uint8Array;
    try {
      encoded = this.type.toBuffer(native);
    } catch (e: unknown) {
      throw new Error(`the datum does not encode under the declared Avro schema: ${e instanceof Error ? e.message : String(e)}`);
    }
    if (this.framing !== FRAMING_CONFLUENT) return encoded;
    const framed = new Uint8Array(5 + encoded.byteLength);
    new DataView(framed.buffer).setUint32(1, this.schemaId, false);
    framed.set(encoded, 5);
    return framed;
  }

  /**
   * Reads the wire's Avro binary octets back to the logical value:
   * unframe, binary → native datum → Avro JSON Encoding text → logical
   * JSON.
   */
  decode(wire: Uint8Array): unknown {
    if (this.framing === FRAMING_CONFLUENT) {
      if (wire.byteLength < 5 || wire[0] !== 0) {
        throw new Error("the payload does not carry the Confluent wire prefix the framing configuration declares");
      }
      const id = new DataView(wire.buffer, wire.byteOffset).getUint32(1, false);
      if (id !== this.schemaId) {
        throw new Error(`the payload's Confluent schema id ${id} is not the configured schemaId ${this.schemaId}`);
      }
      wire = wire.subarray(5);
    }
    let native: unknown;
    try {
      const buffer = Buffer.from(wire.buffer, wire.byteOffset, wire.byteLength);
      const result = this.type.decode(buffer);
      if (result.value === undefined || result.offset < 0) {
        throw new Error("truncated datum");
      }
      if (result.offset !== buffer.byteLength) {
        throw new Error(`the payload carries ${buffer.byteLength - result.offset} bytes beyond the Avro datum`);
      }
      native = result.value;
    } catch (e: unknown) {
      throw new Error(`the payload is not the Avro binary encoding of a datum under the declared schema: ${e instanceof Error ? e.message : String(e)}`);
    }
    try {
      return JSON.parse(this.type.toString(native)) as unknown;
    } catch (e: unknown) {
      throw new Error(`the datum does not render in the Avro JSON encoding: ${e instanceof Error ? e.message : String(e)}`);
    }
  }
}
