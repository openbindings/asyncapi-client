import { describe, expect, it, vi } from "vitest";

import { isAvroSchemaFormat } from "./avro.js";
import { AsyncAPIClient } from "./client.js";
import type { AsyncAPIExecutionError } from "./engine.js";

describe("isAvroSchemaFormat", () => {
  const cases: Array<[string, boolean]> = [
    ["application/vnd.apache.avro;version=1.9.0", true],
    ["application/vnd.apache.avro+json;version=1.11.1", true],
    ["application/vnd.apache.avro+yaml", true],
    ["Application/VND.Apache.Avro;Version=1.9.0", true],
    ["application/vnd.apache.avro;version=2.0.0", false],
    ["application/schema+json;version=draft-07", false],
    ["avro", false],
    ["", false],
  ];
  it.each(cases)("isAvroSchemaFormat(%j) → %s", (format, want) => {
    expect(isAvroSchemaFormat(format)).toBe(want);
  });
});

function avroDocument(contentType: string, withReply: boolean, schema?: unknown): Record<string, unknown> {
  return {
    asyncapi: "3.0.0",
    info: { title: "Avro correspondence", version: "1.0.0" },
    servers: { production: { host: "api.example.test", protocol: "https" } },
    channels: {
      records: {
        address: "/records",
        messages: {
          Record: {
            contentType,
            payload: {
              schemaFormat: "application/vnd.apache.avro;version=1.9.0",
              schema: schema ?? { type: "record", name: "Record", fields: [{ name: "id", type: "long" }] },
            },
          },
        },
      },
    },
    operations: {
      store: {
        action: "receive",
        channel: { $ref: "#/channels/records" },
        messages: [{ $ref: "#/channels/records/messages/Record" }],
        bindings: { http: { method: "POST" } },
        ...(withReply ? { reply: { messages: [{ $ref: "#/channels/records/messages/Record" }] } } : {}),
      },
    },
  };
}

// The Avro binary encoding of Record{id: long} with id=7: one field, a
// long, zigzag(7) = 14 = 0x0E. Hand-derived from the Avro specification's
// binary encoding — the wire pin is against the spec, not the library.
const AVRO_WIRE_ID_7 = new Uint8Array([0x0e]);

async function requestBytes(input: Request | string | URL, init?: RequestInit): Promise<Uint8Array> {
  const request = input instanceof Request ? input : new Request(String(input), init);
  return new Uint8Array(await request.arrayBuffer());
}

describe("the named Avro correspondence at invocation", () => {
  // Encode side: the caller's logical value crosses the boundary and the
  // wire carries exactly the Avro binary encoding of the datum under the
  // artifact's schema (bare framing, the default).
  it("encodes the logical value as the Avro binary wire", async () => {
    let seen = new Uint8Array(0);
    const fetch = vi.fn(async (input: Request | string | URL, init?: RequestInit) => {
      seen = await requestBytes(input, init);
      return new Response(null, { status: 204 });
    });
    const client = await AsyncAPIClient.load(avroDocument("avro/binary", false), {
      fetch: fetch as unknown as typeof globalThis.fetch,
    });
    try {
      await expect(client.publish("store", { id: 7 })).resolves.toEqual([]);
      expect(seen).toEqual(AVRO_WIRE_ID_7);
    } finally {
      client.close();
    }
  });

  // Decode side: an avro/binary reply's octets come back as the logical
  // value — the full round trip a bespoke client would perform.
  it("decodes an Avro binary reply to the logical value", async () => {
    const fetch = vi.fn(async () => new Response(AVRO_WIRE_ID_7, {
      status: 200,
      headers: { "content-type": "avro/binary" },
    }));
    const client = await AsyncAPIClient.load(avroDocument("avro/binary", true), {
      fetch: fetch as unknown as typeof globalThis.fetch,
    });
    try {
      await expect(client.publish("store", { id: 7 })).resolves.toEqual([{ id: 7 }]);
    } finally {
      client.close();
    }
  });

  // The confluent framing configuration point: magic byte 0x00 plus the
  // big-endian 4-byte configuration.schemaId prefixes the binary encoding
  // on the wire, and decode verifies and strips the same prefix.
  it("frames and unframes the Confluent wire prefix", async () => {
    const framed = new Uint8Array([0x00, 0x00, 0x00, 0x00, 0x2a, ...AVRO_WIRE_ID_7]);
    let seen = new Uint8Array(0);
    const fetch = vi.fn(async (input: Request | string | URL, init?: RequestInit) => {
      seen = await requestBytes(input, init);
      return new Response(framed, { status: 200, headers: { "content-type": "avro/binary" } });
    });
    const client = await AsyncAPIClient.load(avroDocument("avro/binary", true), {
      fetch: fetch as unknown as typeof globalThis.fetch,
      context: { configuration: { framing: "confluent", schemaId: 42 } },
    });
    try {
      await expect(client.publish("store", { id: 7 })).resolves.toEqual([{ id: 7 }]);
      expect(seen).toEqual(framed);
    } finally {
      client.close();
    }
  });

  // The codec-capability refusal survives for the unqualifiable case: an
  // on-list declaration whose schema is not a valid Avro schema cannot
  // build a codec, so the invocation refuses before dispatch (ERR_REFUSED)
  // — never the byte boundary, never a dial.
  it("refuses an unqualifiable Avro declaration, never dialing", async () => {
    const fetch = vi.fn(async () => new Response(null, { status: 204 }));
    const client = await AsyncAPIClient.load(
      avroDocument("avro/binary", false, { type: "record", name: "Record" }),
      { fetch: fetch as unknown as typeof globalThis.fetch },
    );
    try {
      const failure = client.publish("store", { id: 7 }).catch((error: unknown) => error);
      await expect(failure).resolves.toEqual(expect.objectContaining<Partial<AsyncAPIExecutionError>>({
        code: "ERR_REFUSED",
      }));
      expect(fetch).not.toHaveBeenCalled();
    } finally {
      client.close();
    }
  });

  // An Avro-declared payload with JSON-family media needs no binary codec:
  // the wire is the Avro-JSON encoding, which the ordinary JSON lane
  // carries — the logical value crosses the boundary end to end.
  it("carries JSON media through the JSON lane as the Avro-JSON wire", async () => {
    let seen = "";
    const fetch = vi.fn(async (input: Request | string | URL, init?: RequestInit) => {
      seen = await new Request(input instanceof Request ? input : String(input), init).text();
      return new Response(null, { status: 204 });
    });
    const client = await AsyncAPIClient.load(avroDocument("application/json", false), {
      fetch: fetch as unknown as typeof globalThis.fetch,
    });
    try {
      await expect(client.publish("store", { id: 7 })).resolves.toEqual([]);
      expect(JSON.parse(seen)).toEqual({ id: 7 });
    } finally {
      client.close();
    }
  });
});
