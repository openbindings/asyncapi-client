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

function avroDocument(contentType: string): Record<string, unknown> {
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
              schema: { type: "record", name: "Record", fields: [{ name: "id", type: "long" }] },
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
      },
    },
  };
}

describe("the named Avro correspondence at invocation", () => {
  // An Avro-declared payload with binary media is a codec capability this
  // build has not qualified: the invocation refuses before dispatch
  // (ERR_REFUSED) instead of falling back to the byte boundary, whose
  // base64 strings the synthesized logical schema does not admit.
  it("refuses binary media as an unqualified codec, never dialing", async () => {
    const fetch = vi.fn(async () => new Response(null, { status: 204 }));
    const client = await AsyncAPIClient.load(avroDocument("avro/binary"), { fetch });
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

  // An Avro-declared payload with JSON-family media needs no extra codec:
  // the wire is the Avro-JSON encoding, which the ordinary JSON lane
  // carries — the logical value crosses the boundary end to end.
  it("carries JSON media through the JSON lane as the Avro-JSON wire", async () => {
    let seen = "";
    const fetch = vi.fn(async (input: Request | string | URL, init?: RequestInit) => {
      seen = await new Request(input instanceof Request ? input : String(input), init).text();
      return new Response(null, { status: 204 });
    });
    const client = await AsyncAPIClient.load(avroDocument("application/json"), {
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
