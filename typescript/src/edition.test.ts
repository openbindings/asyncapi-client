import { describe, expect, it } from "vitest";

import { parseAsyncAPIDocument, rawParsedDocument } from "./util.js";

const rejectingFetch = (async () => {
  throw new Error("must not fetch");
}) as unknown as typeof globalThis.fetch;

function fetchServing(documents: Record<string, string>): typeof globalThis.fetch {
  return (async (input: RequestInfo | URL) => {
    const url = String(input);
    for (const [suffix, body] of Object.entries(documents)) {
      if (url.endsWith(suffix)) return new Response(body, { status: 200 });
    }
    return new Response("not found", { status: 404 });
  }) as unknown as typeof globalThis.fetch;
}

// MC5 seal-1 finding F-V3-1: an AsyncAPI 2.x document carrying Reference
// Objects at positions its own declared edition does not admit them has no
// interpretation under that edition and refuses whole-artifact, BEFORE any
// composition fetch — the adjudicated consistent-loud-refusal convergence
// for the parser-tolerance class (Go twin: ValidateReferenceAdmission).
describe("AsyncAPI 2.x Reference Object position admission", () => {
  const cases: Array<[string, Record<string, unknown>]> = [
    ["an Operation Object position", {
      asyncapi: "2.4.0",
      info: { title: "Bad", version: "1" },
      channels: { events: { publish: { $ref: "./ops.yml#/publish" } } },
    }],
    ["the whole servers map", {
      asyncapi: "2.4.0",
      info: { title: "Bad", version: "1" },
      servers: { $ref: "./servers.yml" },
      channels: {},
    }],
    ["a string-typed channel description", {
      asyncapi: "2.4.0",
      info: { title: "Bad", version: "1" },
      channels: {
        events: {
          description: { $ref: "./descriptions.yml#/events" },
          subscribe: { message: { payload: { type: "object" } } },
        },
      },
    }],
    ["a servers-map value before 2.4.0", {
      asyncapi: "2.2.0",
      info: { title: "Bad", version: "1" },
      servers: { main: { $ref: "./servers.yml#/main" } },
      channels: {},
    }],
    ["a channel servers list member", {
      asyncapi: "2.4.0",
      info: { title: "Bad", version: "1" },
      servers: { main: { url: "wss://example.test", protocol: "wss" } },
      channels: {
        events: {
          servers: [{ $ref: "#/servers/main" }],
          subscribe: { message: { payload: { type: "object" } } },
        },
      },
    }],
    ["a string-typed message contentType", {
      asyncapi: "2.6.0",
      info: { title: "Bad", version: "1" },
      channels: {
        events: {
          subscribe: {
            message: { contentType: { $ref: "#/components/x-media/json" }, payload: { type: "object" } },
          },
        },
      },
    }],
  ];

  for (const [name, document] of cases) {
    it(`refuses a Reference Object at ${name} without fetching`, async () => {
      await expect(
        parseAsyncAPIDocument("https://example.test/root.yaml", document, {}, rejectingFetch),
      ).rejects.toThrow(/does not admit a Reference Object/);
    });
  }

  it("admits the positions the edition text admits", async () => {
    // A servers-map VALUE ref is legal from 2.4.0 (Server Object |
    // Reference Object) and the Channel Item's own $ref field is legal in
    // every 2.x edition.
    const parsed = await parseAsyncAPIDocument(undefined, {
      asyncapi: "2.4.0",
      info: { title: "Fine", version: "1" },
      servers: { main: { $ref: "#/components/x-servers/main" } },
      channels: { events: { subscribe: { message: { payload: { type: "object" } } } } },
    }, {}, rejectingFetch);
    expect(Object.keys(parsed.operations ?? {})).toContain("v2:subscribe:events");
  });

  it("leaves 3.x documents untouched", async () => {
    const parsed = await parseAsyncAPIDocument(undefined, {
      asyncapi: "3.0.0",
      info: { title: "Fine", version: "1" },
      channels: { events: { address: "/events", messages: { e: { payload: { type: "object" } } } } },
      operations: {
        pub: { action: "receive", channel: { $ref: "#/channels/events" } },
      },
    }, {}, rejectingFetch);
    expect(Object.keys(parsed.operations ?? {})).toEqual(["pub"]);
  });
});

// MC5 seal-1 finding F-V3-2: a top-level Avro union is a JSON ARRAY — a
// legal Avro schema form — so an external .avsc referenced from an
// Avro-declared payload position must compose even though its document root
// is not an object, taking the Multi Format Schema Object wrapper shape the
// Go client's typed model carries (hoistNonObjectAvroPayloads).
describe("Avro non-object external schema documents", () => {
  const unionAvsc = JSON.stringify([
    "null",
    { type: "record", name: "File", fields: [{ name: "path", type: "string" }] },
  ]);

  it("composes a top-level union at a 2.x message-level Avro payload ref", async () => {
    const parsed = await parseAsyncAPIDocument(
      "https://example.test/root.yaml",
      {
        asyncapi: "2.6.0",
        info: { title: "Avro union", version: "1" },
        channels: {
          files: {
            publish: {
              message: {
                name: "file",
                schemaFormat: "application/vnd.apache.avro;version=1.9.0",
                payload: { $ref: "./File.avsc" },
              },
            },
          },
        },
      },
      {},
      fetchServing({ "/File.avsc": unionAvsc }),
    );
    const message = parsed.channels?.["files"]?.messages?.["file"] as Record<string, unknown>;
    const payload = message["payload"] as Record<string, unknown>;
    expect(payload["schemaFormat"]).toBe("application/vnd.apache.avro;version=1.9.0");
    expect(Array.isArray(payload["schema"])).toBe(true);
    expect((payload["schema"] as unknown[]).length).toBe(2);
    // The retained raw tree carries the same wrapper shape for the
    // synthesis boundary lane.
    const raw = rawParsedDocument(parsed)!;
    const rawChannels = raw["channels"] as Record<string, Record<string, unknown>>;
    const rawMessages = rawChannels["files"]!["messages"] as Record<string, Record<string, unknown>>;
    const rawPayload = rawMessages["file"]!["payload"] as Record<string, unknown>;
    expect(Array.isArray(rawPayload["schema"])).toBe(true);
  });

  it("composes a top-level union at a 3.x wrapper schema ref", async () => {
    const parsed = await parseAsyncAPIDocument(
      "https://example.test/root.yaml",
      {
        asyncapi: "3.0.0",
        info: { title: "Avro union wrapper", version: "1" },
        channels: {
          files: {
            address: "files.v1",
            messages: {
              file: {
                payload: {
                  schemaFormat: "application/vnd.apache.avro;version=1.9.0",
                  schema: { $ref: "./File.avsc" },
                },
              },
            },
          },
        },
        operations: {
          publishFile: {
            action: "receive",
            channel: { $ref: "#/channels/files" },
            messages: [{ $ref: "#/channels/files/messages/file" }],
          },
        },
      },
      {},
      fetchServing({ "/File.avsc": '["null", "string"]' }),
    );
    const message = parsed.channels?.["files"]?.messages?.["file"] as Record<string, unknown>;
    const payload = message["payload"] as Record<string, unknown>;
    expect(payload["schema"]).toEqual(["null", "string"]);
  });

  it("keeps the object demand at structural positions", async () => {
    await expect(parseAsyncAPIDocument(
      "https://example.test/root.yaml",
      {
        asyncapi: "3.0.0",
        info: { title: "Bad structural ref", version: "1" },
        channels: { events: { $ref: "./channel.json" } },
        operations: {},
      },
      {},
      fetchServing({ "/channel.json": '["not", "a", "channel"]' }),
    )).rejects.toThrow(/did not return an object document/);
  });
});
