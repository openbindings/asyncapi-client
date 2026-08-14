import { describe, expect, it, vi } from "vitest";

import { AsyncAPIClient } from "./client.js";
import type { AsyncAPIExecutionError } from "./engine.js";

function parameterizedDocument(): Record<string, unknown> {
  return {
    asyncapi: "3.0.0",
    info: { title: "Envelope", version: "1.0.0" },
    servers: { production: { host: "api.example.test", protocol: "https" } },
    channels: {
      orders: {
        address: "/orders/{region}",
        parameters: { region: { enum: ["emea", "amer"] } },
        messages: { Order: { contentType: "application/json", payload: { type: "object" } } },
      },
    },
    operations: {
      place: {
        action: "receive",
        channel: { $ref: "#/channels/orders" },
        messages: [{ $ref: "#/channels/orders/messages/Order" }],
        bindings: { http: { method: "POST" } },
      },
    },
  };
}

// The routed envelope's parameter lane (§9.2, ruled 2026-08-14; Go twin:
// envelope_test.go): the publish input {payload, region} splits
// pre-dispatch — the parameter expands the channel address, the payload
// alone rides the codec lane — and an explicitly supplied field wins over
// the configuration.address.parameters pre-fill.
describe("the routed envelope at invocation", () => {
  it("splits the envelope, explicit parameter winning over pre-fill", async () => {
    let path = "";
    let body = "";
    const fetch = vi.fn(async (input: Request | string | URL, init?: RequestInit) => {
      const request = input instanceof Request ? input : new Request(String(input), init);
      path = new URL(request.url).pathname;
      body = await request.text();
      return new Response(null, { status: 204 });
    });
    const client = await AsyncAPIClient.load(parameterizedDocument(), {
      fetch: fetch as unknown as typeof globalThis.fetch,
      context: { configuration: { address: { parameters: { region: "amer" } } } },
    });
    try {
      await expect(client.publish("place", { payload: { id: 9 }, region: "emea" })).resolves.toEqual([]);
      expect(path).toBe("/orders/emea");
      expect(JSON.parse(body)).toEqual({ id: 9 });
    } finally {
      client.close();
    }
  });

  it("pre-fills from configuration and refuses a bare non-envelope value", async () => {
    let path = "";
    const fetch = vi.fn(async (input: Request | string | URL, init?: RequestInit) => {
      const request = input instanceof Request ? input : new Request(String(input), init);
      path = new URL(request.url).pathname;
      return new Response(null, { status: 204 });
    });
    const client = await AsyncAPIClient.load(parameterizedDocument(), {
      fetch: fetch as unknown as typeof globalThis.fetch,
      context: { configuration: { address: { parameters: { region: "amer" } } } },
    });
    try {
      await expect(client.publish("place", { payload: { id: 1 } })).resolves.toEqual([]);
      expect(path).toBe("/orders/amer");
      const calls = fetch.mock.calls.length;
      const failure = client.publish("place", { id: 1 }).catch((error: unknown) => error);
      await expect(failure).resolves.toEqual(expect.objectContaining<Partial<AsyncAPIExecutionError>>({
        code: "ERR_REFUSED",
      }));
      expect(fetch.mock.calls.length).toBe(calls);
    } finally {
      client.close();
    }
  });
});

// Output-direction carriage (Go twin: TestClientProjectsReplyHeadersIntoOutputEnvelope):
// a headers-declaring reply rides the routed envelope — the decoded payload
// pairs with the declared application headers projected from the HTTP
// response's fields, declared-type parsing applied, transport fields never
// leaking.
describe("reply headers projection", () => {
  it("projects declared reply headers into the output envelope", async () => {
    const document = {
      asyncapi: "3.0.0",
      info: { title: "Reply headers", version: "1.0.0" },
      servers: { production: { host: "api.example.test", protocol: "https" } },
      channels: {
        commands: {
          address: "/commands",
          messages: {
            Command: { contentType: "application/json", payload: { type: "object" } },
            Result: {
              contentType: "application/json",
              payload: { type: "object" },
              headers: { type: "object", properties: { requestId: { type: "string" }, attempt: { type: "integer" } } },
            },
          },
        },
      },
      operations: {
        submit: {
          action: "receive",
          channel: { $ref: "#/channels/commands" },
          messages: [{ $ref: "#/channels/commands/messages/Command" }],
          bindings: { http: { method: "POST" } },
          reply: { messages: [{ $ref: "#/channels/commands/messages/Result" }] },
        },
      },
    };
    const fetch = vi.fn(async () => new Response(JSON.stringify({ accepted: true }), {
      status: 200,
      headers: {
        "content-type": "application/json",
        requestid: "r-42",
        attempt: "3",
        "x-transport": "never-projected",
      },
    }));
    const client = await AsyncAPIClient.load(document, { fetch: fetch as unknown as typeof globalThis.fetch });
    try {
      await expect(client.publish("submit", { id: 1 })).resolves.toEqual([
        { payload: { accepted: true }, headers: { requestId: "r-42", attempt: 3 } },
      ]);
    } finally {
      client.close();
    }
  });
});
