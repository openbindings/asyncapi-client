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
