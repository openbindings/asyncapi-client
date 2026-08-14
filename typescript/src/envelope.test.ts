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

// The driver-seam headers channel (Go twin:
// TestKafkaCarriesRecordHeadersBothDirections): a driver declaring
// carriesMessageHeaders receives each publish value as a wire unit —
// payload octets plus sorted application-header pairs — and its received
// units project declared headers into the output envelope.
describe("driver-seam header carriage", () => {
  const artifact = (action: string) => ({
    asyncapi: "3.0.0",
    info: { title: "Record headers", version: "1" },
    servers: { broker: { host: "broker.example:9092", protocol: "kafka" } },
    channels: {
      orders: {
        address: "orders.v1",
        messages: {
          Order: {
            contentType: "application/json",
            payload: { type: "object" },
            headers: { type: "object", properties: { traceId: { type: "string" }, attempt: { type: "integer" } } },
          },
        },
      },
    },
    operations: {
      op: {
        action,
        channel: { $ref: "#/channels/orders" },
        messages: [{ $ref: "#/channels/orders/messages/Order" }],
      },
    },
  });

  it("carries publish headers as unit pairs and projects received units", async () => {
    const sent: Array<{ payload: string; headers: Record<string, string> }> = [];
    const decoder = new TextDecoder();
    const driver = {
      protocols: ["kafka"],
      carriesMessageHeaders: true,
      async execute(request: any, session: any) {
        if (request.action === "receive") {
          for await (const value of session.inputs) {
            const unit = await request.input.encodeUnit(value);
            const headers: Record<string, string> = {};
            for (const pair of unit.headers) headers[pair.key] = decoder.decode(pair.value);
            sent.push({ payload: decoder.decode(unit.payload), headers });
          }
          return;
        }
        await session.closeInput();
        const encoder = new TextEncoder();
        await session.emit(await request.output.decodeUnit({
          payload: encoder.encode(JSON.stringify({ id: 9 })),
          headers: [
            { key: "traceId", value: encoder.encode("t-9") },
            { key: "attempt", value: encoder.encode("5") },
            { key: "x-infra", value: encoder.encode("never-projected") },
          ],
        }));
        session.complete();
      },
    };

    const publisher = await AsyncAPIClient.load(artifact("receive"), { drivers: [driver] });
    try {
      await expect(publisher.publish("op", {
        payload: { id: 4 },
        headers: { traceId: "t-7", attempt: 2 },
      })).resolves.toEqual([]);
    } finally {
      publisher.close();
    }
    expect(sent).toEqual([{ payload: '{"id":4}', headers: { traceId: "t-7", attempt: "2" } }]);

    const subscriber = await AsyncAPIClient.load(artifact("send"), { drivers: [driver] });
    try {
      const execution = await subscriber.start("op");
      const outputs: unknown[] = [];
      for await (const event of execution.events) outputs.push(event.value);
      await execution.completed;
      expect(outputs).toEqual([
        { payload: { id: 9 }, headers: { traceId: "t-9", attempt: 5 } },
      ]);
    } finally {
      subscriber.close();
    }
  });
});
