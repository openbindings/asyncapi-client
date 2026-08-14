import { describe, expect, it, vi } from "vitest";
import { createServer } from "node:http";
import { WebSocket as NodeWebSocket, WebSocketServer } from "ws";
import { AsyncAPIClient } from "./client.js";
import { AsyncAPIEngine, ASYNCAPI_USE_DEFAULT } from "./engine.js";
import { AsyncAPIExecutionError } from "./engine.js";
import type { AsyncAPIProtocolDriver } from "./driver.js";

function httpDocument() {
  return {
    asyncapi: "3.0.0",
    info: { title: "Standalone client", version: "1.0.0" },
    defaultContentType: "application/json",
    servers: { production: { host: "api.example.test", protocol: "https" } },
    channels: {
      commands: {
        address: "/commands",
        messages: {
          Command: { payload: { type: "object" } },
          Result: { payload: { type: "object" } },
        },
      },
    },
    operations: {
      submit: {
        action: "receive",
        channel: { $ref: "#/channels/commands" },
        messages: [{ $ref: "#/channels/commands/messages/Command" }],
        bindings: { http: { method: "PUT" } },
        reply: { messages: [{ $ref: "#/channels/commands/messages/Result" }] },
      },
    },
  };
}

describe("AsyncAPIClient", () => {
  it("loads and inventories authored operations without an OBI", async () => {
    const client = await AsyncAPIClient.load(httpDocument());
    expect(client.operations()).toEqual([
      expect.objectContaining({
        key: "submit",
        ref: "#/operations/submit",
        action: "receive",
        interaction: "publish",
      }),
    ]);
    client.close();
  });

  it("normalizes AsyncAPI 2.x perspective while preserving its native ref", async () => {
    const document = {
      asyncapi: "2.6.0",
      info: { title: "Legacy artifact", version: "1" },
      defaultContentType: "application/json",
      servers: {
        production: { url: "https://api.example.test/events", protocol: "https" },
      },
      channels: {
        "commands/{tenant}": {
          parameters: { tenant: { schema: { type: "string" } } },
          publish: {
            message: { messageId: "Command", payload: { type: "object" } },
            bindings: { http: { method: "POST" } },
          },
        },
      },
    };
    const fetch = vi.fn(async () => new Response(null, { status: 204 }));
    const client = await AsyncAPIClient.load(document, {
      fetch,
      context: { configuration: { address: { parameters: { tenant: "acme" } } } },
    });
    expect(client.operations()).toEqual([
      expect.objectContaining({
        ref: "#/channels/commands~1{tenant}/publish",
        action: "receive",
        interaction: "publish",
      }),
    ]);
    await expect(
      client.publish("#/channels/commands~1{tenant}/publish", { id: 1 }),
    ).resolves.toEqual([]);
    expect(fetch).toHaveBeenCalledOnce();
    client.close();
  });

  it("accepts the structurally compatible AsyncAPI 3.1 edition", async () => {
    const document = httpDocument();
    document.asyncapi = "3.1.0";
    const fetch = vi.fn(async () => new Response(JSON.stringify({ accepted: true }), {
      status: 200,
      headers: { "content-type": "application/json" },
    }));
    const client = await AsyncAPIClient.load(document, { fetch });
    await expect(client.publish("submit", { id: 3 })).resolves.toEqual([{ accepted: true }]);
    client.close();
  });

  it("publishes through the artifact-declared HTTP method and decodes its reply", async () => {
    const seen: Request[] = [];
    const fetch = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      const request = input instanceof Request ? input : new Request(input, init);
      seen.push(request);
      return new Response(JSON.stringify({ accepted: true }), {
        status: 200,
        headers: { "content-type": "application/json" },
      });
    });
    const client = await AsyncAPIClient.load(httpDocument(), { fetch });
    await expect(client.publish("submit", { id: 7 })).resolves.toEqual([{ accepted: true }]);
    expect(seen).toHaveLength(1);
    expect(seen[0]?.method).toBe("PUT");
    expect(seen[0]?.url).toBe("https://api.example.test/commands");
    await expect(seen[0]!.json()).resolves.toEqual({ id: 7 });
    client.close();
  });

  it("applies operation and message traits before invocation", async () => {
    const document = httpDocument() as any;
    delete document.defaultContentType;
    delete document.operations.submit.bindings;
    document.components = {
      operationTraits: {
        httpPost: { summary: "Trait summary", bindings: { http: { method: "POST" } } },
        httpPatch: { bindings: { http: { method: "PATCH" } } },
      },
      messageTraits: {
        json: { contentType: "application/json" },
      },
    };
    document.operations.submit.summary = "Target summary";
    document.operations.submit.traits = [
      { $ref: "#/components/operationTraits/httpPost" },
      { $ref: "#/components/operationTraits/httpPatch" },
    ];
    document.channels.commands.messages.Command.traits = [{ $ref: "#/components/messageTraits/json" }];
    document.channels.commands.messages.Result.traits = [{ $ref: "#/components/messageTraits/json" }];

    const fetch = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      const request = input instanceof Request ? input : new Request(input, init);
      expect(request.method).toBe("PATCH");
      expect(request.headers.get("content-type")).toBe("application/json");
      return new Response(JSON.stringify({ accepted: true }), {
        status: 200,
        headers: { "content-type": "application/json" },
      });
    });
    const client = await AsyncAPIClient.load(document, { fetch });
    expect(client.operations()[0]?.title).toBe("Target summary");
    await expect(client.publish("submit", { id: 7 })).resolves.toEqual([{ accepted: true }]);
    expect(fetch).toHaveBeenCalledOnce();
    client.close();
  });

  it("refuses invocation when a message trait declares uncarried headers", async () => {
    const document = httpDocument() as any;
    document.components = {
      messageTraits: { traced: { headers: { type: "object" } } },
    };
    document.channels.commands.messages.Command.traits = [{ $ref: "#/components/messageTraits/traced" }];
    const fetch = vi.fn();
    const client = await AsyncAPIClient.load(document, { fetch });
    await expect(client.publish("submit", { id: 7 })).rejects.toThrow("declares headers");
    expect(fetch).not.toHaveBeenCalled();
    client.close();
  });

  it("resolves an external artifact closure and retains recursive local schemas", async () => {
    const root = {
      asyncapi: "3.0.0",
      info: { title: "External", version: "1" },
      servers: { api: { host: "api.example.test", protocol: "https" } },
      channels: { commands: { $ref: "./channels.yaml#/channels/commands" } },
      operations: {
        submit: {
          action: "receive",
          channel: { $ref: "#/channels/commands" },
          traits: [{ $ref: "#/components/operationTraits/http" }],
        },
      },
      components: {
        operationTraits: { http: { bindings: { http: { method: "POST" } } } },
        schemas: { Node: { type: "object", properties: { next: { $ref: "#/components/schemas/Node" } } } },
      },
    };
    const fetch = vi.fn(async (input: RequestInfo | URL) => {
      const url = String(input);
      if (url.endsWith("/channels.yaml")) {
        return new Response(JSON.stringify({
          channels: { commands: { address: "/commands", messages: {
            Command: { contentType: "application/json", payload: { $ref: "https://artifact.example.test/root.yaml#/components/schemas/Node" } },
          } } },
        }), { status: 200 });
      }
      return new Response(null, { status: 404 });
    });
    const client = await AsyncAPIClient.load({
      location: "https://artifact.example.test/root.yaml",
      content: root,
    }, { fetch });
    expect(client.operations()).toEqual([expect.objectContaining({ key: "submit" })]);
    expect(fetch).toHaveBeenCalledOnce();
    client.close();
  });

  it("refuses an explicit protocol-binding revision outside the frozen profile", async () => {
    const document = httpDocument();
    (document.operations.submit.bindings.http as Record<string, unknown>).bindingVersion = "0.4.0";
    const fetch = vi.fn();
    const client = await AsyncAPIClient.load(document, { fetch });
    await expect(client.publish("submit", { id: 7 })).rejects.toThrow("binding version");
    expect(fetch).not.toHaveBeenCalled();
    client.close();
  });

  it("reports artifact-derived prerequisites during side-effect-free preparation", async () => {
    const document = httpDocument() as ReturnType<typeof httpDocument> & {
      components?: Record<string, unknown>;
    };
    document.components = {
      securitySchemes: {
        bearer: { type: "http", scheme: "bearer" },
      },
    };
    document.servers.production = {
      ...document.servers.production,
      security: [{ $ref: "#/components/securitySchemes/bearer" }],
    } as typeof document.servers.production;
    const engine = new AsyncAPIEngine();
    const prepared = await engine.prepare({
      source: { content: document },
      ref: "#/operations/submit",
    });
    expect(prepared.prerequisites).toEqual(expect.objectContaining({
      target: "https://api.example.test",
      alternatives: [expect.objectContaining({
        requirements: [expect.objectContaining({ type: "auth.bearer", name: "bearer" })],
      })],
    }));
    engine.close();
  });

  it("executes an isolated reply-bearing WebSocket receive session", async () => {
    const httpServer = createServer();
    const wsServer = new WebSocketServer({ server: httpServer });
    wsServer.on("connection", (socket) => {
      socket.on("message", (data) => {
        const request = JSON.parse(data.toString()) as { id: number };
        socket.send(JSON.stringify({ accepted: request.id }));
        socket.close(1000);
      });
    });
    await new Promise<void>((resolve) => httpServer.listen(0, "127.0.0.1", resolve));
    const address = httpServer.address();
    if (!address || typeof address === "string") throw new Error("test server did not bind TCP");
    const previous = globalThis.WebSocket;
    globalThis.WebSocket = NodeWebSocket as unknown as typeof WebSocket;
    const document = httpDocument();
    document.servers.production = { host: `127.0.0.1:${address.port}`, protocol: "ws" };
    delete (document.operations.submit as Record<string, unknown>).bindings;
    document.operations.submit.reply = {
      channel: document.channels.commands,
      messages: [document.channels.commands.messages.Result],
    } as any;
    const client = await AsyncAPIClient.load(document, {
      context: { configuration: { websocketMessageType: "text" } },
    });
    try {
      await expect(client.publish("submit", { id: 17 })).resolves.toEqual([{ accepted: 17 }]);
      const concurrent = await Promise.all([
        client.publish("submit", { id: 31 }),
        client.publish("submit", { id: 47 }),
      ]);
      expect(concurrent).toEqual([[{ accepted: 31 }], [{ accepted: 47 }]]);
    } finally {
      client.close();
      globalThis.WebSocket = previous;
      await new Promise<void>((resolve) => wsServer.close(() => resolve()));
      await new Promise<void>((resolve) => httpServer.close(() => resolve()));
    }
  });

  it("keeps reply-bearing WebSocket send input and output directions distinct", async () => {
    const httpServer = createServer();
    const wsServer = new WebSocketServer({ server: httpServer });
    const received: unknown[] = [];
    wsServer.on("connection", (socket) => {
      socket.send(JSON.stringify({ command: 23 }));
      socket.on("message", (data) => {
        received.push(JSON.parse(data.toString()));
        socket.close(1000);
      });
    });
    await new Promise<void>((resolve) => httpServer.listen(0, "127.0.0.1", resolve));
    const address = httpServer.address();
    if (!address || typeof address === "string") throw new Error("test server did not bind TCP");
    const previous = globalThis.WebSocket;
    globalThis.WebSocket = NodeWebSocket as unknown as typeof WebSocket;
    const document = httpDocument();
    document.servers.production = { host: `127.0.0.1:${address.port}`, protocol: "ws" };
    document.operations.submit.action = "send";
    delete (document.operations.submit as Record<string, unknown>).bindings;
    document.operations.submit.reply = {
      channel: document.channels.commands,
      messages: [document.channels.commands.messages.Result],
    } as any;
    const client = await AsyncAPIClient.load(document, {
      context: { configuration: { websocketMessageType: "text" } },
    });
    try {
      const execution = await client.start("submit");
      await execution.send({ accepted: 23 });
      await execution.finishInput();
      const outputs: unknown[] = [];
      for await (const event of execution.events) outputs.push(event.value);
      await execution.completed;
      expect(outputs).toEqual([{ command: 23 }]);
      expect(received).toEqual([{ accepted: 23 }]);
    } finally {
      client.close();
      globalThis.WebSocket = previous;
      await new Promise<void>((resolve) => wsServer.close(() => resolve()));
      await new Promise<void>((resolve) => httpServer.close(() => resolve()));
    }
  });

  it("coordinates a static reply channel on a distinct WebSocket endpoint", async () => {
    const httpServer = createServer();
    const wsServer = new WebSocketServer({ server: httpServer });
    let replySocket: import("ws").WebSocket | undefined;
    wsServer.on("connection", (socket, request) => {
      if (request.url === "/replies") {
        replySocket = socket;
        return;
      }
      socket.on("message", (data) => {
        const requestValue = JSON.parse(data.toString()) as { id: number };
        replySocket?.send(JSON.stringify({ accepted: requestValue.id }));
        replySocket?.close(1000);
        socket.close(1000);
      });
    });
    await new Promise<void>((resolve) => httpServer.listen(0, "127.0.0.1", resolve));
    const address = httpServer.address();
    if (!address || typeof address === "string") throw new Error("test server did not bind TCP");
    const previous = globalThis.WebSocket;
    globalThis.WebSocket = NodeWebSocket as unknown as typeof WebSocket;
    const document = httpDocument() as any;
    document.servers.production = { host: `127.0.0.1:${address.port}`, protocol: "ws" };
    delete document.operations.submit.bindings;
    document.channels.replies = {
      address: "/replies",
      messages: { Result: document.channels.commands.messages.Result },
    };
    document.operations.submit.reply = {
      channel: document.channels.replies,
      messages: [document.channels.replies.messages.Result],
    };
    const client = await AsyncAPIClient.load(document, {
      context: { configuration: { websocketMessageType: "text" } },
    });
    try {
      await expect(client.publish("submit", { id: 71 })).resolves.toEqual([{ accepted: 71 }]);
    } finally {
      client.close();
      globalThis.WebSocket = previous;
      await new Promise<void>((resolve) => wsServer.close(() => resolve()));
      await new Promise<void>((resolve) => httpServer.close(() => resolve()));
    }
  });

  it("refuses an application-header reply address before opening a WebSocket", async () => {
    const document = httpDocument() as any;
    document.servers.production = { host: "api.example.test", protocol: "wss" };
    delete document.operations.submit.bindings;
    document.operations.submit.reply = {
      channel: document.channels.commands,
      messages: [document.channels.commands.messages.Result],
      address: { location: "$message.header#/replyTo" },
    };
    const opened = vi.fn();
    const previous = globalThis.WebSocket;
    globalThis.WebSocket = opened as unknown as typeof WebSocket;
    const client = await AsyncAPIClient.load(document, {
      context: { configuration: { websocketMessageType: "text" } },
    });
    try {
      const execution = await client.start("submit");
      const failure = execution.completed.catch((error: unknown) => error);
      await expect(failure).resolves.toEqual(expect.objectContaining<Partial<AsyncAPIExecutionError>>({
        code: "ERR_SOURCE_CONFIG_ERROR",
      }));
      expect(opened).not.toHaveBeenCalled();
    } finally {
      client.close();
      globalThis.WebSocket = previous;
    }
  });

  it("propagates cancellation through artifact retrieval", async () => {
    const controller = new AbortController();
    const fetch = vi.fn((_input: RequestInfo | URL, init?: RequestInit) => new Promise<Response>((_resolve, reject) => {
      init?.signal?.addEventListener("abort", () => reject(init.signal?.reason), { once: true });
    }));
    const loading = AsyncAPIClient.load("https://example.test/asyncapi.yaml", {
      fetch,
      signal: controller.signal,
    });
    controller.abort(new Error("stopped"));
    await expect(loading).rejects.toThrow("stopped");
  });

  it("delegates an arbitrary artifact protocol to an installed driver", async () => {
    const document = httpDocument();
    document.servers.production = { host: "broker.example.test", protocol: "mqtt" };
    (document.operations.submit as Record<string, unknown>).bindings = {
      mqtt: { qos: 1 },
      future: {
        marker: "preserved",
        config: { $ref: "#/components/schemas/DriverConfig" },
      },
    };
    delete (document.operations.submit as Record<string, unknown>).reply;
    (document.servers.production as Record<string, unknown>).security = [
      { $ref: "#/components/securitySchemes/mqttBasic" },
    ];
    (document as unknown as Record<string, unknown>).components = {
      securitySchemes: { mqttBasic: { type: "userPassword" } },
      schemas: { DriverConfig: { type: "object" } },
    };
    const seen: unknown[] = [];
    const driver: AsyncAPIProtocolDriver = {
      protocols: ["mqtt"],
      async execute(request, session) {
        expect(request.protocol).toBe("mqtt");
        expect(request.operationKey).toBe("submit");
        expect(request.input?.address).toBe("/commands");
        expect(request.server?.protocol).toBe("mqtt");
        expect(request.input?.messages).toHaveLength(1);
        expect(new TextDecoder().decode(request.input!.encode({ id: 9 }))).toBe('{"id":9}');
        expect(request.operation.bindings).toEqual({
          mqtt: { qos: 1 },
          future: { marker: "preserved", config: { type: "object" } },
        });
        expect(request.securityAlternatives).toEqual([[
          { name: "mqttBasic", scheme: expect.objectContaining({ type: "userPassword" }) },
        ]]);
        for await (const value of session.inputs) seen.push(value);
        await session.emit({ accepted: true });
      },
    };
    const client = await AsyncAPIClient.load(document, {
      drivers: [driver],
      context: { basic: { username: "sensor", password: "secret" } },
    });
    await expect(client.publish("submit", { id: 9 })).resolves.toEqual([{ accepted: true }]);
    expect(seen).toEqual([{ id: 9 }]);
    client.close();
  });

  it("reports an uninstalled protocol driver as a local capability failure", async () => {
    const document = httpDocument();
    document.servers.production = { host: "broker.example.test", protocol: "mqtt" };
    delete (document.operations.submit as Record<string, unknown>).bindings;
    const client = await AsyncAPIClient.load(document);
    const execution = await client.start("submit");
    const failure = execution.completed.catch((error: unknown) => error);
    await expect(failure).resolves.toEqual(expect.objectContaining({ code: "DRIVER_UNAVAILABLE" }));
    client.close();
  });
});

function byteDocument() {
  return {
    asyncapi: "3.0.0",
    info: { title: "Byte boundary", version: "1.0.0" },
    servers: { production: { host: "api.example.test", protocol: "https" } },
    channels: {
      blobs: {
        address: "/blobs",
        messages: {
          Blob: { contentType: "application/octet-stream", payload: { type: "string", contentEncoding: "base64" } },
          Stored: { contentType: "application/octet-stream" },
        },
      },
    },
    operations: {
      store: {
        action: "receive",
        channel: { $ref: "#/channels/blobs" },
        messages: [{ $ref: "#/channels/blobs/messages/Blob" }],
        bindings: { http: { method: "PUT" } },
        reply: { messages: [{ $ref: "#/channels/blobs/messages/Stored" }] },
      },
    },
  };
}

// The artifact-authorized byte rule (§9.2, ruled 2026-08-13): declared
// binary media carries exact octets, the canonical RFC 4648 §4 Base64
// string being the boundary value in both directions. Go twin:
// TestClientCarriesDeclaredBinaryMediaThroughTheByteBoundary.
describe("the byte boundary", () => {
  const wire = new Uint8Array([0x00, 0x01, 0xfe, 0xff]);
  const wireBase64 = btoa(String.fromCharCode(...wire));

  it("carries declared binary media as canonical Base64 in both directions", async () => {
    const seen: Uint8Array[] = [];
    const fetch = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      const request = input instanceof Request ? input : new Request(input, init);
      seen.push(new Uint8Array(await request.arrayBuffer()));
      return new Response("stored", {
        status: 200,
        headers: { "content-type": "application/octet-stream" },
      });
    });
    const client = await AsyncAPIClient.load(byteDocument(), { fetch });
    const events = await client.publish("store", wireBase64);
    expect(Array.from(seen[0]!)).toEqual(Array.from(wire));
    expect(events).toEqual([btoa("stored")]);
    client.close();
  });

  it("refuses non-canonical Base64 and non-string values before dispatch", async () => {
    const fetch = vi.fn(async () => new Response(null, { status: 204 }));
    const client = await AsyncAPIClient.load(byteDocument(), { fetch });
    await expect(client.publish("store", "AAE_")).rejects.toThrow(/canonical Base64/);
    await expect(client.publish("store", 7)).rejects.toThrow(/must be a canonical Base64 string/);
    expect(fetch).not.toHaveBeenCalled();
    client.close();
  });

  it("enriches through consumer Encode/Decode codec hooks", async () => {
    const seen: Uint8Array[] = [];
    const fetch = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      const request = input instanceof Request ? input : new Request(input, init);
      seen.push(new Uint8Array(await request.arrayBuffer()));
      return new Response(new Uint8Array([0x07, 0x77, 0x69, 0x72, 0x65]), {
        status: 200,
        headers: { "content-type": "application/octet-stream" },
      });
    });
    const client = await AsyncAPIClient.load(byteDocument(), {
      fetch,
      hooks: {
        encode: (_site, value) =>
          typeof value === "string" ? new Uint8Array([0x07, ...new TextEncoder().encode(value)]) : ASYNCAPI_USE_DEFAULT,
        decode: (_site, result) =>
          result.bodyBytes && result.bodyBytes[0] === 0x07
            ? new TextDecoder().decode(result.bodyBytes.subarray(1))
            : ASYNCAPI_USE_DEFAULT,
      },
    });
    const events = await client.publish("store", "payload");
    expect(Array.from(seen[0]!)).toEqual([0x07, ...Array.from(new TextEncoder().encode("payload"))]);
    expect(events).toEqual(["wire"]);
    client.close();
  });
});
