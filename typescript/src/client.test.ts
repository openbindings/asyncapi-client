import { describe, expect, it, vi } from "vitest";
import { AsyncAPIClient } from "./client.js";
import { AsyncAPIEngine, AsyncAPIExecutionError } from "./engine.js";

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

  it("refuses reply-bearing WebSocket operations before establishing a socket", async () => {
    const document = httpDocument();
    document.servers.production = { host: "api.example.test", protocol: "wss" };
    document.operations.submit.action = "send";
    delete (document.operations.submit as Record<string, unknown>).bindings;
    const client = await AsyncAPIClient.load(document);
    const execution = await client.start("submit");
    const failure = execution.completed.catch((error: unknown) => error);
    await expect(failure).resolves.toBeInstanceOf(AsyncAPIExecutionError);
    await expect(failure).resolves.toEqual(expect.objectContaining({ code: "ERR_SOURCE_CONFIG_ERROR" }));
    client.close();
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
});
