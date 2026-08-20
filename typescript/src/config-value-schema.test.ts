import { describe, expect, it } from "vitest";

import { AsyncAPIEngine } from "./engine.js";
import { ConfigRequired, resolveTarget } from "./target.js";
import { contextSatisfies } from "./internal/context.js";
import type { ContextRequiredDetails } from "./internal/invocation.js";
import type { AsyncAPIDocument } from "./asyncapi-types.js";

// config.value schema (2026-08-20 working-draft amendment): the ConfigRequired
// signal carries an engine-asserted JSON Schema where the artifact declares a
// closed value set ({"enum": […]}), absent otherwise; `choices` is removed.

describe("config.value schema emission", () => {
  it("carries an enum schema of the bindable member keys for several servers", () => {
    const doc = {
      servers: {
        eu: { host: "eu.example.com", protocol: "wss" },
        us: { host: "us.example.com", protocol: "wss" },
        mqtt: { host: "q.example.com", protocol: "mqtt" },
      },
    } as unknown as AsyncAPIDocument;
    let thrown: unknown;
    try {
      resolveTarget(doc, undefined, undefined);
    } catch (e: unknown) {
      thrown = e;
    }
    expect(thrown).toBeInstanceOf(ConfigRequired);
    const cr = thrown as ConfigRequired;
    expect(cr.point).toBe("server");
    expect(cr.path).toBe("/key");
    expect(cr.schema).toEqual({ enum: ["eu", "mqtt", "us"] });
  });

  it("carries the declared enum and host hint for an undefaulted server variable", () => {
    const doc = {
      servers: {
        prod: {
          host: "{env}.example.com",
          protocol: "wss",
          variables: { env: { enum: ["eu", "us"] } },
        },
      },
    } as unknown as AsyncAPIDocument;
    let thrown: unknown;
    try {
      resolveTarget(doc, undefined, undefined);
    } catch (e: unknown) {
      thrown = e;
    }
    expect(thrown).toBeInstanceOf(ConfigRequired);
    const cr = thrown as ConfigRequired;
    expect(cr.point).toBe("server");
    expect(cr.path).toBe("/variables/env");
    expect(cr.schema).toEqual({ enum: ["eu", "us"] });
    expect(cr.hostHint).toBe("{env}.example.com");
  });

  it("asserts no schema when the artifact declares no enum", () => {
    const doc = {
      servers: {
        prod: {
          host: "{env}.example.com",
          protocol: "wss",
          variables: { env: {} },
        },
      },
    } as unknown as AsyncAPIDocument;
    let thrown: unknown;
    try {
      resolveTarget(doc, undefined, undefined);
    } catch (e: unknown) {
      thrown = e;
    }
    expect(thrown).toBeInstanceOf(ConfigRequired);
    expect((thrown as ConfigRequired).schema).toBeUndefined();
  });
});

describe("config.value satisfaction under a carried schema", () => {
  const challenge = (schema?: Record<string, unknown>): ContextRequiredDetails => ({
    target: "https://example.com/spec.yaml",
    alternatives: [{
      requirements: [{
        type: "config.value",
        point: "server",
        path: "/key",
        ...(schema !== undefined ? { schema } : {}),
      }],
    }],
  });
  const stored = (value: unknown): Record<string, unknown> => ({
    configuration: { server: { key: value } },
  });

  it("admits a value inside the closed enum and refuses one outside it", () => {
    const enumSchema = { enum: ["eu", "us"] };
    expect(contextSatisfies(stored("eu"), challenge(enumSchema))).toBe(true);
    expect(contextSatisfies(stored("apac"), challenge(enumSchema))).toBe(false);
  });

  it("treats an absent schema as unconstrained (presence satisfies)", () => {
    expect(contextSatisfies(stored("apac"), challenge())).toBe(true);
  });

  it("does not enforce a non-enum schema (twin divergence by necessity)", () => {
    // See requirementSatisfied: the openbindings SDKs validate against the
    // full JSON Schema via their core validator; this repo carries no JSON
    // Schema validator dependency, so only the closed enum member — the one
    // constraint this engine itself asserts — is enforced.
    expect(contextSatisfies(stored("anything"), challenge({ type: "string" }))).toBe(true);
  });
});

// Stage 0 scope assertion (context-scope model, ratified 2026-08-19): the
// challenge target falls back resolved server URL → artifact host hint →
// threaded source location (verbatim — this client has no location
// canonicalizer of its own); a content-only source asserts nothing.
describe("config.value challenge target assertion", () => {
  const twoServerDocument = JSON.stringify({
    asyncapi: "3.0.0",
    info: { title: "Two brokers", version: "1.0.0" },
    servers: {
      eu: { host: "eu.example.test", protocol: "https" },
      us: { host: "us.example.test", protocol: "https" },
    },
    channels: {
      commands: {
        address: "/commands",
        messages: { Command: { payload: { type: "object" } } },
      },
    },
    operations: {
      submit: {
        action: "receive",
        channel: { $ref: "#/channels/commands" },
        messages: [{ $ref: "#/channels/commands/messages/Command" }],
        bindings: { http: { method: "PUT" } },
      },
    },
  });

  async function challengeFor(source: { location?: string; content?: unknown }, fetch?: typeof globalThis.fetch) {
    const engine = new AsyncAPIEngine();
    try {
      const prepared = await engine.prepare({ source, ref: "#/operations/submit", fetch });
      const execution = prepared.start();
      const failure = await execution.completed.catch((error: unknown) => error);
      return failure as { code: string; details?: unknown };
    } finally {
      engine.close();
    }
  }

  it("asserts the threaded source location for the pre-destination server point", async () => {
    const location = "https://example.com/specs/two-brokers.yaml";
    const serving = (async () => new Response(twoServerDocument, { status: 200 })) as unknown as typeof globalThis.fetch;
    const failure = await challengeFor({ location }, serving);
    expect(failure).toEqual(expect.objectContaining({ code: "CONTEXT_REQUIRED" }));
    const details = failure.details as ContextRequiredDetails;
    expect(details.target).toBe(location);
    const requirement = details.alternatives[0]?.requirements[0];
    expect(requirement?.schema).toEqual({ enum: ["eu", "us"] });
    expect(requirement).not.toHaveProperty("choices");
  });

  it("asserts nothing for a content-only source", async () => {
    const failure = await challengeFor({ content: JSON.parse(twoServerDocument) });
    expect(failure).toEqual(expect.objectContaining({ code: "CONTEXT_REQUIRED" }));
    expect((failure.details as ContextRequiredDetails).target).toBe("");
  });
});
