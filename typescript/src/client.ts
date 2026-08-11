import type { AsyncAPIDocument } from "./asyncapi-types.js";
import {
  AsyncAPIEngine,
  type AsyncAPIEngineOptions,
  type AsyncAPIEngineSource,
  type AsyncAPIExecution,
  type AsyncAPIExecutionHooks,
} from "./engine.js";
import { operationRef, parseAsyncAPIDocument, parseRef } from "./util.js";

export type AsyncAPIOperationSelector = string | { ref: string };

export interface AsyncAPIClientOptions extends AsyncAPIEngineOptions {
  context?: Record<string, unknown>;
  signal?: AbortSignal;
}

export interface AsyncAPICallOptions {
  context?: Record<string, unknown>;
  signal?: AbortSignal;
  fetch?: typeof globalThis.fetch;
  hooks?: AsyncAPIExecutionHooks;
  maxDeliveryUnitBytes?: number;
}

export interface AsyncAPIOperationDescription {
  key: string;
  ref: string;
  action: "send" | "receive";
  interaction: "publish" | "subscribe";
  title?: string;
  description?: string;
}

/** Document-driven AsyncAPI client independent of OpenBindings. */
export class AsyncAPIClient {
  readonly source: AsyncAPIEngineSource;
  private readonly document: AsyncAPIDocument;
  private readonly engine: AsyncAPIEngine;
  private readonly options: AsyncAPIClientOptions;

  private constructor(
    source: AsyncAPIEngineSource,
    document: AsyncAPIDocument,
    options: AsyncAPIClientOptions,
  ) {
    this.source = { location: source.location, content: document };
    this.document = document;
    this.options = options;
    this.engine = new AsyncAPIEngine(options);
  }

  static async load(
    source: string | AsyncAPIEngineSource | Record<string, unknown>,
    options: AsyncAPIClientOptions = {},
  ): Promise<AsyncAPIClient> {
    const normalized = normalizeSource(source);
    const document = await parseAsyncAPIDocument(
      normalized.location,
      normalized.content,
      { signal: options.signal },
      options.fetch,
    );
    return new AsyncAPIClient(normalized, document, options);
  }

  operations(): AsyncAPIOperationDescription[] {
    return Object.entries(this.document.operations ?? {}).map(([key, operation]) => ({
      key,
      ref: operationRef(key),
      action: operation.action,
      interaction: operation.action === "receive" ? "publish" : "subscribe",
      ...(operation.summary ? { title: operation.summary } : {}),
      ...(operation.description ? { description: operation.description } : {}),
    }));
  }

  async start<I = unknown, O = unknown>(
    selector: AsyncAPIOperationSelector,
    options: AsyncAPICallOptions = {},
  ): Promise<AsyncAPIExecution<I, O>> {
    const prepared = this.engine.prepareDocument(this.document, {
      source: { location: this.source.location },
      ref: normalizeSelector(selector),
      context: options.context ?? this.options.context,
      signal: options.signal ?? this.options.signal,
      fetch: options.fetch,
      hooks: options.hooks,
      maxDeliveryUnitBytes: options.maxDeliveryUnitBytes,
    });
    return prepared.start<I, O>();
  }

  async publish<I = unknown, O = unknown>(
    selector: AsyncAPIOperationSelector,
    input: I,
    options: AsyncAPICallOptions = {},
  ): Promise<O[]> {
    const operation = operationFor(this.document, selector);
    if (operation.action !== "receive") {
      throw new Error(`operation ${JSON.stringify(normalizeSelector(selector))} is a subscription, not a publish interaction`);
    }
    const execution = await this.start<I, O>(selector, options);
    await execution.send(input);
    await execution.finishInput();
    const outputs: O[] = [];
    for await (const event of execution.events) outputs.push(event.value);
    await execution.completed;
    return outputs;
  }

  async subscribe<O = unknown>(
    selector: AsyncAPIOperationSelector,
    options: AsyncAPICallOptions = {},
  ): Promise<AsyncAPIExecution<never, O>> {
    const operation = operationFor(this.document, selector);
    if (operation.action !== "send") {
      throw new Error(`operation ${JSON.stringify(normalizeSelector(selector))} is a publish, not a subscription interaction`);
    }
    const execution = await this.start<never, O>(selector, options);
    await execution.finishInput();
    return execution;
  }

  close(): void {
    this.engine.close();
  }
}

function normalizeSource(
  source: string | AsyncAPIEngineSource | Record<string, unknown>,
): AsyncAPIEngineSource {
  if (typeof source === "string") {
    try {
      new URL(source);
      return { location: source };
    } catch {
      return { content: source };
    }
  }
  if ("location" in source || "content" in source) return source as AsyncAPIEngineSource;
  return { content: source };
}

function normalizeSelector(selector: AsyncAPIOperationSelector): string {
  if (typeof selector !== "string") return selector.ref;
  return selector.startsWith("#/") ? selector : operationRef(selector);
}

function operationFor(document: AsyncAPIDocument, selector: AsyncAPIOperationSelector) {
  const ref = normalizeSelector(selector);
  const key = parseRef(ref);
  const operation = document.operations?.[key];
  if (!operation) throw new Error(`operation ${JSON.stringify(ref)} was not found`);
  return operation;
}
