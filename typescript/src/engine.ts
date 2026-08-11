import {
  InvocationError,
  InvocationImpl,
  USE_DEFAULT,
  newInvokeHooks,
  type ContextRequiredDetails,
  type Invocation,
  type InvokeHooks,
  type InvokeSite,
  type Metadata,
  type RawResult,
} from "./internal/index.js";
import type { AsyncAPIDocument } from "./asyncapi-types.js";
import { runBinding, requiredContext } from "./invoke.js";
import { ASYNCAPI_PROFILE_FULL, type AsyncAPIExecutionProfile } from "./profile.js";
import { resolveTarget } from "./target.js";
import { errorMessage, parseAsyncAPIDocument, parseRef } from "./util.js";
import { WSPool } from "./ws-pool.js";

export interface AsyncAPIEngineSource {
  location?: string;
  content?: unknown;
}

export interface AsyncAPIHookResult {
  status: number | null;
  body: string;
  metadata: Record<string, string[]>;
}

export interface AsyncAPIHookSite {
  ref: string;
  target: string;
  profile: string;
}

export const ASYNCAPI_USE_DEFAULT: unique symbol = Symbol("asyncapi: use default");

export interface AsyncAPIExecutionHooks {
  decode?(
    site: AsyncAPIHookSite,
    result: AsyncAPIHookResult,
  ): unknown | typeof ASYNCAPI_USE_DEFAULT | Promise<unknown | typeof ASYNCAPI_USE_DEFAULT>;
}

export interface AsyncAPIEngineOptions {
  fetch?: typeof globalThis.fetch;
  hooks?: AsyncAPIExecutionHooks;
  maxDeliveryUnitBytes?: number;
  profile?: AsyncAPIExecutionProfile;
}

export interface AsyncAPIPrepareOptions {
  source: AsyncAPIEngineSource;
  ref: string;
  profile?: AsyncAPIExecutionProfile;
  context?: Record<string, unknown>;
  signal?: AbortSignal;
  fetch?: typeof globalThis.fetch;
  hooks?: AsyncAPIExecutionHooks;
  maxDeliveryUnitBytes?: number;
  acceptsInput?: boolean;
  /** Disable external-reference retrieval for side-effect-free inspection. */
  allowExternalRefs?: boolean;
}

export interface AsyncAPIRequirement {
  type: string;
  name?: string;
  durable?: boolean;
  description?: string;
  [key: string]: unknown;
}

export interface AsyncAPIRequirementAlternative {
  requirements: AsyncAPIRequirement[];
}

export interface AsyncAPIPrerequisites {
  target: string;
  alternatives: AsyncAPIRequirementAlternative[];
}

export interface AsyncAPIExecutionEvent<T = unknown> {
  value: T;
  metadata: Record<string, string[]>;
}

export interface AsyncAPIExecutionDiagnostics {
  readonly leading: Promise<Record<string, string[]>>;
  trailing(): Record<string, string[]>;
}

export interface AsyncAPIExecution<I = unknown, O = unknown> {
  send(input: I): Promise<void>;
  finishInput(): Promise<void>;
  cancel(): Promise<void>;
  readonly events: AsyncIterable<AsyncAPIExecutionEvent<O>>;
  readonly completed: Promise<void>;
  readonly inputFinished: Promise<void>;
  readonly diagnostics: AsyncAPIExecutionDiagnostics;
}

export class AsyncAPIExecutionError extends Error {
  readonly code: string;
  readonly details?: unknown;
  readonly evidence?: unknown;

  constructor(
    code: string,
    message: string,
    options: { cause?: unknown; details?: unknown; evidence?: unknown } = {},
  ) {
    super(message, options.cause !== undefined ? { cause: options.cause } : undefined);
    this.name = "AsyncAPIExecutionError";
    this.code = code;
    if (options.details !== undefined) this.details = options.details;
    if (options.evidence !== undefined) this.evidence = options.evidence;
  }
}

interface PreparedArguments extends AsyncAPIPrepareOptions {
  profile: AsyncAPIExecutionProfile;
  defaultHooks?: AsyncAPIExecutionHooks;
}

export class PreparedAsyncAPIOperation {
  readonly ref: string;
  readonly profile: AsyncAPIExecutionProfile;
  readonly prerequisites: AsyncAPIPrerequisites | null;

  private readonly document: AsyncAPIDocument;
  private readonly args: PreparedArguments;
  private readonly pool: WSPool;

  /** @internal */
  constructor(
    document: AsyncAPIDocument,
    args: PreparedArguments,
    prerequisites: AsyncAPIPrerequisites | null,
    pool: WSPool,
  ) {
    this.document = document;
    this.args = args;
    this.ref = args.ref;
    this.profile = args.profile;
    this.prerequisites = prerequisites;
    this.pool = pool;
  }

  start<I = unknown, O = unknown>(): AsyncAPIExecution<I, O> {
    const metadata: Metadata[] = [];
    const invocation = new InvocationImpl<unknown, unknown>({ signal: this.args.signal });
    const hooks = engineHooks(this.args.hooks, this.args.defaultHooks, this.profile);
    const site: InvokeSite = {
      operation: "",
      invokedAs: "",
      bindingKey: "",
      bindingSpec: `asyncapi/${this.profile.name}`,
      ref: this.ref,
      target: "",
    };
    queueMicrotask(() => {
      void runBinding(
        {
          source: {
            profile: this.profile,
            location: this.args.source.location,
            content: this.document,
          },
          ref: this.ref,
          context: this.args.context,
          signal: this.args.signal,
          fetch: this.args.fetch,
          hooks,
          site,
          acceptsInput: this.args.acceptsInput,
          maxDeliveryUnitBytes: this.args.maxDeliveryUnitBytes,
          observeOutput: (_value, valueMetadata) => metadata.push(cloneMetadata(valueMetadata)),
        },
        invocation,
        this.document,
        this.pool,
      ).catch((error: unknown) => invocation.fireError(toInternalError(error)));
    });
    return executionView<I, O>(invocation, metadata);
  }
}

/** SDK-neutral AsyncAPI artifact loading and execution engine. */
export class AsyncAPIEngine {
  private readonly cache = new Map<string, AsyncAPIDocument>();
  private readonly pool = new WSPool();
  private readonly options: AsyncAPIEngineOptions;

  constructor(options: AsyncAPIEngineOptions = {}) {
    this.options = options;
  }

  async prepare(options: AsyncAPIPrepareOptions): Promise<PreparedAsyncAPIOperation> {
    const args = this.arguments(options);
    let document: AsyncAPIDocument;
    try {
      document = await this.load(args.source, args.signal, args.fetch, args.allowExternalRefs);
    } catch (error: unknown) {
      throw new AsyncAPIExecutionError("SOURCE_LOAD_FAILED", errorMessage(error), { cause: error });
    }
    return this.prepared(document, args);
  }

  /** Prepares an operation from a document already loaded by a native client. */
  prepareDocument(
    document: AsyncAPIDocument,
    options: AsyncAPIPrepareOptions,
  ): PreparedAsyncAPIOperation {
    return this.prepared(document, this.arguments(options));
  }

  async prepareCached(options: AsyncAPIPrepareOptions): Promise<PreparedAsyncAPIOperation | null> {
    const args = this.arguments(options);
    let document: AsyncAPIDocument | undefined;
    if (args.source.content !== undefined) {
      try {
        document = await parseAsyncAPIDocument(
          args.source.location,
          args.source.content,
          { signal: args.signal },
          rejectNetworkFetch,
        );
      } catch {
        return null;
      }
    } else if (args.source.location) {
      document = this.cache.get(args.source.location);
    }
    return document ? this.prepared(document, args) : null;
  }

  close(): void {
    this.pool.closeAll();
    this.cache.clear();
  }

  private arguments(options: AsyncAPIPrepareOptions): PreparedArguments {
    return {
      ...options,
      profile: options.profile ?? this.options.profile ?? ASYNCAPI_PROFILE_FULL,
      fetch: options.fetch ?? this.options.fetch,
      hooks: options.hooks,
      defaultHooks: this.options.hooks,
      maxDeliveryUnitBytes: options.maxDeliveryUnitBytes ?? this.options.maxDeliveryUnitBytes,
    };
  }

  private prepared(
    document: AsyncAPIDocument,
    args: PreparedArguments,
  ): PreparedAsyncAPIOperation {
    const operation = assertOperation(document, args.ref);
    let prerequisites: ContextRequiredDetails | null = null;
    try {
      const target = resolveTarget(document, operation.channel, args.context);
      prerequisites = requiredContext(
        operation,
        target.securityServer,
        target.serverURL,
        args.context,
      );
    } catch {
      // Resolution-dependent configuration is surfaced by start(), where it
      // can carry the precise execution error. Inspection remains inert.
    }
    return new PreparedAsyncAPIOperation(document, args, prerequisites, this.pool);
  }

  private async load(
    source: AsyncAPIEngineSource,
    signal: AbortSignal | undefined,
    fetchFn: typeof globalThis.fetch | undefined,
    allowExternalRefs: boolean | undefined,
  ): Promise<AsyncAPIDocument> {
    const selectedFetch = allowExternalRefs === false ? rejectNetworkFetch : fetchFn;
    if (source.content !== undefined) {
      const document = await parseAsyncAPIDocument(
        source.location,
        source.content,
        { signal },
        selectedFetch,
      );
      if (source.location) this.cache.set(source.location, document);
      return document;
    }
    if (!source.location) {
      return parseAsyncAPIDocument(undefined, undefined, { signal }, selectedFetch);
    }
    const cached = this.cache.get(source.location);
    if (cached) return cached;
    const document = await parseAsyncAPIDocument(source.location, undefined, { signal }, selectedFetch);
    this.cache.set(source.location, document);
    return document;
  }
}

const rejectNetworkFetch: typeof globalThis.fetch = () =>
  Promise.reject(new Error("AsyncAPI cached preparation performs no network I/O"));

function assertOperation(document: AsyncAPIDocument, ref: string) {
  let key: string;
  try {
    key = parseRef(ref);
  } catch (error: unknown) {
    throw new AsyncAPIExecutionError("INVALID_OPERATION_REF", errorMessage(error), { cause: error });
  }
  const operation = document.operations?.[key];
  if (!operation || operation["x-ob-asyncapi-unresolved-trait"] !== undefined) {
    throw new AsyncAPIExecutionError("OPERATION_NOT_FOUND", `operation ${JSON.stringify(ref)} was not found`);
  }
  return operation;
}

function executionView<I, O>(
  invocation: Invocation<unknown, unknown>,
  metadata: Metadata[],
): AsyncAPIExecution<I, O> {
  const completed = invocation.closed.catch((error: unknown) => {
    throw toExecutionError(error);
  });
  void completed.catch(() => undefined);
  return {
    send: async (input: I) => {
      try {
        await invocation.write(input);
      } catch (error: unknown) {
        throw toExecutionError(error);
      }
    },
    finishInput: () => invocation.close(),
    cancel: () => invocation.cancel(),
    events: mapEvents<O>(invocation.outputs, metadata),
    completed,
    inputFinished: invocation.inputClosed,
    diagnostics: invocation.diagnostics,
  };
}

async function* mapEvents<O>(
  outputs: AsyncIterable<unknown>,
  metadata: Metadata[],
): AsyncIterable<AsyncAPIExecutionEvent<O>> {
  try {
    for await (const value of outputs) {
      yield { value: value as O, metadata: cloneMetadata(metadata.shift() ?? {}) };
    }
  } catch (error: unknown) {
    throw toExecutionError(error);
  }
}

function engineHooks(
  perCall: AsyncAPIExecutionHooks | undefined,
  defaults: AsyncAPIExecutionHooks | undefined,
  profile: AsyncAPIExecutionProfile,
): InvokeHooks | null {
  const slots = (hooks: AsyncAPIExecutionHooks | undefined) => ({
    decode: hooks?.decode
      ? async (site: InvokeSite, raw: RawResult) => {
          try {
            const value = await hooks.decode!(
              { ref: site.ref, target: site.target, profile: profile.name },
              { status: raw.status, body: raw.body, metadata: cloneMetadata(raw.meta) },
            );
            return value === ASYNCAPI_USE_DEFAULT ? USE_DEFAULT : value;
          } catch (error: unknown) {
            throw toInternalError(error);
          }
        }
      : undefined,
  });
  return newInvokeHooks(slots(perCall), slots(defaults));
}

function toExecutionError(error: unknown): AsyncAPIExecutionError {
  if (error instanceof AsyncAPIExecutionError) return error;
  if (error instanceof InvocationError) {
    return new AsyncAPIExecutionError(error.code, error.message, {
      cause: error,
      details: error.details,
      evidence: error.diagnostics,
    });
  }
  return new AsyncAPIExecutionError("RUNTIME_ERROR", errorMessage(error), { cause: error });
}

function toInternalError(error: unknown): InvocationError {
  if (error instanceof InvocationError) return error;
  if (error instanceof AsyncAPIExecutionError) {
    return new InvocationError(error.code, error.message, error.details, error.evidence);
  }
  return new InvocationError("ERR_RUNTIME", errorMessage(error));
}

function cloneMetadata(metadata: Record<string, string[]>): Record<string, string[]> {
  return Object.fromEntries(Object.entries(metadata).map(([name, values]) => [name, [...values]]));
}

export {
  ASYNCAPI_PROFILE_COMPAT,
  ASYNCAPI_PROFILE_FULL,
  type AsyncAPIExecutionProfile,
} from "./profile.js";
