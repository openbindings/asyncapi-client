import { admitSource, bounds, ordinaryJson, type ValueLimits } from './json-input.js';
export type { ValueLimits } from './json-input.js';
import initializeWasm, { DocumentHandle, OperationHandle, CompiledOperationHandle, PlanHandle, JsonHandle, HostSessionBuilder, HostSessionHandle, HostSenderHandle, CancellationHandle, IncomingHandle, type InitInput } from '../wasm/asyncapi.js';

export interface SourceLocation {
  readonly uri: string | null;
  readonly pointer: string;
  readonly bytes: { readonly start: number; readonly end: number };
  readonly aliases: ReadonlyArray<{ readonly start: number; readonly end: number }>;
}
export interface OperationIdentity { readonly uri: string | null; readonly pointer: string }
export type Requirement =
  | { readonly kind: 'sourceUri' | 'address' | 'clientIdentity' | 'protocolProfile' | 'peerRoute' | 'evaluator' | 'reply' | 'authentication' }
  | { readonly kind: 'resource'; readonly uri: string }
  | { readonly kind: 'server' | 'message'; readonly choices: ReadonlyArray<string> }
  | { readonly kind: 'variable' | 'parameter'; readonly name: string }
  | { readonly kind: 'codec'; readonly contentType: string | null };
export interface Diagnostic {
  readonly code: string;
  readonly location: SourceLocation | null;
  readonly requirement: Requirement | null;
  readonly detail: string;
}
export interface OperationDescription {
  readonly action: 'send' | 'receive';
  readonly operationId: string | null;
  readonly summary: string | null;
  readonly description: string | null;
  readonly channel: SourceLocation;
  readonly address: string | null;
}
export interface PlanOptions {
  readonly role: 'application' | 'peer';
  readonly server?: string;
  readonly message?: string;
  readonly profile?: 'mqtt311' | 'webSocket6455';
  readonly variables?: Readonly<Record<string, string>>;
  readonly parameters?: Readonly<Record<string, string>>;
  readonly address?: string;
  readonly clientId?: string;
}
export interface MessageDescription {
  readonly key: string;
  readonly selection: SourceLocation;
  readonly definition: SourceLocation;
  readonly name: string | null;
  readonly contentType: string | null;
  readonly payload: SourceLocation | null;
  readonly headers: SourceLocation | null;
}
export interface ServerDescription {
  readonly key: string;
  readonly selection: SourceLocation;
  readonly definition: SourceLocation;
  readonly protocol: string;
  readonly protocolVersion: string | null;
}
export interface CompiledDescription {
  readonly identity: OperationIdentity;
  readonly operation: OperationDescription;
  readonly messages: ReadonlyArray<MessageDescription>;
  readonly servers: ReadonlyArray<ServerDescription>;
  readonly reply: SourceLocation | null;
  readonly operationSecurity: SourceLocation | null;
}
export type TransportPlan =
  | { readonly kind: 'mqtt311'; readonly endpoint: string; readonly clientId: string; readonly cleanSession: boolean; readonly keepAliveSeconds: number; readonly topic: string; readonly qos: 0 | 1 | 2; readonly retain: boolean }
  | { readonly kind: 'webSocket6455'; readonly endpoint: string; readonly method: 'GET' };
export interface PlanDescription {
  readonly identity: OperationIdentity;
  readonly role: 'application' | 'peer';
  readonly applicationAction: 'send' | 'receive';
  readonly wireAction: 'send' | 'receive';
  readonly server: string;
  readonly message: string;
  readonly contentType: string;
  readonly transport: TransportPlan;
}
export type Discovery<T> = { readonly ok: true; readonly value: T } | { readonly ok: false; readonly error: AsyncApiError };

export class AsyncApiError extends Error {
  readonly code: string;
  readonly location: SourceLocation | null;
  readonly requirement: Requirement | null;
  constructor(diagnostic: Diagnostic) {
    super(diagnostic.detail);
    this.name = 'AsyncApiError';
    this.code = diagnostic.code;
    this.location = diagnostic.location;
    this.requirement = diagnostic.requirement;
  }
}
function call<T>(operation: () => T): T {
  try { return operation(); }
  catch (error) {
    if (typeof error === 'string') {
      // The private Rust ABI emits diagnostic JSON; unexpected host failures
      // retain their original identity rather than being relabeled as semantics.
      let value: unknown;
      try { value = JSON.parse(error); } catch { throw error; }
      if (value && typeof value === 'object' && 'code' in value && 'detail' in value
        && typeof value.code === 'string' && typeof value.detail === 'string') {
        throw new AsyncApiError(value as Diagnostic);
      }
    }
    throw error;
  }
}
class Owner<T extends { free(): void }> implements Disposable {
  #handle: T | undefined;
  constructor(handle: T) { this.#handle = handle; }
  protected get handle(): T {
    if (!this.#handle) throw new AsyncApiError({ code:'Disposed', detail:'This handle has been disposed', location:null, requirement:null });
    return this.#handle;
  }
  dispose(): void { this.#handle?.free(); this.#handle = undefined; }
  [Symbol.dispose](): void { this.dispose(); }
}

/** Initialize the Rust engine. Supply a compiled module for Worker module imports. */
let readiness: Promise<void> | undefined;
export async function createClient(options: { wasm?: InitInput | Promise<InitInput> } = {}): Promise<Client> {
  if (!readiness) {
    readiness = (async () => {
      if (options.wasm !== undefined) await initializeWasm({ module_or_path: options.wasm });
      else await initializeWasm();
    })().catch(error => { readiness = undefined; throw error; });
  }
  await readiness;
  return new RustClient();
}
function valueRefusal(code: 'InvalidValue' | 'InvalidConfiguration' | 'Limit', detail: string): never {
  throw new AsyncApiError({ code, detail, location: null, requirement: null });
}
export interface Client {
  /** Admit strict JSON, preserving exact authored numbers. No YAML or schema validation. */
  parseJson(source: string, limits?: ValueLimits): JsonView;
  /** Checked plain JavaScript values; no getter/toJSON invocation or lossy substitution. */
  fromValue(value: unknown, limits?: ValueLimits): JsonView;
  parse(source: string, options?: { sourceUri?: string }): Document;
  openSession(plans: readonly Plan[], options?: HostSessionOptions & { signal?: AbortSignal }): Promise<HostSession>;
}
class RustClient implements Client {
  parseJson(source: string, options: ValueLimits = {}): JsonView {
    const limit = bounds(options, valueRefusal);
    admitSource(source, limit, valueRefusal);
    return call(() => new JsonView(JsonHandle.parse(source, limit.bytes, limit.depth, limit.nodes)));
  }
  fromValue(value: unknown, options: ValueLimits = {}): JsonView {
    const limit = bounds(options, valueRefusal);
    return this.parseJson(ordinaryJson(value, limit, valueRefusal), limit);
  }
  async openSession(plans: readonly Plan[], options: HostSessionOptions & { signal?: AbortSignal } = {}): Promise<HostSession> {
    const builder = new HostSessionBuilder();
    try {
      for (const plan of plans) plan.attachTo(builder);
      const {signal, ...configuration} = options;
      return await cancellable(signal, async token => new HostSession(await builder.open(JSON.stringify(configuration), token) as HostSessionHandle));
    } finally { builder.free(); }
  }
  /** Parses original JSON/YAML source; admission is not full document validation. */
  parse(source: string, options: { sourceUri?: string } = {}): Document {
    return call(() => new Document(new DocumentHandle(source, options.sourceUri)));
  }
}
export class Document extends Owner<DocumentHandle> {
  /** @internal Use Client.parse. */
  constructor(handle: DocumentHandle) { super(handle); }
  get version(): string { return this.handle.version(); }
  get root(): JsonView { return new JsonView(this.handle.root()); }
  withResource(uri: string, source: string): Document {
    return call(() => new Document(this.handle.with_resource(uri, source)));
  }
  operation(id: string): Operation { return call(() => new Operation(this.handle.operation_id(id))); }
  operationAt(pointer: string): Operation { return call(() => new Operation(this.handle.operation_at(pointer))); }
  /** Each yielded handle is owned by the caller and can outlive this document. */
  *operations(): Generator<Discovery<Operation>> {
    const inventory = this.handle.operations();
    try {
      while (true) {
        let next: OperationHandle | undefined;
        try { next = call(() => inventory.next_entry()); }
        catch (error) {
          if (!(error instanceof AsyncApiError)) throw error;
          yield { ok:false, error };
          continue;
        }
        if (!next) return;
        yield { ok:true, value:new Operation(next) };
      }
    } finally { inventory.free(); }
  }
}
export class Operation extends Owner<OperationHandle> {
  /** @internal Use a document selector. */
  constructor(handle: OperationHandle) { super(handle); }
  get identity(): OperationIdentity { return JSON.parse(this.handle.identity_json()) as OperationIdentity; }
  get location(): SourceLocation { return JSON.parse(this.handle.location_json()) as SourceLocation; }
  get authored(): JsonView { return new JsonView(this.handle.authored()); }
  describe(): OperationDescription { return call(() => JSON.parse(this.handle.describe_json()) as OperationDescription); }
  compile(): CompiledOperation { return call(() => new CompiledOperation(this.handle.compile())); }
}
export class CompiledOperation extends Owner<CompiledOperationHandle> {
  /** @internal Use Operation.compile. Owns its snapshot independently. */
  constructor(handle: CompiledOperationHandle) { super(handle); }
  describe(): CompiledDescription { return JSON.parse(this.handle.describe_json()) as CompiledDescription; }
  messageSource(key: string): JsonView | undefined { const value = this.handle.message_source(key); return value ? new JsonView(value) : undefined; }
  serverSource(key: string): JsonView | undefined { const value = this.handle.server_source(key); return value ? new JsonView(value) : undefined; }
  /** Resolves choices in Rust; does not connect, send, or acquire credentials. */
  prepare(options: PlanOptions): Plan { return call(() => new Plan(this.handle.prepare(JSON.stringify(options)))); }
}
export class Plan extends Owner<PlanHandle> {
  /** @internal Use CompiledOperation.prepare. */
  constructor(handle: PlanHandle) { super(handle); }
  describe(): PlanDescription { return JSON.parse(this.handle.describe_json()) as PlanDescription; }
  /** @internal Adds the owning Rust plan to a session builder. */
  attachTo(builder: HostSessionBuilder): void { runtimeCall(() => builder.add(this.handle)); }
}
export class JsonView extends Owner<JsonHandle> {
  /** @internal Obtain a source view from a document or operation. */
  constructor(handle: JsonHandle) { super(handle); }
  get kind(): 'null' | 'boolean' | 'number' | 'string' | 'array' | 'object' {
    return this.handle.kind() as JsonView['kind'];
  }
  get location(): SourceLocation { return JSON.parse(this.handle.location_json()) as SourceLocation; }
  get raw(): string { return this.handle.raw(); }
  get json(): string { return this.handle.json(); }
  get numberText(): string | undefined { return this.handle.number_text(); }
  get string(): string | undefined { return this.handle.string(); }
  get boolean(): boolean | undefined { return this.handle.boolean(); }
  get(key: string): JsonView | undefined { const handle = this.handle.get(key); return handle ? new JsonView(handle) : undefined; }
  at(index: number): JsonView | undefined {
    if (!Number.isSafeInteger(index) || index < 0 || index > 0xffff_ffff) return undefined;
    const handle = this.handle.at(index); return handle ? new JsonView(handle) : undefined;
  }
  pointer(pointer: string): JsonView | undefined { const handle = this.handle.pointer(pointer); return handle ? new JsonView(handle) : undefined; }
}

/** Limits account for Rust-owned queues. Host network buffers are separate. */
export interface HostSessionOptions {
  readonly limits?: { readonly maxMessages?: number; readonly maxBufferedBytes?: number; readonly maxMessageBytes?: number };
  readonly connectTimeoutMs?: number;
  readonly closeTimeoutMs?: number;
}
export interface HostReceipt { readonly kind: 'webSocketHostAccepted' }
export interface HostCloseReceipt { readonly code: number; readonly wasClean: boolean }
export type Incoming =
  | { readonly kind: 'message'; readonly operation: number; readonly payload: Uint8Array }
  | { readonly kind: 'rejected'; readonly reason: string; readonly payloadBytes: number };
export class AsyncApiRuntimeError extends Error {
  readonly code: string;
  readonly deliveryUnknown: boolean;
  constructor(value: { code: string; detail: string; delivery_unknown: boolean }) {
    super(value.detail); this.name = 'AsyncApiRuntimeError';
    this.code = value.code; this.deliveryUnknown = value.delivery_unknown;
  }
}
function runtimeError(error: unknown): unknown {
  if (typeof error !== 'string') return error;
  let value: unknown; try { value = JSON.parse(error); } catch { return error; }
  if (value && typeof value === 'object' && 'code' in value && typeof value.code === 'string'
    && 'detail' in value && typeof value.detail === 'string'
    && 'delivery_unknown' in value && typeof value.delivery_unknown === 'boolean') return new AsyncApiRuntimeError(value as {code:string;detail:string;delivery_unknown:boolean});
  return error;
}
function runtimeCall<T>(operation: () => T): T { try { return operation(); } catch (error) { throw runtimeError(error); } }
async function cancellable<T>(signal: AbortSignal | undefined, operation: (token: CancellationHandle) => Promise<T>): Promise<T> {
  const token = new CancellationHandle();
  const abort = () => token.cancel();
  signal?.addEventListener('abort', abort, {once:true});
  if (signal?.aborted) token.cancel();
  try { return await operation(token); }
  catch (error) { throw runtimeError(error); }
  finally { signal?.removeEventListener('abort', abort); token.free(); }
}
export class HostSender extends Owner<HostSenderHandle> {
  /** @internal Use HostSession.sender. Retaining this does not retain the socket. */
  constructor(handle: HostSenderHandle) { super(handle); }
  /** Returns host-buffer acceptance. Does not promise flush or peer delivery. */
  send(operation: number, payload: Uint8Array): HostReceipt {
    if (!Number.isSafeInteger(operation) || operation < 0 || operation > 0xffff_ffff) throw new AsyncApiRuntimeError({code:'InvalidConfiguration',detail:'Operation index must be a nonnegative 32-bit integer',delivery_unknown:false});
    if (!(payload instanceof Uint8Array)) throw new TypeError('Payload must be a Uint8Array');
    return runtimeCall(() => JSON.parse(this.handle.send(operation, payload)) as HostReceipt);
  }
}
export class HostSession extends Owner<HostSessionHandle> {
  /** @internal Use Client.openSession. Dispose explicitly or await close. */
  constructor(handle: HostSessionHandle) { super(handle); }
  sender(): HostSender { return runtimeCall(() => new HostSender(this.handle.sender())); }
  get usage(): { readonly messages: number; readonly bytes: number } { return runtimeCall(() => JSON.parse(this.handle.usage_json())); }
  async next(options: { signal?: AbortSignal } = {}): Promise<Incoming | undefined> {
    return cancellable(options.signal, async token => {
      const value = await this.handle.next(token) as IncomingHandle | undefined;
      if (!value) return undefined;
      try {
        const metadata = JSON.parse(value.metadata_json()) as {kind:'message';operation:number} | {kind:'rejected';reason:string;payloadBytes:number};
        if (metadata.kind === 'message') return {kind:'message',operation:metadata.operation,payload:value.take_payload()!};
        return metadata;
      } finally { value.free(); }
    });
  }
  async close(options: { signal?: AbortSignal } = {}): Promise<HostCloseReceipt> {
    try { return await cancellable(options.signal, async token => JSON.parse(await this.handle.close(token) as string) as HostCloseReceipt); }
    finally { this.dispose(); }
  }
}
