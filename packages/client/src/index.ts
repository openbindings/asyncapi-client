import initializeWasm, { DocumentHandle, OperationHandle, CompiledOperationHandle, PlanHandle, JsonHandle, type InitInput } from '../wasm/asyncapi.js';

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
export interface Client {
  parse(source: string, options?: { sourceUri?: string }): Document;
}
class RustClient implements Client {
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
