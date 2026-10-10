import initializeWasm, { DocumentHandle, OperationHandle, JsonHandle, type InitInput } from '../wasm/asyncapi.js';

export interface SourceLocation {
  readonly uri: string | null;
  readonly pointer: string;
  readonly bytes: { readonly start: number; readonly end: number };
}
export interface OperationIdentity { readonly uri: string | null; readonly pointer: string }
export type Requirement = { readonly kind: 'sourceUri' } | { readonly kind: 'resource'; readonly uri: string };
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
  /** Parses original JSON source; admission is not full document validation. */
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
}
export class JsonView extends Owner<JsonHandle> {
  /** @internal Obtain a source view from a document or operation. */
  constructor(handle: JsonHandle) { super(handle); }
  get kind(): 'null' | 'boolean' | 'number' | 'string' | 'array' | 'object' {
    return this.handle.kind() as JsonView['kind'];
  }
  get location(): SourceLocation { return JSON.parse(this.handle.location_json()) as SourceLocation; }
  get raw(): string { return this.handle.raw(); }
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
