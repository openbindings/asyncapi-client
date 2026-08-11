/** A protocol-native execution plug-in for one or more AsyncAPI server protocols. */
export interface AsyncAPIProtocolDriver {
  readonly protocols: readonly string[];
  execute(
    request: AsyncAPIProtocolDriverRequest,
    session: AsyncAPIProtocolDriverSession,
  ): void | Promise<void>;
}

/** Artifact and target facts supplied to a protocol driver. */
export interface AsyncAPIProtocolDriverRequest {
  readonly document: Readonly<Record<string, unknown>>;
  readonly operation: Readonly<Record<string, unknown>>;
  readonly channel?: Readonly<Record<string, unknown>>;
  readonly ref: string;
  readonly operationKey: string;
  readonly action: "send" | "receive";
  readonly protocol: string;
  readonly serverURL: string;
  readonly context?: Readonly<Record<string, unknown>>;
  readonly signal: AbortSignal;
}

/** Cardinality-neutral lifecycle surface used by protocol drivers. */
export interface AsyncAPIProtocolDriverSession {
  readonly inputs: AsyncIterable<unknown>;
  readonly signal: AbortSignal;
  closeInput(): Promise<void>;
  emit(value: unknown): Promise<void>;
  setLeadingMetadata(metadata: Record<string, string[]>): void;
  setTrailingMetadata(metadata: Record<string, string[]>): void;
  complete(): void;
}

export function indexProtocolDrivers(
  drivers: readonly AsyncAPIProtocolDriver[] | undefined,
): ReadonlyMap<string, AsyncAPIProtocolDriver> {
  const indexed = new Map<string, AsyncAPIProtocolDriver>();
  for (const driver of drivers ?? []) {
    if (driver.protocols.length === 0) {
      throw new Error("an AsyncAPI protocol driver must declare at least one protocol");
    }
    for (const declared of driver.protocols) {
      const protocol = declared.trim().toLowerCase();
      if (!protocol) throw new Error("an AsyncAPI protocol driver declared an empty protocol");
      if (indexed.has(protocol)) {
        throw new Error(`more than one AsyncAPI protocol driver declares ${JSON.stringify(protocol)}`);
      }
      indexed.set(protocol, driver);
    }
  }
  return indexed;
}
