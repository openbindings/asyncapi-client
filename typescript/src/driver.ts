/** A protocol-native execution plug-in for one or more AsyncAPI server protocols. */
export interface AsyncAPIProtocolDriver {
  readonly protocols: readonly string[];
  /** Declares native per-message header carriage: the driver consumes the
   *  unit seam (encodeUnit/decodeUnit), so the routed envelope's
   *  application headers ride the protocol (§9.2 per-cell capability).
   *  Absent or false, a headers-declaring direction refuses before
   *  dispatch. */
  readonly carriesMessageHeaders?: boolean;
  execute(
    request: AsyncAPIProtocolDriverRequest,
    session: AsyncAPIProtocolDriverSession,
  ): void | Promise<void>;
}

/** One message crossing the driver seam: payload octets plus the
 *  protocol's per-message header pairs. */
export interface AsyncAPIDriverUnit {
  readonly payload: Uint8Array;
  readonly headers: readonly AsyncAPIDriverHeader[];
}

/** One protocol header pair. Values are raw octets — the protocol's
 *  spelling; the client renders and projects application scalars at the
 *  envelope boundary. */
export interface AsyncAPIDriverHeader {
  readonly key: string;
  readonly value: Uint8Array;
}

/** Artifact and target facts supplied to a protocol driver. */
export interface AsyncAPIProtocolDriverRequest {
  readonly document: Readonly<Record<string, unknown>>;
  readonly operation: Readonly<Record<string, unknown>>;
  readonly server?: Readonly<Record<string, unknown>>;
  /** The caller-to-application lane, when the operation accepts values. */
  readonly input?: AsyncAPIProtocolDriverInput;
  /** The application-to-caller lane, when the operation emits values. */
  readonly output?: AsyncAPIProtocolDriverOutput;
  /** Resolved AsyncAPI security alternatives. Each outer item is one
   *  satisfiable alternative; every scheme inside that item applies. */
  readonly securityAlternatives: readonly (readonly AsyncAPIResolvedSecurityScheme[])[];
  readonly ref: string;
  readonly operationKey: string;
  readonly action: "send" | "receive";
  readonly protocol: string;
  readonly serverURL: string;
  readonly context?: Readonly<Record<string, unknown>>;
  readonly signal: AbortSignal;
}

/** Resolved artifact facts shared by one invocation direction. */
export interface AsyncAPIProtocolDriverDirection {
  readonly channel?: Readonly<Record<string, unknown>>;
  readonly server?: Readonly<Record<string, unknown>>;
  readonly protocol: string;
  readonly serverURL: string;
  /** Fully expanded address when it is knowable before a message exists. */
  readonly address?: string;
  /** Governing, resolved message declarations in artifact order. */
  readonly messages: readonly Readonly<Record<string, unknown>>[];
}

export interface AsyncAPIProtocolDriverInput extends AsyncAPIProtocolDriverDirection {
  /** Serializes one application value to the exact wire octets, consulting
   *  the consumer codec seam before the built-in lane (so it may resolve
   *  asynchronously, like decode). Refuses a headers-bearing value rather
   *  than dropping it — a header-carrying driver uses encodeUnit. */
  readonly encode: (value: unknown) => Uint8Array | Promise<Uint8Array>;
  /** Renders one caller input value into its complete wire unit: payload
   *  octets plus the routed envelope's application headers as protocol
   *  header pairs (empty when the governing message declares none). */
  readonly encodeUnit: (value: unknown) => AsyncAPIDriverUnit | Promise<AsyncAPIDriverUnit>;
}

export interface AsyncAPIProtocolDriverOutput extends AsyncAPIProtocolDriverDirection {
  readonly decode: (payload: Uint8Array) => Promise<unknown>;
  /** Decodes one received wire unit: the client pairs the payload with the
   *  DECLARED application headers projected from the received pairs (the
   *  routed envelope on the output direction). */
  readonly decodeUnit: (unit: AsyncAPIDriverUnit) => Promise<unknown>;
}

export interface AsyncAPIResolvedSecurityScheme {
  /** components.securitySchemes key, when the declaration was addressable. */
  readonly name?: string;
  readonly scheme: Readonly<Record<string, unknown>>;
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
