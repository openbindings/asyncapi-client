/**
 * AsyncAPI binding execution over the cardinality-agnostic invocation handle.
 *
 * The action is read from the DESCRIBED APPLICATION's perspective — AsyncAPI
 * 3.0's own rule — and an invocation is the counterparty (ASYNC-P-02,
 * spec/binding-specs/asyncapi): invoking a `send` operation SUBSCRIBES to
 * what the application sends; invoking a `receive` operation PUBLISHES what
 * the application expects to receive. The artifact is never read as
 * describing the invoker.
 *
 * One entrypoint ({@link runBinding}) drives every cell against the
 * binding-facing {@link BindingHandle}:
 *
 *   - receive + http/https  unary publish: one input -> request body using
 *                           the http binding's declared method,
 *                           response -> at most one output
 *   - receive + ws/wss      client-streaming publish: every input -> one
 *                           socket frame; the caller closing input ends it
 *   - send + http/https     excluded by the built-in HTTP driver
 *   - send + ws/wss         server-streaming subscription: socket frames
 *                           -> outputs, no caller input values
 *
 * All pre-dispatch failures (bad ref, no resolvable server, missing
 * context, an unresolved address or server variable, an unsatisfied
 * ws-binding declaration, an excluded input content family, missing publish
 * input) are raised via `fireError` BEFORE any network I/O, per the
 * binding-author contract and ASYNC-P-02/-03/-04's pre-dispatch refusals.
 */

import {
  InvocationError,
  contextRequiredError,
  configValueRequirement,
  contextSatisfies,
  contextBearerTokenFor,
  contextApiKeyFor,
  contextBasicAuthFor,
  contextAccessTokenFor,
  contextHeaders,
  contextCookies,
  contextConfiguration,
  httpErrorCode,
  ERR_INVALID_REF,
  ERR_PROTOCOL,
  ERR_SOURCE_CONFIG_ERROR,
  ERR_REF_NOT_FOUND,
  ERR_MISSING_INPUT,
  ERR_CONNECT_FAILED,
  ERR_RESPONSE_ERROR,
  ERR_STREAM_ERROR,
  ERR_VALIDATION_FAILED,
  type BindingHandle,
  type BindingInvocationArgs,
  type ContextAlternative,
  type ContextRequirement,
  type ContextRequiredDetails,
  type Metadata,
  isJSONContentType,
  decodeThroughHooks,
  encodeThroughHooks,
  resolveDeliveryUnitLimit,
  type InvokeHooks,
  type InvokeSite,
  type OutputDecoder,
  type RawResult,
  ERR_REFUSED,
} from "./internal/index.js";
import type {
  AsyncAPIChannel,
  AsyncAPIDocument,
  AsyncAPIMessage,
  AsyncAPIOperation,
  AsyncAPIOAuthFlow,
  AsyncAPIOAuthFlows,
  AsyncAPISecurityScheme,
  AsyncAPIServer,
} from "./asyncapi-types.js";
import { isSecurityScheme } from "./asyncapi-types.js";
import { REF_NAME_TAG } from "./constants.js";
import {
  mergeQuery,
  protocolFieldValues,
  requestMethod,
  resolveHTTPQuery,
  resolveWSUpgrade,
} from "./bindings.js";
import { AvroBinaryCodec } from "./avro.js";
import { headerFieldText, locationlessParamNames, projectResponseHeaders, splitInputEnvelope } from "./content.js";
import {
  decodeContentType,
  encodeInput,
  governingMessages,
  messagesForDecodeCT,
  isWellFormedUnicode,
  normalizeMediaType,
  resolveReplyContentType,
  replyGoverningMessages,
  validateMessageBindingVersion,
  resolveInputCodec,
  selectedInputMessages,
  type InputCodec,
  isBytesContentType,
  encodeBase64,
} from "./content.js";
import { streamSSE } from "./sse.js";
import { replyMessagesBindable } from "./authoring.js";
import {
  addressConfiguration,
  channelNameOf,
  ConfigRequired,
  joinURL,
  resolveAddress,
  resolveTarget,
  type ResolvedTarget,
} from "./target.js";

/**
 * Maps a resolution failure to the right terminal: a resolvable-missing
 * configuration value (a ConfigRequired signal) becomes a config.value
 * CONTEXT_REQUIRED challenge — retryable after resolution (R1a) — while any
 * other error stays a terminal ERR_SOURCE_CONFIG_ERROR. resolveTarget/
 * resolveAddress already consulted the supplied context and found the value
 * absent, so the challenge fires unconditionally; the operation-invoker's
 * bounded resolve-and-retry loop is the backstop. The challenge target is
 * the engine-asserted scope for the missing value (the context-scope model,
 * ratified 2026-08-19): the resolved server URL when known (empty when
 * server resolution itself failed), else the strongest host hint the
 * artifact provides, else the threaded source location — the artifact-bound
 * identity a point that precedes destination resolution naturally scopes
 * to. The location rides verbatim: this client has no location canonicalizer
 * of its own. A content-only source with no location asserts nothing and
 * the target stays empty. Configuration is not assumed public, so a
 * resolver decides whether the asserted scope is sufficient.
 */
function configOrSourceError(e: unknown, serverURL: string, sourceLocation: string): InvocationError {
  if (e instanceof ConfigRequired) {
    const target = serverURL || e.hostHint || sourceLocation || "";
    return contextRequiredError(e.message, {
      target,
      alternatives: [
        {
          requirements: [
            configValueRequirement(e.point, e.path, e.message, e.schema, e.durable),
          ],
        },
      ],
    });
  }
  return new InvocationError(ERR_REFUSED, errorMessage(e));
}
import { parseRef, errorMessage } from "./util.js";
import type { PooledWS, WSPool } from "./ws-pool.js";
import type {
  AsyncAPIDriverHeader,
  AsyncAPIDriverUnit,
  AsyncAPIProtocolDriver,
  AsyncAPIProtocolDriverSession,
} from "./driver.js";

/**
 * STAYS FIXED under the delivery-unit knob: this constant now caps only the
 * DIAGNOSTICS-side failure-body capture (readErrorBody) — error details,
 * never an emitted output value, so the consumer bound does not apply.
 */
const MAX_RESPONSE_BYTES = 10 * 1024 * 1024;
let nextWebSocketReplySession = 0;

type Handle = BindingHandle<unknown, unknown>;

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

/**
 * Resolves the operation, checks runtime context, and dispatches to the
 * protocol-specific runner. Terminates the handle exactly once.
 */
export async function runBinding(
  args: BindingInvocationArgs,
  h: Handle,
  doc: AsyncAPIDocument,
  wsPool: WSPool,
): Promise<void> {
  let opID: string;
  try {
    opID = parseRef(args.ref);
  } catch (e: unknown) {
    h.fireError(new InvocationError(ERR_INVALID_REF, errorMessage(e)));
    return;
  }

  const asyncOp = (doc.operations ?? {})[opID];
  if (!asyncOp) {
    h.fireError(
      new InvocationError(ERR_REF_NOT_FOUND, `operation "${opID}" not in AsyncAPI doc`),
    );
    return;
  }

  // The binding target is the addressed operation's channel (§8), reached
  // through a resolved server and expanded address (§9.2). A channel `$ref`
  // the dereferencer could not resolve is no channel at all.
  const ch = resolvedChannel(asyncOp.channel);
  const channelName = channelNameOf(ch);

  let target: ResolvedTarget;
  try {
    target = resolveTarget(doc, ch, args.context);
  } catch (e: unknown) {
    h.fireError(configOrSourceError(e, "", args.source.location ?? ""));
    return;
  }

  const externalDriver = args.protocolDrivers?.get(target.protocol);
  if (!["http", "https", "ws", "wss"].includes(target.protocol)) {
    if (!externalDriver) {
      h.fireError(
        new InvocationError(
          "DRIVER_UNAVAILABLE",
          `no AsyncAPI protocol driver is installed for ${JSON.stringify(target.protocol)}`,
        ),
      );
      return;
    }
  }
  if (!externalDriver && (
    (asyncOp as unknown as Record<string, unknown>)["x-ob-asyncapi-v2-security-conjunction"] !== undefined
    || (target.securityServer as unknown as Record<string, unknown> | undefined)?.["x-ob-asyncapi-v2-security-conjunction"] !== undefined
  )) {
    h.fireError(
      new InvocationError(
        ERR_REFUSED,
        "the built-in driver cannot preserve this AsyncAPI 2.x multi-scheme security conjunction",
      ),
    );
    return;
  }

  if (!externalDriver) {
    try {
      validateCell(doc, ch, asyncOp, target.protocol, args.source.profile, args.context);
    } catch (e: unknown) {
      h.fireError(new InvocationError(ERR_REFUSED, errorMessage(e)));
      return;
    }
  }

  // Context negotiation: challenge BEFORE any connection is opened. The
  // requirements derive from the selected artifact server (§9.5), including
  // when configuration replaces only its connection target.
  const required = requiredContext(asyncOp, target.securityServer, target.serverURL, args.context);
  if (required) {
    h.fireError(
      contextRequiredError(
        `operation "${opID}" requires credentials the context does not provide`,
        required,
      ),
    );
    return;
  }

  if (!externalDriver) {
    try {
      validateCredentialDestinations(asyncOp, target.securityServer, target.protocol, args.context);
    } catch (e: unknown) {
      h.fireError(new InvocationError(ERR_REFUSED, errorMessage(e)));
      return;
    }
  }

  // Reply routing is artifact/configuration authority, not application input.
  // Resolve it before any address expression can request a caller value so an
  // unsupported route is a side-effect-free pre-dispatch refusal.
  let preparedWSReplyLane: WebSocketReplyLane | undefined;
  if (!externalDriver && (target.protocol === "ws" || target.protocol === "wss") && asyncOp.reply) {
    try {
      preparedWSReplyLane = resolveWebSocketReplyLane(doc, asyncOp, target, args.context);
    } catch (e: unknown) {
      h.fireError(new InvocationError(ERR_REFUSED, errorMessage(e)));
      return;
    }
  }

  // The address configuration point (ASYNC-P-04): the declared address with
  // every {name} expression expanded — an absent address or an unresolved
  // expression is a pre-dispatch refusal, never a guess.
  let address: string;
  let preparedInput: { ok: true; value: unknown } | undefined;
  let preparedHeaders: Record<string, unknown> | undefined;
  let resolvedAddrParams: Record<string, string> = {};
  let inputHeadersDeclared = false;
  if (asyncOp.action === "receive") {
    try {
      inputHeadersDeclared = selectedInputMessages(asyncOp, ch, args.context).some((m) => m.headers !== undefined);
    } catch {
      // Selection defects surface at codec resolution with their own codes.
    }
  }
  try {
    const addrCfg = addressConfiguration(args.context);
    const needsPayload = channelNeedsOutgoingPayload(ch);
    // The routed envelope (§9.2): a parameterized channel's publish input
    // arrives as {payload, <params>}; the parameter fields must be read
    // before the address can be spelled, exactly like a payload-derived
    // location expression.
    const needsEnvelopeSplit = asyncOp.action === "receive"
      && (locationlessParamNames(ch).length > 0 || inputHeadersDeclared)
      && !noInputDeclared(args);
    if (needsPayload || needsEnvelopeSplit) {
      if (needsPayload && asyncOp.action !== "receive") throw new Error("subscription address uses a message runtime expression before any outgoing message exists");
      if (needsPayload && !externalDriver && target.protocol !== "http" && target.protocol !== "https") {
        throw new Error("WebSocket publish address runtime expressions are not available before connection in the built-in driver");
      }
      if (needsPayload && noInputDeclared(args)) throw new Error("address runtime expression requires an outgoing message, but the operation declares no input");
      const first = await readFirstInput(h);
      if (!first.ok) {
        if (needsPayload) throw new Error("address runtime expression requires an outgoing message, but invocation input is absent");
        throw new Error("the parameterized channel's input is the routed envelope, but invocation input is absent");
      }
      await h.closeInput();
      preparedInput = first;
    }
    if (needsEnvelopeSplit && preparedInput !== undefined) {
      let selected: AsyncAPIMessage[] = [];
      try {
        selected = selectedInputMessages(asyncOp, ch, args.context);
      } catch {
        // Selection defects surface at codec resolution with their own
        // codes; the split only needs the headers declaration.
      }
      const split = splitInputEnvelope(ch, selected, preparedInput.value);
      if (split.envelope) {
        addrCfg.parameters = { ...addrCfg.parameters, ...split.params };
        resolvedAddrParams = { ...addrCfg.parameters };
        if (externalDriver === undefined) {
          preparedInput = { ok: true, value: split.payload };
          if (split.headers !== undefined) preparedHeaders = split.headers;
        }
        // A driver lane keeps the RAW envelope value: its per-unit seam
        // splits every unit uniformly (the first pre-read unit must not
        // arrive pre-split while later units arrive whole).
      }
    }
    if (Object.keys(resolvedAddrParams).length === 0 && addrCfg.parameters) {
      resolvedAddrParams = { ...addrCfg.parameters };
    }
    address = resolveAddress(ch, channelName, addrCfg, preparedInput?.value);
  } catch (e: unknown) {
    h.fireError(configOrSourceError(e, target.serverURL, args.source.location ?? ""));
    return;
  }

  // Header carriage is per protocol cell (§9.2): the built-in HTTP lane
  // carries the envelope's headers as HTTP fields; every other cell in
  // this build (driver protocols, raw WebSocket frames) has no native
  // carriage qualified yet and refuses before dispatch.
  if (inputHeadersDeclared) {
    const carried = externalDriver !== undefined
      ? externalDriver.carriesMessageHeaders === true
      : target.protocol === "http" || target.protocol === "https";
    if (!carried) {
      h.fireError(new InvocationError(
        ERR_REFUSED,
        `the input declares application headers; this build has no header carriage for the ${JSON.stringify(target.protocol)} protocol cell`,
      ));
      return;
    }
  }

  // The complementary perspective (ASYNC-P-02): `receive` means the
  // described application receives, so invoking PUBLISHES; `send` means it
  // sends, so invoking SUBSCRIBES.
  if (asyncOp.action !== "receive" && asyncOp.action !== "send") {
    h.fireError(
      new InvocationError(
        ERR_REFUSED,
        `unknown action "${(asyncOp as { action: string }).action}"`,
      ),
    );
    return;
  }

  if (externalDriver) {
    await runExternalDriver(
      externalDriver,
      args,
      h,
      doc,
      opID,
      asyncOp,
      ch,
      target,
      address,
      resolvedAddrParams,
      preparedInput,
    );
    return;
  }

  switch (target.protocol) {
    case "ws":
    case "wss": {
      // The websockets channel binding governs the upgrade request where it
      // speaks (§8): declared query and header values, supplied like
      // address parameters, with unsatisfied required declarations a
      // pre-dispatch refusal.
      let up: ReturnType<typeof resolveWSUpgrade>;
      try {
        const fields = protocolFieldValues(args.context);
        up = resolveWSUpgrade(ch, channelName, fields.webSocketQuery, fields.webSocketHeaders);
      } catch (e: unknown) {
        h.fireError(new InvocationError(ERR_REFUSED, errorMessage(e)));
        return;
      }
      const dialAddress = mergeQuery(address, up.query);
      if (asyncOp.reply) {
        const replyLane = preparedWSReplyLane;
        let inputMessages: AsyncAPIMessage[];
        let outputMessages: AsyncAPIMessage[];
        try {
          if (!replyLane) throw new Error("WebSocket reply route was not prepared");
          const replyChannel = resolvedChannel(asyncOp.reply.channel);
          inputMessages = asyncOp.action === "receive"
            ? selectedInputMessages(asyncOp, ch, args.context)
            : selectedInputMessages(
                { action: "receive", messages: asyncOp.reply.messages },
                replyChannel,
                args.context,
              );
          outputMessages = asyncOp.action === "receive"
            ? replyGoverningMessages(asyncOp)
            : governingMessages(asyncOp, ch);
        } catch (e: unknown) {
          h.fireError(new InvocationError(ERR_REFUSED, errorMessage(e)));
          return;
        }
        const operationLane: WebSocketReplyLane = { target, dialAddress, headers: up.headers };
        const inputLane = asyncOp.action === "receive" ? operationLane : replyLane;
        const outputLane = asyncOp.action === "receive" ? replyLane : operationLane;
        await runWSSubscribe(
          wsPool,
          outputLane.target,
          outputLane.dialAddress,
          outputLane.headers,
          doc,
          ch,
          asyncOp,
          args,
          h,
          {
            inputMessages,
            outputMessages,
            requireInput: asyncOp.action === "receive",
            isolationKey: `reply-${++nextWebSocketReplySession}`,
            inputLane,
          },
        );
        return;
      }
      if (asyncOp.action === "receive") {
        await runWSPublish(wsPool, target, dialAddress, up.headers, doc, ch, asyncOp, args, h);
      } else {
        await runWSSubscribe(wsPool, target, dialAddress, up.headers, doc, ch, asyncOp, args, h);
      }
      return;
    }
    case "http":
    case "https":
      if (asyncOp.action === "receive") {
        await runUnaryPublish(target, address, doc, ch, asyncOp, args, h, preparedInput, preparedHeaders);
      } else {
        await runSSESubscribe(target, address, doc, ch, asyncOp, args, h);
      }
      return;
    default:
      // resolveTarget only yields bound protocols; defensive.
      h.fireError(
        new InvocationError(
          ERR_REFUSED,
          `protocol "${target.protocol}" is not bound by the supported openbindings.asyncapi revisions (supported: http, https, ws, wss)`,
        ),
      );
  }
}

interface WebSocketReplyLane {
  target: ResolvedTarget;
  dialAddress: string;
  headers?: Record<string, string>;
}

function resolveWebSocketReplyLane(
  doc: AsyncAPIDocument,
  operation: AsyncAPIOperation,
  operationTarget: ResolvedTarget,
  context?: Record<string, unknown>,
): WebSocketReplyLane {
  const reply = operation.reply;
  if (!reply) throw new Error("operation has no reply");
  if (reply.address?.location) {
    const source = reply.address.location.startsWith("$message.header#") ? "application header" : "runtime expression";
    throw new Error(`WebSocket reply address uses an ${source} that the current payload-only session profile cannot resolve`);
  }
  const replyChannel = resolvedChannel(reply.channel);
  if (!replyChannel) throw new Error("WebSocket reply has no resolved reply channel");
  const bindingVersion = replyChannel.bindings?.ws?.bindingVersion;
  if (bindingVersion !== undefined && bindingVersion !== "0.1.0") {
    throw new Error(`reply WebSockets binding version ${JSON.stringify(bindingVersion)} is outside the built-in WebSocket driver's 0.1.0 envelope`);
  }
  const replyTarget = resolveTarget(doc, replyChannel, context);
  if ((replyTarget.protocol !== "ws" && replyTarget.protocol !== "wss")
    || replyTarget.protocol !== operationTarget.protocol
    || replyTarget.serverURL !== operationTarget.serverURL) {
    throw new Error("WebSocket reply channel selects a different protocol or server; cross-target reply sessions are not qualified");
  }
  const replyAddress = resolveAddress(replyChannel, channelNameOf(replyChannel), { address: "" });
  const fields = protocolFieldValues(context);
  const replyUpgrade = resolveWSUpgrade(
    replyChannel,
    channelNameOf(replyChannel),
    fields.webSocketQuery,
    fields.webSocketHeaders,
  );
  const replyDialAddress = mergeQuery(replyAddress, replyUpgrade.query);
  return { target: replyTarget, dialAddress: replyDialAddress, headers: replyUpgrade.headers };
}

async function runExternalDriver(
  driver: AsyncAPIProtocolDriver,
  args: BindingInvocationArgs,
  h: Handle,
  doc: AsyncAPIDocument,
  operationKey: string,
  operation: AsyncAPIOperation,
  channel: AsyncAPIChannel | undefined,
  target: ResolvedTarget,
  address: string,
  resolvedAddrParams: Record<string, string>,
  preparedInput: { ok: true; value: unknown } | undefined,
): Promise<void> {
  let input: {
    channel?: Readonly<Record<string, unknown>>;
    server?: Readonly<Record<string, unknown>>;
    protocol: string;
    serverURL: string;
    address?: string;
    messages: readonly Readonly<Record<string, unknown>>[];
    encode: (value: unknown) => Uint8Array | Promise<Uint8Array>;
    encodeUnit: (value: unknown) => AsyncAPIDriverUnit | Promise<AsyncAPIDriverUnit>;
  } | undefined;
  let output: {
    channel?: Readonly<Record<string, unknown>>;
    server?: Readonly<Record<string, unknown>>;
    protocol: string;
    serverURL: string;
    address?: string;
    messages: readonly Readonly<Record<string, unknown>>[];
    decode: (payload: Uint8Array) => Promise<unknown>;
    decodeUnit: (unit: AsyncAPIDriverUnit) => Promise<unknown>;
  } | undefined;
  try {
    const replyChannel = resolvedChannel(operation.reply?.channel);
    const replyMessages = replyGoverningMessages(operation);
    const replyTarget = replyChannel ? resolveTarget(doc, replyChannel, args.context) : undefined;
    const replyAddress = replyChannel && !operation.reply?.address?.location
      ? channelNameOf(replyChannel) === channelNameOf(channel)
        ? address
        : resolveAddress(replyChannel, channelNameOf(replyChannel), { address: "" })
      : undefined;
    const inputChannel = operation.action === "receive" ? channel : replyChannel;
    const inputTarget = operation.action === "receive" ? target : replyTarget;
    const inputAddress = operation.action === "receive" ? address : replyAddress;
    const inputMessages = operation.action === "receive"
      ? selectedInputMessages(operation, channel, args.context)
      : operation.reply
        ? selectedInputMessages(
            { action: "receive", messages: operation.reply.messages },
            replyChannel,
            args.context,
          )
        : [];
    if (inputMessages.length > 0) {
      const codec = resolveInputCodec(doc, inputMessages, args.context);
      // Every unit splits the routed envelope uniformly: the payload rides
      // the codec lanes, per-unit address parameters must match the
      // invocation's resolved addressing, and application headers become
      // protocol pairs on the unit seam (Go twin: installDriverInputSeams).
      const splitUnit = (value: unknown): { payload: unknown; headers?: Record<string, unknown> } => {
        const split = splitInputEnvelope(inputChannel, inputMessages, value);
        if (!split.envelope) return { payload: value };
        for (const [name, supplied] of Object.entries(split.params)) {
          if (resolvedAddrParams[name] !== supplied) {
            throw new Error(`envelope parameter ${JSON.stringify(name)} = ${JSON.stringify(supplied)} does not match the invocation's resolved addressing (${JSON.stringify(resolvedAddrParams[name])}): an address cannot vary per unit within one dispatch`);
          }
        }
        return split.headers !== undefined ? { payload: split.payload, headers: split.headers } : { payload: split.payload };
      };
      const encodePayload = (payload: unknown) =>
        encodeThroughHooks(args.hooks, siteFor(args, target.serverURL), payload, (v) => {
          const out = encodeInput(codec, v);
          return typeof out === "string" ? new TextEncoder().encode(out) : out;
        });
      input = {
        ...(inputChannel
          ? { channel: inputChannel as unknown as Readonly<Record<string, unknown>> }
          : {}),
        ...(inputTarget?.securityServer
          ? { server: inputTarget.securityServer as unknown as Readonly<Record<string, unknown>> }
          : {}),
        protocol: inputTarget?.protocol ?? target.protocol,
        serverURL: inputTarget?.serverURL ?? target.serverURL,
        ...(inputAddress !== undefined ? { address: inputAddress } : {}),
        messages: inputMessages as unknown as readonly Readonly<Record<string, unknown>>[],
        encode: async (value) => {
          const unit = splitUnit(value);
          if (unit.headers !== undefined) {
            throw new Error("the value carries application headers; this driver lane consumes the payload-only seam and cannot carry them");
          }
          return encodePayload(unit.payload);
        },
        encodeUnit: async (value) => {
          const unit = splitUnit(value);
          const payload = await encodePayload(unit.payload);
          const pairs: AsyncAPIDriverHeader[] = [];
          for (const name of Object.keys(unit.headers ?? {}).sort()) {
            pairs.push({ key: name, value: new TextEncoder().encode(headerFieldText(name, unit.headers![name])) });
          }
          return { payload, headers: pairs };
        },
      };
    }

    const outputChannel = operation.action === "send" ? channel : replyChannel;
    const outputTarget = operation.action === "send" ? target : replyTarget;
    const outputAddress = operation.action === "send" ? address : replyAddress;
    const outputMessages = operation.action === "send"
      ? governingMessages(operation, channel)
      : replyMessages;
    if (outputMessages.length > 0) {
      const contentType = decodeContentType(doc, outputMessages, args.context, driver.carriesMessageHeaders === true);
      const outputAvro = AvroBinaryCodec.resolve(outputMessages, args.context);
      const limit = resolveDeliveryUnitLimit(args);
      const outputHeadersDeclared = outputMessages.some((m) => m.headers !== undefined);
      output = {
        ...(outputChannel
          ? { channel: outputChannel as unknown as Readonly<Record<string, unknown>> }
          : {}),
        ...(outputTarget?.securityServer
          ? { server: outputTarget.securityServer as unknown as Readonly<Record<string, unknown>> }
          : {}),
        protocol: outputTarget?.protocol ?? target.protocol,
        serverURL: outputTarget?.serverURL ?? target.serverURL,
        ...(outputAddress !== undefined ? { address: outputAddress } : {}),
        messages: outputMessages as unknown as readonly Readonly<Record<string, unknown>>[],
        decode: decodePayloadUnit,
        decodeUnit: async (unit) => {
          const value = await decodePayloadUnit(unit.payload);
          if (!outputHeadersDeclared) return value;
          // The routed envelope on the output direction: the payload pairs
          // with the DECLARED application headers projected from the
          // received protocol pairs (§9.2).
          const received = new Headers();
          for (const pair of unit.headers) {
            received.set(pair.key, new TextDecoder("utf-8", { fatal: true }).decode(pair.value));
          }
          return { payload: value, headers: projectResponseHeaders(outputMessages, received) };
        },
      };
      async function decodePayloadUnit(payload: Uint8Array): Promise<unknown> {
        if (payload.byteLength > limit) {
          throw new Error(`delivery unit exceeds configured ${limit}-byte limit`);
        }
        const raw: RawResult = isBytesContentType(contentType)
          // The byte boundary: exact octets, never a UTF-8 text decode.
          ? { status: null, body: "", bodyBytes: payload, meta: {} }
          : { status: null, body: new TextDecoder("utf-8", { fatal: true }).decode(payload), meta: {} };
        return decodeThroughHooks(
          args.hooks,
          siteFor(args, target.serverURL),
          raw,
          builtinDecodeFor(contentType, outputAvro),
        );
      }
    }
  } catch (error: unknown) {
    h.fireError(new InvocationError(ERR_REFUSED, errorMessage(error)));
    return;
  }

  let completed = false;
  const session: AsyncAPIProtocolDriverSession = {
    inputs: driverInputs(preparedInput, h),
    signal: h.signal,
    closeInput: () => h.closeInput(),
    emit: (value) => h.emitOutput(value),
    setLeadingMetadata: (metadata) => h.setHeader(metadata),
    setTrailingMetadata: (metadata) => h.setTrailer(metadata),
    complete: () => {
      if (completed) return;
      completed = true;
      h.closeOutput();
    },
  };
  try {
    await driver.execute(
      {
        document: doc as unknown as Readonly<Record<string, unknown>>,
        operation: operation as unknown as Readonly<Record<string, unknown>>,
        ...(target.securityServer
          ? { server: target.securityServer as unknown as Readonly<Record<string, unknown>> }
          : {}),
        ...(input ? { input } : {}),
        ...(output ? { output } : {}),
        securityAlternatives: driverSecurityAlternatives(operation, target.securityServer),
        ref: args.ref,
        operationKey,
        action: operation.action,
        protocol: target.protocol,
        serverURL: target.serverURL,
        context: args.context,
        signal: h.signal,
      },
      session,
    );
    session.complete();
  } catch (error: unknown) {
    h.fireError(
      new InvocationError(
        "DRIVER_FAILED",
        errorMessage(error),
        undefined,
        { protocol: target.protocol },
      ),
    );
  }
}

async function* driverInputs(
  prepared: { ok: true; value: unknown } | undefined,
  h: Handle,
): AsyncIterable<unknown> {
  if (prepared) yield prepared.value;
  for await (const value of h.inputs()) yield value;
}

/** The operation's resolved channel object, or undefined when the channel
 *  `$ref` did not resolve (the dereferencer leaves dangling refs in place). */
function resolvedChannel(ch: AsyncAPIChannel | undefined): AsyncAPIChannel | undefined {
  if (!ch) return undefined;
  if (typeof (ch as unknown as Record<string, unknown>).$ref === "string") return undefined;
  return ch;
}

function validateCell(
  doc: AsyncAPIDocument,
  ch: AsyncAPIChannel | undefined,
  op: AsyncAPIOperation,
  protocol: string,
  _profile: object,
  context?: Record<string, unknown>,
): void {
  const httpBinding = op.bindings?.http;
  if (httpBinding?.bindingVersion !== undefined && httpBinding.bindingVersion !== "0.3.0") {
    throw new Error(`HTTP binding version ${JSON.stringify(httpBinding.bindingVersion)} is outside the built-in HTTP driver's 0.3.0 envelope`);
  }
  const wsBinding = ch?.bindings?.ws;
  if (wsBinding?.bindingVersion !== undefined && wsBinding.bindingVersion !== "0.1.0") {
    throw new Error(`WebSockets binding version ${JSON.stringify(wsBinding.bindingVersion)} is outside the built-in WebSocket driver's 0.1.0 envelope`);
  }

  if (protocol === "http" || protocol === "https") {
    if (op.action === "send") throw new Error("standalone HTTP send operations are not implemented by the built-in HTTP driver");
    if (!httpBinding?.method?.trim()) throw new Error("HTTP receive operation has no artifact-declared HTTP method; POST is not inferred");
    const selected = selectedInputMessages(op, ch, context);
    validateMessageBindingVersion(selected[0]!);
    resolveInputCodec(doc, selected, context);
    if (!replyMessagesBindable(doc, op)) {
      throw new Error("an HTTP reply message uses carriage outside the built-in HTTP driver's application-value boundary");
    }
    resolveHTTPQuery(op, protocolFieldValues(context).httpQuery);
    return;
  }

  if (op.action === "receive") {
    const selected = selectedInputMessages(op, ch, context);
    validateMessageBindingVersion(selected[0]!);
    resolveInputCodec(doc, selected, context);
    const messageType = contextConfiguration(context)["websocketMessageType"];
    if (messageType !== "text" && messageType !== "binary") {
      throw new Error("configuration.websocketMessageType must select text or binary for a WebSocket publish");
    }
    if (op.reply) decodeContentType(doc, replyGoverningMessages(op), context);
  } else {
    // This validates non-empty output declarations, message-header
    // exclusions, declaration identity, and the decode point when absent.
    decodeContentType(doc, governingMessages(op, ch), context);
    if (op.reply) {
      const replyChannel = resolvedChannel(op.reply.channel);
      const selected = selectedInputMessages(
        { action: "receive", messages: op.reply.messages },
        replyChannel,
        context,
      );
      validateMessageBindingVersion(selected[0]!);
      resolveInputCodec(doc, selected, context);
      const messageType = contextConfiguration(context)["websocketMessageType"];
      if (messageType !== "text" && messageType !== "binary") {
        throw new Error("configuration.websocketMessageType must select text or binary for a WebSocket reply input");
      }
    }
  }
}

function channelNeedsOutgoingPayload(ch: AsyncAPIChannel | undefined): boolean {
  return Object.values(ch?.parameters ?? {}).some((parameter) => typeof parameter.location === "string");
}

function validateCredentialDestinations(
  op: AsyncAPIOperation,
  server: AsyncAPIServer | undefined,
  protocol: string,
  context?: Record<string, unknown>,
): void {
  if (!context) return;
  const fields = protocolFieldValues(context);
  const wsQuery = new Set(Object.keys(fields.webSocketQuery ?? {}));
  const wsHeaders = new Set(Object.keys(fields.webSocketHeaders ?? {}).map((name) => name.toLowerCase()));
  const processorHeaders = new Set(protocol === "http" || protocol === "https"
    ? ["host", "content-length", "content-type"]
    : ["host", "connection", "upgrade", "sec-websocket-key", "sec-websocket-version", "sec-websocket-protocol"]);
  const destinations = new Set<string>();
  for (const name of Object.keys(contextHeaders(context))) {
    if (processorHeaders.has(name.toLowerCase())) {
      throw new Error(`configured header ${JSON.stringify(name)} collides with a processor-owned transport field`);
    }
  }
  for (const { scheme, name } of resolveSecuritySchemes(op, server)) {
    let destination: string | undefined;
    let credential: string | undefined;
    if (scheme.type === "httpApiKey") {
      credential = contextApiKeyFor(context, name);
      if (credential && scheme.name && scheme.in) destination = `${scheme.in}:${scheme.in === "header" ? scheme.name.toLowerCase() : scheme.name}`;
    } else if (scheme.type === "http" && ["basic", "bearer"].includes((scheme.scheme ?? "").toLowerCase())) {
      credential = (scheme.scheme ?? "").toLowerCase() === "basic"
        ? (contextBasicAuthFor(context, name) ? "present" : undefined)
        : contextBearerTokenFor(context, name);
      if (credential) destination = "header:authorization";
    } else if (scheme.type === "oauth2" || scheme.type === "openIdConnect" || scheme.type === "httpBearer") {
      credential = contextAccessTokenFor(context, name) || contextBearerTokenFor(context, name);
      if (credential) destination = "header:authorization";
    }
    if (!destination) continue;
    if (destinations.has(destination)) throw new Error(`two credentials target the same ${destination} destination`);
    destinations.add(destination);
    const [channel, rawName] = destination.split(":", 2) as [string, string];
    if (channel === "header" && processorHeaders.has(rawName)) throw new Error(`credential destination ${rawName} collides with a processor-owned transport field`);
    if (protocol === "ws" || protocol === "wss") {
      if (channel === "query" && wsQuery.has(rawName)) throw new Error(`credential query destination ${rawName} collides with a declared WebSocket protocol field`);
      if (channel === "header" && wsHeaders.has(rawName)) throw new Error(`credential header destination ${rawName} collides with a declared WebSocket protocol field`);
    }
  }
}

// Server and address resolution (the §9.2 configuration points) live in
// target.ts; protocol-bindings honoring in bindings.ts; governing
// content-type resolution in content.ts.

// ---------------------------------------------------------------------------
// Context requirements (CONTEXT_REQUIRED negotiation)
// ---------------------------------------------------------------------------

/** A security scheme paired with its addressable name (rule A). */
interface NamedSecurityScheme {
  scheme: AsyncAPISecurityScheme;
  name?: string;
}

/**
 * Reads the components.securitySchemes key a `$ref`-resolved scheme object
 * came from, off the {@link REF_NAME_TAG} field util.ts's
 * tagSecurityRefNames tagged onto the entry BEFORE the shared dereferencer
 * ran (rule A). Object identity cannot recover this: the shared
 * dereferencer (deref.ts) resolves an internal `$ref` by looking it up
 * against the ORIGINAL document it was given, not the clone it progressively
 * walks and returns, so a resolved scheme is never reference-equal to
 * anything reachable from the FINAL document — tagging is what survives that
 * gap (dereference's merge-copy path carries extra `$ref`-node keys onto the
 * resolved object). An inline scheme object (declared directly in
 * `security`, no `$ref`) was never tagged and gets no name — rule A: "an
 * inline scheme object with no addressable name emits no `name`."
 */
function nameForScheme(scheme: AsyncAPISecurityScheme): string | undefined {
  const tagged = (scheme as unknown as Record<string, unknown>)[REF_NAME_TAG];
  return typeof tagged === "string" ? tagged : undefined;
}

/** One declared security list, as resolved named schemes (after dereference,
 * security items are resolved scheme objects). */
function schemeList(raw: AsyncAPIOperation["security"]): NamedSecurityScheme[] {
  return (raw ?? []).filter(isSecurityScheme).map((scheme) => ({ scheme, name: nameForScheme(scheme) }));
}

/** The security list of the server whose declared security applies (§9.5)
 * — never some other server's declaration. */
function serverSecuritySchemes(server: AsyncAPIServer | undefined): NamedSecurityScheme[] {
  return schemeList(server?.security);
}

/** The operation's own security list. It never displaces the server's: the
 * two lists are conjunctive (ASYNC-P-07) — the server's security applies,
 * and the operation's applies in addition. */
function operationSecuritySchemes(asyncOp: AsyncAPIOperation): NamedSecurityScheme[] {
  return schemeList(asyncOp.security);
}

/**
 * The security schemes applicable to an operation, flattened for credential
 * placement: the targeted server's list then the operation's list, in
 * declaration order — both apply, the conjunctive reading (ASYNC-P-07) —
 * with a scheme declared on both levels placed once.
 */
function resolveSecuritySchemes(
  asyncOp: AsyncAPIOperation,
  server: AsyncAPIServer | undefined,
): NamedSecurityScheme[] {
  const result: NamedSecurityScheme[] = [];
  const seen = new Set<string>();
  for (const named of [...serverSecuritySchemes(server), ...operationSecuritySchemes(asyncOp)]) {
    const key = `${named.scheme.type}\x00${named.scheme.scheme ?? ""}\x00${named.name ?? ""}`;
    if (seen.has(key)) continue;
    seen.add(key);
    result.push(named);
  }
  return result;
}

function driverSecurityAlternatives(
  asyncOp: AsyncAPIOperation,
  server: AsyncAPIServer | undefined,
): Array<Array<{ name?: string; scheme: Readonly<Record<string, unknown>> }>> {
  const serverSchemes = serverSecuritySchemes(server);
  const operationSchemes = operationSecuritySchemes(asyncOp);
  const combinations: NamedSecurityScheme[][] = [];
  if (serverSchemes.length > 0 && operationSchemes.length > 0) {
    for (const serverScheme of serverSchemes) {
      for (const operationScheme of operationSchemes) {
        combinations.push([serverScheme, operationScheme]);
      }
    }
  } else {
    for (const scheme of [...serverSchemes, ...operationSchemes]) combinations.push([scheme]);
  }
  return combinations.map((alternative) => {
    const seen = new Set<string>();
    return alternative.flatMap(({ scheme, name }) => {
      const key = `${scheme.type}\x00${scheme.scheme ?? ""}\x00${name ?? ""}`;
      if (seen.has(key)) return [];
      seen.add(key);
      return [{ ...(name ? { name } : {}), scheme: scheme as unknown as Readonly<Record<string, unknown>> }];
    });
  });
}

/**
 * Maps an AsyncAPI security scheme to a context requirement's type-specific
 * fields (name/description are layered on by schemeRequirement below). Every
 * scheme maps to SOMETHING: a recognized family, or — per the R2.c ruling —
 * a surfaced "auth.<T>" requirement (an http scheme with an unmapped
 * `scheme` value becomes "auth.http.<scheme>"; any other unmapped artifact
 * `type` becomes "auth.<type>" verbatim, e.g. "auth.futureSasl",
 * "auth.X509") so the alternative stays discoverable instead of being
 * silently dropped.
 */
function mapScheme(scheme: AsyncAPISecurityScheme, baseURL: string): ContextRequirement {
  switch (scheme.type) {
    case "http": {
      const s = (scheme.scheme ?? "").toLowerCase();
      if (s === "bearer") return { type: "auth.bearer" };
      if (s === "basic") return { type: "auth.basic" };
      return { type: s ? `auth.http.${s}` : "auth.http" };
    }
    case "httpBearer":
      return { type: "auth.bearer" };
    case "userPassword":
    case "scramSha256":
    case "scramSha512":
      return { type: "auth.basic" };
    case "apiKey":
    case "httpApiKey":
      return { type: "auth.apiKey" };
    case "oauth2":
      return oauth2Requirement(scheme, baseURL);
    default:
      return { type: `auth.${scheme.type}` };
  }
}

/**
 * Builds an `auth.oauth2` requirement carrying the flow's authorize/token
 * URLs, scopes, and `grantType` (rule B) under the binding-invoker
 * contract's convention field names. Selection mirrors the OpenAPI format's
 * oauth2Requirement EXACTLY: authorizationCode, then implicit, then whichever
 * of password/clientCredentials declares a `tokenUrl` (password checked
 * first) — a fixed priority, not declaration order. Relative URLs are
 * resolved against the server base. No flow means no grantType.
 */
function oauth2Requirement(scheme: AsyncAPISecurityScheme, baseURL: string): ContextRequirement {
  const req: ContextRequirement = { type: "auth.oauth2" };
  const flows = scheme.flows;
  const tokenOnlyFlow = [flows?.password, flows?.clientCredentials].find(
    (f): f is AsyncAPIOAuthFlow => !!f && typeof f.tokenUrl === "string",
  );
  const flow = flows?.authorizationCode ?? flows?.implicit ?? tokenOnlyFlow;
  if (flow?.authorizationUrl) req.authorizeUrl = absolutize(flow.authorizationUrl, baseURL);
  if (flow?.tokenUrl) req.tokenUrl = absolutize(flow.tokenUrl, baseURL);
  if (flow?.scopes && Object.keys(flow.scopes).length > 0) {
    req.scopes = Object.keys(flow.scopes);
  }
  if (flow) req.grantType = grantTypeFor(flows, flow);
  return req;
}

/** Names the OAuth2 grant type for the flow oauth2Requirement selected, by identity. */
function grantTypeFor(flows: AsyncAPIOAuthFlows | undefined, flow: AsyncAPIOAuthFlow): string {
  if (flow === flows?.authorizationCode) return "authorization_code";
  if (flow === flows?.implicit) return "implicit";
  if (flow === flows?.password) return "password";
  return "client_credentials";
}

/** Resolves a possibly-relative URL against the server base; passes absolute URLs through. */
function absolutize(url: string, baseURL: string): string {
  try {
    return new URL(url, baseURL).toString();
  } catch {
    return url;
  }
}

/** Wraps mapScheme with the requirement's name (rule A) and description. */
function schemeRequirement(
  scheme: AsyncAPISecurityScheme,
  baseURL: string,
  name: string | undefined,
): ContextRequirement {
  const req = mapScheme(scheme, baseURL);
  if (name) req.name = name;
  req.durable = true;
  if (scheme.description) req.description = scheme.description;
  return req;
}

/**
 * Computes the context the binding requires for this operation, or null when
 * the provided context already satisfies it (or the doc declares nothing
 * checkable). The declaration semantics are AsyncAPI 3.0's, incorporated,
 * and they are CONJUNCTIVE (ASYNC-P-07): the targeted server's `security`
 * applies (`server` is resolveTarget's selected artifact server per §9.5,
 * including when configuration replaces only its connection target),
 * and the operation's `security`, when declared, applies IN ADDITION.
 * Within each declared list one entry suffices, so the challenge is the
 * cross product: each alternative pairs one server entry with one operation
 * entry (or is a single entry when only one list is declared).
 * Side-effect-free; shared by runBinding and prepareBinding.
 */
export function requiredContext(
  asyncOp: AsyncAPIOperation,
  server: AsyncAPIServer | undefined,
  serverURL: string,
  ctx?: Record<string, unknown>,
): ContextRequiredDetails | null {
  const serverReqs = serverSecuritySchemes(server).map(({ scheme, name }) =>
    schemeRequirement(scheme, serverURL, name),
  );
  const opReqs = operationSecuritySchemes(asyncOp).map(({ scheme, name }) =>
    schemeRequirement(scheme, serverURL, name),
  );

  const alternatives: ContextAlternative[] = [];
  if (serverReqs.length > 0 && opReqs.length > 0) {
    for (const s of serverReqs) {
      for (const o of opReqs) {
        // The same scheme declared on both levels is one requirement, not a
        // duplicated conjunct.
        const requirements =
          o.type === s.type && o.name === s.name ? [s] : [s, o];
        alternatives.push({ requirements });
      }
    }
  } else {
    for (const r of [...serverReqs, ...opReqs]) {
      alternatives.push({ requirements: [r] });
    }
  }
  if (alternatives.length === 0) return null;

  const details: ContextRequiredDetails = {
    target: serverURL,
    alternatives,
  };
  if (ctx && contextSatisfies(ctx, details)) return null;
  return details;
}

// ---------------------------------------------------------------------------
// Credential application
// ---------------------------------------------------------------------------

function applyCredentialsViaSchemes(
  headers: Headers,
  asyncOp: AsyncAPIOperation,
  server: AsyncAPIServer | undefined,
  ctx: Record<string, unknown>,
): { applied: boolean; queryParams?: Record<string, string> } {
  const named = resolveSecuritySchemes(asyncOp, server);
  if (!named.length) return { applied: false };

  let applied = false;
  let queryParams: Record<string, string> | undefined;

  for (const { scheme, name: schemeName } of named) {
    const schemeType = scheme.type;
    switch (schemeType) {
      case "apiKey":
      case "httpApiKey": {
        // Rule D: the requirement's addressable name (schemeName, the
        // components.securitySchemes key) resolves the credential — distinct
        // from scheme.name below, which is the WIRE placement name (header/
        // query/cookie), not the lookup key.
        const val = contextApiKeyFor(ctx, schemeName);
        if (!val) continue;
        const loc = scheme.in;
        const name = scheme.name;
        switch (loc) {
          case "header":
            headers.set(name ?? "Authorization", val);
            applied = true;
            break;
          case "query":
            if (name) {
              queryParams ??= {};
              queryParams[name] = val;
              applied = true;
            }
            break;
          case "cookie":
            if (name) {
              headers.append("Cookie", `${name}=${encodeURIComponent(val)}`);
              applied = true;
            }
            break;
        }
        break;
      }
      case "http":
        switch ((scheme.scheme ?? "").toLowerCase()) {
          case "bearer": {
            const token = contextBearerTokenFor(ctx, schemeName);
            if (token) {
              headers.set("Authorization", `Bearer ${token}`);
              applied = true;
            }
            break;
          }
          case "basic": {
            const basic = contextBasicAuthFor(ctx, schemeName);
            if (basic) {
              const encoded = btoa(`${basic.username}:${basic.password}`);
              headers.set("Authorization", `Basic ${encoded}`);
              applied = true;
            }
            break;
          }
        }
        break;
      case "httpBearer": {
        const token = contextBearerTokenFor(ctx, schemeName);
        if (token) {
          headers.set("Authorization", `Bearer ${token}`);
          applied = true;
        }
        break;
      }
      case "oauth2": {
        const token = contextAccessTokenFor(ctx, schemeName) || contextBearerTokenFor(ctx, schemeName);
        if (token) {
          headers.set("Authorization", `Bearer ${token}`);
          applied = true;
        }
        break;
      }
      case "userPassword": {
        const basic = contextBasicAuthFor(ctx, schemeName);
        if (basic) {
          const encoded = btoa(`${basic.username}:${basic.password}`);
          headers.set("Authorization", `Basic ${encoded}`);
          applied = true;
        }
        break;
      }
    }
  }

  return { applied, queryParams };
}

function applyContext(
  headers: Headers,
  asyncOp: AsyncAPIOperation,
  server: AsyncAPIServer | undefined,
  ctx?: Record<string, unknown>,
): Record<string, string> | undefined {
  let queryParams: Record<string, string> | undefined;

  if (ctx) {
    const result = applyCredentialsViaSchemes(headers, asyncOp, server, ctx);
    queryParams = result.queryParams;
    for (const [k, v] of Object.entries(contextHeaders(ctx))) {
      headers.set(k, v);
    }
    const cookies = contextCookies(ctx);
    const parts: string[] = [];
    for (const [k, v] of Object.entries(cookies)) {
      parts.push(`${k}=${encodeURIComponent(v)}`);
    }
    if (parts.length > 0) {
      headers.append("Cookie", parts.join("; "));
    }
  }

  return queryParams;
}

// ---------------------------------------------------------------------------
// Publish over HTTP (`receive` action): unary, artifact-declared method
// ---------------------------------------------------------------------------

async function runUnaryPublish(
  target: ResolvedTarget,
  address: string,
  doc: AsyncAPIDocument,
  ch: AsyncAPIChannel | undefined,
  asyncOp: AsyncAPIOperation,
  args: BindingInvocationArgs,
  h: Handle,
  preparedInput?: { ok: true; value: unknown },
  preparedHeaders?: Record<string, unknown>,
): Promise<void> {
  // Unary: the one input IS the message payload (ASYNC-P-03). A publish
  // invocation requires an input value — this family defines no empty
  // message, so absence is a pre-dispatch refusal, never an empty-object
  // substitute. An operation-layer call for an operation declaring no input
  // is refused up front: callers of no-input operations never write, so
  // reading would park.
  if (noInputDeclared(args)) {
    h.fireError(
      new InvocationError(
        ERR_REFUSED,
        "publish invocation requires an input message (the input is the message; the operation declares no input)",
      ),
    );
    return;
  }

  // Input encoding follows the governing request-side declaration
  // (ASYNC-P-03); an excluded declared family refuses BEFORE dispatch.
  let codec: InputCodec;
  try {
    codec = resolveInputCodec(doc, selectedInputMessages(asyncOp, ch, args.context), args.context);
  } catch (e: unknown) {
    h.fireError(new InvocationError(ERR_REFUSED, errorMessage(e)));
    return;
  }

  const first = preparedInput ?? await readFirstInput(h);
  if (!first.ok) {
    h.fireError(
      new InvocationError(ERR_REFUSED, "publish invocation requires an input message"),
    );
    return;
  }
  await h.closeInput();

  let body: string | Uint8Array;
  try {
    body = await encodeThroughHooks(args.hooks, siteFor(args, target.serverURL), first.value, (v) => {
      const out = encodeInput(codec, v);
      return typeof out === "string" ? new TextEncoder().encode(out) : out;
    });
  } catch (e: unknown) {
    h.fireError(new InvocationError(ERR_REFUSED, errorMessage(e)));
    return;
  }

  let url = joinURL(target.serverURL, address);

  const headers = new Headers();
  if (codec.contentType !== "") headers.set("Content-Type", codec.contentType);
  // The routed envelope's application headers ride the HTTP cell's native
  // carriage (§9.2): each member becomes one request field.
  if (preparedHeaders !== undefined) {
    try {
      for (const [name, value] of Object.entries(preparedHeaders)) {
        headers.set(name, headerFieldText(name, value));
      }
    } catch (e: unknown) {
      h.fireError(new InvocationError(ERR_REFUSED, errorMessage(e)));
      return;
    }
  }
  const fields = protocolFieldValues(args.context);
  let requestQuery: Record<string, string> | undefined;
  try {
    requestQuery = resolveHTTPQuery(asyncOp, fields.httpQuery);
  } catch (e: unknown) {
    h.fireError(new InvocationError(ERR_REFUSED, errorMessage(e)));
    return;
  }
  if (requestQuery) url = appendQuery(url, requestQuery);
  const authQueryParams = applyContext(headers, asyncOp, target.securityServer, args.context);
  if (authQueryParams) {
    for (const name of Object.keys(authQueryParams)) {
      if (requestQuery?.[name] !== undefined) {
        h.fireError(new InvocationError(ERR_REFUSED, `credential query destination ${JSON.stringify(name)} collides with an HTTP protocol field`));
        return;
      }
    }
    url = appendQuery(url, authQueryParams);
  }

  const doFetch = args.fetch ?? fetch;
  let resp: Response;
  try {
    // The request method is the required http operation binding's `method`
    // (§8, ASYNC-P-02); validateCell already refused its absence.
    resp = await doFetch(url, {
      method: requestMethod(asyncOp, ""),
      headers,
      body: body as BodyInit,
      signal: h.signal,
      redirect: "manual",
    });
  } catch (e: unknown) {
    if (h.signal.aborted) return;
    h.fireError(new InvocationError(ERR_CONNECT_FAILED, errorMessage(e)));
    return;
  }

  // Classification (§9.4, ASYNC-P-06): a unary publish succeeds IFF the final
  // status, after any redirects, is 2xx. Mirrors the SSE establishment path's
  // strict-2xx test in this same file — a 3xx final (304, or a Location-less
  // redirect fetch does not follow) is a publish the server plausibly did not
  // accept, never a success.
  if (resp.status < 200 || resp.status >= 300) {
    const errBody = await readErrorBody(resp);
    h.fireError(
      new InvocationError(
        httpErrorCode(resp.status),
        "Invocation completed unsuccessfully",
        undefined,
        { status: resp.status, body: errBody },
      ),
    );
    return;
  }

  h.setHeader(headersToMetadata(resp.headers));

  let respBytes: Uint8Array;
  try {
    // The unary reply body is one delivery unit: the consumer-configurable
    // delivery-unit bound applies (args.maxDeliveryUnitBytes, default 10MB).
    respBytes = await readResponseBytes(resp, resolveDeliveryUnitLimit(args));
  } catch (e: unknown) {
    h.fireError(new InvocationError(ERR_RESPONSE_ERROR, errorMessage(e)));
    return;
  }

  if (respBytes.byteLength === 0) {
    // An empty body (202/204 acknowledgments included) yields no output
    // value: an acknowledgment is not a message and emits no value (§8).
    // The rule is body-based, never status-based.
    h.closeOutput();
    return;
  }

  if (!asyncOp.reply) {
    // The artifact declares no result message. Do not promote an arbitrary
    // HTTP response body into the OpenBindings operation boundary.
    h.closeOutput();
    return;
  }

  let replyDecode: string;
  try {
    replyDecode = resolveReplyContentType(
      doc,
      asyncOp,
      resp.status,
      resp.headers.get("Content-Type") ?? "",
      args.context,
    );
  } catch (e: unknown) {
    h.fireError(new InvocationError(ERR_PROTOCOL, errorMessage(e)));
    return;
  }

  // Decode through the consultation seam — content-independent, per the
  // conventions record's recommended built-in defaults: the reply-side
  // governing declaration decides the lane (direction-correct decode,
  // ASYNC-P-05) — JSON for application/json and +json suffixes (a
  // declared-JSON payload that fails to parse is loud), text otherwise.
  // Never sniffed.
  let output: unknown;
  try {
    let raw: RawResult;
    if (isBytesContentType(replyDecode)) {
      // The byte boundary: exact octets, never a UTF-8 text decode.
      raw = { status: resp.status, body: "", bodyBytes: respBytes, meta: headersToMetadata(resp.headers) };
    } else {
      const respText = new TextDecoder("utf-8", { fatal: true, ignoreBOM: true }).decode(respBytes);
      raw = { status: resp.status, body: respText, meta: headersToMetadata(resp.headers) };
    }
    output = await decodeThroughHooks(
      args.hooks,
      siteFor(args, target.serverURL),
      raw,
      builtinDecodeFor(
        replyDecode,
        AvroBinaryCodec.resolve(messagesForDecodeCT(doc, replyGoverningMessages(asyncOp), replyDecode), args.context),
      ),
    );
    // A headers-declaring reply rides the routed envelope on the output
    // direction too: the payload pairs with the declared application
    // headers projected from the HTTP response's fields (§9.2).
    const replyCandidates = messagesForDecodeCT(doc, replyGoverningMessages(asyncOp), replyDecode);
    if (replyCandidates.some((m) => m.headers !== undefined)) {
      output = { payload: output, headers: projectResponseHeaders(replyCandidates, resp.headers) };
    }
  } catch (e: unknown) {
    h.fireError(toInvocationError(e));
    return;
  }

  // Success provenance stamps (per the conventions record's recommended
  // built-in defaults): decode is spec/content-type (the governing declared
  // contentType decides the lane), hook when overridden; classify is
  // not-consulted (asyncapi runs no result classifier — the HTTP status
  // guard above is transport, not a format verdict).
  h.setTrailer(decodeTrailer(args.hooks, "spec/content-type"));
  args.observeOutput?.(output, headersToMetadata(resp.headers));
  await h.emitOutput(output);
  h.closeOutput();
}

// ---------------------------------------------------------------------------
// Subscribe over HTTP (`send` action): SSE
// ---------------------------------------------------------------------------

async function runSSESubscribe(
  target: ResolvedTarget,
  address: string,
  doc: AsyncAPIDocument,
  ch: AsyncAPIChannel | undefined,
  asyncOp: AsyncAPIOperation,
  args: BindingInvocationArgs,
  h: Handle,
): Promise<void> {
  // The described application sends; we subscribe. An SSE subscription
  // takes no input: input closes on entry, and a late write rejects
  // non-terminally at the handle (the refusal surface for supplied input).
  void h.closeInput();

  // The decode codec resolves BEFORE the request: an unqualifiable Avro
  // declaration (invalid schema, ambiguous candidates, bad framing
  // configuration) refuses with the never-dispatched guarantee.
  const sseMessages = governingMessages(asyncOp, ch);
  let sseAvro: AvroBinaryCodec | undefined;
  try {
    sseAvro = AvroBinaryCodec.resolve(sseMessages, args.context);
  } catch (e: unknown) {
    h.fireError(new InvocationError(ERR_REFUSED, errorMessage(e)));
    return;
  }

  let url = joinURL(target.serverURL, address);
  const headers = new Headers({ Accept: "text/event-stream" });
  const authQueryParams = applyContext(headers, asyncOp, target.securityServer, args.context);
  if (authQueryParams) {
    const sep = url.includes("?") ? "&" : "?";
    url += sep + new URLSearchParams(authQueryParams).toString();
  }

  const doFetch = args.fetch ?? fetch;
  let resp: Response;
  try {
    // The subscription framing is this specification's own pin (§8): the
    // request is a GET unless the http operation binding declares otherwise
    // (ASYNC-P-02: bindings are authoritative where they speak).
    resp = await doFetch(url, {
      method: requestMethod(asyncOp, "GET"),
      headers,
      signal: h.signal,
    });
  } catch (e: unknown) {
    if (h.signal.aborted) return; // cancellation is already terminal
    h.fireError(new InvocationError(ERR_CONNECT_FAILED, errorMessage(e)));
    return;
  }

  // Establishment (§8, ASYNC-P-06): a 2xx response bearing the
  // text/event-stream content type, judged on the FINAL response after any
  // redirects (fetch followed them; resp is final). Anything else is a
  // failure — non-2xx classifies as the transport status does; a 2xx
  // without the pinned framing is a protocol error, never a silent
  // reclassification.
  if (resp.status < 200 || resp.status >= 300) {
    const body = await readErrorBody(resp);
    h.fireError(
      new InvocationError(
        httpErrorCode(resp.status),
        "Invocation completed unsuccessfully",
        undefined,
        { status: resp.status, body },
      ),
    );
    return;
  }
  const ct = resp.headers.get("Content-Type") ?? "";
  if (normalizeMediaType(ct) !== "text/event-stream") {
    h.fireError(
      new InvocationError(
        ERR_PROTOCOL,
        `SSE subscription establishment requires a text/event-stream response, got content type ${JSON.stringify(ct)} (openbindings.asyncapi §8)`,
      ),
    );
    return;
  }

  const invocationMeta = headersToMetadata(resp.headers);
  h.setHeader(invocationMeta);

  // One transport, one invocation: transport close COMPLETES the
  // subscription — reconnection (`retry`, `Last-Event-ID`) is excluded from
  // the built-in SSE profile, so no reconnect is ever attempted here. Outputs decode
  // by the operation's own message declarations (direction-correct decode,
  // ASYNC-P-05).
  const decodeCT = decodeContentType(doc, sseMessages);
  await streamSSE(
    resp,
    args,
    siteFor(args, target.serverURL),
    h,
    invocationMeta,
    builtinPerEventDecodeFor(decodeCT, sseAvro),
  );
}

// ---------------------------------------------------------------------------
// WebSocket upgrade material
// ---------------------------------------------------------------------------

/** One FNV-1a (32-bit) pass over `str`, seeded, returned as an unsigned int. */
function fnv1a(str: string, seed: number): number {
  let hash = seed;
  for (let i = 0; i < str.length; i++) {
    hash ^= str.charCodeAt(i);
    hash = Math.imul(hash, 0x01000193);
  }
  return hash >>> 0;
}

/**
 * The upgrade request a WebSocket dial for this operation would send: the
 * full dial URL (server + dial address, query-placed credentials applied),
 * the upgrade headers (credential placement via {@link applyContext} plus
 * the resolved ws-binding header values), and a fingerprint of exactly that
 * material.
 *
 * NO credential ever rides a message body or a first frame under this
 * specification (§9.5, ASYNC-P-07) — the upgrade request IS the
 * connection's credential identity, so the fingerprint hashes exactly the
 * upgrade material (mirrors the Go SDK's credentialDigest). A digest — not
 * the raw material — feeds the pool key, so credentials never sit in map
 * keys. Two invocations whose fingerprints differ must never share a
 * pooled connection (cross-tenant credential leak).
 *
 * Non-cryptographic by design: two 32-bit FNV-1a passes with distinct seeds
 * give a 64-bit digest, which is plenty for a pool-key partition function
 * (the realistic collision surface is auth material containing high-entropy
 * tokens, not an adversary crafting collisions). This avoids a
 * `globalThis.crypto.subtle` dependency, which is not guaranteed available
 * without a flag on this package's minimum supported Node (18).
 */
function wsUpgradeMaterial(
  target: ResolvedTarget,
  dialAddress: string,
  asyncOp: AsyncAPIOperation,
  wsHeaders: Record<string, string> | undefined,
  ctx?: Record<string, unknown>,
): { url: string; headers: Record<string, string>; fingerprint: string } {
  const headers = new Headers();
  const authQueryParams = applyContext(headers, asyncOp, target.securityServer, ctx);
  // The resolved ws-binding header values (§8) are set after credential
  // placement, mirroring the Go SDK's createConn.
  for (const [name, val] of Object.entries(wsHeaders ?? {})) {
    headers.set(name, val);
  }

  const url = new URL(joinURL(target.serverURL, dialAddress));
  if (authQueryParams) {
    for (const [k, v] of Object.entries(authQueryParams)) {
      url.searchParams.set(k, v);
    }
  }

  const headerRecord: Record<string, string> = {};
  const headerParts: string[] = [];
  headers.forEach((value, name) => {
    headerRecord[name] = value;
    headerParts.push(`h:${name}=${value}`);
  });
  headerParts.sort();
  const material = [...headerParts, `q:${url.search}`].join(" ");
  const a = fnv1a(material, 0x811c9dc5);
  const b = fnv1a(material, 0x9e3779b9);
  return {
    url: url.toString(),
    headers: headerRecord,
    fingerprint: a.toString(16).padStart(8, "0") + b.toString(16).padStart(8, "0"),
  };
}

/** A host adapter may narrow an otherwise publishing interaction to no input. */
function noInputDeclared(args: BindingInvocationArgs): boolean {
  return args.acceptsInput === false;
}

// ---------------------------------------------------------------------------
// Subscribe over WebSocket (`send` action): bidi-capable, on a pooled socket
// ---------------------------------------------------------------------------

/**
 * Backpressure bounds for the undelivered-frame buffer between the socket
 * and the output pump: whichever bound trips first fails the stream
 * (bounded-queue-fail-loud, per the asyncapi binding spec's WS
 * slow-consumer ruling — Redis client-output-buffer-limit, NATS
 * slow-consumer, and MQTT max_queued_messages are the pub/sub-ecosystem
 * precedent, and NATS pairs a count bound with a byte bound the same way).
 * The handle's bounded output buffer IS the backpressure contract for a
 * draining consumer; an unbounded second buffer here would defeat it for a
 * non-draining one, so overflow fails the stream and closes the socket
 * instead. Reference-package defaults, not spec-mandated numbers.
 *
 * `let`, not `const`: setBackpressureBoundsForTest lowers these for a test
 * instead of pushing the full frame count / byte volume through a real
 * socket. This module is never re-exported from index.ts, so the mutable
 * bindings are intra-package only, not part of the public API.
 */
let MAX_BUFFERED_FRAMES = 1024;
let MAX_BUFFERED_BYTES = 64 * 1024 * 1024; // 64 MiB

/**
 * Test-only seam: lowers the WS receive backpressure bounds so overflow
 * tests can trip them deterministically without pushing the full volume
 * through a real socket. Returns a restore function.
 */
export function setBackpressureBoundsForTest(frames: number, bytes: number): () => void {
  const prevFrames = MAX_BUFFERED_FRAMES;
  const prevBytes = MAX_BUFFERED_BYTES;
  MAX_BUFFERED_FRAMES = frames;
  MAX_BUFFERED_BYTES = bytes;
  return () => {
    MAX_BUFFERED_FRAMES = prevFrames;
    MAX_BUFFERED_BYTES = prevBytes;
  };
}

async function runWSSubscribe(
  pool: WSPool,
  target: ResolvedTarget,
  dialAddress: string,
  wsHeaders: Record<string, string> | undefined,
  doc: AsyncAPIDocument,
  ch: AsyncAPIChannel | undefined,
  asyncOp: AsyncAPIOperation,
  args: BindingInvocationArgs,
  h: Handle,
  exchange?: {
    inputMessages: AsyncAPIMessage[];
    outputMessages: AsyncAPIMessage[];
    requireInput: boolean;
    isolationKey: string;
    inputLane: WebSocketReplyLane;
  },
): Promise<void> {
  // This cell is server-streaming, not bidirectional: the selected send
  // operation defines only what the described application emits.
  if (!exchange) await h.closeInput();
  // Outputs decode by the operation's own message declarations
  // (direction-correct decode, ASYNC-P-05); forwarded input frames use the
  // same governing declaration (§9.1). An excluded declared family refuses
  // only when an input frame actually arrives — a duplex subscription's
  // inputs are optional, and the exclusion belongs to the input lane.
  const wsContentType = decodeContentType(
    doc,
    exchange?.outputMessages ?? governingMessages(asyncOp, ch),
    args.context,
  );
  // Raw WebSocket frames have no native header carriage; a
  // headers-declaring input refuses per cell (§9.2) before any dial.
  if (exchange !== undefined && exchange.inputMessages.some((m) => m.headers !== undefined)) {
    h.fireError(new InvocationError(
      ERR_REFUSED,
      "the reply input declares application headers; this build has no header carriage for the WebSocket cell",
    ));
    return;
  }
  let codec: InputCodec | undefined;
  let codecErr: unknown;
  try {
    codec = resolveInputCodec(
      doc,
      exchange?.inputMessages ?? selectedInputMessages(asyncOp, ch, args.context),
      args.context,
    );
  } catch (e: unknown) {
    codecErr = e;
  }
  // The reply-input codec refusal fires BEFORE any socket is dialed: a
  // known-bad codec must refuse with the never-dispatched guarantee, not
  // dial and then fail post-upgrade (Go twin: runWSSubscribe).
  if (exchange && (codecErr !== undefined || !codec)) {
    h.fireError(new InvocationError(ERR_REFUSED, errorMessage(codecErr)));
    return;
  }

  // The output-side Avro codec resolves BEFORE any socket is dialed: an
  // unqualifiable declaration (invalid schema, ambiguous candidates, bad
  // framing configuration) refuses with the never-dispatched guarantee.
  // The pool delivers binary frames as exact octets, so the qualified
  // codec carries the Avro binary wire over WebSocket.
  let wsAvro: AvroBinaryCodec | undefined;
  try {
    wsAvro = AvroBinaryCodec.resolve(exchange?.outputMessages ?? governingMessages(asyncOp, ch), args.context);
  } catch (e: unknown) {
    h.fireError(new InvocationError(ERR_REFUSED, errorMessage(e)));
    return;
  }

  const material = wsUpgradeMaterial(target, dialAddress, asyncOp, wsHeaders, args.context);
  let pooled: PooledWS;
  try {
    pooled = await pool.acquire(target.serverURL, dialAddress, {
      buildURL: () => material.url,
      credentialKey: material.fingerprint,
      headers: material.headers,
      signal: h.signal,
      ...(exchange ? { isolationKey: exchange.isolationKey, captureInitialMessages: true } : {}),
    });
  } catch (e: unknown) {
    if (h.signal.aborted) return;
    h.fireError(new InvocationError(ERR_CONNECT_FAILED, errorMessage(e)));
    return;
  }

  let inputPooled = pooled;
  let removeInputClose: () => void = () => undefined;
  if (exchange && (exchange.inputLane.target.serverURL !== target.serverURL
    || exchange.inputLane.dialAddress !== dialAddress
    || JSON.stringify(sortedStringRecord(exchange.inputLane.headers)) !== JSON.stringify(sortedStringRecord(wsHeaders)))) {
    const inputMaterial = wsUpgradeMaterial(
      exchange.inputLane.target,
      exchange.inputLane.dialAddress,
      asyncOp,
      exchange.inputLane.headers,
      args.context,
    );
    try {
      inputPooled = await pool.acquire(
        exchange.inputLane.target.serverURL,
        exchange.inputLane.dialAddress,
        {
          buildURL: () => inputMaterial.url,
          credentialKey: inputMaterial.fingerprint,
          headers: inputMaterial.headers,
          signal: h.signal,
          isolationKey: exchange.isolationKey,
        },
      );
      removeInputClose = inputPooled.onClose((error) => {
        if (error && !h.signal.aborted) h.fireError(new InvocationError(ERR_STREAM_ERROR, error.message));
      });
    } catch (e: unknown) {
      pooled.release();
      if (!h.signal.aborted) h.fireError(new InvocationError(ERR_CONNECT_FAILED, errorMessage(e)));
      return;
    }
  }

  // NO in-band auth: no credential ever rides a message body or a first
  // frame under this specification (§9.5, ASYNC-P-07) — credentials ride
  // the upgrade request. In-band auth conventions (a first-frame bearer
  // message) are consumer configuration riding this duplex cell as an
  // ordinary input frame, never a built-in.

  const frames: (string | Uint8Array)[] = [];
  // Parallel to `frames`: the byte length of each still-buffered frame, so
  // the running bufferedBytes total can be decremented in the output pump
  // without re-encoding the frame just to measure it again.
  const frameByteLengths: number[] = [];
  let bufferedBytes = 0;
  let overflowed = false;
  let overflowMessage = "";
  let frameDecodeError: Error | undefined;
  let socketClosed = false;
  let socketError: Error | undefined;
  let wake: (() => void) | undefined;
  const notify = () => wake?.();
  const byteEncoder = new TextEncoder();

  // The delivery-unit bound for this subscription's frames (each frame is
  // one delivery unit). Enforcement point differs from Go BY PLATFORM
  // IDIOM, not behavior: nhooyr/coder websocket exposes SetReadLimit (a
  // pre-delivery connection-level read limit), while the browser/undici
  // WebSocket API has no read-limit seam, so the TS lane checks each
  // message's byte size POST-RECEIVE, before decode — same bound, same
  // ERR_STREAM_ERROR terminal. Per-subscription, so an oversized frame
  // never tears down the shared pooled socket under sibling subscriptions.
  const maxUnitBytes = resolveDeliveryUnitLimit(args);

  const removeMsg = pooled.onMessage((data, frameError) => {
    if (overflowed || frameDecodeError) return;
    if (frameError) {
      frameDecodeError = frameError;
      notify();
      return;
    }
    const frameBytes = typeof data === "string" ? byteEncoder.encode(data).length : data.byteLength;
    if (frameBytes > maxUnitBytes) {
      // Refuse loudly: mark terminal (the output pump drains what was
      // already buffered, then fails the stream) and drop this and every
      // subsequent frame.
      overflowed = true;
      overflowMessage = `WebSocket message exceeds ${maxUnitBytes} byte limit`;
      notify();
      return;
    }
    if (frames.length >= MAX_BUFFERED_FRAMES) {
      overflowed = true;
      overflowMessage = `backpressure overflow: more than ${MAX_BUFFERED_FRAMES} undelivered frames`;
    } else if (bufferedBytes + frameBytes > MAX_BUFFERED_BYTES) {
      overflowed = true;
      overflowMessage = `backpressure overflow: more than ${MAX_BUFFERED_BYTES} undelivered bytes`;
    }
    if (overflowed) {
      // The consumer is not draining; stop buffering for THIS subscription
      // (the guard above drops further frames) and let the output pump fail
      // the stream after draining what's already buffered. The socket stays
      // open — it is shared with any sibling subscriptions (the pool is
      // ref-counted), so a slow consumer must never tear it down under
      // them; this listener detaches when the pump's terminal returns
      // through the finally below. Mirrors Go's wsSubscription overflow.
      notify();
      return;
    }
    frames.push(data);
    frameByteLengths.push(frameBytes);
    bufferedBytes += frameBytes;
    notify();
  });
  const removeClose = pooled.onClose((err) => {
    socketClosed = true;
    socketError = err;
    notify();
  });
  const onAbort = () => notify();
  h.signal.addEventListener("abort", onAbort);

  // Socket -> outputs. Owns the terminal transition. Each frame is one
  // delivery unit decoded through the consultation seam by the governing
  // declared content type (status null — a WS frame has no completion
  // status; never fabricated). Convention envelopes ({error}/{data}
  // unwrapping) are consumer knowledge: a decode hook's job, never the
  // builtin's.
  const wsSite = siteFor(args, target.serverURL);
  const outputPump = async (): Promise<void> => {
    while (true) {
      // Drain-before-terminal: unconditionally, regardless of `overflowed`
      // — a synchronous flood of incoming messages can set `overflowed`
      // before this pump ever gets a turn, and every frame already
      // buffered by then must still reach the consumer before the
      // terminal error does (matches the decode-error and socket-closed
      // paths below, and the Go SDK's wsSubscription.next).
      while (frames.length > 0) {
        const frame = frames.shift()!;
        bufferedBytes -= frameByteLengths.shift()!;
        let out: unknown;
        try {
          out = await decodeThroughHooks(
            args.hooks,
            wsSite,
            typeof frame === "string"
              ? { status: null, body: frame, meta: {} }
              : { status: null, body: "", bodyBytes: frame, meta: {} },
            builtinDecodeFor(wsContentType, wsAvro),
          );
        } catch (e: unknown) {
          // A decode error mid-stream is terminal; already-emitted
          // outputs stand (drain-before-terminal).
          h.fireError(toInvocationError(e));
          return;
        }
        // Throws if the invocation terminated while parked: stop emitting.
        args.observeOutput?.(out, {});
        await h.emitOutput(out);
      }
      if (overflowed) {
        h.fireError(new InvocationError(ERR_STREAM_ERROR, overflowMessage));
        return;
      }
      if (frameDecodeError) {
        h.fireError(new InvocationError(ERR_VALIDATION_FAILED, frameDecodeError.message));
        return;
      }
      if (h.signal.aborted) return;
      if (socketClosed) {
        if (socketError) {
          h.fireError(new InvocationError(ERR_STREAM_ERROR, socketError.message));
        } else {
          h.closeOutput();
        }
        return;
      }
      await new Promise<void>((r) => {
        wake = r;
      });
      wake = undefined;
    }
  };

  // Inputs -> socket: the duplex lane — caller-supplied input values forward
  // as frames, and closing input does NOT end the subscription (outputs keep
  // flowing). Frames encode per the same governing declaration as §9.1
  // (resolved above); an excluded declared family refuses only here, when
  // an input actually arrives.
  const inputPump = async (): Promise<void> => {
    if (!exchange) return;
    let sent = 0;
    try {
      for await (const msg of h.inputs()) {
        if (codecErr !== undefined || !codec) {
          h.fireError(new InvocationError(ERR_SOURCE_CONFIG_ERROR, errorMessage(codecErr)));
          return;
        }
        let frame: string | Uint8Array;
        try {
          frame = await encodeThroughHooks(args.hooks, siteFor(args, target.serverURL), msg, (v) => {
            const out = encodeInput(codec, v);
            return typeof out === "string" ? new TextEncoder().encode(out) : out;
          });
        } catch (e: unknown) {
          h.fireError(new InvocationError(ERR_VALIDATION_FAILED, errorMessage(e)));
          return;
        }
        const messageType = contextConfiguration(args.context)["websocketMessageType"];
        inputPooled.send(typeof frame === "string" ? (messageType === "binary" ? new TextEncoder().encode(frame) : frame) : frame);
        sent++;
      }
      if (exchange.requireInput && sent === 0) {
        h.fireError(new InvocationError(ERR_MISSING_INPUT, "request/reply invocation requires at least one input message"));
      }
    } catch {
      // Invocation terminated; the output pump owns the terminal transition.
    }
  };

  try {
    await Promise.all([
      outputPump().catch((e: unknown) => {
        h.fireError(
          e instanceof InvocationError
            ? e
            : new InvocationError(ERR_STREAM_ERROR, errorMessage(e)),
        );
      }),
      inputPump(),
    ]);
  } catch (e: unknown) {
    h.fireError(
      e instanceof InvocationError
        ? e
        : new InvocationError(ERR_STREAM_ERROR, errorMessage(e)),
    );
  } finally {
    h.signal.removeEventListener("abort", onAbort);
    removeMsg();
    removeClose();
    removeInputClose();
    if (inputPooled !== pooled) inputPooled.release();
    pooled.release();
  }
}

function sortedStringRecord(value: Record<string, string> | undefined): Record<string, string> {
  return Object.fromEntries(Object.entries(value ?? {}).sort(([a], [b]) => a.localeCompare(b)));
}

// ---------------------------------------------------------------------------
// Publish over WebSocket (`receive` action): client-streaming, pooled socket
// ---------------------------------------------------------------------------

async function runWSPublish(
  pool: WSPool,
  target: ResolvedTarget,
  dialAddress: string,
  wsHeaders: Record<string, string> | undefined,
  doc: AsyncAPIDocument,
  ch: AsyncAPIChannel | undefined,
  asyncOp: AsyncAPIOperation,
  args: BindingInvocationArgs,
  h: Handle,
): Promise<void> {
  // A publish invocation requires input — the input IS the message
  // (ASYNC-P-03); this family defines no empty message. An operation-layer
  // call for an operation declaring no input is refused before dispatch
  // (callers of no-input operations never write, so reading would park).
  if (noInputDeclared(args)) {
    h.fireError(
      new InvocationError(
        ERR_REFUSED,
        "publish invocation requires an input message (the input is the message; the operation declares no input)",
      ),
    );
    return;
  }

  // Input encoding follows the governing request-side declaration
  // (ASYNC-P-03); an excluded declared family refuses BEFORE dispatch —
  // before any socket is dialed.
  let codec: InputCodec;
  try {
    codec = resolveInputCodec(doc, selectedInputMessages(asyncOp, ch, args.context), args.context);
  } catch (e: unknown) {
    h.fireError(new InvocationError(ERR_REFUSED, errorMessage(e)));
    return;
  }

  const material = wsUpgradeMaterial(target, dialAddress, asyncOp, wsHeaders, args.context);
  let pooled: PooledWS;
  try {
    pooled = await pool.acquire(target.serverURL, dialAddress, {
      buildURL: () => material.url,
      credentialKey: material.fingerprint,
      headers: material.headers,
      signal: h.signal,
    });
  } catch (e: unknown) {
    if (h.signal.aborted) return;
    h.fireError(new InvocationError(ERR_CONNECT_FAILED, errorMessage(e)));
    return;
  }

  // A socket that dies mid-stream must fail the publish, not silently
  // swallow frames and then close as success. The terminal also rejects
  // any caller write parked on the input channel.
  const removeClose = pooled.onClose((err) => {
    h.fireError(
      new InvocationError(
        ERR_STREAM_ERROR,
        err ? `socket failed mid-publish: ${err.message}` : "socket closed mid-publish",
      ),
    );
  });

  try {
    // NO auth frame, ever: no credential rides a message body or a first
    // frame under this specification (§9.5, ASYNC-P-07) — credentials ride
    // the upgrade request.
    //
    // Every input is one frame, encoded per the governing declaration
    // (§9.1); the loop ends cleanly when the caller closes input, and
    // throws if the invocation terminates. `send` throws on a dead socket,
    // so no frame is ever silently dropped. Closing with zero messages sent
    // is the streaming face of the presence rule (ASYNC-P-03): nothing was
    // published, so the invocation fails loudly.
    let sent = 0;
    for await (const msg of h.inputs()) {
      let frame: string | Uint8Array;
      try {
        frame = await encodeThroughHooks(args.hooks, siteFor(args, target.serverURL), msg, (v) => {
          const out = encodeInput(codec, v);
          return typeof out === "string" ? new TextEncoder().encode(out) : out;
        });
      } catch (e: unknown) {
        h.fireError(new InvocationError(ERR_VALIDATION_FAILED, errorMessage(e)));
        return;
      }
      const messageType = contextConfiguration(args.context)["websocketMessageType"];
      pooled.send(typeof frame === "string" ? (messageType === "binary" ? new TextEncoder().encode(frame) : frame) : frame);
      sent++;
    }
    if (sent === 0) {
      h.fireError(
        new InvocationError(
          ERR_MISSING_INPUT,
          "publish invocation requires an input message (input closed with no messages sent)",
        ),
      );
      return;
    }
    h.closeOutput();
  } catch (e: unknown) {
    h.fireError(
      e instanceof InvocationError
        ? e
        : new InvocationError(ERR_STREAM_ERROR, errorMessage(e)),
    );
  } finally {
    removeClose();
    pooled.release();
  }
}

// ---------------------------------------------------------------------------
// Utilities
// ---------------------------------------------------------------------------

async function readFirstInput(
  h: Handle,
): Promise<{ ok: true; value: unknown } | { ok: false }> {
  for await (const v of h.inputs()) {
    return { ok: true, value: v };
  }
  return { ok: false };
}

function appendQuery(url: string, values: Record<string, string>): string {
  const parsed = new URL(url);
  for (const [name, value] of Object.entries(values)) parsed.searchParams.append(name, value);
  return parsed.toString();
}

function headersToMetadata(headers: Headers): Metadata {
  const md: Metadata = {};
  headers.forEach((value, key) => {
    md[key] = [value];
  });
  return md;
}

async function readErrorBody(resp: Response): Promise<unknown> {
  // The raw capture, verbatim (details are diagnostics, never a decoded
  // value — no sniffing on the failure path either).
  try {
    const text = await readResponseText(resp, MAX_RESPONSE_BYTES);
    return text || undefined;
  } catch {
    return undefined;
  }
}

// ---------------------------------------------------------------------------
// The consultation seam's format half
// ---------------------------------------------------------------------------

/**
 * Returns the builtin decoder for a governing declared content type: strict
 * JSON for application/json and +json suffixes (a declared-JSON payload
 * that fails to parse is loud), text otherwise; an empty body is a null
 * output. Content-independent — the declaration decides, never the bytes.
 */
export function builtinDecodeFor(contentType: string, avro?: AvroBinaryCodec): OutputDecoder {
  const perEvent = builtinPerEventDecodeFor(contentType, avro);
  return (site: InvokeSite, raw: RawResult): unknown => {
    // An empty delivery unit emits no value. This rule is
    // whole-unit-scoped (an HTTP reply, a WS frame): the SSE per-event
    // lane bypasses it via builtinPerEventDecodeFor — a DISPATCHED event
    // whose data text is empty (a lone empty `data:` line, WHATWG) is a
    // value, never an absent output.
    const empty = raw.bodyBytes !== undefined ? raw.bodyBytes.byteLength === 0 : raw.body.length === 0;
    if (empty) return null;
    return perEvent(site, raw);
  };
}

/**
 * builtinDecodeFor's declaration-keyed lane set without the
 * empty-unit→no-value rule, used by the SSE per-event lane where an empty
 * data text is the empty-string value under the text lane (and a
 * declared-JSON contentType judges it as any other non-JSON text — loud).
 */
export function builtinPerEventDecodeFor(contentType: string, avro?: AvroBinaryCodec): OutputDecoder {
  const isJSON = isJSONContentType(contentType);
  const isBytes = isBytesContentType(contentType);
  return (_site: InvokeSite, raw: RawResult): unknown => {
    if (avro !== undefined && !isJSON) {
      // The named Avro correspondence's binary wire: octets decode to the
      // logical value through the qualified codec (a JSON-family
      // declaration instead carries the Avro-JSON encoding, which the
      // ordinary JSON branch below parses).
      const octets = raw.bodyBytes ?? new TextEncoder().encode(raw.body);
      try {
        return avro.decode(octets);
      } catch (e: unknown) {
        throw new InvocationError(ERR_RESPONSE_ERROR, errorMessage(e));
      }
    }
    if (isBytes) {
      // The byte boundary: exact octets as the canonical Base64 string.
      const octets = raw.bodyBytes ?? new TextEncoder().encode(raw.body);
      return encodeBase64(octets);
    }
    // A textual lane over a byte-delivered frame (a WS binary frame, a
    // driver payload) decodes strictly here: whether bytes are text is the
    // DECLARED content type's judgment, and a frame that fails it is a
    // lying producer — ERR_RESPONSE_ERROR, matching the Go builtin.
    let body = raw.body;
    if (raw.bodyBytes !== undefined) {
      try {
        body = new TextDecoder("utf-8", { fatal: true, ignoreBOM: true }).decode(raw.bodyBytes);
      } catch {
        throw new InvocationError(
          ERR_RESPONSE_ERROR,
          `message declares ${JSON.stringify(contentType)} but the payload is not valid UTF-8`,
        );
      }
    }
    if (isJSON) {
      try {
        return JSON.parse(body);
      } catch (e: unknown) {
        throw new InvocationError(
          ERR_RESPONSE_ERROR,
          `message declares ${JSON.stringify(contentType)} but the payload is not valid JSON: ${errorMessage(e)}`,
        );
      }
    }
    if (!isWellFormedUnicode(body)) {
      throw new InvocationError(
        ERR_RESPONSE_ERROR,
        `message declares ${JSON.stringify(contentType)} but the payload is not valid UTF-8`,
      );
    }
    return body;
  };
}

/**
 * Completes the site for one dispatch with the format-known target (the
 * resolved server URL). A missing site (direct format-package call) gets a
 * minimal one so hook tables keyed on format/ref still match.
 */
function siteFor(args: BindingInvocationArgs, serverURL: string): InvokeSite {
  const site: InvokeSite = args.site
    ? { ...args.site }
    : {
        operation: "",
        invokedAs: "",
        bindingKey: "",
        bindingSpec: `asyncapi/${args.source.profile.name}`,
        ref: args.ref,
        target: "",
      };
  if (site.target === "") site.target = serverURL;
  return site;
}

/**
 * Builds the x-ob-decode stamp (and the fixed x-ob-classify not-consulted
 * stamp — asyncapi runs no classifier) for a successful message decode,
 * given the builtin decode provenance token, per the conventions record's
 * recommended built-in defaults.
 */
function decodeTrailer(hooks: InvokeHooks | null | undefined, builtinDecode: string): Metadata {
  const decode = hooks?.decodeDecidedBy() === "hook" ? "hook" : builtinDecode;
  return { "x-ob-decode": [decode], "x-ob-classify": ["not-consulted"] };
}

/** Converts a seam failure into the terminal InvocationError to surface. */
function toInvocationError(e: unknown): InvocationError {
  if (e instanceof InvocationError) return e;
  return new InvocationError(ERR_RESPONSE_ERROR, errorMessage(e));
}

async function readResponseBytes(resp: Response, maxBytes: number): Promise<Uint8Array> {
  if (!resp.body) return new Uint8Array(await resp.arrayBuffer());
  const reader = resp.body.getReader();
  const chunks: Uint8Array[] = [];
  let total = 0;
  try {
    for (;;) {
      const { done, value } = await reader.read();
      if (done) break;
      total += value.byteLength;
      if (total > maxBytes) {
        // Cancel the body stream before bailing; releasing the lock alone
        // leaves the response socket pinned on the remaining bytes.
        await reader.cancel().catch(() => {});
        throw new Error(`response exceeds ${maxBytes} byte limit`);
      }
      chunks.push(value);
    }
  } finally {
    reader.releaseLock();
  }
  const out = new Uint8Array(total);
  let offset = 0;
  for (const chunk of chunks) {
    out.set(chunk, offset);
    offset += chunk.byteLength;
  }
  return out;
}

async function readResponseText(resp: Response, maxBytes: number): Promise<string> {
  if (!resp.body) return resp.text();

  const reader = resp.body.getReader();
  // Preserve a leading BOM as U+FEFF rather than silently stripping source
  // bytes. JSON decoding then rejects it consistently with Go; the text lane
  // returns it as declared payload content.
  const decoder = new TextDecoder("utf-8", { fatal: true, ignoreBOM: true });
  const chunks: string[] = [];
  let total = 0;

  try {
    for (;;) {
      const { done, value } = await reader.read();
      if (done) break;
      total += value.byteLength;
      if (total > maxBytes) {
        // Cancel the body stream before bailing; releasing the lock alone
        // leaves the response socket pinned on the remaining bytes (the
        // pattern openapi's readResponseBytes and this package's own sse.ts
        // already follow).
        await reader.cancel().catch(() => {});
        throw new Error(`response exceeds ${maxBytes} byte limit`);
      }
      chunks.push(decoder.decode(value, { stream: true }));
    }
    chunks.push(decoder.decode());
  } finally {
    reader.releaseLock();
  }

  return chunks.join("");
}
