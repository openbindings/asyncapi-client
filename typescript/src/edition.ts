const V2_EDITIONS = new Set([
  "2.0.0",
  "2.1.0",
  "2.2.0",
  "2.3.0",
  "2.4.0",
  "2.5.0",
  "2.6.0",
]);
const V3_EDITIONS = new Set(["3.0.0", "3.1.0"]);

export const SUPPORTED_ASYNCAPI_EDITIONS = Object.freeze([
  ...V2_EDITIONS,
  ...V3_EDITIONS,
]);

/**
 * The exact-edition gate alone (ASYNC-P-01's discriminator): callable before
 * any reference resolution so an unsupported edition refuses without
 * fetching a closure this client will never interpret. Mirrors the Go
 * pipeline's discriminate -> resolve externals -> normalize order.
 */
export function discriminateAsyncAPIEdition(source: Record<string, unknown>): string {
  const edition = source.asyncapi;
  if (typeof edition !== "string" || !SUPPORTED_ASYNCAPI_EDITIONS.includes(edition)) {
    throw new Error(
      `unsupported AsyncAPI version ${JSON.stringify(edition)}: this client accepts exactly ${SUPPORTED_ASYNCAPI_EDITIONS.join(", ")}`,
    );
  }
  return edition;
}

export function normalizeAsyncAPIEnvelope(
  source: Record<string, unknown>,
): Record<string, unknown> {
  const edition = discriminateAsyncAPIEdition(source);
  if (V3_EDITIONS.has(edition)) return source;
  return normalizeV2(source, edition);
}

export function v2OperationKey(channel: string, verb: "publish" | "subscribe"): string {
  return `v2:${verb}:${channel}`;
}

export function v2OperationRef(channel: string, verb: "publish" | "subscribe"): string {
  return `#/channels/${escapePointerToken(channel)}/${verb}`;
}

export function parseV2OperationRef(ref: string): string | undefined {
  const match = /^#\/channels\/([^/]+)\/(publish|subscribe)$/.exec(ref);
  if (!match) return undefined;
  const channel = unescapePointerToken(match[1]!);
  return v2OperationKey(channel, match[2] as "publish" | "subscribe");
}

export function refForNormalizedOperationKey(key: string): string | undefined {
  const match = /^v2:(publish|subscribe):(.*)$/.exec(key);
  if (!match) return undefined;
  return v2OperationRef(
    match[2]!,
    match[1] as "publish" | "subscribe",
  );
}

function normalizeV2(
  source: Record<string, unknown>,
  edition: string,
): Record<string, unknown> {
  const result: Record<string, unknown> = {
    ...source,
    asyncapi: "3.0.0",
    "x-ob-asyncapi-source-edition": edition,
  };
  result.servers = normalizeServers(source.servers);
  const operations: Record<string, unknown> = {};
  const channels: Record<string, unknown> = {};
  for (const [channelName, rawChannel] of entries(source.channels)) {
    const channel = object(rawChannel);
    const messages: Record<string, unknown> = {};
    const normalizedChannel: Record<string, unknown> = {
      ...channel,
      address: channelName,
      messages,
    };
    delete normalizedChannel.publish;
    delete normalizedChannel.subscribe;
    if (Array.isArray(channel.servers)) {
      normalizedChannel.servers = channel.servers
        .filter((name): name is string => typeof name === "string")
        .map((name) => ({ $ref: `#/servers/${escapePointerToken(name)}` }));
    }
    for (const verb of ["publish", "subscribe"] as const) {
      const rawOperation = channel[verb];
      if (rawOperation == null || typeof rawOperation !== "object" || Array.isArray(rawOperation)) {
        continue;
      }
      const operation = { ...(rawOperation as Record<string, unknown>) };
      const messageRefs = normalizeV2Messages(operation.message, verb, channelName, messages);
      delete operation.message;
      operation.action = verb === "publish" ? "receive" : "send";
      operation.channel = { $ref: `#/channels/${escapePointerToken(channelName)}` };
      operation.messages = messageRefs;
      operation.security = normalizeSecurity(operation.security, operation);
      operation["x-ob-asyncapi-source-ref"] = v2OperationRef(channelName, verb);
      operations[v2OperationKey(channelName, verb)] = operation;
    }
    channels[channelName] = normalizedChannel;
  }
  result.channels = channels;
  result.operations = operations;
  return result;
}

function normalizeServers(value: unknown): Record<string, unknown> {
  const out: Record<string, unknown> = {};
  for (const [name, raw] of entries(value)) {
    const server = { ...object(raw) };
    const url = typeof server.url === "string" ? server.url : "";
    delete server.url;
    const protocol = typeof server.protocol === "string" ? server.protocol : "";
    const parts = splitServerURL(url, protocol);
    server.host = parts.host;
    if (parts.pathname) server.pathname = parts.pathname;
    server.security = normalizeSecurity(server.security, server);
    out[name] = server;
  }
  return out;
}

function normalizeSecurity(value: unknown, owner: Record<string, unknown>): unknown {
  if (!Array.isArray(value)) return value;
  const refs: Array<Record<string, unknown>> = [];
  for (const alternative of value) {
    if (alternative == null || typeof alternative !== "object" || Array.isArray(alternative)) continue;
    const names = Object.keys(alternative as Record<string, unknown>);
    if (names.length === 0) return [];
    if (names.length > 1) owner["x-ob-asyncapi-v2-security-conjunction"] = alternative;
    for (const name of names) {
      refs.push({ $ref: `#/components/securitySchemes/${escapePointerToken(name)}` });
    }
  }
  return refs;
}

function normalizeV2Messages(
  value: unknown,
  verb: string,
  channel: string,
  messages: Record<string, unknown>,
): Array<{ $ref: string }> {
  const raw = object(value);
  const alternatives = Array.isArray(raw.oneOf) ? raw.oneOf : value == null ? [] : [value];
  const refs: Array<{ $ref: string }> = [];
  let index = 0;
  for (const alternative of alternatives) {
    index += 1;
    const message = object(alternative);
    const suggested =
      typeof message.messageId === "string" && message.messageId
        ? message.messageId
        : typeof message.name === "string" && message.name
          ? message.name
          : `${verb}Message${index}`;
    const key = uniqueMessageKey(suggested, messages);
    messages[key] = alternative;
    refs.push({
      $ref: `#/channels/${escapePointerToken(channel)}/messages/${escapePointerToken(key)}`,
    });
  }
  return refs;
}

function splitServerURL(url: string, protocol: string): { host: string; pathname: string } {
  const prefix = `${protocol}://`;
  const remainder = url.startsWith(prefix) ? url.slice(prefix.length) : url.replace(/^[a-z][a-z0-9+.-]*:\/\//i, "");
  const slash = remainder.indexOf("/");
  if (slash < 0) return { host: remainder, pathname: "" };
  return { host: remainder.slice(0, slash), pathname: remainder.slice(slash) };
}

function uniqueMessageKey(suggested: string, messages: Record<string, unknown>): string {
  if (!Object.hasOwn(messages, suggested)) return suggested;
  for (let i = 2; ; i += 1) {
    const candidate = `${suggested}_${i}`;
    if (!Object.hasOwn(messages, candidate)) return candidate;
  }
}

function entries(value: unknown): Array<[string, unknown]> {
  return value != null && typeof value === "object" && !Array.isArray(value)
    ? Object.entries(value as Record<string, unknown>)
    : [];
}

function object(value: unknown): Record<string, unknown> {
  return value != null && typeof value === "object" && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : {};
}

function escapePointerToken(value: string): string {
  return value.replaceAll("~", "~0").replaceAll("/", "~1");
}

function unescapePointerToken(value: string): string {
  return value.replaceAll("~1", "/").replaceAll("~0", "~");
}
