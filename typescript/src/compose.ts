/**
 * Post-pass of the external-composition step, mirroring the Go client's
 * normalizeResolvedReferenceUnions byte-for-byte: after external references
 * inline, positions AsyncAPI requires to BE Reference Objects (an
 * operation's `channel`, a reply's `channel`) or arrays of them (operation
 * and reply `messages`, a channel's `servers` subset) are re-hoisted into
 * their owning maps under synthetic keys and re-pointed by internal
 * reference. The synthetic names can surface in coverage identities, so the
 * two implementations must generate IDENTICAL spellings — do not "improve"
 * one side alone.
 */

function isObject(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

function inlineReferenceObject(raw: unknown): Record<string, unknown> | undefined {
  return isObject(raw) && raw["$ref"] === undefined ? raw : undefined;
}

function ensureReferenceObjectMap(owner: Record<string, unknown>, field: string): Record<string, unknown> {
  const existing = owner[field];
  if (isObject(existing)) return existing;
  const created: Record<string, unknown> = {};
  owner[field] = created;
  return created;
}

function sortedReferenceKeys(values: Record<string, unknown>): string[] {
  return Object.keys(values).sort();
}

function uniqueReferenceKey(values: Record<string, unknown>, base: string): string {
  if (!Object.hasOwn(values, base)) return base;
  for (let suffix = 2; ; suffix++) {
    const candidate = `${base}_${suffix}`;
    if (!Object.hasOwn(values, candidate)) return candidate;
  }
}

function sanitizeReferenceKey(value: string): string {
  // Go's twin keeps Unicode letters and digits and `_`, replacing everything
  // else with `_` (unicode.IsLetter/IsDigit). \p{L}\p{N} matches that.
  const sanitized = [...value]
    .map((character) => (/[\p{L}\p{N}_]/u.test(character) ? character : "_"))
    .join("");
  return sanitized.length === 0 ? "item" : sanitized;
}

function escapeReferenceToken(value: string): string {
  return value.replaceAll("~", "~0").replaceAll("/", "~1");
}

export function normalizeResolvedReferenceUnions(root: Record<string, unknown>): void {
  const servers = ensureReferenceObjectMap(root, "servers");
  const channels = ensureReferenceObjectMap(root, "channels");
  const operations = isObject(root["operations"]) ? (root["operations"] as Record<string, unknown>) : undefined;

  const normalizeChannel = (channelName: string, raw: unknown): void => {
    if (!isObject(raw)) return;
    const rawServers = raw["servers"];
    if (!Array.isArray(rawServers)) return;
    for (let index = 0; index < rawServers.length; index++) {
      const server = rawServers[index];
      if (!isObject(server) || server["$ref"] !== undefined) continue;
      const name = uniqueReferenceKey(
        servers,
        `__openbindings_external_${sanitizeReferenceKey(channelName)}_server_${index}`,
      );
      servers[name] = server;
      rawServers[index] = { $ref: `#/servers/${escapeReferenceToken(name)}` };
    }
  };

  for (const name of sortedReferenceKeys(channels)) {
    normalizeChannel(name, channels[name]);
  }

  for (const operationName of operations ? sortedReferenceKeys(operations) : []) {
    const operation = operations![operationName];
    if (!isObject(operation)) continue;
    const inlineChannel = inlineReferenceObject(operation["channel"]);
    if (inlineChannel) {
      const name = uniqueReferenceKey(
        channels,
        `__openbindings_external_${sanitizeReferenceKey(operationName)}_channel`,
      );
      channels[name] = inlineChannel;
      normalizeChannel(name, inlineChannel);
      operation["channel"] = { $ref: `#/channels/${escapeReferenceToken(name)}` };
    }
    normalizeInlineMessages(root, `${operationName}_message`, operation, "messages");
    const reply = operation["reply"];
    if (isObject(reply)) {
      const inlineReplyChannel = inlineReferenceObject(reply["channel"]);
      if (inlineReplyChannel) {
        const name = uniqueReferenceKey(
          channels,
          `__openbindings_external_${sanitizeReferenceKey(operationName)}_reply_channel`,
        );
        channels[name] = inlineReplyChannel;
        normalizeChannel(name, inlineReplyChannel);
        reply["channel"] = { $ref: `#/channels/${escapeReferenceToken(name)}` };
      }
      normalizeInlineMessages(root, `${operationName}_reply_message`, reply, "messages");
    }
  }

  if (Object.keys(servers).length === 0) delete root["servers"];
  if (Object.keys(channels).length === 0) delete root["channels"];
}

function normalizeInlineMessages(
  root: Record<string, unknown>,
  prefix: string,
  owner: Record<string, unknown>,
  field: string,
): void {
  const values = owner[field];
  if (!Array.isArray(values)) return;
  for (let index = 0; index < values.length; index++) {
    const message = inlineReferenceObject(values[index]);
    if (!message) continue;
    const components = ensureReferenceObjectMap(root, "components");
    const messages = ensureReferenceObjectMap(components, "messages");
    const name = uniqueReferenceKey(
      messages,
      `__openbindings_external_${sanitizeReferenceKey(prefix)}_${index}`,
    );
    messages[name] = message;
    values[index] = { $ref: `#/components/messages/${escapeReferenceToken(name)}` };
  }
}
