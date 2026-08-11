import type {
  AsyncAPIChannel,
  AsyncAPIDocument,
  AsyncAPIMessage,
  AsyncAPIOperation,
  AsyncAPIOperationReply,
} from "./asyncapi-types.js";
import { UNRESOLVED_TRAIT_TAG } from "./constants.js";

type JSONObject = Record<string, unknown>;

/**
 * Applies AsyncAPI 3.0's Traits Merge Mechanism after reference resolution.
 * Traits are JSON Merge Patches applied in declaration order, followed by the
 * target itself so a trait never overrides an explicitly declared property.
 */
export function applyDocumentTraits(document: AsyncAPIDocument): AsyncAPIDocument {
  if (document.operations) {
    for (const [key, operation] of Object.entries(document.operations)) {
      document.operations[key] = applyTraits(operation as unknown as JSONObject) as unknown as AsyncAPIOperation;
    }
  }

  visitMessages(document.components?.messages);
  for (const channel of Object.values(document.channels ?? {})) visitChannelMessages(channel);
  for (const operation of Object.values(document.operations ?? {})) {
    visitMessageList(operation.messages);
    visitChannelMessages(operation.channel);
    visitReplyMessages(operation.reply);
  }
  return document;
}

function visitReplyMessages(reply: AsyncAPIOperationReply | undefined): void {
  if (!reply) return;
  visitMessageList(reply.messages);
  visitChannelMessages(reply.channel);
}

function visitChannelMessages(channel: AsyncAPIChannel | undefined): void {
  if (channel) visitMessages(channel.messages);
}

function visitMessages(messages: Record<string, AsyncAPIMessage> | undefined): void {
  if (!messages) return;
  for (const [key, message] of Object.entries(messages)) {
    messages[key] = applyTraits(message as unknown as JSONObject) as unknown as AsyncAPIMessage;
  }
}

function visitMessageList(messages: AsyncAPIMessage[] | undefined): void {
  if (!messages) return;
  for (let index = 0; index < messages.length; index += 1) {
    messages[index] = applyTraits(messages[index] as unknown as JSONObject) as unknown as AsyncAPIMessage;
  }
}

function applyTraits(target: JSONObject): JSONObject {
  const declared = target["traits"];
  if (!Array.isArray(declared)) return target;

  let inherited: unknown = {};
  let unresolved: string | undefined;
  for (const trait of declared) {
    if (!isJSONObject(trait)) continue;
    if (typeof trait["$ref"] === "string") {
      unresolved ??= trait["$ref"];
      continue;
    }
    inherited = mergePatch(inherited, trait);
  }

  const own = { ...target };
  delete own["traits"];
  const merged = mergePatch(inherited, own) as JSONObject;
  if (unresolved) merged[UNRESOLVED_TRAIT_TAG] = unresolved;
  return merged;
}

/** RFC 7396 JSON Merge Patch over JSON-compatible AsyncAPI objects. */
function mergePatch(
  target: unknown,
  patch: unknown,
  mergedPatches = new WeakMap<object, unknown>(),
): unknown {
  if (!isJSONObject(patch)) return cloneGraph(patch);

  // Reference resolution turns recursive schemas into cyclic object graphs.
  // JSON Merge Patch is defined over JSON trees, but applying the same rules to
  // that resolved graph must preserve a back-edge instead of recursively walking
  // it forever.
  const previous = mergedPatches.get(patch);
  if (previous !== undefined) return previous;

  const result: JSONObject = isJSONObject(target) ? cloneGraph(target) as JSONObject : {};
  mergedPatches.set(patch, result);
  for (const [key, value] of Object.entries(patch)) {
    if (value === null) delete result[key];
    else result[key] = mergePatch(result[key], value, mergedPatches);
  }
  return result;
}

function cloneGraph<T>(value: T, clones = new WeakMap<object, unknown>()): T {
  if (value === null || typeof value !== "object") return value;

  const previous = clones.get(value);
  if (previous !== undefined) return previous as T;

  if (Array.isArray(value)) {
    const result: unknown[] = [];
    clones.set(value, result);
    for (const member of value) result.push(cloneGraph(member, clones));
    return result as T;
  }
  if (isJSONObject(value)) {
    const result: JSONObject = {};
    clones.set(value, result);
    for (const [key, member] of Object.entries(value)) result[key] = cloneGraph(member, clones);
    return result as T;
  }
  return value;
}

function isJSONObject(value: unknown): value is JSONObject {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}
