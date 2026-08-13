import yaml from "js-yaml";
import { dereference } from "./internal/index.js";

import type { AsyncAPIDocument } from "./asyncapi-types.js";
import {
  CHANNEL_NAME_TAG,
  CHANNEL_REF_TAG,
  MESSAGE_NAME_TAG,
  MESSAGE_REF_TAG,
  REF_NAME_TAG,
  SERVER_NAME_TAG,
} from "./constants.js";
import { applyDocumentTraits } from "./traits.js";
import { normalizeResolvedReferenceUnions } from "./compose.js";
import {
  discriminateAsyncAPIEdition,
  normalizeAsyncAPIEnvelope,
  parseV2OperationRef,
  refForNormalizedOperationKey,
} from "./edition.js";

// The u flag makes the class match whole code points, so an astral-plane
// character replaces as one underscore, not one per surrogate half
// (Go parity: SanitizeKey's regexp operates on runes).
/**
 * Keywords whose map values hold schemas under arbitrary member names. Inside
 * one, every child key is a member NAME, not a keyword.
 */
const SCHEMA_MAP_CONTAINER_KEYS = new Set([
  "properties",
  "patternProperties",
  "definitions",
  "$defs",
  "dependentSchemas",
]);

const NON_KEY_CHARS = /[^a-zA-Z0-9._-]/gu;

/** Replaces non-alphanumeric characters with underscores to produce a valid key. */
export function sanitizeKey(name: string): string {
  const key = name.replace(NON_KEY_CHARS, "_").replace(/^_+|_+$/g, "");
  if (!key) return "unnamed";
  // OBI-D-03 requires the first character to be a letter or underscore
  // (Go parity: SanitizeKey).
  return /^[A-Za-z_]/.test(key) ? key : `_${key}`;
}

/** Returns a unique variant of `key` by appending a numeric suffix if it already exists in `used`. */
export function uniqueKey(key: string, used: Set<string>): string {
  if (!used.has(key)) return key;
  for (let i = 2; ; i++) {
    const candidate = `${key}_${i}`;
    if (!used.has(candidate)) return candidate;
  }
}

/**
 * Compares strings by Unicode code point: the canonical ordering for
 * synthesis and inspection (Go parity: Go compares strings byte-wise, and
 * UTF-8 byte order is code point order). Neither `localeCompare` (collates
 * under the host locale, so output varies machine to machine) nor default
 * sort / UTF-16 code-unit `<` (ranks astral-plane code points below
 * U+E000..U+FFFF) matches the reference implementation. The order is
 * load-bearing beyond emission: it decides which of two colliding names
 * wins the bare key in {@link uniqueKey}.
 */
export function codePointCompare(a: string, b: string): number {
  let i = 0;
  while (i < a.length && i < b.length) {
    const ca = a.codePointAt(i) as number; // i < a.length, so defined
    const cb = b.codePointAt(i) as number;
    if (ca !== cb) return ca < cb ? -1 : 1;
    i += ca > 0xffff ? 2 : 1;
  }
  return a.length - b.length;
}

const SECURITY_SCHEME_REF_PREFIX = "#/components/securitySchemes/";

/**
 * Tags each internal `$ref`-only entry of one `security` list with the
 * components.securitySchemes key it points to (rule A's `name`), under
 * {@link REF_NAME_TAG} — a field the shared dereferencer's merge-copy path
 * carries onto the resolved scheme object (extra keys on a `$ref` node are
 * copied onto the target when the target doesn't already have them). Left
 * untouched: an inline scheme (no `$ref` — no addressable name to tag), a
 * `$ref` elsewhere in the document (channel/message $refs are unaffected —
 * only entries of a `security` array are visited), and an external or
 * non-securitySchemes-local `$ref` (no LOCAL components.securitySchemes key
 * exists for it).
 */
function tagSecurityRefNames(list: unknown): void {
  if (!Array.isArray(list)) return;
  for (const entry of list) {
    if (entry == null || typeof entry !== "object") continue;
    const e = entry as Record<string, unknown>;
    if (
      typeof e.$ref !== "string" ||
      !e.$ref.startsWith(SECURITY_SCHEME_REF_PREFIX)
    )
      continue;
    e[REF_NAME_TAG] = e.$ref.slice(SECURITY_SCHEME_REF_PREFIX.length);
  }
}

/**
 * Applies {@link tagSecurityRefNames} to every `security` list in the raw
 * (pre-dereference) document: each operation's and each server's. Mirrors
 * tagNameKeys' shape — a targeted pass over the RAW document before the
 * generic dereferencer runs.
 */
function tagAllSecurityRefNames(raw: unknown): void {
  if (raw == null || typeof raw !== "object") return;
  const doc = raw as Record<string, unknown>;
  const operations = doc.operations;
  if (operations != null && typeof operations === "object") {
    for (const op of Object.values(operations as Record<string, unknown>)) {
      if (op != null && typeof op === "object") {
        tagSecurityRefNames((op as Record<string, unknown>).security);
      }
    }
  }
  const servers = doc.servers;
  if (servers != null && typeof servers === "object") {
    for (const server of Object.values(servers as Record<string, unknown>)) {
      if (server != null && typeof server === "object") {
        tagSecurityRefNames((server as Record<string, unknown>).security);
      }
    }
  }
}

/**
 * Tags each entry of a raw top-level map (`channels`, `servers`) with its
 * own map key under `tag`, so the key survives dereferencing: an
 * operation's channel `$ref` and a channel's `servers` subset entries
 * resolve to clones of these tagged objects, and the invoke path reads the
 * name back off the resolved object (target.ts's channelNameOf /
 * serverNameOf). The tag rides the TARGET object — the name is the entry's
 * own map key, a property of the target, not of any one reference.
 */
function tagNameKeys(raw: unknown, mapField: string, tag: string): void {
  if (raw == null || typeof raw !== "object") return;
  const map = (raw as Record<string, unknown>)[mapField];
  if (map == null || typeof map !== "object") return;
  for (const [name, entry] of Object.entries(map as Record<string, unknown>)) {
    if (entry == null || typeof entry !== "object") continue;
    (entry as Record<string, unknown>)[tag] = name;
  }
}

function tagChannelMessageNames(raw: unknown): void {
  if (raw == null || typeof raw !== "object") return;
  const channels = (raw as Record<string, unknown>)["channels"];
  if (
    channels == null ||
    typeof channels !== "object" ||
    Array.isArray(channels)
  )
    return;
  for (const channel of Object.values(channels as Record<string, unknown>)) {
    if (
      channel == null ||
      typeof channel !== "object" ||
      Array.isArray(channel)
    )
      continue;
    const messages = (channel as Record<string, unknown>)["messages"];
    if (
      messages == null ||
      typeof messages !== "object" ||
      Array.isArray(messages)
    )
      continue;
    for (const [name, message] of Object.entries(
      messages as Record<string, unknown>,
    )) {
      if (
        message != null &&
        typeof message === "object" &&
        !Array.isArray(message)
      ) {
        (message as Record<string, unknown>)[MESSAGE_NAME_TAG] = name;
      }
    }
  }
}

/** Retains the source spelling of operation and reply message references. */
function tagMessageRefs(raw: unknown): void {
  if (raw == null || typeof raw !== "object") return;
  // An operation may be declared inline in the operations map OR in
  // components.operations and referenced from the map; both spellings carry
  // taggable message references (the reply twin of this rule lives below,
  // and the same lesson applied: a form the walk does not visit loses its
  // declared identity silently).
  const owners: unknown[] = [(raw as Record<string, unknown>)["operations"]];
  const components = (raw as Record<string, unknown>)["components"];
  if (components != null && typeof components === "object" && !Array.isArray(components)) {
    owners.push((components as Record<string, unknown>)["operations"]);
  }
  for (const operations of owners) {
    tagOperationsMessageRefs(operations);
  }
  const componentsReplies =
    components != null && typeof components === "object" && !Array.isArray(components)
      ? (components as Record<string, unknown>)["replies"]
      : undefined;
  if (componentsReplies != null && typeof componentsReplies === "object" && !Array.isArray(componentsReplies)) {
    for (const reply of Object.values(componentsReplies as Record<string, unknown>)) {
      if (reply == null || typeof reply !== "object" || Array.isArray(reply)) continue;
      tagMessageListRefs((reply as Record<string, unknown>)["messages"]);
    }
  }
}

function tagOperationsMessageRefs(operations: unknown): void {
  if (operations == null || typeof operations !== "object" || Array.isArray(operations)) return;
  for (const operation of Object.values(operations as Record<string, unknown>)) {
    if (operation == null || typeof operation !== "object" || Array.isArray(operation)) continue;
    const op = operation as Record<string, unknown>;
    for (const owner of [op, op["reply"]]) {
      if (owner == null || typeof owner !== "object" || Array.isArray(owner)) continue;
      const channel = (owner as Record<string, unknown>)["channel"];
      if (channel == null || typeof channel !== "object" || Array.isArray(channel)) continue;
      const ref = (channel as Record<string, unknown>)["$ref"];
      if (typeof ref === "string" && !ref.startsWith("#/")) {
        (channel as Record<string, unknown>)[CHANNEL_REF_TAG] = ref;
      }
    }
    const lists: unknown[] = [op["messages"]];
    const reply = op["reply"];
    if (reply != null && typeof reply === "object" && !Array.isArray(reply)) {
      lists.push((reply as Record<string, unknown>)["messages"]);
    }
    for (const list of lists) {
      tagMessageListRefs(list);
    }
  }
}

function tagMessageListRefs(list: unknown): void {
  if (!Array.isArray(list)) return;
  for (const member of list) {
    if (member == null || typeof member !== "object" || Array.isArray(member)) continue;
    const ref = (member as Record<string, unknown>)["$ref"];
    if (typeof ref === "string") {
      (member as Record<string, unknown>)[MESSAGE_REF_TAG] = ref;
    }
  }
}

function validateRawFixedFields(raw: Record<string, unknown>): void {
  const operations = raw["operations"];
  if (operations != null) {
    if (typeof operations !== "object" || Array.isArray(operations)) {
      throw new Error("not a valid AsyncAPI document (operations must be an object)");
    }
    for (const [name, value] of Object.entries(operations as Record<string, unknown>)) {
      if (value == null || typeof value !== "object" || Array.isArray(value)) {
        throw new Error(`not a valid AsyncAPI document (operation ${JSON.stringify(name)} must be an object)`);
      }
      const operation = value as Record<string, unknown>;
      if (typeof operation["$ref"] === "string") continue;
      const channel = operation["channel"];
      if (
        channel == null
        || typeof channel !== "object"
        || Array.isArray(channel)
        || typeof (channel as Record<string, unknown>)["$ref"] !== "string"
      ) {
        throw new Error(`not a valid AsyncAPI document (operation ${JSON.stringify(name)} channel must be a Reference Object)`);
      }
    }
  }

  const channels = raw["channels"];
  if (channels == null || typeof channels !== "object" || Array.isArray(channels)) return;
  for (const [name, value] of Object.entries(channels as Record<string, unknown>)) {
    if (value == null || typeof value !== "object" || Array.isArray(value)) continue;
    const servers = (value as Record<string, unknown>)["servers"];
    if (servers === undefined) continue;
    if (
      !Array.isArray(servers)
      || servers.some((member) =>
        member == null
        || typeof member !== "object"
        || Array.isArray(member)
        || typeof (member as Record<string, unknown>)["$ref"] !== "string")
    ) {
      throw new Error(`not a valid AsyncAPI document (channel ${JSON.stringify(name)} servers must contain Reference Objects)`);
    }
  }
}

/** Fetches (if needed) and parses an AsyncAPI document from a location URL or inline content. */
export async function parseAsyncAPIDocument(
  location?: string,
  content?: unknown,
  options?: { signal?: AbortSignal },
  fetchFn?: typeof globalThis.fetch,
): Promise<AsyncAPIDocument> {
  let raw: unknown;

  if (content !== undefined) {
    // A present content that is not the document text (string) or the
    // document itself (object) — a present null included — fails the
    // document checks below loudly.
    if (typeof content === "string") {
      raw = parseDocumentText(content);
    } else {
      raw = content;
    }
  } else if (location) {
    // `location` must be an absolute URI (ASYNC-D-02) — a bare filesystem
    // path is refused loudly before any fetch is attempted.
    validateDocumentAddress(location);
    const resp = await (fetchFn ?? fetch)(location, { signal: options?.signal });
    if (!resp.ok) {
      throw new Error(
        `failed to fetch ${location}: ${resp.status} ${resp.statusText}`,
      );
    }
    const text = await resp.text();
    raw = parseDocumentText(text);
  } else {
    throw new Error("source must have location or content");
  }

  if (raw === null || typeof raw !== "object") {
    throw new Error("not a valid AsyncAPI document (missing 'asyncapi' field)");
  }

  // ASYNC-P-01 is the root discriminator and therefore precedes reference
  // resolution. An unsupported edition must refuse deterministically without
  // fetching a closure that this binding will never interpret.
  const declaredVersion = (raw as Record<string, unknown>)["asyncapi"];
  if (declaredVersion === undefined) {
    throw new Error("not a valid AsyncAPI document (missing 'asyncapi' field)");
  }
  // External-composition pre-pass, mirroring the Go client's artifact
  // resolver: external references inline BEFORE edition normalization (so a
  // v2 message declared as an external file ref is keyed by its resolved
  // name/messageId, not a synthetic position key), and a fragment reference
  // INSIDE an inlined external resource resolves against that resource's own
  // root rather than dangling in the entry document's context. The entry
  // document's internal references are preserved untouched for the ordinary
  // position-aware resolution below. Skipped entirely when no external-form
  // reference exists, which is the common case.
  discriminateAsyncAPIEdition(raw as Record<string, unknown>);
  if (containsExternalRef(raw)) {
    // Tag source identity BEFORE inlining: the original reference spellings
    // are the artifact's deepest declared addresses, and coverage reports
    // them. The dereferencer's sibling merge carries these private tags onto
    // each inlined target.
    tagMessageRefs(raw);
    raw = await dereference<Record<string, unknown>>(
      raw as Record<string, unknown>,
      {
        baseUrl: location,
        fetch: fetchFn ?? fetch,
        parse: parseDocumentText,
        signal: options?.signal,
        internalRefs: "preserve",
        // Fetch and parse failures are document-level errors (thrown); a
        // missing fragment inside a fetched document is preserved for
        // per-operation eligibility and coverage, exactly as in Go.
        allowUnresolved: true,
        shouldTraverseChild: literalAwareTraversal,
      },
    );
    // AsyncAPI requires several positions to BE Reference Objects; re-hoist
    // what inlining flattened, exactly as the Go client does (the synthetic
    // key spellings must stay identical between the two implementations).
    normalizeResolvedReferenceUnions(raw as Record<string, unknown>);
  }

  raw = normalizeAsyncAPIEnvelope(raw as Record<string, unknown>);

  validateRawFixedFields(raw as Record<string, unknown>);

  tagNameKeys(raw, "channels", CHANNEL_NAME_TAG);
  tagNameKeys(raw, "servers", SERVER_NAME_TAG);
  tagChannelMessageNames(raw);
  tagMessageRefs(raw);
  tagAllSecurityRefNames(raw);

  // Resolve all $ref pointers. External $refs fetch through the injected
  // fetch (callers that must stay side-effect-free inject a rejecting one).
  const resolved = await dereference<AsyncAPIDocument>(
    raw as Record<string, unknown>,
    {
      baseUrl: location,
      fetch: fetchFn ?? fetch,
      parse: parseDocumentText,
      signal: options?.signal,
      // AsyncAPI synthesis accounts for invalid operations individually.
      // Preserve a dangling target here so the eligibility/coverage layer can
      // exclude that operation without rejecting unrelated valid operations.
      allowUnresolved: true,
      // These fields carry application data, not reference-bearing artifact
      // structure. A literal object such as {"$ref":"customer-value"} in
      // an enum or example must survive unchanged. Extension values are also
      // owned by their extension vocabulary, not by AsyncAPI's Reference
      // Object rules.
      shouldTraverseChild: literalAwareTraversal,
      // AsyncAPI 3.0 Reference Objects cannot be extended; siblings are
      // ignored. Preserve only our private identity tags, which are removed
      // from projected operation schemas and exist solely to retain source
      // addresses after dereferencing.
      mergeRefSiblings: (target, reference) => {
        const merged = { ...target };
        for (const tag of [
          CHANNEL_NAME_TAG,
          CHANNEL_REF_TAG,
          MESSAGE_NAME_TAG,
          MESSAGE_REF_TAG,
          REF_NAME_TAG,
          SERVER_NAME_TAG,
        ]) {
          if (!Object.hasOwn(merged, tag) && Object.hasOwn(reference, tag)) {
            merged[tag] = reference[tag];
          }
        }
        return merged;
      },
    },
  );

  if (
    resolved.info == null
    || typeof resolved.info !== "object"
    || typeof resolved.info.title !== "string"
    || typeof resolved.info.version !== "string"
  ) {
    throw new Error("not a valid AsyncAPI document (info.title and info.version are required strings)");
  }

  return applyDocumentTraits(resolved);
}

/** Parse JSON-looking documents as strict JSON and YAML as YAML 1.2's JSON schema. */
/**
 * Position-aware traversal filter shared by the external-composition
 * pre-pass and the ordinary dereference: literal-value keys (`const`,
 * `default`, `enum`, `example`, `examples`) and extension values carry
 * application data, not reference-bearing structure — EXCEPT inside a
 * map-of-schemas container, where every child key is a member NAME, not a
 * keyword (a corpus artifact carried exactly `properties.enum.$ref`, which a
 * positionless skip leaves dangling).
 */
function literalAwareTraversal(
  _owner: Record<string, unknown>,
  key: string,
  _value: unknown,
  ownerKey?: string,
): boolean {
  return (
    (ownerKey !== undefined && SCHEMA_MAP_CONTAINER_KEYS.has(ownerKey))
    || (!["const", "default", "enum", "example", "examples"].includes(key)
      && !key.toLowerCase().startsWith("x-"))
  );
}

/**
 * Whether any external-form `$ref` (one with a non-fragment part) exists,
 * outside literal-value positions. Gates the external-composition pre-pass.
 */
function containsExternalRef(value: unknown, ownerKey?: string): boolean {
  if (value === null || typeof value !== "object") return false;
  if (Array.isArray(value)) return value.some((child) => containsExternalRef(child, ownerKey));
  const object = value as Record<string, unknown>;
  if (typeof object.$ref === "string" && !object.$ref.startsWith("#")) return true;
  for (const [key, child] of Object.entries(object)) {
    if (!literalAwareTraversal(object, key, child, ownerKey)) continue;
    if (containsExternalRef(child, key)) return true;
  }
  return false;
}

function parseDocumentText(text: string): unknown {
  const normalized = text.charCodeAt(0) === 0xfeff ? text.slice(1) : text;
  const first = normalized.trimStart()[0];
  if (first === "{" || first === "[") return JSON.parse(normalized);
  return yaml.load(normalized, { schema: yaml.JSON_SCHEMA });
}

/**
 * Checks ASYNC-D-02's location grammar offline, without dereferencing:
 * `location`, when present, is an absolute URI addressing the AsyncAPI
 * document itself. A bare filesystem path is a relative reference in form
 * (core OBI-D-05) and is refused — a local artifact is addressed as
 * file:// or embedded as the source's content.
 */
export function validateDocumentAddress(location: string): void {
  try {
    new URL(location);
  } catch {
    throw new Error(
      `asyncapi location ${JSON.stringify(location)} is not an absolute URI addressing the document (ASYNC-D-02): a local artifact is addressed as file:// or embedded as the source's content`,
    );
  }
}

/**
 * Parses a binding ref per ASYNC-D-03: a JSON Pointer
 * `#/operations/<operation-key>` addressing an operations-map entry is the
 * ONLY conformant spelling. A bare operation key without the pointer
 * prefix is refused (the former lenience is gone), and an unescaped `/`
 * after the prefix addresses a deeper path — never an operations-map
 * entry — so it is refused too. Operation keys containing `/` or `~` carry
 * RFC 6901 escaping in the pointer: `~1` → `/`, `~0` → `~`, decoded in
 * that order.
 */
export function parseRef(ref: string): string {
  ref = ref.trim();
  if (!ref) {
    throw new Error(
      "ref is required and must be a JSON Pointer #/operations/<operation-key> (ASYNC-D-03)",
    );
  }

  const v2 = parseV2OperationRef(ref);
  if (v2) return v2;
  const prefix = "#/operations/";
  if (!ref.startsWith(prefix)) {
    throw new Error(
      `ref "${ref}" is not a JSON Pointer #/operations/<operation-key>: the pointer is the only conformant spelling — a bare operation key is not accepted (ASYNC-D-03)`,
    );
  }
  const token = ref.slice(prefix.length);
  if (!token)
    throw new Error(`empty operation key in ref "${ref}" (ASYNC-D-03)`);
  if (token.includes("/")) {
    throw new Error(
      `ref "${ref}" addresses a deeper path, not an operations-map entry: an operation key containing / carries RFC 6901 escaping (~1) (ASYNC-D-03)`,
    );
  }
  return token.replaceAll("~1", "/").replaceAll("~0", "~");
}

/**
 * Builds the conformant ASYNC-D-03 spelling for an operation key:
 * `#/operations/` + the RFC 6901-escaped key (`~` → `~0` first, then
 * `/` → `~1` — escape order is the reverse of decode order).
 */
export function operationRef(opID: string): string {
  const v2 = refForNormalizedOperationKey(opID);
  if (v2) return v2;
  return `#/operations/${opID.replaceAll("~", "~0").replaceAll("/", "~1")}`;
}

/** Extracts a human-readable message from an unknown thrown value. */
export function errorMessage(e: unknown): string {
  if (e instanceof Error) return e.message;
  return String(e);
}
