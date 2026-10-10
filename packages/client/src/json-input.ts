/** Host representation conversion only. Strict JSON admission stays in Rust. */
export interface ValueLimits {
  readonly bytes?: number;
  readonly depth?: number;
  readonly nodes?: number;
}
export type Bounds = { readonly bytes: number; readonly depth: number; readonly nodes: number };
type Refuse = (code: 'InvalidValue' | 'InvalidConfiguration' | 'Limit', detail: string) => never;
export function bounds(options: ValueLimits, refuse: Refuse): Bounds {
  if (!options || typeof options !== 'object' || Array.isArray(options)) refuse('InvalidConfiguration', 'JSON limits must be an options object');
  const result = { bytes: options.bytes === undefined ? 16 * 1024 * 1024 : options.bytes, depth: options.depth === undefined ? 96 : options.depth, nodes: options.nodes === undefined ? 500_000 : options.nodes };
  for (const value of Object.values(result)) {
    if (!Number.isInteger(value) || value < 0 || value > 0xffff_ffff) refuse('InvalidConfiguration', 'JSON limits must be unsigned 32-bit integers');
  }
  return { ...result, depth: Math.min(result.depth, 96) };
}
/** Check UTF-16 -> UTF-8 conversion before Wasm allocation, without changing source. */
export function admitSource(source: string, limit: Bounds, refuse: Refuse): void {
  if (typeof source !== 'string') refuse('InvalidValue', 'JSON source must be a string');
  if (source.length > limit.bytes) refuse('Limit', 'JSON source byte limit exceeded');
  let bytes = 0;
  for (let i = 0; i < source.length; i++) {
    const code = source.charCodeAt(i);
    if (code >= 0xd800 && code <= 0xdbff) {
      const next = source.charCodeAt(++i);
      if (!(next >= 0xdc00 && next <= 0xdfff)) refuse('InvalidValue', 'JSON source must contain Unicode scalar values');
      bytes += 4;
    } else if (code >= 0xdc00 && code <= 0xdfff) refuse('InvalidValue', 'JSON source must contain Unicode scalar values');
    else bytes += code < 128 ? 1 : code < 2048 ? 2 : 3;
    if (bytes > limit.bytes) refuse('Limit', 'JSON source byte limit exceeded');
  }
}
export function ordinaryJson(value: unknown, limit: Bounds, refuse: Refuse): string {
  const chunks: string[] = [];
  const ancestors = new Set<object>();
  let bytes = 0, nodes = 0;
  function room(count: number): void {
    if (count > limit.bytes - bytes) refuse('Limit', 'JSON construction byte limit exceeded');
  }
  function ascii(text: string): void { room(text.length); bytes += text.length; chunks.push(text); }
  function string(text: string): void {
    // Count escaped UTF-8 before allocating JSON.stringify's output. JavaScript
    // permits lone surrogates; the Rust value model intentionally does not.
    if (text.length > limit.bytes - bytes) refuse('Limit', 'JSON construction byte limit exceeded');
    let size = 2;
    for (let i = 0; i < text.length; i++) {
      const code = text.charCodeAt(i);
      if (code >= 0xd800 && code <= 0xdbff) {
        const next = text.charCodeAt(++i);
        if (!(next >= 0xdc00 && next <= 0xdfff)) refuse('InvalidValue', 'JSON strings must contain Unicode scalar values');
        size += 4;
      } else if (code >= 0xdc00 && code <= 0xdfff) refuse('InvalidValue', 'JSON strings must contain Unicode scalar values');
      else if (code === 34 || code === 92 || [8, 9, 10, 12, 13].includes(code)) size += 2;
      else if (code < 32) size += 6;
      else size += code < 128 ? 1 : code < 2048 ? 2 : 3;
      room(size);
    }
    room(size); bytes += size; chunks.push(JSON.stringify(text));
  }
  function property(object: object, key: PropertyKey): unknown {
    const descriptor = Object.getOwnPropertyDescriptor(object, key);
    if (!descriptor || !('value' in descriptor) || !descriptor.enumerable) {
      refuse('InvalidValue', 'JSON records require enumerable own data properties');
    }
    return descriptor.value;
  }
  function visit(input: unknown, depth: number): void {
    if (depth > limit.depth || nodes >= limit.nodes) refuse('Limit', 'JSON construction depth or node limit exceeded');
    nodes++;
    if (input === null) { ascii('null'); return; }
    if (typeof input === 'boolean') { ascii(input ? 'true' : 'false'); return; }
    if (typeof input === 'number') {
      if (!Number.isFinite(input)) refuse('InvalidValue', 'Nonfinite numbers cannot be represented as JSON');
      ascii(Object.is(input, -0) ? '-0' : String(input)); return;
    }
    if (typeof input === 'string') { string(input); return; }
    if (typeof input !== 'object') refuse('InvalidValue', 'Value cannot be represented as JSON');
    if (ancestors.has(input)) refuse('InvalidValue', 'Cyclic values cannot be represented as JSON');
    ancestors.add(input);
    const array = Array.isArray(input), prototype = Object.getPrototypeOf(input);
    if ((!array && prototype !== Object.prototype && prototype !== null) || (array && prototype !== Array.prototype)) {
      refuse('InvalidValue', 'JSON input must use plain records or arrays');
    }
    const keys = Reflect.ownKeys(input);
    if (array) {
      // Check the length descriptor instead of performing a property get (which
      // a Proxy could turn into unrelated behavior). Traps remain caller code.
      const lengthDescriptor = Object.getOwnPropertyDescriptor(input, 'length');
      const length = lengthDescriptor && 'value' in lengthDescriptor ? lengthDescriptor.value as unknown : undefined;
      if (typeof length !== 'number' || !Number.isInteger(length) || length < 0 || length > 0xffff_ffff) refuse('InvalidValue', 'Invalid array length');
      if (length > limit.nodes - nodes) refuse('Limit', 'JSON construction node limit exceeded');
      if (keys.length !== length + 1 || keys.some(key => typeof key !== 'string' || (key !== 'length' && (String(Number(key)) !== key || Number(key) < 0 || Number(key) >= length || !Number.isInteger(Number(key)))))) {
        refuse('InvalidValue', 'JSON arrays require every element and no extra own properties');
      }
      ascii('[');
      for (let i = 0; i < length; i++) { if (i) ascii(','); visit(property(input, String(i)), depth + 1); }
      ascii(']');
    } else {
      if (keys.length > limit.nodes - nodes) refuse('Limit', 'JSON construction node limit exceeded');
      ascii('{');
      keys.forEach((key, index) => {
        if (typeof key !== 'string') refuse('InvalidValue', 'JSON records require string keys');
        const entry = property(input, key);
        if (index) ascii(','); string(key); ascii(':'); visit(entry, depth + 1);
      });
      ascii('}');
    }
    ancestors.delete(input);
  }
  visit(value, 0);
  return chunks.join('');
}
