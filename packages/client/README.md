# Rust-backed AsyncAPI TypeScript preview

This private development package exposes the same Rust source inspection, compilation and pure preparation as the native crate. JSON/YAML documents in the 2.6, 3.0 and 3.1 reader families are admitted; complete edition conformance and protocol qualification remain open work. Preparation currently supports a single schema-free binary, JSON or UTF-8 text message over MQTT 3.1.1 or WebSocket RFC 6455. The historical TypeScript engine is not used.

Build with `npm run build:wasm` (Rust and wasm-bindgen 0.2.129 for package developers), then `npm run build`. A consumer of the built package needs the included Wasm assets, not Rust. Package-consumer and actual browser/Worker execution qualification is still pending.

```ts
import { createClient } from '@openbindings/asyncapi-client';
const client = await createClient();
using document = client.parse(source, { sourceUri: 'https://example.test/api.json' });
for (const entry of document.operations()) {
  if (!entry.ok) { console.error(entry.error.code); continue; }
  using operation = entry.value;
  console.log(operation.identity, operation.describe());
}
```

For Workers, pass a compiled Wasm module to `createClient({ wasm: module })`. For testing in Node, pass the asset bytes explicitly. The generated ABI is private. Initialization shares one module instance, including concurrent calls; independent Wasm heaps cannot exchange these handles.

Each document, operation, compilation, plan and source view owns its backing source. Dispose each returned handle with `using`, `[Symbol.dispose]()` or `.dispose()`. Disposing a parent does not invalidate retained children. Source views expose raw JSON and exact number tokens without forcing values through JavaScript numbers. Parse original text when numeric fidelity matters; previously rounded JavaScript numbers cannot recover their original values.

For a YAML source view, `.raw` is the defining authored snippet and `.json` is its logical JSON representation. `location.bytes` and `location.aliases` use UTF-8 byte offsets, not JavaScript string indices. Alias-use ranges make expansion provenance explicit. Numeric and boolean-looking plain mapping keys stay strings; scalar values use the documented JSON-compatible YAML policy.

Independent values use the same owning Rust views:

```ts
using exact = client.parseJson('{"id":900719925474099312345}');
using id = exact.get('id');
console.log(id?.numberText); // exact decimal token
using ordinary = client.fromValue({ enabled: true, labels: ['a', 'b'] });
```

`parseJson` accepts only strict JSON. `fromValue` accepts finite numbers, Unicode
strings, booleans, null, dense plain arrays and plain records with enumerable own
string data properties. It preserves negative zero. It rejects undefined,
nonfinite numbers, BigInt, holes, symbols, cycles, accessors, custom prototypes
and non-JSON properties. It never invokes getters or `toJSON`. Repeated shared
objects are allowed when they do not form a cycle. Proxy traps and reflection
remain caller/host work; enumerating an object's keys can allocate before its
size is known. Previously rounded JavaScript numbers cannot be recovered.

Both methods accept `{bytes, nodes, depth}` limits; defaults are 16 MiB, 500,000
nodes and depth 96. Root depth is zero; depth is capped at 96. UTF-16 is checked
before Wasm conversion so malformed surrogate input cannot silently change.
The facade converts host representations; Rust owns JSON admission and values.
These constructors do not validate schemas. Prepared JSON/text transport uses the same value model.

Compile once, then make a reusable deployment plan in Rust through the facade:

```ts
using operation = document.operation('emit');
using compiled = operation.compile();
console.log(compiled.describe().servers, compiled.describe().messages);
using plan = compiled.prepare({ role: 'application' });
console.log(plan.describe().transport);
```

Preparation performs no network I/O. Missing choices produce structured requirements;
unsupported schemas, authentication schemes and replies are refused explicitly by
this initial slice. A WebSocket peer requires a real peer route, not a second
connection to the same server. Plan descriptions contain endpoint and client
identity but never credentials. See [preparation contract](../../docs/preparation.md).

## Host WebSocket sessions

The first transport API uses Rust session policy and Rust-owned host callbacks.
Prepare send and receive plans for the same WebSocket endpoint, then:

```ts
using session = await client.openSession([sendPlan, receivePlan]);
using sender = session.sender();
const receipt = sender.send(0, new Uint8Array([1, 2, 3]));
// receipt.kind === 'webSocketHostAccepted': no flush/delivery promise.
const incoming = await session.next({ signal: AbortSignal.timeout(5000) });
if (incoming?.kind === 'message') console.log(incoming.operation, incoming.payload);
else if (incoming?.kind === 'rejected') console.log(incoming.reason);
await session.close();
```

`next` allows one pending wait. Cancellation leaves a queued observation intact;
closing or disposing a session invalidates retained senders. Close consumes its
owner and waits for a clean close event. Canceling close disposes that owner.
Dropping/disposal removes callbacks and initiates close, without promising that
the remote peer has observed it. Call `.dispose()` explicitly if `using` is not
available. Runtime failures expose `code` and `deliveryUnknown`.

Options accept `connectTimeoutMs`, `closeTimeoutMs` and `limits` with
`maxMessages`, `maxBufferedBytes`, `maxMessageBytes`. Defaults/maxima are 64
library-owned messages and 1 MiB of payload. Queue overflow terminates the
session, with queued observations drained before the terminal error. The host
send-buffer amount is checked separately before submission. Browser buffering
before a message event cannot be controlled by this API and is outside the Rust
queue budget. Authentication headers and protocol profiles beyond this initial
binary WebSocket path are still open.

Actual local Worker exchanges use the tested workerd 1.20261010.1 constructor
profile, with compatibility date 2026-10-08. The previous runtime had an
intermittent shutdown anomaly; its failures remain recorded. The updated
constructor profile passed the client suites and 800 diagnostic connections,
while other raw connection paths still reproduced errors. Every host error
stays a failure even if followed by a clean close event. Full transport
qualification and deployed Cloudflare destination verification remain open.

## JSON and text messages

The prepared content type selects the codec; WebSocket frame policy is exposed in
`plan.describe().transport.frame`. JSON/text default to text frames, with an
explicit `websocketFrame: 'binary'` option. Schema-bearing declarations still
require an evaluator and are refused during preparation.

`sender.sendText(operation, text)` sends UTF-8 values. For JSON, use
`sender.sendValue(operation, plainValue)` or `sender.sendJson(operation, exactView)`.
`send(operation, Uint8Array)` admits already encoded bytes through the selected
codec. Invalid outbound values fail before the host send, with `deliveryUnknown`
false. Runtime codec diagnostics are available as `error.diagnostic`.

Every accepted incoming message has `payload` bytes and a `codec` discriminator.
UTF-8 messages additionally expose `text`; JSON messages expose an owning `value`
(JsonView), which the caller must dispose. A retained child remains valid after
that value or the session is disposed. `kind: 'invalidPayload'` includes a safe
`diagnostic` and `payloadBytes`; it is distinct from wrong-frame `rejected` and
terminal session errors. Neither observation prevents the next valid message.
