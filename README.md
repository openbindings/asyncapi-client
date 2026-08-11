# OpenBindings AsyncAPI Client

A document-driven AsyncAPI 2.x/3.x client for invoking brownfield event APIs directly from their AsyncAPI documents.

The client does not generate source code and does not require an OpenBindings Interface (OBI). Load a document, select an authored operation, then publish or subscribe. This client's contract deliberately follows the artifact and incorporated AsyncAPI/protocol-binding rules for server and address resolution, protocol bindings, message selection, content type, security placement, replies, and lifecycle behavior.

This repository is also the execution substrate used by the OpenBindings AsyncAPI adapter. Its public API is intentionally AsyncAPI-native; protocol abstraction belongs in the adapter above it.

> Status: pre-release. TypeScript and Go clients are runnable and tested while
> their public APIs and qualification corpus are being stabilized.

## TypeScript

```ts
import { AsyncAPIClient } from "@openbindings/asyncapi-client";

const client = await AsyncAPIClient.load("https://example.com/asyncapi.yaml", {
  context: { bearerToken: process.env.EXAMPLE_TOKEN },
});

await client.publish("sendCommand", { id: "c-17" });

const subscription = await client.subscribe<{ id: string }>("commandEvents");
for await (const event of subscription.events) {
  console.log(event.value);
}
await subscription.completed;
```

`start` exposes the lower-level cardinality-neutral session when a caller
needs explicit input half-close, output iteration, cancellation, metadata,
and terminal completion.

## Go

```go
client, err := asyncapiclient.Load(ctx, asyncapiclient.Source{
    Location: "https://example.com/asyncapi.yaml",
}, asyncapiclient.LoadOptions{
    Context: map[string]any{"bearerToken": os.Getenv("EXAMPLE_TOKEN")},
})
if err != nil {
    log.Fatal(err)
}
defer client.Close()

events, err := client.Publish(ctx, "sendCommand", map[string]any{"id": "c-17"}, asyncapiclient.InvocationOptions{})
```

Go's `Client.Start` and `Execution` expose the same explicit session lifecycle
as TypeScript. `Client.Operations` inventories the artifact without creating
or synthesizing an OBI.

## Artifact semantics

- Exact AsyncAPI editions 2.0.0–2.6.0, 3.0.0, and 3.1.0 are accepted; other editions fail loudly.
- `receive` is invoked as a publish interaction; `send` is invoked as a subscription, because the artifact describes the application rather than the caller.
- Built-in HTTP/HTTPS execution uses the authored HTTP operation-binding method and message/reply declarations.
- Built-in WebSocket execution preserves ordering, input half-close, cancellation, bounded backpressure, connection sharing, and isolated subscriber failure.
- Additional protocols are installed as drivers. A missing driver is a local pre-dispatch capability error; document inventory remains independent of installed drivers.
- Server, server-variable, channel-address, protocol-field, message, content-type, and security choices are resolved from the artifact plus explicit caller context. The client does not guess when several valid choices remain.
- Operation and message traits are dereferenced and applied with AsyncAPI 3.0's ordered JSON Merge Patch rule before inventory, preparation, or dispatch; explicitly authored target properties retain precedence.
- Unsupported or ambiguous cells refuse before dispatch.

The standalone API may expose AsyncAPI, HTTP, WebSocket, and transport facts.
The OpenBindings adapter translates those facts into protocol-independent
outputs, errors, metadata, and lifecycle behavior.

Go consumers that maintain their own artifact AST can call
`NormalizeDocument` to share the client's AsyncAPI envelope validation and
trait interpretation without importing OpenBindings or the client's internal
document representation. TypeScript exposes the normalized parser from the
`@openbindings/asyncapi-client/analysis` entry point.

## Scope boundary

The client is an invocation runtime, not an AsyncAPI code generator, broker,
server framework, documentation renderer, or workflow engine. Protocols not
covered by a built-in or installed driver remain explicit execution gaps
rather than guessed implementations.

See [architecture](docs/architecture.md), [fidelity contract](docs/fidelity-contract.md), [adapter contract](docs/adapter-contract.md), [extraction ledger](docs/extraction-ledger.md), [release qualification](docs/release-qualification.md), and [conformance](conformance/README.md).

## Development

```sh
pnpm install
pnpm qualify:release
```

The release gate builds and tests both languages, verifies there is no
OpenBindings SDK runtime dependency, and installs the packed packages into
clean consumers.

## License

Apache-2.0.
