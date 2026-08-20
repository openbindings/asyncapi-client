import { describe, expect, it } from "vitest";
import { streamSSE } from "./sse.js";
import { builtinPerEventDecodeFor } from "./invoke.js";
import type { BindingHandle, BindingInvocationArgs, InvocationError, InvokeSite } from "./internal/index.js";

// The retained SSE event framing follows the WHATWG processing model: a
// lone empty `data:` line DISPATCHES an event whose data is the empty
// string (the data-buffer emptiness check precedes the trailing-LF strip),
// at its position in the stream; a block that carried no data line —
// comment-only or `event:`/`id:`-only — dispatches nothing; an incomplete
// final event is discarded. The SSE subscribe lane has no shipped entry
// today (every standalone HTTP send refuses before dispatch), so this
// exercises the retained framing engine directly, with the same per-event
// builtin the SSE lane wires. The stream bytes and the expected output
// sequence are the family-shared empty-data case, byte-identical with the
// openapi engines' shipped-path tests.
describe("SSE event framing (WHATWG dispatch)", () => {
  it("dispatches the empty string for a lone empty data line at its position", async () => {
    const stream =
      ": comment only\n\n" + // comment-only: nothing
      "event: tick\nid: 7\n\n" + // fields-only: nothing
      "data: first\n\n" + // emits "first"
      "data:\n\n" + // lone empty data line: emits ""
      "data: third\n\n" + // emits "third"
      "data: incomplete-final-event"; // no blank line: discarded
    const values: unknown[] = [];
    let closed = false;
    let error: InvocationError | undefined;
    const handle = {
      signal: new AbortController().signal,
      emitOutput: async (v: unknown) => {
        values.push(v);
      },
      closeOutput: () => {
        closed = true;
      },
      fireError: (e: InvocationError) => {
        error = e;
      },
    } as unknown as BindingHandle<unknown, unknown>;
    const site: InvokeSite = {
      operation: "",
      invokedAs: "",
      bindingKey: "",
      bindingSpec: "asyncapi/full",
      ref: "",
      target: "",
    };
    const resp = new Response(stream, {
      status: 200,
      headers: { "Content-Type": "text/event-stream" },
    });
    await streamSSE(resp, {} as BindingInvocationArgs, site, handle, {}, builtinPerEventDecodeFor(""));
    expect(error).toBeUndefined();
    expect(closed).toBe(true);
    expect(values).toEqual(["first", "", "third"]);
  });
});
