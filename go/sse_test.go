package asyncapiclient

import (
	"context"
	"io"
	"net/http"
	"reflect"
	"strings"
	"testing"
)

// sseCollectorHandle is a minimal artifactHandle that records the emitted
// output sequence and the terminal transition.
type sseCollectorHandle struct {
	values []any
	closed bool
	err    *ExecutionError
}

func (h *sseCollectorHandle) ReadInput(context.Context) (any, error) { return nil, io.EOF }
func (h *sseCollectorHandle) CloseInput() error                      { return nil }
func (h *sseCollectorHandle) EmitOutput(v any) error {
	h.values = append(h.values, v)
	return nil
}
func (h *sseCollectorHandle) CloseOutput()                { h.closed = true }
func (h *sseCollectorHandle) FireError(e *ExecutionError) { h.err = e }
func (h *sseCollectorHandle) Done() <-chan struct{}       { return nil }
func (h *sseCollectorHandle) SetHeader(Metadata) error    { return nil }
func (h *sseCollectorHandle) SetTrailer(Metadata)         {}

// The retained SSE event framing follows the WHATWG processing model: a
// lone empty `data:` line DISPATCHES an event whose data is the empty
// string (the data-buffer emptiness check precedes the trailing-LF strip),
// at its position in the stream; a block that carried no data line —
// comment-only or `event:`/`id:`-only — dispatches nothing; an incomplete
// final event is discarded. The SSE subscribe lane has no shipped entry
// today (validateCell refuses every standalone HTTP send before dispatch),
// so this exercises the retained framing engine directly. The stream bytes
// and the expected output sequence are the family-shared empty-data case,
// byte-identical with the openapi engines' shipped-path tests.
func TestStreamSSEEmptyDataEventDispatchesEmptyString(t *testing.T) {
	stream := ": comment only\n\n" + // comment-only: nothing
		"event: tick\nid: 7\n\n" + // fields-only: nothing
		"data: first\n\n" + // emits "first"
		"data:\n\n" + // lone empty data line: emits ""
		"data: third\n\n" + // emits "third"
		"data: incomplete-final-event" // no blank line: discarded
	resp := &http.Response{
		StatusCode: 200,
		Header:     http.Header{"Content-Type": {"text/event-stream"}},
		Body:       io.NopCloser(strings.NewReader(stream)),
	}
	h := &sseCollectorHandle{}
	streamSSE(context.Background(), resp, "", nil, &executionArgs{}, invokeSite{}, h)
	if h.err != nil {
		t.Fatalf("stream error: %v", h.err)
	}
	if !h.closed {
		t.Fatal("output boundary was not closed")
	}
	want := []any{"first", "", "third"}
	if !reflect.DeepEqual(h.values, want) {
		t.Fatalf("values = %#v, want %#v", h.values, want)
	}
}
