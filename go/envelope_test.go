package asyncapiclient

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
)

func parameterizedArtifact() []byte {
	return []byte(`{
  "asyncapi":"3.0.0",
  "info":{"title":"Envelope","version":"1.0.0"},
  "servers":{"production":{"host":"api.example.test","protocol":"https"}},
  "channels":{"orders":{"address":"/orders/{region}","parameters":{"region":{"enum":["emea","amer"]}},
    "messages":{"Order":{"contentType":"application/json","payload":{"type":"object"}}}}},
  "operations":{"place":{
    "action":"receive",
    "channel":{"$ref":"#/channels/orders"},
    "messages":[{"$ref":"#/channels/orders/messages/Order"}],
    "bindings":{"http":{"method":"POST"}}
  }}
}`)
}

// The routed envelope's parameter lane (§9.2, ruled 2026-08-14): the
// publish input {payload, region} splits pre-dispatch — the parameter
// expands the channel address, the payload alone rides the codec lane —
// and an explicitly supplied field wins over the
// configuration.address.parameters pre-fill.
func TestClientSplitsRoutedEnvelopeInput(t *testing.T) {
	var path, body string
	httpClient := &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		path = request.URL.Path
		payload, _ := io.ReadAll(request.Body)
		body = string(payload)
		return &http.Response{
			StatusCode: 204, Status: "204 No Content", Header: http.Header{},
			Body: io.NopCloser(strings.NewReader("")), Request: request,
		}, nil
	})}
	client, err := Load(context.Background(), Source{Content: parameterizedArtifact()}, LoadOptions{
		HTTPClient: httpClient,
		Context:    map[string]any{"configuration": map[string]any{"address": map[string]any{"parameters": map[string]any{"region": "amer"}}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close() }()
	if _, err := client.Publish(context.Background(), "place",
		map[string]any{"payload": map[string]any{"id": 9}, "region": "emea"}, InvocationOptions{}); err != nil {
		t.Fatal(err)
	}
	if path != "/orders/emea" {
		t.Fatalf("path = %q, want the explicit envelope field to win over the pre-fill", path)
	}
	if body != `{"id":9}` {
		t.Fatalf("body = %q, want the bare payload", body)
	}
}

// Config pre-fill is the amortized supply of the input: with no explicit
// field, the configured parameter expands the address; with neither, the
// invocation refuses before dispatch.
func TestClientEnvelopeParameterPreFillAndRefusal(t *testing.T) {
	var path string
	requests := 0
	httpClient := &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		requests++
		path = request.URL.Path
		return &http.Response{
			StatusCode: 204, Status: "204 No Content", Header: http.Header{},
			Body: io.NopCloser(strings.NewReader("")), Request: request,
		}, nil
	})}
	client, err := Load(context.Background(), Source{Content: parameterizedArtifact()}, LoadOptions{
		HTTPClient: httpClient,
		Context:    map[string]any{"configuration": map[string]any{"address": map[string]any{"parameters": map[string]any{"region": "amer"}}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close() }()
	if _, err := client.Publish(context.Background(), "place",
		map[string]any{"payload": map[string]any{"id": 1}}, InvocationOptions{}); err != nil {
		t.Fatal(err)
	}
	if path != "/orders/amer" {
		t.Fatalf("path = %q, want the configured pre-fill", path)
	}

	bare, err := Load(context.Background(), Source{Content: parameterizedArtifact()}, LoadOptions{HTTPClient: httpClient})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = bare.Close() }()
	before := requests
	_, err = bare.Publish(context.Background(), "place", map[string]any{"payload": map[string]any{"id": 1}}, InvocationOptions{})
	var failure *ExecutionError
	if !errors.As(err, &failure) {
		t.Fatalf("err = %v, want a pre-dispatch failure", err)
	}
	if requests != before {
		t.Fatalf("unresolved parameter dispatched anyway (requests %d -> %d)", before, requests)
	}

	// A bare (non-envelope) value on a parameterized channel refuses: the
	// schema is the envelope, and silently treating the object as payload
	// would misroute caller data.
	_, err = client.Publish(context.Background(), "place", map[string]any{"id": 1}, InvocationOptions{})
	if !errors.As(err, &failure) || failure.Code != ErrCodeRefused {
		t.Fatalf("bare value: failure = %#v (err %v)", failure, err)
	}
}

// Output-direction carriage: a headers-declaring reply rides the routed
// envelope — the decoded payload pairs with the declared application
// headers projected from the HTTP response's fields, declared-type parsing
// applied (declaration-driven, never sniffing), transport fields never
// leaking.
func TestClientProjectsReplyHeadersIntoOutputEnvelope(t *testing.T) {
	artifact := []byte(`{
  "asyncapi":"3.0.0",
  "info":{"title":"Reply headers","version":"1.0.0"},
  "servers":{"production":{"host":"api.example.test","protocol":"https"}},
  "channels":{"commands":{"address":"/commands","messages":{
    "Command":{"contentType":"application/json","payload":{"type":"object"}},
    "Result":{"contentType":"application/json","payload":{"type":"object"},
      "headers":{"type":"object","properties":{"requestId":{"type":"string"},"attempt":{"type":"integer"}}}}
  }}},
  "operations":{"submit":{
    "action":"receive",
    "channel":{"$ref":"#/channels/commands"},
    "messages":[{"$ref":"#/channels/commands/messages/Command"}],
    "bindings":{"http":{"method":"POST"}},
    "reply":{"messages":[{"$ref":"#/channels/commands/messages/Result"}]}
  }}
}`)
	httpClient := &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: 200, Status: "200 OK",
			Header: http.Header{
				"Content-Type": {"application/json"},
				"Requestid":    {"r-42"},
				"Attempt":      {"3"},
				"X-Transport":  {"never-projected"},
			},
			Body: io.NopCloser(strings.NewReader(`{"accepted":true}`)), Request: request,
		}, nil
	})}
	client, err := Load(context.Background(), Source{Content: artifact}, LoadOptions{HTTPClient: httpClient})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close() }()
	events, err := client.Publish(context.Background(), "submit", map[string]any{"id": 1}, InvocationOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 {
		t.Fatalf("events = %#v", events)
	}
	envelope, _ := events[0].Value.(map[string]any)
	payload, _ := envelope["payload"].(map[string]any)
	if payload["accepted"] != true {
		t.Fatalf("payload = %#v", envelope)
	}
	headers, _ := envelope["headers"].(map[string]any)
	if headers["requestId"] != "r-42" {
		t.Fatalf("headers = %#v, want requestId projected case-insensitively", headers)
	}
	if headers["attempt"] != float64(3) {
		t.Fatalf("headers = %#v, want attempt parsed as the declared integer", headers)
	}
	if _, leaked := headers["X-Transport"]; leaked {
		t.Fatalf("transport field leaked into application headers: %#v", headers)
	}
}
