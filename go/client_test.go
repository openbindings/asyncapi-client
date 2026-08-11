package asyncapiclient

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
)

func httpArtifact() []byte {
	return []byte(`{
  "asyncapi":"3.0.0",
  "info":{"title":"Standalone client","version":"1.0.0"},
  "defaultContentType":"application/json",
  "servers":{"production":{"host":"api.example.test","protocol":"https"}},
  "channels":{"commands":{"address":"/commands","messages":{
    "Command":{"payload":{"type":"object"}},
    "Result":{"payload":{"type":"object"}}
  }}},
  "operations":{"submit":{
    "action":"receive",
    "channel":{"$ref":"#/channels/commands"},
    "messages":[{"$ref":"#/channels/commands/messages/Command"}],
    "bindings":{"http":{"method":"PUT"}},
    "reply":{"messages":[{"$ref":"#/channels/commands/messages/Result"}]}
  }}
}`)
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (fn roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) { return fn(request) }

func TestClientLoadsAndInventoriesWithoutOBI(t *testing.T) {
	client, err := Load(context.Background(), Source{Content: httpArtifact()}, LoadOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close() }()
	operations := client.Operations()
	if len(operations) != 1 || operations[0].ID != "submit" || operations[0].Action != "receive" {
		t.Fatalf("operations = %#v", operations)
	}
}

func TestClientPublishesWithArtifactMethodAndDecodesReply(t *testing.T) {
	var seen *http.Request
	httpClient := &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		seen = request
		return &http.Response{
			StatusCode: 200, Status: "200 OK", Header: http.Header{"Content-Type": {"application/json"}},
			Body: io.NopCloser(strings.NewReader(`{"accepted":true}`)), Request: request,
		}, nil
	})}
	client, err := Load(context.Background(), Source{Content: httpArtifact()}, LoadOptions{HTTPClient: httpClient})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close() }()
	events, err := client.Publish(context.Background(), "submit", map[string]any{"id": 7}, InvocationOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if seen == nil || seen.Method != http.MethodPut || seen.URL.String() != "https://api.example.test/commands" {
		t.Fatalf("request = %#v", seen)
	}
	var requestBody map[string]any
	if err := json.NewDecoder(seen.Body).Decode(&requestBody); err != nil || requestBody["id"] != float64(7) {
		t.Fatalf("request body = %#v, err = %v", requestBody, err)
	}
	if len(events) != 1 {
		t.Fatalf("events = %#v", events)
	}
	value, ok := events[0].Value.(map[string]any)
	if !ok || value["accepted"] != true {
		t.Fatalf("output = %#v", events[0].Value)
	}
}

func TestClientAppliesOperationAndMessageTraitsBeforeInvocation(t *testing.T) {
	doc := strings.Replace(string(httpArtifact()), `"defaultContentType":"application/json",`, "", 1)
	doc = strings.Replace(doc, `"Command":{"payload":{"type":"object"}}`, `"Command":{"payload":{"type":"object"},"traits":[{"$ref":"#/components/messageTraits/json"}]}`, 1)
	doc = strings.Replace(doc, `"Result":{"payload":{"type":"object"}}`, `"Result":{"payload":{"type":"object"},"traits":[{"$ref":"#/components/messageTraits/json"}]}`, 1)
	doc = strings.Replace(doc, `"operations":{"submit":{`, `"components":{"operationTraits":{"httpPost":{"summary":"Trait summary","bindings":{"http":{"method":"POST"}}},"httpPatch":{"bindings":{"http":{"method":"PATCH"}}}},"messageTraits":{"json":{"contentType":"application/json"}}},"operations":{"submit":{`, 1)
	doc = strings.Replace(doc, `"bindings":{"http":{"method":"PUT"}},`, `"summary":"Target summary","traits":[{"$ref":"#/components/operationTraits/httpPost"},{"$ref":"#/components/operationTraits/httpPatch"}],`, 1)

	var seen *http.Request
	httpClient := &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		seen = request
		return &http.Response{
			StatusCode: 200, Status: "200 OK", Header: http.Header{"Content-Type": {"application/json"}},
			Body: io.NopCloser(strings.NewReader(`{"accepted":true}`)), Request: request,
		}, nil
	})}
	client, err := Load(context.Background(), Source{Content: []byte(doc)}, LoadOptions{HTTPClient: httpClient})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close() }()
	if _, err := client.Publish(context.Background(), "submit", map[string]any{"id": 7}, InvocationOptions{}); err != nil {
		t.Fatal(err)
	}
	if seen == nil || seen.Method != http.MethodPatch || seen.Header.Get("Content-Type") != "application/json" {
		t.Fatalf("request = %#v", seen)
	}
	if operations := client.Operations(); len(operations) != 1 || operations[0].Summary != "Target summary" {
		t.Fatalf("operations = %#v", operations)
	}
}

func TestClientRefusesMessageHeadersInheritedFromTrait(t *testing.T) {
	doc := strings.Replace(string(httpArtifact()), `"Command":{"payload":{"type":"object"}}`, `"Command":{"payload":{"type":"object"},"traits":[{"$ref":"#/components/messageTraits/traced"}]}`, 1)
	doc = strings.Replace(doc, `"operations":{"submit":{`, `"components":{"messageTraits":{"traced":{"headers":{"type":"object"}}}},"operations":{"submit":{`, 1)
	requests := 0
	httpClient := &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		requests++
		return nil, errors.New("must not dispatch")
	})}
	client, err := Load(context.Background(), Source{Content: []byte(doc)}, LoadOptions{HTTPClient: httpClient})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close() }()
	_, err = client.Publish(context.Background(), "submit", map[string]any{"id": 7}, InvocationOptions{})
	if err == nil || !strings.Contains(err.Error(), "declares headers") {
		t.Fatalf("error = %v", err)
	}
	if requests != 0 {
		t.Fatalf("network requests = %d, want 0", requests)
	}
}

func TestClientResolvesExternalClosureAndRetainsRecursiveSchemas(t *testing.T) {
	root := []byte(`{
  "asyncapi":"3.0.0","info":{"title":"External","version":"1"},
  "servers":{"api":{"host":"api.example.test","protocol":"https"}},
  "channels":{"commands":{"$ref":"./channels.json#/channels/commands"}},
  "operations":{"submit":{"action":"receive","channel":{"$ref":"#/channels/commands"},"bindings":{"http":{"method":"POST"}}}},
  "components":{"schemas":{"Node":{"type":"object","properties":{"next":{"$ref":"#/components/schemas/Node"}}}}}
}`)
	httpClient := &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		if request.URL.String() != "https://artifact.example.test/channels.json" {
			return &http.Response{StatusCode: 404, Body: io.NopCloser(strings.NewReader("")), Header: http.Header{}, Request: request}, nil
		}
		body := `{"channels":{"commands":{"address":"/commands","messages":{"Command":{"contentType":"application/json","payload":{"$ref":"https://artifact.example.test/root.json#/components/schemas/Node"}}}}}}`
		return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(body)), Header: http.Header{}, Request: request}, nil
	})}
	client, err := Load(context.Background(), Source{Location: "https://artifact.example.test/root.json", Content: root}, LoadOptions{HTTPClient: httpClient})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close() }()
	operations := client.Operations()
	if len(operations) != 1 || operations[0].ID != "submit" {
		t.Fatalf("operations = %#v", operations)
	}
}

func TestClientRefusesOutOfProfileProtocolBindingVersion(t *testing.T) {
	doc := strings.Replace(string(httpArtifact()), `"method":"PUT"`, `"method":"PUT","bindingVersion":"0.4.0"`, 1)
	requests := 0
	httpClient := &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		requests++
		return nil, errors.New("must not dispatch")
	})}
	client, err := Load(context.Background(), Source{Content: []byte(doc)}, LoadOptions{HTTPClient: httpClient})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close() }()
	_, err = client.Publish(context.Background(), "submit", map[string]any{"id": 7}, InvocationOptions{})
	if err == nil || !strings.Contains(err.Error(), "binding version") {
		t.Fatalf("error = %v", err)
	}
	if requests != 0 {
		t.Fatalf("network requests = %d, want 0", requests)
	}
}

func TestEngineReportsArtifactPrerequisitesWithoutDispatch(t *testing.T) {
	doc := strings.Replace(string(httpArtifact()),
		`"servers":{"production":{"host":"api.example.test","protocol":"https"}}`,
		`"servers":{"production":{"host":"api.example.test","protocol":"https","security":[{"$ref":"#/components/securitySchemes/bearer"}]}},"components":{"securitySchemes":{"bearer":{"type":"http","scheme":"bearer"}}}`,
		1,
	)
	engine := NewEngine(nil)
	defer func() { _ = engine.Close() }()
	prepared, err := engine.Prepare(context.Background(), PrepareOptions{Source: Source{Content: []byte(doc)}, Ref: "#/operations/submit"})
	if err != nil {
		t.Fatal(err)
	}
	prerequisites := prepared.Prerequisites()
	if prerequisites == nil || prerequisites.Target != "https://api.example.test" || len(prerequisites.Alternatives) != 1 {
		t.Fatalf("prerequisites = %#v", prerequisites)
	}
	requirement := prerequisites.Alternatives[0].Requirements[0]
	if requirement.Type != "auth.bearer" || requirement.Name != "bearer" {
		t.Fatalf("requirement = %#v", requirement)
	}
}

func TestReplyBearingWebSocketRefusesBeforeDial(t *testing.T) {
	doc := strings.Replace(string(httpArtifact()), `"protocol":"https"`, `"protocol":"wss"`, 1)
	doc = strings.Replace(doc, `"action":"receive"`, `"action":"send"`, 1)
	doc = strings.Replace(doc, `"bindings":{"http":{"method":"PUT"}},`, "", 1)
	requests := 0
	httpClient := &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		requests++
		return nil, errors.New("must not dial")
	})}
	client, err := Load(context.Background(), Source{Content: []byte(doc)}, LoadOptions{HTTPClient: httpClient})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close() }()
	execution, err := client.Start(context.Background(), "submit", InvocationOptions{})
	if err != nil {
		t.Fatal(err)
	}
	for range execution.Events() {
	}
	var failure *ExecutionError
	if !errors.As(execution.Wait(), &failure) || failure.Code != ErrCodeSourceConfigError {
		t.Fatalf("failure = %#v", failure)
	}
	if requests != 0 {
		t.Fatalf("network requests = %d, want 0", requests)
	}
}

func TestArtifactRetrievalCancellationPropagates(t *testing.T) {
	httpClient := &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		<-request.Context().Done()
		return nil, request.Context().Err()
	})}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := Load(ctx, Source{Location: "https://example.test/asyncapi.yaml"}, LoadOptions{HTTPClient: httpClient})
	if !errors.Is(err, context.Canceled) && !strings.Contains(errorMessage(err), "canceled") {
		t.Fatalf("error = %v", err)
	}
}
