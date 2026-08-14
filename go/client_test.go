package asyncapiclient

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
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

func TestClientRetainsDraft07PlainNameIDsAndDanglingSchemaFragments(t *testing.T) {
	root := []byte(`{
  "asyncapi":"3.0.0","info":{"title":"External schema","version":"1"},
  "servers":{"api":{"host":"api.example.test","protocol":"https"}},
  "channels":{"commands":{"$ref":"./channel.json#/channels/commands"}},
  "operations":{"submit":{"action":"receive","channel":{"$ref":"#/channels/commands"},"bindings":{"http":{"method":"POST"}}}}
}`)
	httpClient := &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		if request.URL.String() != "https://artifact.example.test/channel.json" {
			return &http.Response{StatusCode: 404, Body: io.NopCloser(strings.NewReader("")), Header: http.Header{}, Request: request}, nil
		}
		body := `{"channels":{"commands":{"address":"/commands","messages":{"Command":{"contentType":"application/json","payload":{"$schema":"http://json-schema.org/draft-07/schema#","$id":"#Command","type":"object","properties":{"optional":{"$ref":"#/missing"}}}}}}}}`
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

func TestReplyBearingWebSocketReceiveSession(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer conn.CloseNow()
		_, payload, err := conn.Read(r.Context())
		if err != nil {
			return
		}
		var request map[string]any
		_ = json.Unmarshal(payload, &request)
		response, _ := json.Marshal(map[string]any{"accepted": request["id"]})
		_ = conn.Write(r.Context(), websocket.MessageText, response)
		_ = conn.Close(websocket.StatusNormalClosure, "done")
	}))
	defer server.Close()
	doc := websocketReplyArtifact(server.URL, "receive")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	client, err := Load(ctx, Source{Content: doc}, LoadOptions{Context: map[string]any{"configuration": map[string]any{"websocketMessageType": "text"}}})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close() }()
	events, err := client.Publish(ctx, "submit", map[string]any{"id": 17}, InvocationOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 || events[0].Value.(map[string]any)["accepted"] != float64(17) {
		t.Fatalf("events = %#v", events)
	}
	type result struct {
		id     int
		events []Event
		err    error
	}
	results := make(chan result, 2)
	for _, id := range []int{31, 47} {
		go func(id int) {
			got, invokeErr := client.Publish(ctx, "submit", map[string]any{"id": id}, InvocationOptions{})
			results <- result{id: id, events: got, err: invokeErr}
		}(id)
	}
	for range 2 {
		got := <-results
		if got.err != nil || len(got.events) != 1 || got.events[0].Value.(map[string]any)["accepted"] != float64(got.id) {
			t.Fatalf("concurrent result = %#v", got)
		}
	}
}

func TestReplyBearingWebSocketSendKeepsDirectionsDistinct(t *testing.T) {
	replies := make(chan map[string]any, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer conn.CloseNow()
		_ = conn.Write(r.Context(), websocket.MessageText, []byte(`{"command":23}`))
		_, payload, err := conn.Read(r.Context())
		if err == nil {
			var reply map[string]any
			_ = json.Unmarshal(payload, &reply)
			replies <- reply
		}
		_ = conn.Close(websocket.StatusNormalClosure, "done")
	}))
	defer server.Close()
	doc := websocketReplyArtifact(server.URL, "send")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	client, err := Load(ctx, Source{Content: doc}, LoadOptions{Context: map[string]any{"configuration": map[string]any{"websocketMessageType": "text"}}})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close() }()
	execution, err := client.Start(ctx, "submit", InvocationOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := execution.Send(ctx, map[string]any{"accepted": 23}); err != nil {
		t.Fatal(err)
	}
	_ = execution.FinishInput()
	var events []Event
	for event := range execution.Events() {
		events = append(events, event)
	}
	if err := execution.Wait(); err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 || events[0].Value.(map[string]any)["command"] != float64(23) {
		t.Fatalf("events = %#v", events)
	}
	select {
	case reply := <-replies:
		if reply["accepted"] != float64(23) {
			t.Fatalf("reply = %#v", reply)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
}

func TestReplyBearingWebSocketCoordinatesDistinctStaticEndpoint(t *testing.T) {
	replyReady := make(chan *websocket.Conn, 1)
	replyDone := make(chan struct{})
	mux := http.NewServeMux()
	mux.HandleFunc("/replies", func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer conn.CloseNow()
		replyReady <- conn
		<-replyDone
	})
	mux.HandleFunc("/commands", func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer conn.CloseNow()
		_, payload, err := conn.Read(r.Context())
		if err != nil {
			return
		}
		var request map[string]any
		_ = json.Unmarshal(payload, &request)
		reply := <-replyReady
		response, _ := json.Marshal(map[string]any{"accepted": request["id"]})
		_ = reply.Write(context.Background(), websocket.MessageText, response)
		_ = reply.Close(websocket.StatusNormalClosure, "done")
		_ = conn.Close(websocket.StatusNormalClosure, "done")
		close(replyDone)
	})
	server := httptest.NewServer(mux)
	defer server.Close()
	var document map[string]any
	if err := json.Unmarshal(websocketReplyArtifact(server.URL, "receive"), &document); err != nil {
		t.Fatal(err)
	}
	channels := document["channels"].(map[string]any)
	commands := channels["commands"].(map[string]any)
	channels["replies"] = map[string]any{
		"address":  "/replies",
		"messages": map[string]any{"Result": commands["messages"].(map[string]any)["Result"]},
	}
	operation := document["operations"].(map[string]any)["submit"].(map[string]any)
	operation["reply"] = map[string]any{
		"channel":  map[string]any{"$ref": "#/channels/replies"},
		"messages": []any{map[string]any{"$ref": "#/channels/replies/messages/Result"}},
	}
	content, _ := json.Marshal(document)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	client, err := Load(ctx, Source{Content: content}, LoadOptions{Context: map[string]any{"configuration": map[string]any{"websocketMessageType": "text"}}})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close() }()
	events, err := client.Publish(ctx, "submit", map[string]any{"id": 71}, InvocationOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 || events[0].Value.(map[string]any)["accepted"] != float64(71) {
		t.Fatalf("events = %#v", events)
	}
}

func TestReplyBearingWebSocketRefusesHeaderAddressBeforeDial(t *testing.T) {
	var document map[string]any
	if err := json.Unmarshal(websocketReplyArtifact("http://api.example.test", "receive"), &document); err != nil {
		t.Fatal(err)
	}
	operation := document["operations"].(map[string]any)["submit"].(map[string]any)
	operation["reply"].(map[string]any)["address"] = map[string]any{"location": "$message.header#/replyTo"}
	content, _ := json.Marshal(document)
	requests := 0
	httpClient := &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		requests++
		return nil, errors.New("must not dial")
	})}
	client, err := Load(context.Background(), Source{Content: content}, LoadOptions{
		HTTPClient: httpClient,
		Context:    map[string]any{"configuration": map[string]any{"websocketMessageType": "text"}},
	})
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
	// A pre-dial refusal carries the never-dispatched guarantee (ERR_REFUSED,
	// ruled 2026-08-14) — and the zero-request assertion below is that
	// guarantee, wire-proven.
	if !errors.As(execution.Wait(), &failure) || failure.Code != ErrCodeRefused {
		t.Fatalf("failure = %#v", failure)
	}
	if requests != 0 {
		t.Fatalf("network requests = %d, want 0", requests)
	}
}

func websocketReplyArtifact(serverURL, action string) []byte {
	doc := strings.Replace(string(httpArtifact()), `"host":"api.example.test","protocol":"https"`, `"host":"`+strings.TrimPrefix(serverURL, "http://")+`","protocol":"ws"`, 1)
	doc = strings.Replace(doc, `"action":"receive"`, `"action":"`+action+`"`, 1)
	doc = strings.Replace(doc, `"bindings":{"http":{"method":"PUT"}},`, "", 1)
	doc = strings.Replace(doc, `"reply":{"messages":`, `"reply":{"channel":{"$ref":"#/channels/commands"},"messages":`, 1)
	return []byte(doc)
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

type testProtocolDriver struct{ seen []any }

func (d *testProtocolDriver) Protocols() []string { return []string{"mqtt"} }

func (d *testProtocolDriver) Execute(ctx context.Context, request DriverRequest, session DriverSession) error {
	if request.Protocol != "mqtt" || request.OperationKey != "submit" || len(request.Artifact) == 0 {
		return errors.New("driver request did not preserve the artifact target")
	}
	if request.Input == nil || request.Input.Address != "/commands" || request.Server["protocol"] != "mqtt" || len(request.Input.Messages) != 1 || request.Input.Encode == nil {
		return errors.New("driver request did not carry resolved AsyncAPI semantics")
	}
	bindings, _ := request.Operation["bindings"].(map[string]any)
	future, _ := bindings["future"].(map[string]any)
	config, _ := future["config"].(map[string]any)
	if mqtt, ok := bindings["mqtt"].(map[string]any); !ok || mqtt["qos"] != float64(1) || future["marker"] != "preserved" || config["type"] != "object" {
		return errors.New("driver request did not preserve open protocol-binding entries")
	}
	if len(request.SecurityAlternatives) != 1 || len(request.SecurityAlternatives[0]) != 1 || request.SecurityAlternatives[0][0].Name != "mqttBasic" || request.SecurityAlternatives[0][0].Scheme["type"] != "userPassword" {
		return errors.New("driver request did not carry resolved security alternatives")
	}
	encoded, err := request.Input.Encode(map[string]any{"id": 9})
	if err != nil || string(encoded) != `{"id":9}` {
		return errors.New("driver request did not carry the artifact codec")
	}
	value, err := session.Receive(ctx)
	if err != nil {
		return err
	}
	d.seen = append(d.seen, value)
	if err := session.CloseInput(); err != nil {
		return err
	}
	return session.Emit(map[string]any{"accepted": true})
}

func mqttArtifact() []byte {
	doc := strings.Replace(string(httpArtifact()), `"protocol":"https"`, `"protocol":"mqtt"`, 1)
	doc = strings.Replace(doc, `"protocol":"mqtt"`, `"protocol":"mqtt","security":[{"$ref":"#/components/securitySchemes/mqttBasic"}]`, 1)
	doc = strings.Replace(doc, `"operations":`, `"components":{"securitySchemes":{"mqttBasic":{"type":"userPassword"}},"schemas":{"DriverConfig":{"type":"object"}}},"operations":`, 1)
	doc = strings.Replace(doc, `"bindings":{"http":{"method":"PUT"}}`, `"bindings":{"mqtt":{"qos":1},"future":{"marker":"preserved","config":{"$ref":"#/components/schemas/DriverConfig"}}}`, 1)
	return []byte(doc)
}

func TestClientDelegatesArbitraryProtocolToInstalledDriver(t *testing.T) {
	driver := &testProtocolDriver{}
	client, err := Load(context.Background(), Source{Content: mqttArtifact()}, LoadOptions{
		Drivers: []ProtocolDriver{driver},
		Context: map[string]any{"basic": map[string]any{"username": "sensor", "password": "secret"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close() }()
	events, err := client.Publish(context.Background(), "submit", map[string]any{"id": 9}, InvocationOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 || len(driver.seen) != 1 {
		t.Fatalf("events = %#v, seen = %#v", events, driver.seen)
	}
}

func TestClientNormalizesAsyncAPIV2PerspectiveAndPreservesNativeRef(t *testing.T) {
	artifact := []byte(`{
  "asyncapi":"2.6.0",
  "info":{"title":"Legacy artifact","version":"1"},
  "defaultContentType":"application/json",
  "servers":{"production":{"url":"https://api.example.test/events","protocol":"https"}},
  "channels":{"commands/{tenant}":{
    "parameters":{"tenant":{"schema":{"type":"string"}}},
    "publish":{"message":{"messageId":"Command","payload":{"type":"object"}},"bindings":{"http":{"method":"POST"}}}
  }}
}`)
	requests := 0
	httpClient := &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		requests++
		return &http.Response{StatusCode: 204, Status: "204 No Content", Header: make(http.Header), Body: io.NopCloser(strings.NewReader("")), Request: request}, nil
	})}
	client, err := Load(context.Background(), Source{Content: artifact}, LoadOptions{
		HTTPClient: httpClient,
		Context: map[string]any{"configuration": map[string]any{
			"address": map[string]any{"parameters": map[string]any{"tenant": "acme"}},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close() }()
	operations := client.Operations()
	if len(operations) != 1 || operations[0].Ref != "#/channels/commands~1{tenant}/publish" || operations[0].Action != "receive" {
		t.Fatalf("operations = %#v", operations)
	}
	events, err := client.Publish(context.Background(), operations[0].Ref, map[string]any{"id": 1}, InvocationOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 0 || requests != 1 {
		t.Fatalf("events = %#v, requests = %d", events, requests)
	}
}

func TestClientAcceptsAsyncAPI31(t *testing.T) {
	artifact := bytes.Replace(httpArtifact(), []byte(`"asyncapi":"3.0.0"`), []byte(`"asyncapi":"3.1.0"`), 1)
	client, err := Load(context.Background(), Source{Content: artifact}, LoadOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close() }()
	if len(client.Operations()) != 1 {
		t.Fatalf("operations = %#v", client.Operations())
	}
}

func TestClientReportsUninstalledProtocolDriverAsCapabilityFailure(t *testing.T) {
	client, err := Load(context.Background(), Source{Content: mqttArtifact()}, LoadOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close() }()
	_, err = client.Publish(context.Background(), "submit", map[string]any{"id": 9}, InvocationOptions{})
	var failure *ExecutionError
	if !errors.As(err, &failure) || failure.Code != ErrCodeDriverUnavailable {
		t.Fatalf("failure = %#v (err %v)", failure, err)
	}
}

func byteArtifact() []byte {
	return []byte(`{
  "asyncapi":"3.0.0",
  "info":{"title":"Byte boundary","version":"1.0.0"},
  "servers":{"production":{"host":"api.example.test","protocol":"https"}},
  "channels":{"blobs":{"address":"/blobs","messages":{
    "Blob":{"contentType":"application/octet-stream","payload":{"type":"string","contentEncoding":"base64"}},
    "Stored":{"contentType":"application/octet-stream"}
  }}},
  "operations":{"store":{
    "action":"receive",
    "channel":{"$ref":"#/channels/blobs"},
    "messages":[{"$ref":"#/channels/blobs/messages/Blob"}],
    "bindings":{"http":{"method":"PUT"}},
    "reply":{"messages":[{"$ref":"#/channels/blobs/messages/Stored"}]}
  }}
}`)
}

// The artifact-authorized byte rule (§9.2, ruled 2026-08-13): declared
// binary media carries exact octets, the canonical RFC 4648 §4 Base64
// string being the boundary value in both directions.
func TestClientCarriesDeclaredBinaryMediaThroughTheByteBoundary(t *testing.T) {
	wire := []byte{0x00, 0x01, 0xFE, 0xFF}
	var seen []byte
	httpClient := &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		seen, _ = io.ReadAll(request.Body)
		return &http.Response{
			StatusCode: 200, Status: "200 OK", Header: http.Header{"Content-Type": {"application/octet-stream"}},
			Body: io.NopCloser(strings.NewReader("stored")), Request: request,
		}, nil
	})}
	client, err := Load(context.Background(), Source{Content: byteArtifact()}, LoadOptions{HTTPClient: httpClient})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close() }()

	events, err := client.Publish(context.Background(), "store", base64.StdEncoding.EncodeToString(wire), InvocationOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(seen, wire) {
		t.Fatalf("wire bytes = %#v, want %#v", seen, wire)
	}
	if len(events) != 1 || events[0].Value != base64.StdEncoding.EncodeToString([]byte("stored")) {
		t.Fatalf("output = %#v, want the reply octets as canonical Base64", events)
	}

	// Non-canonical Base64 refuses loudly before dispatch.
	if _, err := client.Publish(context.Background(), "store", "AAE_", InvocationOptions{}); err == nil {
		t.Fatal("non-canonical Base64 must refuse")
	}
	if _, err := client.Publish(context.Background(), "store", 7, InvocationOptions{}); err == nil {
		t.Fatal("a non-string byte-boundary value must refuse")
	}
}

// The codec seam enrichment: consumer Encode/Decode hooks keyed on the
// declared media turn logical values into wire octets and back, overriding
// the Base64 floor.
func TestClientCodecHooksEnrichTheByteBoundary(t *testing.T) {
	var seen []byte
	httpClient := &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		seen, _ = io.ReadAll(request.Body)
		return &http.Response{
			StatusCode: 200, Status: "200 OK", Header: http.Header{"Content-Type": {"application/octet-stream"}},
			Body: io.NopCloser(strings.NewReader("\x07wire")), Request: request,
		}, nil
	})}
	client, err := Load(context.Background(), Source{Content: byteArtifact()}, LoadOptions{HTTPClient: httpClient})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close() }()

	hooks := &Hooks{
		Encode: func(_ HookSite, value any) ([]byte, bool, error) {
			text, ok := value.(string)
			if !ok {
				return nil, false, nil
			}
			return append([]byte{0x07}, []byte(text)...), true, nil
		},
		Decode: func(_ HookSite, raw RawResult) (any, bool, error) {
			if len(raw.Body) > 0 && raw.Body[0] == 0x07 {
				return string(raw.Body[1:]), true, nil
			}
			return nil, false, nil
		},
	}
	events, err := client.Publish(context.Background(), "store", "payload", InvocationOptions{Hooks: hooks})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(seen, append([]byte{0x07}, []byte("payload")...)) {
		t.Fatalf("wire bytes = %#v", seen)
	}
	if len(events) != 1 || events[0].Value != "wire" {
		t.Fatalf("output = %#v, want the hook-decoded logical value", events)
	}
}
