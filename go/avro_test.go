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

func TestIsAvroSchemaFormat(t *testing.T) {
	cases := []struct {
		format string
		want   bool
	}{
		{"application/vnd.apache.avro;version=1.9.0", true},
		{"application/vnd.apache.avro+json;version=1.11.1", true},
		{"application/vnd.apache.avro+yaml", true},
		{"Application/VND.Apache.Avro;Version=1.9.0", true},
		{"application/vnd.apache.avro;version=2.0.0", false},
		{"application/schema+json;version=draft-07", false},
		{"avro", false},
		{"", false},
	}
	for _, c := range cases {
		if got := isAvroSchemaFormat(c.format); got != c.want {
			t.Errorf("isAvroSchemaFormat(%q) = %v, want %v", c.format, got, c.want)
		}
	}
}

func avroArtifact(contentType string) []byte {
	return []byte(`{
  "asyncapi":"3.0.0",
  "info":{"title":"Avro correspondence","version":"1.0.0"},
  "servers":{"production":{"host":"api.example.test","protocol":"https"}},
  "channels":{"records":{"address":"/records","messages":{
    "Record":{"contentType":"` + contentType + `","payload":{
      "schemaFormat":"application/vnd.apache.avro;version=1.9.0",
      "schema":{"type":"record","name":"Record","fields":[{"name":"id","type":"long"}]}
    }}
  }}},
  "operations":{"store":{
    "action":"receive",
    "channel":{"$ref":"#/channels/records"},
    "messages":[{"$ref":"#/channels/records/messages/Record"}],
    "bindings":{"http":{"method":"POST"}}
  }}
}`)
}

// An Avro-declared payload with binary media is a codec capability this
// build has not qualified: the invocation refuses before dispatch
// (ERR_REFUSED) instead of falling back to the byte boundary, whose base64
// strings the synthesized logical schema does not admit.
func TestClientRefusesAvroBinaryMediaAsUnqualifiedCodec(t *testing.T) {
	requests := 0
	httpClient := &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		requests++
		return nil, errors.New("must not dial")
	})}
	client, err := Load(context.Background(), Source{Content: avroArtifact("avro/binary")}, LoadOptions{HTTPClient: httpClient})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close() }()
	_, err = client.Publish(context.Background(), "store", map[string]any{"id": 7}, InvocationOptions{})
	var failure *ExecutionError
	if !errors.As(err, &failure) || failure.Code != ErrCodeRefused {
		t.Fatalf("failure = %#v (err %v)", failure, err)
	}
	if requests != 0 {
		t.Fatalf("network requests = %d, want 0", requests)
	}
}

// An Avro-declared payload with JSON-family media needs no extra codec: the
// wire is the Avro-JSON encoding, which the ordinary JSON lane carries — the
// logical value crosses the boundary end to end.
func TestClientCarriesAvroJSONMediaThroughTheJSONLane(t *testing.T) {
	var seen []byte
	httpClient := &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		seen, _ = io.ReadAll(request.Body)
		return &http.Response{
			StatusCode: 204, Status: "204 No Content", Header: http.Header{},
			Body: io.NopCloser(strings.NewReader("")), Request: request,
		}, nil
	})}
	client, err := Load(context.Background(), Source{Content: avroArtifact("application/json")}, LoadOptions{HTTPClient: httpClient})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close() }()
	if _, err := client.Publish(context.Background(), "store", map[string]any{"id": 7}, InvocationOptions{}); err != nil {
		t.Fatal(err)
	}
	var payload map[string]any
	if err := json.Unmarshal(seen, &payload); err != nil {
		t.Fatalf("wire is not JSON: %v (%q)", err, seen)
	}
	if payload["id"] != float64(7) {
		t.Fatalf("wire payload = %#v", payload)
	}
}
