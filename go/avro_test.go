package asyncapiclient

import (
	"bytes"
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

func avroArtifact(contentType string, withReply bool) []byte {
	reply := ""
	if withReply {
		reply = `,
    "reply":{"messages":[{"$ref":"#/channels/records/messages/Record"}]}`
	}
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
    "bindings":{"http":{"method":"POST"}}` + reply + `
  }}
}`)
}

// The Avro binary encoding of Record{id: long} with id=7: one field, a
// long, zigzag(7) = 14 = 0x0E. Hand-derived from the Avro specification's
// binary encoding — the wire pin is against the spec, not the library.
var avroWireID7 = []byte{0x0E}

// The named Avro correspondence's binary wire, encode side: the caller's
// logical value crosses the boundary and the wire carries exactly the Avro
// binary encoding of the datum under the artifact's schema (bare framing,
// the default).
func TestClientEncodesAvroBinaryWire(t *testing.T) {
	var seen []byte
	httpClient := &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		seen, _ = io.ReadAll(request.Body)
		return &http.Response{
			StatusCode: 204, Status: "204 No Content", Header: http.Header{},
			Body: io.NopCloser(strings.NewReader("")), Request: request,
		}, nil
	})}
	client, err := Load(context.Background(), Source{Content: avroArtifact("avro/binary", false)}, LoadOptions{HTTPClient: httpClient})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close() }()
	if _, err := client.Publish(context.Background(), "store", map[string]any{"id": 7}, InvocationOptions{}); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(seen, avroWireID7) {
		t.Fatalf("wire = %#v, want %#v", seen, avroWireID7)
	}
}

// Decode side: an avro/binary reply's octets come back as the logical
// value — the full round trip a bespoke client would perform.
func TestClientDecodesAvroBinaryReply(t *testing.T) {
	httpClient := &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: 200, Status: "200 OK", Header: http.Header{"Content-Type": {"avro/binary"}},
			Body: io.NopCloser(bytes.NewReader(avroWireID7)), Request: request,
		}, nil
	})}
	client, err := Load(context.Background(), Source{Content: avroArtifact("avro/binary", true)}, LoadOptions{HTTPClient: httpClient})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close() }()
	events, err := client.Publish(context.Background(), "store", map[string]any{"id": 7}, InvocationOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 {
		t.Fatalf("events = %#v", events)
	}
	value, _ := events[0].Value.(map[string]any)
	if value["id"] != float64(7) {
		t.Fatalf("decoded value = %#v", events[0].Value)
	}
}

// The confluent framing configuration point: magic byte 0x00 plus the
// big-endian 4-byte configuration.schemaId prefixes the binary encoding on
// the wire, and decode verifies and strips the same prefix.
func TestClientAvroConfluentFraming(t *testing.T) {
	framed := append([]byte{0x00, 0x00, 0x00, 0x00, 0x2A}, avroWireID7...)
	var seen []byte
	httpClient := &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		seen, _ = io.ReadAll(request.Body)
		return &http.Response{
			StatusCode: 200, Status: "200 OK", Header: http.Header{"Content-Type": {"avro/binary"}},
			Body: io.NopCloser(bytes.NewReader(framed)), Request: request,
		}, nil
	})}
	client, err := Load(context.Background(), Source{Content: avroArtifact("avro/binary", true)}, LoadOptions{
		HTTPClient: httpClient,
		Context:    map[string]any{"configuration": map[string]any{"framing": "confluent", "schemaId": 42}},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close() }()
	events, err := client.Publish(context.Background(), "store", map[string]any{"id": 7}, InvocationOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(seen, framed) {
		t.Fatalf("wire = %#v, want %#v", seen, framed)
	}
	if len(events) != 1 {
		t.Fatalf("events = %#v", events)
	}
	value, _ := events[0].Value.(map[string]any)
	if value["id"] != float64(7) {
		t.Fatalf("decoded value = %#v", events[0].Value)
	}
}

// The codec-capability refusal survives for the unqualifiable case: an
// on-list declaration whose schema is not a valid Avro schema cannot build
// a codec, so the invocation refuses before dispatch (ERR_REFUSED) — never
// the byte boundary, never a dial.
func TestClientRefusesUnqualifiableAvroDeclaration(t *testing.T) {
	artifact := bytes.Replace(avroArtifact("avro/binary", false),
		[]byte(`"schema":{"type":"record","name":"Record","fields":[{"name":"id","type":"long"}]}`),
		[]byte(`"schema":{"type":"record","name":"Record"}`), 1)
	requests := 0
	httpClient := &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		requests++
		return nil, errors.New("must not dial")
	})}
	client, err := Load(context.Background(), Source{Content: artifact}, LoadOptions{HTTPClient: httpClient})
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

// An Avro-declared payload with JSON-family media needs no binary codec:
// the wire is the Avro-JSON encoding, which the ordinary JSON lane carries
// — the logical value crosses the boundary end to end.
func TestClientCarriesAvroJSONMediaThroughTheJSONLane(t *testing.T) {
	var seen []byte
	httpClient := &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		seen, _ = io.ReadAll(request.Body)
		return &http.Response{
			StatusCode: 204, Status: "204 No Content", Header: http.Header{},
			Body: io.NopCloser(strings.NewReader("")), Request: request,
		}, nil
	})}
	client, err := Load(context.Background(), Source{Content: avroArtifact("application/json", false)}, LoadOptions{HTTPClient: httpClient})
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
