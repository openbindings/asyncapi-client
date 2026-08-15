package asyncapiclient

import (
	"encoding/json"
	"strings"
	"testing"
)

// MC5 seal-1 finding F-V3-1: an AsyncAPI 2.x document carrying Reference
// Objects at positions its own declared edition does not admit them has no
// interpretation under that edition and refuses whole-artifact — the
// adjudicated consistent-loud-refusal convergence for the parser-tolerance
// class. Position admission is pinned from the edition texts (TypeScript
// twin: validateReferenceAdmission).
func TestNormalizeDocumentRefusesV2ReferenceObjectAtNonAdmittingPositions(t *testing.T) {
	cases := []struct {
		name     string
		document string
	}{
		{"operation position", `{"asyncapi":"2.4.0","info":{"title":"Bad","version":"1"},
			"channels":{"events":{"publish":{"$ref":"#/components/operations/publishEvents"}}}}`},
		{"whole servers map", `{"asyncapi":"2.4.0","info":{"title":"Bad","version":"1"},
			"servers":{"$ref":"#/components/servers"},"channels":{}}`},
		{"whole channels map", `{"asyncapi":"2.4.0","info":{"title":"Bad","version":"1"},
			"channels":{"$ref":"#/components/channels"}}`},
		{"string-typed channel description", `{"asyncapi":"2.4.0","info":{"title":"Bad","version":"1"},
			"channels":{"events":{"description":{"$ref":"#/components/x-descriptions/events"},
			"subscribe":{"message":{"payload":{"type":"object"}}}}}}`},
		{"servers value before 2.4.0", `{"asyncapi":"2.2.0","info":{"title":"Bad","version":"1"},
			"servers":{"main":{"$ref":"#/components/x-servers/main"}},"channels":{}}`},
		{"channel servers list member", `{"asyncapi":"2.4.0","info":{"title":"Bad","version":"1"},
			"servers":{"main":{"url":"wss://example.test","protocol":"wss"}},
			"channels":{"events":{"servers":[{"$ref":"#/servers/main"}],
			"subscribe":{"message":{"payload":{"type":"object"}}}}}}`},
		{"string-typed message contentType", `{"asyncapi":"2.6.0","info":{"title":"Bad","version":"1"},
			"channels":{"events":{"subscribe":{"message":{"contentType":{"$ref":"#/components/x-media/json"},
			"payload":{"type":"object"}}}}}}`},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			_, err := NormalizeDocument([]byte(testCase.document))
			if err == nil || !strings.Contains(err.Error(), "does not admit a Reference Object") {
				t.Fatalf("err = %v, want the position-admission refusal", err)
			}
		})
	}
}

func TestNormalizeDocumentAdmitsV2ReferenceObjectAtAdmittingPositions(t *testing.T) {
	// A servers-map VALUE ref is legal from 2.4.0 (Server Object | Reference
	// Object), the Channel Item's own $ref field is legal in every 2.x
	// edition, and operation.message may be a Reference Object.
	document := `{"asyncapi":"2.4.0","info":{"title":"Fine","version":"1"},
		"servers":{"main":{"$ref":"#/components/x-servers/main"}},
		"channels":{"events":{"subscribe":{"message":{"$ref":"#/components/messages/event"}}}},
		"components":{"messages":{"event":{"payload":{"type":"object"}}}}}`
	if _, err := NormalizeDocument([]byte(document)); err != nil {
		t.Fatalf("NormalizeDocument: %v", err)
	}
}

// MC5 seal-1 finding F-V3-2's normalization leg: a non-object Avro schema at
// a message-level payload (a top-level union array, a bare primitive name)
// takes the Multi Format Schema Object wrapper shape, which the typed
// document model can carry (hoistNonObjectAvroPayloads).
func TestNormalizeDocumentHoistsNonObjectAvroPayloadIntoWrapper(t *testing.T) {
	document := `{"asyncapi":"2.6.0","info":{"title":"Avro union","version":"1"},
		"channels":{"files":{"publish":{"message":{"name":"file",
		"schemaFormat":"application/vnd.apache.avro;version=1.9.0",
		"payload":["null",{"type":"record","name":"File","fields":[{"name":"path","type":"string"}]}]}}}}}`
	normalized, err := NormalizeDocument([]byte(document))
	if err != nil {
		t.Fatalf("NormalizeDocument: %v", err)
	}
	var envelope map[string]any
	if err := json.Unmarshal(normalized, &envelope); err != nil {
		t.Fatalf("decode normalized document: %v", err)
	}
	channels := envelope["channels"].(map[string]any)
	messages := channels["files"].(map[string]any)["messages"].(map[string]any)
	payload, ok := messages["file"].(map[string]any)["payload"].(map[string]any)
	if !ok {
		t.Fatalf("payload = %#v, want the hoisted wrapper object", messages["file"].(map[string]any)["payload"])
	}
	if payload["schemaFormat"] != "application/vnd.apache.avro;version=1.9.0" {
		t.Fatalf("wrapper schemaFormat = %v", payload["schemaFormat"])
	}
	union, ok := payload["schema"].([]any)
	if !ok || len(union) != 2 {
		t.Fatalf("wrapper schema = %#v, want the two-branch union array", payload["schema"])
	}
	// The typed parse the wrapper shape exists for.
	if _, err := parseDocument(normalized); err != nil {
		t.Fatalf("parseDocument after hoist: %v", err)
	}
}
