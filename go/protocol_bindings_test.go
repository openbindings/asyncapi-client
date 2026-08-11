package asyncapiclient

import (
	"encoding/json"
	"testing"
)

func TestProtocolBindingsPreserveUnknownProtocolsAndKnownBindingFields(t *testing.T) {
	var operation asyncOperation
	if err := json.Unmarshal([]byte(`{
		"action":"receive",
		"channel":{"$ref":"#/channels/events"},
		"bindings":{
			"http":{"method":"PUT","futureField":{"enabled":true}},
			"future":{"marker":"preserved"}
		}
	}`), &operation); err != nil {
		t.Fatal(err)
	}
	if operation.Bindings == nil || operation.Bindings.HTTP == nil || operation.Bindings.HTTP.Method != "PUT" {
		t.Fatalf("typed HTTP binding = %#v", operation.Bindings)
	}
	encoded, err := json.Marshal(operation)
	if err != nil {
		t.Fatal(err)
	}
	var roundTrip map[string]any
	if err := json.Unmarshal(encoded, &roundTrip); err != nil {
		t.Fatal(err)
	}
	bindings := roundTrip["bindings"].(map[string]any)
	httpBinding := bindings["http"].(map[string]any)
	futureBinding := bindings["future"].(map[string]any)
	if httpBinding["futureField"].(map[string]any)["enabled"] != true || futureBinding["marker"] != "preserved" {
		t.Fatalf("preserved bindings = %#v", bindings)
	}
}

func TestProtocolBindingsStillRejectMalformedKnownBindings(t *testing.T) {
	var operation asyncOperation
	if err := json.Unmarshal([]byte(`{
		"action":"receive",
		"channel":{"$ref":"#/channels/events"},
		"bindings":{"http":"not-an-object"}
	}`), &operation); err == nil {
		t.Fatal("malformed known HTTP binding was accepted")
	}
}
