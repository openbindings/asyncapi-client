package mqtt

import (
	"strings"
	"testing"

	asyncapiclient "github.com/openbindings/asyncapi-client/go"
)

func TestResolveProfileAppliesAuthoredMQTT311Bindings(t *testing.T) {
	request := profileRequest()
	profile, err := resolveProfile(request, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if profile.QoS != 2 || !profile.Retain || !profile.Clean || profile.KeepAlive != 30 || profile.ClientID != "artifact-client" {
		t.Fatalf("profile = %#v", profile)
	}
}

func TestResolveProfileRefusesAmbiguousOrMQTT5Cells(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*asyncapiclient.DriverRequest)
		want   string
	}{
		{
			name:   "missing protocol version",
			mutate: func(request *asyncapiclient.DriverRequest) { delete(request.Server, "protocolVersion") },
			want:   "protocolVersion",
		},
		{
			name: "MQTT 5 message field",
			mutate: func(request *asyncapiclient.DriverRequest) {
				request.Input.Messages[0]["bindings"] = map[string]any{"mqtt": map[string]any{"bindingVersion": "0.2.0", "responseTopic": "responses"}}
			},
			want: "MQTT 5 message-binding fields",
		},
		{
			name: "unknown binding version",
			mutate: func(request *asyncapiclient.DriverRequest) {
				request.Operation["bindings"] = map[string]any{"mqtt": map[string]any{"bindingVersion": "9.0.0"}}
			},
			want: "outside the 0.1.0/0.2.0",
		},
		{
			name: "undeclared operation binding field",
			mutate: func(request *asyncapiclient.DriverRequest) {
				request.Operation["bindings"].(map[string]any)["mqtt"].(map[string]any)["future"] = true
			},
			want: "undeclared fields",
		},
		{
			name:   "publish topic wildcard",
			mutate: func(request *asyncapiclient.DriverRequest) { request.Input.Address = "events/#" },
			want:   "publish topic names cannot contain wildcard",
		},
		{
			name: "persistent session is not yet qualified",
			mutate: func(request *asyncapiclient.DriverRequest) {
				request.Server["bindings"].(map[string]any)["mqtt"].(map[string]any)["cleanSession"] = false
			},
			want: "persistent MQTT sessions",
		},
		{
			name: "last will is not yet qualified",
			mutate: func(request *asyncapiclient.DriverRequest) {
				request.Server["bindings"].(map[string]any)["mqtt"].(map[string]any)["lastWill"] = map[string]any{
					"topic": "offline", "message": "gone", "qos": float64(1), "retain": true,
				}
			},
			want: "Last Will",
		},
		{
			name: "normalized v2 security conjunction",
			mutate: func(request *asyncapiclient.DriverRequest) {
				request.Server["x-ob-asyncapi-v2-security-conjunction"] = map[string]any{"basic": []any{}, "certificate": []any{}}
			},
			want: "multi-scheme security conjunctions",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request := profileRequest()
			test.mutate(&request)
			_, err := resolveProfile(request, Options{})
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error = %v, want %q", err, test.want)
			}
		})
	}
}

func TestClientOptionsSelectsDeclaredSecurityWithoutVolunteeringContext(t *testing.T) {
	request := profileRequest()
	request.ServerURL = "mqtt://broker.example.test"
	request.Protocol = "mqtt"
	request.Context = map[string]any{"basic": map[string]any{"username": "sensor", "password": "secret"}}
	profile, err := resolveProfile(request, Options{})
	if err != nil {
		t.Fatal(err)
	}
	options, err := clientOptions(request, profile, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if options.Username != "" || options.Password != "" {
		t.Fatalf("undeclared credentials were volunteered: %q/%q", options.Username, options.Password)
	}

	request.SecurityAlternatives = [][]asyncapiclient.DriverSecurityScheme{
		{{Scheme: map[string]any{"type": "oauth2"}}},
		{{Name: "mqttBasic", Scheme: map[string]any{"type": "userPassword"}}},
	}
	options, err = clientOptions(request, profile, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if options.Username != "sensor" || options.Password != "secret" {
		t.Fatalf("selected credentials = %q/%q", options.Username, options.Password)
	}
}

func TestValidateTopicNamesAndFilters(t *testing.T) {
	for _, test := range []struct {
		topic  string
		action string
		ok     bool
	}{
		{"events/acme", "receive", true},
		{"events/+/#", "send", true},
		{"events/#/tail", "send", false},
		{"events/a+", "send", false},
		{"events/+", "receive", false},
		{"", "send", false},
	} {
		if err := validateTopic(test.topic, test.action); (err == nil) != test.ok {
			t.Errorf("validateTopic(%q, %q) = %v, want ok=%t", test.topic, test.action, err, test.ok)
		}
	}
}

func profileRequest() asyncapiclient.DriverRequest {
	return asyncapiclient.DriverRequest{
		Action: "receive",
		Server: map[string]any{
			"protocolVersion": "3.1.1",
			"bindings": map[string]any{"mqtt": map[string]any{
				"bindingVersion": "0.2.0", "clientId": "artifact-client", "cleanSession": true, "keepAlive": float64(30),
			}},
		},
		Operation: map[string]any{"bindings": map[string]any{"mqtt": map[string]any{
			"bindingVersion": "0.2.0", "qos": float64(2), "retain": true,
		}}},
		Input: &asyncapiclient.DriverInput{DriverDirection: asyncapiclient.DriverDirection{
			Address: "events/acme", Channel: map[string]any{}, Messages: []map[string]any{{}},
		}},
	}
}
