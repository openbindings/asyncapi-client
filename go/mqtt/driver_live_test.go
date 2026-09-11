package mqtt

import (
	"context"
	"encoding/json"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	asyncapiclient "github.com/openbindings/asyncapi-client/go"
)

func TestLivePublishSubscribeUsesOneAuthoredIdentity(t *testing.T) {
	brokerURL := os.Getenv("ASYNCAPI_MQTT_TEST_URL")
	if brokerURL == "" {
		t.Skip("ASYNCAPI_MQTT_TEST_URL is not set")
	}
	parsed, err := url.Parse(brokerURL)
	if err != nil {
		t.Fatal(err)
	}
	document := mqttTestDocument(parsed.Host)
	driver := New(Options{})
	client, err := asyncapiclient.Load(context.Background(), asyncapiclient.Source{Content: document}, asyncapiclient.LoadOptions{
		Drivers: []asyncapiclient.ProtocolDriver{driver},
		Context: map[string]any{"basic": map[string]any{"username": "sensor", "password": "secret"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	subscription, err := client.Subscribe(ctx, "observe", asyncapiclient.InvocationOptions{})
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(3 * time.Second)
	for subscription.Diagnostics().Leading["mqtt-subscription"] == nil {
		if time.Now().After(deadline) {
			t.Fatal("MQTT subscription did not become ready")
		}
		time.Sleep(10 * time.Millisecond)
	}

	for _, published := range []struct {
		operation string
		id        string
	}{{"publishQ0", "evt-0"}, {"publish", "evt-17"}, {"publishQ2", "evt-2"}} {
		if _, err := client.Publish(context.Background(), published.operation, map[string]any{"payload": map[string]any{"id": published.id}}, asyncapiclient.InvocationOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	for _, expected := range []string{"evt-0", "evt-17", "evt-2"} {
		select {
		case event := <-subscription.Events():
			value, ok := event.Value.(map[string]any)
			if !ok || value["id"] != expected {
				t.Fatalf("event = %#v, want id %q", event, expected)
			}
		case <-time.After(3 * time.Second):
			t.Fatalf("timed out waiting for MQTT event %q", expected)
		}
	}
	subscription.Cancel()
}

func TestLiveConnectionLossPreservesPartialOutput(t *testing.T) {
	brokerURL := os.Getenv("ASYNCAPI_MQTT_TEST_URL")
	if brokerURL == "" {
		t.Skip("ASYNCAPI_MQTT_TEST_URL is not set")
	}
	parsed, err := url.Parse(brokerURL)
	if err != nil {
		t.Fatal(err)
	}
	document := mqttTestDocument(parsed.Host)
	var decoded map[string]any
	if err := json.Unmarshal(document, &decoded); err != nil {
		t.Fatal(err)
	}
	channels := decoded["channels"].(map[string]any)
	events := channels["events"].(map[string]any)
	events["address"] = "failure/{tenant}"
	document, err = json.Marshal(decoded)
	if err != nil {
		t.Fatal(err)
	}

	client, err := asyncapiclient.Load(context.Background(), asyncapiclient.Source{Content: document}, asyncapiclient.LoadOptions{
		Drivers: []asyncapiclient.ProtocolDriver{New(Options{})},
		Context: map[string]any{"basic": map[string]any{"username": "sensor", "password": "secret"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	subscription, err := client.Subscribe(context.Background(), "observe", asyncapiclient.InvocationOptions{})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case event, open := <-subscription.Events():
		if !open {
			t.Fatal("subscription ended before its first output")
		}
		value, ok := event.Value.(map[string]any)
		if !ok || value["id"] != "before-disconnect" {
			t.Fatalf("event = %#v", event)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for output before connection loss")
	}
	select {
	case _, open := <-subscription.Events():
		if open {
			t.Fatal("unexpected output after broker connection loss")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("subscription did not terminate after broker connection loss")
	}
	if err := subscription.Wait(); err == nil || !strings.Contains(strings.ToLower(err.Error()), "mqtt connection lost") {
		t.Fatalf("terminal error = %v", err)
	}
}

func mqttTestDocument(host string) []byte {
	document := map[string]any{
		"asyncapi":           "3.0.0",
		"info":               map[string]any{"title": "MQTT Go conformance", "version": "1"},
		"defaultContentType": "application/json",
		"servers": map[string]any{"production": map[string]any{
			"host": host, "protocol": "mqtt", "protocolVersion": "3.1.1",
			"security": []any{map[string]any{"type": "userPassword"}},
			"bindings": map[string]any{"mqtt": map[string]any{
				"clientId": "ob-mqtt-go-test", "cleanSession": true, "keepAlive": 15, "bindingVersion": "0.2.0",
			}},
		}},
		"channels": map[string]any{"events": map[string]any{
			"address":    "events/{tenant}",
			"parameters": map[string]any{"tenant": map[string]any{"default": "acme"}},
			"messages": map[string]any{"Event": map[string]any{
				"contentType": "application/json",
				"payload":     map[string]any{"type": "object", "required": []any{"id"}, "properties": map[string]any{"id": map[string]any{"type": "string"}}},
			}},
		}},
		"operations": map[string]any{
			"publish": map[string]any{
				"action": "receive", "channel": map[string]any{"$ref": "#/channels/events"},
				"messages": []any{map[string]any{"$ref": "#/channels/events/messages/Event"}},
				"bindings": map[string]any{"mqtt": map[string]any{"qos": 1, "retain": true, "bindingVersion": "0.2.0"}},
			},
			"publishQ0": map[string]any{
				"action": "receive", "channel": map[string]any{"$ref": "#/channels/events"},
				"messages": []any{map[string]any{"$ref": "#/channels/events/messages/Event"}},
				"bindings": map[string]any{"mqtt": map[string]any{"qos": 0, "retain": false, "bindingVersion": "0.2.0"}},
			},
			"publishQ2": map[string]any{
				"action": "receive", "channel": map[string]any{"$ref": "#/channels/events"},
				"messages": []any{map[string]any{"$ref": "#/channels/events/messages/Event"}},
				"bindings": map[string]any{"mqtt": map[string]any{"qos": 2, "retain": false, "bindingVersion": "0.2.0"}},
			},
			"observe": map[string]any{
				"action": "send", "channel": map[string]any{"$ref": "#/channels/events"},
				"messages": []any{map[string]any{"$ref": "#/channels/events/messages/Event"}},
				"bindings": map[string]any{"mqtt": map[string]any{"qos": 1, "bindingVersion": "0.2.0"}},
			},
		},
	}
	result, _ := json.Marshal(document)
	return result
}
