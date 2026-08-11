package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"testing"
	"time"

	asyncapiclient "github.com/openbindings/asyncapi-client/go"
)

func TestLivePublishSubscribeUsesAuthoredKafkaFacts(t *testing.T) {
	brokerURL := os.Getenv("ASYNCAPI_KAFKA_TEST_URL")
	topic := os.Getenv("ASYNCAPI_KAFKA_TEST_TOPIC_GO")
	if brokerURL == "" || topic == "" {
		t.Skip("ASYNCAPI_KAFKA_TEST_URL and ASYNCAPI_KAFKA_TEST_TOPIC_GO are not set")
	}
	parsed, err := url.Parse(brokerURL)
	if err != nil {
		t.Fatal(err)
	}
	client, err := asyncapiclient.Load(context.Background(), asyncapiclient.Source{Content: kafkaTestDocument(t, parsed.Host, topic, fmt.Sprintf("ob-kafka-go-%d", os.Getpid()))}, asyncapiclient.LoadOptions{
		Drivers: []asyncapiclient.ProtocolDriver{New(Options{FromBeginning: true})},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	for _, id := range []string{"go-1", "go-2", "go-3"} {
		if _, err := client.Publish(context.Background(), "publish", map[string]any{"id": id}, asyncapiclient.InvocationOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	subscription, err := client.Subscribe(context.Background(), "observe", asyncapiclient.InvocationOptions{})
	if err != nil {
		t.Fatal(err)
	}
	for _, expected := range []string{"go-1", "go-2", "go-3"} {
		select {
		case event, open := <-subscription.Events():
			if !open {
				t.Fatal("Kafka subscription ended before all outputs")
			}
			value, ok := event.Value.(map[string]any)
			if !ok || value["id"] != expected {
				t.Fatalf("event = %#v, want id %q", event, expected)
			}
		case <-time.After(10 * time.Second):
			t.Fatalf("timed out waiting for Kafka event %q", expected)
		}
	}
	subscription.Cancel()
}

func TestLiveTransientBrokerLossPreservesOutputsAndRecovers(t *testing.T) {
	brokerURL := os.Getenv("ASYNCAPI_KAFKA_TEST_URL")
	topic := os.Getenv("ASYNCAPI_KAFKA_TEST_TOPIC_GO_RECOVERY")
	container := os.Getenv("ASYNCAPI_KAFKA_TEST_CONTAINER")
	if brokerURL == "" || topic == "" || container == "" {
		t.Skip("Kafka recovery qualification environment is not set")
	}
	parsed, err := url.Parse(brokerURL)
	if err != nil {
		t.Fatal(err)
	}
	client, err := asyncapiclient.Load(context.Background(), asyncapiclient.Source{Content: kafkaTestDocument(t, parsed.Host, topic, fmt.Sprintf("ob-kafka-go-recovery-%d", os.Getpid()))}, asyncapiclient.LoadOptions{
		Drivers: []asyncapiclient.ProtocolDriver{New(Options{FromBeginning: true})},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	if _, err := client.Publish(context.Background(), "publish", map[string]any{"id": "before-loss"}, asyncapiclient.InvocationOptions{}); err != nil {
		t.Fatal(err)
	}
	subscription, err := client.Subscribe(context.Background(), "observe", asyncapiclient.InvocationOptions{})
	if err != nil {
		t.Fatal(err)
	}
	readKafkaEvent(t, subscription, "before-loss")

	paused := false
	defer func() {
		if paused {
			_ = exec.Command("docker", "unpause", container).Run()
		}
	}()
	if output, err := exec.Command("docker", "pause", container).CombinedOutput(); err != nil {
		t.Fatalf("pause Kafka broker: %v: %s", err, output)
	}
	paused = true
	time.Sleep(time.Second)
	if output, err := exec.Command("docker", "unpause", container).CombinedOutput(); err != nil {
		t.Fatalf("unpause Kafka broker: %v: %s", err, output)
	}
	paused = false

	if _, err := client.Publish(context.Background(), "publish", map[string]any{"id": "after-recovery"}, asyncapiclient.InvocationOptions{}); err != nil {
		t.Fatal(err)
	}
	readKafkaEvent(t, subscription, "after-recovery")
	subscription.Cancel()
}

func TestLiveSCRAMUsesAbstractBasicContext(t *testing.T) {
	brokerURL := os.Getenv("ASYNCAPI_KAFKA_TEST_URL")
	topic := os.Getenv("ASYNCAPI_KAFKA_TEST_TOPIC_GO_SECURITY")
	if brokerURL == "" || topic == "" {
		t.Skip("Kafka SCRAM qualification environment is not set")
	}
	parsed, err := url.Parse(brokerURL)
	if err != nil {
		t.Fatal(err)
	}
	content := kafkaTestDocument(t, parsed.Host, topic, fmt.Sprintf("ob-kafka-go-security-%d", os.Getpid()))
	var document map[string]any
	if err := json.Unmarshal(content, &document); err != nil {
		t.Fatal(err)
	}
	servers := document["servers"].(map[string]any)
	production := servers["production"].(map[string]any)
	production["security"] = []any{map[string]any{"type": "scramSha256"}}
	content, err = json.Marshal(document)
	if err != nil {
		t.Fatal(err)
	}
	client, err := asyncapiclient.Load(context.Background(), asyncapiclient.Source{Content: content}, asyncapiclient.LoadOptions{
		Drivers: []asyncapiclient.ProtocolDriver{New(Options{FromBeginning: true})},
		Context: map[string]any{"basic": map[string]any{"username": "orders", "password": "secret-password"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	if _, err := client.Publish(context.Background(), "publish", map[string]any{"id": "secured-go"}, asyncapiclient.InvocationOptions{}); err != nil {
		t.Fatal(err)
	}
	subscription, err := client.Subscribe(context.Background(), "observe", asyncapiclient.InvocationOptions{})
	if err != nil {
		t.Fatal(err)
	}
	readKafkaEvent(t, subscription, "secured-go")
	subscription.Cancel()
}

func readKafkaEvent(t *testing.T, subscription *asyncapiclient.Execution, expected string) {
	t.Helper()
	select {
	case event, open := <-subscription.Events():
		if !open {
			t.Fatal("Kafka subscription ended before expected output")
		}
		value, ok := event.Value.(map[string]any)
		if !ok || value["id"] != expected {
			t.Fatalf("event = %#v, want id %q", event, expected)
		}
	case <-time.After(10 * time.Second):
		t.Fatalf("timed out waiting for Kafka event %q", expected)
	}
}

func kafkaTestDocument(t *testing.T, host, topic, groupID string) []byte {
	t.Helper()
	document := map[string]any{
		"asyncapi":           "3.0.0",
		"info":               map[string]any{"title": "Kafka Go live qualification", "version": "1"},
		"defaultContentType": "application/json",
		"servers": map[string]any{"production": map[string]any{
			"host": host, "protocol": "kafka", "bindings": map[string]any{"kafka": map[string]any{"bindingVersion": "0.5.0"}},
		}},
		"channels": map[string]any{"events": map[string]any{
			"address": "orders/{tenant}", "parameters": map[string]any{"tenant": map[string]any{"default": "acme"}},
			"bindings": map[string]any{"kafka": map[string]any{"topic": topic, "partitions": 3, "replicas": 1, "bindingVersion": "0.5.0"}},
			"messages": map[string]any{"Event": map[string]any{
				"contentType": "application/json",
				"payload":     map[string]any{"type": "object", "required": []any{"id"}, "properties": map[string]any{"id": map[string]any{"type": "string"}}},
				"bindings":    map[string]any{"kafka": map[string]any{"key": map[string]any{"type": "string", "const": "tenant-a"}, "bindingVersion": "0.5.0"}},
			}},
		}},
		"operations": map[string]any{
			"publish": map[string]any{
				"action": "receive", "channel": map[string]any{"$ref": "#/channels/events"}, "messages": []any{map[string]any{"$ref": "#/channels/events/messages/Event"}},
				"bindings": map[string]any{"kafka": map[string]any{"clientId": map[string]any{"type": "string", "const": "ob-kafka-go-producer"}, "bindingVersion": "0.5.0"}},
			},
			"observe": map[string]any{
				"action": "send", "channel": map[string]any{"$ref": "#/channels/events"}, "messages": []any{map[string]any{"$ref": "#/channels/events/messages/Event"}},
				"bindings": map[string]any{"kafka": map[string]any{
					"clientId": map[string]any{"type": "string", "const": "ob-kafka-go-consumer"},
					"groupId":  map[string]any{"type": "string", "const": groupID}, "bindingVersion": "0.5.0",
				}},
			},
		},
	}
	result, err := json.Marshal(document)
	if err != nil {
		t.Fatal(err)
	}
	return result
}
