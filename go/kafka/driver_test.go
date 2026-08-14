package kafka

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	asyncapiclient "github.com/openbindings/asyncapi-client/go"
)

func TestResolveProfileMapsAuthoredKafkaBinding(t *testing.T) {
	request := profileRequest()
	request.Context = map[string]any{"configuration": map[string]any{"kafka": map[string]any{"fromBeginning": true}}}
	profile, err := resolveProfile(request, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if len(profile.Connection.Brokers) != 1 || profile.Connection.Brokers[0] != "broker.example.test:9092" {
		t.Fatalf("brokers = %#v", profile.Connection.Brokers)
	}
	if profile.Connection.ClientID != "orders-client" || profile.GroupID != "orders-workers" || profile.Topic != "orders.v1" || string(profile.Key) != "tenant-a" || !profile.FromBeginning {
		t.Fatalf("profile = %#v", profile)
	}
}

func TestResolveProfileRefusesUnsupportedCellsBeforeClientConstruction(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*asyncapiclient.DriverRequest)
		want   string
	}{
		{
			name: "future binding revision",
			mutate: func(request *asyncapiclient.DriverRequest) {
				request.Operation["bindings"].(map[string]any)["kafka"].(map[string]any)["bindingVersion"] = "9.0.0"
			},
			want: "outside the 0.1.0-0.5.0",
		},
		{
			name: "undeclared channel field",
			mutate: func(request *asyncapiclient.DriverRequest) {
				request.Output.Channel["bindings"].(map[string]any)["kafka"].(map[string]any)["future"] = true
			},
			want: "undeclared fields",
		},
		{
			name: "pre-0.4 topic configuration",
			mutate: func(request *asyncapiclient.DriverRequest) {
				request.Output.Channel["bindings"].(map[string]any)["kafka"].(map[string]any)["bindingVersion"] = "0.3.0"
			},
			want: "predates topicConfiguration",
		},
		{
			name: "Schema Registry framing",
			mutate: func(request *asyncapiclient.DriverRequest) {
				request.Output.Messages[0]["bindings"].(map[string]any)["kafka"].(map[string]any)["schemaIdLocation"] = "header"
			},
			want: "Schema Registry",
		},
		{
			name: "dynamic key",
			mutate: func(request *asyncapiclient.DriverRequest) {
				request.Output.Messages[0]["bindings"].(map[string]any)["kafka"].(map[string]any)["key"] = map[string]any{"type": "string"}
			},
			want: "does not select one value",
		},
		{
			name: "invalid topic",
			mutate: func(request *asyncapiclient.DriverRequest) {
				request.Output.Channel["bindings"].(map[string]any)["kafka"].(map[string]any)["topic"] = "orders/wild"
			},
			want: "Kafka topic",
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

func TestDriverDelegatesValuesAndPreservesOutputBeforeFailure(t *testing.T) {
	factory := &fakeFactory{}
	driver := New(Options{ClientFactory: factory})
	publish := profileRequest()
	publish.Action = "receive"
	publish.Input = &asyncapiclient.DriverInput{DriverDirection: publish.Output.DriverDirection, Encode: func(value any) ([]byte, error) { return []byte(value.(string)), nil }}
	publish.Output = nil
	session := newFakeSession("first", "second")
	if err := driver.Execute(context.Background(), publish, session); err != nil {
		t.Fatal(err)
	}
	if len(factory.producer.sent) != 2 || string(factory.producer.sent[0].value) != "first" || string(factory.producer.sent[1].value) != "second" {
		t.Fatalf("sent = %#v", factory.producer.sent)
	}
	if factory.producer.sent[0].topic != "orders.v1" || string(factory.producer.sent[0].key) != "tenant-a" {
		t.Fatalf("sent record = %#v", factory.producer.sent[0])
	}

	factory.consumer.messages = []ConsumerMessage{{Key: []byte("tenant-a"), Value: []byte("before-failure")}}
	factory.consumer.failure = errors.New("Kafka broker connection lost")
	subscribe := profileRequest()
	subscribe.Action = "send"
	subscribe.Output.Decode = func(payload []byte) (any, error) { return string(payload), nil }
	output := newFakeSession()
	err := driver.Execute(context.Background(), subscribe, output)
	if err == nil || !strings.Contains(err.Error(), "Kafka broker connection lost") {
		t.Fatalf("error = %v", err)
	}
	if len(output.outputs) != 1 || output.outputs[0] != "before-failure" {
		t.Fatalf("outputs = %#v", output.outputs)
	}
	if !output.inputClosed {
		t.Fatal("subscription did not half-close input")
	}
}

func TestSelectSecurityDoesNotVolunteerUndeclaredCredential(t *testing.T) {
	request := profileRequest()
	request.Context = map[string]any{"basic": map[string]any{"username": "orders", "password": "secret"}}
	profile, err := resolveProfile(request, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if profile.Connection.SASL != nil {
		t.Fatalf("undeclared credential was volunteered: %#v", profile.Connection.SASL)
	}
	request.SecurityAlternatives = [][]asyncapiclient.DriverSecurityScheme{{{
		Name: "kafkaBasic", Scheme: map[string]any{"type": "userPassword"},
	}}}
	profile, err = resolveProfile(request, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if profile.Connection.SASL == nil || profile.Connection.SASL.Mechanism != "plain" || profile.Connection.SASL.Username != "orders" {
		t.Fatalf("selected security = %#v", profile.Connection.SASL)
	}
}

func TestResolveProfileCompletesSchemasOnlyFromExplicitConfiguration(t *testing.T) {
	request := profileRequest()
	operation := request.Operation["bindings"].(map[string]any)["kafka"].(map[string]any)
	operation["clientId"] = map[string]any{"type": "string", "pattern": "^cfg-"}
	operation["groupId"] = map[string]any{"type": "string", "pattern": "^group-"}
	message := request.Output.Messages[0]["bindings"].(map[string]any)["kafka"].(map[string]any)
	message["key"] = map[string]any{"type": "string", "pattern": "^key-"}
	request.Context = map[string]any{"configuration": map[string]any{"kafka": map[string]any{
		"clientId": "cfg-client", "groupId": "group-workers", "key": "key-tenant",
	}}}
	profile, err := resolveProfile(request, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if profile.Connection.ClientID != "cfg-client" || profile.GroupID != "group-workers" || string(profile.Key) != "key-tenant" {
		t.Fatalf("profile = %#v", profile)
	}
}

func profileRequest() asyncapiclient.DriverRequest {
	return asyncapiclient.DriverRequest{
		Action: "send", ServerURL: "kafka://broker.example.test:9092",
		Server: map[string]any{"bindings": map[string]any{"kafka": map[string]any{"bindingVersion": "0.5.0"}}},
		Operation: map[string]any{"bindings": map[string]any{"kafka": map[string]any{
			"clientId":       map[string]any{"type": "string", "const": "orders-client"},
			"groupId":        map[string]any{"type": "string", "const": "orders-workers"},
			"bindingVersion": "0.5.0",
		}}},
		Output: &asyncapiclient.DriverOutput{DriverDirection: asyncapiclient.DriverDirection{
			Address: "orders/acme",
			Channel: map[string]any{"bindings": map[string]any{"kafka": map[string]any{
				"topic": "orders.v1", "partitions": float64(3), "replicas": float64(1),
				"topicConfiguration": map[string]any{"cleanup.policy": "delete", "retention.ms": float64(86400000)},
				"bindingVersion":     "0.5.0",
			}}},
			Messages: []map[string]any{{"bindings": map[string]any{"kafka": map[string]any{
				"key": map[string]any{"type": "string", "const": "tenant-a"}, "bindingVersion": "0.5.0",
			}}}},
		}},
		Context: map[string]any{},
	}
}

type sentRecord struct {
	topic   string
	key     []byte
	value   []byte
	headers []asyncapiclient.DriverHeader
}

type fakeFactory struct {
	configs  []ConnectionConfig
	producer fakeProducer
	consumer fakeConsumer
}

func (f *fakeFactory) Create(config ConnectionConfig) Client {
	f.configs = append(f.configs, config)
	return fakeClient{factory: f}
}

type fakeClient struct{ factory *fakeFactory }

func (f fakeClient) Producer() Producer                { return &f.factory.producer }
func (f fakeClient) Consumer(ConsumerOptions) Consumer { return &f.factory.consumer }

type fakeProducer struct{ sent []sentRecord }

func (f *fakeProducer) Connect(context.Context) error { return nil }
func (f *fakeProducer) Send(_ context.Context, topic string, key, value []byte, headers []asyncapiclient.DriverHeader) error {
	f.sent = append(f.sent, sentRecord{topic: topic, key: append([]byte(nil), key...), value: append([]byte(nil), value...), headers: headers})
	return nil
}
func (f *fakeProducer) Disconnect() {}

type fakeConsumer struct {
	messages []ConsumerMessage
	failure  error
	topic    string
}

func (f *fakeConsumer) Connect(context.Context) error { return nil }
func (f *fakeConsumer) Subscribe(topic string)        { f.topic = topic }
func (f *fakeConsumer) Run(_ context.Context, deliver func(ConsumerMessage) error) error {
	for _, message := range f.messages {
		if err := deliver(message); err != nil {
			return err
		}
	}
	return f.failure
}
func (f *fakeConsumer) Disconnect() {}

type fakeSession struct {
	inputs      []any
	outputs     []any
	inputClosed bool
	done        chan struct{}
}

func newFakeSession(inputs ...any) *fakeSession {
	return &fakeSession{inputs: inputs, done: make(chan struct{})}
}

func (f *fakeSession) Receive(context.Context) (any, error) {
	if len(f.inputs) == 0 {
		return nil, io.EOF
	}
	value := f.inputs[0]
	f.inputs = f.inputs[1:]
	return value, nil
}
func (f *fakeSession) CloseInput() error                                { f.inputClosed = true; return nil }
func (f *fakeSession) Emit(value any) error                             { f.outputs = append(f.outputs, value); return nil }
func (f *fakeSession) SetLeadingMetadata(asyncapiclient.Metadata) error { return nil }
func (f *fakeSession) SetTrailingMetadata(asyncapiclient.Metadata)      {}
func (f *fakeSession) Complete()                                        {}
func (f *fakeSession) Done() <-chan struct{}                            { return f.done }

// Kafka record-header carriage (§9.2 per-cell capability, qualified here):
// the routed envelope's application headers ride the unit seam as record
// headers on publish, and received record headers project into the output
// envelope on subscribe — end to end through the client.
func TestKafkaCarriesRecordHeadersBothDirections(t *testing.T) {
	artifact := func(action string) []byte {
		return []byte(`{
  "asyncapi":"3.0.0",
  "info":{"title":"Record headers","version":"1"},
  "servers":{"broker":{"host":"broker.example:9092","protocol":"kafka"}},
  "channels":{"orders":{"address":"orders.v1","messages":{
    "Order":{"contentType":"application/json","payload":{"type":"object"},
      "headers":{"type":"object","properties":{"traceId":{"type":"string"},"attempt":{"type":"integer"}}}}
  }}},
  "operations":{"op":{
    "action":"` + action + `",
    "channel":{"$ref":"#/channels/orders"},
    "messages":[{"$ref":"#/channels/orders/messages/Order"}]
  }}
}`)
	}

	factory := &fakeFactory{}
	client, err := asyncapiclient.Load(context.Background(), asyncapiclient.Source{Content: artifact("receive")}, asyncapiclient.LoadOptions{
		Drivers: []asyncapiclient.ProtocolDriver{New(Options{ClientFactory: factory})},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close() }()
	if _, err := client.Publish(context.Background(), "op",
		map[string]any{"payload": map[string]any{"id": 4}, "headers": map[string]any{"traceId": "t-7", "attempt": 2}},
		asyncapiclient.InvocationOptions{}); err != nil {
		t.Fatal(err)
	}
	if len(factory.producer.sent) != 1 {
		t.Fatalf("sent = %#v", factory.producer.sent)
	}
	record := factory.producer.sent[0]
	if string(record.value) != `{"id":4}` {
		t.Fatalf("record value = %q, want the bare payload", record.value)
	}
	got := map[string]string{}
	for _, pair := range record.headers {
		got[pair.Key] = string(pair.Value)
	}
	if got["traceId"] != "t-7" || got["attempt"] != "2" {
		t.Fatalf("record headers = %#v", got)
	}

	subFactory := &fakeFactory{}
	subFactory.consumer.messages = []ConsumerMessage{{
		Value: []byte(`{"id":9}`),
		Headers: []asyncapiclient.DriverHeader{
			{Key: "traceId", Value: []byte("t-9")},
			{Key: "attempt", Value: []byte("5")},
			{Key: "x-infra", Value: []byte("never-projected")},
		},
	}}
	subscriber, err := asyncapiclient.Load(context.Background(), asyncapiclient.Source{Content: artifact("send")}, asyncapiclient.LoadOptions{
		Drivers: []asyncapiclient.ProtocolDriver{New(Options{ClientFactory: subFactory})},
		Context: map[string]any{"configuration": map[string]any{"kafka": map[string]any{"groupId": "g-1"}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = subscriber.Close() }()
	execution, err := subscriber.Start(context.Background(), "op", asyncapiclient.InvocationOptions{})
	if err != nil {
		t.Fatal(err)
	}
	var outputs []any
	for event := range execution.Events() {
		outputs = append(outputs, event.Value)
	}
	if err := execution.Wait(); err != nil {
		t.Fatal(err)
	}
	if len(outputs) != 1 {
		t.Fatalf("outputs = %#v", outputs)
	}
	envelope, _ := outputs[0].(map[string]any)
	payload, _ := envelope["payload"].(map[string]any)
	headers, _ := envelope["headers"].(map[string]any)
	if payload["id"] != float64(9) {
		t.Fatalf("payload = %#v", envelope)
	}
	if headers["traceId"] != "t-9" || headers["attempt"] != float64(5) {
		t.Fatalf("headers = %#v, want declared projection with integer parsing", headers)
	}
	if _, leaked := headers["x-infra"]; leaked {
		t.Fatalf("undeclared record header leaked: %#v", headers)
	}
}
