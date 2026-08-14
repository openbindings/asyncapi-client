package kafka

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/url"
	"regexp"
	"sort"
	"strings"

	asyncapiclient "github.com/openbindings/asyncapi-client/go"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

type Options struct {
	Protocols     []string
	ClientID      string
	GroupID       string
	FromBeginning bool
	ClientFactory ClientFactory
}

type ConnectionConfig struct {
	Brokers  []string
	ClientID string
	SASL     *SASLConfig
}

type SASLConfig struct {
	Mechanism string
	Username  string
	Password  string
}

type ClientFactory interface {
	Create(ConnectionConfig) Client
}

type Client interface {
	Producer() Producer
	Consumer(ConsumerOptions) Consumer
}

type Producer interface {
	Connect(context.Context) error
	Send(context.Context, string, []byte, []byte, []asyncapiclient.DriverHeader) error
	Disconnect()
}

type ConsumerOptions struct {
	GroupID       string
	FromBeginning bool
}

type ConsumerMessage struct {
	Value   []byte
	Key     []byte
	Null    bool
	Headers []asyncapiclient.DriverHeader
}

type Consumer interface {
	Connect(context.Context) error
	Subscribe(string)
	Run(context.Context, func(ConsumerMessage) error) error
	Disconnect()
}

type Driver struct {
	options Options
}

func New(options Options) *Driver { return &Driver{options: options} }

// CarriesMessageHeaders declares the Kafka cell's native per-message
// header carriage (record headers): the routed envelope's application
// headers ride the unit seam (§9.2 per-cell capability).
func (d *Driver) CarriesMessageHeaders() bool { return true }

func (d *Driver) Protocols() []string {
	if len(d.options.Protocols) > 0 {
		return append([]string(nil), d.options.Protocols...)
	}
	return []string{"kafka"}
}

func (d *Driver) Execute(ctx context.Context, request asyncapiclient.DriverRequest, session asyncapiclient.DriverSession) error {
	profile, err := resolveProfile(request, d.options)
	if err != nil {
		return err
	}
	factory := d.options.ClientFactory
	if factory == nil {
		factory = franzFactory{}
	}
	client := factory.Create(profile.Connection)
	if request.Action == "receive" {
		return publishInputs(ctx, client.Producer(), request, session, profile)
	}
	if profile.GroupID == "" {
		return fmt.Errorf("Kafka subscription requires a single authored or configured groupId")
	}
	return subscribeOutputs(ctx, client.Consumer(ConsumerOptions{
		GroupID: profile.GroupID, FromBeginning: profile.FromBeginning,
	}), request, session, profile)
}

type kafkaProfile struct {
	Connection    ConnectionConfig
	Topic         string
	GroupID       string
	Key           []byte
	FromBeginning bool
}

func resolveProfile(request asyncapiclient.DriverRequest, options Options) (kafkaProfile, error) {
	if _, exists := request.Operation["x-ob-asyncapi-v2-security-conjunction"]; exists {
		return kafkaProfile{}, fmt.Errorf("the Kafka profile does not admit normalized AsyncAPI 2.x multi-scheme security conjunctions")
	}
	if _, exists := request.Server["x-ob-asyncapi-v2-security-conjunction"]; exists {
		return kafkaProfile{}, fmt.Errorf("the Kafka profile does not admit normalized AsyncAPI 2.x multi-scheme security conjunctions")
	}
	if _, exists := request.Operation["reply"]; exists {
		return kafkaProfile{}, fmt.Errorf("the Kafka driver does not admit AsyncAPI reply operations")
	}

	serverBinding := binding(request.Server, "kafka")
	direction := request.Input
	if direction == nil && request.Output != nil {
		direction = &asyncapiclient.DriverInput{DriverDirection: request.Output.DriverDirection}
	}
	if direction == nil {
		return kafkaProfile{}, fmt.Errorf("Kafka request has no invocation direction")
	}
	channelBinding := binding(direction.Channel, "kafka")
	operationBinding := binding(request.Operation, "kafka")
	serverVersion, err := validateBinding("server", serverBinding)
	if err != nil {
		return kafkaProfile{}, err
	}
	channelVersion, err := validateBinding("channel", channelBinding)
	if err != nil {
		return kafkaProfile{}, err
	}
	if _, err := validateBinding("operation", operationBinding); err != nil {
		return kafkaProfile{}, err
	}
	if err := validateServerBinding(serverBinding, serverVersion); err != nil {
		return kafkaProfile{}, err
	}
	if err := validateChannelBinding(channelBinding, channelVersion); err != nil {
		return kafkaProfile{}, err
	}
	if err := validateOperationBinding(operationBinding); err != nil {
		return kafkaProfile{}, err
	}

	configuration := kafkaConfiguration(request)
	topic := stringValue(channelBinding["topic"])
	if topic == "" {
		topic = direction.Address
	}
	if err := validateTopic(topic); err != nil {
		return kafkaProfile{}, err
	}

	var key []byte
	for _, message := range direction.Messages {
		// Declared Message headers ride the routed envelope through the
		// unit seam as Kafka record headers (§9.2 per-cell capability,
		// qualified by this driver) — no longer a profile exclusion.
		messageBinding := binding(message, "kafka")
		version, err := validateBinding("message", messageBinding)
		if err != nil {
			return kafkaProfile{}, err
		}
		if err := validateMessageBinding(messageBinding, version); err != nil {
			return kafkaProfile{}, err
		}
		if hasRegistryMessageFields(messageBinding) {
			return kafkaProfile{}, fmt.Errorf("Kafka Schema Registry message framing is outside the currently qualified profile")
		}
		authored, err := literalBytes(messageBinding["key"], configuration["key"], "Kafka message key")
		if err != nil {
			return kafkaProfile{}, err
		}
		if authored != nil {
			if key != nil && !bytes.Equal(key, authored) {
				return kafkaProfile{}, fmt.Errorf("selected Kafka messages declare different keys")
			}
			key = authored
		}
	}

	configuredGroup := valueOr(configuration, "groupId", options.GroupID)
	groupID, err := literalString(operationBinding["groupId"], configuredGroup, "Kafka groupId")
	if err != nil {
		return kafkaProfile{}, err
	}
	configuredClient := valueOr(configuration, "clientId", options.ClientID)
	clientID, err := literalString(operationBinding["clientId"], configuredClient, "Kafka clientId")
	if err != nil {
		return kafkaProfile{}, err
	}
	if clientID == "" {
		clientID = "openbindings-asyncapi"
	}
	fromBeginning, err := booleanConfiguration(configuration["fromBeginning"], options.FromBeginning)
	if err != nil {
		return kafkaProfile{}, err
	}
	brokers, err := kafkaBrokers(request.ServerURL)
	if err != nil {
		return kafkaProfile{}, err
	}
	security, err := selectSecurity(request)
	if err != nil {
		return kafkaProfile{}, err
	}

	return kafkaProfile{
		Connection: ConnectionConfig{Brokers: brokers, ClientID: clientID, SASL: security},
		Topic:      topic, GroupID: groupID, Key: key, FromBeginning: fromBeginning,
	}, nil
}

func publishInputs(ctx context.Context, producer Producer, request asyncapiclient.DriverRequest, session asyncapiclient.DriverSession, profile kafkaProfile) error {
	if request.Input == nil {
		return fmt.Errorf("Kafka publish request has no artifact input lane")
	}
	if err := producer.Connect(ctx); err != nil {
		return fmt.Errorf("connect Kafka producer: %w", err)
	}
	defer producer.Disconnect()
	count := 0
	for {
		value, err := session.Receive(ctx)
		if err == io.EOF {
			break
		}
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		unit, err := encodeUnit(request.Input, value)
		if err != nil {
			return err
		}
		if err := producer.Send(ctx, profile.Topic, profile.Key, unit.Payload, unit.Headers); err != nil {
			return fmt.Errorf("publish Kafka record: %w", err)
		}
		count++
	}
	if count == 0 {
		return fmt.Errorf("Kafka publish invocation requires at least one input value")
	}
	return nil
}

func subscribeOutputs(ctx context.Context, consumer Consumer, request asyncapiclient.DriverRequest, session asyncapiclient.DriverSession, profile kafkaProfile) error {
	if request.Output == nil {
		return fmt.Errorf("Kafka subscription request has no artifact output lane")
	}
	if err := session.CloseInput(); err != nil {
		return err
	}
	if err := consumer.Connect(ctx); err != nil {
		return fmt.Errorf("connect Kafka consumer: %w", err)
	}
	defer consumer.Disconnect()
	consumer.Subscribe(profile.Topic)
	err := consumer.Run(ctx, func(message ConsumerMessage) error {
		if profile.Key != nil && !bytes.Equal(message.Key, profile.Key) {
			return fmt.Errorf("received Kafka record key does not match the authored message key")
		}
		if message.Null {
			return fmt.Errorf("Kafka tombstone records are outside the currently qualified payload profile")
		}
		value, err := decodeUnit(request.Output, asyncapiclient.DriverUnit{Payload: message.Value, Headers: message.Headers})
		if err != nil {
			return err
		}
		return session.Emit(value)
	})
	if ctx.Err() != nil || err == context.Canceled {
		return nil
	}
	return err
}

func recordHeaders(headers []kgo.RecordHeader) []asyncapiclient.DriverHeader {
	out := make([]asyncapiclient.DriverHeader, 0, len(headers))
	for _, pair := range headers {
		out = append(out, asyncapiclient.DriverHeader{
			Key: pair.Key, Value: append([]byte(nil), pair.Value...),
		})
	}
	return out
}

// encodeUnit prefers the unit seam (payload + record headers); a request
// built without it falls back to the payload-only seam.
func encodeUnit(input *asyncapiclient.DriverInput, value any) (asyncapiclient.DriverUnit, error) {
	if input.EncodeUnit != nil {
		return input.EncodeUnit(value)
	}
	payload, err := input.Encode(value)
	if err != nil {
		return asyncapiclient.DriverUnit{}, err
	}
	return asyncapiclient.DriverUnit{Payload: payload}, nil
}

// decodeUnit prefers the unit seam (record headers project into the routed
// envelope); a request built without it falls back to the payload-only
// seam, dropping no headers because none were contracted.
func decodeUnit(output *asyncapiclient.DriverOutput, unit asyncapiclient.DriverUnit) (any, error) {
	if output.DecodeUnit != nil {
		return output.DecodeUnit(unit)
	}
	return output.Decode(unit.Payload)
}

type franzFactory struct{}

func (franzFactory) Create(config ConnectionConfig) Client {
	return &franzClient{config: config}
}

type franzClient struct{ config ConnectionConfig }

func (c *franzClient) Producer() Producer { return &franzProducer{config: c.config} }
func (c *franzClient) Consumer(options ConsumerOptions) Consumer {
	return &franzConsumer{config: c.config, options: options}
}

type franzProducer struct {
	config ConnectionConfig
	client *kgo.Client
}

func (p *franzProducer) Connect(ctx context.Context) error {
	client, err := kgo.NewClient(franzOptions(p.config)...)
	if err != nil {
		return err
	}
	p.client = client
	return client.Ping(ctx)
}

func (p *franzProducer) Send(ctx context.Context, topic string, key, value []byte, headers []asyncapiclient.DriverHeader) error {
	record := &kgo.Record{Topic: topic, Value: append([]byte(nil), value...)}
	if key != nil {
		record.Key = append([]byte(nil), key...)
	}
	for _, pair := range headers {
		record.Headers = append(record.Headers, kgo.RecordHeader{
			Key: pair.Key, Value: append([]byte(nil), pair.Value...),
		})
	}
	return p.client.ProduceSync(ctx, record).FirstErr()
}

func (p *franzProducer) Disconnect() {
	if p.client != nil {
		p.client.Close()
	}
}

type franzConsumer struct {
	config  ConnectionConfig
	options ConsumerOptions
	topic   string
	client  *kgo.Client
}

func (c *franzConsumer) Connect(ctx context.Context) error {
	offset := kgo.NewOffset().AtEnd()
	if c.options.FromBeginning {
		offset = kgo.NewOffset().AtStart()
	}
	options := append(franzOptions(c.config),
		kgo.ConsumerGroup(c.options.GroupID),
		kgo.ConsumeResetOffset(offset),
		kgo.DisableAutoCommit(),
	)
	client, err := kgo.NewClient(options...)
	if err != nil {
		return err
	}
	c.client = client
	return client.Ping(ctx)
}

func (c *franzConsumer) Subscribe(topic string) {
	c.topic = topic
	c.client.AddConsumeTopics(topic)
}

func (c *franzConsumer) Run(ctx context.Context, deliver func(ConsumerMessage) error) error {
	for {
		fetches := c.client.PollFetches(ctx)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if errs := fetches.Errors(); len(errs) > 0 {
			return fmt.Errorf("Kafka consume: %v", errs)
		}
		for _, record := range fetches.Records() {
			if record.Topic != c.topic {
				continue
			}
			if err := deliver(ConsumerMessage{
				Value:   append([]byte(nil), record.Value...),
				Key:     append([]byte(nil), record.Key...),
				Null:    record.Value == nil,
				Headers: recordHeaders(record.Headers),
			}); err != nil {
				return err
			}
			if err := c.client.CommitRecords(ctx, record); err != nil {
				return fmt.Errorf("commit Kafka record: %w", err)
			}
		}
	}
}

func (c *franzConsumer) Disconnect() {
	if c.client != nil {
		c.client.Close()
	}
}

func franzOptions(config ConnectionConfig) []kgo.Opt {
	options := []kgo.Opt{kgo.SeedBrokers(config.Brokers...), kgo.ClientID(config.ClientID)}
	if config.SASL != nil {
		var mechanism sasl.Mechanism
		switch config.SASL.Mechanism {
		case "plain":
			mechanism = plain.Auth{User: config.SASL.Username, Pass: config.SASL.Password}.AsMechanism()
		case "scram-sha-256":
			mechanism = scram.Auth{User: config.SASL.Username, Pass: config.SASL.Password}.AsSha256Mechanism()
		case "scram-sha-512":
			mechanism = scram.Auth{User: config.SASL.Username, Pass: config.SASL.Password}.AsSha512Mechanism()
		}
		if mechanism != nil {
			options = append(options, kgo.SASL(mechanism))
		}
	}
	return options
}

type kafkaBindingVersion string

var kafkaBindingVersions = map[kafkaBindingVersion]bool{
	"0.1.0": true, "0.2.0": true, "0.3.0": true, "0.4.0": true, "0.5.0": true,
}

func validateBinding(location string, value map[string]any) (kafkaBindingVersion, error) {
	version := kafkaBindingVersion(stringValue(valueOr(value, "bindingVersion", "0.5.0")))
	if !kafkaBindingVersions[version] {
		return "", fmt.Errorf("Kafka %s binding version %q is outside the 0.1.0-0.5.0 driver profile", location, version)
	}
	return version, nil
}

func validateServerBinding(value map[string]any, version kafkaBindingVersion) error {
	if err := validateAllowedFields(value, []string{"schemaRegistryUrl", "schemaRegistryVendor", "bindingVersion"}, "server"); err != nil {
		return err
	}
	if version < "0.3.0" && (value["schemaRegistryUrl"] != nil || value["schemaRegistryVendor"] != nil) {
		return fmt.Errorf("Kafka server binding %s predates Schema Registry fields", version)
	}
	registryURL, hasURL := value["schemaRegistryUrl"]
	vendor, hasVendor := value["schemaRegistryVendor"]
	if hasVendor && !hasURL {
		return fmt.Errorf("Kafka schemaRegistryVendor requires schemaRegistryUrl")
	}
	if hasURL {
		raw := stringValue(registryURL)
		parsed, err := url.Parse(raw)
		if raw == "" || err != nil || parsed.Scheme == "" || parsed.Host == "" {
			return fmt.Errorf("Kafka schemaRegistryUrl must be a valid URL")
		}
	}
	if hasVendor && stringValue(vendor) == "" {
		return fmt.Errorf("Kafka schemaRegistryVendor must be a non-empty string")
	}
	return nil
}

func validateChannelBinding(value map[string]any, version kafkaBindingVersion) error {
	if err := validateAllowedFields(value, []string{"topic", "partitions", "replicas", "topicConfiguration", "bindingVersion"}, "channel"); err != nil {
		return err
	}
	if version < "0.3.0" {
		for name := range value {
			if name != "bindingVersion" {
				return fmt.Errorf("Kafka channel binding %s must be empty", version)
			}
		}
	}
	if topic, exists := value["topic"]; exists {
		if err := validateTopic(stringValue(topic)); err != nil {
			return err
		}
	}
	if err := positiveInteger(value["partitions"], "Kafka partitions"); err != nil {
		return err
	}
	if err := positiveInteger(value["replicas"], "Kafka replicas"); err != nil {
		return err
	}
	if raw, exists := value["topicConfiguration"]; exists {
		if version < "0.4.0" {
			return fmt.Errorf("Kafka channel binding %s predates topicConfiguration", version)
		}
		configuration := mapValue(raw)
		if configuration == nil {
			return fmt.Errorf("Kafka topicConfiguration must be an object")
		}
		if err := validateTopicConfiguration(configuration, version); err != nil {
			return err
		}
	}
	return nil
}

func validateOperationBinding(value map[string]any) error {
	if err := validateAllowedFields(value, []string{"groupId", "clientId", "bindingVersion"}, "operation"); err != nil {
		return err
	}
	for _, name := range []string{"groupId", "clientId"} {
		if raw, exists := value[name]; exists && mapValue(raw) == nil {
			return fmt.Errorf("Kafka %s must be a Schema Object", name)
		}
	}
	return nil
}

func validateMessageBinding(value map[string]any, version kafkaBindingVersion) error {
	if err := validateAllowedFields(value, []string{"key", "schemaIdLocation", "schemaIdPayloadEncoding", "schemaLookupStrategy", "bindingVersion"}, "message"); err != nil {
		return err
	}
	if raw, exists := value["key"]; exists && mapValue(raw) == nil {
		return fmt.Errorf("Kafka message key must be a Schema Object in the qualified JSON Schema profile")
	}
	if version < "0.3.0" && hasRegistryMessageFields(value) {
		return fmt.Errorf("Kafka message binding %s predates Schema Registry fields", version)
	}
	return nil
}

func validateTopicConfiguration(value map[string]any, version kafkaBindingVersion) error {
	base := []string{"cleanup.policy", "retention.ms", "retention.bytes", "delete.retention.ms", "max.message.bytes"}
	confluent := []string{"confluent.key.schema.validation", "confluent.key.subject.name.strategy", "confluent.value.schema.validation", "confluent.value.subject.name.strategy"}
	if version == "0.4.0" {
		if err := validateAllowedFields(value, base, "topicConfiguration"); err != nil {
			return err
		}
	}
	if version < "0.5.0" {
		var future []string
		for _, name := range confluent {
			if _, exists := value[name]; exists {
				future = append(future, name)
			}
		}
		if len(future) > 0 {
			return fmt.Errorf("Kafka topicConfiguration %s predates fields: %s", version, strings.Join(future, ", "))
		}
	}
	if policy, exists := value["cleanup.policy"]; exists && policy != "delete" && policy != "compact" {
		return fmt.Errorf("Kafka cleanup.policy must be delete or compact")
	}
	for _, name := range []string{"retention.ms", "retention.bytes", "delete.retention.ms", "max.message.bytes"} {
		if raw, exists := value[name]; exists {
			number, ok := numberValue(raw)
			if !ok || number < 0 || number != float64(int64(number)) {
				return fmt.Errorf("Kafka %s must be a non-negative integer", name)
			}
		}
	}
	return nil
}

func validateAllowedFields(value map[string]any, allowed []string, location string) error {
	known := map[string]bool{}
	for _, name := range allowed {
		known[name] = true
	}
	var unknown []string
	for name := range value {
		if !known[name] {
			unknown = append(unknown, name)
		}
	}
	if len(unknown) == 0 {
		return nil
	}
	sort.Strings(unknown)
	return fmt.Errorf("Kafka %s binding contains undeclared fields: %s", location, strings.Join(unknown, ", "))
}

func hasRegistryMessageFields(value map[string]any) bool {
	for _, name := range []string{"schemaIdLocation", "schemaIdPayloadEncoding", "schemaLookupStrategy"} {
		if _, exists := value[name]; exists {
			return true
		}
	}
	return false
}

func kafkaBrokers(serverURL string) ([]string, error) {
	target, err := url.Parse(serverURL)
	if err != nil {
		return nil, fmt.Errorf("parse Kafka target: %w", err)
	}
	if target.Scheme != "kafka" {
		return nil, fmt.Errorf("Kafka driver cannot execute target scheme %q", target.Scheme)
	}
	if target.Path != "" && target.Path != "/" || target.RawQuery != "" || target.Fragment != "" || target.User != nil {
		return nil, fmt.Errorf("Kafka targets must contain only a broker host and optional port")
	}
	return []string{target.Host}, nil
}

var topicPattern = regexp.MustCompile(`^[a-zA-Z0-9._-]+$`)

func validateTopic(value string) error {
	if len(value) == 0 || len([]byte(value)) > 249 || value == "." || value == ".." || !topicPattern.MatchString(value) {
		return fmt.Errorf("Kafka topic must be 1-249 bytes and contain only ASCII letters, digits, '.', '_', or '-'")
	}
	return nil
}

func positiveInteger(value any, name string) error {
	if value == nil {
		return nil
	}
	number, ok := numberValue(value)
	if !ok || number <= 0 || number != float64(int64(number)) {
		return fmt.Errorf("%s must be a positive integer", name)
	}
	return nil
}

func literalString(schemaValue, configured any, name string) (string, error) {
	if schemaValue == nil {
		if configured == nil || configured == "" {
			return "", nil
		}
		value := stringValue(configured)
		if value == "" {
			return "", fmt.Errorf("%s configuration must be a non-empty string", name)
		}
		return value, nil
	}
	schema := mapValue(schemaValue)
	if schema == nil {
		return "", fmt.Errorf("%s must be a Schema Object", name)
	}
	literal, exists := schemaLiteral(schema)
	if exists {
		value := stringValue(literal)
		if value == "" {
			return "", fmt.Errorf("%s authored value must be a non-empty string", name)
		}
		if configured != nil && configured != "" && configured != value {
			return "", fmt.Errorf("%s configuration conflicts with the authored value", name)
		}
		return value, nil
	}
	value := stringValue(configured)
	if value == "" {
		return "", fmt.Errorf("%s schema does not select one value; configuration.kafka must complete it", name)
	}
	if err := validateStringSchema(value, schema, name); err != nil {
		return "", err
	}
	return value, nil
}

func literalBytes(schemaValue, configured any, name string) ([]byte, error) {
	if schemaValue == nil {
		if configured == nil {
			return nil, nil
		}
		return bytesValue(configured, name+" configuration")
	}
	schema := mapValue(schemaValue)
	if schema == nil {
		return nil, fmt.Errorf("%s must be a Schema Object", name)
	}
	literal, exists := schemaLiteral(schema)
	selected := configured
	if exists {
		selected = literal
	}
	if selected == nil {
		return nil, fmt.Errorf("%s schema does not select one value; configuration.kafka.key must complete it", name)
	}
	selectedBytes, err := bytesValue(selected, name)
	if err != nil {
		return nil, err
	}
	if exists && configured != nil {
		configuredBytes, err := bytesValue(configured, name+" configuration")
		if err != nil {
			return nil, err
		}
		if !bytes.Equal(selectedBytes, configuredBytes) {
			return nil, fmt.Errorf("%s configuration conflicts with the authored value", name)
		}
	}
	if text, ok := selected.(string); ok {
		if err := validateStringSchema(text, schema, name); err != nil {
			return nil, err
		}
	}
	return selectedBytes, nil
}

func schemaLiteral(schema map[string]any) (any, bool) {
	if value, exists := schema["const"]; exists {
		return value, true
	}
	if value, exists := schema["default"]; exists {
		return value, true
	}
	if values, ok := schema["enum"].([]any); ok && len(values) == 1 {
		return values[0], true
	}
	return nil, false
}

func validateStringSchema(value string, schema map[string]any, name string) error {
	if kind, exists := schema["type"]; exists && kind != "string" {
		return fmt.Errorf("%s schema must describe a string", name)
	}
	if minimum, ok := numberValue(schema["minLength"]); ok && float64(len([]rune(value))) < minimum {
		return fmt.Errorf("%s is shorter than minLength", name)
	}
	if maximum, ok := numberValue(schema["maxLength"]); ok && float64(len([]rune(value))) > maximum {
		return fmt.Errorf("%s is longer than maxLength", name)
	}
	if pattern := stringValue(schema["pattern"]); pattern != "" {
		compiled, err := regexp.Compile(pattern)
		if err != nil || !compiled.MatchString(value) {
			return fmt.Errorf("%s does not match its pattern", name)
		}
	}
	if values, ok := schema["enum"].([]any); ok {
		found := false
		for _, candidate := range values {
			if candidate == value {
				found = true
			}
		}
		if !found {
			return fmt.Errorf("%s is outside its enum", name)
		}
	}
	return nil
}

func bytesValue(value any, name string) ([]byte, error) {
	switch typed := value.(type) {
	case string:
		return []byte(typed), nil
	case []byte:
		return append([]byte(nil), typed...), nil
	default:
		return nil, fmt.Errorf("%s must be a string or byte slice", name)
	}
}

func selectSecurity(request asyncapiclient.DriverRequest) (*SASLConfig, error) {
	if len(request.SecurityAlternatives) == 0 {
		return nil, nil
	}
	username, password, available := basicCredential(request.Context)
	types := map[string]bool{}
	for _, alternative := range request.SecurityAlternatives {
		if len(alternative) != 1 || !available {
			continue
		}
		typeName := stringValue(alternative[0].Scheme["type"])
		types[typeName] = true
		switch typeName {
		case "userPassword":
			return &SASLConfig{Mechanism: "plain", Username: username, Password: password}, nil
		case "scramSha256":
			return &SASLConfig{Mechanism: "scram-sha-256", Username: username, Password: password}, nil
		case "scramSha512":
			return &SASLConfig{Mechanism: "scram-sha-512", Username: username, Password: password}, nil
		}
	}
	var names []string
	for typeName := range types {
		names = append(names, typeName)
	}
	sort.Strings(names)
	return nil, fmt.Errorf("no declared AsyncAPI security alternative can be satisfied by the Kafka driver (%s)", strings.Join(names, ", "))
}

func basicCredential(ctx map[string]any) (string, string, bool) {
	basic := mapValue(ctx["basic"])
	if basic == nil {
		return "", "", false
	}
	username := stringValue(basic["username"])
	password := stringValue(basic["password"])
	return username, password, username != "" && password != ""
}

func kafkaConfiguration(request asyncapiclient.DriverRequest) map[string]any {
	configuration := mapValue(request.Context["configuration"])
	if configuration == nil {
		return map[string]any{}
	}
	result := mapValue(configuration["kafka"])
	if result == nil {
		return map[string]any{}
	}
	return result
}

func booleanConfiguration(value any, fallback bool) (bool, error) {
	if value == nil {
		return fallback, nil
	}
	result, ok := value.(bool)
	if !ok {
		return false, fmt.Errorf("configuration.kafka.fromBeginning must be boolean")
	}
	return result, nil
}

func binding(owner map[string]any, name string) map[string]any {
	bindings := mapValue(owner["bindings"])
	if bindings == nil {
		return map[string]any{}
	}
	value := mapValue(bindings[name])
	if value == nil {
		return map[string]any{}
	}
	return value
}

func mapValue(value any) map[string]any {
	result, _ := value.(map[string]any)
	return result
}

func stringValue(value any) string {
	result, _ := value.(string)
	return result
}

func numberValue(value any) (float64, bool) {
	switch typed := value.(type) {
	case float64:
		return typed, true
	case float32:
		return float64(typed), true
	case int:
		return float64(typed), true
	case int32:
		return float64(typed), true
	case int64:
		return float64(typed), true
	default:
		return 0, false
	}
}

func valueOr(value map[string]any, key string, fallback any) any {
	if result, exists := value[key]; exists {
		return result
	}
	return fallback
}
