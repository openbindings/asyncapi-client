// Package mqtt supplies the optional MQTT 3.1.1 protocol driver for the
// standalone AsyncAPI client. It interprets AsyncAPI's MQTT binding and uses
// Eclipse Paho for the concrete exchange; it has no OpenBindings dependency.
package mqtt

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"fmt"
	"io"
	"net/url"
	"sort"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	paho "github.com/eclipse/paho.mqtt.golang"
	asyncapiclient "github.com/openbindings/asyncapi-client/go"
)

type Options struct {
	ProtocolVersion string
	ClientID        string
	TLSConfig       *tls.Config
	Protocols       []string
}

type Driver struct {
	options     Options
	mu          sync.Mutex
	connections map[string]*mqttConnection
}

func New(options Options) *Driver {
	return &Driver{options: options, connections: map[string]*mqttConnection{}}
}

func (d *Driver) Protocols() []string {
	if len(d.options.Protocols) > 0 {
		return append([]string(nil), d.options.Protocols...)
	}
	return []string{"mqtt"}
}

func (d *Driver) Execute(ctx context.Context, request asyncapiclient.DriverRequest, session asyncapiclient.DriverSession) error {
	profile, err := resolveProfile(request, d.options)
	if err != nil {
		return err
	}
	clientOptions, err := clientOptions(request, profile, d.options)
	if err != nil {
		return err
	}
	connection, err := d.acquire(ctx, request, profile, clientOptions)
	if err != nil {
		return err
	}
	defer d.release(connection)

	if request.Action == "receive" {
		return publishInputs(ctx, connection.client, request, session, profile)
	}
	return d.subscribeOutputs(ctx, connection, request, session, profile)
}

type mqttProfile struct {
	QoS       byte
	Retain    bool
	Clean     bool
	KeepAlive uint16
	ClientID  string
}

type mqttSubscriber struct {
	request asyncapiclient.DriverRequest
	session asyncapiclient.DriverSession
	errors  chan error
}

type mqttConnection struct {
	key         string
	client      paho.Client
	users       int
	ready       chan struct{}
	connectErr  error
	subscribers map[string]map[*mqttSubscriber]struct{}
}

func (d *Driver) acquire(ctx context.Context, request asyncapiclient.DriverRequest, profile mqttProfile, options *paho.ClientOptions) (*mqttConnection, error) {
	key := connectionKey(request, profile, options)
	d.mu.Lock()
	if existing := d.connections[key]; existing != nil {
		existing.users++
		d.mu.Unlock()
		select {
		case <-ctx.Done():
			d.release(existing)
			return nil, ctx.Err()
		case <-existing.ready:
			if existing.connectErr != nil {
				d.release(existing)
				return nil, existing.connectErr
			}
			return existing, nil
		}
	}

	connection := &mqttConnection{
		key: key, users: 1, ready: make(chan struct{}),
		subscribers: map[string]map[*mqttSubscriber]struct{}{},
	}
	options.SetDefaultPublishHandler(func(_ paho.Client, message paho.Message) {
		d.dispatch(connection, message)
	})
	options.SetConnectionLostHandler(func(_ paho.Client, err error) {
		d.failSubscribers(connection, fmt.Errorf("MQTT connection lost: %w", err))
	})
	connection.client = paho.NewClient(options)
	d.connections[key] = connection
	d.mu.Unlock()

	err := waitToken(ctx, connection.client.Connect())
	d.mu.Lock()
	connection.connectErr = err
	close(connection.ready)
	if err != nil && d.connections[key] == connection {
		delete(d.connections, key)
	}
	d.mu.Unlock()
	if err != nil {
		connection.client.Disconnect(0)
		return nil, fmt.Errorf("connect MQTT: %w", err)
	}
	return connection, nil
}

func (d *Driver) release(connection *mqttConnection) {
	d.mu.Lock()
	connection.users--
	if connection.users > 0 {
		d.mu.Unlock()
		return
	}
	if d.connections[connection.key] == connection {
		delete(d.connections, connection.key)
	}
	d.mu.Unlock()
	connection.client.Disconnect(250)
}

func (d *Driver) dispatch(connection *mqttConnection, message paho.Message) {
	d.mu.Lock()
	set := connection.subscribers[message.Topic()]
	subscribers := make([]*mqttSubscriber, 0, len(set))
	for subscriber := range set {
		subscribers = append(subscribers, subscriber)
	}
	d.mu.Unlock()
	for _, subscriber := range subscribers {
		value, err := subscriber.request.Output.Decode(append([]byte(nil), message.Payload()...))
		if err == nil {
			err = subscriber.session.Emit(value)
		}
		if err != nil {
			select {
			case subscriber.errors <- err:
			default:
			}
		}
	}
}

func (d *Driver) failSubscribers(connection *mqttConnection, err error) {
	d.mu.Lock()
	var subscribers []*mqttSubscriber
	for _, set := range connection.subscribers {
		for subscriber := range set {
			subscribers = append(subscribers, subscriber)
		}
	}
	d.mu.Unlock()
	for _, subscriber := range subscribers {
		select {
		case subscriber.errors <- err:
		default:
		}
	}
}

func resolveProfile(request asyncapiclient.DriverRequest, options Options) (mqttProfile, error) {
	if _, exists := request.Operation["x-ob-asyncapi-v2-security-conjunction"]; exists {
		return mqttProfile{}, fmt.Errorf("the MQTT 3.1.1 profile does not admit normalized AsyncAPI 2.x multi-scheme security conjunctions")
	}
	if _, exists := request.Server["x-ob-asyncapi-v2-security-conjunction"]; exists {
		return mqttProfile{}, fmt.Errorf("the MQTT 3.1.1 profile does not admit normalized AsyncAPI 2.x multi-scheme security conjunctions")
	}
	if _, exists := request.Operation["reply"]; exists {
		return mqttProfile{}, fmt.Errorf("the MQTT 3.1.1 driver does not admit AsyncAPI reply operations")
	}
	direction := request.Input
	if direction == nil && request.Output != nil {
		direction = &asyncapiclient.DriverInput{DriverDirection: request.Output.DriverDirection}
	}
	if direction == nil {
		return mqttProfile{}, fmt.Errorf("MQTT request has no invocation direction")
	}
	version := stringValue(request.Server["protocolVersion"])
	if version == "" {
		version = stringValue(mqttConfiguration(request)["protocolVersion"])
	}
	if version == "" {
		version = options.ProtocolVersion
	}
	if version != "3.1.1" {
		return mqttProfile{}, fmt.Errorf("MQTT execution requires server.protocolVersion or configuration.mqtt.protocolVersion to select exactly 3.1.1")
	}

	serverBinding := binding(request.Server, "mqtt")
	operationBinding := binding(request.Operation, "mqtt")
	channelBinding := binding(direction.Channel, "mqtt")
	if err := validateBindingVersion(serverBinding, "server"); err != nil {
		return mqttProfile{}, err
	}
	if err := validateBindingVersion(operationBinding, "operation"); err != nil {
		return mqttProfile{}, err
	}
	if err := validateAllowedFields(serverBinding, []string{"clientId", "cleanSession", "lastWill", "keepAlive", "sessionExpiryInterval", "maximumPacketSize", "bindingVersion"}, "server"); err != nil {
		return mqttProfile{}, err
	}
	if err := validateAllowedFields(operationBinding, []string{"qos", "retain", "messageExpiryInterval", "bindingVersion"}, "operation"); err != nil {
		return mqttProfile{}, err
	}
	if len(channelBinding) > 0 {
		return mqttProfile{}, fmt.Errorf("the AsyncAPI MQTT 0.2.0 channel binding must be empty")
	}
	for _, message := range direction.Messages {
		messageBinding := binding(message, "mqtt")
		if err := validateBindingVersion(messageBinding, "message"); err != nil {
			return mqttProfile{}, err
		}
		var mqtt5 []string
		for key := range messageBinding {
			if key != "bindingVersion" {
				mqtt5 = append(mqtt5, key)
			}
		}
		if len(mqtt5) > 0 {
			sort.Strings(mqtt5)
			return mqttProfile{}, fmt.Errorf("MQTT 3.1.1 cannot apply MQTT 5 message-binding fields: %s", strings.Join(mqtt5, ", "))
		}
	}
	if err := validateTopic(direction.Address, request.Action); err != nil {
		return mqttProfile{}, err
	}

	qos, err := qosValue(valueOr(operationBinding, "qos", float64(0)), "operation qos")
	if err != nil {
		return mqttProfile{}, err
	}
	retain, err := booleanValue(valueOr(operationBinding, "retain", false), "operation retain")
	if err != nil {
		return mqttProfile{}, err
	}
	if request.Action == "send" {
		if _, exists := operationBinding["retain"]; exists {
			return mqttProfile{}, fmt.Errorf("the MQTT retain operation-binding field applies only when publishing")
		}
	}
	if _, exists := operationBinding["messageExpiryInterval"]; exists {
		return mqttProfile{}, fmt.Errorf("messageExpiryInterval is MQTT 5-only and is outside the MQTT 3.1.1 driver profile")
	}

	clean, err := booleanValue(valueOr(serverBinding, "cleanSession", true), "server cleanSession")
	if err != nil {
		return mqttProfile{}, err
	}
	if !clean {
		return mqttProfile{}, fmt.Errorf("persistent MQTT sessions are outside the currently qualified MQTT 3.1.1 profile")
	}
	keepAlive, err := integerValue(valueOr(serverBinding, "keepAlive", float64(60)), "server keepAlive", 0, 65535)
	if err != nil {
		return mqttProfile{}, err
	}
	if _, exists := serverBinding["sessionExpiryInterval"]; exists {
		return mqttProfile{}, fmt.Errorf("the selected MQTT server binding uses MQTT 5-only sessionExpiryInterval")
	}
	if _, exists := serverBinding["maximumPacketSize"]; exists {
		return mqttProfile{}, fmt.Errorf("the selected MQTT server binding uses MQTT 5-only maximumPacketSize")
	}
	if _, exists := serverBinding["lastWill"]; exists {
		return mqttProfile{}, fmt.Errorf("MQTT Last Will is outside the currently qualified MQTT 3.1.1 profile")
	}
	clientID := stringValue(serverBinding["clientId"])
	if clientID == "" {
		clientID = stringValue(mqttConfiguration(request)["clientId"])
	}
	if clientID == "" {
		clientID = options.ClientID
	}
	if !clean && clientID == "" {
		return mqttProfile{}, fmt.Errorf("a persistent MQTT session requires an artifact- or configuration-selected clientId")
	}

	return mqttProfile{QoS: qos, Retain: retain, Clean: clean, KeepAlive: uint16(keepAlive), ClientID: clientID}, nil
}

func clientOptions(request asyncapiclient.DriverRequest, profile mqttProfile, options Options) (*paho.ClientOptions, error) {
	target, err := url.Parse(request.ServerURL)
	if err != nil {
		return nil, fmt.Errorf("parse MQTT server URL: %w", err)
	}
	if target.Scheme != "mqtt" && target.Scheme != "mqtts" {
		return nil, fmt.Errorf("MQTT driver cannot execute target scheme %q", target.Scheme)
	}
	if target.Path != "" && target.Path != "/" {
		return nil, fmt.Errorf("MQTT TCP targets cannot apply an AsyncAPI server pathname")
	}

	selectedSecurity, err := selectSecurity(request, options, target.Scheme)
	if err != nil {
		return nil, err
	}

	result := paho.NewClientOptions().AddBroker(request.ServerURL)
	result.SetProtocolVersion(4)
	result.SetAutoReconnect(false)
	result.SetConnectRetry(false)
	result.SetCleanSession(profile.Clean)
	result.SetKeepAlive(time.Duration(profile.KeepAlive) * time.Second)
	result.SetOrderMatters(true)
	if profile.ClientID != "" {
		result.SetClientID(profile.ClientID)
	}
	if selectedSecurity.UseBasic {
		result.SetUsername(selectedSecurity.Username)
		result.SetPassword(selectedSecurity.Password)
	}
	if target.Scheme == "mqtts" && options.TLSConfig != nil {
		result.SetTLSConfig(options.TLSConfig.Clone())
	}
	return result, nil
}

func publishInputs(ctx context.Context, client paho.Client, request asyncapiclient.DriverRequest, session asyncapiclient.DriverSession, profile mqttProfile) error {
	if request.Input == nil {
		return fmt.Errorf("MQTT publish request has no artifact input lane")
	}
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
		payload, err := request.Input.Encode(value)
		if err != nil {
			return err
		}
		if err := waitToken(ctx, client.Publish(request.Input.Address, profile.QoS, profile.Retain, payload)); err != nil {
			return fmt.Errorf("publish MQTT message: %w", err)
		}
		count++
	}
	if count == 0 {
		return fmt.Errorf("MQTT publish invocation requires at least one input value")
	}
	return nil
}

func (d *Driver) subscribeOutputs(ctx context.Context, connection *mqttConnection, request asyncapiclient.DriverRequest, session asyncapiclient.DriverSession, profile mqttProfile) error {
	if request.Output == nil {
		return fmt.Errorf("MQTT subscription request has no artifact output lane")
	}
	if err := session.CloseInput(); err != nil {
		return err
	}
	subscriber := &mqttSubscriber{request: request, session: session, errors: make(chan error, 1)}
	d.mu.Lock()
	set := connection.subscribers[request.Output.Address]
	if set == nil {
		set = map[*mqttSubscriber]struct{}{}
		connection.subscribers[request.Output.Address] = set
	}
	set[subscriber] = struct{}{}
	d.mu.Unlock()
	defer func() {
		d.mu.Lock()
		delete(set, subscriber)
		if len(set) == 0 {
			delete(connection.subscribers, request.Output.Address)
		}
		d.mu.Unlock()
	}()
	if err := waitToken(ctx, connection.client.Subscribe(request.Output.Address, profile.QoS, nil)); err != nil {
		return fmt.Errorf("subscribe MQTT topic: %w", err)
	}
	if err := session.SetLeadingMetadata(asyncapiclient.Metadata{"mqtt-subscription": {"ready"}}); err != nil {
		return err
	}
	select {
	case <-ctx.Done():
		return nil
	case <-session.Done():
		return nil
	case err := <-subscriber.errors:
		return err
	}
}

func connectionKey(request asyncapiclient.DriverRequest, profile mqttProfile, options *paho.ClientOptions) string {
	identity := fmt.Sprintf("%s\x00%s\x00%t\x00%d\x00%s\x00%s\x00%s",
		request.ServerURL, request.Protocol, profile.Clean, profile.KeepAlive,
		profile.ClientID, options.Username, options.Password)
	return fmt.Sprintf("%x", sha256.Sum256([]byte(identity)))
}

func waitToken(ctx context.Context, token paho.Token) error {
	for !token.WaitTimeout(25 * time.Millisecond) {
		if err := ctx.Err(); err != nil {
			return err
		}
	}
	return token.Error()
}

func binding(owner map[string]any, name string) map[string]any {
	bindings := mapValue(owner["bindings"])
	if bindings == nil {
		return map[string]any{}
	}
	if value := mapValue(bindings[name]); value != nil {
		return value
	}
	return map[string]any{}
}

func validateBindingVersion(value map[string]any, location string) error {
	version := valueOr(value, "bindingVersion", "0.2.0")
	if version != "0.1.0" && version != "0.2.0" {
		return fmt.Errorf("MQTT %s binding version %q is outside the 0.1.0/0.2.0 driver profile", location, version)
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
	return fmt.Errorf("MQTT %s binding contains undeclared fields: %s", location, strings.Join(unknown, ", "))
}

func validateTopic(topic, action string) error {
	if topic == "" || len([]byte(topic)) > 65535 || !utf8.ValidString(topic) || strings.ContainsRune(topic, '\x00') {
		return fmt.Errorf("MQTT topic must be a non-empty, valid UTF-8 string no larger than 65535 bytes")
	}
	if action == "receive" {
		if strings.ContainsAny(topic, "#+") {
			return fmt.Errorf("MQTT publish topic names cannot contain wildcard characters")
		}
		return nil
	}
	levels := strings.Split(topic, "/")
	for index, level := range levels {
		if strings.Contains(level, "#") && (level != "#" || index != len(levels)-1) || strings.Contains(level, "+") && level != "+" {
			return fmt.Errorf("MQTT subscription wildcards must occupy an entire level and # must be final")
		}
	}
	return nil
}

func mqttConfiguration(request asyncapiclient.DriverRequest) map[string]any {
	configuration := mapValue(request.Context["configuration"])
	if configuration == nil {
		return nil
	}
	return mapValue(configuration["mqtt"])
}

type selectedSecurity struct {
	UseBasic bool
	Username string
	Password string
	UseX509  bool
}

func selectSecurity(request asyncapiclient.DriverRequest, options Options, scheme string) (selectedSecurity, error) {
	if len(request.SecurityAlternatives) == 0 {
		return selectedSecurity{}, nil
	}
	username, password, hasBasic := basicCredential(request.Context)
	hasX509 := scheme == "mqtts" && options.TLSConfig != nil && len(options.TLSConfig.Certificates) > 0
	types := map[string]bool{}
	for _, alternative := range request.SecurityAlternatives {
		selected := selectedSecurity{}
		satisfiable := len(alternative) > 0
		for _, resolved := range alternative {
			typeName := stringValue(resolved.Scheme["type"])
			types[typeName] = true
			switch typeName {
			case "userPassword":
				selected.UseBasic = true
				selected.Username, selected.Password = username, password
				if !hasBasic {
					satisfiable = false
				}
			case "X509":
				selected.UseX509 = true
				if !hasX509 {
					satisfiable = false
				}
			default:
				satisfiable = false
			}
		}
		if satisfiable {
			return selected, nil
		}
	}
	var names []string
	for name := range types {
		names = append(names, name)
	}
	sort.Strings(names)
	return selectedSecurity{}, fmt.Errorf("no declared AsyncAPI security alternative can be satisfied by the MQTT 3.1.1 driver (%s)", strings.Join(names, ", "))
}

func basicCredential(contextValue map[string]any) (string, string, bool) {
	basic := mapValue(contextValue["basic"])
	username := stringValue(basic["username"])
	password := stringValue(basic["password"])
	return username, password, username != "" || password != ""
}

func qosValue(value any, name string) (byte, error) {
	number, ok := value.(float64)
	if ok && (number == 0 || number == 1 || number == 2) {
		return byte(number), nil
	}
	return 0, fmt.Errorf("%s must be 0, 1, or 2", name)
}

func booleanValue(value any, name string) (bool, error) {
	result, ok := value.(bool)
	if !ok {
		return false, fmt.Errorf("%s must be boolean", name)
	}
	return result, nil
}

func integerValue(value any, name string, minimum, maximum int) (int, error) {
	number, ok := value.(float64)
	integer := int(number)
	if !ok || number != float64(integer) || integer < minimum || integer > maximum {
		return 0, fmt.Errorf("%s must be an integer from %d through %d", name, minimum, maximum)
	}
	return integer, nil
}

func valueOr(value map[string]any, key string, fallback any) any {
	if selected, exists := value[key]; exists {
		return selected
	}
	return fallback
}

func mapValue(value any) map[string]any {
	result, _ := value.(map[string]any)
	return result
}

func stringValue(value any) string {
	result, _ := value.(string)
	return result
}
