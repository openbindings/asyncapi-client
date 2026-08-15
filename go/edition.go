package asyncapiclient

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

var supportedAsyncAPIEditions = map[string]bool{
	"2.0.0": true, "2.1.0": true, "2.2.0": true, "2.3.0": true,
	"2.4.0": true, "2.5.0": true, "2.6.0": true,
	"3.0.0": true, "3.1.0": true,
}

func normalizeEdition(envelope map[string]any) (map[string]any, error) {
	edition, ok := envelope["asyncapi"].(string)
	if !ok || !supportedAsyncAPIEditions[edition] {
		return nil, fmt.Errorf("unsupported AsyncAPI version %v: this client accepts exactly 2.0.0–2.6.0, 3.0.0, and 3.1.0", envelope["asyncapi"])
	}
	if strings.HasPrefix(edition, "3.") {
		return envelope, nil
	}
	if err := validateV2ReferenceAdmission(envelope, edition); err != nil {
		return nil, err
	}
	return normalizeV2Envelope(envelope, edition), nil
}

// ValidateReferenceAdmission refuses an AsyncAPI 2.x document that writes a
// Reference Object at a position its own declared edition does not admit
// one. Position admission is pinned from the edition texts (the `X Object |
// Reference Object` unions of the official 2.x specifications); a document
// violating them has no interpretation under its edition, so the refusal is
// whole-artifact and deliberately consistent across implementations. The
// composers call this BEFORE external reference composition — inlining a
// reference at a non-admitting position would silently erase the evidence —
// and normalization calls it again for directly supplied documents. A 3.x
// document passes through untouched.
func ValidateReferenceAdmission(envelope map[string]any) error {
	edition, ok := envelope["asyncapi"].(string)
	if !ok || !supportedAsyncAPIEditions[edition] || strings.HasPrefix(edition, "3.") {
		return nil
	}
	return validateV2ReferenceAdmission(envelope, edition)
}

// isV2ReferenceObject reports whether a value is a Reference Object: an
// object whose $ref member is a string.
func isV2ReferenceObject(value any) bool {
	object, ok := value.(map[string]any)
	if !ok {
		return false
	}
	_, isRef := object["$ref"].(string)
	return isRef
}

// serversAdmitReferenceValues: the Servers Object's patterned field is typed
// `Server Object` through 2.3.0 and `Server Object | Reference Object` from
// 2.4.0 (pinned against the official texts of every accepted 2.x edition).
var v2ServersAdmitReferenceValues = map[string]bool{
	"2.4.0": true, "2.5.0": true, "2.6.0": true,
}

// The string-typed fields checked per object kind. Every entry is `string`
// in the edition text of every accepted 2.x edition that defines it, so a
// Reference Object there is never admitted. (Fields an older edition does
// not define are simply absent from documents of that edition; checking the
// union is harmless and keeps the table edition-independent.)
var (
	v2ServerStringFields    = []string{"url", "protocol", "protocolVersion", "description"}
	v2ChannelStringFields   = []string{"description"}
	v2OperationStringFields = []string{"operationId", "summary", "description"}
	v2MessageStringFields   = []string{"messageId", "name", "title", "summary", "description", "contentType", "schemaFormat"}
)

func v2ReferenceAdmissionError(edition, position string) error {
	return fmt.Errorf("not a valid AsyncAPI document (%s does not admit a Reference Object in AsyncAPI %s)", position, edition)
}

func validateV2ReferenceAdmission(envelope map[string]any, edition string) error {
	if isV2ReferenceObject(envelope["servers"]) {
		return v2ReferenceAdmissionError(edition, "the servers field is a Servers Object map and")
	}
	for name, raw := range anyMap(envelope["servers"]) {
		if isV2ReferenceObject(raw) && !v2ServersAdmitReferenceValues[edition] {
			return v2ReferenceAdmissionError(edition, fmt.Sprintf("server %q is typed Server Object and", name))
		}
		if err := v2StringFieldsAdmission(edition, anyMap(raw), v2ServerStringFields, "server "+strconv.Quote(name)); err != nil {
			return err
		}
	}
	if isV2ReferenceObject(envelope["channels"]) {
		return v2ReferenceAdmissionError(edition, "the channels field is a Channels Object map and")
	}
	for name, raw := range anyMap(envelope["channels"]) {
		channel := anyMap(raw)
		if channel == nil {
			continue
		}
		// The Channel Item Object's own `$ref` field is admitted (all 2.x
		// editions; deprecated from 2.4.0 but legal), so the channel value
		// itself is never refused here.
		if err := v2StringFieldsAdmission(edition, channel, v2ChannelStringFields, "channel "+strconv.Quote(name)); err != nil {
			return err
		}
		if members, ok := channel["servers"].([]any); ok {
			for _, member := range members {
				if isV2ReferenceObject(member) {
					return v2ReferenceAdmissionError(edition, fmt.Sprintf("channel %q servers is a list of server-name strings and", name))
				}
			}
		}
		for _, verb := range []string{"publish", "subscribe"} {
			operation := channel[verb]
			if operation == nil {
				continue
			}
			if isV2ReferenceObject(operation) {
				return v2ReferenceAdmissionError(edition, fmt.Sprintf("channel %q %s is an Operation Object and", name, verb))
			}
			if err := v2OperationAdmission(edition, anyMap(operation), fmt.Sprintf("channel %q %s", name, verb)); err != nil {
				return err
			}
		}
	}
	if components := anyMap(envelope["components"]); components != nil {
		for name, raw := range anyMap(components["messages"]) {
			// A components.messages VALUE may be a Reference Object; an
			// inline message carries the message string fields.
			if !isV2ReferenceObject(raw) {
				if err := v2MessageAdmission(edition, anyMap(raw), "components message "+strconv.Quote(name)); err != nil {
					return err
				}
			}
		}
		for name, raw := range anyMap(components["operationTraits"]) {
			if !isV2ReferenceObject(raw) {
				if err := v2StringFieldsAdmission(edition, anyMap(raw), v2OperationStringFields, "components operation trait "+strconv.Quote(name)); err != nil {
					return err
				}
			}
		}
		for name, raw := range anyMap(components["messageTraits"]) {
			if !isV2ReferenceObject(raw) {
				if err := v2StringFieldsAdmission(edition, anyMap(raw), v2MessageStringFields, "components message trait "+strconv.Quote(name)); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func v2OperationAdmission(edition string, operation map[string]any, position string) error {
	if operation == nil {
		return nil
	}
	if err := v2StringFieldsAdmission(edition, operation, v2OperationStringFields, position); err != nil {
		return err
	}
	if traits, ok := operation["traits"].([]any); ok {
		for index, trait := range traits {
			// A traits member may itself be a Reference Object (admitted).
			if !isV2ReferenceObject(trait) {
				if err := v2StringFieldsAdmission(edition, anyMap(trait), v2OperationStringFields, fmt.Sprintf("%s trait %d", position, index)); err != nil {
					return err
				}
			}
		}
	}
	// operation.message may be a Message Object, a Reference Object
	// (admitted), or the oneOf list of those.
	message := anyMap(operation["message"])
	if message == nil {
		return nil
	}
	if alternatives, ok := message["oneOf"].([]any); ok {
		for index, alternative := range alternatives {
			if !isV2ReferenceObject(alternative) {
				if err := v2MessageAdmission(edition, anyMap(alternative), fmt.Sprintf("%s message alternative %d", position, index)); err != nil {
					return err
				}
			}
		}
		return nil
	}
	if !isV2ReferenceObject(operation["message"]) {
		return v2MessageAdmission(edition, message, position+" message")
	}
	return nil
}

func v2MessageAdmission(edition string, message map[string]any, position string) error {
	if message == nil {
		return nil
	}
	if err := v2StringFieldsAdmission(edition, message, v2MessageStringFields, position); err != nil {
		return err
	}
	if traits, ok := message["traits"].([]any); ok {
		for index, trait := range traits {
			if !isV2ReferenceObject(trait) {
				if err := v2StringFieldsAdmission(edition, anyMap(trait), v2MessageStringFields, fmt.Sprintf("%s trait %d", position, index)); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func v2StringFieldsAdmission(edition string, object map[string]any, fields []string, position string) error {
	if object == nil {
		return nil
	}
	for _, field := range fields {
		if isV2ReferenceObject(object[field]) {
			return v2ReferenceAdmissionError(edition, fmt.Sprintf("%s %s is typed string and", position, field))
		}
	}
	return nil
}

func normalizeV2Envelope(source map[string]any, edition string) map[string]any {
	result := cloneEditionMap(source)
	result["asyncapi"] = "3.0.0"
	result["x-ob-asyncapi-source-edition"] = edition
	result["servers"] = normalizeV2Servers(source["servers"])
	channels := map[string]any{}
	operations := map[string]any{}
	for channelName, rawChannel := range anyMap(source["channels"]) {
		channel := cloneEditionMap(anyMap(rawChannel))
		messages := map[string]any{}
		channel["address"] = channelName
		channel["messages"] = messages
		delete(channel, "publish")
		delete(channel, "subscribe")
		if names, ok := rawChannelServers(anyMap(rawChannel)["servers"]); ok {
			refs := make([]any, 0, len(names))
			for _, name := range names {
				refs = append(refs, map[string]any{"$ref": "#/servers/" + escapeRefToken(name)})
			}
			channel["servers"] = refs
		}
		for _, verb := range []string{"publish", "subscribe"} {
			rawOperation, ok := anyMap(rawChannel)[verb].(map[string]any)
			if !ok {
				continue
			}
			operation := cloneEditionMap(rawOperation)
			operation["action"] = map[string]string{"publish": "receive", "subscribe": "send"}[verb]
			operation["channel"] = map[string]any{"$ref": "#/channels/" + escapeRefToken(channelName)}
			operation["messages"] = normalizeV2Messages(operation["message"], verb, channelName, messages)
			delete(operation, "message")
			operation["security"] = normalizeV2Security(operation["security"], operation)
			operation["x-ob-asyncapi-source-ref"] = v2OperationRef(channelName, verb)
			operations[v2OperationKey(channelName, verb)] = operation
		}
		channels[channelName] = channel
	}
	result["channels"] = channels
	result["operations"] = operations
	return result
}

func normalizeV2Servers(value any) map[string]any {
	out := map[string]any{}
	for name, raw := range anyMap(value) {
		server := cloneEditionMap(anyMap(raw))
		urlValue, _ := server["url"].(string)
		protocol, _ := server["protocol"].(string)
		delete(server, "url")
		host, pathname := splitV2ServerURL(urlValue, protocol)
		server["host"] = host
		if pathname != "" {
			server["pathname"] = pathname
		}
		server["security"] = normalizeV2Security(server["security"], server)
		out[name] = server
	}
	return out
}

func normalizeV2Security(value any, owner map[string]any) any {
	list, ok := value.([]any)
	if !ok {
		return value
	}
	refs := []any{}
	for _, raw := range list {
		alternative := anyMap(raw)
		if len(alternative) == 0 {
			return []any{}
		}
		if len(alternative) > 1 {
			owner["x-ob-asyncapi-v2-security-conjunction"] = alternative
		}
		for name := range alternative {
			refs = append(refs, map[string]any{"$ref": "#/components/securitySchemes/" + escapeRefToken(name)})
		}
	}
	return refs
}

func normalizeV2Messages(value any, verb, channel string, messages map[string]any) []any {
	alternatives := []any{}
	if object := anyMap(value); object != nil {
		if oneOf, ok := object["oneOf"].([]any); ok {
			alternatives = oneOf
		} else if value != nil {
			alternatives = []any{value}
		}
	}
	refs := make([]any, 0, len(alternatives))
	for index, alternative := range alternatives {
		message := anyMap(alternative)
		suggested, _ := message["messageId"].(string)
		if suggested == "" {
			suggested, _ = message["name"].(string)
		}
		if suggested == "" {
			suggested = fmt.Sprintf("%sMessage%d", verb, index+1)
		}
		key := uniqueV2MessageKey(suggested, messages)
		messages[key] = alternative
		refs = append(refs, map[string]any{"$ref": "#/channels/" + escapeRefToken(channel) + "/messages/" + escapeRefToken(key)})
	}
	return refs
}

func v2OperationKey(channel, verb string) string { return "v2:" + verb + ":" + channel }
func v2OperationRef(channel, verb string) string {
	return "#/channels/" + escapeRefToken(channel) + "/" + verb
}

func parseV2OperationRef(ref string) (string, bool) {
	parts := strings.Split(strings.TrimPrefix(ref, "#/"), "/")
	if len(parts) != 3 || parts[0] != "channels" || (parts[2] != "publish" && parts[2] != "subscribe") {
		return "", false
	}
	return v2OperationKey(unescapeRefToken(parts[1]), parts[2]), true
}

func refForNormalizedOperationKey(key string) (string, bool) {
	parts := strings.SplitN(key, ":", 3)
	if len(parts) != 3 || parts[0] != "v2" || (parts[1] != "publish" && parts[1] != "subscribe") {
		return "", false
	}
	return v2OperationRef(parts[2], parts[1]), true
}

func splitV2ServerURL(value, protocol string) (string, string) {
	remainder := strings.TrimPrefix(value, protocol+"://")
	if remainder == value {
		remainder = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9+.-]*://`).ReplaceAllString(value, "")
	}
	if slash := strings.IndexByte(remainder, '/'); slash >= 0 {
		return remainder[:slash], remainder[slash:]
	}
	return remainder, ""
}

func uniqueV2MessageKey(suggested string, messages map[string]any) string {
	if _, exists := messages[suggested]; !exists {
		return suggested
	}
	for index := 2; ; index++ {
		candidate := fmt.Sprintf("%s_%d", suggested, index)
		if _, exists := messages[candidate]; !exists {
			return candidate
		}
	}
}

func anyMap(value any) map[string]any {
	result, _ := value.(map[string]any)
	return result
}

func cloneEditionMap(value map[string]any) map[string]any {
	out := make(map[string]any, len(value))
	for key, item := range value {
		out[key] = item
	}
	return out
}

func rawChannelServers(value any) ([]string, bool) {
	list, ok := value.([]any)
	if !ok {
		return nil, false
	}
	out := make([]string, 0, len(list))
	for _, item := range list {
		if name, ok := item.(string); ok {
			out = append(out, name)
		}
	}
	return out, true
}
