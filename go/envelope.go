package asyncapiclient

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
)

// The routed operation envelope (openbindings.asyncapi@1 §9.2, ruled
// 2026-08-14): a channel that declares location-less parameters makes the
// publish input an envelope object — the payload under the protocol-neutral
// "payload" field, each parameter under its own name — because addressing
// data varies per invocation and is therefore operation input.
// configuration.address.parameters remains the amortized pre-fill; an
// explicitly supplied envelope field wins.

// locationlessParamNames returns the channel's location-less parameter
// names, sorted. A parameter whose location declares a runtime expression
// stays derived from that expression's source and never rides the envelope.
func locationlessParamNames(ch *channel) []string {
	if ch == nil {
		return nil
	}
	var names []string
	for name, declared := range ch.Parameters {
		if declared.Location == "" {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	return names
}

// clientEnvelopeFieldName mirrors synthesis field naming: a parameter
// literally named "payload" (or "headers") keeps its own name and the
// reserved field takes the "_value"-suffixed spelling.
func clientEnvelopeFieldName(reserved string, params []string) string {
	for _, name := range params {
		if name == reserved {
			return reserved + "_value"
		}
	}
	return reserved
}

// splitInputEnvelope separates a publish input riding the routed envelope
// into its payload value, address-parameter values, and (when the selected
// input message declares a headers contract) the application-headers
// value. envelope=false means neither condition holds and the value is the
// bare wholesale payload, unchanged. Whether the selected protocol cell can
// CARRY the headers value is the dispatch site's per-cell capability
// judgment, not this split's.
func splitInputEnvelope(ch *channel, msgs []message, value any) (payload any, params map[string]string, headers map[string]any, envelope bool, err error) {
	names := locationlessParamNames(ch)
	headersDeclared := false
	for _, m := range msgs {
		if m.Headers != nil {
			headersDeclared = true
		}
	}
	if len(names) == 0 && !headersDeclared {
		return value, nil, nil, false, nil
	}
	object, ok := value.(map[string]any)
	if !ok {
		return nil, nil, nil, true, fmt.Errorf("this operation's input is the routed envelope: supply one object carrying %q and the declared envelope fields, got %T", "payload", value)
	}
	payloadField := clientEnvelopeFieldName("payload", names)
	headersField := clientEnvelopeFieldName("headers", names)
	declared := map[string]bool{payloadField: true}
	if headersDeclared {
		declared[headersField] = true
	}
	for _, name := range names {
		declared[name] = true
	}
	params = map[string]string{}
	for field, member := range object {
		if !declared[field] {
			return nil, nil, nil, true, fmt.Errorf("the routed envelope is closed: field %q matches no declared envelope field", field)
		}
		switch field {
		case payloadField:
		case headersField:
			if headersDeclared {
				supplied, ok := member.(map[string]any)
				if !ok {
					return nil, nil, nil, true, fmt.Errorf("the envelope's %q field must be an object valued under the artifact's headers schema, got %T", headersField, member)
				}
				headers = supplied
				continue
			}
		default:
			text, terr := parameterText(member)
			if terr != nil {
				return nil, nil, nil, true, fmt.Errorf("envelope parameter %q: %v", field, terr)
			}
			params[field] = text
		}
	}
	supplied, present := object[payloadField]
	if !present {
		return nil, nil, nil, true, fmt.Errorf("the routed envelope requires the %q field carrying the message payload", payloadField)
	}
	if headersDeclared && headers == nil {
		return nil, nil, nil, true, fmt.Errorf("the selected message declares a headers contract: the routed envelope requires the %q field", headersField)
	}
	return supplied, params, headers, true, nil
}

// headerFieldText renders one application-header value as an HTTP field
// value: strings ride as-is, other scalars ride their JSON text; a
// structured value has no faithful field spelling and refuses.
func headerFieldText(name string, value any) (string, error) {
	switch v := value.(type) {
	case string:
		return v, nil
	case bool, float64, int, int64, json.Number:
		encoded, err := json.Marshal(v)
		if err != nil {
			return "", err
		}
		return string(encoded), nil
	default:
		return "", fmt.Errorf("application header %q: a header value must be a scalar, got %T", name, value)
	}
}

// projectResponseHeaders derives the output envelope's headers value from a
// carried protocol response: only the DECLARED top-level properties of the
// governing headers contracts are application data (transport fields never
// leak), matched case-insensitively per HTTP. A property whose declared
// schema types it number/integer/boolean parses its field text as that JSON
// type when the text conforms — declaration-driven, never sniffing.
func projectResponseHeaders(msgs []message, header map[string][]string) map[string]any {
	projected := map[string]any{}
	byLower := map[string]string{}
	for name, values := range header {
		if len(values) > 0 {
			byLower[strings.ToLower(name)] = values[0]
		}
	}
	for _, m := range msgs {
		if m.Headers == nil {
			continue
		}
		declared := m.Headers
		if inner, ok := declared["schema"].(map[string]any); ok {
			if _, wrapped := declared["schemaFormat"].(string); wrapped {
				declared = inner
			}
		}
		properties, _ := declared["properties"].(map[string]any)
		for name, memberSchema := range properties {
			text, present := byLower[strings.ToLower(name)]
			if !present {
				continue
			}
			projected[name] = parseDeclaredScalar(memberSchema, text)
		}
	}
	return projected
}

func parseDeclaredScalar(memberSchema any, text string) any {
	schema, _ := memberSchema.(map[string]any)
	declaredType, _ := schema["type"].(string)
	switch declaredType {
	case "integer", "number":
		var parsed float64
		if err := json.Unmarshal([]byte(text), &parsed); err == nil {
			if declaredType == "number" || parsed == float64(int64(parsed)) {
				return parsed
			}
		}
	case "boolean":
		if text == "true" {
			return true
		}
		if text == "false" {
			return false
		}
	}
	return text
}

// parameterText renders one envelope parameter value for address expansion:
// strings ride as-is; other scalars ride their JSON text (a 2.x Parameter
// Object may declare a non-string schema); structured values have no
// address spelling and refuse.
func parameterText(value any) (string, error) {
	switch v := value.(type) {
	case string:
		return v, nil
	case bool, float64, int, int64, json.Number:
		encoded, err := json.Marshal(v)
		if err != nil {
			return "", err
		}
		return string(encoded), nil
	default:
		return "", fmt.Errorf("an address parameter value must be a scalar, got %T", value)
	}
}

// installDriverInputSeams wires one driver input lane's Encode and
// EncodeUnit closures. Every unit splits the routed envelope uniformly:
// the payload rides the codec lanes, per-unit address parameters must
// match the invocation's resolved addressing (a topic or address cannot
// vary per unit within one dispatch), and application headers become
// protocol pairs on the unit seam. The legacy Encode seam refuses a
// headers-bearing value rather than dropping it.
func installDriverInputSeams(input *DriverInput, ch *channel, msgs []message, codec inputCodec, resolvedParams map[string]string, args *executionArgs) {
	split := func(value any) (any, map[string]any, error) {
		payload, params, headers, isEnvelope, err := splitInputEnvelope(ch, msgs, value)
		if err != nil {
			return nil, nil, err
		}
		if !isEnvelope {
			return value, nil, nil
		}
		for name, supplied := range params {
			if resolved, present := resolvedParams[name]; !present || resolved != supplied {
				return nil, nil, fmt.Errorf("envelope parameter %q = %q does not match the invocation's resolved addressing (%q): an address cannot vary per unit within one dispatch", name, supplied, resolvedParams[name])
			}
		}
		return payload, headers, nil
	}
	encodePayload := func(payload any) ([]byte, error) {
		return args.Hooks.EncodeInput(siteFor(args, ""), payload, func(v any) ([]byte, error) { return encodeInput(codec, v) })
	}
	input.Encode = func(value any) ([]byte, error) {
		payload, headers, err := split(value)
		if err != nil {
			return nil, err
		}
		if headers != nil {
			return nil, fmt.Errorf("the value carries application headers; this driver lane consumes the payload-only seam and cannot carry them")
		}
		return encodePayload(payload)
	}
	input.EncodeUnit = func(value any) (DriverUnit, error) {
		payload, headers, err := split(value)
		if err != nil {
			return DriverUnit{}, err
		}
		encoded, err := encodePayload(payload)
		if err != nil {
			return DriverUnit{}, err
		}
		unit := DriverUnit{Payload: encoded}
		names := make([]string, 0, len(headers))
		for name := range headers {
			names = append(names, name)
		}
		sort.Strings(names)
		for _, name := range names {
			text, herr := headerFieldText(name, headers[name])
			if herr != nil {
				return DriverUnit{}, herr
			}
			unit.Headers = append(unit.Headers, DriverHeader{Key: name, Value: []byte(text)})
		}
		return unit, nil
	}
}
