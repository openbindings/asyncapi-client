package asyncapiclient

import (
	"fmt"
	"regexp"
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
	return normalizeV2Envelope(envelope, edition), nil
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
