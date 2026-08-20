package asyncapiclient

import (
	"reflect"
	"strings"
)

func contextBearerToken(ctx map[string]any) string { return contextString(ctx, "bearerToken") }

func contextNamedCredential(ctx map[string]any, name string) any {
	if ctx == nil || name == "" {
		return nil
	}
	values, _ := ctx["credentials"].(map[string]any)
	return values[name]
}

func contextBearerTokenFor(ctx map[string]any, name string) string {
	if value, ok := contextNamedCredential(ctx, name).(string); ok && value != "" {
		return value
	}
	return contextBearerToken(ctx)
}

func contextAPIKeyFor(ctx map[string]any, name string) string {
	if ctx == nil {
		return ""
	}
	if name != "" {
		if value, ok := contextNamedCredential(ctx, name).(string); ok && value != "" {
			return value
		}
		if values, ok := ctx["apiKeys"].(map[string]any); ok {
			if value, ok := values[name].(string); ok && value != "" {
				return value
			}
		}
	}
	return contextString(ctx, "apiKey")
}

func contextBasicAuthFor(ctx map[string]any, name string) (string, string, bool) {
	if value, ok := contextNamedCredential(ctx, name).(map[string]any); ok {
		username, _ := value["username"].(string)
		password, _ := value["password"].(string)
		if username != "" || password != "" {
			return username, password, true
		}
	}
	return contextBasicAuth(ctx)
}

func contextAccessTokenFor(ctx map[string]any, name string) string {
	if value, ok := contextNamedCredential(ctx, name).(map[string]any); ok {
		if token, ok := value["accessToken"].(string); ok && token != "" {
			return token
		}
	}
	return contextString(ctx, "accessToken")
}

func contextBasicAuth(ctx map[string]any) (string, string, bool) {
	if ctx == nil {
		return "", "", false
	}
	value, _ := ctx["basic"].(map[string]any)
	username, _ := value["username"].(string)
	password, _ := value["password"].(string)
	return username, password, username != "" || password != ""
}

func contextString(ctx map[string]any, key string) string {
	if ctx == nil {
		return ""
	}
	value, _ := ctx[key].(string)
	return value
}

func contextHeaders(ctx map[string]any) map[string]string { return contextStringMap(ctx, "headers") }
func contextCookies(ctx map[string]any) map[string]string { return contextStringMap(ctx, "cookies") }

func contextMetadata(ctx map[string]any) map[string]any {
	if ctx == nil {
		return nil
	}
	value, _ := ctx["metadata"].(map[string]any)
	return value
}

func contextConfiguration(ctx map[string]any) map[string]any {
	if ctx == nil {
		return nil
	}
	value, _ := ctx["configuration"].(map[string]any)
	return value
}

func contextStringMap(ctx map[string]any, key string) map[string]string {
	if ctx == nil {
		return nil
	}
	raw, _ := ctx[key].(map[string]any)
	out := make(map[string]string, len(raw))
	for name, value := range raw {
		if text, ok := value.(string); ok {
			out[name] = text
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func contextSatisfies(ctx map[string]any, details *Prerequisites) bool {
	if details == nil {
		return true
	}
	for _, alternative := range details.Alternatives {
		if len(alternative.Requirements) == 0 {
			continue
		}
		ok := true
		for _, requirement := range alternative.Requirements {
			if !contextSatisfiesRequirement(ctx, requirement, flatRequirementIsUnambiguous(details, requirement)) {
				ok = false
				break
			}
		}
		if ok {
			return true
		}
	}
	return false
}

func flatRequirementIsUnambiguous(details *Prerequisites, requirement Requirement) bool {
	identities := map[string]struct{}{}
	unnamed := 0
	for _, alternative := range details.Alternatives {
		for _, candidate := range alternative.Requirements {
			if candidate.Type != requirement.Type {
				continue
			}
			if candidate.Name == "" {
				unnamed++
			} else {
				identities[candidate.Name] = struct{}{}
			}
		}
	}
	return len(identities)+unnamed == 1
}

func contextSatisfiesRequirement(ctx map[string]any, requirement Requirement, allowFlatNamedCredential bool) bool {
	switch requirement.Type {
	case "auth.bearer":
		if value, ok := contextNamedCredential(ctx, requirement.Name).(string); ok && value != "" {
			return true
		}
		return allowFlatNamedCredential && contextBearerToken(ctx) != ""
	case "auth.apiKey":
		if value, ok := contextNamedCredential(ctx, requirement.Name).(string); ok && value != "" {
			return true
		}
		if requirement.Name != "" {
			if values, ok := ctx["apiKeys"].(map[string]any); ok {
				if value, ok := values[requirement.Name].(string); ok && value != "" {
					return true
				}
			}
		}
		return allowFlatNamedCredential && contextString(ctx, "apiKey") != ""
	case "auth.basic":
		_, _, ok := contextBasicAuthFor(ctx, requirement.Name)
		return ok && (contextNamedCredential(ctx, requirement.Name) != nil || allowFlatNamedCredential)
	case "auth.oauth2":
		named := contextNamedCredential(ctx, requirement.Name)
		if values, ok := named.(map[string]any); ok {
			if token, ok := values["accessToken"].(string); ok && token != "" {
				return true
			}
		}
		return allowFlatNamedCredential && (contextString(ctx, "accessToken") != "" || contextBearerToken(ctx) != "")
	case "config.value":
		point, _ := requirement.Extra["point"].(string)
		path, pathPresent := requirement.Extra["path"].(string)
		value, present := contextConfiguration(ctx)[point]
		if !present || !pathPresent {
			return false
		}
		selected, selectedPresent := configurationValueAt(value, path)
		if !selectedPresent || selected == nil || selected == "" {
			return false
		}
		// When the requirement carries an engine-asserted schema, presence is
		// not enough: the selected value must also validate against it.
		// Twin divergence by necessity: the openbindings-go SDK validates
		// against the full JSON Schema via its core validator; this repo has
		// no JSON Schema validator dependency of its own and does not add
		// one, so it enforces only the closed `enum` member (the one
		// constraint this engine itself asserts — every schema it emits is
		// enum-only or absent). A general non-enum schema is not enforced
		// here.
		if schemaRaw, schemaPresent := requirement.Extra["schema"]; schemaPresent {
			schema, ok := schemaRaw.(map[string]any)
			if !ok {
				return false
			}
			if enum, hasEnum := schema["enum"]; hasEnum {
				members, ok := enum.([]any)
				if !ok {
					return false
				}
				admitted := false
				for _, member := range members {
					if reflect.DeepEqual(member, selected) {
						admitted = true
						break
					}
				}
				if !admitted {
					return false
				}
			}
		}
		return true
	default:
		value, present := ctx[requirement.Type]
		return present && value != nil && value != ""
	}
}

func configurationValueAt(root any, path string) (any, bool) {
	if path == "" {
		return root, true
	}
	if !strings.HasPrefix(path, "/") {
		return nil, false
	}
	current := root
	for _, raw := range strings.Split(path[1:], "/") {
		for index := 0; index < len(raw); index++ {
			if raw[index] == '~' && (index+1 >= len(raw) || (raw[index+1] != '0' && raw[index+1] != '1')) {
				return nil, false
			}
			if raw[index] == '~' {
				index++
			}
		}
		token := strings.ReplaceAll(strings.ReplaceAll(raw, "~1", "/"), "~0", "~")
		record, ok := current.(map[string]any)
		if !ok {
			return nil, false
		}
		current, ok = record[token]
		if !ok {
			return nil, false
		}
	}
	return current, true
}
