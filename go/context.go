package asyncapiclient

func contextBearerToken(ctx map[string]any) string { return contextString(ctx, "bearerToken") }

func contextAPIKeyFor(ctx map[string]any, name string) string {
	if ctx == nil {
		return ""
	}
	if name != "" {
		if values, ok := ctx["apiKeys"].(map[string]any); ok {
			if value, ok := values[name].(string); ok && value != "" {
				return value
			}
		}
	}
	return contextString(ctx, "apiKey")
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
			if !contextSatisfiesRequirement(ctx, requirement) {
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

func contextSatisfiesRequirement(ctx map[string]any, requirement Requirement) bool {
	switch requirement.Type {
	case "auth.bearer":
		return contextBearerToken(ctx) != ""
	case "auth.apiKey":
		return contextAPIKeyFor(ctx, requirement.Name) != ""
	case "auth.basic":
		_, _, ok := contextBasicAuth(ctx)
		return ok
	case "auth.oauth2":
		return contextString(ctx, "accessToken") != "" || contextBearerToken(ctx) != ""
	case "config.value":
		point, _ := requirement.Extra["point"].(string)
		key, _ := requirement.Extra["key"].(string)
		value, present := contextConfiguration(ctx)[point]
		if !present {
			return false
		}
		if key == "" {
			return value != nil
		}
		if record, ok := value.(map[string]any); ok {
			candidate, present := record[key]
			return present && candidate != nil && candidate != ""
		}
		return key == point && value != nil && value != ""
	default:
		value, present := ctx[requirement.Type]
		return present && value != nil && value != ""
	}
}
