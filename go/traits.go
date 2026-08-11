package asyncapiclient

const unresolvedTraitField = "x-ob-asyncapi-unresolved-trait"

// applyDocumentTraits implements AsyncAPI 3.0's Traits Merge Mechanism on
// operation and message objects. Traits apply in declaration order using
// JSON Merge Patch, then the target applies last so it always wins.
func applyDocumentTraits(root map[string]any) {
	if operations, ok := objectMap(root["operations"]); ok {
		for name, raw := range operations {
			if operation, ok := objectMap(raw); ok {
				if resolved, ok := resolveTraitTarget(operation, root, nil); ok {
					operations[name] = applyTraits(resolved, root)
				}
			}
		}
	}
	visitMessageMap(root, root["channels"])
	if components, ok := objectMap(root["components"]); ok {
		visitMessages(root, components["messages"])
	}
}

func visitMessageMap(root map[string]any, raw any) {
	channels, ok := objectMap(raw)
	if !ok {
		return
	}
	for name, value := range channels {
		channel, ok := objectMap(value)
		if !ok {
			continue
		}
		if resolved, ok := resolveTraitTarget(channel, root, nil); ok {
			visitMessages(root, resolved["messages"])
			channels[name] = resolved
		}
	}
}

func visitMessages(root map[string]any, raw any) {
	messages, ok := objectMap(raw)
	if !ok {
		return
	}
	for name, value := range messages {
		message, ok := objectMap(value)
		if !ok {
			continue
		}
		if resolved, ok := resolveTraitTarget(message, root, nil); ok {
			messages[name] = applyTraits(resolved, root)
		}
	}
}

func applyTraits(target map[string]any, root map[string]any) map[string]any {
	declared, ok := target["traits"].([]any)
	if !ok {
		return target
	}
	var inherited any = map[string]any{}
	var unresolved string
	for _, raw := range declared {
		trait, ok := objectMap(raw)
		if !ok {
			continue
		}
		resolved, ok := resolveTraitTarget(trait, root, nil)
		if !ok {
			if ref, refOK := trait["$ref"].(string); refOK && unresolved == "" {
				unresolved = ref
			}
			continue
		}
		inherited = mergePatch(inherited, resolved)
	}
	own := cloneMap(target)
	delete(own, "traits")
	merged := mergePatch(inherited, own).(map[string]any)
	if unresolved != "" {
		merged[unresolvedTraitField] = unresolved
	}
	return merged
}

func resolveTraitTarget(value map[string]any, root map[string]any, visited map[string]bool) (map[string]any, bool) {
	ref, hasRef := value["$ref"].(string)
	if !hasRef {
		return cloneMap(value), true
	}
	if visited == nil {
		visited = map[string]bool{}
	}
	if visited[ref] {
		return nil, false
	}
	visited[ref] = true
	target, ok := objectMap(resolveJSONPointer(root, ref))
	if !ok {
		return nil, false
	}
	return resolveTraitTarget(target, root, visited)
}

func mergePatch(target, patch any) any {
	patchObject, ok := objectMap(patch)
	if !ok {
		return cloneValue(patch)
	}
	result := map[string]any{}
	if targetObject, ok := objectMap(target); ok {
		result = cloneMap(targetObject)
	}
	for key, value := range patchObject {
		if value == nil {
			delete(result, key)
			continue
		}
		result[key] = mergePatch(result[key], value)
	}
	return result
}

func objectMap(value any) (map[string]any, bool) {
	result, ok := value.(map[string]any)
	return result, ok
}

func cloneMap(value map[string]any) map[string]any {
	result := make(map[string]any, len(value))
	for key, member := range value {
		result[key] = cloneValue(member)
	}
	return result
}

func cloneValue(value any) any {
	switch typed := value.(type) {
	case map[string]any:
		return cloneMap(typed)
	case []any:
		result := make([]any, len(typed))
		for index, member := range typed {
			result[index] = cloneValue(member)
		}
		return result
	default:
		return value
	}
}
