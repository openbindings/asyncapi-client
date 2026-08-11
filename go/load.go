package asyncapiclient

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
)

func loadDocument(ctx context.Context, client *http.Client, location string, content []byte) (*document, error) {
	data, err := sourceToBytes(ctx, client, location, content)
	if err != nil {
		return nil, err
	}
	if err := discriminateDocument(data); err != nil {
		return nil, err
	}
	data, err = resolveArtifactReferences(ctx, client, location, data)
	if err != nil {
		return nil, err
	}
	return parseDocument(data)
}

func parseDocument(data []byte) (*document, error) {
	normalized, err := NormalizeDocument(data)
	if err != nil {
		return nil, err
	}
	var doc document
	if err := json.Unmarshal(normalized, &doc); err != nil {
		return nil, fmt.Errorf("parse normalized AsyncAPI document: %w", err)
	}
	doc.raw = append([]byte(nil), normalized...)
	resolveRefs(&doc)
	return &doc, nil
}

// NormalizeDocument validates the artifact envelope and applies AsyncAPI
// 3.0's trait merge mechanism, returning a JSON spelling of the normalized
// document. It is useful to artifact consumers that need their own AST while
// sharing the client's upstream-spec interpretation.
func NormalizeDocument(data []byte) ([]byte, error) {
	var envelope map[string]any
	data = trimUTF8BOM(data)
	if isJSON(data) {
		if err := json.Unmarshal(data, &envelope); err != nil {
			return nil, fmt.Errorf("parse AsyncAPI document: %w", err)
		}
	} else {
		var err error
		envelope, err = decodeYAMLObject(data)
		if err != nil {
			return nil, fmt.Errorf("parse AsyncAPI document: %w", err)
		}
	}
	envelope, err := normalizeEdition(envelope)
	if err != nil {
		return nil, err
	}
	infoValue, hasInfo := envelope["info"]
	infoObject, infoIsObject := infoValue.(map[string]any)
	_, hasTitle := infoObject["title"].(string)
	_, hasVersion := infoObject["version"].(string)
	if !hasInfo || !infoIsObject || !hasTitle || !hasVersion {
		return nil, fmt.Errorf("not a valid AsyncAPI document (info.title and info.version are required strings)")
	}
	applyDocumentTraits(envelope)
	normalized, err := json.Marshal(envelope)
	if err != nil {
		return nil, fmt.Errorf("normalize AsyncAPI document: %w", err)
	}
	return normalized, nil
}

func discriminateDocument(data []byte) error {
	var envelope map[string]any
	data = trimUTF8BOM(data)
	if isJSON(data) {
		if err := json.Unmarshal(data, &envelope); err != nil {
			return fmt.Errorf("parse AsyncAPI document: %w", err)
		}
	} else {
		var err error
		envelope, err = decodeYAMLObject(data)
		if err != nil {
			return fmt.Errorf("parse AsyncAPI document: %w", err)
		}
	}
	return discriminateEnvelope(envelope)
}

func discriminateEnvelope(envelope map[string]any) error {
	value, ok := envelope["asyncapi"]
	if !ok {
		return fmt.Errorf("not a valid AsyncAPI document (missing 'asyncapi' field)")
	}
	version, ok := value.(string)
	if !ok || !supportedAsyncAPIEditions[version] {
		return fmt.Errorf("unsupported AsyncAPI version %v: this client accepts exactly 2.0.0–2.6.0, 3.0.0, and 3.1.0", value)
	}
	return nil
}

func validateDocumentAddress(location string) error {
	u, err := url.Parse(location)
	if err != nil || u.Scheme == "" || u.Opaque != "" {
		return fmt.Errorf("AsyncAPI location %q is not an absolute URI; use file:// for local artifacts", location)
	}
	return nil
}

func sourceToBytes(ctx context.Context, client *http.Client, location string, content []byte) ([]byte, error) {
	if content != nil {
		return append([]byte(nil), content...), nil
	}
	if location == "" {
		return nil, fmt.Errorf("source must have location, content, or a parsed document")
	}
	if err := validateDocumentAddress(location); err != nil {
		return nil, err
	}
	u, _ := url.Parse(location)
	switch u.Scheme {
	case "http", "https":
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, location, nil)
		if err != nil {
			return nil, fmt.Errorf("fetch %q: %w", location, err)
		}
		response, err := client.Do(req)
		if err != nil {
			return nil, fmt.Errorf("fetch %q: %w", location, err)
		}
		defer func() { _ = response.Body.Close() }()
		if response.StatusCode >= 400 {
			return nil, fmt.Errorf("fetch %q: HTTP %d", location, response.StatusCode)
		}
		return io.ReadAll(io.LimitReader(response.Body, 10<<20))
	case "file":
		return os.ReadFile(u.Path)
	default:
		return nil, fmt.Errorf("AsyncAPI location scheme %q is unsupported (supported: file, http, https)", u.Scheme)
	}
}

func isJSON(data []byte) bool {
	data = trimUTF8BOM(data)
	for _, value := range data {
		switch value {
		case ' ', '\t', '\n', '\r':
			continue
		case '{', '[':
			return true
		default:
			return false
		}
	}
	return false
}

func trimUTF8BOM(data []byte) []byte {
	if len(data) >= 3 && data[0] == 0xef && data[1] == 0xbb && data[2] == 0xbf {
		return data[3:]
	}
	return data
}

func extractRefName(ref string) string {
	path := strings.TrimPrefix(ref, "#/")
	parts := strings.Split(path, "/")
	if len(parts) == 0 {
		return ""
	}
	return unescapeRefToken(parts[len(parts)-1])
}

func unescapeRefToken(value string) string {
	return strings.ReplaceAll(strings.ReplaceAll(value, "~1", "/"), "~0", "~")
}

func resolveMessageRef(doc *document, ref messageRef) *message {
	return resolveMessageRefSeen(doc, ref, map[string]bool{})
}

func resolveMessageRefSeen(doc *document, ref messageRef, seen map[string]bool) *message {
	if ref.Ref == "" {
		return nil
	}
	if seen[ref.Ref] {
		return nil
	}
	seen[ref.Ref] = true
	parts := strings.Split(strings.TrimPrefix(ref.Ref, "#/"), "/")
	if len(parts) == 3 && parts[0] == "components" && parts[1] == "messages" && doc.Components != nil {
		if value, ok := doc.Components.Messages[unescapeRefToken(parts[2])]; ok {
			if value.Ref != "" {
				return resolveMessageRefSeen(doc, messageRef{Ref: value.Ref}, seen)
			}
			return &value
		}
	}
	if len(parts) == 4 && parts[0] == "channels" && parts[2] == "messages" {
		if channel, ok := doc.Channels[unescapeRefToken(parts[1])]; ok {
			if value, ok := channel.Messages[unescapeRefToken(parts[3])]; ok {
				if value.Ref != "" {
					return resolveMessageRefSeen(doc, messageRef{Ref: value.Ref}, seen)
				}
				return &value
			}
		}
	}
	return nil
}
