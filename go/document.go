package asyncapiclient

import (
	"sort"
	"strings"
)

// Document is an opaque, parsed AsyncAPI 3.0 document. It exposes artifact-
// native inventory and resolution without leaking the client's internal AST.
type Document struct{ doc *document }

func ParseDocument(data []byte) (*Document, error) {
	doc, err := parseDocument(data)
	if err != nil {
		return nil, err
	}
	return &Document{doc: doc}, nil
}

type Operation struct {
	ID          string
	Ref         string
	Action      string
	Summary     string
	Description string
	Channel     string
}

func (d *Document) Operations() []Operation {
	if d == nil || d.doc == nil {
		return nil
	}
	ids := sortedKeys(d.doc.Operations)
	out := make([]Operation, 0, len(ids))
	for _, id := range ids {
		op := d.doc.Operations[id]
		out = append(out, Operation{
			ID: id, Ref: operationRef(id), Action: op.Action,
			Summary: op.Summary, Description: op.Description, Channel: extractRefName(op.Channel.Ref),
		})
	}
	return out
}

func sortedKeys[V any](values map[string]V) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func escapeRefToken(value string) string {
	return strings.ReplaceAll(strings.ReplaceAll(value, "~", "~0"), "/", "~1")
}
