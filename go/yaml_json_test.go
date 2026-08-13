package asyncapiclient

import (
	"strings"
	"testing"
)

func TestDecodeYAMLAllowsTabAsFirstBlockScalarContentCharacter(t *testing.T) {
	document, err := decodeYAMLObject([]byte("description: >\n  \tInformation about the last close.\nnext: value\n"))
	if err != nil {
		t.Fatalf("decodeYAMLObject: %v", err)
	}
	if got, want := document["description"], "\tInformation about the last close.\n"; got != want {
		t.Fatalf("description = %#v, want %#v", got, want)
	}
}

func TestDecodeYAMLStillRejectsTabIndentation(t *testing.T) {
	_, err := decodeYAMLObject([]byte("root:\n\tchild: value\n"))
	if err == nil {
		t.Fatal("decodeYAMLObject accepted a tab used as indentation")
	}
}

func TestDecodeYAMLPreservesTabOnlyBlockScalarLine(t *testing.T) {
	document, err := decodeYAMLObject([]byte("description: |\n  \t\n  next\n"))
	if err != nil {
		t.Fatalf("decodeYAMLObject: %v", err)
	}
	if got, want := document["description"], "\t\nnext\n"; got != want {
		t.Fatalf("description = %#v, want %#v", got, want)
	}
}

func TestDecodeYAMLPreservesSentinelLikeContent(t *testing.T) {
	literal := "__OPENBINDINGS_YAML_BLOCK_TAB__"
	document, err := decodeYAMLObject([]byte("description: |\n  \t" + literal + "\n"))
	if err != nil {
		t.Fatalf("decodeYAMLObject: %v", err)
	}
	if got := document["description"]; got != "\t"+literal+"\n" {
		t.Fatalf("description = %#v", got)
	}
	if strings.Contains(document["description"].(string), literal+"_") {
		t.Fatalf("collision sentinel leaked into description: %#v", document["description"])
	}
}

func TestDecodeYAMLKeepsDateLikeScalarsVerbatim(t *testing.T) {
	// AsyncAPI types info.version as a string. yaml.v3 implicitly resolves an
	// unquoted date to !!timestamp, and formatting the resolved time rewrites the
	// authored value, which also disagrees with the TypeScript runtime.
	for _, testCase := range []struct {
		name     string
		document string
		want     string
	}{
		{"date", "version: 2026-03-01\n", "2026-03-01"},
		{"date time", "version: 2026-03-01T12:30:00Z\n", "2026-03-01T12:30:00Z"},
		{"spaced date time", "version: 2026-03-01 12:30:00\n", "2026-03-01 12:30:00"},
		{"quoted date", "version: \"2026-03-01\"\n", "2026-03-01"},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			document, err := decodeYAMLObject([]byte(testCase.document))
			if err != nil {
				t.Fatalf("decodeYAMLObject: %v", err)
			}
			if got := document["version"]; got != testCase.want {
				t.Fatalf("version = %#v, want %#v", got, testCase.want)
			}
		})
	}
}

func TestDecodeYAMLKeepsDateLikeScalarsVerbatimUnderTabProtection(t *testing.T) {
	// The block-scalar tab retry re-decodes protected bytes; timestamps must stay
	// verbatim on that path too.
	document, err := decodeYAMLObject([]byte("version: 2026-03-01\ndescription: >\n  \tnote\n"))
	if err != nil {
		t.Fatalf("decodeYAMLObject: %v", err)
	}
	if got, want := document["version"], "2026-03-01"; got != want {
		t.Fatalf("version = %#v, want %#v", got, want)
	}
	if got, want := document["description"], "\tnote\n"; got != want {
		t.Fatalf("description = %#v, want %#v", got, want)
	}
}

func TestDraft07PlainNameIDGrammar(t *testing.T) {
	for _, valid := range []string{"#A", "#MassCancelRequest", "#a-b_c:d.e9"} {
		if !isDraft07PlainNameID(valid) {
			t.Errorf("isDraft07PlainNameID(%q) = false", valid)
		}
	}
	for _, invalid := range []string{"#", "#9startsWithDigit", "#/pointer", "other.json#name", "#space here"} {
		if isDraft07PlainNameID(invalid) {
			t.Errorf("isDraft07PlainNameID(%q) = true", invalid)
		}
	}
}
