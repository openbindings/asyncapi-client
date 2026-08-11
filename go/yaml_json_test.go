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
