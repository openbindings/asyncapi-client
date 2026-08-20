package asyncapiclient

import (
	"testing"
)

// config.value schema (2026-08-20 working-draft amendment): the configRequired
// signal carries an engine-asserted JSON Schema where the artifact declares a
// closed value set ({"enum": […]}), absent otherwise; `choices` is removed.

func TestEnumSchema(t *testing.T) {
	if enumSchema(nil) != nil {
		t.Error("no declared values assert nothing (nil = absent)")
	}
	schema := enumSchema([]string{"prod", "staging"})
	members, _ := schema["enum"].([]any)
	if len(members) != 2 || members[0] != "prod" || members[1] != "staging" {
		t.Errorf("enumSchema = %v, want {\"enum\": [prod staging]}", schema)
	}
}

func TestResolveTarget_SeveralServersChallengeCarriesEnumSchema(t *testing.T) {
	doc := &document{Servers: map[string]server{
		"eu":   {Host: "eu.example.com", Protocol: "wss"},
		"us":   {Host: "us.example.com", Protocol: "wss"},
		"mqtt": {Host: "q.example.com", Protocol: "mqtt"},
	}}
	_, err := resolveTarget(doc, nil, nil)
	cr, ok := err.(*configRequired)
	if !ok {
		t.Fatalf("expected *configRequired, got %v", err)
	}
	if cr.point != "server" || cr.path != "/key" {
		t.Fatalf("challenge = {point:%q path:%q}, want {server /key}", cr.point, cr.path)
	}
	members, _ := cr.schema["enum"].([]any)
	if len(members) != 3 {
		t.Fatalf("schema = %v, want an enum of the three bindable member keys", cr.schema)
	}
}

func TestResolveTarget_UndefaultedVariableChallengeCarriesEnumSchema(t *testing.T) {
	doc := &document{Servers: map[string]server{
		"prod": {Host: "{env}.example.com", Protocol: "wss", Variables: map[string]serverVariable{
			"env": {Enum: []string{"eu", "us"}},
		}},
	}}
	_, err := resolveTarget(doc, nil, nil)
	cr, ok := err.(*configRequired)
	if !ok {
		t.Fatalf("expected *configRequired, got %v", err)
	}
	if cr.point != "server" || cr.path != "/variables/env" {
		t.Fatalf("challenge = {point:%q path:%q}, want {server /variables/env}", cr.point, cr.path)
	}
	members, _ := cr.schema["enum"].([]any)
	if len(members) != 2 || members[0] != "eu" || members[1] != "us" {
		t.Errorf("schema = %v, want the declared enum", cr.schema)
	}
	if cr.hostHint != "{env}.example.com" {
		t.Errorf("hostHint = %q, want the artifact host template", cr.hostHint)
	}
}

// Stage 0 scope assertion (context-scope model, ratified 2026-08-19): the
// challenge target falls back resolved server URL → artifact host hint →
// threaded source location (verbatim — this client has no location
// canonicalizer, and the loader admits only an absolute URI there); a
// content-only source asserts nothing.
func TestConfigOrSourceError_TargetFallbackChain(t *testing.T) {
	base := configRequired{point: "server", path: "/key", description: "select a member"}

	withHint := base
	withHint.hostHint = "broker.example.com"
	if got := challengeTarget(t, configOrSourceError(&withHint, "wss://resolved.example.com", "https://example.com/spec.yaml")); got != "wss://resolved.example.com" {
		t.Errorf("target = %q, want the resolved server URL to win", got)
	}
	if got := challengeTarget(t, configOrSourceError(&withHint, "", "https://example.com/spec.yaml")); got != "broker.example.com" {
		t.Errorf("target = %q, want the artifact host hint next", got)
	}
	noHint := base
	if got := challengeTarget(t, configOrSourceError(&noHint, "", "https://example.com/specs/a.yaml")); got != "https://example.com/specs/a.yaml" {
		t.Errorf("target = %q, want the threaded source location verbatim", got)
	}
	if got := challengeTarget(t, configOrSourceError(&noHint, "", "")); got != "" {
		t.Errorf("target = %q, want empty for a content-only source (asserts nothing)", got)
	}
}

func TestConfigOrSourceError_RequirementCarriesSchema(t *testing.T) {
	durable := true
	cr := &configRequired{
		point: "server", path: "/key", description: "select a member",
		schema:  map[string]any{"enum": []any{"eu", "us"}},
		durable: &durable,
	}
	details := challengeDetails(t, configOrSourceError(cr, "", ""))
	req := details.Alternatives[0].Requirements[0]
	schema, _ := req.Extra["schema"].(map[string]any)
	if members, _ := schema["enum"].([]any); len(members) != 2 {
		t.Errorf("requirement schema = %v, want the signal's enum schema", req.Extra["schema"])
	}
	if _, present := req.Extra["choices"]; present {
		t.Error("choices is removed from the contract; nothing may emit it")
	}
}

func TestConfigValueSatisfactionEnforcesEnumSchema(t *testing.T) {
	challenge := func(schema map[string]any) *Prerequisites {
		extra := map[string]any{"point": "server", "path": "/key"}
		if schema != nil {
			extra["schema"] = schema
		}
		return &Prerequisites{
			Target: "https://example.com/spec.yaml",
			Alternatives: []RequirementAlternative{{Requirements: []Requirement{
				{Type: "config.value", Extra: extra},
			}}},
		}
	}
	stored := func(value any) map[string]any {
		return map[string]any{"configuration": map[string]any{
			"server": map[string]any{"key": value},
		}}
	}

	enum := map[string]any{"enum": []any{"eu", "us"}}
	if !contextSatisfies(stored("eu"), challenge(enum)) {
		t.Error("a value inside the closed enum satisfies")
	}
	if contextSatisfies(stored("apac"), challenge(enum)) {
		t.Error("a value outside the closed enum must not satisfy")
	}
	if !contextSatisfies(stored("apac"), challenge(nil)) {
		t.Error("without a schema, presence of a non-empty value satisfies (unconstrained)")
	}
	// Twin divergence by necessity (see contextSatisfiesRequirement): a
	// non-enum schema member is carried but not enforced here — presence
	// still satisfies.
	if !contextSatisfies(stored("anything"), challenge(map[string]any{"type": "string"})) {
		t.Error("a non-enum schema is not enforced by this engine's satisfaction check")
	}
}

func challengeDetails(t *testing.T, err *ExecutionError) *Prerequisites {
	t.Helper()
	if err == nil || err.Code != ErrCodeContextRequired {
		t.Fatalf("expected a CONTEXT_REQUIRED challenge, got %v", err)
	}
	details, ok := err.Details.(*Prerequisites)
	if !ok {
		t.Fatalf("details = %#v, want *Prerequisites", err.Details)
	}
	if len(details.Alternatives) != 1 || len(details.Alternatives[0].Requirements) != 1 {
		t.Fatalf("expected one alternative with one requirement, got %+v", details.Alternatives)
	}
	return details
}

func challengeTarget(t *testing.T, err *ExecutionError) string {
	t.Helper()
	return challengeDetails(t, err).Target
}
