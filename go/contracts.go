package asyncapiclient

import (
	"context"
	"errors"
	"fmt"
	"net/http"
)

const DefaultMaxDeliveryUnitBytes int64 = 10 << 20

type Profile string

const (
	ProfileFull Profile = "asyncapi-2.0-3.1"
)

func normalizedProfile(profile Profile) Profile {
	if profile == "" {
		return ProfileFull
	}
	return profile
}

// Source identifies an AsyncAPI artifact without an OBI.
type Source struct {
	Location string
	Content  []byte
	Document *Document
}

type Requirement struct {
	Type        string
	Name        string
	Durable     *bool
	Description string
	Extra       map[string]any
}

type RequirementAlternative struct{ Requirements []Requirement }

type Prerequisites struct {
	Target       string
	Alternatives []RequirementAlternative
}

// newConfigValueRequirement builds a config.value requirement. schema is the
// engine-asserted JSON Schema for the value at (point, path) — artifact-
// derived where the artifact speaks, nil where it does not (absent =
// unconstrained); an `enum` member is a closed admissible set.
func newConfigValueRequirement(point, path, description string, schema map[string]any, durable *bool) Requirement {
	extra := map[string]any{"point": point, "path": path}
	if schema != nil {
		extra["schema"] = schema
	}
	return Requirement{Type: "config.value", Description: description, Durable: durable, Extra: extra}
}

func newContextRequiredError(message string, details *Prerequisites) *ExecutionError {
	return &ExecutionError{Code: ErrCodeContextRequired, Message: message, Details: details, DetailsPresent: true}
}

type Metadata map[string][]string

type HookSite struct {
	Operation string
	Ref       string
	Target    string
	Profile   Profile
}

type invokeSite struct {
	Operation  string
	InvokedAs  string
	BindingKey string
	Profile    string
	Ref        string
	Target     string
}

type RawResult struct {
	Status *int
	Body   []byte
	Meta   Metadata
}

var ErrUseDefault = errors.New("asyncapi-client: use default")

type outputDecoder func(invokeSite, RawResult) (any, error)

// Hooks are AsyncAPI-native customization points. handled=false declines to
// the artifact runtime's built-in codec.
type Hooks struct {
	Decode func(HookSite, RawResult) (value any, handled bool, err error)
	// Encode is the input-side codec seam (§9.2's byte rule enrichment,
	// ruled 2026-08-13): a consumer codec keyed on the site's declared
	// content type may serialize the application value to the exact wire
	// octets — an Avro codec turning logical values into Avro bytes, for
	// example. handled=false declines to the built-in lane (JSON, text, or
	// the canonical Base64 byte boundary).
	Encode func(HookSite, any) (payload []byte, handled bool, err error)
}

type invokeHooks struct {
	profile Profile
	hooks   *Hooks
	decided string
}

func (h *invokeHooks) DecodeOutput(site invokeSite, raw RawResult, builtin outputDecoder) (any, error) {
	if h != nil && h.hooks != nil && h.hooks.Decode != nil {
		value, handled, err := h.hooks.Decode(HookSite{Operation: site.Operation, Ref: site.Ref, Target: site.Target, Profile: h.profile}, raw)
		if err != nil {
			return nil, asExecutionError(err)
		}
		if handled {
			h.decided = "hook"
			return value, nil
		}
	}
	if h != nil {
		h.decided = "builtin"
	}
	if builtin == nil {
		return nil, &ExecutionError{Code: ErrCodeRuntime, Message: "AsyncAPI execution has no output decoder"}
	}
	return builtin(site, raw)
}

// EncodeInput consults the consumer Encode codec before the built-in lane:
// a handled result's bytes are the exact wire payload; declining falls to
// the resolved built-in codec (JSON, text, or the Base64 byte boundary).
func (h *invokeHooks) EncodeInput(site invokeSite, value any, builtin func(any) ([]byte, error)) ([]byte, error) {
	if h != nil && h.hooks != nil && h.hooks.Encode != nil {
		payload, handled, err := h.hooks.Encode(HookSite{Operation: site.Operation, Ref: site.Ref, Target: site.Target, Profile: h.profile}, value)
		if err != nil {
			return nil, asExecutionError(err)
		}
		if handled {
			return payload, nil
		}
	}
	return builtin(value)
}

func (h *invokeHooks) DecodeDecidedBy() string {
	if h == nil {
		return "builtin"
	}
	return h.decided
}

type PrepareOptions struct {
	Source               Source
	Ref                  string
	Profile              Profile
	Context              map[string]any
	HTTPClient           *http.Client
	Hooks                *Hooks
	MaxDeliveryUnitBytes int64
	AcceptsInput         *bool
}

type invocationSource struct {
	Profile  string
	Location string
	Content  []byte
}

type executionArgs struct {
	Source               invocationSource
	Ref                  string
	Context              map[string]any
	Hooks                *invokeHooks
	Site                 *invokeSite
	MaxDeliveryUnitBytes int64
	AcceptsInput         *bool
	ProtocolDrivers      map[string]ProtocolDriver
}

func (a *executionArgs) DeliveryUnitLimit() int64 {
	if a != nil && a.MaxDeliveryUnitBytes > 0 {
		return a.MaxDeliveryUnitBytes
	}
	return DefaultMaxDeliveryUnitBytes
}

type Event struct {
	Value    any
	Metadata Metadata
}

type Diagnostics struct {
	Leading  Metadata
	Trailing Metadata
}

type artifactHandle[I, O any] interface {
	ReadInput(context.Context) (I, error)
	CloseInput() error
	EmitOutput(O) error
	CloseOutput()
	FireError(*ExecutionError)
	Done() <-chan struct{}
	SetHeader(Metadata) error
	SetTrailer(Metadata)
}

func newDefaultHTTPClient() *http.Client {
	return &http.Client{CheckRedirect: func(_ *http.Request, _ []*http.Request) error { return http.ErrUseLastResponse }}
}

func toStringAnyMap(value any) (map[string]any, bool) {
	result, ok := value.(map[string]any)
	return result, ok
}

func cloneMetadata(value Metadata) Metadata {
	out := make(Metadata, len(value))
	for name, values := range value {
		out[name] = append([]string(nil), values...)
	}
	return out
}

func clonePrerequisites(value *Prerequisites) *Prerequisites {
	if value == nil {
		return nil
	}
	out := &Prerequisites{Target: value.Target, Alternatives: make([]RequirementAlternative, len(value.Alternatives))}
	for i, alternative := range value.Alternatives {
		out.Alternatives[i].Requirements = append([]Requirement(nil), alternative.Requirements...)
	}
	return out
}

func errorMessage(value any) string {
	if value == nil {
		return ""
	}
	if err, ok := value.(error); ok {
		return err.Error()
	}
	return fmt.Sprint(value)
}
