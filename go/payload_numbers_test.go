package asyncapiclient_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	client "github.com/openbindings/asyncapi-client/go"
)

type payloadTransport func(*http.Request) (*http.Response, error)

func (f payloadTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func payloadArtifact(ct string) []byte {
	return []byte(fmt.Sprintf(`{"asyncapi":"3.0.0","info":{"title":"P03","version":"1"},"defaultContentType":%q,"servers":{"s":{"host":"payload.test","protocol":"https"}},"channels":{"c":{"address":"/payload","messages":{"In":{"payload":{}},"Out":{"payload":{}}}}},"operations":{"send":{"action":"receive","channel":{"$ref":"#/channels/c"},"messages":[{"$ref":"#/channels/c/messages/In"}],"bindings":{"http":{"method":"POST"}},"reply":{"messages":[{"$ref":"#/channels/c/messages/Out"}]}}}}`, ct))
}

func TestP03NativePayloadNumbers(t *testing.T) {
	for _, body := range []string{"9007199254740993", "-9007199254740993", "1e400", "1e-400", "0.12345678901234567890123456789", "42", "-0", `{"a":[1e400,1e-400],"n":9007199254740993}`, "null", "true", `"text"`} {
		for _, ct := range []string{"application/json", "application/problem+json"} {
			for _, mode := range []string{"none", "decline", "handle"} {
				t.Run(ct+"/"+mode+"/"+body, func(t *testing.T) {
					ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
					defer cancel()
					var seen string
					calls := 0
					transport := &http.Client{Transport: payloadTransport(func(r *http.Request) (*http.Response, error) {
						b, err := io.ReadAll(r.Body)
						if err != nil {
							return nil, err
						}
						seen = string(b)
						return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": {ct}}, Body: io.NopCloser(strings.NewReader(body)), Request: r}, nil
					})}
					var hooks *client.Hooks
					if mode != "none" {
						hooks = &client.Hooks{Decode: func(client.HookSite, client.RawResult) (any, bool, error) {
							calls++
							if mode == "handle" {
								return "override", true, nil
							}
							return nil, false, nil
						}}
					}
					loaded, err := client.Load(ctx, client.Source{Content: payloadArtifact(ct)}, client.LoadOptions{HTTPClient: transport, Hooks: hooks})
					if err != nil {
						t.Fatal(err)
					}
					defer loaded.Close()
					inputToken := "9007199254740993"
					if body[0] == '-' || body[0] >= '0' && body[0] <= '9' {
						inputToken = body
					}
					events, err := loaded.Publish(ctx, "send", json.Number(inputToken), client.InvocationOptions{})
					if err != nil {
						t.Fatalf("P03_EXACT: valid response %s refused: %v", body, err)
					}
					if seen != inputToken {
						t.Fatalf("P03_OUTBOUND: %q", seen)
					}
					if len(events) != 1 {
						t.Fatalf("P03_EVENTS: %#v", events)
					}
					want := body
					if mode == "handle" {
						want = `"override"`
					}
					encoded, err := json.Marshal(events[0].Value)
					if err != nil || string(encoded) != want {
						t.Fatalf("P03_EXACT: %s became %s (%T): %v", body, encoded, events[0].Value, err)
					}
					if mode != "none" && calls != 1 {
						t.Fatalf("hook calls=%d", calls)
					}
				})
			}
		}
	}
}

func TestP03NativePayloadRefusalAndLanes(t *testing.T) {
	for _, tc := range []struct {
		ct, body, want string
		invalid, empty bool
	}{
		{"application/json", "1 2", "", true, false}, {"application/json", "1x", "", true, false},
		{"application/json", "1e", "", true, false}, {"application/json", "[1,]", "", true, false},
		{"application/json", "NaN", "", true, false}, {"application/json", "Infinity", "", true, false},
		{"application/json", " ", "", true, false}, {"application/json", "", "", false, true},
		{"text/plain", "1e400", `"1e400"`, false, false}, {"application/octet-stream", "abc", `"YWJj"`, false, false},
		{"application/json", `"\ud800"`, `"�"`, false, false}, {"application/json", `{"x":1,"x":2}`, `{"x":2}`, false, false},
	} {
		t.Run(tc.ct+"/"+tc.body, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			httpClient := &http.Client{Transport: payloadTransport(func(r *http.Request) (*http.Response, error) {
				return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": {tc.ct}}, Body: io.NopCloser(strings.NewReader(tc.body)), Request: r}, nil
			})}
			loaded, err := client.Load(ctx, client.Source{Content: payloadArtifact(tc.ct)}, client.LoadOptions{HTTPClient: httpClient})
			if err != nil {
				t.Fatal(err)
			}
			defer loaded.Close()
			input := any("YWJj")
			if tc.ct == "application/json" {
				input = nil
			}
			events, err := loaded.Publish(ctx, "send", input, client.InvocationOptions{})
			if tc.invalid {
				var failure *client.ExecutionError
				if !errors.As(err, &failure) || failure.Code != client.ErrCodeResponseError {
					t.Fatalf("P03_INVALID: %v", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if tc.empty {
				if len(events) != 0 {
					t.Fatalf("empty unit emitted %#v", events)
				}
				return
			}
			if len(events) != 1 {
				t.Fatalf("events=%#v", events)
			}
			encoded, err := json.Marshal(events[0].Value)
			if err != nil || string(encoded) != tc.want {
				t.Fatalf("lane=%s, %v", encoded, err)
			}
		})
	}
}
