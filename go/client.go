package asyncapiclient

import (
	"context"
	"fmt"
	"net/http"
	"strings"
)

type LoadOptions struct {
	HTTPClient *http.Client
	Profile    Profile
	Context    map[string]any
	Hooks      *Hooks
}

type InvocationOptions struct {
	Context              map[string]any
	Hooks                *Hooks
	MaxDeliveryUnitBytes int64
	AcceptsInput         *bool
}

// Client is a loaded AsyncAPI artifact with a small, artifact-native API.
// It does not require, construct, or expose an OBI.
type Client struct {
	engine   *Engine
	source   Source
	document *Document
	options  LoadOptions
}

func Load(ctx context.Context, source Source, options LoadOptions) (*Client, error) {
	engine := NewEngine(options.HTTPClient)
	doc, err := engine.load(ctx, source, options.HTTPClient)
	if err != nil {
		_ = engine.Close()
		return nil, err
	}
	return &Client{engine: engine, source: Source{Location: source.Location, Document: &Document{doc: doc}}, document: &Document{doc: doc}, options: options}, nil
}

func (c *Client) Close() error { return c.engine.Close() }

func (c *Client) Operations() []Operation { return c.document.Operations() }

func (c *Client) Start(ctx context.Context, operation string, options InvocationOptions) (*Execution, error) {
	ref := operation
	if !strings.HasPrefix(ref, "#/") {
		ref = "#/operations/" + escapeRefToken(operation)
	}
	contextValue := options.Context
	if contextValue == nil {
		contextValue = c.options.Context
	}
	hooks := options.Hooks
	if hooks == nil {
		hooks = c.options.Hooks
	}
	prepared, err := c.engine.Prepare(ctx, PrepareOptions{
		Source: c.source, Ref: ref, Profile: c.options.Profile, Context: contextValue,
		HTTPClient: c.options.HTTPClient, Hooks: hooks,
		MaxDeliveryUnitBytes: options.MaxDeliveryUnitBytes, AcceptsInput: options.AcceptsInput,
	})
	if err != nil {
		return nil, err
	}
	return prepared.Start(ctx)
}

func (c *Client) Publish(ctx context.Context, operation string, value any, options InvocationOptions) ([]Event, error) {
	execution, err := c.Start(ctx, operation, options)
	if err != nil {
		return nil, err
	}
	if !execution.InputRequested() {
		execution.Cancel()
		return nil, fmt.Errorf("AsyncAPI operation %q does not accept published input", operation)
	}
	if err := execution.Send(ctx, value); err != nil {
		return nil, err
	}
	_ = execution.FinishInput()
	var events []Event
	for event := range execution.Events() {
		events = append(events, event)
	}
	return events, execution.Wait()
}

func (c *Client) Subscribe(ctx context.Context, operation string, options InvocationOptions) (*Execution, error) {
	execution, err := c.Start(ctx, operation, options)
	if err != nil {
		return nil, err
	}
	if execution.InputRequested() {
		execution.Cancel()
		return nil, fmt.Errorf("AsyncAPI operation %q is a publish operation", operation)
	}
	return execution, nil
}
