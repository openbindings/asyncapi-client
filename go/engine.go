package asyncapiclient

import (
	"context"
	"fmt"
	"net/http"
	"sync"
)

// Engine owns artifact caching and reusable WebSocket sessions. It has no
// dependency on OpenBindings and can be used directly by AsyncAPI consumers.
type Engine struct {
	client *http.Client
	pool   *wsPool
	mu     sync.RWMutex
	cache  map[string]*document
}

func NewEngine(client *http.Client) *Engine {
	if client == nil {
		client = newDefaultHTTPClient()
	}
	return &Engine{client: client, pool: newWSPool(client), cache: map[string]*document{}}
}

func (e *Engine) Close() error {
	e.pool.closeAll()
	return nil
}

type PreparedOperation struct {
	document      *document
	options       PrepareOptions
	prerequisites *Prerequisites
	inputRequired bool
	client        *http.Client
	pool          *wsPool
}

func (p *PreparedOperation) Ref() string      { return p.options.Ref }
func (p *PreparedOperation) Profile() Profile { return p.options.Profile }
func (p *PreparedOperation) Prerequisites() *Prerequisites {
	return clonePrerequisites(p.prerequisites)
}
func (p *PreparedOperation) InputRequired() bool { return p.inputRequired }

func (e *Engine) Prepare(ctx context.Context, options PrepareOptions) (*PreparedOperation, error) {
	options.Profile = normalizedProfile(options.Profile)
	doc, err := e.load(ctx, options.Source, options.HTTPClient)
	if err != nil {
		return nil, &ExecutionError{Code: ErrCodeSourceLoadFailed, Message: err.Error(), Cause: err}
	}
	prepared, err := prepareDocument(doc, options)
	if err == nil {
		prepared.attach(e)
	}
	return prepared, err
}

func (e *Engine) PrepareCached(_ context.Context, options PrepareOptions) (*PreparedOperation, error) {
	options.Profile = normalizedProfile(options.Profile)
	if options.Source.Document != nil || options.Source.Content != nil {
		prepared, err := prepareSourceDocument(options.Source, options)
		if err == nil && prepared != nil {
			prepared.attach(e)
		}
		return prepared, err
	}
	if options.Source.Location == "" {
		return nil, nil
	}
	e.mu.RLock()
	doc := e.cache[options.Source.Location]
	e.mu.RUnlock()
	if doc == nil {
		return nil, nil
	}
	prepared, err := prepareDocument(doc, options)
	if err == nil {
		prepared.attach(e)
	}
	return prepared, err
}

func (p *PreparedOperation) attach(engine *Engine) {
	p.client = engine.client
	p.pool = engine.pool
	if p.options.HTTPClient != nil && p.options.HTTPClient != engine.client {
		p.client = p.options.HTTPClient
		p.pool = nil
	}
}

func (e *Engine) load(ctx context.Context, source Source, override *http.Client) (*document, error) {
	if source.Document != nil {
		if source.Document.doc == nil {
			return nil, fmt.Errorf("source document is empty")
		}
		return source.Document.doc, nil
	}
	client := override
	if client == nil {
		client = e.client
	}
	doc, err := loadDocument(ctx, client, source.Location, source.Content)
	if err != nil {
		return nil, err
	}
	if source.Location != "" {
		e.mu.Lock()
		e.cache[source.Location] = doc
		e.mu.Unlock()
	}
	return doc, nil
}

func prepareSourceDocument(source Source, options PrepareOptions) (*PreparedOperation, error) {
	if source.Document != nil && source.Document.doc != nil {
		return prepareDocument(source.Document.doc, options)
	}
	if source.Content == nil {
		return nil, nil
	}
	doc, err := parseDocument(source.Content)
	if err != nil {
		return nil, &ExecutionError{Code: ErrCodeSourceLoadFailed, Message: err.Error(), Cause: err}
	}
	return prepareDocument(doc, options)
}

func prepareDocument(doc *document, options PrepareOptions) (*PreparedOperation, error) {
	opID, err := parseRef(options.Ref)
	if err != nil {
		return nil, &ExecutionError{Code: ErrCodeInvalidRef, Message: err.Error(), Cause: err}
	}
	op, ok := doc.Operations[opID]
	if !ok || op.Ref != "" || op.UnresolvedTrait != "" {
		return nil, &ExecutionError{Code: ErrCodeRefNotFound, Message: fmt.Sprintf("operation %q was not found", opID)}
	}
	channelName := extractRefName(op.Channel.Ref)
	var ch *channel
	if value, ok := doc.Channels[channelName]; ok {
		ch = &value
	}
	target, targetErr := resolveTarget(doc, ch, options.Context)
	var prerequisites *Prerequisites
	if targetErr == nil {
		prerequisites = requiredContext(doc, &op, target.SecurityServer, target.ServerURL, options.Context)
	}
	return &PreparedOperation{
		document: doc, options: options, prerequisites: prerequisites, inputRequired: op.Action == "receive",
	}, nil
}

func (p *PreparedOperation) Start(ctx context.Context) (*Execution, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	execution := newExecution(ctx, p.inputRequired)
	profile := normalizedProfile(p.options.Profile)
	args := &executionArgs{
		Source: invocationSource{Profile: string(profile), Location: p.options.Source.Location, Content: p.options.Source.Content},
		Ref:    p.options.Ref, Context: p.options.Context,
		Hooks:                &invokeHooks{profile: profile, hooks: p.options.Hooks},
		Site:                 &invokeSite{Ref: p.options.Ref, Profile: string(profile)},
		MaxDeliveryUnitBytes: p.options.MaxDeliveryUnitBytes,
		AcceptsInput:         p.options.AcceptsInput,
	}
	client := p.client
	if client == nil {
		client = newDefaultHTTPClient()
	}
	pool := p.pool
	ownedPool := false
	if pool == nil {
		pool = newWSPool(client)
		ownedPool = true
	}
	go func() {
		if ownedPool {
			defer pool.closeAll()
		}
		runBinding(execution.ctx, client, pool, args, execution, p.document)
		execution.finishAfterRun()
	}()
	return execution, nil
}
