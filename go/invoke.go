package asyncapiclient

import (
	"bufio"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"unicode/utf8"
)

// configOrSourceError maps a resolution error to the right terminal. A
// resolvable-missing configuration value (a configRequired signal) becomes a
// config.value CONTEXT_REQUIRED challenge — retryable after resolution (R1a) —
// while any other error stays a terminal ERR_SOURCE_CONFIG_ERROR for source
// misconfiguration no runtime can fix. resolveTarget/resolveAddress already
// consulted the supplied context and found the value absent, so the challenge
// fires unconditionally; the operation-invoker's bounded resolve-and-retry
// loop is the backstop against a resolver that keeps supplying an insufficient
// value. The challenge target is the engine-asserted scope for the missing
// value (the context-scope model, ratified 2026-08-19): the resolved server
// URL when known (empty when server resolution itself failed), else the
// strongest host hint the artifact provides, else the threaded source
// location — the artifact-bound identity a point that precedes destination
// resolution naturally scopes to. The location rides verbatim: this client
// has no location canonicalizer of its own and the loader admits only an
// absolute URI there (validateDocumentAddress). A content-only source with
// no location asserts nothing and the target stays empty. A resolver decides
// whether the asserted scope is sufficient for stored-value release;
// configuration is not assumed public.
func configOrSourceError(err error, serverURL, sourceLocation string) *ExecutionError {
	var cr *configRequired
	if errors.As(err, &cr) {
		target := serverURL
		if target == "" {
			target = cr.hostHint
		}
		if target == "" {
			target = sourceLocation
		}
		req := newConfigValueRequirement(cr.point, cr.path, cr.description, cr.schema, cr.durable)
		return newContextRequiredError(cr.description, &Prerequisites{
			Target:       target,
			Alternatives: []RequirementAlternative{{Requirements: []Requirement{req}}},
		})
	}
	return &ExecutionError{Code: ErrCodeSourceConfigError, Message: err.Error()}
}

// AsyncAPI binding execution over the cardinality-agnostic invocation handle.
//
// The action is read from the DESCRIBED APPLICATION's perspective — AsyncAPI
// 3.0's own rule — and an invocation is the counterparty (ASYNC-P-02,
// spec/binding-specs/asyncapi): invoking a `send` operation SUBSCRIBES to
// what the application sends; invoking a `receive` operation PUBLISHES what
// the application expects to receive. The artifact is never read as
// describing the invoker.
//
// One entrypoint (runBinding) drives every cell against the binding-facing
// artifactHandle:
//
//	receive + http/https  unary publish: one input -> request body using the
//	                      artifact-declared method, response -> at most one output
//	receive + ws/wss      client-streaming publish: every input -> one
//	                      socket frame; the caller closing input ends it
//	send + http/https     excluded by the built-in HTTP driver
//	send + ws/wss         server-streaming subscription: socket frames ->
//	                      outputs, no caller input values
//
// All pre-dispatch failures (bad ref, no resolvable server, missing
// context, an unresolved address or server variable, an unsatisfied
// ws-binding declaration, an excluded input content family, missing publish
// input) are raised via FireError BEFORE any network I/O, per the
// binding-author contract and ASYNC-P-02/-03/-04's pre-dispatch refusals.

// maxResponseBytes bounds how much of an HTTP error body is retained by
// httpStatusError. Deliberately fixed: this is a standalone-runtime evidence
// bound on the error path, not a delivery unit —
// executionArgs.MaxDeliveryUnitBytes does not apply here.
const maxResponseBytes = 10 * 1024 * 1024 // 10 MB

// sseMaxLineBytes bounds individual SSE line length to prevent runaway memory
// use from a misbehaving server (parity with openapi/sse.go).
//
// Deliberately fixed: a line-scanner internal guard, not the delivery-unit
// bound — executionArgs.MaxDeliveryUnitBytes does not apply here.
const sseMaxLineBytes = 16 * 1024 * 1024

var nextWebSocketReplySession atomic.Uint64

type handle = artifactHandle[any, any]

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

// runBinding resolves the operation, checks runtime context, and dispatches
// to the protocol-specific runner. Terminates the handle exactly once. ctx is
// expected to be bound to the invocation's lifetime (DoneContext).
func runBinding(ctx context.Context, client *http.Client, pool *wsPool, args *executionArgs, h handle, doc *document) {
	opID, err := parseRef(args.Ref)
	if err != nil {
		h.FireError(&ExecutionError{Code: ErrCodeInvalidRef, Message: err.Error()})
		return
	}

	asyncOp, ok := doc.Operations[opID]
	if !ok {
		h.FireError(&ExecutionError{
			Code:    ErrCodeRefused,
			Message: fmt.Sprintf("operation %q not in AsyncAPI doc", opID),
		})
		return
	}
	if asyncOp.Ref != "" || asyncOp.UnresolvedTrait != "" {
		// An operations-map entry that is a Reference Object resolves
		// through it before the operation-object test (ASYNC-D-03);
		// resolveRefs leaves Ref set only when the reference dangles.
		h.FireError(&ExecutionError{
			Code:    ErrCodeRefused,
			Message: fmt.Sprintf("operation %q contains an unresolved reference", opID),
		})
		return
	}

	// The binding target is the addressed operation's channel (§8), reached
	// through a resolved server and expanded address (§9.2).
	channelName := extractRefName(asyncOp.Channel.Ref)
	var ch *channel
	if c, ok := doc.Channels[channelName]; ok {
		ch = &c
	}

	target, err := resolveTarget(doc, ch, args.Context)
	if err != nil {
		h.FireError(configOrSourceError(err, "", args.Source.Location))
		return
	}
	driver := args.ProtocolDrivers[strings.ToLower(target.Protocol)]
	if driver == nil && target.Protocol != "http" && target.Protocol != "https" && target.Protocol != "ws" && target.Protocol != "wss" {
		h.FireError(&ExecutionError{
			Code:    ErrCodeDriverUnavailable,
			Message: fmt.Sprintf("no AsyncAPI protocol driver is installed for %q", target.Protocol),
		})
		return
	}
	if driver == nil && (asyncOp.V2SecurityConjunction != nil || (target.SecurityServer != nil && target.SecurityServer.V2SecurityConjunction != nil)) {
		h.FireError(&ExecutionError{
			Code:    ErrCodeRefused,
			Message: "the built-in driver cannot preserve this AsyncAPI 2.x multi-scheme security conjunction",
		})
		return
	}
	if driver == nil {
		if err := validateCell(doc, ch, &asyncOp, target.Protocol, args.Source.Profile, args.Context); err != nil {
			h.FireError(&ExecutionError{Code: ErrCodeRefused, Message: err.Error()})
			return
		}
		if err := validateCredentialDestinations(doc, &asyncOp, target.SecurityServer, target.Protocol, args.Context); err != nil {
			h.FireError(&ExecutionError{Code: ErrCodeRefused, Message: err.Error()})
			return
		}
	}

	// Context negotiation: challenge BEFORE any connection is opened.
	if details := requiredContext(doc, &asyncOp, target.SecurityServer, target.ServerURL, args.Context); details != nil {
		h.FireError(newContextRequiredError(
			fmt.Sprintf("operation %q requires credentials the context does not provide", opID), details))
		return
	}

	// The address configuration point (ASYNC-P-04): the declared address
	// with every {name} expression expanded — an absent address or an
	// unresolved expression is a pre-dispatch refusal, never a guess.
	addrCfg, err := addressConfiguration(args.Context)
	if err != nil {
		h.FireError(&ExecutionError{Code: ErrCodeRefused, Message: err.Error()})
		return
	}
	// A reply route is artifact/configuration authority, not an application
	// value. Resolve it before waiting for caller input so an unsupported route
	// is a true pre-dispatch refusal and cannot strand an invocation waiting for
	// a value that no conforming execution could use.
	var preparedWSReplyLane *wsReplyLane
	if driver == nil && (target.Protocol == "ws" || target.Protocol == "wss") && asyncOp.Reply != nil {
		preparedWSReplyLane, err = resolveWebSocketReplyLane(doc, &asyncOp, target, args.Context)
		if err != nil {
			h.FireError(&ExecutionError{Code: ErrCodeRefused, Message: err.Error()})
			return
		}
	}
	var prepared *preparedInput
	if asyncOp.Action == "receive" && (driver == nil || channelNeedsOutgoingPayload(ch)) {
		if args.AcceptsInput != nil && !*args.AcceptsInput {
			h.FireError(&ExecutionError{Code: ErrCodeRefused, Message: "publish invocation requires an input message"})
			return
		}
		value, readErr := h.ReadInput(ctx)
		if readErr == io.EOF {
			h.FireError(&ExecutionError{Code: ErrCodeRefused, Message: "publish invocation requires an input message"})
			return
		}
		if readErr != nil {
			return
		}
		prepared = &preparedInput{Value: value}
	}
	// The routed envelope (§9.2): a parameterized channel's publish input
	// arrives as {payload, <params>}; parameter fields split off for
	// address expansion (explicit input winning over the
	// configuration.address.parameters pre-fill) and the payload rides on
	// to the ordinary codec lanes alone.
	if prepared != nil {
		selected, selErr := selectedInputMessages(doc, &asyncOp, ch, args.Context)
		if selErr != nil {
			// Selection defects surface at codec resolution with their own
			// codes; the split only needs the headers declaration.
			selected = nil
		}
		payload, envelopeParams, envelopeHeaders, isEnvelope, eerr := splitInputEnvelope(ch, selected, prepared.Value)
		if eerr != nil {
			h.FireError(&ExecutionError{Code: ErrCodeRefused, Message: eerr.Error()})
			return
		}
		if isEnvelope {
			merged := map[string]string{}
			for name, value := range addrCfg.Parameters {
				merged[name] = value
			}
			for name, value := range envelopeParams {
				merged[name] = value
			}
			addrCfg.Parameters = merged
			if driver == nil {
				prepared.Value = payload
				prepared.Headers = envelopeHeaders
			}
			// A driver lane keeps the RAW envelope value: its per-unit
			// seam splits every unit uniformly (the first pre-read unit
			// must not arrive pre-split while later units arrive whole).
		}
	}
	var outgoing any
	if prepared != nil {
		outgoing = prepared.Value
	}
	address, err := resolveAddress(ch, channelName, addrCfg, outgoing)
	if err != nil {
		h.FireError(configOrSourceError(err, target.ServerURL, args.Source.Location))
		return
	}

	// The complementary perspective (ASYNC-P-02): `receive` means the
	// described application receives, so invoking PUBLISHES; `send` means
	// it sends, so invoking SUBSCRIBES.
	switch asyncOp.Action {
	case "receive", "send":
	default:
		h.FireError(&ExecutionError{
			Code:    ErrCodeRefused,
			Message: fmt.Sprintf("unknown action %q", asyncOp.Action),
		})
		return
	}

	// Header carriage is per protocol cell (§9.2): the built-in HTTP lane
	// carries the envelope's headers as HTTP fields; every other cell in
	// this build (driver protocols, raw WebSocket frames) has no native
	// carriage qualified yet and refuses before dispatch.
	if asyncOp.Action == "receive" {
		inputHeadersDeclared := false
		if selected, selErr := selectedInputMessages(doc, &asyncOp, ch, args.Context); selErr == nil {
			for _, m := range selected {
				if m.Headers != nil {
					inputHeadersDeclared = true
				}
			}
		}
		if inputHeadersDeclared {
			carried := target.Protocol == "http" || target.Protocol == "https"
			if driver != nil {
				carried = driverCarriesHeaders(driver)
			}
			if !carried {
				h.FireError(&ExecutionError{Code: ErrCodeRefused, Message: fmt.Sprintf("the input declares application headers; this build has no header carriage for the %q protocol cell", target.Protocol)})
				return
			}
		}
	}
	if driver != nil {
		runProtocolDriver(ctx, driver, target, opID, &asyncOp, ch, address, addrCfg.Parameters, prepared, args, h, doc)
		return
	}

	switch target.Protocol {
	case "ws", "wss":
		// The websockets channel binding governs the upgrade request where
		// it speaks (§8): declared query and header values, supplied like
		// address parameters, with unsatisfied required declarations a
		// pre-dispatch refusal.
		fields, ferr := protocolFieldValues(args.Context)
		if ferr != nil {
			h.FireError(&ExecutionError{Code: ErrCodeRefused, Message: ferr.Error()})
			return
		}
		up, uerr := resolveWSUpgrade(ch, channelName, fields.WebSocketQuery, fields.WebSocketHeaders)
		if uerr != nil {
			h.FireError(&ExecutionError{Code: ErrCodeRefused, Message: uerr.Error()})
			return
		}
		dialAddress := mergeQuery(address, up.Query)
		if asyncOp.Reply != nil {
			replyLane := preparedWSReplyLane
			if replyLane == nil {
				h.FireError(&ExecutionError{Code: ErrCodeRefused, Message: "WebSocket reply route was not prepared"})
				return
			}
			var inputMessages, outputMessages []message
			if asyncOp.Action == "receive" {
				inputMessages, err = selectedInputMessages(doc, &asyncOp, ch, args.Context)
				outputMessages = replyGoverningMessages(doc, &asyncOp)
			} else {
				inputMessages, err = selectedReplyInputMessages(doc, &asyncOp, replyChannel(doc, &asyncOp), args.Context)
				outputMessages = governingMessages(doc, &asyncOp, ch)
			}
			if err != nil {
				h.FireError(&ExecutionError{Code: ErrCodeRefused, Message: err.Error()})
				return
			}
			operationLane := wsReplyLane{Target: target, Address: dialAddress, Headers: up.Headers}
			inputLane, outputLane := operationLane, *replyLane
			if asyncOp.Action == "send" {
				inputLane, outputLane = *replyLane, operationLane
			}
			runWSSubscribe(ctx, pool, outputLane.Target, outputLane.Address, outputLane.Headers, doc, ch, &asyncOp, args, h, &wsReplyExchange{
				InputMessages: inputMessages, OutputMessages: outputMessages,
				RequireInput: asyncOp.Action == "receive",
				IsolationKey: "reply-" + strconv.FormatUint(nextWebSocketReplySession.Add(1), 10),
				Prepared:     prepared, InputLane: inputLane,
			})
			return
		}
		if asyncOp.Action == "receive" {
			runWSPublish(ctx, pool, target, dialAddress, up.Headers, doc, ch, &asyncOp, args, h, prepared)
		} else {
			runWSSubscribe(ctx, pool, target, dialAddress, up.Headers, doc, ch, &asyncOp, args, h, nil)
		}
	case "http", "https":
		if asyncOp.Action == "receive" {
			runUnaryPublish(ctx, client, target, address, doc, ch, &asyncOp, args, h, prepared)
		} else {
			runSSESubscribe(ctx, client, target, address, doc, ch, &asyncOp, args, h)
		}
	default:
		// resolveTarget only yields bound protocols; defensive.
		h.FireError(&ExecutionError{
			Code:    ErrCodeRefused,
			Message: fmt.Sprintf("protocol %q is not bound by the supported asyncapi revisions (supported: http, https, ws, wss)", target.Protocol),
		})
	}
}

type wsReplyLane struct {
	Target  resolvedTarget
	Address string
	Headers map[string]string
}

func resolveWebSocketReplyLane(doc *document, operation *asyncOperation, operationTarget resolvedTarget, bindCtx map[string]any) (*wsReplyLane, error) {
	reply := operation.Reply
	if reply == nil {
		return nil, fmt.Errorf("operation has no reply")
	}
	if reply.Address != nil && reply.Address.Location != "" {
		source := "runtime expression"
		if strings.HasPrefix(reply.Address.Location, "$message.header#") {
			source = "application header"
		}
		return nil, fmt.Errorf("WebSocket reply address uses an %s that the current payload-only session profile cannot resolve", source)
	}
	replyCh := replyChannel(doc, operation)
	if replyCh == nil {
		return nil, fmt.Errorf("WebSocket reply has no resolved reply channel")
	}
	if replyCh.Bindings != nil && replyCh.Bindings.WS != nil {
		version := replyCh.Bindings.WS.BindingVersion
		if version != "" && version != "0.1.0" {
			return nil, fmt.Errorf("reply WebSockets binding version %q is outside the built-in WebSocket driver's 0.1.0 envelope", version)
		}
	}
	replyTarget, err := resolveTarget(doc, replyCh, bindCtx)
	if err != nil {
		return nil, err
	}
	if (replyTarget.Protocol != "ws" && replyTarget.Protocol != "wss") || replyTarget.Protocol != operationTarget.Protocol || replyTarget.ServerURL != operationTarget.ServerURL {
		return nil, fmt.Errorf("WebSocket reply channel selects a different protocol or server; cross-target reply sessions are not qualified")
	}
	replyName := extractRefName(reply.Channel.Ref)
	replyAddress, err := resolveAddress(replyCh, replyName, addressConfig{})
	if err != nil {
		return nil, err
	}
	fields, err := protocolFieldValues(bindCtx)
	if err != nil {
		return nil, err
	}
	replyUpgrade, err := resolveWSUpgrade(replyCh, replyName, fields.WebSocketQuery, fields.WebSocketHeaders)
	if err != nil {
		return nil, err
	}
	return &wsReplyLane{Target: replyTarget, Address: mergeQuery(replyAddress, replyUpgrade.Query), Headers: replyUpgrade.Headers}, nil
}

func equalStringMap(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for key, value := range a {
		if b[key] != value {
			return false
		}
	}
	return true
}

func channelNeedsOutgoingPayload(ch *channel) bool {
	if ch == nil {
		return false
	}
	for _, parameter := range ch.Parameters {
		if parameter.Location != "" {
			return true
		}
	}
	return false
}

func runProtocolDriver(ctx context.Context, driver ProtocolDriver, target resolvedTarget, operationKey string, operation *asyncOperation, ch *channel, address string, addrParams map[string]string, prepared *preparedInput, args *executionArgs, h handle, doc *document) {
	request, err := prepareProtocolDriverRequest(target, operationKey, operation, ch, address, addrParams, driverCarriesHeaders(driver), args, doc)
	if err != nil {
		h.FireError(&ExecutionError{Code: ErrCodeRefused, Message: err.Error(), Cause: err})
		return
	}
	session := &handleDriverSession{handle: h, prepared: prepared}
	if err := driver.Execute(ctx, request, session); err != nil {
		h.FireError(&ExecutionError{
			Code: ErrCodeDriverFailed, Message: err.Error(), Cause: err,
			Diagnostics: map[string]any{"protocol": target.Protocol},
		})
		return
	}
	session.Complete()
}

func prepareProtocolDriverRequest(target resolvedTarget, operationKey string, operation *asyncOperation, ch *channel, address string, addrParams map[string]string, headersCarried bool, args *executionArgs, doc *document) (DriverRequest, error) {
	driverDocument := objectMapForDriver(doc)
	request := DriverRequest{
		Artifact: append([]byte(nil), doc.raw...), Document: driverDocument,
		Operation: objectMapForDriver(operation),
		Server:    objectMapForDriver(target.SecurityServer), Ref: args.Ref, OperationKey: operationKey,
		Action: operation.Action, Protocol: target.Protocol, ServerURL: target.ServerURL,
		Context:              args.Context,
		SecurityAlternatives: driverSecurityAlternatives(doc, operation, target.SecurityServer),
	}
	resolveDriverBindingRefs(request.Operation, driverDocument)
	resolveDriverBindingRefs(request.Server, driverDocument)
	var inputMessages, outputMessages []message
	inputChannel, outputChannel := ch, ch
	inputTarget, outputTarget := target, target
	inputAddress, outputAddress := address, address
	var replyTarget resolvedTarget
	replyAddress := ""
	if operation.Reply != nil {
		replyCh := replyChannel(doc, operation)
		if replyCh != nil {
			var targetErr error
			replyTarget, targetErr = resolveTarget(doc, replyCh, args.Context)
			if targetErr != nil {
				return DriverRequest{}, targetErr
			}
			if operation.Reply.Address == nil || operation.Reply.Address.Location == "" {
				if extractRefName(operation.Channel.Ref) == extractRefName(operation.Reply.Channel.Ref) {
					replyAddress = address
				} else {
					replyAddress, targetErr = resolveAddress(replyCh, extractRefName(operation.Reply.Channel.Ref), addressConfig{})
					if targetErr != nil {
						return DriverRequest{}, targetErr
					}
				}
			}
		}
	}
	if operation.Action == "receive" {
		selected, err := selectedInputMessages(doc, operation, ch, args.Context)
		if err != nil {
			return DriverRequest{}, err
		}
		codec, err := resolveInputCodec(doc, selected, args.Context)
		if err != nil {
			return DriverRequest{}, err
		}
		inputMessages = selected
		request.Input = &DriverInput{DriverDirection: driverDirection(inputChannel, inputTarget, inputAddress, inputMessages, driverDocument)}
		installDriverInputSeams(request.Input, ch, selected, codec, addrParams, args)
		if operation.Reply != nil {
			outputMessages = replyGoverningMessages(doc, operation)
			outputChannel = replyChannel(doc, operation)
			outputTarget, outputAddress = replyTarget, replyAddress
		}
	} else {
		outputMessages = governingMessages(doc, operation, ch)
		if operation.Reply != nil {
			inputChannel = replyChannel(doc, operation)
			inputTarget, inputAddress = replyTarget, replyAddress
			selected, err := selectedReplyInputMessages(doc, operation, inputChannel, args.Context)
			if err != nil {
				return DriverRequest{}, err
			}
			codec, err := resolveInputCodec(doc, selected, args.Context)
			if err != nil {
				return DriverRequest{}, err
			}
			inputMessages = selected
			request.Input = &DriverInput{DriverDirection: driverDirection(inputChannel, inputTarget, inputAddress, inputMessages, driverDocument)}
			installDriverInputSeams(request.Input, inputChannel, selected, codec, nil, args)
		}
	}
	if len(outputMessages) > 0 {
		contentType, err := resolveSubscriptionContentType(doc, outputMessages, args.Context, headersCarried)
		if err != nil {
			return DriverRequest{}, err
		}
		outputAvro, err := resolveAvroBinaryCodec(outputMessages, args.Context)
		if err != nil {
			return DriverRequest{}, err
		}
		request.Output = &DriverOutput{DriverDirection: driverDirection(outputChannel, outputTarget, outputAddress, outputMessages, driverDocument)}
		decodePayload := func(payload []byte) (any, error) {
			if int64(len(payload)) > args.DeliveryUnitLimit() {
				return nil, fmt.Errorf("delivery unit exceeds configured %d-byte limit", args.DeliveryUnitLimit())
			}
			return args.Hooks.DecodeOutput(siteFor(args, target.ServerURL), RawResult{Body: payload}, builtinDecodeFor(contentType, outputAvro))
		}
		request.Output.Decode = decodePayload
		outputHeadersDeclared := false
		for _, m := range outputMessages {
			if m.Headers != nil {
				outputHeadersDeclared = true
			}
		}
		capturedOutputs := outputMessages
		request.Output.DecodeUnit = func(unit DriverUnit) (any, error) {
			value, err := decodePayload(unit.Payload)
			if err != nil {
				return nil, err
			}
			if !outputHeadersDeclared {
				return value, nil
			}
			// The routed envelope on the output direction: the payload
			// pairs with the DECLARED application headers projected from
			// the received protocol pairs (§9.2).
			received := map[string][]string{}
			for _, pair := range unit.Headers {
				if !utf8.Valid(pair.Value) {
					return nil, fmt.Errorf("received header %q is not valid UTF-8; the declared application-headers contract has no faithful value", pair.Key)
				}
				received[pair.Key] = append(received[pair.Key], string(pair.Value))
			}
			return map[string]any{
				"payload": value,
				"headers": projectResponseHeaders(capturedOutputs, received),
			}, nil
		}
	}
	return request, nil
}

func driverDirection(ch *channel, target resolvedTarget, address string, messages []message, document map[string]any) DriverDirection {
	direction := DriverDirection{
		Channel: objectMapForDriver(ch), Server: objectMapForDriver(target.SecurityServer),
		Protocol: target.Protocol, ServerURL: target.ServerURL, Address: address,
		Messages: make([]map[string]any, len(messages)),
	}
	resolveDriverBindingRefs(direction.Channel, document)
	resolveDriverBindingRefs(direction.Server, document)
	for i := range messages {
		direction.Messages[i] = objectMapForDriver(messages[i])
		resolveDriverBindingRefs(direction.Messages[i], document)
	}
	return direction
}

func replyChannel(doc *document, operation *asyncOperation) *channel {
	if operation == nil || operation.Reply == nil || operation.Reply.Channel == nil {
		return nil
	}
	if ch, ok := doc.Channels[extractRefName(operation.Reply.Channel.Ref)]; ok {
		return &ch
	}
	return nil
}

func selectedReplyInputMessages(doc *document, operation *asyncOperation, ch *channel, bindCtx map[string]any) ([]message, error) {
	copy := *operation
	copy.Messages = operation.Reply.Messages
	copy.Reply = nil
	return selectedInputMessages(doc, &copy, ch, bindCtx)
}

func resolveDriverBindingRefs(owner map[string]any, document map[string]any) {
	bindings, _ := owner["bindings"].(map[string]any)
	for name, raw := range bindings {
		bindingObject, _ := raw.(map[string]any)
		if bindingObject == nil {
			continue
		}
		bindings[name] = resolveSchemaRefs(deepCopyMap(bindingObject), document, nil)
	}
}

func objectMapForDriver(value any) map[string]any {
	if value == nil {
		return nil
	}
	data, err := json.Marshal(value)
	if err != nil {
		return nil
	}
	var result map[string]any
	if json.Unmarshal(data, &result) != nil {
		return nil
	}
	return result
}

type preparedInput struct {
	Value any
	// Headers is the routed envelope's application-headers value (nil when
	// the selected input message declares no headers contract). Carriage is
	// per protocol cell: HTTP fields carry it; a cell with no native
	// carriage refuses pre-dispatch.
	Headers map[string]any
}

func validateCell(doc *document, ch *channel, op *asyncOperation, protocol, bindingSpec string, bindCtx map[string]any) error {
	var httpBinding *httpOperationBinding
	if op.Bindings != nil {
		httpBinding = op.Bindings.HTTP
	}
	if httpBinding != nil && httpBinding.BindingVersion != "" && httpBinding.BindingVersion != "0.3.0" {
		return fmt.Errorf("HTTP binding version %q is outside the built-in HTTP driver's 0.3.0 envelope", httpBinding.BindingVersion)
	}
	var wsBinding *wsChannelBinding
	if ch != nil && ch.Bindings != nil {
		wsBinding = ch.Bindings.WS
	}
	if wsBinding != nil && wsBinding.BindingVersion != "" && wsBinding.BindingVersion != "0.1.0" {
		return fmt.Errorf("WebSockets binding version %q is outside the built-in WebSocket driver's 0.1.0 envelope", wsBinding.BindingVersion)
	}
	if protocol == "http" || protocol == "https" {
		if op.Action == "send" {
			return fmt.Errorf("standalone HTTP send operations are not implemented by the built-in HTTP driver")
		}
		if httpBinding == nil || strings.TrimSpace(httpBinding.Method) == "" {
			return fmt.Errorf("HTTP receive operation has no artifact-declared HTTP method; POST is not inferred")
		}
		selected, err := selectedInputMessages(doc, op, ch, bindCtx)
		if err != nil {
			return err
		}
		if err := validateMessageBindingVersion(selected[0]); err != nil {
			return err
		}
		if _, err = resolveInputCodec(doc, selected, bindCtx); err != nil {
			return err
		}
		if !replyMessagesBindable(doc, op) {
			return fmt.Errorf("an HTTP reply message uses carriage outside the built-in HTTP driver's application-value boundary")
		}
		fields, err := protocolFieldValues(bindCtx)
		if err != nil {
			return err
		}
		_, err = resolveHTTPQuery(op, fields.HTTPQuery)
		return err
	}
	if op.Action == "receive" {
		selected, err := selectedInputMessages(doc, op, ch, bindCtx)
		if err != nil {
			return err
		}
		if err := validateMessageBindingVersion(selected[0]); err != nil {
			return err
		}
		if _, err = resolveInputCodec(doc, selected, bindCtx); err != nil {
			return err
		}
		messageType, _ := contextConfiguration(bindCtx)["websocketMessageType"].(string)
		if messageType != "text" && messageType != "binary" {
			return fmt.Errorf("configuration.websocketMessageType must select text or binary for a WebSocket publish")
		}
		if op.Reply != nil {
			_, err = resolveSubscriptionContentType(doc, replyGoverningMessages(doc, op), bindCtx)
			return err
		}
		return nil
	}
	if ch != nil {
		for _, parameter := range ch.Parameters {
			if parameter.Location != "" {
				return fmt.Errorf("WebSocket subscription address uses a runtime expression that requires an outgoing payload")
			}
		}
	}
	_, err := resolveSubscriptionContentType(doc, governingMessages(doc, op, ch), bindCtx)
	if err != nil || op.Reply == nil {
		return err
	}
	replyCh := replyChannel(doc, op)
	selected, err := selectedReplyInputMessages(doc, op, replyCh, bindCtx)
	if err != nil {
		return err
	}
	if err := validateMessageBindingVersion(selected[0]); err != nil {
		return err
	}
	if _, err := resolveInputCodec(doc, selected, bindCtx); err != nil {
		return err
	}
	messageType, _ := contextConfiguration(bindCtx)["websocketMessageType"].(string)
	if messageType != "text" && messageType != "binary" {
		return fmt.Errorf("configuration.websocketMessageType must select text or binary for a WebSocket reply input")
	}
	return nil
}

func validateCredentialDestinations(doc *document, op *asyncOperation, server *server, protocol string, bindCtx map[string]any) error {
	fields, err := protocolFieldValues(bindCtx)
	if err != nil {
		return err
	}
	reserved := map[string]bool{}
	if protocol == "http" || protocol == "https" {
		for _, n := range []string{"host", "content-length", "content-type"} {
			reserved[n] = true
		}
	} else {
		for _, n := range []string{"host", "connection", "upgrade", "sec-websocket-key", "sec-websocket-version", "sec-websocket-protocol"} {
			reserved[n] = true
		}
	}
	for name := range contextHeaders(bindCtx) {
		if reserved[strings.ToLower(name)] {
			return fmt.Errorf("configured header %q collides with a processor-owned transport field", name)
		}
	}
	for _, named := range resolveSecuritySchemes(doc, server, op) {
		s := named.Scheme
		if s.Type != "apiKey" && s.Type != "httpApiKey" {
			continue
		}
		if contextAPIKeyFor(bindCtx, named.Name) == "" {
			continue
		}
		switch s.In {
		case "header":
			lower := strings.ToLower(s.Name)
			if reserved[lower] {
				return fmt.Errorf("credential header destination %q collides with a processor-owned transport field", s.Name)
			}
			for name := range fields.WebSocketHeaders {
				if strings.EqualFold(name, s.Name) {
					return fmt.Errorf("credential header destination %q collides with a WebSocket protocol field", s.Name)
				}
			}
		case "query":
			if _, ok := fields.WebSocketQuery[s.Name]; ok {
				return fmt.Errorf("credential query destination %q collides with a WebSocket protocol field", s.Name)
			}
			if _, ok := fields.HTTPQuery[s.Name]; ok {
				return fmt.Errorf("credential query destination %q collides with an HTTP protocol field", s.Name)
			}
		}
	}
	return nil
}

// ---------------------------------------------------------------------------
// Ref parsing & server resolution
// ---------------------------------------------------------------------------

// parseRef parses a binding ref per ASYNC-D-03: a JSON Pointer
// `#/operations/<operation-key>` addressing an operations-map entry is the
// ONLY conformant spelling. A bare operation key without the pointer prefix
// is refused (the former lenience is gone), and an unescaped `/` after the
// prefix addresses a deeper path — never an operations-map entry — so it is
// refused too. Operation keys containing `/` or `~` carry RFC 6901 escaping
// in the pointer: ~1 → /, ~0 → ~, decoded in that order.
func parseRef(ref string) (string, error) {
	ref = strings.TrimSpace(ref)
	if ref == "" {
		return "", fmt.Errorf("ref is required and must be a JSON Pointer #/operations/<operation-key> (ASYNC-D-03)")
	}
	if operation, ok := parseV2OperationRef(ref); ok {
		return operation, nil
	}

	const prefix = "#/operations/"
	if !strings.HasPrefix(ref, prefix) {
		return "", fmt.Errorf("ref %q is not a JSON Pointer #/operations/<operation-key>: the pointer is the only conformant spelling — a bare operation key is not accepted (ASYNC-D-03)", ref)
	}
	token := ref[len(prefix):]
	if token == "" {
		return "", fmt.Errorf("empty operation key in ref %q (ASYNC-D-03)", ref)
	}
	if strings.Contains(token, "/") {
		return "", fmt.Errorf("ref %q addresses a deeper path, not an operations-map entry: an operation key containing / carries RFC 6901 escaping (~1) (ASYNC-D-03)", ref)
	}
	opID := strings.ReplaceAll(token, "~1", "/")
	opID = strings.ReplaceAll(opID, "~0", "~")
	return opID, nil
}

// operationRef builds the conformant ASYNC-D-03 spelling for an operation
// key: `#/operations/` + the RFC 6901-escaped key (~ → ~0 first, then
// / → ~1 — escape order is the reverse of decode order).
func operationRef(opID string) string {
	if ref, ok := refForNormalizedOperationKey(opID); ok {
		return ref
	}
	escaped := strings.ReplaceAll(opID, "~", "~0")
	escaped = strings.ReplaceAll(escaped, "/", "~1")
	return "#/operations/" + escaped
}

// Server and address resolution (the §9.2 configuration points) live in
// target.go; protocol-bindings honoring in bindings.go; governing
// content-type resolution in content.go.

// ---------------------------------------------------------------------------
// Context requirements (CONTEXT_REQUIRED negotiation)
// ---------------------------------------------------------------------------

// requirementType maps an AsyncAPI security scheme to a standard requirement
// family, or "" when the scheme family is unmapped — a scheme this SDK
// cannot itself resolve (see unmappedRequirementType, which requiredContext
// consults instead of dropping the scheme, per the R2.c ruling).
func requirementType(s securityScheme) string {
	switch s.Type {
	case "http":
		switch strings.ToLower(s.Scheme) {
		case "bearer":
			return "auth.bearer"
		case "basic":
			return "auth.basic"
		}
		return ""
	case "httpBearer":
		return "auth.bearer"
	case "userPassword", "scramSha256", "scramSha512":
		return "auth.basic"
	case "apiKey", "httpApiKey":
		return "auth.apiKey"
	case "oauth2":
		return "auth.oauth2"
	}
	return ""
}

// unmappedRequirementType derives the R2.c surfaced-requirement type for a
// scheme family requirementType doesn't map: "auth.http.<scheme>" for an
// HTTP auth scheme other than bearer/basic (e.g. "auth.http.digest"), or
// "auth." + the artifact's own type verbatim otherwise (e.g.
// "auth.futureSasl", "auth.X509"). The alternative stays discoverable to a
// runtime with a resolver for that family, rather than silently dropped.
func unmappedRequirementType(s securityScheme) string {
	if s.Type == "http" {
		if s.Scheme == "" {
			// A missing scheme value degrades to the bare family, never a
			// trailing dot (TS parity).
			return "auth.http"
		}
		return "auth.http." + strings.ToLower(s.Scheme)
	}
	return "auth." + s.Type
}

// requiredContext computes the context the binding requires for this
// operation, or nil when the provided context already satisfies it (or the
// doc declares nothing checkable). The declaration semantics are AsyncAPI
// 3.0's, incorporated, and they are CONJUNCTIVE (ASYNC-P-07): the targeted
// server's `security` applies, and the operation's `security`, when
// declared, applies IN ADDITION. Within each declared list — a flat LIST of
// Security Scheme Objects or Reference Objects, not OpenAPI-style
// requirement-maps — satisfying any ONE entry suffices. That OR-within,
// AND-across shape maps onto the challenge contract as a cross product:
// each alternative pairs one resolvable server entry with one resolvable
// operation entry (or is a single entry when only one list is declared).
// An unresolvable $ref is skipped entirely (nothing to check); an entry
// whose scheme family requirementType doesn't map is SURFACED with a
// derived type (R2.c ruling) rather than dropped, so the alternative stays
// discoverable to a runtime with a resolver for it. Side-effect-free;
// shared by runBinding and PrepareBinding.
//
// secSrv is the selected artifact server whose declared security applies
// (§9.5), including when its target is replaced by a complete URL
// (resolveTarget's SecurityServer). nil means no artifact server was selected.
func requiredContext(doc *document, asyncOp *asyncOperation, secSrv *server, serverURL string, ctx map[string]any) *Prerequisites {
	serverReqs := resolveRequirementList(doc, serverSecurityRequirements(secSrv), serverURL)
	opReqs := resolveRequirementList(doc, operationSecurityRequirements(asyncOp), serverURL)

	var alternatives []RequirementAlternative
	switch {
	case len(serverReqs) > 0 && len(opReqs) > 0:
		for _, s := range serverReqs {
			for _, o := range opReqs {
				reqs := []Requirement{s}
				// The same scheme declared on both levels is one
				// requirement, not a duplicated conjunct.
				if o.Type != s.Type || o.Name != s.Name {
					reqs = append(reqs, o)
				}
				alternatives = append(alternatives, RequirementAlternative{Requirements: reqs})
			}
		}
	case len(serverReqs) > 0:
		for _, s := range serverReqs {
			alternatives = append(alternatives, RequirementAlternative{Requirements: []Requirement{s}})
		}
	case len(opReqs) > 0:
		for _, o := range opReqs {
			alternatives = append(alternatives, RequirementAlternative{Requirements: []Requirement{o}})
		}
	}
	if len(alternatives) == 0 {
		return nil
	}

	details := &Prerequisites{
		Target:       serverURL,
		Alternatives: alternatives,
	}
	if contextSatisfies(ctx, details) {
		return nil
	}
	return details
}

// oauth2Requirement builds an auth.oauth2 requirement carrying the SELECTED
// flow's grantType (R2.b ruling) alongside its authorize/token URLs and
// scopes, under the binding-invoker contract's convention field names
// (grantType, authorizeUrl, tokenUrl, scopes) — mirrors openapi's
// oauth2Requirement exactly. Fixed priority, surfaced not changed:
// authorizationCode > implicit > password > clientCredentials, the last two
// selected only when they carry a tokenUrl (the field both formats restrict
// them to). Relative URLs are resolved against the server URL.
func oauth2Requirement(s securityScheme, serverURL string) Requirement {
	req := Requirement{Type: "auth.oauth2"}
	if s.Flows == nil {
		return req
	}
	var flow *oauthFlow
	var grantType string
	switch {
	case s.Flows.AuthorizationCode != nil:
		flow, grantType = s.Flows.AuthorizationCode, "authorization_code"
	case s.Flows.Implicit != nil:
		flow, grantType = s.Flows.Implicit, "implicit"
	case s.Flows.Password != nil && s.Flows.Password.TokenURL != "":
		flow, grantType = s.Flows.Password, "password"
	case s.Flows.ClientCredentials != nil && s.Flows.ClientCredentials.TokenURL != "":
		flow, grantType = s.Flows.ClientCredentials, "client_credentials"
	}
	if flow == nil {
		return req
	}
	extra := map[string]any{"grantType": grantType}
	if flow.AuthorizationURL != "" {
		extra["authorizeUrl"] = absolutizeURL(flow.AuthorizationURL, serverURL)
	}
	if flow.TokenURL != "" {
		extra["tokenUrl"] = absolutizeURL(flow.TokenURL, serverURL)
	}
	if len(flow.Scopes) > 0 {
		scopes := make([]string, 0, len(flow.Scopes))
		for k := range flow.Scopes {
			scopes = append(scopes, k)
		}
		sort.Strings(scopes)
		extra["scopes"] = scopes
	}
	req.Extra = extra
	return req
}

// absolutizeURL resolves a possibly-relative flow URL against the server
// base; absolute URLs pass through unchanged. Mirrors openapi's
// absolutizeURL (same behavior; kept local since the format packages don't
// share private helpers).
func absolutizeURL(ref, baseURL string) string {
	u, err := url.Parse(ref)
	if err != nil {
		return ref
	}
	if u.IsAbs() {
		return ref
	}
	base, err := url.Parse(baseURL)
	if err != nil {
		return ref
	}
	return base.ResolveReference(u).String()
}

// serverSecurityRequirements returns the security list of the server whose
// declared security applies (§9.5, ASYNC-P-07) — resolveTarget's
// SecurityServer, so the requirements always describe the selected artifact
// server, including when configuration replaces only its connection target.
func serverSecurityRequirements(secSrv *server) []securityRequirement {
	if secSrv != nil {
		return secSrv.Security
	}
	return nil
}

// operationSecurityRequirements returns the operation's own security list.
// It never displaces the server's: the two lists are conjunctive
// (ASYNC-P-07) — the server's security applies, and the operation's applies
// in addition.
func operationSecurityRequirements(asyncOp *asyncOperation) []securityRequirement {
	if asyncOp == nil {
		return nil
	}
	return asyncOp.Security
}

// resolveRequirementList resolves one declared security list into concrete
// Requirements, in declaration order: unresolvable $refs are skipped
// (not checkable, not enforced, never degraded); unmapped scheme families
// are surfaced with a derived type (R2.c). Each requirement carries the
// components.securitySchemes key its entry's $ref resolved through as Name
// (R2.a; empty for inline schemes).
func resolveRequirementList(doc *document, requirements []securityRequirement, serverURL string) []Requirement {
	var out []Requirement
	for _, entry := range requirements {
		scheme, ok := resolveSecurityRequirement(doc, entry)
		if !ok {
			continue
		}
		var req Requirement
		if typ := requirementType(scheme); typ != "" {
			if typ == "auth.oauth2" {
				req = oauth2Requirement(scheme, serverURL)
			} else {
				req = Requirement{Type: typ}
			}
		} else {
			req = Requirement{Type: unmappedRequirementType(scheme)}
		}
		if scheme.Description != "" {
			req.Description = scheme.Description
		}
		req.Name = securityRequirementName(entry)
		durable := true
		req.Durable = &durable
		out = append(out, req)
	}
	return out
}

func driverSecurityAlternatives(doc *document, operation *asyncOperation, server *server) [][]DriverSecurityScheme {
	serverSchemes := driverSecuritySchemes(doc, serverSecurityRequirements(server))
	operationSchemes := driverSecuritySchemes(doc, operationSecurityRequirements(operation))
	var combinations [][]DriverSecurityScheme
	switch {
	case len(serverSchemes) > 0 && len(operationSchemes) > 0:
		for _, serverScheme := range serverSchemes {
			for _, operationScheme := range operationSchemes {
				alternative := []DriverSecurityScheme{serverScheme}
				if serverScheme.Name != operationScheme.Name || serverScheme.Scheme["type"] != operationScheme.Scheme["type"] || serverScheme.Scheme["scheme"] != operationScheme.Scheme["scheme"] {
					alternative = append(alternative, operationScheme)
				}
				combinations = append(combinations, alternative)
			}
		}
	case len(serverSchemes) > 0:
		for _, scheme := range serverSchemes {
			combinations = append(combinations, []DriverSecurityScheme{scheme})
		}
	case len(operationSchemes) > 0:
		for _, scheme := range operationSchemes {
			combinations = append(combinations, []DriverSecurityScheme{scheme})
		}
	}
	return combinations
}

func driverSecuritySchemes(doc *document, requirements []securityRequirement) []DriverSecurityScheme {
	var result []DriverSecurityScheme
	for _, requirement := range requirements {
		scheme, ok := resolveSecurityRequirement(doc, requirement)
		if !ok {
			continue
		}
		result = append(result, DriverSecurityScheme{
			Name:   securityRequirementName(requirement),
			Scheme: objectMapForDriver(scheme),
		})
	}
	return result
}

// resolveSecurityRequirement resolves one `security` list entry to a concrete
// securityScheme: a $ref is looked up by name in
// doc.Components.SecuritySchemes; an inline entry (no $ref) is used exactly
// as declared. ok is false when a $ref cannot be resolved — an unresolvable
// reference is not checkable, not enforced, never degraded into a weaker
// requirement.
func resolveSecurityRequirement(doc *document, req securityRequirement) (securityScheme, bool) {
	if req.Ref != "" {
		if doc.Components == nil {
			return securityScheme{}, false
		}
		scheme, ok := doc.Components.SecuritySchemes[extractRefName(req.Ref)]
		return scheme, ok
	}
	if req.Type == "" {
		// Neither a $ref nor an inline scheme (e.g. a stray empty object):
		// nothing to check.
		return securityScheme{}, false
	}
	return req.securityScheme, true
}

// securityRequirementName returns the components.securitySchemes key a
// security list entry's $ref resolves through (the R2.a ruling's Name),
// or "" for an inline scheme object — it has no addressable name.
func securityRequirementName(req securityRequirement) string {
	if req.Ref == "" {
		return ""
	}
	return extractRefName(req.Ref)
}

// ---------------------------------------------------------------------------
// Publish over HTTP (`receive` action): unary, artifact-declared method
// ---------------------------------------------------------------------------

func runUnaryPublish(ctx context.Context, client *http.Client, target resolvedTarget, address string, doc *document, ch *channel, asyncOp *asyncOperation, args *executionArgs, h handle, prepared *preparedInput) {
	// Unary: the one input IS the message payload (ASYNC-P-03). A publish
	// invocation requires an input value — this family defines no empty
	// message, so absence is a pre-dispatch refusal, never an empty-object
	// substitute. An operation-layer call for an operation declaring no
	// input (Binding != nil, InputSchema == nil) is refused up front:
	// callers of no-input operations never write, so reading would park.
	if args.AcceptsInput != nil && !*args.AcceptsInput {
		h.FireError(&ExecutionError{
			Code:    ErrCodeRefused,
			Message: "publish invocation requires an input message (the input is the message; the operation declares no input)",
		})
		return
	}

	// Input encoding follows the governing request-side declaration
	// (ASYNC-P-03); an excluded declared family refuses BEFORE dispatch.
	selected, cerr := selectedInputMessages(doc, asyncOp, ch, args.Context)
	if cerr != nil {
		h.FireError(&ExecutionError{Code: ErrCodeRefused, Message: cerr.Error()})
		return
	}
	codec, cerr := resolveInputCodec(doc, selected, args.Context)
	if cerr != nil {
		h.FireError(&ExecutionError{Code: ErrCodeRefused, Message: cerr.Error()})
		return
	}

	var first any
	var rerr error
	if prepared != nil {
		first = prepared.Value
	} else {
		first, rerr = h.ReadInput(ctx)
	}
	if rerr == io.EOF {
		h.FireError(&ExecutionError{
			Code:    ErrCodeRefused,
			Message: "publish invocation requires an input message",
		})
		return
	}
	if rerr != nil {
		return // invocation already terminal (or cancelled)
	}
	_ = h.CloseInput()

	body, err := args.Hooks.EncodeInput(siteFor(args, target.ServerURL), first, func(v any) ([]byte, error) { return encodeInput(codec, v) })
	if err != nil {
		h.FireError(&ExecutionError{Code: ErrCodeRefused, Message: err.Error()})
		return
	}

	// The request method is the required http operation binding's `method`
	// (§8, ASYNC-P-02); validateCell already refused its absence.
	requestURL := joinURL(target.ServerURL, address)
	fields, ferr := protocolFieldValues(args.Context)
	if ferr != nil {
		h.FireError(&ExecutionError{Code: ErrCodeRefused, Message: ferr.Error()})
		return
	}
	query, qerr := resolveHTTPQuery(asyncOp, fields.HTTPQuery)
	if qerr != nil {
		h.FireError(&ExecutionError{Code: ErrCodeRefused, Message: qerr.Error()})
		return
	}
	if len(query) > 0 {
		parsed, _ := url.Parse(requestURL)
		values := parsed.Query()
		for name, value := range query {
			values.Set(name, value)
		}
		parsed.RawQuery = values.Encode()
		requestURL = parsed.String()
	}
	req, err := http.NewRequestWithContext(ctx, requestMethod(asyncOp, ""), requestURL, bytes.NewReader(body))
	if err != nil {
		h.FireError(asExecutionError(err))
		return
	}
	if codec.ContentType != "" {
		req.Header.Set("Content-Type", codec.ContentType)
	}
	// The routed envelope's application headers ride the HTTP cell's native
	// carriage (§9.2): each member becomes one request field.
	if prepared != nil {
		for name, value := range prepared.Headers {
			text, herr := headerFieldText(name, value)
			if herr != nil {
				h.FireError(&ExecutionError{Code: ErrCodeRefused, Message: herr.Error()})
				return
			}
			req.Header.Set(name, text)
		}
	}
	// Direction-correct Accept: the reply-side governing declaration, when
	// it names exactly one type (ASYNC-P-05); nothing is advertised when
	// the declaration names none.
	applyHTTPContext(req, doc, target.SecurityServer, asyncOp, args.Context)

	resp, err := client.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return // cancellation is already terminal
		}
		h.FireError(&ExecutionError{Code: ErrCodeConnectFailed, Message: err.Error()})
		return
	}
	defer func() { _ = resp.Body.Close() }()

	// Classification (§9.4, ASYNC-P-06): a unary publish succeeds IFF the
	// final status, after any redirects, is 2xx. Mirrors the SSE
	// establishment path's strict-2xx test in this same file — a 3xx final
	// (304, or a Location-less redirect fetch/net.http does not follow) is a
	// publish the server plausibly did not accept, never a success.
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		h.FireError(httpStatusError(resp))
		return
	}

	_ = h.SetHeader(headerMetadata(resp.Header))

	// The unary reply body is one delivery unit: consumer-bounded via
	// executionArgs.MaxDeliveryUnitBytes (default 10 MiB). The +1
	// sentinel distinguishes an at-limit response from an over-limit one.
	maxUnit := args.DeliveryUnitLimit()
	respBody, err := io.ReadAll(io.LimitReader(resp.Body, maxUnit+1))
	if err != nil {
		if ctx.Err() != nil {
			return
		}
		h.FireError(&ExecutionError{Code: ErrCodeResponseError, Message: err.Error()})
		return
	}
	if int64(len(respBody)) > maxUnit {
		h.FireError(&ExecutionError{
			Code:    ErrCodeResponseError,
			Message: fmt.Sprintf("response exceeds %d byte limit", maxUnit),
		})
		return
	}

	if len(respBody) == 0 {
		// An empty body (202/204 acknowledgments included) yields no output
		// value: an acknowledgment is not a message and emits no value (§8).
		h.CloseOutput()
		return
	}
	if asyncOp.Reply == nil {
		h.CloseOutput()
		return
	}
	replyDecode, rderr := resolveReplyContentType(doc, asyncOp, resp.StatusCode, resp.Header.Get("Content-Type"), args.Context)
	if rderr != nil {
		h.FireError(&ExecutionError{Code: ErrCodeProtocol, Message: rderr.Error()})
		return
	}
	replyAvro, raerr := resolveAvroBinaryCodec(messagesForDecodeCT(doc, replyGoverningMessages(doc, asyncOp), replyDecode), args.Context)
	if raerr != nil {
		h.FireError(&ExecutionError{Code: ErrCodeProtocol, Message: raerr.Error()})
		return
	}

	status := resp.StatusCode
	raw := RawResult{Status: &status, Body: respBody, Meta: headerMetadata(resp.Header)}
	output, derr := args.Hooks.DecodeOutput(siteFor(args, target.ServerURL), raw,
		builtinDecodeFor(replyDecode, replyAvro))
	if derr != nil {
		h.FireError(asExecutionError(derr))
		return
	}
	// A headers-declaring reply rides the routed envelope on the output
	// direction too: the payload pairs with the declared application
	// headers projected from the HTTP response's fields (§9.2).
	replyCandidates := messagesForDecodeCT(doc, replyGoverningMessages(doc, asyncOp), replyDecode)
	for _, m := range replyCandidates {
		if m.Headers != nil {
			output = map[string]any{
				"payload": output,
				"headers": projectResponseHeaders(replyCandidates, resp.Header),
			}
			break
		}
	}

	// Success provenance stamps (the conventions record,
	// spec/binding-specs/README.md): decode is spec/content-type (the message's
	// declared contentType decides the lane), hook when overridden;
	// classify is not-consulted (asyncapi runs no result classifier — the
	// HTTP 4xx guard above is transport, not a format verdict).
	h.SetTrailer(decodeTrailer(args.Hooks, "spec/content-type"))
	if h.EmitOutput(output) != nil {
		return // invocation terminated while the emit was parked
	}
	h.CloseOutput()
}

// ---------------------------------------------------------------------------
// Subscribe over HTTP (`send` action): SSE
// ---------------------------------------------------------------------------

func runSSESubscribe(ctx context.Context, client *http.Client, target resolvedTarget, address string, doc *document, ch *channel, asyncOp *asyncOperation, args *executionArgs, h handle) {
	// The described application sends; we subscribe. An SSE subscription
	// takes no input: input closes on entry, and a late write rejects
	// non-terminally at the handle (the refusal surface for supplied input).
	_ = h.CloseInput()

	// The decode codec resolves BEFORE the request: an unqualifiable Avro
	// declaration (invalid schema, ambiguous candidates, bad framing
	// configuration) refuses with the never-dispatched guarantee.
	sseMessages := governingMessages(doc, asyncOp, ch)
	decodeAvro, avroErr := resolveAvroBinaryCodec(sseMessages, args.Context)
	if avroErr != nil {
		h.FireError(&ExecutionError{Code: ErrCodeRefused, Message: avroErr.Error()})
		return
	}

	// The subscription framing is this specification's own pin (§8): the
	// request is a GET unless the http operation binding declares otherwise
	// (ASYNC-P-02: bindings are authoritative where they speak).
	req, err := http.NewRequestWithContext(ctx, requestMethod(asyncOp, http.MethodGet), joinURL(target.ServerURL, address), nil)
	if err != nil {
		h.FireError(asExecutionError(err))
		return
	}
	req.Header.Set("Accept", "text/event-stream")
	applyHTTPContext(req, doc, target.SecurityServer, asyncOp, args.Context)

	resp, err := client.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return
		}
		h.FireError(&ExecutionError{Code: ErrCodeConnectFailed, Message: err.Error()})
		return
	}
	defer func() { _ = resp.Body.Close() }()

	// Establishment (§8, ASYNC-P-06): a 2xx response bearing the
	// text/event-stream content type, judged on the FINAL response after
	// any redirects (the client followed them; resp is final). Anything
	// else is a failure — non-2xx classifies as the transport status does;
	// a 2xx without the pinned framing is a protocol error, never a silent
	// reclassification.
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		h.FireError(httpStatusError(resp))
		return
	}
	if ct := resp.Header.Get("Content-Type"); normalizeMediaType(ct) != "text/event-stream" {
		h.FireError(&ExecutionError{
			Code:    ErrCodeProtocol,
			Message: fmt.Sprintf("SSE subscription establishment requires a text/event-stream response, got content type %q (asyncapi §8)", ct),
		})
		return
	}

	_ = h.SetHeader(headerMetadata(resp.Header))

	// One transport, one invocation: transport close COMPLETES the
	// subscription — reconnection (`retry`, `Last-Event-ID`) is excluded
	// from the built-in SSE profile, so no reconnect is ever attempted here.
	streamSSE(ctx, resp, decodeContentType(doc, sseMessages), decodeAvro, args, siteFor(args, target.ServerURL), h)
}

// streamSSE reads an established text/event-stream response per the WHATWG
// server-sent events processing model — incorporated for EVENT FRAMING ONLY
// (§8) — emitting one output value per event as units arrive (ASYNC-P-05).
// Owns the terminal transition: CloseOutput on clean transport close (which
// COMPLETES the subscription), ERR_STREAM_ERROR on a read failure, a clean
// return when the caller cancels. decodeCT is the governing declared
// content type (decode point default; "" = the text lane).
//
// Event extraction, per the WHATWG model (mirrors openapi/sse.go — format
// packages do not share private helpers):
//
//   - `data:` lines accumulate; an event's data lines joined with U+000A
//     form the event's text
//   - a block that carried no `data` line — comment-only and
//     `event:`/`id:`-only blocks included — dispatches nothing, while a
//     received `data` line whose value is empty dispatches like any other:
//     a lone empty `data:` line emits the empty string (WHATWG dispatch
//     checks the data buffer for emptiness BEFORE the trailing-LF strip)
//   - `event`, `id`, and `retry` are FRAMING: they never enter the output
//     value; they surface out of band on the per-unit Meta
//     (x-sse-event / x-sse-id / x-sse-retry). `retry` is never acted on:
//     reconnection is a built-in-driver exclusion
//   - an incomplete final event (end of stream before its dispatching
//     blank line) is discarded, never flushed
func streamSSE(ctx context.Context, resp *http.Response, decodeCT string, decodeAvro *avroBinaryCodec, args *executionArgs, site invokeSite, h handle) {
	scanner := bufio.NewScanner(resp.Body)
	scanner.Buffer(make([]byte, 64*1024), sseMaxLineBytes)
	scanner.Split(scanSSELines)

	var (
		eventName   string
		lastEventID string
		dataLines   []string
		retryMs     int
		eventBytes  int64
		firstLine   = true
	)
	maxUnit := args.DeliveryUnitLimit()

	status := resp.StatusCode
	invocationMeta := headerMetadata(resp.Header)

	// dispatch emits the accumulated event; false stops the read loop (the
	// invocation terminated, or decode failed terminally).
	dispatch := func() bool {
		hadDataLine := len(dataLines) > 0
		rawData := strings.Join(dataLines, "\n")
		name := eventName
		eventName = ""
		dataLines = nil
		// A block that carried no data line dispatches nothing (WHATWG
		// dispatch step 2: the data buffer is the empty string; comment-only
		// and `event:`/`id:`-only blocks included). The emptiness check
		// precedes the trailing-LF strip, so a received data line whose
		// value is empty dispatches like any other — a lone empty `data:`
		// line emits the empty string.
		if !hadDataLine {
			return true
		}

		// Per-unit Meta: invocation-scoped headers merged with this event's
		// framing fields (out of band — never the output value).
		meta := make(Metadata, len(invocationMeta)+3)
		for k, v := range invocationMeta {
			meta[k] = v
		}
		if name != "" {
			meta["x-sse-event"] = []string{name}
		}
		if lastEventID != "" {
			meta["x-sse-id"] = []string{lastEventID}
		}
		if retryMs != 0 {
			meta["x-sse-retry"] = []string{strconv.Itoa(retryMs)}
			retryMs = 0
		}

		raw := RawResult{Status: &status, Body: []byte(rawData), Meta: meta}
		ev, derr := args.Hooks.DecodeOutput(site, raw, builtinPerEventDecodeFor(decodeCT, decodeAvro))
		if derr != nil {
			// A decode error mid-stream is terminal; already-emitted
			// outputs stand (drain-before-terminal).
			h.FireError(asExecutionError(derr))
			return false
		}
		return h.EmitOutput(ev) == nil
	}

	for scanner.Scan() {
		if ctx.Err() != nil {
			return // cancelled; the handle is already terminal
		}
		line := scanner.Text()
		if firstLine {
			// One leading U+FEFF BOM is ignored per the WHATWG grammar.
			line = strings.TrimPrefix(line, "\uFEFF")
			firstLine = false
		}

		// The size cap is PER EVENT, not cumulative: a long-lived
		// subscription legitimately streams more than one delivery unit in
		// total (the same choice connect/streaming.go documents for its
		// per-envelope cap). One event is one delivery unit, consumer-bounded
		// via executionArgs.MaxDeliveryUnitBytes (default 10 MiB).
		eventBytes += int64(len(line)) + 1 // +1 for newline
		if eventBytes > maxUnit {
			h.FireError(&ExecutionError{
				Code:    ErrCodeResponseError,
				Message: fmt.Sprintf("SSE event exceeds %d byte limit", maxUnit),
			})
			return
		}

		if line == "" {
			eventBytes = 0
			if !dispatch() {
				return
			}
			continue
		}
		if strings.HasPrefix(line, ":") {
			continue // comment line; ignored per spec
		}

		var field, value string
		if i := strings.IndexByte(line, ':'); i >= 0 {
			field = line[:i]
			value = line[i+1:]
			// Exactly one leading space in the value is stripped, per spec.
			if strings.HasPrefix(value, " ") {
				value = value[1:]
			}
		} else {
			// A line with no colon is a field with an empty value.
			field = line
			value = ""
		}

		switch field {
		case "event":
			eventName = value
		case "id":
			// A value containing U+0000 NULL is ignored; otherwise it sets
			// the last event ID (an empty value resets it), per WHATWG.
			if !strings.ContainsRune(value, '\x00') {
				lastEventID = value
			}
		case "data":
			dataLines = append(dataLines, value)
		case "retry":
			// ASCII digits only, per WHATWG; recorded on Meta only — never
			// acted on (reconnection is excluded from the built-in driver).
			if value != "" && strings.IndexFunc(value, func(r rune) bool { return r < '0' || r > '9' }) < 0 {
				if ms, err := strconv.Atoi(value); err == nil {
					retryMs = ms
				}
			}
		}
		// Unknown fields are ignored per spec.
	}

	// End of stream: an incomplete final event (no dispatching blank line)
	// is discarded per the WHATWG processing model — never flushed.

	if serr := scanner.Err(); serr != nil {
		if ctx.Err() == nil {
			h.FireError(&ExecutionError{Code: ErrCodeStreamError, Message: serr.Error()})
		}
		return
	}
	h.CloseOutput()
}

// scanSSELines is a bufio.SplitFunc for the WHATWG event-stream line
// grammar: lines end with CRLF, a lone LF, or a lone CR. A CR at the end of
// the buffered data waits for more input (the LF of a CRLF pair may not
// have arrived yet) unless the stream is at EOF. Mirrors openapi/sse.go.
func scanSSELines(data []byte, atEOF bool) (advance int, token []byte, err error) {
	if atEOF && len(data) == 0 {
		return 0, nil, nil
	}
	if i := bytes.IndexAny(data, "\r\n"); i >= 0 {
		if data[i] == '\n' {
			return i + 1, data[:i], nil
		}
		// CR: swallow a following LF when it is available.
		if i+1 < len(data) {
			if data[i+1] == '\n' {
				return i + 2, data[:i], nil
			}
			return i + 1, data[:i], nil
		}
		if atEOF {
			return i + 1, data[:i], nil
		}
		return 0, nil, nil // need more data to decide CR vs CRLF
	}
	if atEOF {
		return len(data), data, nil
	}
	return 0, nil, nil
}

// ---------------------------------------------------------------------------
// WebSocket frames
// ---------------------------------------------------------------------------

// decodeWSFrame decodes one socket frame through the consultation seam:
// Status is NIL (a WS frame has no scalar completion status — never
// fabricated); the builtin follows the DECLARED message contentType. The
// former `{"error"}`/`{"data"}` convention unwrapping left the builtin —
// it is the hook channel's business now (a returned decode error is
// terminal, which is exactly the override channel for error-frame
// conventions).
func decodeWSFrame(args *executionArgs, site invokeSite, contentType string, avro *avroBinaryCodec, frame []byte) (any, error) {
	raw := RawResult{Body: frame}
	return args.Hooks.DecodeOutput(site, raw, builtinDecodeFor(contentType, avro))
}

// ---------------------------------------------------------------------------
// Subscribe over WebSocket (`send` action): server-streaming, on a pooled socket
// ---------------------------------------------------------------------------

type wsReplyExchange struct {
	InputMessages  []message
	OutputMessages []message
	RequireInput   bool
	IsolationKey   string
	Prepared       *preparedInput
	InputLane      wsReplyLane
}

func runWSSubscribe(ctx context.Context, pool *wsPool, target resolvedTarget, address string, extraHeaders map[string]string, doc *document, ch *channel, asyncOp *asyncOperation, args *executionArgs, h handle, exchange *wsReplyExchange) {
	if exchange == nil {
		_ = h.CloseInput()
	}
	outputMessages := governingMessages(doc, asyncOp, ch)
	if exchange != nil {
		outputMessages = exchange.OutputMessages
	}
	decodeCT, decodeErr := resolveSubscriptionContentType(doc, outputMessages, args.Context)
	if decodeErr != nil {
		h.FireError(&ExecutionError{Code: ErrCodeRefused, Message: decodeErr.Error()})
		return
	}
	decodeAvro, avroErr := resolveAvroBinaryCodec(outputMessages, args.Context)
	if avroErr != nil {
		h.FireError(&ExecutionError{Code: ErrCodeRefused, Message: avroErr.Error()})
		return
	}

	// The reply-input codec resolves BEFORE any socket is dialed: a
	// known-bad codec must refuse with the never-dispatched guarantee, not
	// dial and then fail post-upgrade.
	var exchangeCodec inputCodec
	if exchange != nil {
		for _, m := range exchange.InputMessages {
			if m.Headers != nil {
				// Raw WebSocket frames have no native header carriage; a
				// headers-declaring reply input refuses per cell (§9.2).
				h.FireError(&ExecutionError{Code: ErrCodeRefused, Message: "the reply input declares application headers; this build has no header carriage for the WebSocket cell"})
				return
			}
		}
		var codecErr error
		exchangeCodec, codecErr = resolveInputCodec(doc, exchange.InputMessages, args.Context)
		if codecErr != nil {
			h.FireError(&ExecutionError{Code: ErrCodeRefused, Message: codecErr.Error()})
			return
		}
	}

	// The subscription is registered inside acquire (before the reader
	// starts on a fresh dial) so no early server push can be lost.
	sub := newWSSubscription()
	isolationKey := ""
	if exchange != nil {
		isolationKey = exchange.IsolationKey
	}
	pw, unsubscribe, err := pool.acquire(ctx, target.ServerURL, address, doc, target.SecurityServer, asyncOp, args.Context, extraHeaders,
		args.DeliveryUnitLimit(), &wsListener{onFrame: sub.push, onClose: sub.close}, isolationKey)
	if err != nil {
		if ctx.Err() != nil {
			return
		}
		h.FireError(&ExecutionError{Code: ErrCodeConnectFailed, Message: err.Error()})
		return
	}
	defer pw.release()
	defer unsubscribe()
	inputPW := pw
	if exchange != nil && (exchange.InputLane.Target.ServerURL != target.ServerURL || exchange.InputLane.Address != address || !equalStringMap(exchange.InputLane.Headers, extraHeaders)) {
		inputListener := &wsListener{onClose: func(closeErr error) {
			if closeErr != nil && ctx.Err() == nil {
				h.FireError(&ExecutionError{Code: ErrCodeStreamError, Message: closeErr.Error()})
			}
		}}
		var removeInputListener func()
		inputPW, removeInputListener, err = pool.acquire(
			ctx, exchange.InputLane.Target.ServerURL, exchange.InputLane.Address, doc,
			exchange.InputLane.Target.SecurityServer, asyncOp, args.Context, exchange.InputLane.Headers,
			args.DeliveryUnitLimit(), inputListener, exchange.IsolationKey,
		)
		if err != nil {
			if ctx.Err() == nil {
				h.FireError(&ExecutionError{Code: ErrCodeConnectFailed, Message: err.Error()})
			}
			return
		}
		defer inputPW.release()
		defer removeInputListener()
	}

	if exchange != nil {
		codec := exchangeCodec
		go func() {
			sent := 0
			for {
				var value any
				var readErr error
				if exchange.Prepared != nil {
					value = exchange.Prepared.Value
					exchange.Prepared = nil
				} else {
					value, readErr = h.ReadInput(ctx)
				}
				if readErr == io.EOF {
					if exchange.RequireInput && sent == 0 {
						h.FireError(&ExecutionError{Code: ErrCodeMissingInput, Message: "request/reply invocation requires at least one input message"})
					}
					return
				}
				if readErr != nil {
					return
				}
				frame, encodeErr := args.Hooks.EncodeInput(siteFor(args, target.ServerURL), value, func(v any) ([]byte, error) { return encodeInput(codec, v) })
				if encodeErr != nil {
					h.FireError(&ExecutionError{Code: ErrCodeValidationFailed, Message: encodeErr.Error()})
					return
				}
				messageType, _ := contextConfiguration(args.Context)["websocketMessageType"].(string)
				if messageType == "text" && !utf8.Valid(frame) {
					h.FireError(&ExecutionError{Code: ErrCodeValidationFailed, Message: "WebSocket text message payload is not valid UTF-8"})
					return
				}
				if sendErr := inputPW.sendType(ctx, frame, messageType); sendErr != nil {
					if ctx.Err() == nil {
						h.FireError(&ExecutionError{Code: ErrCodeStreamError, Message: sendErr.Error()})
					}
					return
				}
				sent++
			}
		}()
	}

	// Socket -> outputs. Owns the terminal transition: clean socket close ->
	// CloseOutput; socket error or backpressure overflow -> ERR_STREAM_ERROR.
	// Overflow fails only this subscription — the shared reader keeps
	// broadcasting to the pooled connection's other listeners untouched.
	for {
		res, ok := sub.next(ctx)
		if !ok {
			return // invocation terminated (cancelled) while waiting
		}
		if res.Overflowed {
			h.FireError(&ExecutionError{Code: ErrCodeStreamError, Message: res.OverflowMsg})
			return
		}
		if res.Closed {
			if res.CloseErr != nil {
				h.FireError(&ExecutionError{Code: ErrCodeStreamError, Message: res.CloseErr.Error()})
			} else {
				h.CloseOutput()
			}
			return
		}
		out, derr := decodeWSFrame(args, siteFor(args, target.ServerURL), decodeCT, decodeAvro, res.Frame)
		if derr != nil {
			// A decode error mid-stream is terminal; already-emitted
			// outputs stand (drain-before-terminal).
			h.FireError(asExecutionError(derr))
			return
		}
		if h.EmitOutput(out) != nil {
			return // invocation terminated while the emit was parked
		}
	}
}

// Backpressure bounds for the undelivered-frame buffer between a pooled
// socket's broadcast and one subscription's consumer: whichever
// bound trips first fails THAT subscription loudly rather than buffering
// unboundedly (bounded-queue-fail-loud, per spec/binding-specs/asyncapi/asyncapi.md's WS
// slow-consumer ruling — Redis client-output-buffer-limit, NATS
// slow-consumer, and MQTT max_queued_messages are the pub/sub-ecosystem
// precedent, and NATS pairs a count bound with a byte bound the same way).
// Reference-package defaults, not spec-mandated numbers. var, not const, so
// tests can lower them instead of pushing the full volume through a test
// socket.
var (
	maxWSBufferedFrames = 1024
	maxWSBufferedBytes  = 64 * 1024 * 1024 // 64 MiB
)

// wsSubscription buffers broadcast frames from a pooled socket for one
// consumer, preserving arrival order without blocking the shared reader
// goroutine. The buffer is itself bounded (maxWSBufferedFrames /
// maxWSBufferedBytes, whichever trips first): once a consumer stops
// draining, push() stops accepting new frames for THIS subscription only —
// the shared reader keeps broadcasting to every other listener on the
// pooled connection unaffected — and next() surfaces the terminal error
// after draining whatever was already buffered (drain-before-terminal).
// runWSSubscribe's deferred unsubscribe() detaches this listener once the
// terminal fires; the pooled socket itself is never touched.
type wsSubscription struct {
	mu          sync.Mutex
	frames      [][]byte
	bufBytes    int
	closed      bool
	closeErr    error
	overflowed  bool
	overflowMsg string
	notify      chan struct{} // 1-buffered wake signal
}

func newWSSubscription() *wsSubscription {
	return &wsSubscription{notify: make(chan struct{}, 1)}
}

// push appends a broadcast frame unless the subscription has already
// overflowed or closed. Tripping either bound marks the subscription
// overflowed and drops this (and every subsequent) frame — the tripping
// frame itself is never buffered, mirroring the TS reference invoker.
func (s *wsSubscription) push(frame []byte) {
	s.mu.Lock()
	if s.overflowed || s.closed {
		s.mu.Unlock()
		return
	}
	switch {
	case len(s.frames) >= maxWSBufferedFrames:
		s.overflowed = true
		s.overflowMsg = fmt.Sprintf("backpressure overflow: more than %d undelivered frames", maxWSBufferedFrames)
	case s.bufBytes+len(frame) > maxWSBufferedBytes:
		s.overflowed = true
		s.overflowMsg = fmt.Sprintf("backpressure overflow: more than %d undelivered bytes", maxWSBufferedBytes)
	default:
		s.frames = append(s.frames, frame)
		s.bufBytes += len(frame)
	}
	s.mu.Unlock()
	s.wake()
}

func (s *wsSubscription) close(err error) {
	s.mu.Lock()
	s.closed = true
	s.closeErr = err
	s.mu.Unlock()
	s.wake()
}

func (s *wsSubscription) wake() {
	select {
	case s.notify <- struct{}{}:
	default:
	}
}

// wsSubResult is one dequeued step of a wsSubscription: exactly one of
// (Frame set), Overflowed, or Closed is true.
type wsSubResult struct {
	Frame       []byte
	Overflowed  bool
	OverflowMsg string
	Closed      bool
	CloseErr    error
}

// next returns the next frame (buffered frames always drain first before
// Overflowed or Closed surfaces), or ok=false when ctx ends first.
func (s *wsSubscription) next(ctx context.Context) (res wsSubResult, ok bool) {
	for {
		s.mu.Lock()
		if len(s.frames) > 0 {
			frame := s.frames[0]
			s.frames = s.frames[1:]
			s.bufBytes -= len(frame)
			s.mu.Unlock()
			return wsSubResult{Frame: frame}, true
		}
		if s.overflowed {
			msg := s.overflowMsg
			s.mu.Unlock()
			return wsSubResult{Overflowed: true, OverflowMsg: msg}, true
		}
		if s.closed {
			err := s.closeErr
			s.mu.Unlock()
			return wsSubResult{Closed: true, CloseErr: err}, true
		}
		s.mu.Unlock()

		select {
		case <-s.notify:
		case <-ctx.Done():
			return wsSubResult{}, false
		}
	}
}

// ---------------------------------------------------------------------------
// Publish over WebSocket (`receive` action): client-streaming, pooled socket
// ---------------------------------------------------------------------------

func runWSPublish(ctx context.Context, pool *wsPool, target resolvedTarget, address string, extraHeaders map[string]string, doc *document, ch *channel, asyncOp *asyncOperation, args *executionArgs, h handle, prepared *preparedInput) {
	// A publish invocation requires input — the input IS the message
	// (ASYNC-P-03); this family defines no empty message. An operation-layer
	// call for an operation declaring no input is refused before dispatch
	// (callers of no-input operations never write, so reading would park).
	if args.AcceptsInput != nil && !*args.AcceptsInput {
		h.FireError(&ExecutionError{
			Code:    ErrCodeRefused,
			Message: "publish invocation requires an input message (the input is the message; the operation declares no input)",
		})
		return
	}

	// Input encoding follows the governing request-side declaration
	// (ASYNC-P-03); an excluded declared family refuses BEFORE dispatch —
	// before any socket is dialed.
	selected, cerr := selectedInputMessages(doc, asyncOp, ch, args.Context)
	if cerr != nil {
		h.FireError(&ExecutionError{Code: ErrCodeRefused, Message: cerr.Error()})
		return
	}
	codec, cerr := resolveInputCodec(doc, selected, args.Context)
	if cerr != nil {
		h.FireError(&ExecutionError{Code: ErrCodeRefused, Message: cerr.Error()})
		return
	}

	pw, _, err := pool.acquire(ctx, target.ServerURL, address, doc, target.SecurityServer, asyncOp, args.Context, extraHeaders, args.DeliveryUnitLimit(), nil)
	if err != nil {
		if ctx.Err() != nil {
			return
		}
		h.FireError(&ExecutionError{Code: ErrCodeConnectFailed, Message: err.Error()})
		return
	}
	defer pw.release()

	// Client-streaming publish: every input is one frame; the caller closing
	// input after at least one message completes the call with zero outputs
	// (a publish yields no outputs; frames the server sends during the
	// exchange are discarded — a defined disposal; auth rides the upgrade
	// request, never the message body). Closing with zero messages sent is
	// the streaming face of the same refusal: nothing was published.
	sent := 0
	for {
		var msg any
		var rerr error
		if prepared != nil {
			msg = prepared.Value
			prepared = nil
		} else {
			msg, rerr = h.ReadInput(ctx)
		}
		if rerr == io.EOF {
			if sent == 0 {
				h.FireError(&ExecutionError{
					Code:    ErrCodeMissingInput,
					Message: "publish invocation requires an input message (input closed with no messages sent)",
				})
				return
			}
			h.CloseOutput()
			return
		}
		if rerr != nil {
			return // invocation already terminal (or cancelled)
		}
		frame, merr := args.Hooks.EncodeInput(siteFor(args, target.ServerURL), msg, func(v any) ([]byte, error) { return encodeInput(codec, v) })
		if merr != nil {
			h.FireError(&ExecutionError{Code: ErrCodeValidationFailed, Message: merr.Error()})
			return
		}
		messageType, _ := contextConfiguration(args.Context)["websocketMessageType"].(string)
		if messageType == "text" && !utf8.Valid(frame) {
			h.FireError(&ExecutionError{Code: ErrCodeValidationFailed, Message: "WebSocket text message payload is not valid UTF-8"})
			return
		}
		if werr := pw.sendType(ctx, frame, messageType); werr != nil {
			// Broken socket: evict it so the next caller dials a fresh one.
			pool.evict(pw)
			if ctx.Err() != nil {
				return
			}
			h.FireError(&ExecutionError{Code: ErrCodeStreamError, Message: werr.Error()})
			return
		}
		sent++
	}
}

// ---------------------------------------------------------------------------
// Utilities
// ---------------------------------------------------------------------------

// headerMetadata converts HTTP response headers to invocation metadata.
// Keys are lowercased for cross-SDK portability (the TS SDK's Headers
// iteration yields lowercase keys).
func headerMetadata(hdr http.Header) Metadata {
	md := make(Metadata, len(hdr))
	for k, vs := range hdr {
		md[strings.ToLower(k)] = append([]string(nil), vs...)
	}
	return md
}

// httpStatusError builds the terminal error for an HTTP error response,
// attaching the bounded body to the explicit diagnostic lane.
func httpStatusError(resp *http.Response) *ExecutionError {
	ie := httpError(resp.StatusCode, resp.Status)
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes+1))
	if err != nil || len(body) == 0 {
		return ie
	}
	details := map[string]any{"status": resp.StatusCode}
	if len(body) > maxResponseBytes {
		details["body"] = fmt.Sprintf("response exceeds %d byte limit", maxResponseBytes)
	} else {
		// The raw capture, verbatim: diagnostics carry bytes-as-text,
		// never a sniffed parse (payload-independence, per the conventions
		// record's recommended built-in defaults).
		details["body"] = string(body)
	}
	ie.Diagnostics = details
	return ie
}

// ---------------------------------------------------------------------------
// Credential application
// ---------------------------------------------------------------------------

// applyHTTPContext applies opaque binding context (credentials via well-known
// fields) and execution options (headers, cookies) to an HTTP request, using
// AsyncAPI securitySchemes for spec-driven credential placement. secSrv is
// the server whose declared security applies (§9.5).
func applyHTTPContext(req *http.Request, doc *document, secSrv *server, asyncOp *asyncOperation, bindCtx map[string]any) {
	if len(bindCtx) > 0 {
		_, queryParams := applyCredentialsViaSecuritySchemes(req, doc, secSrv, asyncOp, bindCtx)
		if len(queryParams) > 0 {
			q := req.URL.Query()
			for k, vs := range queryParams {
				for _, v := range vs {
					q.Set(k, v)
				}
			}
			req.URL.RawQuery = q.Encode()
		}
	}

	for k, v := range contextHeaders(bindCtx) {
		req.Header.Set(k, v)
	}
	for k, v := range contextCookies(bindCtx) {
		req.AddCookie(&http.Cookie{Name: k, Value: v})
	}
}

// namedSecurityScheme pairs a resolved AsyncAPI security scheme with the
// components.securitySchemes key its requirement entry's $ref resolved
// through (empty for an inline scheme) — the same name requiredContext
// stamps onto the Requirement's Name (R2.a ruling), needed here so
// credential application can look up a NAMED apiKey scheme's key via
// contextAPIKeyFor without re-deriving it.
type namedSecurityScheme struct {
	Name   string
	Scheme securityScheme
}

// resolveSecuritySchemes returns the security schemes applicable to an
// operation, flattened for credential placement: the targeted server's list
// then the operation's list, in declaration order — both apply, the
// conjunctive reading (ASYNC-P-07) — with a scheme declared on both levels
// placed once. An entry that fails to resolve (a dangling $ref) is dropped.
func resolveSecuritySchemes(doc *document, secSrv *server, asyncOp *asyncOperation) []namedSecurityScheme {
	var result []namedSecurityScheme
	seen := map[string]bool{}
	for _, requirements := range [][]securityRequirement{serverSecurityRequirements(secSrv), operationSecurityRequirements(asyncOp)} {
		for _, req := range requirements {
			if scheme, ok := resolveSecurityRequirement(doc, req); ok {
				key := scheme.Type + "\x00" + scheme.Scheme + "\x00" + securityRequirementName(req)
				if seen[key] {
					continue
				}
				seen[key] = true
				result = append(result, namedSecurityScheme{Name: securityRequirementName(req), Scheme: scheme})
			}
		}
	}
	return result
}

// applyCredentialsViaSecuritySchemes reads the AsyncAPI doc's securitySchemes
// and operation/server-level security requirements to place credentials exactly
// where the spec declares (header, query, or cookie with the correct name).
// An apiKey/httpApiKey scheme looks up its credential by the
// securitySchemes key first (R2.d ruling: context.apiKeys[name]), falling
// back to the single context.apiKey.
func applyCredentialsViaSecuritySchemes(req *http.Request, doc *document, secSrv *server, asyncOp *asyncOperation, bindCtx map[string]any) (applied bool, queryParams url.Values) {
	schemes := resolveSecuritySchemes(doc, secSrv, asyncOp)
	if len(schemes) == 0 {
		return false, nil
	}

	queryParams = url.Values{}

	for _, named := range schemes {
		s := named.Scheme
		switch s.Type {
		case "apiKey", "httpApiKey":
			val := contextAPIKeyFor(bindCtx, named.Name)
			if val == "" {
				continue
			}
			switch s.In {
			case "header":
				name := s.Name
				if name == "" {
					name = "Authorization"
				}
				req.Header.Set(name, val)
				applied = true
			case "query":
				if s.Name != "" {
					queryParams.Set(s.Name, val)
					applied = true
				}
			case "cookie":
				if s.Name != "" {
					req.AddCookie(&http.Cookie{Name: s.Name, Value: val})
					applied = true
				}
			}

		case "http":
			switch strings.ToLower(s.Scheme) {
			case "bearer":
				if token := contextBearerTokenFor(bindCtx, named.Name); token != "" {
					req.Header.Set("Authorization", "Bearer "+token)
					applied = true
				}
			case "basic":
				if u, p, ok := contextBasicAuthFor(bindCtx, named.Name); ok {
					req.SetBasicAuth(u, p)
					applied = true
				}
			}

		case "httpBearer":
			if token := contextBearerTokenFor(bindCtx, named.Name); token != "" {
				req.Header.Set("Authorization", "Bearer "+token)
				applied = true
			}

		case "oauth2":
			token := contextAccessTokenFor(bindCtx, named.Name)
			if token == "" {
				token = contextBearerTokenFor(bindCtx, named.Name)
			}
			if token != "" {
				req.Header.Set("Authorization", "Bearer "+token)
				applied = true
			}

		case "userPassword":
			if u, p, ok := contextBasicAuthFor(bindCtx, named.Name); ok {
				req.SetBasicAuth(u, p)
				applied = true
			}
		}
	}

	return applied, queryParams
}

// builtinDecodeFor is the asyncapi builtin decoder: strict JSON when the
// DECLARED message contentType is application/json or a +json suffix
// (a declared-JSON payload that fails to parse is a lying producer — a
// loud terminal, never a silent string), text otherwise. The
// `{"error":...}`/`{"data":...}` convention unwrapping LEFT the builtin
// (round-4 unification): a consumer whose stream speaks it attaches an
// outputDecoder — a returned error is terminal, which IS the override
// channel for error-frame conventions.
func builtinDecodeFor(contentType string, avro *avroBinaryCodec) outputDecoder {
	perEvent := builtinPerEventDecodeFor(contentType, avro)
	return func(site invokeSite, raw RawResult) (any, error) {
		// An empty delivery unit emits no value. This rule is
		// whole-unit-scoped (an HTTP reply, a WS frame): the SSE per-event
		// lane bypasses it via builtinPerEventDecodeFor — a DISPATCHED
		// event whose data text is empty (a lone empty `data:` line,
		// WHATWG) is a value, never an absent output.
		if len(raw.Body) == 0 {
			return nil, nil
		}
		return perEvent(site, raw)
	}
}

// builtinPerEventDecodeFor is builtinDecodeFor's declaration-keyed lane set
// without the empty-unit→no-value rule, used by the SSE per-event lane
// where an empty data text is the empty-string value under the text lane
// (and a declared-JSON contentType judges it as any other non-JSON text —
// a loud terminal).
func builtinPerEventDecodeFor(contentType string, avro *avroBinaryCodec) outputDecoder {
	isJSON := isJSONContentType(contentType)
	isBytes := contentType != "" && !isJSON && !isTextContentType(contentType)
	return func(_ invokeSite, raw RawResult) (any, error) {
		if avro != nil && !isJSON {
			// The named Avro correspondence's binary wire: octets decode to
			// the logical value through the qualified codec (a JSON-family
			// declaration instead carries the Avro-JSON encoding, which the
			// ordinary JSON branch below parses).
			value, err := avro.decode(raw.Body)
			if err != nil {
				return nil, &ExecutionError{Code: ErrCodeResponseError, Message: err.Error()}
			}
			return value, nil
		}
		if isBytes {
			// The byte boundary: exact octets as the canonical Base64 string.
			return base64.StdEncoding.EncodeToString(raw.Body), nil
		}
		if isJSON {
			var parsed any
			if err := json.Unmarshal(raw.Body, &parsed); err != nil {
				return nil, &ExecutionError{
					Code:    ErrCodeResponseError,
					Message: fmt.Sprintf("message declares %q but the payload is not valid JSON: %v", contentType, err),
				}
			}
			return parsed, nil
		}
		if !utf8.Valid(raw.Body) {
			return nil, &ExecutionError{
				Code:    ErrCodeResponseError,
				Message: fmt.Sprintf("message declares %q but the payload is not valid UTF-8", contentType),
			}
		}
		return string(raw.Body), nil
	}
}

// decodeTrailer builds the x-ob-decode provenance stamp (the conventions
// record, spec/binding-specs/README.md) — and the fixed x-ob-classify
// not-consulted stamp, asyncapi runs no classifier — for a
// successful message decode, given the builtin decode provenance token.
func decodeTrailer(hooks *invokeHooks, builtinDecode string) Metadata {
	decode := builtinDecode
	if hooks.DecodeDecidedBy() == "hook" {
		decode = "hook"
	}
	return Metadata{
		"x-ob-decode":   {decode},
		"x-ob-classify": {"not-consulted"},
	}
}

// isJSONContentType mirrors the openapi rule: application/json or any
// +json structured-suffix type; absent/unparseable → NOT JSON. Never
// sniffed.
func isJSONContentType(contentType string) bool {
	mt := normalizeMediaType(contentType)
	return mt == "application/json" || strings.HasSuffix(mt, "+json")
}

// siteFor completes the core-stamped site with the format-known Target
// (the resolved server URL).
func siteFor(args *executionArgs, serverURL string) invokeSite {
	var site invokeSite
	if args.Site != nil {
		site = *args.Site
	} else {
		site.Profile = args.Source.Profile
		site.Ref = args.Ref
	}
	if site.Target == "" {
		site.Target = serverURL
	}
	return site
}
