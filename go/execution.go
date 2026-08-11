package asyncapiclient

import (
	"context"
	"io"
	"sync"
)

const (
	inputBufferCapacity  = 1
	outputBufferCapacity = 4
)

// Execution is one AsyncAPI operation session. Inputs and events preserve
// ordering and apply bounded backpressure; Wait reports the terminal outcome.
type Execution struct {
	ctx    context.Context
	cancel context.CancelFunc

	inputs    chan any
	inputDone chan struct{}
	events    chan Event
	done      chan struct{}

	mu             sync.Mutex
	inputClosed    bool
	inputRequested bool
	terminal       bool
	err            error
	cancelErr      error
	diagnostics    Diagnostics
	inputOnce      sync.Once
	doneOnce       sync.Once
}

func newExecution(parent context.Context, inputRequested bool) *Execution {
	if parent == nil {
		parent = context.Background()
	}
	ctx, cancel := context.WithCancel(parent)
	return &Execution{
		ctx: ctx, cancel: cancel,
		inputs: make(chan any, inputBufferCapacity), inputDone: make(chan struct{}),
		events: make(chan Event, outputBufferCapacity), done: make(chan struct{}),
		inputRequested: inputRequested,
		diagnostics:    Diagnostics{Leading: Metadata{}, Trailing: Metadata{}},
	}
}

func (e *Execution) Send(ctx context.Context, value any) error {
	e.mu.Lock()
	if e.terminal {
		err := e.err
		e.mu.Unlock()
		if err != nil {
			return err
		}
		return &ExecutionError{Code: ErrCodeInvocationClosed, Message: "execution is closed"}
	}
	if e.inputClosed {
		e.mu.Unlock()
		return &ExecutionError{Code: ErrCodeInputClosed, Message: "execution input is closed"}
	}
	e.mu.Unlock()
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case e.inputs <- value:
		return nil
	case <-e.inputDone:
		return &ExecutionError{Code: ErrCodeInputClosed, Message: "execution input is closed"}
	case <-ctx.Done():
		return asExecutionError(ctx.Err())
	case <-e.done:
		return e.terminalError()
	}
}

func (e *Execution) FinishInput() error { return e.CloseInput() }

func (e *Execution) Cancel() {
	e.mu.Lock()
	if !e.terminal && e.cancelErr == nil {
		e.cancelErr = &ExecutionError{Code: ErrCodeCancelled, Message: "execution cancelled"}
	}
	e.mu.Unlock()
	e.cancel()
}

func (e *Execution) Events() <-chan Event  { return e.events }
func (e *Execution) Done() <-chan struct{} { return e.done }

func (e *Execution) Wait() error {
	<-e.done
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.err
}

func (e *Execution) Diagnostics() Diagnostics {
	e.mu.Lock()
	defer e.mu.Unlock()
	return Diagnostics{Leading: cloneMetadata(e.diagnostics.Leading), Trailing: cloneMetadata(e.diagnostics.Trailing)}
}

func (e *Execution) InputRequested() bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.inputRequested
}

func (e *Execution) ReadInput(ctx context.Context) (any, error) {
	for {
		e.mu.Lock()
		closed := e.inputClosed
		e.mu.Unlock()
		select {
		case value := <-e.inputs:
			return value, nil
		default:
			if closed {
				return nil, io.EOF
			}
		}
		select {
		case value := <-e.inputs:
			return value, nil
		case <-e.inputDone:
			continue
		case <-e.done:
			return nil, e.terminalError()
		case <-ctx.Done():
			return nil, asExecutionError(ctx.Err())
		}
	}
}

func (e *Execution) CloseInput() error {
	e.inputOnce.Do(func() {
		e.mu.Lock()
		e.inputClosed = true
		e.mu.Unlock()
		close(e.inputDone)
	})
	return nil
}

func (e *Execution) EmitOutput(value any) error {
	e.mu.Lock()
	metadata := cloneMetadata(e.diagnostics.Leading)
	e.mu.Unlock()
	select {
	case e.events <- Event{Value: value, Metadata: metadata}:
		return nil
	case <-e.done:
		return e.terminalError()
	case <-e.ctx.Done():
		return asExecutionError(e.ctx.Err())
	}
}

func (e *Execution) CloseOutput() { e.finish(nil) }

func (e *Execution) FireError(err *ExecutionError) { e.finish(err) }

func (e *Execution) SetHeader(metadata Metadata) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.terminal {
		return e.terminalErrorLocked()
	}
	e.diagnostics.Leading = cloneMetadata(metadata)
	return nil
}

func (e *Execution) SetTrailer(metadata Metadata) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if !e.terminal {
		e.diagnostics.Trailing = cloneMetadata(metadata)
	}
}

func (e *Execution) finishAfterRun() {
	e.mu.Lock()
	if e.terminal {
		e.mu.Unlock()
		return
	}
	err := e.cancelErr
	if err == nil && e.ctx.Err() != nil {
		err = asExecutionError(e.ctx.Err())
	}
	if err == nil {
		err = &ExecutionError{Code: ErrCodeRuntime, Message: "AsyncAPI execution returned without a terminal outcome"}
	}
	e.mu.Unlock()
	e.finish(err)
}

func (e *Execution) finish(err error) {
	e.doneOnce.Do(func() {
		e.mu.Lock()
		e.terminal = true
		e.inputClosed = true
		e.err = err
		e.mu.Unlock()
		close(e.events)
		close(e.done)
		e.cancel()
	})
}

func (e *Execution) terminalError() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.terminalErrorLocked()
}

func (e *Execution) terminalErrorLocked() error {
	if e.err != nil {
		return e.err
	}
	return &ExecutionError{Code: ErrCodeInvocationClosed, Message: "execution is closed"}
}
