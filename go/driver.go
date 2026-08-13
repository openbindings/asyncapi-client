package asyncapiclient

import (
	"context"
	"fmt"
	"strings"
)

// ProtocolDriver executes one or more AsyncAPI server protocols. It receives
// the normalized artifact so protocol-specific bindings remain authoritative.
type ProtocolDriver interface {
	Protocols() []string
	Execute(context.Context, DriverRequest, DriverSession) error
}

type DriverRequest struct {
	Artifact     []byte
	Document     map[string]any
	Operation    map[string]any
	Server       map[string]any
	Ref          string
	OperationKey string
	Action       string
	Protocol     string
	ServerURL    string
	Input        *DriverInput
	Output       *DriverOutput
	// SecurityAlternatives contains resolved AsyncAPI schemes. Every outer
	// item is one alternative; all schemes within that item apply.
	SecurityAlternatives [][]DriverSecurityScheme
	Context              map[string]any
}

type DriverDirection struct {
	Channel   map[string]any
	Server    map[string]any
	Protocol  string
	ServerURL string
	Address   string
	Messages  []map[string]any
}

type DriverInput struct {
	DriverDirection
	Encode func(any) ([]byte, error)
}

type DriverOutput struct {
	DriverDirection
	Decode func([]byte) (any, error)
}

type DriverSecurityScheme struct {
	Name   string
	Scheme map[string]any
}

// DriverSession is a cardinality-neutral lifecycle surface. Returning nil
// from Execute completes output if the driver has not already done so.
type DriverSession interface {
	Receive(context.Context) (any, error)
	CloseInput() error
	Emit(any) error
	SetLeadingMetadata(Metadata) error
	SetTrailingMetadata(Metadata)
	Complete()
	Done() <-chan struct{}
}

func indexProtocolDrivers(drivers []ProtocolDriver) (map[string]ProtocolDriver, error) {
	indexed := map[string]ProtocolDriver{}
	for _, driver := range drivers {
		if driver == nil || len(driver.Protocols()) == 0 {
			return nil, fmt.Errorf("an AsyncAPI protocol driver must declare at least one protocol")
		}
		for _, declared := range driver.Protocols() {
			protocol := strings.ToLower(strings.TrimSpace(declared))
			if protocol == "" {
				return nil, fmt.Errorf("an AsyncAPI protocol driver declared an empty protocol")
			}
			if _, exists := indexed[protocol]; exists {
				return nil, fmt.Errorf("more than one AsyncAPI protocol driver declares %q", protocol)
			}
			indexed[protocol] = driver
		}
	}
	return indexed, nil
}

type handleDriverSession struct {
	handle   handle
	prepared *preparedInput
}

func (s *handleDriverSession) Receive(ctx context.Context) (any, error) {
	if s.prepared != nil {
		value := s.prepared.Value
		s.prepared = nil
		return value, nil
	}
	return s.handle.ReadInput(ctx)
}
func (s *handleDriverSession) CloseInput() error    { return s.handle.CloseInput() }
func (s *handleDriverSession) Emit(value any) error { return s.handle.EmitOutput(value) }
func (s *handleDriverSession) SetLeadingMetadata(value Metadata) error {
	return s.handle.SetHeader(value)
}
func (s *handleDriverSession) SetTrailingMetadata(value Metadata) { s.handle.SetTrailer(value) }
func (s *handleDriverSession) Complete()                          { s.handle.CloseOutput() }
func (s *handleDriverSession) Done() <-chan struct{}              { return s.handle.Done() }
