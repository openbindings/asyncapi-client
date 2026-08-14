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
	// EncodeUnit renders one caller input value into its complete wire
	// unit: payload octets plus the routed envelope's application headers
	// as protocol header pairs (empty when the governing message declares
	// none). A driver whose protocol has native per-message header
	// carriage uses this seam and declares it via HeaderCarriage; Encode
	// remains for headerless lanes and refuses a headers-bearing value
	// rather than dropping it.
	EncodeUnit func(any) (DriverUnit, error)
}

type DriverOutput struct {
	DriverDirection
	Decode func([]byte) (any, error)
	// DecodeUnit decodes one received wire unit: the client pairs the
	// payload with the DECLARED application headers projected from the
	// received pairs (the routed envelope on the output direction).
	DecodeUnit func(DriverUnit) (any, error)
}

// DriverUnit is one message crossing the driver seam: the payload octets
// and the protocol's per-message header pairs.
type DriverUnit struct {
	Payload []byte
	Headers []DriverHeader
}

// DriverHeader is one protocol header pair. Values are raw octets — the
// protocol's spelling (Kafka record headers carry arbitrary bytes); the
// client renders and projects application scalars at the envelope
// boundary.
type DriverHeader struct {
	Key   string
	Value []byte
}

// HeaderCarriage is optionally implemented by protocol drivers whose
// protocol has native per-message header carriage and whose implementation
// consumes the unit seam. Without it, a headers-declaring direction
// refuses before dispatch (the §9.2 per-cell capability bound).
type HeaderCarriage interface {
	CarriesMessageHeaders() bool
}

func driverCarriesHeaders(driver ProtocolDriver) bool {
	carriage, ok := driver.(HeaderCarriage)
	return ok && carriage.CarriesMessageHeaders()
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
