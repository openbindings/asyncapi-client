package asyncapiclient

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"mime"
	"strings"

	"github.com/linkedin/goavro/v2"
)

// The named Avro correspondence (openbindings.asyncapi@1 §9.2): a message
// whose payload declares an on-list Avro schema format crosses the boundary
// as logical application values. A JSON-family effective content type
// carries the Avro-JSON encoding directly — the ordinary JSON lane is that
// wire, so nothing extra is needed. Any other declared media carries the
// Avro BINARY encoding of the datum under the artifact's schema, through
// the qualified codec below (goavro). Wire framing around the binary
// encoding is the named `framing` configuration point: "bare" (the default
// — the binary encoding alone) or "confluent" (the Confluent wire prefix:
// magic byte 0x00 plus the big-endian 4-byte schema id supplied as
// configuration.schemaId).

// avroDeclaredMessage reports whether the message payload is a Multi Format
// Schema Object declaring an on-list Avro schema format.
func avroDeclaredMessage(m message) bool {
	if m.Payload == nil {
		return false
	}
	format, _ := m.Payload["schemaFormat"].(string)
	return isAvroSchemaFormat(format)
}

// isAvroSchemaFormat mirrors the synthesis classifier's on-list rule:
// application/vnd.apache.avro with an optional +json/+yaml suffix, version
// parameter absent or 1.x. Anything else is off-list here.
func isAvroSchemaFormat(format string) bool {
	if strings.TrimSpace(format) == "" {
		return false
	}
	mediaType, params, err := mime.ParseMediaType(format)
	if err != nil {
		return false
	}
	switch strings.ToLower(mediaType) {
	case "application/vnd.apache.avro", "application/vnd.apache.avro+json", "application/vnd.apache.avro+yaml":
	default:
		return false
	}
	version := params["version"]
	return version == "" || strings.HasPrefix(version, "1.")
}

const (
	avroFramingBare      = "bare"
	avroFramingConfluent = "confluent"
)

// avroBinaryCodec is the qualified Avro binary codec for one governing
// schema: logical value ⇄ Avro binary octets, via the Avro JSON Encoding as
// the value's textual form (goavro's textual codec IS that encoding).
type avroBinaryCodec struct {
	codec    *goavro.Codec
	framing  string
	schemaID uint32
}

// resolveAvroBinaryCodec builds the codec governing a non-JSON wire from
// the message set's Avro declarations. nil (no error) when no governing
// message declares an on-list Avro format. All Avro-declared members must
// carry one identical schema: bare Avro binary is not self-describing, so
// distinct candidate schemas make the decode declaration ambiguous —
// refused loudly, never guessed at.
func resolveAvroBinaryCodec(msgs []message, bindCtx map[string]any) (*avroBinaryCodec, error) {
	var schemaJSON []byte
	for _, m := range msgs {
		if !avroDeclaredMessage(m) {
			continue
		}
		encoded, err := json.Marshal(m.Payload["schema"])
		if err != nil {
			return nil, fmt.Errorf("the governing Avro schema does not serialize: %v", err)
		}
		if schemaJSON == nil {
			schemaJSON = encoded
			continue
		}
		if !bytes.Equal(schemaJSON, encoded) {
			return nil, fmt.Errorf("the governing messages declare distinct Avro schemas; a bare binary wire cannot select among them")
		}
	}
	if schemaJSON == nil {
		return nil, nil
	}
	codec, err := goavro.NewCodec(string(schemaJSON))
	if err != nil {
		return nil, fmt.Errorf("the declared Avro schema is not a valid Avro schema: %v", err)
	}
	configuration := contextConfiguration(bindCtx)
	framing, _ := configuration["framing"].(string)
	if framing == "" {
		framing = avroFramingBare
	}
	resolved := &avroBinaryCodec{codec: codec, framing: framing}
	switch framing {
	case avroFramingBare:
	case avroFramingConfluent:
		id, ok := asSchemaID(configuration["schemaId"])
		if !ok {
			return nil, fmt.Errorf("configuration.framing %q requires configuration.schemaId (an unsigned 32-bit integer)", framing)
		}
		resolved.schemaID = id
	default:
		return nil, fmt.Errorf("configuration.framing %q is not a named framing (bare, confluent)", framing)
	}
	return resolved, nil
}

func asSchemaID(value any) (uint32, bool) {
	switch v := value.(type) {
	case float64:
		if v < 0 || v != float64(uint32(v)) {
			return 0, false
		}
		return uint32(v), true
	case int:
		if v < 0 || int64(v) > int64(^uint32(0)) {
			return 0, false
		}
		return uint32(v), true
	default:
		return 0, false
	}
}

// encode renders one logical value as the wire's Avro binary octets:
// logical JSON → Avro JSON Encoding text → native datum → binary encoding,
// framed per the configuration point.
func (c *avroBinaryCodec) encode(v any) ([]byte, error) {
	text, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("the input value does not serialize as JSON: %v", err)
	}
	native, _, err := c.codec.NativeFromTextual(text)
	if err != nil {
		return nil, fmt.Errorf("the input value is not the Avro-JSON encoding of a datum under the declared schema: %v", err)
	}
	var framed []byte
	if c.framing == avroFramingConfluent {
		framed = make([]byte, 5)
		binary.BigEndian.PutUint32(framed[1:], c.schemaID)
	}
	encoded, err := c.codec.BinaryFromNative(framed, native)
	if err != nil {
		return nil, fmt.Errorf("the datum does not encode under the declared Avro schema: %v", err)
	}
	return encoded, nil
}

// decode reads the wire's Avro binary octets back to the logical value:
// unframe, binary → native datum → Avro JSON Encoding text → logical JSON.
func (c *avroBinaryCodec) decode(wire []byte) (any, error) {
	if c.framing == avroFramingConfluent {
		if len(wire) < 5 || wire[0] != 0 {
			return nil, fmt.Errorf("the payload does not carry the Confluent wire prefix the framing configuration declares")
		}
		if id := binary.BigEndian.Uint32(wire[1:5]); id != c.schemaID {
			return nil, fmt.Errorf("the payload's Confluent schema id %d is not the configured schemaId %d", id, c.schemaID)
		}
		wire = wire[5:]
	}
	native, rest, err := c.codec.NativeFromBinary(wire)
	if err != nil {
		return nil, fmt.Errorf("the payload is not the Avro binary encoding of a datum under the declared schema: %v", err)
	}
	if len(rest) != 0 {
		return nil, fmt.Errorf("the payload carries %d bytes beyond the Avro datum", len(rest))
	}
	text, err := c.codec.TextualFromNative(nil, native)
	if err != nil {
		return nil, fmt.Errorf("the datum does not render in the Avro JSON encoding: %v", err)
	}
	var value any
	if err := json.Unmarshal(text, &value); err != nil {
		return nil, fmt.Errorf("the Avro JSON encoding did not parse: %v", err)
	}
	return value, nil
}
