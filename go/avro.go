package asyncapiclient

import (
	"fmt"
	"mime"
	"strings"
)

// The named Avro correspondence (openbindings.asyncapi@1 §9.2): a message
// whose payload declares an on-list Avro schema format crosses the boundary
// as logical application values. A JSON-family effective content type
// carries the Avro-JSON encoding directly — the ordinary JSON lane is that
// wire, so nothing extra is needed. Any other declared media carries the
// Avro BINARY encoding, a codec capability: this build has not qualified an
// Avro binary codec, so such an operation direction refuses before
// dispatch, exactly as an unqualified protocol driver does. It MUST NOT
// fall back to the byte boundary — the synthesized schema is the logical
// one, and base64 strings do not satisfy it.

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

// avroMediaGuard refuses the unqualified-codec case for one governing
// message: Avro declared, effective media outside the JSON family (whose
// wire is the Avro binary encoding). Callers with an artifact-silent
// content type apply the guard to the configured lane instead.
func avroMediaGuard(m message, effectiveContentType string) error {
	if !avroDeclaredMessage(m) || effectiveContentType == "" {
		return nil
	}
	if isJSONContentType(effectiveContentType) {
		return nil
	}
	return fmt.Errorf("the governing message declares the Avro correspondence with media %q, whose wire is the Avro binary encoding; this build has no qualified Avro binary codec", effectiveContentType)
}
