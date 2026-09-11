# Binding correspondence provenance

The standalone client implements application-value and addressing behavior
without importing an OpenBindings SDK. The Go Avro codec, external Avro schema
composition, and routed operation envelope were originally qualified against
`openbindings.asyncapi@1` section 9.2; the routed-envelope ruling was recorded
on August 14, 2026. Those references describe the provenance of the behavior,
not a runtime dependency or a requirement for callers to adopt OpenBindings.
