//! Development cases, with expectations derived from the expression grammar.
use dynamic_asyncapi_client::{
    Code, Document, ExpressionSource, Json, Limits, PlanOptions, RuntimeExpression,
};
use serde_json::{Value, json};

fn value(source: &str) -> Json {
    Json::parse(source, Limits::default()).unwrap()
}
fn source(edition: &str, correlation: Value) -> Value {
    let message = json!({"contentType":"application/json","correlationId":correlation});
    let mut source = json!({"asyncapi":edition,"info":{"title":"expressions","version":"1"},
        "servers":{"s":{"host":"localhost:8080","protocol":"ws"}},
        "channels":{"c":{"address":"/events","messages":{"m":message}}},
        "operations":{"emit":{"action":"send","channel":{"$ref":"#/channels/c"}}}});
    if edition == "2.6.0" {
        source["servers"]["s"]["url"] = source["servers"]["s"]["host"].take();
        source["channels"] =
            json!({"/events":{"subscribe":{"operationId":"emit","message":message}}});
        source.as_object_mut().unwrap().remove("operations");
    }
    source
}

#[test]
fn expression_preserves_exact_types_and_owning_results() {
    let expression = RuntimeExpression::parse("$message.payload#/id").unwrap();
    let payload = value(r#"{"id":9007199254740993123456789}"#);
    let result = expression.evaluate(None, Some(&payload)).unwrap();
    drop(payload);
    drop(expression);
    assert_eq!(result.kind(), "number");
    assert_eq!(result.number_text(), Some("9007199254740993123456789"));
    for text in ["null", "true", "false", r#""123""#, "[]", "{}", "1.00e9999"] {
        let root = value(text);
        let result = RuntimeExpression::parse("$message.header")
            .unwrap()
            .evaluate(Some(&root), None)
            .unwrap();
        assert_eq!(result.to_json(), text);
        assert_eq!(result.kind(), root.kind());
    }
}

#[test]
fn expression_distinguishes_missing_null_and_message_roots() {
    let header = value(r#"{"id":"header"}"#);
    let payload = value(r#"{"id":null}"#);
    let expression = RuntimeExpression::parse("$message.payload#/id").unwrap();
    assert!(expression.evaluate(Some(&header), None).is_none());
    assert!(
        expression
            .evaluate(Some(&header), Some(&payload))
            .unwrap()
            .is_null()
    );
    assert!(
        RuntimeExpression::parse("$message.payload#/missing")
            .unwrap()
            .evaluate(None, Some(&payload))
            .is_none()
    );
    let expression = RuntimeExpression::parse("$message.header#/id").unwrap();
    assert_eq!(
        expression
            .evaluate(Some(&header), Some(&payload))
            .unwrap()
            .as_str(),
        Some("header")
    );
    for spelling in ["$message.payload", "$message.payload#"] {
        assert_eq!(
            RuntimeExpression::parse(spelling)
                .unwrap()
                .evaluate(None, Some(&payload))
                .unwrap()
                .to_json(),
            payload.to_json()
        );
    }
}

#[test]
fn expression_pointer_grammar_handles_escapes_arrays_unicode_and_literal_percent() {
    let payload = value(
        r##"{"a/b":{"~key":[{"雪":42}]},"":{"#":"hash"},"a%2Fb":"literal","a":{"b":"nested"},"~1":"tilde-one"}"##,
    );
    for (pointer, expected) in [
        ("/a~1b/~0key/0/雪", "42"),
        ("//#", r#""hash""#),
        ("/a%2Fb", r#""literal""#),
        ("/~01", r#""tilde-one""#),
    ] {
        let expression = RuntimeExpression::parse(&format!("$message.payload#{pointer}")).unwrap();
        assert_eq!(expression.pointer(), pointer);
        assert_eq!(
            expression.evaluate(None, Some(&payload)).unwrap().to_json(),
            expected
        );
    }
    for pointer in [
        "/a~1b/~0key/01",
        "/a~1b/~0key/-",
        "/a~1b/~0key/99999999999999999999999",
        "/a~1b/~0key/1",
    ] {
        assert!(
            RuntimeExpression::parse(&format!("$message.payload#{pointer}"))
                .unwrap()
                .evaluate(None, Some(&payload))
                .is_none()
        );
    }
}

#[test]
fn expression_syntax_and_limits_refuse_without_copying_input_into_errors() {
    for text in [
        "",
        "$message",
        "$message.headers#/id",
        "$message.payload/id",
        "$message.payload#id",
        "$message.payload#/a~",
        "$message.payload#/a~2",
        "prefix$message.payload",
        "$message.payload#secret-sentinel",
    ] {
        let error = RuntimeExpression::parse(text).unwrap_err();
        assert_eq!(error.code, Code::InvalidValue);
        assert!(!error.to_string().contains("secret-sentinel"));
    }
    assert!(RuntimeExpression::parse(&format!("$message.payload#{}", "/x".repeat(256))).is_ok());
    assert_eq!(
        RuntimeExpression::parse(&format!("$message.payload#{}", "/x".repeat(257)))
            .unwrap_err()
            .code,
        Code::Limit
    );
    let prefix = "$message.payload#/";
    assert!(
        RuntimeExpression::parse(&format!("{prefix}{}", "x".repeat(16384 - prefix.len()))).is_ok()
    );
    assert_eq!(
        RuntimeExpression::parse(&format!("{prefix}{}", "x".repeat(16385 - prefix.len())))
            .unwrap_err()
            .code,
        Code::Limit
    );
}

#[test]
fn correlation_inspection_is_lazy_and_preserves_all_supported_editions() {
    for edition in ["2.6.0", "3.0.0", "3.1.0"] {
        let mut v = source(edition, json!({"$ref":"#/components/correlationIds/id"}));
        v["components"] = json!({"correlationIds":{"id":{"location":"$message.payload#/id","description":"trace"}}});
        let compiled = Document::parse(&v.to_string())
            .unwrap()
            .operation_id("emit")
            .unwrap()
            .compile()
            .unwrap();
        let key = &compiled.describe().messages[0].key;
        let description = compiled.correlation(key).unwrap().unwrap();
        assert_eq!(
            description.definition.pointer,
            "/components/correlationIds/id"
        );
        assert_eq!(
            description.location.pointer,
            "/components/correlationIds/id/location"
        );
        assert_eq!(description.expression.source(), ExpressionSource::Payload);
        assert_eq!(description.description.as_deref(), Some("trace"));
        assert!(compiled.describe().messages[0].correlation_id.is_some());
        // Inspection must not accidentally permit unsupported execution.
        assert_eq!(
            compiled
                .prepare(&PlanOptions::application())
                .unwrap_err()
                .code,
            Code::UnsupportedFeature
        );
        assert_eq!(
            compiled.correlation("unknown").unwrap_err().code,
            Code::InvalidConfiguration
        );
    }
}

#[test]
fn correlation_trait_reference_uses_its_resource_origin_and_effective_fields() {
    let mut v = source("3.1.0", json!({"description":"local description"}));
    v["channels"]["c"]["messages"]["m"]["traits"] = json!([{"$ref":"traits/envelope.json"}]);
    let doc = Document::parse_at(&v.to_string(), "https://fixture.test/api.json")
        .unwrap()
        .with_resource(
            "https://fixture.test/traits/envelope.json",
            r#"{"correlationId":{"$ref":"id.json"}}"#,
        )
        .unwrap()
        .with_resource(
            "https://fixture.test/traits/id.json",
            r#"{"location":"$message.header#/id","description":"referenced"}"#,
        )
        .unwrap();
    let compiled = doc.operation_id("emit").unwrap().compile().unwrap();
    let correlation = compiled.correlation("m").unwrap().unwrap();
    assert_eq!(
        correlation.location.uri.as_deref(),
        Some("https://fixture.test/traits/id.json")
    );
    assert_eq!(correlation.description.as_deref(), Some("referenced")); // Reference siblings have no effect.
    v["channels"]["c"]["messages"]["m"]["traits"] =
        json!([{"correlationId":{"location":"$message.payload#/id","description":"trait"}}]);
    let compiled = Document::parse(&v.to_string())
        .unwrap()
        .operation_id("emit")
        .unwrap()
        .compile()
        .unwrap();
    let correlation = compiled.correlation("m").unwrap().unwrap();
    assert_eq!(
        correlation.description.as_deref(),
        Some("local description")
    );
    assert_eq!(
        correlation.location.pointer,
        "/channels/c/messages/m/traits/0/correlationId/location"
    );
    assert_eq!(correlation.expression.expression(), "$message.payload#/id");
}

#[test]
fn broken_correlation_is_local_to_requested_inspection_and_retains_diagnostics() {
    for (declaration, code) in [
        (json!({"$ref":"missing.json"}), Code::MissingResource),
        (json!({}), Code::InvalidOperation),
        (json!({"location":false}), Code::InvalidOperation),
        (
            json!({"location":"secret-sentinel"}),
            Code::InvalidOperation,
        ),
        (json!(false), Code::InvalidOperation),
    ] {
        let v = source("3.1.0", declaration);
        let compiled = Document::parse_at(&v.to_string(), "https://fixture.test/api.json")
            .unwrap()
            .operation_id("emit")
            .unwrap()
            .compile()
            .unwrap();
        let error = compiled.correlation("m").unwrap_err();
        assert_eq!(error.code, code);
        assert!(error.location.is_some());
        assert!(!error.detail.contains("secret-sentinel"));
    }
    let mut v = source("3.1.0", json!({"location":"$message.payload#/id"}));
    v["channels"]["c"]["messages"]["m"]
        .as_object_mut()
        .unwrap()
        .remove("correlationId");
    let compiled = Document::parse(&v.to_string())
        .unwrap()
        .operation_id("emit")
        .unwrap()
        .compile()
        .unwrap();
    assert!(compiled.correlation("m").unwrap().is_none());
    assert!(compiled.describe().messages[0].correlation_id.is_none());
}
