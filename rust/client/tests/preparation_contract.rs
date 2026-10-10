//! Development regressions. These do not substitute for independent profile review.
use dynamic_asyncapi_client::{
    Action, Code, Document, PlanOptions, Requirement, Role, TransportPlan,
};
use serde_json::{Value, json};

fn source() -> Value {
    json!({
        "asyncapi":"3.1.0", "info":{"title":"fixture","version":"1"},
        "servers":{"local":{"host":"127.0.0.1:1883","protocol":"mqtt","protocolVersion":"3.1.1","bindings":{"mqtt":{"clientId":"app","keepAlive":0,"bindingVersion":"0.2.0"}}}},
        "channels":{"events":{"address":"events/{tenant}","parameters":{"tenant":{"default":"acme","enum":["acme","a/b"]}},"messages":{"event":{"contentType":"application/octet-stream"}}}},
        "operations":{"emit":{"action":"send","channel":{"$ref":"#/channels/events"},"bindings":{"mqtt":{"qos":1,"retain":false}}}}
    })
}
fn doc(value: &Value) -> Document {
    Document::parse_at(&value.to_string(), "https://fixture.test/api.json").unwrap()
}
fn plan(
    value: &Value,
) -> Result<dynamic_asyncapi_client::Plan, dynamic_asyncapi_client::Diagnostic> {
    doc(value)
        .operation_id("emit")?
        .compile()?
        .prepare(&PlanOptions::application())
}

#[test]
fn binary_plan_is_reusable_and_outlives_document_and_compiler_handles() {
    let document = doc(&source());
    let compiled = document.operation_id("emit").unwrap().compile().unwrap();
    let plan = compiled.prepare(&PlanOptions::application()).unwrap();
    drop(document);
    drop(compiled);
    let bytes = [0, 255, 16];
    assert!(std::ptr::eq(
        plan.prepare_bytes(&bytes).unwrap().as_ptr(),
        bytes.as_ptr()
    ));
    assert_eq!(plan.describe().application_action, Action::Send);
    assert_eq!(plan.describe().wire_action, Action::Send);
    match &plan.describe().transport {
        TransportPlan::Mqtt311 {
            endpoint,
            topic,
            client_id,
            keep_alive_seconds,
            qos,
            retain,
            ..
        } => {
            assert_eq!(endpoint, "mqtt://127.0.0.1:1883");
            assert_eq!(topic, "events/acme");
            assert_eq!(client_id, "app");
            assert_eq!(*keep_alive_seconds, 0);
            assert_eq!(*qos, 1);
            assert!(!retain);
        }
        _ => panic!("wrong transport"),
    }
}

#[test]
fn equivalent_native_editions_produce_the_same_wire_direction_without_conversion() {
    let legacy = json!({"asyncapi":"2.6.0","info":{"title":"fixture","version":"1"},
        "servers":{"local":{"url":"127.0.0.1:1883","protocol":"mqtt","protocolVersion":"3.1.1","bindings":{"mqtt":{"clientId":"app"}}}},
        "channels":{"events":{"subscribe":{"operationId":"emit","message":{"contentType":"application/octet-stream"}}}}});
    let legacy = plan(&legacy).unwrap();
    assert_eq!(
        legacy.describe().identity.pointer,
        "/channels/events/subscribe"
    );
    assert_eq!(legacy.describe().wire_action, Action::Send);
    for version in ["3.0.0", "3.1.7"] {
        let mut value = source();
        value["asyncapi"] = json!(version);
        assert_eq!(
            plan(&value).unwrap().describe().wire_action,
            legacy.describe().wire_action
        );
    }
    let yaml = "asyncapi: 2.6.0\ninfo: {title: test, version: '1'}\nservers:\n  local:\n    url: '127.0.0.1:1883'\n    protocol: mqtt\n    protocolVersion: 3.1.1\n    bindings: {mqtt: {clientId: app}}\nchannels:\n  events:\n    subscribe:\n      operationId: emit\n      message: {contentType: application/octet-stream}\n";
    assert_eq!(
        Document::parse(yaml)
            .unwrap()
            .operation_id("emit")
            .unwrap()
            .compile()
            .unwrap()
            .prepare(&PlanOptions::application())
            .unwrap()
            .describe()
            .wire_action,
        Action::Send
    );
}

#[test]
fn omitted_and_empty_messages_are_distinct_and_membership_uses_channel_coordinates() {
    let mut value = source();
    assert_eq!(
        doc(&value)
            .operation_id("emit")
            .unwrap()
            .compile()
            .unwrap()
            .describe()
            .messages
            .len(),
        1
    );
    value["operations"]["emit"]["messages"] = json!([]);
    assert!(
        doc(&value)
            .operation_id("emit")
            .unwrap()
            .compile()
            .unwrap()
            .describe()
            .messages
            .is_empty()
    );
    value["components"] = json!({"messages":{"shared":{"contentType":"application/octet-stream"}}});
    value["channels"]["events"]["messages"]["event"] =
        json!({"$ref":"#/components/messages/shared"});
    value["operations"]["emit"]["messages"] = json!([{"$ref":"#/components/messages/shared"}]);
    assert_eq!(
        doc(&value)
            .operation_id("emit")
            .unwrap()
            .compile()
            .unwrap_err()
            .code,
        Code::InvalidOperation
    );
    value["operations"]["emit"]["messages"] = json!([{"$ref":"#/channels/events/messages/event"}]);
    let compiled = doc(&value).operation_id("emit").unwrap().compile().unwrap();
    assert_eq!(
        compiled.describe().messages[0].selection.pointer,
        "/channels/events/messages/event"
    );
    assert_eq!(
        compiled.describe().messages[0].definition.pointer,
        "/components/messages/shared"
    );
    assert!(compiled.prepare(&PlanOptions::application()).is_ok());
}

#[test]
fn root_reference_to_component_operation_uses_its_defining_location_rule() {
    let mut value = source();
    let mut op = value["operations"]["emit"].take();
    op["channel"]["$ref"] = json!("#/components/channels/component");
    value["components"] = json!({"operations":{"shared":op}, "channels":{"component":value["channels"]["events"].clone()}});
    value["operations"]["emit"] = json!({"$ref":"#/components/operations/shared"});
    let p = plan(&value).unwrap();
    assert_eq!(p.describe().identity.pointer, "/operations/emit");
    assert_eq!(
        p.operation().describe().operation.channel.pointer,
        "/components/channels/component"
    );
}

#[test]
fn server_selection_is_explicit_when_ambiguous_and_respects_channel_membership() {
    let mut value = source();
    value["servers"]["other"] = value["servers"]["local"].clone();
    assert!(
        matches!(plan(&value).unwrap_err().requirement, Some(Requirement::Server { choices }) if choices == ["local", "other"])
    );
    value["channels"]["events"]["servers"] = json!([{"$ref":"#/servers/other"}]);
    let compiled = doc(&value).operation_id("emit").unwrap().compile().unwrap();
    assert_eq!(
        compiled
            .prepare(&PlanOptions::application())
            .unwrap()
            .describe()
            .server,
        "other"
    );
    let mut options = PlanOptions::application();
    options.server = Some("local".into());
    assert_eq!(
        compiled.prepare(&options).unwrap_err().code,
        Code::InvalidConfiguration
    );
    value["channels"]["events"]["servers"] = json!([]);
    assert_eq!(
        doc(&value)
            .operation_id("emit")
            .unwrap()
            .compile()
            .unwrap()
            .describe()
            .servers
            .len(),
        2
    );
}

#[test]
fn message_traits_preserve_exact_schema_and_external_binding_reference_origins() {
    let mut value = source();
    value["operations"]["emit"]
        .as_object_mut()
        .unwrap()
        .remove("bindings");
    value["operations"]["emit"]["traits"] = json!([{"$ref":"traits/operation.json"}]);
    value["channels"]["events"]["messages"]["event"] =
        json!({"traits":[{"$ref":"traits/message.json"}]});
    let old = doc(&value);
    let document = old.with_resource("https://fixture.test/traits/operation.json", r#"{"bindings":{"$ref":"operation-binding.json"}}"#).unwrap()
        .with_resource("https://fixture.test/traits/operation-binding.json", r#"{"mqtt":{"qos":2}}"#).unwrap()
        .with_resource("https://fixture.test/traits/message.json", r#"{"contentType":"application/octet-stream","bindings":{"$ref":"message-binding.json"}}"#).unwrap();
    let compiled = document.operation_id("emit").unwrap().compile().unwrap();
    let error = compiled.prepare(&PlanOptions::application()).unwrap_err();
    assert_eq!(
        error.requirement,
        Some(Requirement::Resource {
            uri: "https://fixture.test/traits/message-binding.json".into()
        })
    );
    assert_eq!(
        error.location.unwrap().uri.as_deref(),
        Some("https://fixture.test/traits/message.json")
    );
    let complete = document
        .with_resource(
            "https://fixture.test/traits/message-binding.json",
            r#"{"mqtt":{"bindingVersion":"0.2.0"}}"#,
        )
        .unwrap();
    let p = complete
        .operation_id("emit")
        .unwrap()
        .compile()
        .unwrap()
        .prepare(&PlanOptions::application())
        .unwrap();
    assert!(matches!(
        p.describe().transport,
        TransportPlan::Mqtt311 { qos: 2, .. }
    ));
    assert_eq!(
        compiled
            .prepare(&PlanOptions::application())
            .unwrap_err()
            .code,
        Code::MissingResource
    );
    assert_eq!(
        old.operation_id("emit")
            .unwrap()
            .compile()
            .unwrap_err()
            .code,
        Code::MissingResource
    );
}

#[test]
fn wire_role_requires_a_distinct_peer_identity_and_preserves_application_direction() {
    let mut value = source();
    value["operations"]["emit"]["action"] = json!("receive");
    let compiled = doc(&value).operation_id("emit").unwrap().compile().unwrap();
    let mut options = PlanOptions::application();
    options.role = Role::Peer;
    assert_eq!(
        compiled.prepare(&options).unwrap_err().requirement,
        Some(Requirement::ClientIdentity)
    );
    options.client_id = Some("app".into());
    assert_eq!(
        compiled.prepare(&options).unwrap_err().code,
        Code::InvalidConfiguration
    );
    options.client_id = Some("peer".into());
    let p = compiled.prepare(&options).unwrap();
    assert_eq!(p.describe().wire_action, Action::Send);
    assert_eq!(p.describe().application_action, Action::Receive);
}

#[test]
fn native_topic_substitution_is_not_url_encoding_and_rejects_invalid_publish_routes() {
    let compiled = doc(&source())
        .operation_id("emit")
        .unwrap()
        .compile()
        .unwrap();
    let mut options = PlanOptions::application();
    options.parameters.insert("tenant".into(), "a/b".into());
    assert!(
        matches!(&compiled.prepare(&options).unwrap().describe().transport, TransportPlan::Mqtt311 { topic, .. } if topic == "events/a/b")
    );
    options.parameters.insert("typo".into(), "x".into());
    assert_eq!(
        compiled.prepare(&options).unwrap_err().code,
        Code::InvalidConfiguration
    );
    let mut value = source();
    value["channels"]["events"]["address"] = json!("events/+");
    value["channels"]["events"]
        .as_object_mut()
        .unwrap()
        .remove("parameters");
    assert_eq!(plan(&value).unwrap_err().code, Code::InvalidConfiguration);
}

#[test]
fn document_format_never_implies_wire_codec_and_schema_or_reply_cannot_disappear() {
    let mut value = source();
    value["channels"]["events"]["messages"]["event"] = json!({});
    assert_eq!(
        plan(&value).unwrap_err().requirement,
        Some(Requirement::Codec { content_type: None })
    );
    value["defaultContentType"] = json!("application/octet-stream");
    assert!(plan(&value).is_ok());
    value["channels"]["events"]["messages"]["event"]["payload"] = json!({"const":null});
    assert_eq!(
        plan(&value).unwrap_err().requirement,
        Some(Requirement::Evaluator)
    );
    value["channels"]["events"]["messages"]["event"] = json!({});
    value["operations"]["emit"]["reply"] = json!({});
    assert_eq!(
        plan(&value).unwrap_err().requirement,
        Some(Requirement::Reply)
    );
}

#[test]
fn mqtt_binding_revision_version_scope_and_integer_ranges_refuse_before_io() {
    for (pointer, replacement, code) in [
        (
            "/servers/local/bindings/mqtt/bindingVersion",
            json!("0.99.0"),
            Code::UnsupportedBinding,
        ),
        (
            "/servers/local/bindings/mqtt/keepAlive",
            json!(65536),
            Code::InvalidOperation,
        ),
        (
            "/operations/emit/bindings/mqtt/qos",
            json!(3),
            Code::InvalidOperation,
        ),
        (
            "/servers/local/protocolVersion",
            json!("5"),
            Code::UnsupportedProtocol,
        ),
    ] {
        let mut value = source();
        *value.pointer_mut(pointer).unwrap() = replacement;
        assert_eq!(plan(&value).unwrap_err().code, code, "{pointer}");
    }
    let mut value = source();
    value["servers"]["local"]["bindings"]["mqtt"]["maximumPacketSize"] = json!(2048);
    assert_eq!(plan(&value).unwrap_err().code, Code::UnsupportedProtocol);
    let mut value = source();
    value["channels"]["events"]["bindings"] = json!({"mqtt":{"qos":1}});
    assert_eq!(plan(&value).unwrap_err().code, Code::UnsupportedBinding);
}

fn websocket_source() -> Value {
    let mut value = source();
    value["servers"]["local"] = json!({"host":"example.test","pathname":"/base","protocol":"wss"});
    value["channels"]["events"]["address"] = json!("/events");
    value["channels"]["events"]
        .as_object_mut()
        .unwrap()
        .remove("parameters");
    value["operations"]["emit"]
        .as_object_mut()
        .unwrap()
        .remove("bindings");
    value
}
#[test]
fn websocket_path_and_handshake_are_explicit_and_peer_cannot_open_an_unrelated_connection() {
    let mut value = websocket_source();
    assert!(
        matches!(&plan(&value).unwrap().describe().transport, TransportPlan::WebSocket6455 { endpoint, method, frame: dynamic_asyncapi_client::WebSocketFrame::Binary } if endpoint == "wss://example.test/base/events" && method == "GET")
    );
    let compiled = doc(&value).operation_id("emit").unwrap().compile().unwrap();
    let mut options = PlanOptions::application();
    options.role = Role::Peer;
    assert_eq!(
        compiled.prepare(&options).unwrap_err().requirement,
        Some(Requirement::PeerRoute)
    );
    value["channels"]["events"]["bindings"] = json!({"ws":{"method":"POST"}});
    assert_eq!(plan(&value).unwrap_err().code, Code::UnsupportedProtocol);
    value["channels"]["events"]
        .as_object_mut()
        .unwrap()
        .remove("bindings");
    value["channels"]["events"]["address"] = json!("//attacker.test/events");
    assert_eq!(plan(&value).unwrap_err().code, Code::InvalidConfiguration);
}

#[test]
fn templates_require_known_values_and_cannot_inject_uri_authority_or_query() {
    let mut value = websocket_source();
    value["servers"]["local"]["host"] = json!("{host}:443");
    value["servers"]["local"]["variables"] = json!({"host":{}});
    let compiled = doc(&value).operation_id("emit").unwrap().compile().unwrap();
    assert_eq!(
        compiled
            .prepare(&PlanOptions::application())
            .unwrap_err()
            .requirement,
        Some(Requirement::Variable {
            name: "host".into()
        })
    );
    for host in [
        "good.test@evil.test",
        "evil.test/path",
        "good.test?evil",
        "good.test#evil",
    ] {
        let mut options = PlanOptions::application();
        options.variables.insert("host".into(), host.into());
        assert_eq!(
            compiled.prepare(&options).unwrap_err().code,
            Code::InvalidConfiguration
        );
    }
}

#[test]
fn binding_integer_values_are_exact_even_when_written_with_decimal_or_exponent() {
    for token in ["1", "1.0", "1e0", "10e-1", "0.01e2"] {
        let text = source()
            .to_string()
            .replace("\"qos\":1", &format!("\"qos\":{token}"));
        let p = Document::parse(&text)
            .unwrap()
            .operation_id("emit")
            .unwrap()
            .compile()
            .unwrap()
            .prepare(&PlanOptions::application())
            .unwrap();
        assert!(
            matches!(
                p.describe().transport,
                TransportPlan::Mqtt311 { qos: 1, .. }
            ),
            "{token}"
        );
    }
    for token in ["1.000000000000000000000001", "1e100000", "-1", "1e-100000"] {
        let text = source()
            .to_string()
            .replace("\"qos\":1", &format!("\"qos\":{token}"));
        assert_eq!(
            Document::parse(&text)
                .unwrap()
                .operation_id("emit")
                .unwrap()
                .compile()
                .unwrap()
                .prepare(&PlanOptions::application())
                .unwrap_err()
                .code,
            Code::InvalidOperation
        );
    }
}

#[test]
fn preparation_expansion_has_a_bound_even_when_values_are_repeated() {
    let mut value = source();
    value["channels"]["events"]["address"] = json!("{tenant}{tenant}{tenant}{tenant}");
    value["channels"]["events"]["parameters"]["tenant"]
        .as_object_mut()
        .unwrap()
        .remove("enum");
    let text = value.to_string();
    let document = Document::parse_with(
        &text,
        None,
        dynamic_asyncapi_client::Limits {
            source_bytes: text.len() + 64,
            ..Default::default()
        },
    )
    .unwrap();
    let compiled = document.operation_id("emit").unwrap().compile().unwrap();
    let mut options = PlanOptions::application();
    options
        .parameters
        .insert("tenant".into(), "a".repeat(text.len() / 2));
    assert_eq!(compiled.prepare(&options).unwrap_err().code, Code::Limit);
}

#[test]
fn declared_security_and_multiple_message_classification_are_not_silently_ignored() {
    let mut value = source();
    value["servers"]["local"]["security"] = json!([{"type":"userPassword"}]);
    assert_eq!(
        plan(&value).unwrap_err().requirement,
        Some(Requirement::Authentication)
    );
    value["servers"]["local"]["security"] = json!([]);
    value["channels"]["events"]["messages"]["other"] =
        json!({"contentType":"application/octet-stream"});
    value["operations"]["emit"]["messages"] = json!([{"$ref":"#/channels/events/messages/event"}]);
    assert_eq!(
        plan(&value).unwrap_err().requirement,
        Some(Requirement::Evaluator)
    );
}
