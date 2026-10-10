//! Development security contract tests; no independent qualification credit.
use dynamic_asyncapi_client::{
    Code, CompiledOperation, Document, PlanOptions, Requirement, SecurityScope,
};
use serde_json::{Value, json};

fn source(version: &str) -> Value {
    let mut v = json!({"asyncapi":version,"info":{"title":"auth","version":"1"},
        "servers":{"s":{"host":"localhost:8883","protocol":"mqtts","protocolVersion":"3.1.1","bindings":{"mqtt":{"clientId":"auth-fixture"}}}},
        "channels":{"c":{"address":"events","messages":{"m":{"contentType":"application/octet-stream"}}}},
        "operations":{"emit":{"action":"send","channel":{"$ref":"#/channels/c"}}},
        "components":{"securitySchemes":{"user":{"type":"userPassword"},"cert":{"type":"X509"}}}});
    if version == "2.6.0" {
        v["servers"]["s"]["url"] = v["servers"]["s"]["host"].take();
        v["channels"] = json!({"events":{"subscribe":{"operationId":"emit","message":{"contentType":"application/octet-stream"}}}});
        v.as_object_mut().unwrap().remove("operations");
    }
    v
}
fn compiled(v: &Value) -> CompiledOperation {
    Document::parse_at(&v.to_string(), "https://fixture.test/api.json")
        .unwrap()
        .operation_id("emit")
        .unwrap()
        .compile()
        .unwrap()
}
fn operation(v: &mut Value) -> &mut Value {
    if v["asyncapi"] == "2.6.0" {
        &mut v["channels"]["events"]["subscribe"]
    } else {
        &mut v["operations"]["emit"]
    }
}
fn user(v: &Value) -> Value {
    if v["asyncapi"] == "2.6.0" {
        json!({"user":[]})
    } else {
        json!({"$ref":"#/components/securitySchemes/user"})
    }
}
fn cert(v: &Value) -> Value {
    if v["asyncapi"] == "2.6.0" {
        json!({"cert":[]})
    } else {
        json!({"type":"X509"})
    }
}

#[test]
fn server_and_operation_requirements_both_apply_in_every_native_edition() {
    for edition in ["2.6.0", "3.0.0", "3.1.0"] {
        let mut v = source(edition);
        v["servers"]["s"]["security"] = json!([user(&v)]);
        let certificate = cert(&v);
        operation(&mut v)["security"] = json!([certificate]);
        let c = compiled(&v);
        let auth = c.authentication("s").unwrap();
        assert_eq!(auth.server[0].schemes[0].scheme_type, "userPassword");
        assert_eq!(auth.operation[0].schemes[0].scheme_type, "X509");
        assert_eq!(
            auth.server[0].schemes[0].definition.pointer,
            "/components/securitySchemes/user"
        );
        let plan = c.prepare(&PlanOptions::application()).unwrap();
        assert_eq!(plan.describe().authentication.schemes().count(), 2);
        operation(&mut v)["security"] = json!([]);
        assert_eq!(
            compiled(&v)
                .prepare(&PlanOptions::application())
                .unwrap()
                .describe()
                .authentication
                .schemes()
                .count(),
            1
        );
    }
}
#[test]
fn choices_are_independent_explicit_and_never_pick_a_weaker_alternative() {
    let mut v = source("3.1.0");
    let choices = json!([user(&v), cert(&v)]);
    v["servers"]["s"]["security"] = choices.clone();
    operation(&mut v)["security"] = choices;
    let c = compiled(&v);
    let mut options = PlanOptions::application();
    assert_eq!(
        c.prepare(&options).unwrap_err().requirement,
        Some(Requirement::AuthenticationChoice {
            scope: SecurityScope::Server,
            choices: vec![0, 1]
        })
    );
    options.security.server = Some(0);
    assert_eq!(
        c.prepare(&options).unwrap_err().requirement,
        Some(Requirement::AuthenticationChoice {
            scope: SecurityScope::Operation,
            choices: vec![0, 1]
        })
    );
    options.security.operation = Some(1);
    assert_eq!(
        c.prepare(&options)
            .unwrap()
            .describe()
            .authentication
            .operation
            .as_ref()
            .unwrap()
            .index,
        1
    );
    options.security.operation = Some(2);
    assert_eq!(
        c.prepare(&options).unwrap_err().code,
        Code::InvalidConfiguration
    );
}
#[test]
fn legacy_conjunction_and_empty_alternative_are_preserved() {
    let mut v = source("2.6.0");
    v["servers"]["s"]["security"] = json!([{"user":[],"cert":[]},{}]);
    let c = compiled(&v);
    let inspected = c.authentication("s").unwrap();
    assert_eq!(inspected.server[0].schemes.len(), 2);
    assert!(inspected.server[1].schemes.is_empty());
    let mut o = PlanOptions::application();
    assert_eq!(c.prepare(&o).unwrap_err().code, Code::MissingConfiguration);
    o.security.server = Some(1);
    assert_eq!(
        c.prepare(&o)
            .unwrap()
            .describe()
            .authentication
            .schemes()
            .count(),
        0
    );
    o.security.server = Some(0);
    assert_eq!(
        c.prepare(&o)
            .unwrap()
            .describe()
            .authentication
            .schemes()
            .count(),
        2
    );
}
#[test]
fn unselected_missing_reference_does_not_prevent_explicit_preparation() {
    let mut v = source("3.1.0");
    v["servers"]["s"]["security"] = json!([user(&v),{"$ref":"schemes.json#/certificate"}]);
    let c = compiled(&v);
    assert_eq!(
        c.authentication("s").unwrap_err().code,
        Code::MissingResource
    );
    let mut o = PlanOptions::application();
    o.security.server = Some(0);
    assert!(c.prepare(&o).is_ok());
    o.security.server = Some(1);
    assert_eq!(
        c.prepare(&o).unwrap_err().requirement,
        Some(Requirement::Resource {
            uri: "https://fixture.test/schemes.json".into()
        })
    );
    let original = Document::parse_at(&v.to_string(), "https://fixture.test/api.json").unwrap();
    let completed = original
        .with_resource(
            "https://fixture.test/schemes.json",
            r#"{"certificate":{"type":"X509"}}"#,
        )
        .unwrap();
    let auth = completed
        .operation_id("emit")
        .unwrap()
        .compile()
        .unwrap()
        .prepare(&o)
        .unwrap();
    assert_eq!(
        auth.describe()
            .authentication
            .server
            .as_ref()
            .unwrap()
            .schemes[0]
            .definition
            .uri
            .as_deref(),
        Some("https://fixture.test/schemes.json")
    );
    assert_eq!(c.prepare(&o).unwrap_err().code, Code::MissingResource);
}
#[test]
fn yaml_trait_selection_preserves_use_and_definition_provenance() {
    let yaml = "asyncapi: 3.1.0\ninfo: {title: auth, version: '1'}\nservers:\n  s: {host: 'localhost:8883', protocol: mqtts, protocolVersion: 3.1.1, bindings: {mqtt: {clientId: fixture}}}\nchannels:\n  c: {address: events, messages: {m: {contentType: application/octet-stream}}}\noperations:\n  emit:\n    action: send\n    channel: {$ref: '#/channels/c'}\n    traits: [{$ref: '#/components/operationTraits/auth'}]\ncomponents:\n  operationTraits:\n    auth:\n      security: [{$ref: '#/components/securitySchemes/user'}]\n  securitySchemes:\n    user: {type: userPassword}\n";
    let c = Document::parse(yaml)
        .unwrap()
        .operation_id("emit")
        .unwrap()
        .compile()
        .unwrap();
    let auth = c.authentication("s").unwrap();
    let requirement = &auth.operation[0].schemes[0];
    assert_eq!(
        requirement.selection.pointer,
        "/components/operationTraits/auth/security/0"
    );
    assert_eq!(
        requirement.definition.pointer,
        "/components/securitySchemes/user"
    );
    assert_eq!(
        c.prepare(&PlanOptions::application())
            .unwrap()
            .describe()
            .authentication
            .schemes()
            .count(),
        1
    );
}
#[test]
fn scopes_use_the_native_edition_and_unsupported_mechanisms_remain_inspectable() {
    for version in ["2.6.0", "3.0.0", "3.1.0"] {
        let mut v = source(version);
        v["components"]["securitySchemes"]["token"] =
            json!({"type":"oauth2","flows":{},"scopes":["inline"]});
        v["servers"]["s"]["security"] = if version == "2.6.0" {
            json!([{"token":["named"]}])
        } else {
            json!([{"$ref":"#/components/securitySchemes/token"}])
        };
        let c = compiled(&v);
        assert_eq!(
            c.authentication("s").unwrap().server[0].schemes[0].scopes,
            vec![if version == "2.6.0" {
                "named"
            } else {
                "inline"
            }]
        );
        assert_eq!(
            c.prepare(&PlanOptions::application())
                .unwrap_err()
                .requirement,
            Some(Requirement::Authentication)
        );
    }
}
#[test]
fn malformed_security_is_not_anonymous() {
    for version in ["2.6.0", "3.1.0"] {
        let mut v = source(version);
        for bad in [
            json!(null),
            json!({}),
            json!([null]),
            json!([{"missing":[]}]),
            json!([{"type":false}]),
        ] {
            v["servers"]["s"]["security"] = bad;
            assert_eq!(
                compiled(&v).authentication("s").unwrap_err().code,
                Code::InvalidOperation
            );
        }
        v["servers"]["s"]["security"] = if version == "2.6.0" {
            json!([{"user":["scope"]}])
        } else {
            json!([{"type":"userPassword","scopes":["scope"]}])
        };
        assert_eq!(
            compiled(&v).authentication("s").unwrap_err().code,
            Code::InvalidOperation
        );
    }
}
#[test]
fn x509_requires_secure_transport_and_user_password_requires_mqtt() {
    for protocol in ["mqtt", "ws", "wss"] {
        let mut v = source("3.1.0");
        v["servers"]["s"]["protocol"] = json!(protocol);
        v["servers"]["s"]["security"] =
            json!([{"type":if protocol=="mqtt" {"X509"} else {"userPassword"}}]);
        if protocol.starts_with("ws") {
            v["servers"]["s"]["protocolVersion"] = json!("13");
        }
        assert_eq!(
            compiled(&v)
                .prepare(&PlanOptions::application())
                .unwrap_err()
                .requirement,
            Some(Requirement::Authentication)
        );
    }
}
#[test]
fn security_bounds_include_resolved_scope_amplification() {
    let mut v = source("3.1.0");
    v["servers"]["s"]["security"] = json!(vec![json!({"type":"X509"}); 65]);
    assert_eq!(
        compiled(&v).authentication("s").unwrap_err().code,
        Code::Limit
    );
    v["servers"]["s"]["security"] = json!([{"type":"oauth2","scopes":vec!["scope";257]}]);
    assert_eq!(
        compiled(&v).authentication("s").unwrap_err().code,
        Code::Limit
    );
    v["components"]["securitySchemes"]["token"] =
        json!({"type":"oauth2","scopes":["s".repeat(1024)]});
    v["servers"]["s"]["security"] = json!(vec![
        json!({"$ref":"#/components/securitySchemes/token"});
        10
    ]);
    let text = v.to_string();
    let d = Document::parse_with(
        &text,
        None,
        dynamic_asyncapi_client::Limits {
            source_bytes: text.len(),
            ..Default::default()
        },
    )
    .unwrap();
    assert_eq!(
        d.operation_id("emit")
            .unwrap()
            .compile()
            .unwrap()
            .authentication("s")
            .unwrap_err()
            .code,
        Code::Limit
    );
}
#[test]
fn unknown_server_and_spurious_security_choice_refuse() {
    let c = compiled(&source("3.1.0"));
    assert_eq!(
        c.authentication("missing").unwrap_err().code,
        Code::InvalidConfiguration
    );
    let mut o = PlanOptions::application();
    o.security.server = Some(0);
    assert_eq!(c.prepare(&o).unwrap_err().code, Code::InvalidConfiguration);
}
