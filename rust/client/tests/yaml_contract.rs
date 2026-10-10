//! Development assertions against the registered YAML adapter policy.
use dynamic_asyncapi_client::{Action, Code, Document, Limits};

const HEADER: &str = "asyncapi: 3.1.0\ninfo:\n  title: YAML development\n  version: '1'\n";
fn source(extra: &str) -> String {
    format!("{HEADER}{extra}")
}

#[test]
fn yaml_inspection_uses_the_same_native_operation_semantics() {
    let document=Document::parse(&source("channels:\n  events:\n    address: notifications\noperations:\n  emit:\n    action: send\n    channel:\n      $ref: '#/channels/events'\n")).unwrap();
    let operation = document.operation_id("emit").unwrap();
    assert_eq!(operation.describe().unwrap().action, Action::Send);
    assert_eq!(
        operation.describe().unwrap().address.as_deref(),
        Some("notifications")
    );
    assert_eq!(operation.identity().pointer, "/operations/emit");
    let json = document.root().to_json();
    let equivalent = Document::parse(&json).unwrap();
    assert_eq!(
        equivalent
            .operation_id("emit")
            .unwrap()
            .describe()
            .unwrap()
            .action,
        Action::Send
    );
}

#[test]
fn yaml_large_numbers_radix_and_negative_zero_are_exact() {
    let document=Document::parse(&source("x-values:\n  huge: 900719925474099312345\n  exponent: 1.2500e+900\n  negative: -0\n  hex: 0xffffffffffffffffffff\n  octal: 0o777\n  leading: +00012.50\n  fractional: .5\n")).unwrap();
    let values = document.root().get("x-values").unwrap();
    for (key, expected) in [
        ("huge", "900719925474099312345"),
        ("exponent", "1.2500e+900"),
        ("negative", "-0"),
        ("hex", "1208925819614629174706175"),
        ("octal", "511"),
        ("leading", "12.50"),
        ("fractional", "0.5"),
    ] {
        assert_eq!(values.get(key).unwrap().number_text(), Some(expected));
    }
    assert_eq!(values.get("hex").unwrap().raw(), "0xffffffffffffffffffff");
    let normalized = values.to_json();
    assert!(normalized.contains("\"negative\":-0"));
    assert!(normalized.contains("1208925819614629174706175"));
    assert!(!normalized.contains("0xffff"));
}

#[test]
fn mapping_keys_follow_the_string_failsafe_rule() {
    let document = Document::parse(&source(
        "x-values:\n  true: yes\n  42: no\n  null: null\n  <<: ordinary\n",
    ))
    .unwrap();
    let values = document.root().get("x-values").unwrap();
    assert_eq!(values.get("true").unwrap().as_str(), Some("yes"));
    assert_eq!(values.get("42").unwrap().as_str(), Some("no"));
    assert!(values.get("null").unwrap().is_null());
    assert_eq!(values.get("<<").unwrap().as_str(), Some("ordinary"));
    let error = Document::parse(&source("x-values:\n  !!int 42: value\n")).unwrap_err();
    assert_eq!(error.code, Code::UnsupportedYaml);
    let error = Document::parse(&source("x-number: &n 42\nx-values:\n  *n : value\n")).unwrap_err();
    assert_eq!(error.code, Code::UnsupportedYaml);
}

#[test]
fn scalar_styles_and_unicode_keep_original_byte_ranges() {
    let text = source(
        "x-text: |\n  snow ☃\n  second line\nx-folded: >-\n  first\n  second\nx-string: 'true'\n",
    );
    let document = Document::parse(&text).unwrap();
    let literal = document.root().get("x-text").unwrap();
    assert_eq!(literal.as_str(), Some("snow ☃\nsecond line\n"));
    assert!(literal.raw().contains("snow ☃"));
    assert_eq!(literal.raw(), &text[literal.location().bytes]);
    assert_eq!(
        document.root().get("x-folded").unwrap().as_str(),
        Some("first second")
    );
    assert_eq!(
        document.root().get("x-string").unwrap().as_str(),
        Some("true")
    );
    let normalized = literal.to_json();
    assert_eq!(
        serde_json::from_str::<String>(&normalized).unwrap(),
        "snow ☃\nsecond line\n"
    );
}

#[test]
fn alias_expansion_records_definition_and_use_separately() {
    let text = source("x-base: &base\n  n: 900719925474099312345\nx-copy: *base\n");
    let document = Document::parse_at(&text, "https://example.test/a.yaml").unwrap();
    let copy = document.root().pointer("/x-copy/n").unwrap();
    let location = copy.location();
    assert_eq!(location.pointer, "/x-copy/n");
    assert_eq!(&text[location.bytes], "900719925474099312345");
    assert_eq!(location.aliases.len(), 1);
    assert_eq!(&text[location.aliases[0].clone()], "*base");
    drop(document);
    assert_eq!(copy.number_text(), Some("900719925474099312345"));
}

#[test]
fn recursive_aliases_and_expansion_amplification_are_bounded() {
    assert_eq!(
        Document::parse(&source("x-loop: &loop [*loop]\n"))
            .unwrap_err()
            .code,
        Code::UnsupportedYaml
    );
    let text = source(
        "x-base: &base abcdefghijklmnopqrstuvwxyz\nx-copies: [*base,*base,*base,*base,*base]\n",
    );
    let limits = Limits {
        expanded_bytes: 150,
        ..Limits::default()
    };
    assert_eq!(
        Document::parse_with(&text, None, limits).unwrap_err().code,
        Code::Limit
    );
    let limits = Limits {
        nodes: 20,
        ..Limits::default()
    };
    let text = source("x-a: &a [1,2,3]\nx-b: &b [*a,*a,*a]\nx-c: [*b,*b,*b]\n");
    assert_eq!(
        Document::parse_with(&text, None, limits).unwrap_err().code,
        Code::Limit
    );
    let text = source("x-big: 0xffffffffffffffffffff\n");
    let limits = Limits {
        number_conversion_work: 100,
        ..Limits::default()
    };
    assert_eq!(
        Document::parse_with(&text, None, limits).unwrap_err().code,
        Code::Limit
    );
}

#[test]
fn duplicate_decoded_yaml_keys_are_rejected() {
    let text = source("x-values:\n  a: first\n  \"\\u0061\": second\n");
    let error = Document::parse(&text).unwrap_err();
    assert_eq!(error.code, Code::DuplicateMember);
    assert_eq!(error.location.unwrap().pointer, "/x-values/a");
    assert_eq!(
        Document::parse(&source("x-values:\n  true: first\n  'true': second\n"))
            .unwrap_err()
            .code,
        Code::DuplicateMember
    );
}

#[test]
fn unsupported_tags_versions_and_multiple_documents_fail_locally() {
    for extra in [
        "x: !!timestamp 2026-10-10\n",
        "x: !custom value\n",
        "x: .nan\n",
        "x: -.Inf\n",
        "x: !!int nope\n",
        "? [a,b]\n: value\n",
    ] {
        assert_eq!(
            Document::parse(&source(extra)).unwrap_err().code,
            Code::UnsupportedYaml,
            "{extra}"
        );
    }
    assert_eq!(
        Document::parse(&format!("%YAML 1.1\n---\n{HEADER}"))
            .unwrap_err()
            .code,
        Code::UnsupportedYaml
    );
    assert_eq!(
        Document::parse(&format!("{HEADER}---\n{HEADER}"))
            .unwrap_err()
            .code,
        Code::UnsupportedYaml
    );
    assert!(Document::parse(&format!("%YAML 1.2\n---\n{HEADER}")).is_ok());
}

#[test]
fn mixed_format_resources_preserve_their_own_sources() {
    let json = r#"{"asyncapi":"2.6.0","info":{"title":"mixed","version":"1"},"channels":{"events":{"$ref":"channel.yaml"}}}"#;
    let document = Document::parse_at(json, "https://example.test/api.json")
        .unwrap()
        .with_resource(
            "https://example.test/channel.yaml",
            "subscribe:\n  operationId: emit\n",
        )
        .unwrap();
    let operation = document.operation_id("emit").unwrap();
    assert_eq!(operation.describe().unwrap().action, Action::Send);
    assert_eq!(
        operation.location().uri.as_deref(),
        Some("https://example.test/channel.yaml")
    );
    assert!(operation.authored().raw().contains("operationId: emit"));
    assert_eq!(operation.authored().to_json(), r#"{"operationId":"emit"}"#);
}

#[test]
fn flow_style_yaml_and_private_looking_property_names_remain_data() {
    let text = source("x-values: {$serde_json::private::Number: '42', other: true}\n");
    let document = Document::parse(&text).unwrap();
    assert_eq!(
        document
            .root()
            .pointer("/x-values/$serde_json::private::Number")
            .unwrap()
            .as_str(),
        Some("42")
    );
    assert_eq!(
        document
            .root()
            .pointer("/x-values/other")
            .unwrap()
            .as_bool(),
        Some(true)
    );
}

#[test]
fn unsupported_tag_diagnostic_points_to_the_authored_tag() {
    let text = source("x-value: !custom value\n");
    let error = Document::parse(&text).unwrap_err();
    assert_eq!(error.code, Code::UnsupportedYaml);
    assert!(text[error.location.unwrap().bytes].starts_with("!custom"));
}
