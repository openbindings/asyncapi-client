use dynamic_asyncapi_client::{Code, Document, Json, Limits};
use serde::{
    Deserialize, Serialize,
    ser::{SerializeMap, SerializeSeq, SerializeStruct},
};

#[test]
fn strict_values_preserve_exact_tokens_and_outlive_their_parent() {
    let text = r#" {"n":900719925474099312345,"zero":-0,"exponent":1e400,"$serde_json::private::Number":"literal"} "#;
    let value = Json::parse(text, Limits::default()).unwrap();
    let n = value.get("n").unwrap();
    assert_eq!(value.get("zero").unwrap().number_text(), Some("-0"));
    assert_eq!(value.get("exponent").unwrap().number_text(), Some("1e400"));
    assert_eq!(
        value.get("$serde_json::private::Number").unwrap().as_str(),
        Some("literal")
    );
    drop(value);
    assert_eq!(n.number_text(), Some("900719925474099312345"));
    assert_eq!(n.deserialize::<u128>().unwrap(), 900719925474099312345);
}

#[test]
fn strict_json_refuses_yaml_duplicates_trailing_values_and_malformed_unicode() {
    for text in [
        "yes",
        "a: 1",
        "1 2",
        "NaN",
        "[1,]",
        r#""\ud800""#,
        r#"{"\udfff":1}"#,
    ] {
        assert_eq!(
            Json::parse(text, Limits::default()).unwrap_err().code,
            Code::InvalidJson,
            "{text}"
        );
    }
    assert_eq!(
        Json::parse(r#"{"a":1,"\u0061":2}"#, Limits::default())
            .unwrap_err()
            .code,
        Code::DuplicateMember
    );
}

#[test]
fn ordinary_rust_values_retain_integers_enums_and_negative_zero() {
    #[derive(Serialize, Deserialize, Debug, PartialEq)]
    struct Event {
        id: u128,
        delta: i128,
        state: State,
        optional: Option<String>,
        zero: f64,
    }
    #[derive(Serialize, Deserialize, Debug, PartialEq)]
    enum State {
        Ready,
        Pair(u32, String),
        Named { value: bool },
    }
    let original = Event {
        id: u128::MAX,
        delta: i128::MIN,
        state: State::Pair(42, "ok".into()),
        optional: None,
        zero: -0.0,
    };
    let json = Json::from_serializable(&original, Limits::default()).unwrap();
    assert_eq!(
        json.get("id").unwrap().number_text(),
        Some(u128::MAX.to_string().as_str())
    );
    assert_eq!(json.deserialize::<Event>().unwrap(), original);
    assert!(
        json.get("zero")
            .unwrap()
            .deserialize::<f64>()
            .unwrap()
            .is_sign_negative()
    );
    let value: serde_json::Value =
        serde_json::from_str(r#"{"exact":123456789012345678901234567890.00001}"#).unwrap();
    assert_eq!(
        Json::from_serializable(&value, Limits::default())
            .unwrap()
            .get("exact")
            .unwrap()
            .number_text(),
        Some("123456789012345678901234567890.00001")
    );
}

#[test]
fn nonfinite_and_swallowed_serializer_errors_cannot_become_null() {
    for value in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
        assert_eq!(
            Json::from_serializable(&vec![value], Limits::default())
                .unwrap_err()
                .code,
            Code::InvalidValue
        );
    }
    struct Swallow;
    impl Serialize for Swallow {
        fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
            let mut seq = serializer.serialize_seq(Some(1))?;
            let _ = seq.serialize_element(&f64::NAN);
            let _ = seq.serialize_element(&());
            seq.end()
        }
    }
    assert_eq!(
        Json::from_serializable(&Swallow, Limits::default())
            .unwrap_err()
            .code,
        Code::InvalidValue
    );
}

#[test]
fn construction_and_admission_enforce_byte_node_and_depth_bounds() {
    for limits in [
        Limits {
            source_bytes: 4,
            ..Default::default()
        },
        Limits {
            nodes: 1,
            ..Default::default()
        },
        Limits {
            depth: 0,
            ..Default::default()
        },
    ] {
        assert_eq!(
            Json::from_serializable(&vec!["x"], limits)
                .unwrap_err()
                .code,
            Code::Limit
        );
    }
    let limits = Limits {
        source_bytes: 5,
        nodes: 2,
        depth: 1,
        ..Default::default()
    };
    assert_eq!(
        Json::from_serializable(&vec!["x"], limits)
            .unwrap()
            .to_json(),
        r#"["x"]"#
    );
    for text in [r#"["x"]"#, r#"{"x":1}"#] {
        assert_eq!(
            Json::parse(
                text,
                Limits {
                    depth: 0,
                    ..Default::default()
                }
            )
            .unwrap_err()
            .code,
            Code::Limit
        );
    }
    assert!(
        Json::from_serializable(
            &Vec::<u8>::new(),
            Limits {
                depth: 0,
                ..Default::default()
            }
        )
        .is_ok()
    );
    assert!(
        Json::parse(
            "[]",
            Limits {
                depth: 0,
                ..Default::default()
            }
        )
        .is_ok()
    );
}

#[test]
fn private_number_protocol_is_validated_and_literal_map_keys_remain_data() {
    struct FakeNumber(&'static str);
    impl Serialize for FakeNumber {
        fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
            let mut value = serializer.serialize_struct("$serde_json::private::Number", 1)?;
            value.serialize_field("$serde_json::private::Number", self.0)?;
            value.end()
        }
    }
    for text in ["null", "[]", "1,2", " 1", "NaN"] {
        assert_eq!(
            Json::from_serializable(&FakeNumber(text), Limits::default())
                .unwrap_err()
                .code,
            Code::InvalidValue
        );
    }
    let value = Json::from_serializable(
        &std::collections::BTreeMap::from([("$serde_json::private::Number", "literal")]),
        Limits::default(),
    )
    .unwrap();
    assert_eq!(value.kind(), "object");
    assert_eq!(
        value.get("$serde_json::private::Number").unwrap().as_str(),
        Some("literal")
    );
    let raw = serde_json::value::RawValue::from_string("[1]".into()).unwrap();
    assert_eq!(
        Json::from_serializable(&raw, Limits::default())
            .unwrap_err()
            .code,
        Code::InvalidValue
    );
}

#[test]
fn duplicate_serde_keys_and_sensitive_custom_errors_fail_safely() {
    struct Duplicate;
    impl Serialize for Duplicate {
        fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
            let mut map = serializer.serialize_map(Some(2))?;
            map.serialize_entry(&1, &"first")?;
            map.serialize_entry("1", &"second")?;
            map.end()
        }
    }
    assert_eq!(
        Json::from_serializable(&Duplicate, Limits::default())
            .unwrap_err()
            .code,
        Code::DuplicateMember
    );
    struct Sensitive;
    impl Serialize for Sensitive {
        fn serialize<S: serde::Serializer>(&self, _: S) -> Result<S::Ok, S::Error> {
            Err(serde::ser::Error::custom("secret-message-123"))
        }
    }
    let error = Json::from_serializable(&Sensitive, Limits::default()).unwrap_err();
    assert!(!format!("{error:?} {error}").contains("secret-message-123"));
    let error = Json::parse(r#""secret-message-123""#, Limits::default())
        .unwrap()
        .deserialize::<u64>()
        .unwrap_err();
    assert!(!format!("{error:?} {error}").contains("secret-message-123"));
    assert!(error.detail().to_string().contains("secret-message-123"));
}

#[test]
fn typed_projection_of_yaml_views_uses_exact_json_not_authored_yaml() {
    let doc = Document::parse(
        "asyncapi: 3.1.0\ninfo: {title: value, version: '1'}\nx-number: 0xffffffffffffffffffff\n",
    )
    .unwrap();
    let number = doc.root().get("x-number").unwrap();
    drop(doc);
    assert_eq!(
        number.deserialize::<u128>().unwrap(),
        1208925819614629174706175
    );
    assert_eq!(number.raw(), "0xffffffffffffffffffff");
}

#[test]
fn document_unicode_errors_return_locations_without_panicking() {
    let value = Document::parse(r#"{"asyncapi":"3.1.0","x":"\ud800"}"#).unwrap_err();
    assert_eq!(value.code, Code::InvalidJson);
    assert_eq!(value.location.unwrap().pointer, "/x");
    let key = Document::parse(r#"{"asyncapi":"3.1.0","\udfff":1}"#).unwrap_err();
    assert_eq!(key.code, Code::InvalidJson);
    assert_eq!(key.location.unwrap().pointer, "");
}
