use bytes::Bytes;
use dynamic_asyncapi_client::{
    Code, Codec, Document, Json, Payload, Plan, PlanOptions, Requirement, WebSocketFrame,
};
use serde_json::json;
fn document(media: &str) -> Document {
    Document::parse(&json!({"asyncapi":"3.1.0","info":{"title":"codecs","version":"1"},"servers":{"s":{"host":"example.test","protocol":"ws"}},"channels":{"c":{"address":"/events","messages":{"m":{"contentType":media}}}},"operations":{"emit":{"action":"send","channel":{"$ref":"#/channels/c"}}}}).to_string()).unwrap()
}
fn plan(media: &str) -> Plan {
    document(media)
        .operation_id("emit")
        .unwrap()
        .compile()
        .unwrap()
        .prepare(&PlanOptions::application())
        .unwrap()
}
#[test]
fn content_type_selects_encoding_and_exposes_the_websocket_frame_policy() {
    for (media, codec, frame) in [
        (
            "application/octet-stream",
            Codec::Binary,
            WebSocketFrame::Binary,
        ),
        ("application/json", Codec::Json, WebSocketFrame::Text),
        (
            "Application/Vnd.events+JSON; charset=\"UTF-8\"",
            Codec::Json,
            WebSocketFrame::Text,
        ),
        ("text/plain", Codec::Utf8, WebSocketFrame::Text),
        (
            "text/plain; charset=utf-8",
            Codec::Utf8,
            WebSocketFrame::Text,
        ),
    ] {
        let plan = plan(media);
        assert_eq!(plan.describe().codec, codec);
        assert_eq!(plan.websocket_frame(), Some(frame));
    }
    for media in [
        "application/xml",
        "application/json; charset=utf-16",
        "text/plain; charset=ascii",
        "application/json; charset=utf-8; charset=utf-8",
        "application/*+json",
        "text/plain; language=en",
    ] {
        let err = document(media)
            .operation_id("emit")
            .unwrap()
            .compile()
            .unwrap()
            .prepare(&PlanOptions::application())
            .unwrap_err();
        assert!(matches!(err.requirement, Some(Requirement::Codec { .. })));
    }
}
#[test]
fn framing_override_is_explicit_and_cannot_turn_arbitrary_binary_into_text() {
    let mut options = PlanOptions::application();
    options.websocket_frame = Some(WebSocketFrame::Binary);
    let plan = document("application/json")
        .operation_id("emit")
        .unwrap()
        .compile()
        .unwrap()
        .prepare(&options)
        .unwrap();
    assert_eq!(plan.describe().codec, Codec::Json);
    assert_eq!(plan.websocket_frame(), Some(WebSocketFrame::Binary));
    options.websocket_frame = Some(WebSocketFrame::Text);
    assert_eq!(
        document("application/octet-stream")
            .operation_id("emit")
            .unwrap()
            .compile()
            .unwrap()
            .prepare(&options)
            .unwrap_err()
            .code,
        Code::InvalidConfiguration
    );
}
#[test]
fn decoded_json_retains_wire_bytes_and_exact_values_independently() {
    let source = b"  {\"id\":900719925474099312345,\"zero\":-0} \n";
    let plan = plan("application/json");
    let payload = plan.decode_payload(Bytes::from_static(source)).unwrap();
    let id = payload.as_json().unwrap().get("id").unwrap();
    assert_eq!(payload.as_bytes(), source);
    assert_eq!(
        payload
            .as_json()
            .unwrap()
            .get("zero")
            .unwrap()
            .number_text(),
        Some("-0")
    );
    drop(plan);
    drop(payload);
    assert_eq!(id.number_text(), Some("900719925474099312345"));
}
#[test]
fn malformed_wire_payloads_refuse_and_typed_values_cannot_bypass_the_selected_codec() {
    let json = plan("application/json");
    for source in [b"yaml: yes".as_slice(), b"{", b"\xff", b"{\"a\":1,\"a\":2}"] {
        assert!(json.decode_payload(Bytes::copy_from_slice(source)).is_err());
        assert!(json.prepare_bytes(source).is_err());
    }
    assert!(
        plan("text/plain")
            .decode_payload(Bytes::from_static(b"\xff"))
            .is_err()
    );
    assert!(json.prepare_payload(&Payload::text("{}")).is_err());
    let value = Payload::from_json(Json::parse("null", Default::default()).unwrap());
    assert!(json.prepare_payload(&value).is_ok());
    assert!(
        plan("application/octet-stream")
            .prepare_payload(&value)
            .is_err()
    );
}
#[test]
fn selected_yaml_values_encode_as_json_without_copying_the_containing_document() {
    let doc=Document::parse("asyncapi: 3.1.0\ninfo: {title: values, version: '1'}\nx-data: {n: 0xffffffffffffffffffff}\n").unwrap();
    let payload = Payload::from_json(doc.root().get("x-data").unwrap());
    drop(doc);
    assert_eq!(payload.as_bytes(), b"{\"n\":1208925819614629174706175}");
    assert_eq!(
        payload.as_json().unwrap().get("n").unwrap().number_text(),
        Some("1208925819614629174706175")
    );
}
#[test]
fn binary_ownership_stays_shared_and_utf8_text_can_be_empty_or_multibyte() {
    let bytes = Bytes::from_static(b"\0\xff");
    let payload = plan("application/octet-stream")
        .decode_payload(bytes.clone())
        .unwrap();
    assert_eq!(payload.as_bytes().as_ptr(), bytes.as_ptr());
    for text in ["", "snow☃ 😀"] {
        let payload = plan("text/plain")
            .decode_payload(Bytes::copy_from_slice(text.as_bytes()))
            .unwrap();
        assert_eq!(payload.as_text(), Some(text));
    }
}
