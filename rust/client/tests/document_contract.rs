//! Development cases derived from the pinned specifications, not a maturity oracle.
use dynamic_asyncapi_client::{Action, Code, Document, Edition, Limits, Requirement};

fn v3(extra: &str) -> String {
    format!(r#"{{"asyncapi":"3.1.0","info":{{"title":"test","version":"1"}},{extra}}}"#)
}
fn v2(channels: &str, extra: &str) -> String {
    format!(
        r#"{{"asyncapi":"2.6.0","info":{{"title":"test","version":"1"}},"channels":{channels}{extra}}}"#
    )
}
fn single_operation(extra: &str) -> String {
    v3(&format!(
        r##""channels":{{"c":{{"address":null}}}},"operations":{{"go":{{"action":"send","channel":{{"$ref":"#/channels/c"}}{extra}}}}}"##
    ))
}

#[test]
fn reader_family_accepts_patch_revisions_and_refuses_unimplemented_families() {
    for (version, edition) in [
        ("2.6.0", Edition::V2_6),
        ("2.6.19", Edition::V2_6),
        ("3.0.7", Edition::V3_0),
        ("3.1.0", Edition::V3_1),
        ("3.1.0-rc1", Edition::V3_1),
    ] {
        let text = v3(r#""channels":{}"#).replace("3.1.0", version);
        assert_eq!(Document::parse(&text).unwrap().edition(), edition);
    }
    for version in ["2.5.0", "3.2.0", "3.1", "3.1.-1", "3.1.0-", "3.1.0-rc.1"] {
        let text = v3(r#""channels":{}"#).replace("3.1.0", version);
        assert_eq!(
            Document::parse(&text).unwrap_err().code,
            Code::UnsupportedVersion
        );
    }
}

#[test]
fn exact_authored_numbers_unknown_members_and_owning_views_survive() {
    let text = v3(r#""x-data":{"n":900719925474099312345,"e":1.2500e+900,"z":-0,"a/b~c":"☃"}"#);
    let document = Document::parse_at(&text, "https://example.test/api.json").unwrap();
    let data = document.root().get("x-data").unwrap();
    let snow = data.pointer("/a~1b~0c").unwrap();
    assert_eq!(
        data.get("n").unwrap().number_text().unwrap(),
        "900719925474099312345"
    );
    assert_eq!(data.get("e").unwrap().number_text().unwrap(), "1.2500e+900");
    assert_eq!(data.get("z").unwrap().number_text().unwrap(), "-0");
    assert!(data.to_json().contains("\"z\":-0"));
    assert_eq!(snow.as_str(), Some("☃"));
    assert_eq!(&text[snow.location().bytes], "\"☃\"");
    assert_eq!(snow.location().pointer, "/x-data/a~1b~0c");
    assert!(data.pointer("/a~2b").is_none());
    drop(document);
    assert_eq!(snow.as_str(), Some("☃"));
    assert!(snow.source_text().contains("900719925474099312345"));
}

#[test]
fn invalid_two_x_channel_is_visible_without_hiding_good_channel() {
    let doc = Document::parse(&v2(r#"{"bad":false,"good":{"subscribe":{}}}"#, "")).unwrap();
    let entries: Vec<_> = doc.operations().collect();
    assert_eq!(entries.len(), 2);
    assert_eq!(entries[0].as_ref().unwrap_err().code, Code::InvalidDocument);
    assert!(
        doc.operation_at("/channels/good/subscribe")
            .unwrap()
            .describe()
            .is_ok()
    );
}

#[test]
fn serde_private_looking_keys_are_ordinary_application_data() {
    let text = v3(r#""x-data":{"$serde_json::private::Number":"42","other":true}"#);
    let document = Document::parse(&text).unwrap();
    let data = document.root().get("x-data").unwrap();
    assert_eq!(
        data.get("$serde_json::private::Number").unwrap().as_str(),
        Some("42")
    );
    assert_eq!(data.get("other").unwrap().as_bool(), Some(true));
}

#[test]
fn duplicate_decoded_keys_have_original_source_coordinates() {
    let text = v3(r#""x-data":{"a":1,"\u0061":2}"#);
    let error = Document::parse(&text).unwrap_err();
    assert_eq!(error.code, Code::DuplicateMember);
    let location = error.location.unwrap();
    assert_eq!(location.pointer, "/x-data/a");
    assert_eq!(&text[location.bytes], r#""\u0061""#);
}

#[test]
fn admission_rejects_malformed_sources_and_enforces_resource_bounds() {
    for text in ["", "{} true", "{", "[]", "null", "{\"asyncapi\":3}"] {
        assert!(Document::parse(text).is_err());
    }
    let text = single_operation("");
    for limits in [
        Limits {
            source_bytes: 4,
            ..Limits::default()
        },
        Limits {
            total_source_bytes: 4,
            ..Limits::default()
        },
        Limits {
            nodes: 2,
            ..Limits::default()
        },
        Limits {
            depth: 1,
            ..Limits::default()
        },
        Limits {
            resources: 0,
            ..Limits::default()
        },
    ] {
        assert_eq!(
            Document::parse_with(&text, None, limits).unwrap_err().code,
            Code::Limit
        );
    }
    let doc = Document::parse_with(
        &text,
        None,
        Limits {
            resources: 1,
            ..Limits::default()
        },
    )
    .unwrap();
    assert_eq!(
        doc.with_resource("https://example.test/extra", "{}")
            .unwrap_err()
            .code,
        Code::Limit
    );
    let deep = v3(&format!(
        r#""x-deep":{}0{}"#,
        "[".repeat(500),
        "]".repeat(500)
    ));
    assert_eq!(
        Document::parse_with(
            &deep,
            None,
            Limits {
                depth: usize::MAX,
                ..Limits::default()
            }
        )
        .unwrap_err()
        .code,
        Code::Limit
    );
}

#[test]
fn two_six_operation_directions_describe_the_application() {
    let doc = Document::parse(&v2(
        r#"{"events":{"publish":{"operationId":"accept"},"subscribe":{"operationId":"emit"}}}"#,
        "",
    ))
    .unwrap();
    assert_eq!(
        doc.operation_id("accept")
            .unwrap()
            .describe()
            .unwrap()
            .action,
        Action::Receive
    );
    let emit = doc.operation_id("emit").unwrap();
    assert_eq!(emit.identity().pointer, "/channels/events/subscribe");
    let facts = emit.describe().unwrap();
    assert_eq!(facts.action, Action::Send);
    assert_eq!(facts.address.as_deref(), Some("events"));
}

#[test]
fn absent_and_null_three_x_addresses_remain_unknown() {
    for channel in [r#"{}"#, r#"{"address":null}"#] {
        let source = single_operation("").replace(r#"{"address":null}"#, channel);
        let doc = Document::parse(&source).unwrap();
        let facts = doc.operation_id("go").unwrap().describe().unwrap();
        assert_eq!(facts.action, Action::Send);
        assert_eq!(facts.address, None);
    }
}

#[test]
fn missing_dependency_recovery_creates_a_new_snapshot_and_keeps_native_identity() {
    let source = v2(r#"{"events":{"$ref":"parts/channel.json"}}"#, "");
    let old = Document::parse_at(&source, "https://example.test/api.json").unwrap();
    let error = old.operation_at("/channels/events/subscribe").unwrap_err();
    assert_eq!(error.code, Code::MissingResource);
    assert_eq!(
        error.requirement,
        Some(Requirement::Resource {
            uri: "https://example.test/parts/channel.json".into()
        })
    );
    let external = r#"{"subscribe":{"operationId":"emit"}}"#;
    let complete = old
        .with_resource("https://example.test/parts/channel.json", external)
        .unwrap();
    let op = complete.operation_at("/channels/events/subscribe").unwrap();
    assert_eq!(
        op.identity().uri.as_deref(),
        Some("https://example.test/api.json")
    );
    assert_eq!(op.identity().pointer, "/channels/events/subscribe");
    assert_eq!(
        op.location().uri.as_deref(),
        Some("https://example.test/parts/channel.json")
    );
    assert_eq!(op.location().pointer, "/subscribe");
    assert_eq!(&external[op.location().bytes], r#"{"operationId":"emit"}"#);
    assert_eq!(
        old.operations().next().unwrap().unwrap_err().code,
        Code::MissingResource
    );
    drop(complete);
    assert_eq!(op.describe().unwrap().address.as_deref(), Some("events"));
}

#[test]
fn resource_identity_is_normalized_and_immutable() {
    let doc =
        Document::parse_at(&single_operation(""), "https://EXAMPLE.test/a/../api.json").unwrap();
    assert_eq!(
        doc.root().location().uri.as_deref(),
        Some("https://example.test/api.json")
    );
    let doc = doc
        .with_resource("https://example.test/parts", "{}")
        .unwrap();
    assert!(
        doc.with_resource("https://EXAMPLE.test/parts", "{}")
            .is_ok()
    );
    assert_eq!(
        doc.with_resource("https://example.test/parts", "[]")
            .unwrap_err()
            .code,
        Code::InvalidReference
    );
    for uri in [
        "relative.json",
        "https://user:secret@example.test/a",
        "https://example.test/a#",
    ] {
        assert_eq!(
            Document::parse_at(&single_operation(""), uri)
                .unwrap_err()
                .code,
            Code::InvalidReference
        );
    }
}

#[test]
fn anonymous_relative_dependencies_require_a_source_uri() {
    let doc = Document::parse(&v2(r#"{"events":{"$ref":"parts.json"}}"#, "")).unwrap();
    let error = doc.operations().next().unwrap().unwrap_err();
    assert_eq!(error.requirement, Some(Requirement::SourceUri));
    assert!(!error.to_string().contains("parts.json"));
}

#[test]
fn references_ignore_siblings_and_resolve_percent_escaped_json_pointers() {
    let doc = Document::parse(&v3(r##""channels":{"a/b":{"address":"real"}},"operations":{"a/~":{"$ref":"#/components/operations/%C3%A9","action":"receive"}},"components":{"operations":{"é":{"action":"send","channel":{"$ref":"#/channels/a~1b"}}}}"##)).unwrap();
    let op = doc.operation_id("a/~").unwrap();
    assert_eq!(op.identity().pointer, "/operations/a~1~0");
    let facts = op.describe().unwrap();
    assert_eq!(facts.action, Action::Send);
    assert_eq!(facts.address.as_deref(), Some("real"));
}

#[test]
fn root_operation_must_select_root_channel_before_following_external_definition() {
    let source = v3(
        r##""channels":{"c":{"$ref":"channel.json"}},"operations":{"go":{"action":"send","channel":{"$ref":"#/channels/c"}}}"##,
    );
    let doc = Document::parse_at(&source, "https://example.test/api.json")
        .unwrap()
        .with_resource(
            "https://example.test/channel.json",
            r#"{"address":"events"}"#,
        )
        .unwrap();
    assert_eq!(
        doc.operation_id("go")
            .unwrap()
            .describe()
            .unwrap()
            .address
            .as_deref(),
        Some("events")
    );
    let bad = source.replace(r##""$ref":"#/channels/c""##, r#""$ref":"channel.json""#);
    let doc = Document::parse_at(&bad, "https://example.test/api.json")
        .unwrap()
        .with_resource(
            "https://example.test/channel.json",
            r#"{"address":"events"}"#,
        )
        .unwrap();
    assert_eq!(
        doc.operation_id("go").unwrap().describe().unwrap_err().code,
        Code::InvalidOperation
    );
}

#[test]
fn broken_operations_do_not_hide_unrelated_three_x_operations() {
    let source = single_operation("").replace(
        r#""go":{"#,
        r##""bad":{"$ref":"missing.json"},"cycle":{"$ref":"#/operations/cycle"},"go":{"##,
    );
    let doc = Document::parse_at(&source, "https://example.test/api").unwrap();
    assert_eq!(doc.operations().count(), 3);
    assert!(doc.operation_id("go").unwrap().describe().is_ok());
    assert_eq!(
        doc.operation_id("bad")
            .unwrap()
            .describe()
            .unwrap_err()
            .code,
        Code::MissingResource
    );
    assert_eq!(
        doc.operation_id("cycle")
            .unwrap()
            .describe()
            .unwrap_err()
            .code,
        Code::ReferenceCycle
    );
}

#[test]
fn trait_order_and_target_precedence_follow_merge_patch() {
    let doc = Document::parse(&single_operation(r#", "summary":"authored", "traits":[{"summary":"first","description":"first"},{"summary":"second","description":"second"}]"#)).unwrap();
    let facts = doc.operation_id("go").unwrap().describe().unwrap();
    assert_eq!(facts.summary.as_deref(), Some("authored"));
    assert_eq!(facts.description.as_deref(), Some("second"));
    let doc = Document::parse(&single_operation(
        r#", "traits":[{"description":"first"},{"description":null}]"#,
    ))
    .unwrap();
    assert_eq!(
        doc.operation_id("go")
            .unwrap()
            .describe()
            .unwrap()
            .description,
        None
    );
}

#[test]
fn traits_cannot_supply_forbidden_operation_fields() {
    for field in ["action", "channel", "messages", "traits"] {
        let doc = Document::parse(&single_operation(&format!(
            r#", "traits":[{{"{field}":null}}]"#
        )))
        .unwrap();
        assert_eq!(
            doc.operation_id("go").unwrap().describe().unwrap_err().code,
            Code::InvalidOperation
        );
    }
    for field in ["message", "traits"] {
        let doc = Document::parse(&v2(
            &format!(r#"{{"c":{{"subscribe":{{"traits":[{{"{field}":null}}]}}}}}}"#),
            "",
        ))
        .unwrap();
        assert_eq!(
            doc.operation_at("/channels/c/subscribe")
                .unwrap()
                .describe()
                .unwrap_err()
                .code,
            Code::InvalidOperation
        );
    }
}

#[test]
fn inherited_operation_ids_participate_in_two_x_ambiguity_checks() {
    let doc = Document::parse(&v2(r#"{"c":{"subscribe":{"traits":[{"operationId":"emit"}]}},"d":{"subscribe":{"operationId":"emit"}}}"#, "")).unwrap();
    assert_eq!(
        doc.operation_id("emit").unwrap_err().code,
        Code::AmbiguousOperation
    );
    assert_eq!(
        doc.operation_at("/channels/c/subscribe")
            .unwrap()
            .authored_id()
            .unwrap()
            .as_deref(),
        Some("emit")
    );
}

#[test]
fn trait_merge_and_reference_work_are_bounded() {
    let source = single_operation(r#", "traits":[{"description":"first"},{"summary":"second"}]"#);
    for limits in [
        Limits {
            trait_count: 1,
            ..Limits::default()
        },
        Limits {
            merge_nodes: 2,
            ..Limits::default()
        },
    ] {
        let doc = Document::parse_with(&source, None, limits).unwrap();
        assert_eq!(
            doc.operation_id("go").unwrap().describe().unwrap_err().code,
            Code::Limit
        );
    }
    let source = single_operation("").replace(
        r#""go":{"#,
        r##""go":{"$ref":"#/operations/second"},"second":{"##,
    );
    let doc = Document::parse_with(
        &source,
        None,
        Limits {
            reference_steps: 0,
            ..Limits::default()
        },
    )
    .unwrap();
    assert_eq!(
        doc.operation_id("go").unwrap().describe().unwrap_err().code,
        Code::Limit
    );
}

#[test]
fn explicit_selection_reports_missing_nested_dependency_at_its_actual_source() {
    let doc = Document::parse_at(
        &v2(r#"{"events":{"$ref":"channel.json#/shared"}}"#, ""),
        "https://example.test/api.json",
    )
    .unwrap()
    .with_resource(
        "https://example.test/channel.json",
        r#"{"shared":{"$ref":"missing.json"}}"#,
    )
    .unwrap();
    let error = doc.operation_at("/channels/events/subscribe").unwrap_err();
    assert_eq!(error.code, Code::MissingResource);
    assert_eq!(
        error.location.unwrap().uri.as_deref(),
        Some("https://example.test/channel.json")
    );
    assert_eq!(
        error.requirement,
        Some(Requirement::Resource {
            uri: "https://example.test/missing.json".into()
        })
    );
}
