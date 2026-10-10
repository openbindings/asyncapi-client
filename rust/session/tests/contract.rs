use dynamic_asyncapi_client::{Document, PlanOptions};
use dynamic_asyncapi_session::{Budget, ConnectionPlan, Limits, Route, RuntimeCode, SessionPlan};
fn plans() -> Vec<dynamic_asyncapi_client::Plan> {
    let source = r##"{"asyncapi":"3.1.0","info":{"title":"session","version":"1"},"servers":{"s":{"host":"example.test","protocol":"wss"}},"channels":{"c":{"address":"/events","messages":{"m":{"contentType":"application/octet-stream"}}}},"operations":{"emit":{"action":"send","channel":{"$ref":"#/channels/c"}},"listen":{"action":"receive","channel":{"$ref":"#/channels/c"}},"other":{"action":"receive","channel":{"$ref":"#/channels/c"}}}}"##;
    let d = Document::parse(source).unwrap();
    ["emit", "listen", "other"]
        .into_iter()
        .map(|id| {
            d.operation_id(id)
                .unwrap()
                .compile()
                .unwrap()
                .prepare(&PlanOptions::application())
                .unwrap()
        })
        .collect()
}
#[test]
fn plans_survive_sources_and_route_without_a_runtime() {
    let plans = plans();
    let session = SessionPlan::new(&plans[..2]).unwrap();
    drop(plans);
    assert_eq!(
        session.connection(),
        &ConnectionPlan::WebSocket("wss://example.test/events".into())
    );
    assert_eq!(session.websocket_route(true), Route::Operation(1));
    assert!(matches!(session.websocket_route(false), Route::Rejected(_)));
    assert!(session.send_plan(0).is_ok());
    assert_eq!(
        session.send_plan(1).err().unwrap().code,
        RuntimeCode::InvalidConfiguration
    );
    assert_eq!(
        session.send_plan(2).err().unwrap().code,
        RuntimeCode::InvalidConfiguration
    );
}
#[test]
fn ambiguity_and_duplicate_native_operations_fail_before_io() {
    let plans = plans();
    assert_eq!(
        SessionPlan::new(&plans).err().unwrap().code,
        RuntimeCode::Unsupported
    );
    assert_eq!(
        SessionPlan::new(&[plans[0].clone(), plans[0].clone()])
            .err()
            .unwrap()
            .code,
        RuntimeCode::InvalidConfiguration
    );
    assert_eq!(
        SessionPlan::new(&[]).err().unwrap().code,
        RuntimeCode::InvalidConfiguration
    );
}
#[test]
fn rejected_reservations_do_not_consume_either_limit() {
    let budget = Budget::new(Limits {
        max_messages: 2,
        max_buffered_bytes: 4,
        max_message_bytes: 4,
    })
    .unwrap();
    let lease = budget.reserve(4).unwrap();
    assert_eq!(
        budget.reserve(1).err().unwrap().code,
        RuntimeCode::Backpressure
    );
    assert_eq!(budget.usage().messages, 1);
    assert_eq!(budget.usage().bytes, 4);
    let empty = budget.reserve(0).unwrap();
    assert!(budget.reserve(0).is_err());
    drop(lease);
    let lease = budget.reserve(4).unwrap();
    drop(empty);
    drop(lease);
    assert_eq!(budget.usage(), Default::default());
    assert!(budget.reserve(usize::MAX).is_err());
}
#[test]
fn reservations_remain_bounded_and_release_across_threads() {
    let budget = Budget::new(Limits {
        max_messages: 4,
        max_buffered_bytes: 64,
        max_message_bytes: 16,
    })
    .unwrap();
    std::thread::scope(|scope| {
        for _ in 0..8 {
            let budget = budget.clone();
            scope.spawn(move || {
                for _ in 0..1000 {
                    if let Ok(lease) = budget.reserve(16) {
                        let usage = budget.usage();
                        assert!(usage.messages <= 4 && usage.bytes <= 64);
                        std::thread::yield_now();
                        drop(lease);
                    }
                }
            });
        }
    });
    assert_eq!(budget.usage(), Default::default());
}

#[test]
fn portable_mqtt_routing_treats_requested_qos_as_a_maximum() {
    for requested in 0..=2 {
        let source = r##"{"asyncapi":"3.1.0","info":{"title":"qos","version":"1"},"servers":{"s":{"host":"example.test","protocol":"mqtt","protocolVersion":"3.1.1","bindings":{"mqtt":{"clientId":"routing"}}}},"channels":{"c":{"address":"events","messages":{"m":{"contentType":"application/octet-stream"}}}},"operations":{"listen":{"action":"receive","channel":{"$ref":"#/channels/c"},"bindings":{"mqtt":{"qos":QOS}}}}}"##.replace("QOS",&requested.to_string());
        let document = Document::parse(&source).unwrap();
        let plan = document
            .operation_id("listen")
            .unwrap()
            .compile()
            .unwrap()
            .prepare(&PlanOptions::application())
            .unwrap();
        let session = SessionPlan::new(&[plan]).unwrap();
        for actual in 0..=2 {
            let route = session.mqtt_route("events", actual);
            if actual <= requested {
                assert_eq!(route, Route::Operation(0));
            } else {
                assert!(matches!(route, Route::Rejected(_)));
            }
        }
        assert!(matches!(session.mqtt_route("other", 0), Route::Rejected(_)));
    }
}
