use dynamic_asyncapi_client::{Document, PlanOptions, TransportPlan};
use dynamic_asyncapi_session::{QueryCredentials, RuntimeCode, SessionPlan};

fn plan(server: &str, operation: &str) -> SessionPlan {
    let source = format!(
        r##"{{"asyncapi":"3.1.0","info":{{"title":"auth","version":"1"}},
    "servers":{{"s":{{"host":"example.test","protocol":"wss","security":{server}}}}},
    "channels":{{"c":{{"address":"/events","messages":{{"m":{{"contentType":"application/octet-stream"}}}}}}}},
    "operations":{{"emit":{{"action":"send","channel":{{"$ref":"#/channels/c"}},"security":{operation}}}}}}}"##
    );
    let plan = Document::parse(&source)
        .unwrap()
        .operation_id("emit")
        .unwrap()
        .compile()
        .unwrap()
        .prepare(&PlanOptions::application())
        .unwrap();
    SessionPlan::new(&[plan]).unwrap()
}
const KEY: &str = r#"[{"type":"httpApiKey","in":"query","name":"access key+雪"}]"#;
#[test]
fn query_keys_are_encoded_once_without_changing_plans_or_logging_secrets() {
    let plan = plan(KEY, "[]");
    let secret = "v&=+?/雪#% ";
    let credentials = QueryCredentials::new([("access key+雪", secret)]).unwrap();
    let endpoint = plan.websocket_endpoint(&credentials).unwrap();
    assert_eq!(
        endpoint.as_str(),
        "wss://example.test/events?access%20key%2B%E9%9B%AA=v%26%3D%2B%3F%2F%E9%9B%AA%23%25%20"
    );
    assert!(!format!("{credentials:?} {endpoint:?}").contains(secret));
    let TransportPlan::WebSocket6455 { endpoint, .. } = &plan.plans()[0].describe().transport
    else {
        panic!()
    };
    assert_eq!(endpoint, "wss://example.test/events");
}
#[test]
fn missing_and_unrelated_query_credentials_refuse_instead_of_being_volunteered() {
    let selected = plan(KEY, "[]");
    for credentials in [
        QueryCredentials::default(),
        QueryCredentials::new([("access key+雪", "secret"), ("unrelated", "private")]).unwrap(),
    ] {
        let error = selected.websocket_endpoint(&credentials).unwrap_err();
        assert_eq!(error.code, RuntimeCode::InvalidConfiguration);
        assert!(!format!("{error:?}").contains("private"));
    }
    assert!(
        plan("[]", "[]")
            .websocket_endpoint(&QueryCredentials::default())
            .is_ok()
    );
    assert_eq!(
        plan("[]", "[]")
            .websocket_endpoint(&QueryCredentials::new([("key", "secret")]).unwrap())
            .unwrap_err()
            .code,
        RuntimeCode::InvalidConfiguration
    );
}
#[test]
fn query_requirements_from_server_and_operation_are_conjunctive_and_deduplicated() {
    let other = r#"[{"type":"httpApiKey","in":"query","name":"scope"}]"#;
    let credentials = QueryCredentials::new([("access key+雪", "a"), ("scope", "b")]).unwrap();
    assert_eq!(
        plan(KEY, other)
            .websocket_endpoint(&credentials)
            .unwrap()
            .as_str(),
        "wss://example.test/events?access%20key%2B%E9%9B%AA=a&scope=b"
    );
    assert_eq!(
        plan(KEY, other)
            .websocket_endpoint(&QueryCredentials::new([("access key+雪", "a")]).unwrap())
            .unwrap_err()
            .code,
        RuntimeCode::InvalidConfiguration
    );
    assert_eq!(
        plan(KEY, KEY)
            .websocket_endpoint(&QueryCredentials::new([("access key+雪", "a")]).unwrap())
            .unwrap()
            .as_str(),
        "wss://example.test/events?access%20key%2B%E9%9B%AA=a"
    );
}
#[test]
fn reusing_a_plan_does_not_share_query_credentials_between_sessions() {
    let plan = plan(KEY, "[]");
    let a = plan
        .websocket_endpoint(&QueryCredentials::new([("access key+雪", "first")]).unwrap())
        .unwrap();
    let b = plan
        .websocket_endpoint(&QueryCredentials::new([("access key+雪", "second")]).unwrap())
        .unwrap();
    assert!(a.as_str().ends_with("=first"));
    assert!(b.as_str().ends_with("=second"));
    assert_eq!(
        plan.websocket_endpoint(&QueryCredentials::default())
            .unwrap_err()
            .code,
        RuntimeCode::InvalidConfiguration
    );
}
#[test]
fn query_credential_admission_bounds_count_utf8_and_reject_duplicates() {
    assert!(QueryCredentials::new((0..32).map(|i| (i.to_string(), String::new()))).is_ok());
    assert!(QueryCredentials::new((0..33).map(|i| (i.to_string(), String::new()))).is_err());
    assert!(QueryCredentials::new([("x", "a"), ("x", "b")]).is_err());
    assert!(QueryCredentials::new([("雪".repeat(86), String::new())]).is_err());
    assert!(QueryCredentials::new([("x", "雪".repeat(5462))]).is_err());
    assert!(QueryCredentials::new((0..4).map(|i| (i.to_string(), "x".repeat(16384)))).is_err());
    assert!(QueryCredentials::new([("", "")]).is_ok());
}
