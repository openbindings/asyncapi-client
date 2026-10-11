use dynamic_asyncapi_client::{Document, PlanOptions};
use dynamic_asyncapi_native::{RuntimeCode, Session, SessionOptions, TlsConfig};
const CA: &[u8] = include_bytes!("fixtures/tls/fixture-ca.pem");
const CERT: &[u8] = include_bytes!("fixtures/tls/client.pem");
const KEY: &[u8] = include_bytes!("fixtures/tls/client.key.pem");
const OTHER_KEY: &[u8] = include_bytes!("fixtures/tls/server.key.pem");

#[test]
fn tls_material_is_bounded_and_errors_do_not_expose_input() {
    for input in [
        b"private-sentinel".to_vec(),
        vec![b'x'; 4 * 1024 * 1024 + 1],
        CA.repeat(513),
    ] {
        let error = TlsConfig::from_ca_pem(&input).unwrap_err();
        assert_eq!(error.code, RuntimeCode::InvalidConfiguration);
        assert!(!format!("{error:?}").contains("private-sentinel"));
    }
    let config = TlsConfig::from_ca_pem(CA).unwrap();
    assert!(format!("{config:?}").contains("redacted"));
    assert!(config.with_client_identity(CERT, KEY).is_ok());
    for key in [
        Vec::new(),
        KEY.repeat(2),
        OTHER_KEY.to_vec(),
        vec![b'x'; 64 * 1024 + 1],
    ] {
        assert_eq!(
            config.with_client_identity(CERT, &key).unwrap_err().code,
            RuntimeCode::InvalidConfiguration
        );
    }
    assert_eq!(
        config
            .with_client_identity(&CERT.repeat(17), KEY)
            .unwrap_err()
            .code,
        RuntimeCode::InvalidConfiguration
    );
}

#[tokio::test]
async fn tls_configuration_must_match_endpoint_before_connection() {
    for protocol in ["ws", "wss", "mqtt", "mqtts"] {
        let source=serde_json::json!({"asyncapi":"3.1.0","info":{"title":"TLS","version":"1"},
            "servers":{"s":{"host":"127.0.0.1:1","protocol":protocol,"protocolVersion":if protocol.starts_with("mqtt") {"3.1.1"} else {"13"}}},
            "channels":{"c":{"address":if protocol.starts_with("mqtt") {"events"} else {"/events"},"messages":{"m":{"contentType":"application/octet-stream"}}}},
            "operations":{"send":{"action":"send","channel":{"$ref":"#/channels/c"}}}}).to_string();
        let document = Document::parse(&source).unwrap();
        let mut options = PlanOptions::application();
        if protocol.starts_with("mqtt") {
            options.client_id = Some("tls-fixture".into());
        }
        let plan = document
            .operation_id("send")
            .unwrap()
            .compile()
            .unwrap()
            .prepare(&options)
            .unwrap();
        let tls = if matches!(protocol, "mqtts" | "wss") {
            None
        } else {
            Some(TlsConfig::from_ca_pem(CA).unwrap())
        };
        let result = Session::open(
            &[plan],
            SessionOptions {
                tls,
                ..Default::default()
            },
        )
        .await;
        assert!(matches!(result,Err(error) if error.code==RuntimeCode::InvalidConfiguration));
    }
}

#[tokio::test]
async fn declared_authentication_requires_runtime_material_before_connecting() {
    for protocol in ["mqtt", "mqtts", "wss"] {
        for kind in ["userPassword", "X509"] {
            if (protocol == "mqtt" && kind == "X509")
                || (protocol == "wss" && kind == "userPassword")
            {
                continue;
            }
            let v = serde_json::json!({"asyncapi":"3.1.0","info":{"title":"auth","version":"1"},
                "servers":{"s":{"host":"127.0.0.1:1","protocol":protocol,"protocolVersion":if protocol=="wss" {"13"} else {"3.1.1"},"security":[{"type":kind}]}},
                "channels":{"c":{"address":if protocol=="wss" {"/events"} else {"events"},"messages":{"m":{"contentType":"application/octet-stream"}}}},
                "operations":{"emit":{"action":"send","channel":{"$ref":"#/channels/c"}}}});
            let mut o = PlanOptions::application();
            if protocol != "wss" {
                o.client_id = Some("auth-fixture".into());
            }
            let p = Document::parse(&v.to_string())
                .unwrap()
                .operation_id("emit")
                .unwrap()
                .compile()
                .unwrap()
                .prepare(&o)
                .unwrap();
            let tls = if protocol == "mqtt" {
                None
            } else {
                Some(TlsConfig::from_ca_pem(CA).unwrap())
            };
            let result = Session::open(
                &[p],
                SessionOptions {
                    tls,
                    ..Default::default()
                },
            )
            .await;
            assert!(matches!(result,Err(e) if e.code==RuntimeCode::InvalidConfiguration));
        }
    }
}

#[test]
fn query_credential_deserialization_refuses_duplicates_and_redacts_invalid_values() {
    use dynamic_asyncapi_native::QueryCredentials;
    for source in [
        r#""secret-sentinel""#,
        r#"{"token":"secret-sentinel","token":"other"}"#,
        r#"{"token":["secret-sentinel"]}"#,
    ] {
        let error = serde_json::from_str::<QueryCredentials>(source).unwrap_err();
        assert!(!format!("{error:?}").contains("secret-sentinel"));
    }
    let credentials: QueryCredentials =
        serde_json::from_str(r#"{"token":"secret-sentinel"}"#).unwrap();
    assert!(!format!("{credentials:?}").contains("secret-sentinel"));
}
