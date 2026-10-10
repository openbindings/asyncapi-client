//! External consumer: client calls stay inside Rust; only this outer API crosses JS.
use dynamic_asyncapi_client::Document;

#[cfg_attr(feature = "wasm", wasm_bindgen::prelude::wasm_bindgen)]
pub fn inspect_from_outer_api(source: &str) -> Result<String, String> {
    let document = Document::parse(source).map_err(|error| error.to_string())?;
    let operations = document.operations().collect::<Vec<_>>();
    drop(document);
    let mut facts = Vec::new();
    for operation in operations {
        facts.push(
            operation
                .map_err(|e| e.to_string())?
                .describe()
                .map_err(|e| e.to_string())?,
        );
    }
    serde_json::to_string(&facts).map_err(|error| error.to_string())
}

/// A downstream Rust adapter can compile and prepare directly. Only its outer
/// result crosses Wasm; it never routes through the standalone TypeScript API.
#[cfg_attr(feature = "wasm", wasm_bindgen::prelude::wasm_bindgen)]
pub fn prepare_from_outer_api(source: &str, operation: &str) -> Result<String, String> {
    let document = Document::parse(source).map_err(|e| e.to_string())?;
    let compiled = document
        .operation_id(operation)
        .and_then(|o| o.compile())
        .map_err(|e| e.to_string())?;
    drop(document);
    let plan = compiled
        .prepare(&dynamic_asyncapi_client::PlanOptions::application())
        .map_err(|e| e.to_string())?;
    drop(compiled);
    let payload = [0, 255, 32];
    if plan.prepare_bytes(&payload).unwrap() != payload {
        return Err("binary codec altered the payload".into());
    }
    serde_json::to_string(plan.describe()).map_err(|e| e.to_string())
}

#[cfg(test)]
mod tests {
    #[test]
    fn external_consumer_prepares_without_the_standalone_typescript_facade() {
        let source = r##"{"asyncapi":"3.1.0","info":{"title":"consumer","version":"1"},"servers":{"s":{"host":"example.test","protocol":"wss"}},"channels":{"events":{"address":"/events","messages":{"event":{"contentType":"application/octet-stream"}}}},"operations":{"emit":{"action":"send","channel":{"$ref":"#/channels/events"}}}}"##;
        let result = super::prepare_from_outer_api(source, "emit").unwrap();
        let plan: serde_json::Value = serde_json::from_str(&result).unwrap();
        assert_eq!(plan["transport"]["endpoint"], "wss://example.test/events");
        assert_eq!(plan["wireAction"], "send");
    }
    #[test]
    fn external_consumer_retains_operations_and_calls_the_client_directly() {
        let result = super::inspect_from_outer_api(r#"{"asyncapi":"2.6.0","info":{"title":"consumer","version":"1"},"channels":{"events":{"subscribe":{"operationId":"emit"}}}}"#).unwrap();
        let facts: serde_json::Value = serde_json::from_str(&result).unwrap();
        assert_eq!(facts[0]["action"], "send");
        assert_eq!(facts[0]["address"], "events");
    }
}

/// Full transport execution remains inside this consumer's Rust/Wasm module.
#[cfg(feature = "wasm")]
#[wasm_bindgen::prelude::wasm_bindgen]
pub async fn exchange_from_outer_api(source: &str, payload: Vec<u8>) -> Result<Vec<u8>, String> {
    use dynamic_asyncapi_client::PlanOptions;
    use dynamic_asyncapi_host::{Incoming, Session, SessionOptions};
    let document = Document::parse(source).map_err(|e| e.to_string())?;
    let plans = ["emit", "listen"]
        .into_iter()
        .map(|id| {
            document
                .operation_id(id)?
                .compile()?
                .prepare(&PlanOptions::application())
        })
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| e.to_string())?;
    drop(document);
    let session = Session::open(&plans, SessionOptions::default(), None)
        .await
        .map_err(|e| e.to_string())?;
    drop(plans);
    session
        .sender()
        .send(0, &payload)
        .map_err(|e| e.to_string())?;
    let received = loop {
        match session.next(None).await.map_err(|e| e.to_string())? {
            Some(Incoming::Message {
                operation: 1,
                payload,
            }) => break payload.into_bytes().into(),
            Some(Incoming::Rejected { .. }) => {}
            _ => return Err("outer Rust consumer did not receive its operation".into()),
        }
    };
    session.close(None).await.map_err(|e| e.to_string())?;
    Ok(received)
}

/// The outer module calls both typed and exact client APIs directly in Rust.
#[cfg_attr(feature = "wasm", wasm_bindgen::prelude::wasm_bindgen)]
pub fn values_from_outer_api(source: &str) -> Result<String, String> {
    #[derive(serde::Serialize, serde::Deserialize)]
    struct Event {
        id: u128,
        label: String,
    }
    let exact = dynamic_asyncapi_client::Json::parse(source, Default::default())
        .map_err(|e| e.to_string())?;
    let typed: Event = exact.deserialize().map_err(|e| e.to_string())?;
    drop(exact);
    dynamic_asyncapi_client::Json::from_serializable(&typed, Default::default())
        .map(|value| value.to_json())
        .map_err(|e| e.to_string())
}

#[test]
fn external_typed_values_do_not_cross_a_javascript_number() {
    let source = r#"{"id":900719925474099312345,"label":"event"}"#;
    assert_eq!(values_from_outer_api(source).unwrap(), source);
}

/// JSON transport and decoding stay inside the downstream Rust/Wasm module.
#[cfg(feature = "wasm")]
#[wasm_bindgen::prelude::wasm_bindgen]
pub async fn exchange_codec_from_outer_api(
    source: &str,
    binary_frame: bool,
) -> Result<String, String> {
    use dynamic_asyncapi_client::{Json, PlanOptions, WebSocketFrame};
    use dynamic_asyncapi_host::{Incoming, Session, SessionOptions};
    let document = Document::parse(source).map_err(|e| e.to_string())?;
    let mut options = PlanOptions::application();
    if binary_frame {
        options.websocket_frame = Some(WebSocketFrame::Binary);
    }
    let plans = ["emit", "listen"]
        .map(|id| document.operation_id(id)?.compile()?.prepare(&options))
        .into_iter()
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| e.to_string())?;
    let session = Session::open(&plans, SessionOptions::default(), None)
        .await
        .map_err(|e| e.to_string())?;
    drop(plans);
    drop(document);
    #[derive(serde::Serialize)]
    struct Event {
        id: u128,
    }
    let value = Json::from_serializable(
        &Event {
            id: 900719925474099312345,
        },
        Default::default(),
    )
    .map_err(|e| e.to_string())?;
    session
        .sender()
        .send_json(0, &value)
        .map_err(|e| e.to_string())?;
    drop(value);
    let (mut rejected, mut malformed) = (0, 0);
    let value = loop {
        match session.next(None).await.map_err(|e| e.to_string())? {
            Some(Incoming::Rejected { .. }) => rejected += 1,
            Some(Incoming::InvalidPayload { .. }) => malformed += 1,
            Some(Incoming::Message {
                operation: 1,
                payload,
            }) => {
                break payload
                    .as_json()
                    .and_then(|j| j.get("id"))
                    .ok_or("no exact JSON id")?;
            }
            _ => return Err("wrong receive outcome".into()),
        }
    };
    session.close(None).await.map_err(|e| e.to_string())?;
    Ok(
        serde_json::json!({"id":value.number_text(),"rejected":rejected,"malformed":malformed})
            .to_string(),
    )
}
