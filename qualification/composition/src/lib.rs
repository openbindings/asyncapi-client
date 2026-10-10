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
    if plan.prepare_bytes(&payload) != payload {
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
