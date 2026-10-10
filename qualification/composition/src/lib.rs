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

#[cfg(test)]
mod tests {
    #[test]
    fn external_consumer_retains_operations_and_calls_the_client_directly() {
        let result = super::inspect_from_outer_api(r#"{"asyncapi":"2.6.0","info":{"title":"consumer","version":"1"},"channels":{"events":{"subscribe":{"operationId":"emit"}}}}"#).unwrap();
        let facts: serde_json::Value = serde_json::from_str(&result).unwrap();
        assert_eq!(facts[0]["action"], "send");
        assert_eq!(facts[0]["address"], "events");
    }
}
