//! Ordinary source explorer: no network, protocol driver, or OpenBindings SDK.
use dynamic_asyncapi_client::Document;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let path = args
        .next()
        .ok_or("usage: inspect SOURCE_FILE [SOURCE_URI]")?;
    let source = std::fs::read_to_string(path)?;
    let document = match args.next() {
        Some(uri) => Document::parse_at(&source, &uri)?,
        None => Document::parse(&source)?,
    };
    let mut entries = Vec::new();
    let mut errors = 0;
    for entry in document.operations() {
        let operation = match entry {
            Ok(operation) => operation,
            Err(error) => {
                errors += 1;
                entries.push(serde_json::json!({"inventoryError":error}));
                continue;
            }
        };
        match operation.describe() {
            Ok(description) => entries.push(
                serde_json::json!({"identity":operation.identity(),"description":description}),
            ),
            Err(error) => {
                errors += 1;
                entries.push(
                    serde_json::json!({"identity":operation.identity(),"descriptionError":error}),
                );
            }
        }
    }
    println!(
        "{}",
        serde_json::json!({"version":document.version(),"operations":entries,"errors":errors,
        "claim":"operation inspection only; no payload validation or execution"})
    );
    if errors > 0 {
        return Err("inspection contains unresolved or invalid declarations".into());
    }
    Ok(())
}
