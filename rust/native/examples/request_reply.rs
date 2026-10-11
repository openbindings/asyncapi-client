//! Run against a caller-supplied local or remote WebSocket service:
//! cargo run -p dynamic-asyncapi-native --example request_reply -- document.json operation '{"requestId":"unique"}'
use dynamic_asyncapi_client::{Document, ExchangeOptions, Json, PlanOptions};
use dynamic_asyncapi_native::{SessionOptions, request_reply};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<_> = std::env::args().collect();
    if args.len() != 4 {
        return Err("usage: request_reply DOCUMENT OPERATION JSON_REQUEST".into());
    }
    let document = Document::parse(&std::fs::read_to_string(&args[1])?)?;
    let plan = document
        .operation_id(&args[2])?
        .compile()?
        .prepare_exchange(&ExchangeOptions::first(
            PlanOptions::application(),
            PlanOptions::application(),
            5000,
        ))?;
    let request = Json::parse(&args[3], Default::default())?;
    let response = request_reply(&plan, &request, SessionOptions::default()).await?;
    println!("{}", response.reply.payload.as_json().unwrap().to_json());
    if let Err(error) = response.close {
        eprintln!("Reply received; graceful shutdown failed: {error}");
    }
    Ok(())
}
