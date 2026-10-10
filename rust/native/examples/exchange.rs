//! Development consumer: every operation/route/setting comes from the input document.
use dynamic_asyncapi_client::{Document, PlanOptions};
use dynamic_asyncapi_native::{
    Credentials, Delivery, Incoming, Session, SessionOptions, TlsConfig,
};
use serde_json::json;
use std::{error::Error, time::Duration};
use tokio::time::timeout;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<_> = std::env::args().collect();
    if args.len() != 6 {
        return Err("usage: exchange DOCUMENT SEND_ID RECEIVE_ID COUNT BYTES".into());
    }
    let count: usize = args[4].parse()?;
    let size: usize = args[5].parse()?;
    if count == 0 || count > 10000 || !(4..=1024 * 1024).contains(&size) {
        return Err("workload outside fixture bounds".into());
    }
    let source = std::fs::read_to_string(&args[1])?;
    let document = Document::parse_at(&source, "https://fixture.test/api")?;
    let send = document
        .operation_id(&args[2])?
        .compile()?
        .prepare(&PlanOptions::application())?;
    let receive = document
        .operation_id(&args[3])?
        .compile()?
        .prepare(&PlanOptions::application())?;
    drop(document);
    let credentials = match (
        std::env::var("ASYNCAPI_FIXTURE_USERNAME"),
        std::env::var("ASYNCAPI_FIXTURE_PASSWORD"),
    ) {
        (Ok(username), Ok(password)) => Some(Credentials::new(username, password)),
        _ => None,
    };
    let tls = match std::env::var("ASYNCAPI_FIXTURE_CA") {
        Ok(value) => {
            let config = if value == "system" {
                TlsConfig::system_roots()?
            } else {
                TlsConfig::from_ca_pem(&std::fs::read(value)?)?
            };
            Some(
                match (
                    std::env::var("ASYNCAPI_FIXTURE_CERT"),
                    std::env::var("ASYNCAPI_FIXTURE_KEY"),
                ) {
                    (Ok(cert), Ok(key)) => {
                        config.with_client_identity(&std::fs::read(cert)?, &std::fs::read(key)?)?
                    }
                    (Err(_), Err(_)) => config,
                    _ => {
                        return Err(
                            "fixture client identity requires both certificate and key".into()
                        );
                    }
                },
            )
        }
        Err(_) => None,
    };
    let connect_timeout = std::env::var("ASYNCAPI_FIXTURE_CONNECT_TIMEOUT_MS")
        .ok()
        .map(|value| value.parse::<u64>())
        .transpose()?
        .map(Duration::from_millis)
        .unwrap_or(Duration::from_secs(5));
    let mut session = Session::open(
        &[send, receive],
        SessionOptions {
            credentials,
            tls,
            connect_timeout,
            ..Default::default()
        },
    )
    .await?;
    let subscriptions = session.mqtt_subscriptions().to_vec();
    let mut received_qos = Vec::new();
    let mut receipts = Vec::new();
    let mut received = 0;
    let mut rejected = 0;
    for sequence in 0..count {
        let mut bytes = vec![0; size];
        bytes[..4].copy_from_slice(&(sequence as u32).to_be_bytes());
        for (offset, byte) in bytes.iter_mut().enumerate().skip(4) {
            *byte = ((sequence + offset) % 251) as u8;
        }
        let receipt = session.send(0, bytes.clone()).await?;
        receipts.push(receipt);
        loop {
            match timeout(Duration::from_secs(5), session.next())
                .await??
                .ok_or("session ended before expected message")?
            {
                Incoming::Message(message) => {
                    if message.operation != 1 || message.payload.as_ref() != bytes {
                        return Err("received operation or bytes differ".into());
                    }
                    if let Delivery::Mqtt { qos, .. } = message.delivery {
                        received_qos.push(qos);
                    }
                    received += 1;
                    break;
                }
                Incoming::InvalidPayload { .. } => return Err("invalid fixture payload".into()),
                Incoming::Rejected {
                    reason,
                    payload_bytes,
                } => {
                    if reason != "text WebSocket frame does not match the binary codec"
                        || payload_bytes != 29
                    {
                        return Err("unexpected rejected event".into());
                    }
                    rejected += 1;
                }
            }
        }
    }
    let sender = session.sender();
    let close = session.close().await?;
    let after_close = sender.send(0, vec![0; 4]).await.unwrap_err();
    println!(
        "{}",
        json!({"kind":"dynamic-client-development-execution","received":received,"rejected":rejected,"receipts":receipts,"subscriptions":subscriptions,"receivedQos":received_qos,"close":close,"afterClose":after_close.code})
    );
    Ok(())
}
