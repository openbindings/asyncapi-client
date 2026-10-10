//! Consumer job for codec fixtures; peer expectations are maintained separately.
use dynamic_asyncapi_client::{Codec, Document, Json, PlanOptions, WebSocketFrame};
use dynamic_asyncapi_native::{Credentials, Incoming, RuntimeCode, Session, SessionOptions};
use std::{error::Error, time::Duration};
#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<_> = std::env::args().collect();
    let document = Document::parse(&std::fs::read_to_string(
        args.get(1).ok_or("document required")?,
    )?)?;
    let mut options = PlanOptions::application();
    if args.get(2).is_some_and(|s| s == "binary") {
        options.websocket_frame = Some(WebSocketFrame::Binary);
    }
    let plans = ["emit", "listen"]
        .map(|id| document.operation_id(id)?.compile()?.prepare(&options))
        .into_iter()
        .collect::<Result<Vec<_>, _>>()?;
    let codec = plans[0].describe().codec;
    let credentials = std::env::var("ASYNCAPI_FIXTURE_USERNAME")
        .ok()
        .zip(std::env::var("ASYNCAPI_FIXTURE_PASSWORD").ok())
        .map(|(u, p)| Credentials::new(u, p));
    let mut session = Session::open(
        &plans,
        SessionOptions {
            credentials,
            ..Default::default()
        },
    )
    .await?;
    drop(plans);
    drop(document);
    let sender = session.sender();
    let error = sender.send(0, vec![255]).await.unwrap_err();
    if error.code != RuntimeCode::InvalidPayload || error.delivery_unknown {
        return Err("invalid send was not rejected before delivery".into());
    }
    let mut notices = 0;
    for sequence in 0..4 {
        let text = if codec == Codec::Json {
            format!(r#"{{"id":900719925474099312345,"sequence":{sequence},"text":"snow☃"}}"#)
        } else {
            format!("sequence={sequence}; snow☃")
        };
        if sequence % 2 == 1 {
            sender.send(0, text.clone()).await?;
        } else if codec == Codec::Json {
            #[derive(serde::Serialize)]
            struct Event {
                id: u128,
                sequence: usize,
                text: &'static str,
            }
            let value = if sequence == 0 {
                Json::from_serializable(
                    &Event {
                        id: 900719925474099312345,
                        sequence,
                        text: "snow☃",
                    },
                    Default::default(),
                )?
            } else {
                Json::parse(&text, Default::default())?
            };
            sender.send_json(0, &value).await?;
        } else {
            sender.send_text(0, text.clone()).await?;
        }
        loop {
            match tokio::time::timeout(Duration::from_secs(3), session.next())
                .await??
                .ok_or("closed early")?
            {
                Incoming::Message(message) => {
                    if message.payload.as_bytes() == b"{\"kind\":\"unsolicited-notice\"}" {
                        notices += 1;
                        continue;
                    }
                    if message.operation != 1 || message.payload.as_bytes() != text.as_bytes() {
                        return Err("wrong received bytes or operation".into());
                    }
                    if codec == Codec::Json
                        && message
                            .payload
                            .as_json()
                            .and_then(|v| v.get("id"))
                            .and_then(|v| v.number_text().map(str::to_owned))
                            .as_deref()
                            != Some("900719925474099312345")
                    {
                        return Err("lost exact number".into());
                    }
                    if codec == Codec::Utf8 && message.payload.as_text() != Some(text.as_str()) {
                        return Err("wrong text".into());
                    }
                    break;
                }
                Incoming::Rejected {
                    payload_bytes: 29, ..
                } => {
                    notices += 1;
                }
                _ => return Err("unexpected receive outcome".into()),
            }
        }
    }
    session.close().await?;
    if notices > 1 {
        return Err("unexpected repeated notice".into());
    }
    println!(
        "{}",
        serde_json::json!({"received":4,"codec":codec,"invalidSend":"InvalidPayload","notices":notices})
    );
    Ok(())
}
