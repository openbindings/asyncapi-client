//! Direct protocol libraries only: a baseline/control, not the AsyncAPI client.
use futures_util::{SinkExt, StreamExt};
use rumqttc::{AsyncClient, Event, Incoming, MqttOptions, Outgoing, QoS};
use std::{
    error::Error,
    time::{Duration, Instant},
};
use tokio::time::timeout;
use tokio_tungstenite::tungstenite::Message;

type Failure = Box<dyn Error>;
const WARMUP: usize = 20;

fn payload(sequence: usize, size: usize) -> Vec<u8> {
    let mut bytes = vec![0; size];
    bytes[..4].copy_from_slice(&(sequence as u32).to_be_bytes());
    for (offset, byte) in bytes.iter_mut().enumerate().skip(4) {
        *byte = ((sequence + offset) % 251) as u8;
    }
    bytes
}
fn report(protocol: &str, mut samples: Vec<u128>, connect_micros: u128, size: usize) {
    samples.sort_unstable();
    let percentile = |p: usize| samples[(samples.len() - 1) * p / 100];
    println!(
        "{}",
        serde_json::json!({"kind":"direct-driver-control","protocol":protocol,"samples":samples.len(),
        "warmup":WARMUP,"payload_bytes":size,"connect_micros":connect_micros,
        "p50_micros":percentile(50),"p95_micros":percentile(95),"p99_micros":percentile(99),
        "receipt":if protocol=="mqtt" {"broker PubAck"} else {"peer binary echo"},"sorted_micros":samples})
    );
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Failure> {
    let args: Vec<_> = std::env::args().collect();
    if args.len() < 5 {
        return Err("usage: controls mqtt|websocket PORT COUNT BYTES [MQTT_TOPIC]".into());
    }
    let protocol = &args[1];
    let port: u16 = args[2].parse()?;
    let count: usize = args[3].parse()?;
    let size: usize = args[4].parse()?;
    if count == 0 || count > 10000 || !(4..=1024 * 1024).contains(&size) {
        return Err("control workload exceeds bounds".into());
    }
    let result = timeout(Duration::from_secs(60), async {
        match protocol.as_str() {
            "mqtt" => {
                mqtt(
                    port,
                    count,
                    size,
                    args.get(5).map_or("fixture/events", String::as_str),
                )
                .await
            }
            "websocket" => websocket(port, count, size).await,
            _ => Err("unknown control protocol".into()),
        }
    })
    .await?;
    result
}

async fn mqtt(port: u16, count: usize, size: usize, topic: &str) -> Result<(), Failure> {
    let mut options = MqttOptions::new(
        format!("raw-control-{}", std::process::id()),
        "127.0.0.1",
        port,
    );
    options.set_keep_alive(Duration::from_secs(10));
    options.set_clean_session(true);
    // Explicit control profile: 1 MiB payload plus bounded protocol metadata.
    options.set_max_packet_size(1024 * 1024 + 4096, 1024 * 1024 + 4096);
    options.set_credentials("fixture-user", "local-disposable-password");
    let (client, mut events) = AsyncClient::new(options, 4);
    let start = Instant::now();
    loop {
        if let Event::Incoming(Incoming::ConnAck(ack)) = events.poll().await? {
            if ack.code != rumqttc::ConnectReturnCode::Success {
                return Err("broker denied control connection".into());
            }
            break;
        }
    }
    let connect = start.elapsed().as_micros();
    let mut samples = Vec::with_capacity(count);
    for sequence in 0..count + WARMUP {
        let bytes = payload(sequence, size);
        let start = Instant::now();
        client
            .publish(topic, QoS::AtLeastOnce, false, bytes)
            .await?;
        let mut packet_id = None;
        loop {
            match events.poll().await? {
                Event::Outgoing(Outgoing::Publish(id)) => packet_id = Some(id),
                Event::Incoming(Incoming::PubAck(ack)) => {
                    if packet_id != Some(ack.pkid) {
                        return Err("acknowledgement identity mismatch".into());
                    }
                    break;
                }
                _ => {}
            }
        }
        if sequence >= WARMUP {
            samples.push(start.elapsed().as_micros());
        }
    }
    client.disconnect().await?;
    loop {
        if matches!(events.poll().await?, Event::Outgoing(Outgoing::Disconnect)) {
            break;
        }
    }
    report("mqtt", samples, connect, size);
    Ok(())
}

async fn websocket(port: u16, count: usize, size: usize) -> Result<(), Failure> {
    let start = Instant::now();
    let (mut socket, _) =
        tokio_tungstenite::connect_async(format!("ws://127.0.0.1:{port}/events")).await?;
    let connect = start.elapsed().as_micros();
    let mut samples = Vec::with_capacity(count);
    let mut notices = 0;
    for sequence in 0..count + WARMUP {
        let bytes = payload(sequence, size);
        let start = Instant::now();
        socket.send(Message::Binary(bytes.clone().into())).await?;
        loop {
            match socket.next().await.ok_or("WebSocket ended before echo")?? {
                Message::Binary(reply) => {
                    if reply.as_ref() != bytes {
                        return Err("peer echo bytes differ".into());
                    }
                    break;
                }
                Message::Text(text) if text == "{\"kind\":\"unsolicited-notice\"}" => {
                    notices += 1;
                }
                Message::Ping(_) | Message::Pong(_) => {}
                _ => return Err("unexpected WebSocket event".into()),
            }
        }
        if sequence >= WARMUP {
            samples.push(start.elapsed().as_micros());
        }
    }
    if notices != 1 {
        return Err("unsolicited traffic control missing".into());
    }
    socket.close(None).await?;
    report("websocket", samples, connect, size);
    Ok(())
}
