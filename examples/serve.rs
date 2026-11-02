//! Keep connections to the edge and relay HTTP requests to ORIGIN.
//! Usage: set -a; . ./.env; set +a; cargo run --example serve
//! Optional: EDGE_ADDRS=<ip>:7844,... skips discovery; CONNECTIONS=<n>; ORIGIN=<host:port>.

use std::{net::SocketAddr, sync::Arc, time::Duration};

use anyhow::{Context, Result, bail};
use cf_tunnel_rs::{ConnectRequest, EdgeSource, IncomingStream, StreamKind, Tunnel, TunnelConfig};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const GRACE: Duration = Duration::from_secs(3);
/// Headers that describe the hop to the origin, not the request itself.
const REQUEST_HOP_HEADERS: [&str; 4] = ["host", "connection", "upgrade", "transfer-encoding"];
const RESPONSE_HOP_HEADERS: [&str; 3] = ["connection", "keep-alive", "transfer-encoding"];

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let token = std::env::var("TUNNEL_TOKEN")
        .context("TUNNEL_TOKEN is not set")?
        .parse()?;
    let origin: Arc<str> = std::env::var("ORIGIN")
        .unwrap_or_else(|_| "localhost:3000".into())
        .into();

    let mut config = TunnelConfig::new(token);

    // Both settings are optional. Without them the edge is discovered.
    if let Ok(list) = std::env::var("EDGE_ADDRS") {
        let addrs = list
            .split(',')
            .map(str::parse)
            .collect::<Result<Vec<SocketAddr>, _>>()?;
        config.edge = EdgeSource::Fixed(addrs);
    }

    if let Ok(count) = std::env::var("CONNECTIONS") {
        config.connections = count.parse()?;
    }

    let tunnel = Tunnel::start(config).await?;
    eprintln!("serving origin {origin}; Ctrl-C to stop");

    // Print the state of the connections whenever it changes.
    let mut status = tunnel.status();
    tokio::spawn(async move {
        while status.changed().await.is_ok() {
            let states: Vec<String> = status.borrow().iter().map(describe).collect();
            eprintln!("connections: {}", states.join(" | "));
        }
    });

    let result = loop {
        tokio::select! {
            stream = tunnel.accept() => match stream {
                // Each request runs in its own task.
                Ok(stream) => {
                    let origin = origin.clone();
                    tokio::spawn(async move {
                        if let Err(e) = proxy(stream, &origin).await {
                            eprintln!("stream: {e:#}");
                        }
                    });
                }
                Err(e) => break Err(e.into()),
            },
            _ = tokio::signal::ctrl_c() => break Ok(()),
        }
    };

    tunnel.shutdown(GRACE).await;

    result
}

fn describe(status: &cf_tunnel_rs::ConnectionStatus) -> String {
    use cf_tunnel_rs::ConnectionStatus::*;

    match status {
        Connecting => "connecting".into(),
        Connected(details) => format!("up {}", details.location),
        Retrying { error, after } => format!("retry in {after:.1?} ({error})"),
        Failed(error) => format!("failed ({error})"),
        Stopped => "stopped".into(),
        _ => "unknown".into(),
    }
}

async fn proxy(stream: IncomingStream, origin: &str) -> Result<()> {
    let (request, mut body, responder) = stream.split();

    // The example relays plain http only.
    if request.kind != StreamKind::Http {
        responder.reject("only http is supported").await?;

        return Ok(());
    }

    let method = request.http_method().unwrap_or("GET");
    let target = request.http_path();

    // The query can carry access tokens, so it stays out of the logs.
    let line = format!("{method} {}", target.split('?').next().unwrap_or("/"));

    // The edge removes chunking in practice. Refuse it rather than drop a body.
    let Some(body_len) = request_body_len(&request) else {
        responder
            .reject("chunked request bodies are not supported")
            .await?;

        bail!("{line}: chunked request body");
    };

    let mut upstream = match tokio::net::TcpStream::connect(origin).await {
        Ok(s) => s,
        Err(e) => {
            responder.reject(&e.to_string()).await?;

            bail!("{line}: origin {origin}: {e}");
        }
    };

    // HTTP/1.0 keeps the origin response free of chunked encoding.
    let mut head = format!(
        "{method} {target} HTTP/1.0\r\nHost: {}\r\n",
        request.http_host().unwrap_or("")
    );

    // Forward the headers that belong to the request.
    for (name, val) in request.http_headers() {
        if !REQUEST_HOP_HEADERS.contains(&name.to_ascii_lowercase().as_str()) {
            head.push_str(&format!("{name}: {val}\r\n"));
        }
    }

    head.push_str("\r\n");
    upstream.write_all(head.as_bytes()).await?;

    // The origin may wait for the body before it answers, so send it first.
    tokio::io::copy(&mut (&mut body).take(body_len), &mut upstream).await?;

    let (status, headers, leftover) = read_response_head(&mut upstream)
        .await
        .with_context(|| line.clone())?;
    let headers: Vec<(&str, &str)> = headers
        .iter()
        .map(|(n, v)| (n.as_str(), v.as_str()))
        .collect();

    // Answer the edge, then stream the rest of the origin body.
    let mut to_edge = responder.respond_http(status, headers).await?;
    to_edge.write_all(&leftover).await?;
    tokio::io::copy(&mut upstream, &mut to_edge).await?;
    to_edge.finish()?;

    eprintln!("{line} -> {status}");

    Ok(())
}

/// Length of the request body, or `None` when it is chunked.
fn request_body_len(request: &ConnectRequest) -> Option<u64> {
    let mut len = 0;

    for (name, val) in request.http_headers() {
        if name.eq_ignore_ascii_case("transfer-encoding")
            && val.to_ascii_lowercase().contains("chunked")
        {
            return None;
        }

        if name.eq_ignore_ascii_case("content-length") {
            len = val.parse().unwrap_or(0);
        }
    }

    Some(len)
}

/// Reads until the response head is complete. Returns the status, the
/// forwardable headers and the body bytes already read.
async fn read_response_head(
    upstream: &mut tokio::net::TcpStream,
) -> Result<(u16, Vec<(String, String)>, Vec<u8>)> {
    let mut buf = Vec::new();

    loop {
        let mut chunk = [0u8; 8192];
        let n = upstream.read(&mut chunk).await?;

        if n == 0 {
            bail!("origin closed before sending headers");
        }

        buf.extend_from_slice(&chunk[..n]);

        // Keep reading until the parser has seen the whole head.
        let mut parsed = [httparse::EMPTY_HEADER; 64];
        let mut response = httparse::Response::new(&mut parsed);
        let httparse::Status::Complete(end) = response.parse(&buf)? else {
            continue;
        };

        let headers = response
            .headers
            .iter()
            .filter(|h| !RESPONSE_HOP_HEADERS.contains(&h.name.to_ascii_lowercase().as_str()))
            .map(|h| {
                (
                    h.name.to_owned(),
                    String::from_utf8_lossy(h.value).into_owned(),
                )
            })
            .collect();

        return Ok((response.code.unwrap_or(502), headers, buf.split_off(end)));
    }
}
