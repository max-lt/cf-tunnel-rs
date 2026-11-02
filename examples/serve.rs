//! Register one connection with the edge, then relay HTTP requests to ORIGIN.
//! Usage: set -a; . ./.env; set +a; EDGE_ADDR=<ip>:7844 cargo run --example serve

use anyhow::{Context, Result};
use cf_tunnel_rs::{ClientConfig, IncomingStream, StreamKind, TunnelClient, header_key};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let token = std::env::var("TUNNEL_TOKEN")
        .context("TUNNEL_TOKEN is not set")?
        .parse()?;
    let edge = std::env::var("EDGE_ADDR")
        .context("EDGE_ADDR is not set")?
        .parse()?;
    let origin = std::env::var("ORIGIN").unwrap_or_else(|_| "localhost:3000".into());

    let client = TunnelClient::connect(ClientConfig::new(token, edge)).await?;
    let details = client.details();
    eprintln!(
        "registered: location={} remotely_managed={} uuid={}",
        details.location, details.remotely_managed, details.uuid
    );
    eprintln!("serving origin {origin}; Ctrl-C to stop");

    loop {
        tokio::select! {
            stream = client.accept() => {
                let origin = origin.clone();
                tokio::spawn(async move {
                    if let Err(e) = proxy(stream?, &origin).await {
                        eprintln!("stream: {e:#}");
                    }
                    anyhow::Ok(())
                });
            }
            _ = tokio::signal::ctrl_c() => break,
        }
    }
    client.shutdown().await;
    Ok(())
}

async fn proxy(stream: IncomingStream, origin: &str) -> Result<()> {
    let (request, mut body, responder) = stream.split();
    if request.kind != StreamKind::Http {
        responder.reject("only http is supported", &[]).await?;
        return Ok(());
    }
    let line = format!(
        "{} {}",
        request.http_method().unwrap_or("GET"),
        request.http_path()
    );
    let mut upstream = match tokio::net::TcpStream::connect(origin).await {
        Ok(s) => s,
        Err(e) => {
            let status = [("HttpStatus".to_owned(), "502".to_owned())];
            responder.reject(&e.to_string(), &status).await?;
            anyhow::bail!("{line}: origin {origin}: {e}");
        }
    };

    // HTTP/1.0 keeps the origin response free of chunked encoding.
    let mut head = format!(
        "{line} HTTP/1.0\r\nHost: {}\r\n",
        request.http_host().unwrap_or("")
    );
    let mut body_len = 0u64;
    for (name, val) in request.http_headers() {
        match name.to_ascii_lowercase().as_str() {
            "host" | "connection" | "upgrade" | "transfer-encoding" => continue,
            "content-length" => body_len = val.parse().unwrap_or(0),
            _ => {}
        }
        head.push_str(&format!("{name}: {val}\r\n"));
    }
    head.push_str("\r\n");
    upstream.write_all(head.as_bytes()).await?;
    if body_len > 0 {
        tokio::io::copy(&mut (&mut body).take(body_len), &mut upstream).await?;
    }

    // Read the response head before accepting, so the status is known.
    let mut buf = Vec::new();
    let (status, meta, body_start) = loop {
        let mut chunk = [0u8; 8192];
        let n = upstream.read(&mut chunk).await?;
        if n == 0 {
            anyhow::bail!("{line}: origin closed before sending headers");
        }
        buf.extend_from_slice(&chunk[..n]);
        let mut parsed = [httparse::EMPTY_HEADER; 64];
        let mut response = httparse::Response::new(&mut parsed);
        if let httparse::Status::Complete(end) = response.parse(&buf)? {
            let status = response.code.unwrap_or(502);
            let mut meta = vec![("HttpStatus".to_owned(), status.to_string())];
            for h in response.headers.iter() {
                if matches!(
                    h.name.to_ascii_lowercase().as_str(),
                    "connection" | "keep-alive" | "transfer-encoding"
                ) {
                    continue;
                }
                meta.push((
                    header_key(h.name),
                    String::from_utf8_lossy(h.value).into_owned(),
                ));
            }
            break (status, meta, end);
        }
    };

    let mut to_edge = responder.accept(&meta).await?;
    to_edge.write_all(&buf[body_start..]).await?;
    tokio::io::copy(&mut upstream, &mut to_edge).await?;
    to_edge.shutdown().await?;
    eprintln!("{line} -> {status}");
    Ok(())
}
