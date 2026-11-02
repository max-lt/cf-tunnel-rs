# cf-tunnel-rs

A Rust client for Cloudflare Tunnel. It connects to the Cloudflare edge over QUIC.
It is a library first.

This project is not affiliated with Cloudflare. Cloudflare can change the edge
protocol without notice. [PROTOCOL.md](PROTOCOL.md) describes what this crate uses.

## Quick start

Create a tunnel in the Cloudflare dashboard. Copy its token. Add a public
hostname to the tunnel. Then serve an axum app through it (`examples/hello.rs`):

```rust
use std::time::Duration;

use axum::{Router, routing::get};
use cf_tunnel_rs::{Tunnel, TunnelConfig};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let token = std::env::var("TUNNEL_TOKEN")?.parse()?;
    let hello = || async { "Hello from cf-tunnel-rs\n" };
    let app = Router::new().route("/", get(hello));

    let tunnel = Tunnel::start(TunnelConfig::new(token)).await?;

    // Serve until Ctrl-C, then unregister.
    tokio::select! {
        result = tunnel.serve(app) => result?,
        _ = tokio::signal::ctrl_c() => {}
    }

    tunnel.shutdown(Duration::from_secs(3)).await;

    Ok(())
}
```

This needs the `http` feature. `serve` accepts a `tower` service. Enable the
`websocket` feature to upgrade WebSocket requests too, for example with an axum
`WebSocketUpgrade` route.

```
TUNNEL_TOKEN=<token> cargo run --features http --example hello
```

## Request streams

`Tunnel::accept()` returns one request stream at a time:

```rust
let tunnel = Tunnel::start(TunnelConfig::new(token)).await?;

loop {
    let (request, body, responder) = tunnel.accept().await?.split();
    // Read `body`, then answer with `responder.respond_http(...)`.
}
```

The edge sends the method, the URL and the headers as metadata entries.
`ConnectRequest` gives access to them.

## What it does

- `Tunnel::start` resolves the edge from its SRV record. It falls back to
  DNS-over-TLS when the system resolver fails.
- It opens 4 connections. Set `TunnelConfig::connections` to change that.
- It reconnects each connection with a backoff. The backoff starts at 1 s and
  doubles up to 60 s.
- It retries on another edge address when the edge refuses a duplicate connection.
- `Tunnel::status` shows the state of each connection.
- `Tunnel::shutdown` unregisters every connection and waits for requests in flight.

`Tunnel::start` returns when one connection is registered. It retries in the
background after a network failure, so wrap it in a timeout if you cannot wait.

`TunnelClient` is one connection, for callers that manage their own set.

## Limits

- TCP and UDP streams are refused. The edge sends them, but nothing
  handles them yet.
- The edge sends its ingress configuration over RPC. The crate ignores it.
  Your code chooses what answers each request.
- Edge discovery runs once, at start.
- Response trailers are not supported.
- The crate is tested against the real edge with axum and with a plain relay, on
  macOS. The streaming speed is not measured.

## Examples

| Example | What it does |
| --- | --- |
| `hello` | Serves an axum app. Needs `--features http`. |
| `relay` | Forwards HTTP requests to a local port. Set `ORIGIN=host:port`. |
| `chat` | Serves a WebSocket chat room. Needs `--features websocket`. |

## Build and test

You need Rust 1.85 or later.

`cargo test` also compares the wire codec in `src/wire.rs` with the reference
`capnp` tool, when the tool is on the PATH. The schemas are in `tests/schema`.
Those tests skip when the tool is missing.

## License

MIT OR Apache-2.0.

The files in `tests/schema/` and `roots/` come from `cloudflare/cloudflared`
(Apache-2.0) and `capnproto/capnproto` (MIT).
