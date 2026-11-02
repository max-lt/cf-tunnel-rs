# cf-tunnel-rs

A Rust client for Cloudflare Tunnel. It connects to the Cloudflare edge over QUIC.
It is a library first.

This project is not affiliated with Cloudflare. Cloudflare can change the edge
protocol without notice. [PROTOCOL.md](PROTOCOL.md) describes what this crate uses.

## Quick start

Create a tunnel in the Cloudflare dashboard. Copy its token. Add a public
hostname to the tunnel. Then serve an axum app through it:

```rust
use axum::{Router, routing::get};
use cf_tunnel_rs::{Tunnel, TunnelConfig};

let token = std::env::var("TUNNEL_TOKEN")?.parse()?;
let app = Router::new().route("/", get(|| async { "Hello" }));

Tunnel::start(TunnelConfig::new(token)).await?.serve(app).await?;
```

This needs the `http` feature. `serve` accepts a `tower` service.

Run the complete example:

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

- WebSocket, TCP and UDP streams are refused. The edge sends them, but nothing
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

## Build and test

You need Rust 1.85 or later and a C compiler. The TLS library `ring` compiles
some C code.

The Cap'n Proto wire format is read and written by `src/wire.rs`. `cargo test`
compares that code with the reference `capnp` tool when the tool is on the PATH,
for example after `brew install capnp`. The tests use the schemas in
`tests/schema`. Those tests skip when the tool is missing.

## License

MIT OR Apache-2.0.

The files in `tests/schema/` and `roots/` come from `cloudflare/cloudflared`
(Apache-2.0) and `capnproto/capnproto` (MIT).
