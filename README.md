# cf-tunnel-rs

A Rust client for Cloudflare Tunnel. It talks to the Cloudflare edge over QUIC.
It is a library first. It contains no Go code.

```rust
let config = TunnelConfig::new(token);
let tunnel = Tunnel::start(config).await?;

loop {
    let (request, body, responder) = tunnel.accept().await?.split();
    // Read `body`, then answer with `responder.respond_http(...)`.
}
```

`Tunnel` discovers the edge, keeps 4 connections open, and reconnects each one
with a backoff. `TunnelClient` is one connection, for callers that want to
manage their own set.

`examples/serve.rs` relays HTTP to a local port. See [PROTOCOL.md](PROTOCOL.md)
for the wire protocol.

Status: early. WebSocket, TCP and UDP streams are not handled yet, and the
edge configuration pushed over RPC is ignored.

Building needs only a Rust toolchain. The Cap'n Proto wire format is read and
written by `src/wire.rs`, with no schema compiler and no C++.

`cargo test` also compares that code with the reference `capnp` tool when it is
on the PATH (for example `brew install capnp`), using the schemas in
`tests/schema`. Without the tool, those tests skip.

License: MIT OR Apache-2.0.

The files in `tests/schema/` and `roots/` come from `cloudflare/cloudflared` and
`capnproto/capnproto` (Apache-2.0 and MIT).
