# cf-tunnel-rs

A Rust client for Cloudflare Tunnel. It talks to the Cloudflare edge over QUIC.
It is a library first. It contains no Go code.

Status: early. The library registers one connection and accepts request streams.
`examples/serve.rs` relays HTTP to a local port.
See [PROTOCOL.md](PROTOCOL.md).

License: MIT OR Apache-2.0.

The files in `schema/` and `roots/` come from `cloudflare/cloudflared` (Apache-2.0).
