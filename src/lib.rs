//! Client for Cloudflare Tunnel. It connects to the Cloudflare edge over QUIC
//! and hands each incoming request stream to the caller.
//!
//! ```no_run
//! # async fn demo() -> Result<(), cf_tunnel_rs::Error> {
//! use cf_tunnel_rs::{ClientConfig, Token, TunnelClient};
//!
//! let token: Token = std::env::var("TUNNEL_TOKEN").unwrap().parse()?;
//! let edge = "198.41.192.167:7844".parse().unwrap();
//! let client = TunnelClient::connect(ClientConfig::new(token, edge)).await?;
//! loop {
//!     let (request, _body, responder) = client.accept().await?.split();
//!     let text = format!("{} {}\n", request.http_method().unwrap_or("-"), request.dest);
//!     let mut out = responder.respond_http(200, [("Content-Type", "text/plain")]).await?;
//!     out.write_all(text.as_bytes()).await.ok();
//!     out.finish().ok();
//! }
//! # }
//! ```

mod client;
mod edge;
mod error;
mod stream;
mod token;
mod tunnel;

#[allow(clippy::all, dead_code)]
mod tunnelrpc_capnp {
    include!(concat!(env!("OUT_DIR"), "/tunnelrpc_capnp.rs"));
}

#[allow(clippy::all, dead_code)]
mod quic_metadata_protocol_capnp {
    include!(concat!(env!("OUT_DIR"), "/quic_metadata_protocol_capnp.rs"));
}

pub use client::{ClientConfig, ConnectionDetails, TunnelClient};
pub use error::Error;
pub use stream::{ConnectRequest, IncomingStream, Responder, StreamKind};
pub use token::Token;
pub use tunnel::{ConnectionStatus, EdgeSource, Tunnel, TunnelConfig};
