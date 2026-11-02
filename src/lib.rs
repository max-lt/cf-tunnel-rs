//! Client for Cloudflare Tunnel. It connects to the Cloudflare edge over QUIC
//! and hands each incoming request stream to the caller.
//!
//! ```no_run
//! # async fn demo() -> Result<(), cf_tunnel_rs::Error> {
//! use cf_tunnel_rs::{ClientConfig, Token, TunnelClient};
//!
//! let token: Token = std::env::var("TUNNEL_TOKEN").unwrap().parse()?;
//! let client = TunnelClient::connect(ClientConfig::new(token, "198.41.192.167:7844".parse().unwrap())).await?;
//! while let Ok(stream) = client.accept().await {
//!     println!("{} {}", stream.request().dest, stream.request().http_method().unwrap_or("-"));
//! }
//! # Ok(()) }
//! ```

mod client;
mod error;
mod stream;
mod token;

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
pub use stream::{ConnectRequest, IncomingStream, Responder, StreamIo, StreamKind, header_key};
pub use token::Token;
