use std::time::Duration;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("invalid tunnel token: {0}")]
    Token(String),
    #[error("QUIC connect: {0}")]
    Connect(#[from] quinn::ConnectError),
    #[error("QUIC connection: {0}")]
    Connection(#[from] quinn::ConnectionError),
    #[error("TLS setup: {0}")]
    Tls(String),
    #[error("RPC: {0}")]
    Rpc(#[from] capnp::Error),
    #[error("RPC schema: {0}")]
    Schema(#[from] capnp::NotInSchema),
    #[error("RPC text: {0}")]
    Utf8(#[from] std::str::Utf8Error),
    #[error("edge refused registration: {cause}")]
    Refused {
        cause: String,
        retry_after: Duration,
        should_retry: bool,
    },
    #[error("protocol: {0}")]
    Protocol(String),
    #[error("stream: {0}")]
    Io(#[from] std::io::Error),
}
