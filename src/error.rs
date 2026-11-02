use std::time::Duration;

#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
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
    Rpc(String),
    #[error("edge discovery: {0}")]
    Dns(String),
    #[error("tunnel stopped")]
    Stopped,
    #[error("registration timed out")]
    RegistrationTimeout,
    #[error("edge refused registration: {cause}")]
    Refused {
        cause: String,
        retry_after: Duration,
        should_retry: bool,
    },
    #[error("protocol: {0}")]
    Protocol(String),
    #[error("stream read: {0}")]
    Read(#[from] quinn::ReadExactError),
    #[error("stream write: {0}")]
    Write(#[from] quinn::WriteError),
    #[error("I/O: {0}")]
    Io(#[from] std::io::Error),
}

impl Error {
    /// True when the edge refused because this client already has a connection
    /// on that edge address. Retrying on another address works.
    pub(crate) fn is_duplicate(&self) -> bool {
        matches!(self, Error::Refused { cause, .. } if cause == "EDUPCONN")
    }

    /// How long the edge asks the client to wait before it tries again.
    pub(crate) fn retry_after(&self) -> Duration {
        match self {
            Error::Refused { retry_after, .. } => *retry_after,
            _ => Duration::ZERO,
        }
    }
}
