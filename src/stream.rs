use quinn::{RecvStream, SendStream};
use tokio::io::Join;
use tokio_util::compat::TokioAsyncReadCompatExt;

use crate::{
    Error,
    quic_metadata_protocol_capnp::{ConnectionType, connect_request, connect_response},
};

const DATA_SIGNATURE: [u8; 6] = [0x0A, 0x36, 0xCD, 0x12, 0xA1, 0x3E];
const RPC_SIGNATURE: [u8; 6] = [0x52, 0xBB, 0x82, 0x5C, 0xDB, 0x65];
const VERSION: &[u8; 2] = b"01";
const HEADER_PREFIX: &str = "HttpHeader:";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamKind {
    Http,
    WebSocket,
    Tcp,
}

/// What the edge asks the client to reach.
#[derive(Debug, Clone)]
pub struct ConnectRequest {
    /// Request URL for http and websocket, `host:port` for tcp.
    pub dest: String,
    pub kind: StreamKind,
    pub metadata: Vec<(String, String)>,
}

impl ConnectRequest {
    fn find(&self, key: &str) -> Option<&str> {
        self.metadata
            .iter()
            .find(|(k, _)| k == key)
            .map(|(_, v)| v.as_str())
    }

    pub fn http_method(&self) -> Option<&str> {
        self.find("HttpMethod")
    }

    pub fn http_host(&self) -> Option<&str> {
        self.find("HttpHost")
    }

    pub fn http_headers(&self) -> impl Iterator<Item = (&str, &str)> {
        self.metadata
            .iter()
            .filter_map(|(k, v)| Some((k.strip_prefix(HEADER_PREFIX)?, v.as_str())))
    }

    /// Path and query of `dest`, or `/` when `dest` has none.
    pub fn http_path(&self) -> &str {
        self.dest
            .split_once("://")
            .and_then(|(_, rest)| rest.find('/').map(|i| &rest[i..]))
            .unwrap_or("/")
    }
}

/// Both halves of an accepted stream.
pub type StreamIo = Join<RecvStream, SendStream>;

/// A request stream opened by the edge. Call [`accept`](Self::accept) or
/// [`reject`](Self::reject) exactly once.
pub struct IncomingStream {
    request: ConnectRequest,
    recv: RecvStream,
    send: SendStream,
}

impl IncomingStream {
    /// Reads the preamble and request. Returns `None` for an RPC stream.
    pub(crate) async fn read(
        send: SendStream,
        mut recv: RecvStream,
    ) -> Result<Option<Self>, Error> {
        let mut signature = [0u8; 6];
        recv.read_exact(&mut signature)
            .await
            .map_err(|e| Error::Protocol(e.to_string()))?;
        if signature == RPC_SIGNATURE {
            return Ok(None);
        }
        if signature != DATA_SIGNATURE {
            return Err(Error::Protocol(format!(
                "unknown signature {signature:02x?}"
            )));
        }
        let mut version = [0u8; 2];
        recv.read_exact(&mut version)
            .await
            .map_err(|e| Error::Protocol(e.to_string()))?;

        let message =
            capnp_futures::serialize::read_message((&mut recv).compat(), Default::default())
                .await?;
        let root = message.get_root::<connect_request::Reader>()?;
        let kind = match root.get_type()? {
            ConnectionType::Http => StreamKind::Http,
            ConnectionType::Websocket => StreamKind::WebSocket,
            ConnectionType::Tcp => StreamKind::Tcp,
        };
        let mut metadata = Vec::new();
        for entry in root.get_metadata()? {
            metadata.push((
                entry.get_key()?.to_str()?.to_owned(),
                entry.get_val()?.to_str()?.to_owned(),
            ));
        }
        Ok(Some(Self {
            request: ConnectRequest {
                dest: root.get_dest()?.to_str()?.to_owned(),
                kind,
                metadata,
            },
            recv,
            send,
        }))
    }

    pub fn request(&self) -> &ConnectRequest {
        &self.request
    }

    /// Splits the stream so the request body can be read before the response
    /// is sent.
    pub fn split(self) -> (ConnectRequest, RecvStream, Responder) {
        (self.request, self.recv, Responder { send: self.send })
    }

    /// Sends a success response, then returns the stream for the body bytes.
    /// For http, `metadata` holds `HttpStatus` and `HttpHeader:<name>` entries.
    pub async fn accept(self, metadata: &[(String, String)]) -> Result<StreamIo, Error> {
        let (_, recv, responder) = self.split();
        Ok(tokio::io::join(recv, responder.accept(metadata).await?))
    }

    /// Sends an error response and closes the stream.
    pub async fn reject(self, error: &str, metadata: &[(String, String)]) -> Result<(), Error> {
        self.split().2.reject(error, metadata).await
    }
}

/// The response half of an [`IncomingStream`].
pub struct Responder {
    send: SendStream,
}

impl Responder {
    /// Sends a success response and returns the stream for the response body.
    pub async fn accept(mut self, metadata: &[(String, String)]) -> Result<SendStream, Error> {
        write_response(&mut self.send, None, metadata).await?;
        Ok(self.send)
    }

    /// Sends an error response and closes the stream.
    pub async fn reject(mut self, error: &str, metadata: &[(String, String)]) -> Result<(), Error> {
        write_response(&mut self.send, Some(error), metadata).await?;
        self.send.finish().ok();
        Ok(())
    }
}

/// Builds the `HttpHeader:<name>` metadata key for a response header.
pub fn header_key(name: &str) -> String {
    format!("{HEADER_PREFIX}{name}")
}

async fn write_response(
    send: &mut SendStream,
    error: Option<&str>,
    metadata: &[(String, String)],
) -> Result<(), Error> {
    let mut message = capnp::message::Builder::new_default();
    {
        let mut response = message.init_root::<connect_response::Builder>();
        if let Some(error) = error {
            response.set_error(error);
        }
        let mut list = response.init_metadata(metadata.len() as u32);
        for (i, (key, val)) in metadata.iter().enumerate() {
            let mut entry = list.reborrow().get(i as u32);
            entry.set_key(key.as_str());
            entry.set_val(val.as_str());
        }
    }
    let mut out = Vec::with_capacity(8 + 256);
    out.extend_from_slice(&DATA_SIGNATURE);
    out.extend_from_slice(VERSION);
    out.extend_from_slice(&capnp::serialize::write_message_to_words(&message));
    send.write_all(&out)
        .await
        .map_err(|e| Error::Protocol(e.to_string()))
}
