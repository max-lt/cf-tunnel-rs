use std::borrow::Cow;

use quinn::{RecvStream, SendStream};
use tokio_util::compat::TokioAsyncReadCompatExt;

use crate::{
    Error,
    quic_metadata_protocol_capnp::{ConnectionType, connect_request, connect_response},
};

const DATA_SIGNATURE: [u8; 6] = [0x0A, 0x36, 0xCD, 0x12, 0xA1, 0x3E];
const RPC_SIGNATURE: [u8; 6] = [0x52, 0xBB, 0x82, 0x5C, 0xDB, 0x65];
const VERSION: [u8; 2] = *b"01";
const HEADER_PREFIX: &str = "HttpHeader:";
const STATUS_KEY: &str = "HttpStatus";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum StreamKind {
    Http,
    WebSocket,
    Tcp,
}

/// What the edge asks the client to reach.
#[derive(Debug, Clone)]
#[non_exhaustive]
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

    /// Path and query of `dest`. A URL without a path gives `/`.
    pub fn http_path(&self) -> Cow<'_, str> {
        let Some((_, rest)) = self.dest.split_once("://") else {
            return Cow::Borrowed("/");
        };
        match rest.find(['/', '?']) {
            Some(i) if rest[i..].starts_with('/') => Cow::Borrowed(&rest[i..]),
            Some(i) => Cow::Owned(format!("/{}", &rest[i..])),
            None => Cow::Borrowed("/"),
        }
    }
}

/// A request stream opened by the edge.
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
        recv.read_exact(&mut signature).await?;
        if signature == RPC_SIGNATURE {
            return Ok(None);
        }
        if signature != DATA_SIGNATURE {
            return Err(Error::Protocol(format!(
                "unknown signature {signature:02x?}"
            )));
        }
        let mut version = [0u8; 2];
        recv.read_exact(&mut version).await?;
        if version != VERSION {
            return Err(Error::Protocol(format!("unknown version {version:02x?}")));
        }
        let message =
            capnp_futures::serialize::read_message((&mut recv).compat(), Default::default())
                .await?;
        Ok(Some(Self {
            request: parse_request(&message)?,
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
}

fn parse_request<S: capnp::message::ReaderSegments>(
    message: &capnp::message::Reader<S>,
) -> Result<ConnectRequest, Error> {
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
    Ok(ConnectRequest {
        dest: root.get_dest()?.to_str()?.to_owned(),
        kind,
        metadata,
    })
}

/// The response half of an [`IncomingStream`].
///
/// The stream returned by `accept` and `respond_http` carries the response
/// body. Dropping it finishes the stream cleanly, so the edge sees a complete
/// response. Call `reset` on it to signal a failure after the head is sent.
pub struct Responder {
    send: SendStream,
}

impl Responder {
    /// Sends a success response with raw metadata entries.
    pub async fn accept(mut self, metadata: &[(&str, &str)]) -> Result<SendStream, Error> {
        self.send.write_all(&response_bytes(None, metadata)).await?;
        Ok(self.send)
    }

    /// Sends an http response head.
    pub async fn respond_http<'a>(
        self,
        status: u16,
        headers: impl IntoIterator<Item = (&'a str, &'a str)>,
    ) -> Result<SendStream, Error> {
        let status = status.to_string();
        let names: Vec<(String, &str)> = headers
            .into_iter()
            .map(|(name, val)| (format!("{HEADER_PREFIX}{name}"), val))
            .collect();
        let mut metadata = vec![(STATUS_KEY, status.as_str())];
        metadata.extend(names.iter().map(|(k, v)| (k.as_str(), *v)));
        self.accept(&metadata).await
    }

    /// Sends an error response and closes the stream.
    pub async fn reject(mut self, error: &str) -> Result<(), Error> {
        self.send
            .write_all(&response_bytes(Some(error), &[]))
            .await?;
        self.send.finish().ok();
        Ok(())
    }
}

/// Preamble followed by one `ConnectResponse` message.
fn response_bytes(error: Option<&str>, metadata: &[(&str, &str)]) -> Vec<u8> {
    let mut message = capnp::message::Builder::new_default();
    {
        let mut response = message.init_root::<connect_response::Builder>();
        if let Some(error) = error {
            response.set_error(error);
        }
        let mut list = response.init_metadata(metadata.len() as u32);
        for (i, (key, val)) in metadata.iter().enumerate() {
            let mut entry = list.reborrow().get(i as u32);
            entry.set_key(key);
            entry.set_val(val);
        }
    }
    let words = capnp::serialize::write_message_to_words(&message);
    let mut out = Vec::with_capacity(DATA_SIGNATURE.len() + VERSION.len() + words.len());
    out.extend_from_slice(&DATA_SIGNATURE);
    out.extend_from_slice(&VERSION);
    out.extend_from_slice(&words);
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request(dest: &str, metadata: &[(&str, &str)]) -> ConnectRequest {
        ConnectRequest {
            dest: dest.to_owned(),
            kind: StreamKind::Http,
            metadata: metadata
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
        }
    }

    #[test]
    fn path_keeps_the_query() {
        assert_eq!(request("https://h/a/b?x=1", &[]).http_path(), "/a/b?x=1");
        assert_eq!(request("https://h?x=1", &[]).http_path(), "/?x=1");
        assert_eq!(request("https://h", &[]).http_path(), "/");
    }

    #[test]
    fn headers_come_from_prefixed_keys_only() {
        let r = request(
            "https://h/",
            &[
                ("HttpMethod", "GET"),
                ("HttpHeader:Accept", "*/*"),
                ("HttpHost", "h"),
            ],
        );
        assert_eq!(r.http_headers().collect::<Vec<_>>(), [("Accept", "*/*")]);
        assert_eq!(r.http_method(), Some("GET"));
        assert_eq!(r.http_host(), Some("h"));
    }

    #[test]
    fn response_round_trips() {
        let bytes = response_bytes(Some("boom"), &[("HttpStatus", "502")]);
        assert_eq!(bytes[..6], DATA_SIGNATURE);
        assert_eq!(bytes[6..8], VERSION);
        let message = capnp::serialize::read_message(&mut &bytes[8..], Default::default()).unwrap();
        let root = message.get_root::<connect_response::Reader>().unwrap();
        assert_eq!(root.get_error().unwrap().to_str().unwrap(), "boom");
        let entry = root.get_metadata().unwrap().get(0);
        assert_eq!(entry.get_key().unwrap().to_str().unwrap(), "HttpStatus");
        assert_eq!(entry.get_val().unwrap().to_str().unwrap(), "502");
    }

    #[test]
    fn request_parses() {
        let mut builder = capnp::message::Builder::new_default();
        {
            let mut root = builder.init_root::<connect_request::Builder>();
            root.set_dest("https://h/p");
            root.set_type(ConnectionType::Websocket);
            let mut list = root.init_metadata(1);
            let mut entry = list.reborrow().get(0);
            entry.set_key("HttpMethod");
            entry.set_val("GET");
        }
        let words = capnp::serialize::write_message_to_words(&builder);
        let message = capnp::serialize::read_message(&mut &words[..], Default::default()).unwrap();
        let parsed = parse_request(&message).unwrap();
        assert_eq!(parsed.dest, "https://h/p");
        assert_eq!(parsed.kind, StreamKind::WebSocket);
        assert_eq!(parsed.http_method(), Some("GET"));
    }
}
