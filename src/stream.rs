use std::borrow::Cow;

use quinn::{RecvStream, SendStream};

use crate::{
    Error,
    wire::{self, Builder, Message},
};

const DATA_SIGNATURE: [u8; 6] = [0x0A, 0x36, 0xCD, 0x12, 0xA1, 0x3E];
const RPC_SIGNATURE: [u8; 6] = [0x52, 0xBB, 0x82, 0x5C, 0xDB, 0x65];
const VERSION: [u8; 2] = *b"01";
const HEADER_PREFIX: &str = "HttpHeader:";
const STATUS_KEY: &str = "HttpStatus";

/// Values of the `ConnectionType` enum.
const HTTP: u16 = 0;
const WEBSOCKET: u16 = 1;
const TCP: u16 = 2;

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

        // The path starts at the first `/`. A query with no path needs a `/` in front.
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

        // RPC streams carry no request, so the caller skips them.
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

        // Fail closed on a version this crate does not know.
        if version != VERSION {
            return Err(Error::Protocol(format!("unknown version {version:02x?}")));
        }

        let message = wire::read_message(&mut recv).await?;

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

/// Reads a `ConnectRequest`: `dest` (pointer 0), `type` (u16 at byte 0) and
/// `metadata` (pointer 1, a list of key-value structs).
pub(crate) fn parse_request(message: &Message) -> Result<ConnectRequest, Error> {
    let root = message.root()?;

    let kind = match root.u16(0) {
        HTTP => StreamKind::Http,
        WEBSOCKET => StreamKind::WebSocket,
        TCP => StreamKind::Tcp,
        other => return Err(Error::Protocol(format!("unknown connection type {other}"))),
    };

    // Copy the key-value entries out of the message.
    let mut metadata = Vec::new();
    for entry in root.struct_list(1)?.iter() {
        metadata.push((entry.text(0)?.to_owned(), entry.text(1)?.to_owned()));
    }

    Ok(ConnectRequest {
        dest: root.text(0)?.to_owned(),
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
        // The edge reads the status and each header from metadata entries.
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

/// Preamble followed by one `ConnectResponse`: `error` (pointer 0) and
/// `metadata` (pointer 1).
pub(crate) fn response_bytes(error: Option<&str>, metadata: &[(&str, &str)]) -> Vec<u8> {
    let mut b = Builder::new();
    let response = b.init_root(0, 2);

    if let Some(error) = error {
        b.set_text(response.slot(0), error);
    }

    if !metadata.is_empty() {
        let list = b.init_struct_list(response.slot(1), metadata.len(), 0, 2);

        for (i, (key, val)) in metadata.iter().enumerate() {
            b.set_text(list.get(i).slot(0), key);
            b.set_text(list.get(i).slot(1), val);
        }
    }

    let message = b.finish();
    let mut out = Vec::with_capacity(DATA_SIGNATURE.len() + VERSION.len() + message.len());
    out.extend_from_slice(&DATA_SIGNATURE);
    out.extend_from_slice(&VERSION);
    out.extend_from_slice(&message);

    out
}

/// What the edge writes at the start of an http request stream, for tests.
#[cfg(test)]
pub(crate) fn request_bytes(dest: &str, metadata: &[(&str, &str)]) -> Vec<u8> {
    let mut b = Builder::new();
    let root = b.init_root(1, 2);
    b.set_u16(root, 0, HTTP);
    b.set_text(root.slot(0), dest);

    let list = b.init_struct_list(root.slot(1), metadata.len(), 0, 2);

    for (i, (key, val)) in metadata.iter().enumerate() {
        b.set_text(list.get(i).slot(0), key);
        b.set_text(list.get(i).slot(1), val);
    }

    let message = b.finish();
    let mut out = Vec::new();
    out.extend_from_slice(&DATA_SIGNATURE);
    out.extend_from_slice(&VERSION);
    out.extend_from_slice(&message);

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

    #[tokio::test]
    async fn response_round_trips() {
        let bytes = response_bytes(Some("boom"), &[("HttpStatus", "502")]);
        assert_eq!(bytes[..6], DATA_SIGNATURE);
        assert_eq!(bytes[6..8], VERSION);

        let message = wire::read_message(&mut &bytes[8..]).await.unwrap();
        let root = message.root().unwrap();
        assert_eq!(root.text(0).unwrap(), "boom");

        let entry = root.struct_list(1).unwrap().get(0).unwrap();
        assert_eq!(entry.text(0).unwrap(), "HttpStatus");
        assert_eq!(entry.text(1).unwrap(), "502");
    }

    #[tokio::test]
    async fn request_parses() {
        let mut b = Builder::new();
        let root = b.init_root(1, 2);
        b.set_u16(root, 0, WEBSOCKET);
        b.set_text(root.slot(0), "https://h/p");

        let list = b.init_struct_list(root.slot(1), 1, 0, 2);
        b.set_text(list.get(0).slot(0), "HttpMethod");
        b.set_text(list.get(0).slot(1), "GET");

        let message = wire::read_message(&mut &b.finish()[..]).await.unwrap();
        let parsed = parse_request(&message).unwrap();

        assert_eq!(parsed.dest, "https://h/p");
        assert_eq!(parsed.kind, StreamKind::WebSocket);
        assert_eq!(parsed.http_method(), Some("GET"));
    }

    #[tokio::test]
    async fn an_unknown_connection_type_is_refused() {
        let mut b = Builder::new();
        let root = b.init_root(1, 2);
        b.set_u16(root, 0, 9);

        let message = wire::read_message(&mut &b.finish()[..]).await.unwrap();

        assert!(parse_request(&message).is_err());
    }
}
