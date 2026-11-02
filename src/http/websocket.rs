//! Serves a WebSocket request with a `tower` service.
//!
//! A service reads the upgrade from hyper's `OnUpgrade` extension, and only a
//! hyper connection creates one. So hyper runs on one end of an in-memory
//! pipe, and this module speaks HTTP/1.1 on the other end: it writes the
//! request, reads the response head, and then relays raw bytes between the
//! pipe and the edge.

use std::{future::poll_fn, io};

use bytes::Bytes;
use http::{HeaderValue, Request, Response, StatusCode, header};
use http_body::Body as HttpBody;
use hyper::{body::Incoming, server::conn::http1, service::service_fn};
use hyper_util::rt::TokioIo;
use quinn::RecvStream;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt, DuplexStream};
use tower_service::Service;

use super::{
    Body, BoxError,
    request::request_builder,
    response::{HOP_HEADERS, status_only},
};
use crate::{ConnectRequest, Responder};

const PIPE_SIZE: usize = 64 * 1024;
const SWITCHING_PROTOCOLS: u16 = 101;
const MAX_HEADERS: usize = 64;
const MAX_HEAD: usize = 16 * 1024;
/// Largest body of a refused upgrade that is passed on to the edge.
const MAX_REFUSAL_BODY: usize = 64 * 1024;

/// The head of the response that hyper writes into the pipe.
struct ResponseHead {
    status: u16,
    headers: Vec<(String, String)>,
    /// Bytes that followed the head in the same read.
    rest: Vec<u8>,
}

impl ResponseHead {
    fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(key, _)| key.eq_ignore_ascii_case(name))
            .map(|(_, value)| value.as_str())
    }

    fn pairs(&self) -> impl Iterator<Item = (&str, &str)> {
        self.headers.iter().map(|(k, v)| (k.as_str(), v.as_str()))
    }
}

/// Answers one WebSocket request stream with `service`.
pub(super) async fn serve<S, B>(
    request: ConnectRequest,
    recv: RecvStream,
    responder: Responder,
    service: S,
) where
    S: Service<Request<Body>, Response = Response<B>> + Clone + Send + 'static,
    S::Error: Into<BoxError> + Send,
    S::Future: Send,
    B: HttpBody<Data = Bytes> + Send + 'static,
    B::Error: Into<BoxError> + Send,
{
    let head = match request_head(&request) {
        Ok(head) => head,
        Err(error) => {
            tracing::debug!("refused a websocket request the edge forwarded: {error}");
            status_only(responder, StatusCode::BAD_REQUEST).await;

            return;
        }
    };

    let (mut pipe, hyper_end) = tokio::io::duplex(PIPE_SIZE);
    tokio::spawn(run_hyper(hyper_end, service));

    let response = match exchange(&mut pipe, &head).await {
        Ok(response) => response,
        Err(error) => {
            tracing::debug!("no websocket response from the service: {error}");
            status_only(responder, StatusCode::BAD_GATEWAY).await;

            return;
        }
    };

    // The service refused the upgrade, so no byte stream follows.
    if response.status != SWITCHING_PROTOCOLS {
        refuse(responder, response, &mut pipe).await;

        return;
    }

    relay(responder, response, pipe, recv).await;
}

/// The HTTP/1.1 bytes of the request head. hyper upgrades only if the head asks.
fn request_head(request: &ConnectRequest) -> Result<Vec<u8>, http::Error> {
    let (mut parts, ()) = request_builder(request)?.body(())?.into_parts();
    parts
        .headers
        .insert(header::UPGRADE, HeaderValue::from_static("websocket"));
    parts
        .headers
        .insert(header::CONNECTION, HeaderValue::from_static("Upgrade"));

    let mut head = format!("{} {} HTTP/1.1\r\n", parts.method, parts.uri).into_bytes();

    // A header value cannot hold a line break, so a header cannot inject another.
    for (name, value) in &parts.headers {
        head.extend_from_slice(name.as_str().as_bytes());
        head.extend_from_slice(b": ");
        head.extend_from_slice(value.as_bytes());
        head.extend_from_slice(b"\r\n");
    }

    head.extend_from_slice(b"\r\n");

    Ok(head)
}

/// Serves the one request of the pipe with `service`, and hands the pipe over
/// to the service once it upgrades.
async fn run_hyper<S, B>(pipe: DuplexStream, service: S)
where
    S: Service<Request<Body>, Response = Response<B>> + Clone + Send + 'static,
    S::Error: Into<BoxError> + Send,
    S::Future: Send,
    B: HttpBody<Data = Bytes> + Send + 'static,
    B::Error: Into<BoxError> + Send,
{
    let service = service_fn(move |request| call(service.clone(), request));
    let connection = http1::Builder::new()
        .serve_connection(TokioIo::new(pipe), service)
        .with_upgrades();

    if let Err(error) = connection.await {
        tracing::debug!("websocket connection ended: {error}");
    }
}

async fn call<S, B>(mut service: S, request: Request<Incoming>) -> Result<Response<B>, BoxError>
where
    S: Service<Request<Body>, Response = Response<B>>,
    S::Error: Into<BoxError>,
{
    // The upgrade request has no body. Its extensions carry hyper's `OnUpgrade`.
    let (parts, _) = request.into_parts();
    let request = Request::from_parts(parts, Body::empty());

    poll_fn(|cx| service.poll_ready(cx))
        .await
        .map_err(Into::into)?;

    service.call(request).await.map_err(Into::into)
}

/// Writes the request head to the pipe and reads the response head.
async fn exchange(pipe: &mut DuplexStream, head: &[u8]) -> io::Result<ResponseHead> {
    pipe.write_all(head).await?;

    let mut buf = Vec::new();

    loop {
        // Read until the head is complete.
        if pipe.read_buf(&mut buf).await? == 0 {
            return Err(io::ErrorKind::UnexpectedEof.into());
        }

        let mut slots = [httparse::EMPTY_HEADER; MAX_HEADERS];
        let mut parsed = httparse::Response::new(&mut slots);
        let status = parsed
            .parse(&buf)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;

        let httparse::Status::Complete(end) = status else {
            if buf.len() > MAX_HEAD {
                return Err(io::Error::new(io::ErrorKind::InvalidData, "head too long"));
            }

            continue;
        };

        // A header value that is not text cannot travel as metadata.
        let headers = parsed
            .headers
            .iter()
            .filter_map(|h| {
                Some((
                    h.name.to_owned(),
                    std::str::from_utf8(h.value).ok()?.to_owned(),
                ))
            })
            .collect();
        let status = parsed.code.unwrap_or(StatusCode::BAD_GATEWAY.as_u16());

        return Ok(ResponseHead {
            status,
            headers,
            rest: buf.split_off(end),
        });
    }
}

/// Passes a refusal to the edge. A body with a known size goes along.
async fn refuse<R: AsyncRead + Unpin>(responder: Responder, mut head: ResponseHead, pipe: &mut R) {
    let length = head
        .header("content-length")
        .and_then(|value| value.trim().parse::<usize>().ok())
        .unwrap_or(0)
        .min(MAX_REFUSAL_BODY);

    let mut body = std::mem::take(&mut head.rest);

    while body.len() < length {
        // Read the rest of the body, unless the pipe ends first.
        if !matches!(pipe.read_buf(&mut body).await, Ok(1..)) {
            break;
        }
    }

    body.truncate(length);

    let headers = head
        .pairs()
        .filter(|(name, _)| !HOP_HEADERS.iter().any(|hop| hop.eq_ignore_ascii_case(name)));

    let Ok(mut send) = responder.respond_http(head.status, headers).await else {
        return;
    };

    let _ = send.write_all(&body).await;
    let _ = send.finish();
}

/// Sends the 101 to the edge, then copies bytes both ways until both sides end.
async fn relay(responder: Responder, head: ResponseHead, pipe: DuplexStream, mut recv: RecvStream) {
    let Ok(mut send) = responder.respond_http(head.status, head.pairs()).await else {
        return;
    };

    // The service may have written its first frame right behind the head.
    if send.write_all(&head.rest).await.is_err() {
        return;
    }

    let (mut from_service, mut to_service) = tokio::io::split(pipe);

    let to_edge = async {
        let _ = tokio::io::copy(&mut from_service, &mut send).await;
        let _ = send.finish();
    };

    let to_service = async {
        let _ = tokio::io::copy(&mut recv, &mut to_service).await;
        let _ = to_service.shutdown().await;
    };

    tokio::join!(to_edge, to_service);
}
