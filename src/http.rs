//! Serves the HTTP requests of a [`Tunnel`] with a `tower` service, for
//! example an axum `Router`.
//!
//! The edge sends the method, the URL and the headers as metadata, not as HTTP
//! bytes. This module builds an [`http::Request`] from them, calls the service,
//! and writes the response back. WebSocket and TCP streams are refused.

use std::{
    future::poll_fn,
    io,
    pin::{Pin, pin},
    task::{Context, Poll},
};

use bytes::{Bytes, BytesMut};
use http::{Method, Request, Response, StatusCode, Version, header};
use http_body::{Body as HttpBody, Frame};
use http_body_util::BodyExt;
use quinn::RecvStream;
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};
use tokio_util::io::poll_read_buf;
use tower_service::Service;

use crate::{ConnectRequest, Error, IncomingStream, Responder, StreamKind, Tunnel};

type BoxError = Box<dyn std::error::Error + Send + Sync>;

const READ_CHUNK: usize = 16 * 1024;
/// Response headers that describe the hop to the edge, not the response.
const HOP_HEADERS: [&str; 3] = ["connection", "keep-alive", "transfer-encoding"];

/// The body of a request, read from its stream.
pub struct Body<R = RecvStream> {
    reader: R,
    chunk: BytesMut,
}

impl<R> Body<R> {
    fn new(reader: R) -> Self {
        Self {
            reader,
            chunk: BytesMut::new(),
        }
    }
}

impl<R: AsyncRead + Unpin> HttpBody for Body<R> {
    type Data = Bytes;
    type Error = io::Error;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, io::Error>>> {
        let this = &mut *self;
        this.chunk.reserve(READ_CHUNK);

        match poll_read_buf(Pin::new(&mut this.reader), cx, &mut this.chunk) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Err(error)) => Poll::Ready(Some(Err(error))),

            // The edge closed its side: the body is complete.
            Poll::Ready(Ok(0)) => Poll::Ready(None),
            Poll::Ready(Ok(_)) => Poll::Ready(Some(Ok(Frame::data(this.chunk.split().freeze())))),
        }
    }
}

impl Tunnel {
    /// Serves the requests of every connection with `service` until the tunnel
    /// stops. Each request runs in its own task.
    ///
    /// ```no_run
    /// # async fn demo(token: cf_tunnel_rs::Token) -> Result<(), cf_tunnel_rs::Error> {
    /// use cf_tunnel_rs::{Tunnel, TunnelConfig};
    ///
    /// let app = axum::Router::new().route("/", axum::routing::get(|| async { "hello" }));
    /// Tunnel::start(TunnelConfig::new(token)).await?.serve(app).await
    /// # }
    /// ```
    pub async fn serve<S, B>(&self, service: S) -> Result<(), Error>
    where
        S: Service<Request<Body>, Response = Response<B>> + Clone + Send + 'static,
        S::Error: Into<BoxError> + Send,
        S::Future: Send,
        B: HttpBody<Data = Bytes> + Send + 'static,
        B::Error: Into<BoxError> + Send,
    {
        loop {
            let stream = self.accept().await?;
            tokio::spawn(serve_stream(stream, service.clone()));
        }
    }
}

/// Answers one request stream with `service`.
async fn serve_stream<S, B>(stream: IncomingStream, mut service: S)
where
    S: Service<Request<Body>, Response = Response<B>>,
    S::Error: Into<BoxError>,
    B: HttpBody<Data = Bytes>,
    B::Error: Into<BoxError>,
{
    let (request, recv, responder) = stream.split();

    if request.kind != StreamKind::Http {
        let _ = responder.reject("only http is supported").await;

        return;
    }

    let request = match build_request(&request, recv) {
        Ok(request) => request,
        Err(error) => {
            tracing::debug!("refused a request the edge forwarded: {error}");
            status_only(responder, StatusCode::BAD_REQUEST).await;

            return;
        }
    };

    let method = request.method().clone();

    // Call the service as tower asks: wait until it is ready, then call it.
    let response = match poll_fn(|cx| service.poll_ready(cx)).await {
        Ok(()) => service.call(request).await,
        Err(error) => Err(error),
    };

    let response = match response {
        Ok(response) => response,
        Err(error) => {
            tracing::debug!("the service failed: {}", error.into());
            status_only(responder, StatusCode::INTERNAL_SERVER_ERROR).await;

            return;
        }
    };

    if let Err(error) = respond(responder, &method, response).await {
        tracing::debug!("could not answer the edge: {error}");
    }
}

/// Builds the request: origin-form URL, headers, and the body stream.
fn build_request<R>(request: &ConnectRequest, reader: R) -> Result<Request<Body<R>>, http::Error> {
    let method = Method::from_bytes(request.http_method().unwrap_or("GET").as_bytes())?;

    let mut builder = Request::builder()
        .method(method)
        .uri(request.http_path().as_ref())
        .version(Version::HTTP_11);

    for (name, value) in request.http_headers() {
        builder = builder.header(name, value);
    }

    // The edge also reports the host on its own.
    let has_host = builder
        .headers_ref()
        .is_some_and(|headers| headers.contains_key(header::HOST));

    if let (false, Some(host)) = (has_host, request.http_host()) {
        builder = builder.header(header::HOST, host);
    }

    builder.body(Body::new(reader))
}

/// Writes the response head, then its body. A `HEAD` response has no body.
async fn respond<B>(
    responder: Responder,
    method: &Method,
    response: Response<B>,
) -> Result<(), Error>
where
    B: HttpBody<Data = Bytes>,
    B::Error: Into<BoxError>,
{
    let (parts, body) = response.into_parts();

    // Header values that are not text cannot travel as metadata.
    let headers = parts
        .headers
        .iter()
        .filter(|(name, _)| !HOP_HEADERS.contains(&name.as_str()))
        .filter_map(|(name, value)| Some((name.as_str(), value.to_str().ok()?)));

    let mut send = responder
        .respond_http(parts.status.as_u16(), headers)
        .await?;

    if method != Method::HEAD && write_body(&mut send, body).await.is_err() {
        // The body broke after the head went out: reset, so the edge sees a failure.
        let _ = send.reset(0u32.into());

        return Ok(());
    }

    let _ = send.finish();

    Ok(())
}

/// Answers with a status and no body.
async fn status_only(responder: Responder, status: StatusCode) {
    if let Ok(mut send) = responder.respond_http(status.as_u16(), []).await {
        let _ = send.finish();
    }
}

/// Copies the data frames of `body` to `out`. Trailers are not supported.
async fn write_body<B, W>(out: &mut W, body: B) -> Result<(), BoxError>
where
    B: HttpBody<Data = Bytes>,
    B::Error: Into<BoxError>,
    W: AsyncWrite + Unpin,
{
    let mut body = pin!(body);

    while let Some(frame) = body.frame().await {
        if let Ok(data) = frame.map_err(Into::into)?.into_data() {
            out.write_all(&data).await?;
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use http_body_util::{BodyExt, Full};

    use super::*;

    fn connect_request(dest: &str, metadata: &[(&str, &str)]) -> ConnectRequest {
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
    fn the_request_has_method_url_and_headers() {
        let edge = connect_request(
            "https://example.com/a/b?c=d",
            &[
                ("HttpMethod", "POST"),
                ("HttpHost", "example.com"),
                ("HttpHeader:Accept", "text/html"),
                ("HttpHeader:Set-Cookie", "a=1"),
                ("HttpHeader:Set-Cookie", "b=2"),
            ],
        );

        let request = build_request(&edge, tokio::io::empty()).unwrap();

        assert_eq!(request.method(), Method::POST);
        assert_eq!(request.uri().path_and_query().unwrap(), "/a/b?c=d");
        assert_eq!(request.headers()["accept"], "text/html");
        assert_eq!(request.headers()["host"], "example.com");
        assert_eq!(request.headers().get_all("set-cookie").iter().count(), 2);
    }

    #[test]
    fn a_host_header_is_not_added_twice() {
        let edge = connect_request(
            "https://example.com/",
            &[
                ("HttpHost", "example.com"),
                ("HttpHeader:Host", "example.com"),
            ],
        );

        let request = build_request(&edge, tokio::io::empty()).unwrap();

        assert_eq!(request.headers().get_all("host").iter().count(), 1);
    }

    #[test]
    fn a_bad_method_or_header_is_an_error() {
        let bad_method = connect_request("https://h/", &[("HttpMethod", "NOT A METHOD")]);
        let bad_header = connect_request("https://h/", &[("HttpHeader:Bad Name", "x")]);

        assert!(build_request(&bad_method, tokio::io::empty()).is_err());
        assert!(build_request(&bad_header, tokio::io::empty()).is_err());
    }

    #[tokio::test]
    async fn the_body_streams_what_the_edge_sends() {
        let data = vec![7u8; 40_000];
        let body = Body::new(&data[..]);

        let collected = body.collect().await.unwrap().to_bytes();

        assert_eq!(collected, data);
    }

    #[tokio::test]
    async fn the_response_body_is_copied_to_the_stream() {
        let mut out = Vec::new();

        write_body(&mut out, Full::new(Bytes::from_static(b"hello")))
            .await
            .unwrap();

        assert_eq!(out, b"hello");
    }

    #[tokio::test]
    async fn a_failing_body_is_an_error() {
        struct Broken;

        impl HttpBody for Broken {
            type Data = Bytes;
            type Error = io::Error;

            fn poll_frame(
                self: Pin<&mut Self>,
                _: &mut Context<'_>,
            ) -> Poll<Option<Result<Frame<Bytes>, io::Error>>> {
                Poll::Ready(Some(Err(io::Error::other("boom"))))
            }
        }

        assert!(write_body(&mut Vec::new(), Broken).await.is_err());
    }
}
