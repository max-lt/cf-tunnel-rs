//! Serves the HTTP requests of a [`Tunnel`] with a `tower` service, for
//! example an axum `Router`.
//!
//! The edge sends the method, the URL and the headers as metadata, not as HTTP
//! bytes. This module builds an [`http::Request`] from them, calls the service,
//! and writes the response back. With the `websocket` feature, a WebSocket
//! request goes through hyper, so the service can upgrade it. TCP streams are
//! refused.

mod body;
mod request;
mod response;
#[cfg(feature = "websocket")]
mod websocket;

use std::future::poll_fn;

use bytes::Bytes;
use http::{Request, Response, StatusCode};
use http_body::Body as HttpBody;
use tower_service::Service;

use crate::{Error, IncomingStream, StreamKind, Tunnel};

pub use body::Body;
use request::build_request;
use response::{respond, status_only};

type BoxError = Box<dyn std::error::Error + Send + Sync>;

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
    S: Service<Request<Body>, Response = Response<B>> + Clone + Send + 'static,
    S::Error: Into<BoxError> + Send,
    S::Future: Send,
    B: HttpBody<Data = Bytes> + Send + 'static,
    B::Error: Into<BoxError> + Send,
{
    let (request, recv, responder) = stream.split();

    // A WebSocket needs hyper, because services read the upgrade from it.
    #[cfg(feature = "websocket")]
    if request.kind == StreamKind::WebSocket {
        websocket::serve(request, recv, responder, service).await;

        return;
    }

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
