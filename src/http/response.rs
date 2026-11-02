use std::pin::pin;

use bytes::Bytes;
use http::{Method, Response, StatusCode};
use http_body::Body as HttpBody;
use http_body_util::BodyExt;
use tokio::io::{AsyncWrite, AsyncWriteExt};

use super::BoxError;
use crate::{Error, Responder};

/// Response headers that describe the hop to the edge, not the response.
pub(super) const HOP_HEADERS: [&str; 3] = ["connection", "keep-alive", "transfer-encoding"];

/// Writes the response head, then its body. A `HEAD` response has no body.
pub(super) async fn respond<B>(
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
pub(super) async fn status_only(responder: Responder, status: StatusCode) {
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
    use std::{
        io,
        pin::Pin,
        task::{Context, Poll},
    };

    use http_body::Frame;
    use http_body_util::Full;

    use super::*;

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
