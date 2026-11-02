use std::{
    io,
    pin::Pin,
    task::{Context, Poll},
};

use bytes::{Bytes, BytesMut};
use http_body::{Body as HttpBody, Frame};
use quinn::RecvStream;
use tokio::io::AsyncRead;
use tokio_util::io::poll_read_buf;

const READ_CHUNK: usize = 16 * 1024;

/// The body of a request, read from its stream.
pub struct Body<R = RecvStream> {
    /// `None` for a body with no data.
    reader: Option<R>,
    chunk: BytesMut,
}

impl<R> Body<R> {
    pub(super) fn new(reader: R) -> Self {
        Self {
            reader: Some(reader),
            chunk: BytesMut::new(),
        }
    }

    /// The body of an upgrade request, which has none.
    #[cfg(feature = "websocket")]
    pub(super) fn empty() -> Self {
        Self {
            reader: None,
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

        // A body with no data is complete at once.
        let Some(reader) = this.reader.as_mut() else {
            return Poll::Ready(None);
        };

        this.chunk.reserve(READ_CHUNK);

        match poll_read_buf(Pin::new(reader), cx, &mut this.chunk) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Err(error)) => Poll::Ready(Some(Err(error))),

            // The edge closed its side: the body is complete.
            Poll::Ready(Ok(0)) => Poll::Ready(None),
            Poll::Ready(Ok(_)) => Poll::Ready(Some(Ok(Frame::data(this.chunk.split().freeze())))),
        }
    }
}

#[cfg(test)]
mod tests {
    use http_body_util::BodyExt;

    use super::*;

    #[tokio::test]
    async fn the_body_streams_what_the_edge_sends() {
        let data = vec![7u8; 40_000];
        let body = Body::new(&data[..]);

        let collected = body.collect().await.unwrap().to_bytes();

        assert_eq!(collected, data);
    }
}
