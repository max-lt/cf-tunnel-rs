use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};

use crate::{
    ClientConfig, ConnectionDetails, Error, proto,
    wire::{self, Message},
};

/// The RPC client on the control stream. It makes the few calls a tunnel
/// connection needs and answers none: the edge never calls this stream.
pub(crate) struct Control<R, W> {
    reader: R,
    writer: W,
    /// The export id of the edge's registration capability.
    cap: u32,
    next_question: u32,
}

impl<R: AsyncRead + Unpin, W: AsyncWrite + Unpin> Control<R, W> {
    /// Asks the edge for its main capability.
    pub(crate) async fn bootstrap(reader: R, writer: W) -> Result<Self, Error> {
        let mut control = Self {
            reader,
            writer,
            cap: 0,
            next_question: 0,
        };

        let question = control.next_question();
        control.transmit(&proto::bootstrap(question)).await?;

        let answer = control.wait_for(question).await?;
        control.cap = proto::bootstrap_cap(&answer)?;

        // Keep the capability: it is the one we call next.
        control
            .transmit(&proto::finish(question, proto::Caps::Keep))
            .await?;

        Ok(control)
    }

    pub(crate) async fn register(
        &mut self,
        config: &ClientConfig,
    ) -> Result<ConnectionDetails, Error> {
        let answer = self
            .call(proto::REGISTER_CONNECTION, |b, slot| {
                proto::register_params(b, slot, config)
            })
            .await?;

        proto::registration(&answer)
    }

    /// Tells the edge this connection is going away.
    pub(crate) async fn unregister(&mut self) -> Result<(), Error> {
        let answer = self
            .call(proto::UNREGISTER_CONNECTION, proto::unregister_params)
            .await?;

        proto::call_results(&answer)?;

        Ok(())
    }

    fn next_question(&mut self) -> u32 {
        let question = self.next_question;
        self.next_question += 1;

        question
    }

    async fn call(
        &mut self,
        method: u16,
        fill: impl FnOnce(&mut wire::Builder, wire::Slot),
    ) -> Result<Message, Error> {
        let question = self.next_question();
        self.transmit(&proto::call(question, self.cap, method, fill))
            .await?;

        let answer = self.wait_for(question).await?;

        // The call is done. A failure to say so changes nothing.
        let _ = self
            .transmit(&proto::finish(question, proto::Caps::Release))
            .await;

        Ok(answer)
    }

    async fn transmit(&mut self, message: &[u8]) -> Result<(), Error> {
        self.writer.write_all(message).await?;
        self.writer.flush().await?;

        Ok(())
    }

    /// Reads messages until the answer to `question` arrives.
    async fn wait_for(&mut self, question: u32) -> Result<Message, Error> {
        loop {
            let message = wire::read_message(&mut self.reader).await?;

            match proto::classify(&message)? {
                proto::Incoming::Return(answer) if answer == question => return Ok(message),
                proto::Incoming::Abort(reason) => return Err(Error::Rpc(reason)),
                _ => {}
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use tokio::io::{ReadHalf, WriteHalf, duplex, split};
    use uuid::Uuid;

    use super::*;
    use crate::{Token, proto::fake_edge};

    type Pipe = tokio::io::DuplexStream;

    fn config() -> ClientConfig {
        ClientConfig::new(Token::sample(), ([127, 0, 0, 1], 7844).into())
    }

    /// The edge's end of the control stream.
    struct Edge {
        recv: ReadHalf<Pipe>,
        send: WriteHalf<Pipe>,
    }

    impl Edge {
        async fn read(&mut self) -> Message {
            wire::read_message(&mut self.recv).await.unwrap()
        }

        async fn write(&mut self, message: Vec<u8>) {
            self.send.write_all(&message).await.unwrap();
        }

        /// Serves the bootstrap exchange and returns the exported capability.
        async fn serve_bootstrap(&mut self) -> u32 {
            let request = self.read().await;
            assert_eq!(request.root().unwrap().u16(0), 8, "bootstrap message");

            self.write(fake_edge::bootstrap_answer(0, 7)).await;

            // The client keeps the capability: release_result_caps is false.
            let finish = self.read().await;
            let root = finish.root().unwrap();
            assert_eq!(root.u16(0), 4, "finish message");
            assert!(root.structure(0).unwrap().unwrap().bool(32));

            7
        }
    }

    /// A client end of the control stream, and the edge's end.
    fn pair() -> (ReadHalf<Pipe>, WriteHalf<Pipe>, Edge) {
        let (client, edge) = duplex(64 * 1024);
        let (client_recv, client_send) = split(client);
        let (edge_recv, edge_send) = split(edge);

        (
            client_recv,
            client_send,
            Edge {
                recv: edge_recv,
                send: edge_send,
            },
        )
    }

    #[tokio::test]
    async fn registers_and_unregisters() {
        let (client_recv, client_send, mut edge) = pair();
        let uuid = Uuid::new_v4();

        let edge_task = tokio::spawn(async move {
            edge.serve_bootstrap().await;

            // registerConnection: check the call, then accept.
            let call = edge.read().await;
            let root = call.root().unwrap();
            assert_eq!(root.u16(0), 2, "call message");

            let body = root.structure(0).unwrap().unwrap();
            assert_eq!(body.u32(0), 1, "question id");
            assert_eq!(body.u16(4), 0, "registerConnection");
            assert_eq!(body.structure(0).unwrap().unwrap().u32(0), 7, "capability");

            let params = body
                .structure(1)
                .unwrap()
                .unwrap()
                .structure(0)
                .unwrap()
                .unwrap();
            let auth = params.structure(0).unwrap().unwrap();
            assert_eq!(auth.text(0).unwrap(), "acct");
            assert_eq!(auth.data_bytes(1).unwrap(), b"secret");
            assert_eq!(params.data_bytes(1).unwrap().len(), 16);

            edge.write(fake_edge::registered(1, "mrs04", uuid)).await;
            assert_eq!(edge.read().await.root().unwrap().u16(0), 4, "finish");

            // unregisterConnection.
            let call = edge.read().await;
            let body = call.root().unwrap().structure(0).unwrap().unwrap();
            assert_eq!(body.u16(4), 1, "unregisterConnection");

            edge.write(fake_edge::empty_answer(2)).await;
        });

        let mut control = Control::bootstrap(client_recv, client_send).await.unwrap();
        let details = control.register(&config()).await.unwrap();

        assert_eq!(details.location, "mrs04");
        assert_eq!(details.uuid, uuid);
        assert!(details.remotely_managed);

        control.unregister().await.unwrap();
        edge_task.await.unwrap();
    }

    #[tokio::test]
    async fn reports_a_refusal() {
        let (client_recv, client_send, mut edge) = pair();

        tokio::spawn(async move {
            edge.serve_bootstrap().await;
            edge.read().await;
            edge.write(fake_edge::refused(1, "EDUPCONN", false)).await;
        });

        let mut control = Control::bootstrap(client_recv, client_send).await.unwrap();
        let error = control.register(&config()).await.unwrap_err();

        assert!(error.is_duplicate());
        assert!(matches!(
            error,
            Error::Refused {
                should_retry: false,
                ..
            }
        ));
    }

    #[tokio::test]
    async fn an_abort_ends_the_wait() {
        let (client_recv, client_send, mut edge) = pair();

        // The edge answers the bootstrap request with an abort.
        let mut b = wire::Builder::new();
        let message = b.init_root(1, 1);
        b.set_u16(message, 0, 1);
        let exception = b.init_struct(message.slot(0), 1, 2);
        b.set_text(exception.slot(0), "shutting down");
        edge.write(b.finish()).await;

        let error = Control::bootstrap(client_recv, client_send)
            .await
            .err()
            .unwrap();

        assert!(matches!(error, Error::Rpc(reason) if reason == "shutting down"));
    }
}
