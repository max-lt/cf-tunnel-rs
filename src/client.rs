use std::{
    net::{Ipv4Addr, Ipv6Addr, SocketAddr},
    sync::Arc,
    time::Duration,
};

use quinn::{RecvStream, SendStream};
use rustls::pki_types::{CertificateDer, pem::PemObject};
use tokio::sync::{Mutex, mpsc};
use uuid::Uuid;

use crate::{Error, IncomingStream, Token, control::Control};

const SERVER_NAME: &str = "quic.cftunnel.com";
const ALPN: &[u8] = b"argotunnel";
const ROOTS: &[u8] = include_bytes!("../roots/cloudflare-origin-ca.crt");
const REGISTER_TIMEOUT: Duration = Duration::from_secs(10);
const UNREGISTER_TIMEOUT: Duration = Duration::from_secs(5);
const PREAMBLE_TIMEOUT: Duration = Duration::from_secs(10);
const STREAM_QUEUE: usize = 64;
// The edge opens one stream per request, so quinn's default of 100 would stall it.
// quinn allocates state for every allowed stream up front, so the limit stays bounded.
const MAX_BIDI_STREAMS: quinn::VarInt = quinn::VarInt::from_u32(10_000);

#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct ClientConfig {
    pub token: Token,
    /// Edge address, usually port 7844.
    pub edge: SocketAddr,
    /// Index of this connection among the tunnel's connections.
    pub conn_index: u8,
    /// Identifies this process to the edge. Connections of one process share it.
    pub client_id: Uuid,
    /// Reported to the edge as the client version.
    pub version: String,
}

impl ClientConfig {
    pub fn new(token: Token, edge: SocketAddr) -> Self {
        Self {
            token,
            edge,
            conn_index: 0,
            client_id: Uuid::new_v4(),
            version: concat!("cf-tunnel-rs-", env!("CARGO_PKG_VERSION")).to_owned(),
        }
    }
}

/// Where the edge registered this connection.
#[derive(Debug, Clone)]
pub struct ConnectionDetails {
    pub uuid: Uuid,
    /// Colo code, for example `mrs04`.
    pub location: String,
    pub remotely_managed: bool,
}

/// One registered connection to the edge.
///
/// Call [`shutdown`](Self::shutdown) to unregister before closing. Dropping the
/// client closes the QUIC connection without unregistering.
pub struct TunnelClient {
    endpoint: quinn::Endpoint,
    conn: quinn::Connection,
    details: ConnectionDetails,
    incoming: Mutex<mpsc::Receiver<IncomingStream>>,
    control: Control<RecvStream, SendStream>,
}

impl TunnelClient {
    /// Dials the edge and registers the connection.
    pub async fn connect(config: ClientConfig) -> Result<Self, Error> {
        // Bind the local socket in the edge's IP family.
        let bind: SocketAddr = match config.edge {
            SocketAddr::V4(_) => (Ipv4Addr::UNSPECIFIED, 0).into(),
            SocketAddr::V6(_) => (Ipv6Addr::UNSPECIFIED, 0).into(),
        };

        let mut endpoint = quinn::Endpoint::client(bind)?;
        endpoint.set_default_client_config(quic_config()?);
        let conn = endpoint.connect(config.edge, SERVER_NAME)?.await?;

        // The edge treats the first stream as the control stream.
        let (send, recv) = conn.open_bi().await?;

        // Bootstrap and registration share one deadline.
        let registered = tokio::time::timeout(REGISTER_TIMEOUT, async {
            let mut control = Control::bootstrap(recv, send).await?;
            let details = control.register(&config).await?;

            Ok::<_, Error>((control, details))
        })
        .await
        .map_err(|_| Error::RegistrationTimeout)??;
        let (control, details) = registered;

        Ok(Self {
            incoming: Mutex::new(spawn_acceptor(conn.clone())),
            endpoint,
            conn,
            details,
            control,
        })
    }

    pub fn details(&self) -> &ConnectionDetails {
        &self.details
    }

    /// Waits for the next request stream from the edge. Safe to cancel.
    /// Fails once the connection has ended.
    pub async fn accept(&self) -> Result<IncomingStream, Error> {
        match self.incoming.lock().await.recv().await {
            Some(stream) => Ok(stream),
            None => Err(self.conn.closed().await.into()),
        }
    }

    /// Unregisters from the edge, then waits up to `grace` for the edge to
    /// close the connection. Requests in flight keep running during the wait.
    pub async fn shutdown(self, grace: Duration) {
        let Self {
            endpoint,
            conn,
            mut control,
            ..
        } = self;

        // The edge may close the connection instead of answering.
        let _ = tokio::time::timeout(UNREGISTER_TIMEOUT, control.unregister()).await;

        // The edge closes the connection after unregister. Wait for it so
        // requests in flight can finish.
        let _ = tokio::time::timeout(grace, conn.closed()).await;

        conn.close(0u32.into(), b"");
        endpoint.wait_idle().await;
    }
}

/// Reads each stream's preamble in its own task, so a silent peer blocks
/// nothing. The task ends when the client is dropped or the connection closes.
fn spawn_acceptor(conn: quinn::Connection) -> mpsc::Receiver<IncomingStream> {
    let (tx, rx) = mpsc::channel(STREAM_QUEUE);

    tokio::spawn(async move {
        loop {
            // Wait for the edge to open a stream. Stop when the connection
            // ends or nobody listens any more.
            let (send, recv) = tokio::select! {
                accepted = conn.accept_bi() => match accepted {
                    Ok(streams) => streams,
                    Err(_) => return,
                },
                _ = tx.closed() => return,
            };

            // Parse the request away from the accept loop.
            let tx = tx.clone();
            tokio::spawn(async move {
                let read = tokio::time::timeout(PREAMBLE_TIMEOUT, IncomingStream::read(send, recv));

                match read.await {
                    Ok(Ok(Some(stream))) => {
                        let _ = tx.send(stream).await;
                    }
                    Ok(Ok(None)) => tracing::debug!("dropped edge RPC stream"),
                    Ok(Err(e)) => tracing::debug!("dropped malformed stream: {e}"),
                    Err(_) => tracing::debug!("dropped stream with no request"),
                }
            });
        }
    });

    rx
}

fn root_store() -> Result<rustls::RootCertStore, Error> {
    let mut roots = rustls::RootCertStore::empty();

    // Every certificate of the bundle must parse and load.
    for cert in CertificateDer::pem_slice_iter(ROOTS) {
        let cert = cert.map_err(|e| Error::Tls(e.to_string()))?;
        roots.add(cert).map_err(|e| Error::Tls(e.to_string()))?;
    }

    Ok(roots)
}

fn quic_config() -> Result<quinn::ClientConfig, Error> {
    // TLS 1.3 against the Cloudflare roots only, with the edge's ALPN.
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let mut tls = rustls::ClientConfig::builder_with_provider(provider)
        .with_protocol_versions(&[&rustls::version::TLS13])
        .map_err(|e| Error::Tls(e.to_string()))?
        .with_root_certificates(root_store()?)
        .with_no_client_auth();
    tls.alpn_protocols = vec![ALPN.to_vec()];

    // Same timings as cloudflared: a keep-alive well under the idle timeout
    // keeps a quiet tunnel up.
    let mut transport = quinn::TransportConfig::default();
    transport.keep_alive_interval(Some(Duration::from_secs(1)));
    transport.max_idle_timeout(Some(
        Duration::from_secs(5)
            .try_into()
            .expect("5 s fits an idle timeout"),
    ));
    transport.initial_mtu(1232);
    transport.max_concurrent_bidi_streams(MAX_BIDI_STREAMS);

    let crypto = quinn::crypto::rustls::QuicClientConfig::try_from(tls)
        .map_err(|e| Error::Tls(e.to_string()))?;
    let mut config = quinn::ClientConfig::new(Arc::new(crypto));
    config.transport_config(Arc::new(transport));

    Ok(config)
}

#[cfg(test)]
mod tests {
    #[test]
    fn loads_every_bundled_root() {
        assert_eq!(super::root_store().unwrap().len(), 3);
    }
}
