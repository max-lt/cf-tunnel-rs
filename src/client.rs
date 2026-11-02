use std::{
    net::{Ipv4Addr, Ipv6Addr, SocketAddr},
    sync::Arc,
    time::Duration,
};

use capnp_rpc::{RpcSystem, rpc_twoparty_capnp::Side, twoparty};
use quinn::{RecvStream, SendStream};
use rustls::pki_types::{CertificateDer, pem::PemObject};
use tokio::sync::{Mutex, mpsc, oneshot};
use tokio_util::compat::{TokioAsyncReadCompatExt, TokioAsyncWriteCompatExt};
use uuid::Uuid;

use crate::{
    Error, IncomingStream, Token,
    tunnelrpc_capnp::{connection_response, registration_server},
};

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
/// Registration runs on a dedicated thread with its own runtime, because
/// `capnp-rpc` is not `Send`. Call [`shutdown`](Self::shutdown) to unregister
/// before closing. Dropping the client unregisters on a best-effort basis and
/// closes the QUIC connection without a grace period.
pub struct TunnelClient {
    endpoint: quinn::Endpoint,
    conn: quinn::Connection,
    details: ConnectionDetails,
    incoming: Mutex<mpsc::Receiver<IncomingStream>>,
    shutdown: oneshot::Sender<()>,
    done: oneshot::Receiver<()>,
}

impl TunnelClient {
    /// Dials the edge and registers the connection.
    pub async fn connect(config: ClientConfig) -> Result<Self, Error> {
        let bind: SocketAddr = match config.edge {
            SocketAddr::V4(_) => (Ipv4Addr::UNSPECIFIED, 0).into(),
            SocketAddr::V6(_) => (Ipv6Addr::UNSPECIFIED, 0).into(),
        };
        let mut endpoint = quinn::Endpoint::client(bind)?;
        endpoint.set_default_client_config(quic_config()?);
        let conn = endpoint.connect(config.edge, SERVER_NAME)?.await?;
        let (send, recv) = conn.open_bi().await?;

        let (ready_tx, ready_rx) = oneshot::channel();
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let (done_tx, done_rx) = oneshot::channel();
        std::thread::Builder::new()
            .name("cf-tunnel-control".into())
            .spawn(move || {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("build control runtime");
                let local = tokio::task::LocalSet::new();
                runtime.block_on(local.run_until(control(
                    send,
                    recv,
                    config,
                    ready_tx,
                    shutdown_rx,
                )));
                let _ = done_tx.send(());
            })?;

        let details = ready_rx
            .await
            .map_err(|_| Error::Protocol("control thread ended before registration".into()))??;
        Ok(Self {
            incoming: Mutex::new(spawn_acceptor(conn.clone())),
            endpoint,
            conn,
            details,
            shutdown: shutdown_tx,
            done: done_rx,
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

    /// Resolves when the connection ends.
    pub async fn closed(&self) -> Error {
        self.conn.closed().await.into()
    }

    /// Unregisters from the edge, then waits up to `grace` for the edge to
    /// close the connection. Requests in flight keep running during the wait.
    pub async fn shutdown(self, grace: Duration) {
        let Self {
            endpoint,
            conn,
            shutdown,
            done,
            ..
        } = self;
        drop(shutdown);
        let _ = tokio::time::timeout(UNREGISTER_TIMEOUT, done).await;
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
            let (send, recv) = tokio::select! {
                accepted = conn.accept_bi() => match accepted {
                    Ok(streams) => streams,
                    Err(_) => return,
                },
                _ = tx.closed() => return,
            };
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
    for cert in CertificateDer::pem_slice_iter(ROOTS) {
        let cert = cert.map_err(|e| Error::Tls(e.to_string()))?;
        roots.add(cert).map_err(|e| Error::Tls(e.to_string()))?;
    }
    Ok(roots)
}

fn quic_config() -> Result<quinn::ClientConfig, Error> {
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let mut tls = rustls::ClientConfig::builder_with_provider(provider)
        .with_protocol_versions(&[&rustls::version::TLS13])
        .map_err(|e| Error::Tls(e.to_string()))?
        .with_root_certificates(root_store()?)
        .with_no_client_auth();
    tls.alpn_protocols = vec![ALPN.to_vec()];

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

/// Registers the connection, reports the result, and unregisters on shutdown.
async fn control(
    send: SendStream,
    recv: RecvStream,
    config: ClientConfig,
    ready: oneshot::Sender<Result<ConnectionDetails, Error>>,
    shutdown: oneshot::Receiver<()>,
) {
    let network = twoparty::VatNetwork::new(
        recv.compat(),
        send.compat_write(),
        Side::Client,
        Default::default(),
    );
    let mut rpc = RpcSystem::new(Box::new(network), None);
    let client: registration_server::Client = rpc.bootstrap(Side::Server);
    tokio::task::spawn_local(rpc);

    let registered = tokio::time::timeout(REGISTER_TIMEOUT, register(&client, &config))
        .await
        .unwrap_or(Err(Error::RegistrationTimeout));
    let ok = registered.is_ok();
    let _ = ready.send(registered);
    if !ok {
        return;
    }
    let _ = shutdown.await;
    let _ = client.unregister_connection_request().send().promise.await;
}

async fn register(
    client: &registration_server::Client,
    config: &ClientConfig,
) -> Result<ConnectionDetails, Error> {
    let mut request = client.register_connection_request();
    {
        let mut params = request.get();
        let mut auth = params.reborrow().init_auth();
        auth.set_account_tag(config.token.account_tag.as_str());
        auth.set_tunnel_secret(&config.token.secret);
        params.set_tunnel_id(config.token.tunnel_id.as_bytes());
        params.set_conn_index(config.conn_index);
        let mut options = params.init_options();
        let mut info = options.reborrow().init_client();
        info.set_client_id(config.client_id.as_bytes());
        info.set_version(config.version.as_str());
        info.set_arch(std::env::consts::ARCH);
    }
    let reply = request.send().promise.await?;
    match reply.get()?.get_result()?.get_result().which()? {
        connection_response::result::Which::ConnectionDetails(details) => {
            let details = details?;
            Ok(ConnectionDetails {
                uuid: Uuid::from_slice(details.get_uuid()?)
                    .map_err(|e| Error::Protocol(e.to_string()))?,
                location: details.get_location_name()?.to_str()?.to_owned(),
                remotely_managed: details.get_tunnel_is_remotely_managed(),
            })
        }
        connection_response::result::Which::Error(error) => {
            let error = error?;
            Err(Error::Refused {
                cause: error.get_cause()?.to_str()?.to_owned(),
                retry_after: Duration::from_nanos(error.get_retry_after().max(0) as u64),
                should_retry: error.get_should_retry(),
            })
        }
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn loads_every_bundled_root() {
        assert_eq!(super::root_store().unwrap().len(), 3);
    }
}
